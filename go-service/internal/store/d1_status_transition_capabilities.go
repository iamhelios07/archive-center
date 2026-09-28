package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// D1 reversible status-transition and supersession-resolution capability.
//
// Two capabilities live in this file because both are "a decision plus the
// evidence trail a later reader must be able to undo":
//
//   - ReversibleStatusTransitionStore is the atomic owner of the current status
//     projection and its immutable change ledger. "Reversible" is the load-
//     bearing word: a transition that is supposed to be undoable has to record
//     what it undid, which is what the derivation parent edge and the
//     previous_value_json column are for.
//   - SupersessionResolutionStore records close/supersede/refine/reverse
//     decisions in the audit trail and applies the matching target-state write.
//
// ---------------------------------------------------------------------------
// What the transaction boundary became
// ---------------------------------------------------------------------------
//
// The MariaDB method runs inside a *sql.Tx and takes five FOR UPDATE row locks
// (source revision, registry row, current projection row, prior projection
// event, pending-thread occurrence). D1 has no such thing. D1Conn.Batch is the
// transaction boundary, and a batch returns only an error: it cannot return
// rows, so a SELECT cannot be issued from the middle of one.
//
// The port therefore splits the method at exactly the point where the MariaDB
// transaction stops reading and starts writing:
//
//	phase 1 (reads, outside the batch)  -> the FOR UPDATE probes
//	phase 2 (writes, one D1Conn.Batch)  -> every INSERT/UPDATE/DELETE
//
// What is lost is row-level serialisation BETWEEN the phases. It is stated here
// rather than hidden, because it is the one real behavioural gap:
//
//   - Two transitions racing on one (owner, status_key) slot can both read
//     "no current row" and both write. The table does not corrupt: the second
//     write is still an upsert on the same UNIQUE
//     (chat_session_id, registry_id, owner_scope, owner_id) key, both events
//     land (that is the history), and the last writer owns the current value.
//     MariaDB would additionally have serialised the two, so one of them could
//     have been rejected as ErrStatusProjectionStale; on D1 that rejection now
//     depends only on the source turns, which is the same rule MariaDB applies
//     once the two transactions are ordered.
//   - A rollback (InvalidateSourceRevisions) landing between phase 1 and phase
//     2 can leave an event whose source revision is no longer active. The
//     in-process window is closed by taking the same memoryDerivationWriteMu
//     mutex the rollback path and the precise-memory writer take. A
//     cross-Container race is the unresolved D1 locking boundary every other
//     D1 write capability documents, and it fails loudly rather than silently
//     overwriting.
//
// FOR UPDATE is not simply deleted everywhere. Three of the five probes are
// reads whose only purpose was the lock, so they are dropped. Two are not: the
// source-revision fence and the registry lock for imported history branch on
// their result, and the pending-thread lookup decides whether the statement
// that follows is an UPDATE-by-id or an INSERT. Those are still issued.
//
// ---------------------------------------------------------------------------
// What LAST_INSERT_ID became
// ---------------------------------------------------------------------------
//
// The MariaDB method chains identities inside the transaction:
//
//	current := INSERT ... ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)
//	event.ID := INSERT INTO status_change_events ...      -> LastInsertId
//	dep    := INSERT INTO memory_derivation_dependencies (..., child_artifact_id = event.ID)
//	event.StatusValueID = current.ID
//
// A D1 batch surfaces no id, and reading back after the commit is too late: the
// dependency row and the event's own status_value_id are written inside the
// batch. Both are therefore expressed as scalar subqueries over the row the
// preceding statement in the SAME batch has just written:
//
//	INSERT INTO status_change_events (..., status_value_id, ...)
//	VALUES (..., NULLIF((SELECT id FROM status_current_values WHERE <upsert key>), 0), ...)
//
//	INSERT INTO memory_derivation_dependencies (..., child_artifact_id, ...)
//	SELECT ..., (SELECT id FROM status_change_events WHERE <the idempotency key> ORDER BY id DESC LIMIT 1), ...
//	WHERE true
//	ON CONFLICT (<the eight identity columns>) DO UPDATE SET ...
//
// The dependency subquery is unambiguous precisely because the replay probe in
// phase 1 already proved that no row carries this (session, revision, unit)
// triple: the highest id under that key is the row this batch inserted. The
// "WHERE true" is not decoration ??SQLite documents that an upsert attached to
// a SELECT is ambiguous with a join's ON clause unless the SELECT has a WHERE.
//
// The ids the CALLER sees are read back after the commit with the same
// statements that produced them, exactly as SavePreciseMemoryUnit reads its
// unit id back after its batch. A failed readback leaves the id zero, which is
// the outcome LastInsertId failure has on the reference path too: the write
// landed, the handle did not.
//
// ---------------------------------------------------------------------------
// Why the evidence is rebuilt before the batch
// ---------------------------------------------------------------------------
//
// stateRepairArtifactsEvidence rewrites the event's evidence from the repair
// artifacts that were actually applied, and repairStatusEvidence stamps the
// state_repair.v1 source contract. Both feed the INSERT, so both must be known
// before the batch. The artifact application itself is the reference routine
// statement for statement; only the per-row SELECT moved into phase 1, because
// it has to read the live row to fill change.Before and to rebase the recorded
// JSON delta onto whatever the row currently holds rather than onto what it
// held when the repair was planned.
//
// ---------------------------------------------------------------------------
// Supersession resolution
// ---------------------------------------------------------------------------
//
// SaveSupersessionResolution is the same two-statement commit the MariaDB
// method is: an audit_logs insert and a target-state update, together or not
// at all. The decision normalisation, the details JSON, the summary and the
// read-back projection are the shared package functions the MariaDB method
// calls, reused rather than reimplemented on purpose: details_json is returned
// to the client verbatim, so a second encoder would be a second contract.

var _ ReversibleStatusTransitionStore = (*d1Store)(nil)
var _ SupersessionResolutionStore = (*d1Store)(nil)

// ---------------------------------------------------------------------------
// statements
// ---------------------------------------------------------------------------

// d1StatusTransitionEventLookup is the idempotency probe for one source unit.
//
// A transition's identity is (chat_session_id, source_revision, source_unit_id)
// and two of those three members live only INSIDE evidence_json. That is why
// the projection predicates cannot be "simplified" into column comparisons:
// there are no columns for them.
//
// COALESCE(json_extract(...), ?? on the revision mirrors the reference
// COALESCE over JSON_UNQUOTE(JSON_EXTRACT(...)): an absent path, an explicit
// JSON null and the empty string all compare equal to a blank revision, which
// is what keeps an imported-repair event (no accepted source) addressable by
// its unit id. The source_unit_id side has no COALESCE on either provider, so
// an event with no unit id never matches a lookup that asks for one.
//
// ORDER BY id DESC LIMIT 1 with no created_at tiebreak is the newest-wins rule
// and is load-bearing: the replay must report the LATEST observation of a unit,
// not the first one ever written.
var d1StatusTransitionEventLookup = `
	SELECT id, chat_session_id, registry_id, status_value_id, status_key, owner_scope, owner_id,
	       event_kind, previous_value_json, new_value_json, evidence_json, source_turn,
	       story_clock_json, event_state, created_at
	FROM status_change_events
	WHERE chat_session_id = ?
	  AND COALESCE(json_extract(evidence_json, '$."source_revision"'), '') = ?
	  AND json_extract(evidence_json, '$."source_unit_id"') = ?
	ORDER BY id DESC
	LIMIT 1`

// d1StatusTransitionEventSubquery is the same probe rendered as a scalar
// subquery so a statement inside the batch can name the event the preceding
// statement inserted. It must not drift from d1StatusTransitionEventLookup: if
// it did, the dependency row would record a child_artifact_id that is not the
// event this transition wrote, and the derivation graph would silently point
// at the wrong node.
const d1StatusTransitionEventSubquery = `(
	SELECT id FROM status_change_events
	WHERE chat_session_id = ?
	  AND COALESCE(json_extract(evidence_json, '$."source_revision"'), '') = ?
	  AND json_extract(evidence_json, '$."source_unit_id"') = ?
	ORDER BY id DESC
	LIMIT 1)`

// d1StatusTransitionCurrentValueProjection is the shared SELECT and revision
// join for every current-value read in this file.
//
// The lifecycle_state filter belongs on the LEFT JOIN, not in the WHERE, on both
// providers: moving it into the WHERE would turn the outer join into an inner
// one and silently drop every slot whose source revision was rolled back. The
// join is to a UNIQUE (source_revision) key, so it can duplicate rows; the
// eligibility predicate below is what actually keeps a slot out of the current
// projection.
const d1StatusTransitionCurrentValueProjection = `
	SELECT current_value.id, current_value.chat_session_id, current_value.registry_id,
	       current_value.status_key, current_value.owner_scope, current_value.owner_id,
	       current_value.owner_label, current_value.value_kind, current_value.value_json,
	       current_value.evidence_json, current_value.source_turn,
	       current_value.write_state, current_value.created_at, current_value.updated_at
	FROM status_current_values current_value
	LEFT JOIN memory_source_revisions source_revision
	  ON source_revision.chat_session_id = current_value.chat_session_id
	 AND source_revision.source_revision = json_extract(current_value.evidence_json, '$."source_revision"')
	 AND source_revision.lifecycle_state = 'active'`

// d1StatusTransitionCurrentValueEligible is the projection-eligibility
// predicate, factored out so the per-owner and per-scope reads cannot drift
// apart: a slot visible to one must be visible to the other.
var d1StatusTransitionCurrentValueEligible = ` AND ` +
	d1StatusProjectionSourceSQL("current_value", "source_revision") +
	`
	  AND current_value.write_state = 'current'`

// d1StatusTransitionCurrentValueForOwner is the one-owner-slot current
// projection read used by the replay branch and by the staleness probe.
var d1StatusTransitionCurrentValueForOwner = d1StatusTransitionCurrentValueProjection + `
	WHERE current_value.chat_session_id = ? AND current_value.owner_scope = ?
	  AND current_value.owner_id = ? AND current_value.status_key = ?` +
	d1StatusTransitionCurrentValueEligible + `
	ORDER BY current_value.updated_at DESC, current_value.id DESC`

// d1StatusTransitionCurrentValueObservationTurn reads the stored row's
// observation turn without the revision join, exactly as the reference's
// `SELECT <turn> FROM status_current_values ... FOR UPDATE` does. The staleness
// rule is about the ROW's own recorded turn; joining the revision here would
// make a rolled-back row read as "never observed" and let an old source
// overwrite it.
var d1StatusTransitionCurrentValueObservationTurn = `
	SELECT ` + d1StatusTransitionTurnSQLCurrentValue + `
	FROM status_current_values current_value
	WHERE current_value.chat_session_id = ? AND current_value.owner_scope = ?
	  AND current_value.owner_id = ? AND current_value.status_key = ?`

// d1StatusTransitionTurnSQLCurrentValue is substituted at package
// initialisation by d1StatusObservationTurnSQL("current_value"); it exists
// because the predicate embeds a function call and so cannot be a constant.
var d1StatusTransitionTurnSQLCurrentValue = d1StatusObservationTurnSQL("current_value")

// d1StatusTransitionRemoveObservationTurn reads the newest surviving removal
// observation for one owner slot.
//
// A removal is still a projection observation; it simply has no row in
// status_current_values. Without this probe an explicit repair that clears a
// value would leave the slot looking unobserved, and a delayed source could
// re-create a value the user already asked to remove. d1StatusProjectionSourceSQL
// is applied for the same reason it is applied to the current-value read: a
// removal recorded against a source the user has since rolled back is not an
// observation any more.
var d1StatusTransitionRemoveObservationTurn = `
	SELECT ` + d1StatusTransitionTurnSQLEvent + `
	FROM status_change_events event
	LEFT JOIN memory_source_revisions source_revision
	  ON source_revision.chat_session_id = event.chat_session_id
	 AND source_revision.source_revision = json_extract(event.evidence_json, '$."source_revision"')
	WHERE event.chat_session_id = ? AND event.owner_scope = ? AND event.owner_id = ? AND event.status_key = ?
	  AND ` + d1StatusProjectionSourceSQL("event", "source_revision") + `
	  AND ` + d1CurrentProjectionPredicate("event.evidence_json") + `
	  AND ` + d1JSONTextEquals("event.evidence_json", d1JSONPath("projection_action"), "remove") + `
	ORDER BY ` + d1StatusTransitionTurnSQLEvent + ` DESC, event.id DESC
	LIMIT 1`

// d1StatusTransitionTurnSQLEvent is substituted at package
// initialisation by d1StatusObservationTurnSQL("event").
var d1StatusTransitionTurnSQLEvent = d1StatusObservationTurnSQL("event")

// d1StatusTransitionTurnSQLPrior is d1StatusObservationTurnSQL("prior").
// The observation-turn expression is alias-qualified, so a statement that joins
// the same table twice needs one expression per alias: reusing the "event" form
// inside a statement whose rows are aliased "prior" would make SQLite look for a
// column that does not exist.
var d1StatusTransitionTurnSQLPrior = d1StatusObservationTurnSQL("prior")

// d1StatusTransitionTurnSQLNewer is d1StatusObservationTurnSQL("newer"),
// the anti-join's "is there something more recent" side.
var d1StatusTransitionTurnSQLNewer = d1StatusObservationTurnSQL("newer")

// d1StatusTransitionPriorProjectionEventID finds the latest active projection
// event for one owner slot; its id becomes the derivation parent of the event
// this transition writes, which is what makes the current value traceable back
// to the observation it replaced. The ORDER BY (observation turn, id) is the
// tiebreak that makes "the prior event" well defined when two observations
// share a source turn.
var d1StatusTransitionPriorProjectionEventID = `
	SELECT prior.id
	FROM status_change_events prior
	LEFT JOIN memory_source_revisions source_revision
	  ON source_revision.chat_session_id = prior.chat_session_id
	 AND source_revision.source_revision = json_extract(prior.evidence_json, '$."source_revision"')
	 AND source_revision.lifecycle_state = 'active'
	WHERE prior.chat_session_id = ?
	  AND ` + d1StatusProjectionSourceSQL("prior", "source_revision") + `
	  AND prior.status_key = ?
	  AND prior.owner_scope = ?
	  AND prior.owner_id = ?
	  AND ` + d1CurrentProjectionPredicate("prior.evidence_json") + `
	ORDER BY ` + d1StatusTransitionTurnSQLPrior + ` DESC, prior.id DESC
	LIMIT 1`

// d1StatusTransitionPendingThreadID is the narrative projection's occurrence
// lookup. A deleted row id is not a new occurrence and may be allocated again,
// so the newest row under a thread key is the occurrence to update.
const d1StatusTransitionPendingThreadID = `
	SELECT id FROM pending_threads
	WHERE chat_session_id = ? AND thread_key = ?
	ORDER BY id DESC LIMIT 1`

// d1StatusTransitionCurrentValueUpsert is the MariaDB current-value
// ON DUPLICATE KEY UPDATE expressed as a SQLite upsert on the same UNIQUE
// (chat_session_id, registry_id, owner_scope, owner_id).
//
// created_at is deliberately absent from the update list so an existing row
// keeps its original creation time while updated_at advances, matching the
// reference. updated_at uses the inline now expression rather than a bound
// value because the reference uses CURRENT_TIMESTAMP(3) there, not the
// caller's timestamp: on an update the caller's CreatedAt is the ORIGINAL
// creation time, and binding it would move updated_at backwards.
const d1StatusTransitionCurrentValueUpsert = `
	INSERT INTO status_current_values (
		chat_session_id, registry_id, status_key, owner_scope, owner_id, owner_label,
		value_kind, value_json, evidence_json, source_turn, write_state, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), ?, ?)
	ON CONFLICT (chat_session_id, registry_id, owner_scope, owner_id) DO UPDATE SET
		status_key = excluded.status_key,
		owner_label = excluded.owner_label,
		value_kind = excluded.value_kind,
		value_json = excluded.value_json,
		evidence_json = excluded.evidence_json,
		source_turn = excluded.source_turn,
		write_state = excluded.write_state,
		updated_at = ` + d1NowExpression

// d1StatusTransitionEventInsert is the MariaDB event insert. The
// status_value_id expression is supplied by the caller because it differs
// between the "this batch just wrote the current value" case (a scalar
// subquery) and every other case (a bound value through NULLIF).
const d1StatusTransitionEventInsert = `
	INSERT INTO status_change_events (
		chat_session_id, registry_id, status_value_id, status_key, owner_scope, owner_id,
		event_kind, previous_value_json, new_value_json, evidence_json, source_turn,
		story_clock_json, event_state, created_at
	) VALUES (?, ?, %s, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), ?, ?, ?)`

// d1StatusTransitionDependencyInsert is insertStatusTransitionDependenciesTx's
// statement, one per parent.
//
// The child id is the scalar subquery over the event just inserted. The
// upsert's job is NOT a refresh: a rollback that invalidated this dependency and
// a later re-derivation of the same event must converge on ONE row that is
// active again, which is why invalidated_at is cleared rather than left alone.
// The conflict target is the canonical eight-column UNIQUE of
// memory_derivation_dependencies ??deriving the child id in a SELECT means
// "WHERE true" is required for SQLite to parse the upsert unambiguously.
const d1StatusTransitionDependencyInsert = `
	INSERT INTO memory_derivation_dependencies (
		contract_version, chat_session_id, source_revision,
		root_source_pointer, child_artifact_type, child_artifact_id,
		parent_artifact_type, parent_artifact_id, derivation_version,
		extractor_version, index_version, lifecycle_state, created_at, updated_at
	) SELECT ?, ?, ?, ?, 'status_change_event', ` + d1StatusTransitionEventSubquery + `, ?, ?, ?, ?, ?, 'active', ?, ?
	WHERE true
	ON CONFLICT (source_revision, child_artifact_type, child_artifact_id, parent_artifact_type,
	              parent_artifact_id, derivation_version, extractor_version, index_version)
	DO UPDATE SET lifecycle_state = 'active', invalidated_at = NULL, updated_at = excluded.updated_at`

// ---------------------------------------------------------------------------
// ApplyReversibleStatusTransition
// ---------------------------------------------------------------------------

// ApplyReversibleStatusTransition commits one current projection row, its
// history event, its derivation parents and its narrative pending projection
// as a single atomic D1 batch.
//
// Replay: a second call with the same (session, source revision, source unit)
// key returns the STORED event and current value with Replayed=true and writes
// nothing. The key is a triple and two of its members are only carried inside
// evidence_json, so a "same key, different payload" write is by construction a
// second observation of the same source unit rather than a conflicting one; the
// reference makes the same choice, which is why the idempotency rule for this
// capability is "detect the replay, never duplicate the row" rather than the
// conflict error used by the precise-memory writer.
//
// The repair branch (SourceContract == state_repair.v1) is the reverse path:
// it is the only caller that may delete a current value, it reinstates the exact
// observed pending snapshot, and it applies the recorded artifact changes. It
// is ported in full because a forward-only implementation that reports success
// here is precisely the failure this capability is named to prevent.
func (s *d1Store) ApplyReversibleStatusTransition(ctx context.Context, transition ReversibleStatusTransition) (ReversibleStatusTransitionResult, error) {
	result := ReversibleStatusTransitionResult{}
	sourceRevision := strings.TrimSpace(transition.SourceRevision)
	sourceUnitID := strings.TrimSpace(transition.SourceUnitID)
	event := transition.Event
	isRepair := strings.TrimSpace(transition.SourceContract) == StateRepairContract
	if isRepair {
		event.EvidenceJSON = repairStatusEvidence(event.EvidenceJSON, sourceRevision, sourceUnitID, transition.DeleteCurrent)
		if transition.CurrentValue != nil {
			current := *transition.CurrentValue
			current.EvidenceJSON = repairStatusEvidence(current.EvidenceJSON, sourceRevision, sourceUnitID, false)
			if transition.PendingSnapshot != nil {
				current.ValueJSON = repairPendingSnapshotValue(current.ValueJSON, transition.PendingSnapshot)
				event.NewValueJSON = current.ValueJSON
			}
			transition.CurrentValue = &current
		}
		if transition.DeleteCurrent {
			value := map[string]any{}
			_ = json.Unmarshal([]byte(event.NewValueJSON), &value)
			if value == nil {
				value = map[string]any{}
			}
			if transition.PendingSnapshot != nil {
				value["pending_thread"] = transition.PendingSnapshot
				value["repair_pending_snapshot"] = true
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				return result, err
			}
			event.NewValueJSON = string(encoded)
		}
	}
	// The reference wraps the return value in
	// reversibleStatusTransitionLockDiagnostic to re-label a MySQL deadlock
	// (1213) or lock-wait timeout (1205) with the transition's identity. D1
	// raises SQLITE_BUSY, which carries neither code nor a MySQL SQLSTATE, so
	// there is nothing to match on and the diagnostic is deliberately NOT
	// reimplemented: inventing a MariaDB error number for a SQLite failure
	// would be a lie that any caller matching on it would believe.
	if err := d1ValidateReversibleStatusTransition(transition, event, sourceRevision, sourceUnitID, isRepair); err != nil {
		return result, err
	}

	// Phase 1 and phase 2 must not straddle a rollback: InvalidateSourceRevisions
	// holds this same mutex, so no in-process invalidation can land between the
	// source fence below and the batch.
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	// ---- phase 1: the reference's locked reads, in the same order ----

	if sourceRevision != "" {
		// The fence. A revision the user has already rolled back, or one that
		// was never accepted, must not admit a projection, and nothing may be
		// written when this refuses.
		var lifecycle string
		err := s.conn.QueryRow(ctx, `SELECT lifecycle_state FROM memory_source_revisions
			WHERE chat_session_id = ? AND source_revision = ?`,
			event.ChatSessionID, sourceRevision).Scan(&lifecycle)
		if errors.Is(err, errD1NoRows) {
			return result, ErrSourceRevisionStale
		}
		if err != nil {
			return result, err
		}
		if strings.TrimSpace(lifecycle) != "active" {
			return result, ErrSourceRevisionStale
		}
	} else if isRepair {
		// Imported history has no accepted-source row to lock. The reference
		// locks its existing registry row to serialise operation replay without
		// fabricating a source. D1 cannot lock, but it must still prove the
		// registry row exists, because the event insert carries a foreign key to
		// it and a missing row would fail the whole batch instead of this
		// transition alone. The reference returns the raw no-rows error here,
		// NOT ErrNotFound, and that distinction is preserved.
		var registryID int64
		if err := s.conn.QueryRow(ctx, `SELECT id FROM status_schema_registry WHERE id = ?`,
			event.RegistryID).Scan(&registryID); err != nil {
			return result, err
		}
	}

	existing, err := d1StatusTransitionScanEvent(s.conn.QueryRow(ctx,
		d1StatusTransitionEventLookup, event.ChatSessionID, sourceRevision, sourceUnitID))
	if err == nil {
		result.Event = existing
		result.Replayed = true
		if transition.CurrentValue != nil && !transition.DeleteCurrent {
			currentRows, readErr := s.d1StatusTransitionListCurrentValues(ctx, event.ChatSessionID, event.OwnerScope, event.OwnerID, event.StatusKey)
			if readErr != nil {
				return ReversibleStatusTransitionResult{}, readErr
			}
			if len(currentRows) > 0 {
				result.CurrentValue = currentRows[0]
			}
		}
		// The reference commits an empty transaction here. A D1 replay performs
		// no write at all, so there is nothing to commit.
		return result, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return result, err
	}

	if transition.CurrentValue != nil || (isRepair && transition.DeleteCurrent) {
		existingTurn, turnErr := s.d1StatusTransitionExistingObservationTurn(ctx, event)
		if turnErr != nil {
			return result, turnErr
		}
		// A projection that already observed a later turn is not overwritten by
		// an earlier one. This is the whole reason the probe exists: without it
		// a late-arriving source would silently resurrect superseded state.
		if existingTurn != nil && *existingTurn > StatusChangeEventObservationTurn(event) {
			return result, ErrStatusProjectionStale
		}
	}

	statements := make([]D1Statement, 0, 8)

	if isRepair && len(transition.ArtifactChanges) > 0 {
		changes, artifactStatements, err := s.d1StatusTransitionApplyRepairArtifacts(ctx, event.ChatSessionID, sourceUnitID, transition.ArtifactChanges, event.CreatedAt)
		if err != nil {
			return result, err
		}
		statements = append(statements, artifactStatements...)
		event.EvidenceJSON = stateRepairArtifactsEvidence(event.EvidenceJSON, changes)
	}

	wroteCurrentValue := false
	switch {
	case isRepair && transition.DeleteCurrent:
		statements = append(statements, D1Statement{
			SQL:  `DELETE FROM status_current_values WHERE chat_session_id = ? AND owner_scope = ? AND owner_id = ? AND status_key = ?`,
			Args: []any{event.ChatSessionID, event.OwnerScope, event.OwnerID, event.StatusKey},
		})
	case transition.CurrentValue != nil:
		current := *transition.CurrentValue
		now := d1TimeValue(current.CreatedAt)
		state := firstNonEmptyString(current.WriteState, "current")
		statements = append(statements, D1Statement{
			SQL: d1StatusTransitionCurrentValueUpsert,
			Args: []any{
				current.ChatSessionID, current.RegistryID, current.StatusKey, current.OwnerScope,
				current.OwnerID, d1NullableString(current.OwnerLabel), current.ValueKind,
				current.ValueJSON, current.EvidenceJSON, current.SourceTurn, state, now,
			},
		})
		wroteCurrentValue = true
		current.CreatedAt = parseD1TimeOrZero(now)
		current.UpdatedAt = current.CreatedAt
		current.WriteState = state
		result.CurrentValue = current
	}

	priorEventID, err := s.d1StatusTransitionLatestActiveEventID(ctx, event)
	if err != nil {
		return result, err
	}

	statusValueSQL := `NULLIF(?, 0)`
	statusValueArgs := []any{event.StatusValueID}
	if wroteCurrentValue {
		// The event has to point at the row this very batch wrote, and a batch
		// cannot report that id. The subquery is the upsert key, which the
		// previous statement in this same batch has just materialised, so the
		// value is the current value's id inside the transaction ??not a
		// readback that could race.
		statusValueSQL = `NULLIF((SELECT id FROM status_current_values
			WHERE chat_session_id = ? AND registry_id = ? AND owner_scope = ? AND owner_id = ?), 0)`
		statusValueArgs = []any{event.ChatSessionID, event.RegistryID, event.OwnerScope, event.OwnerID}
	}

	eventNow := d1TimeValue(event.CreatedAt)
	eventState := firstNonEmptyString(event.EventState, "recorded")
	eventArgs := make([]any, 0, 15)
	eventArgs = append(eventArgs, event.ChatSessionID, event.RegistryID)
	eventArgs = append(eventArgs, statusValueArgs...)
	eventArgs = append(eventArgs,
		event.StatusKey, event.OwnerScope, event.OwnerID,
		event.EventKind, d1NullableString(event.PreviousValueJSON), d1NullableString(event.NewValueJSON),
		event.EvidenceJSON, event.SourceTurn, d1NullableString(event.StoryClockJSON), eventState, eventNow)
	statements = append(statements, D1Statement{SQL: fmt.Sprintf(d1StatusTransitionEventInsert, statusValueSQL), Args: eventArgs})

	if sourceRevision != "" {
		dependencyStatements, err := d1StatusTransitionDependencyStatements(event, sourceRevision, sourceUnitID, priorEventID, eventNow)
		if err != nil {
			return ReversibleStatusTransitionResult{}, err
		}
		statements = append(statements, dependencyStatements...)
	}

	switch {
	case isRepair && transition.PendingSnapshot != nil:
		pendingStatements, err := s.d1StatusTransitionRestorePendingSnapshot(ctx, event.ChatSessionID, transition.PendingSnapshot)
		if err != nil {
			return result, err
		}
		statements = append(statements, pendingStatements...)
	case transition.CurrentValue != nil && !transition.DeleteCurrent && event.StatusKey == "narrative_state":
		pendingStatements, err := s.d1StatusTransitionProjectPendingThread(ctx, event.ChatSessionID, result.CurrentValue.ValueJSON)
		if err != nil {
			return result, err
		}
		statements = append(statements, pendingStatements...)
	}

	// ---- phase 2: one atomic batch ----

	if err := s.conn.Batch(ctx, statements...); err != nil {
		return result, err
	}

	stored, err := d1StatusTransitionScanEvent(s.conn.QueryRow(ctx,
		d1StatusTransitionEventLookup, event.ChatSessionID, sourceRevision, sourceUnitID))
	if err != nil {
		// The batch committed. A failed readback is the LastInsertId-failure
		// shape: the write landed, the handle did not. Returning the in-memory
		// event preserves the reference's "commit succeeded" outcome.
		return result, nil
	}
	result.Event = stored
	if wroteCurrentValue {
		var currentID int64
		if err := s.conn.QueryRow(ctx, `
			SELECT id FROM status_current_values
			WHERE chat_session_id = ? AND registry_id = ? AND owner_scope = ? AND owner_id = ?`,
			result.CurrentValue.ChatSessionID, result.CurrentValue.RegistryID,
			result.CurrentValue.OwnerScope, result.CurrentValue.OwnerID).Scan(&currentID); err == nil {
			result.CurrentValue.ID = currentID
		}
	}
	return result, nil
}

// d1ValidateReversibleStatusTransition is the reference guard block, unchanged.
// It is pure Go on both providers, and it is the only thing standing between a
// malformed transition and a batch that would commit half of it: the batch is
// atomic, so a constraint failure late in the sequence rolls back the whole
// thing, but only after the store has already done the read-side work.
func d1ValidateReversibleStatusTransition(transition ReversibleStatusTransition, event StatusChangeEvent, sourceRevision, sourceUnitID string, isRepair bool) error {
	if (!isRepair && strings.TrimSpace(transition.SourceContract) != acceptedSourceObservationContract) ||
		(!isRepair && sourceRevision == "") || sourceUnitID == "" ||
		strings.TrimSpace(event.ChatSessionID) == "" ||
		strings.TrimSpace(event.StatusKey) == "" ||
		strings.TrimSpace(event.OwnerScope) == "" ||
		strings.TrimSpace(event.OwnerID) == "" ||
		(!isRepair && event.SourceTurn <= 0) || (isRepair && event.SourceTurn < 0) {
		return ErrSourceRevisionStale
	}
	eventEvidence := map[string]any{}
	if json.Unmarshal([]byte(event.EvidenceJSON), &eventEvidence) != nil ||
		strings.TrimSpace(fmt.Sprint(eventEvidence["source_revision"])) != sourceRevision ||
		strings.TrimSpace(fmt.Sprint(eventEvidence["source_unit_id"])) != sourceUnitID {
		return ErrSourceRevisionStale
	}
	if transition.CurrentValue != nil {
		current := transition.CurrentValue
		currentEvidence := map[string]any{}
		if current.ChatSessionID != event.ChatSessionID ||
			current.StatusKey != event.StatusKey ||
			current.OwnerScope != event.OwnerScope ||
			current.OwnerID != event.OwnerID ||
			current.SourceTurn != event.SourceTurn ||
			json.Unmarshal([]byte(current.EvidenceJSON), &currentEvidence) != nil ||
			strings.TrimSpace(fmt.Sprint(currentEvidence["source_revision"])) != sourceRevision ||
			strings.TrimSpace(fmt.Sprint(currentEvidence["source_unit_id"])) != sourceUnitID {
			return ErrSourceRevisionStale
		}
	}
	return nil
}

// d1StatusTransitionExistingObservationTurn is the two-step staleness probe. It
// reports nil when the slot has never been observed at all.
func (s *d1Store) d1StatusTransitionExistingObservationTurn(ctx context.Context, event StatusChangeEvent) (*int, error) {
	var storedTurn *int64
	err := s.conn.QueryRow(ctx, d1StatusTransitionCurrentValueObservationTurn,
		event.ChatSessionID, event.OwnerScope, event.OwnerID, event.StatusKey).Scan(&storedTurn)
	switch {
	case err == nil:
		if storedTurn == nil {
			return nil, nil
		}
		turn := int(*storedTurn)
		return &turn, nil
	case !errors.Is(err, errD1NoRows):
		return nil, err
	}
	// No current row: the slot may still have been observed and then explicitly
	// removed. That removal keeps the same ordering as a materialised value so
	// a delayed source cannot re-create what the user cleared.
	var removalTurn *int64
	if err := s.conn.QueryRow(ctx, d1StatusTransitionRemoveObservationTurn,
		event.ChatSessionID, event.OwnerScope, event.OwnerID, event.StatusKey).Scan(&removalTurn); err != nil {
		if errors.Is(err, errD1NoRows) {
			return nil, nil
		}
		return nil, err
	}
	if removalTurn == nil {
		return nil, nil
	}
	turn := int(*removalTurn)
	return &turn, nil
}

// d1StatusTransitionLatestActiveEventID is latestActiveStatusEventIDTx.
func (s *d1Store) d1StatusTransitionLatestActiveEventID(ctx context.Context, event StatusChangeEvent) (int64, error) {
	var id *int64
	err := s.conn.QueryRow(ctx, d1StatusTransitionPriorProjectionEventID,
		event.ChatSessionID, event.StatusKey, event.OwnerScope, event.OwnerID).Scan(&id)
	if err != nil {
		if errors.Is(err, errD1NoRows) {
			return 0, nil
		}
		return 0, err
	}
	if id == nil {
		return 0, nil
	}
	return *id, nil
}

// d1StatusTransitionDependencyStatements is insertStatusTransitionDependenciesTx
// without the transaction.
//
// The parent set is the source revision, the prior active projection event (the
// "reversible" edge: this observation was derived from that one), and each
// distinct positive direct evidence the event cites. A repeated evidence id is
// collapsed because a duplicated edge would make the event look twice-derived
// from the same record.
func d1StatusTransitionDependencyStatements(event StatusChangeEvent, sourceRevision, sourceUnitID string, priorEventID int64, now string) ([]D1Statement, error) {
	type parent struct{ kind, id string }
	parents := []parent{{kind: "source_revision", id: sourceRevision}}
	if priorEventID > 0 {
		parents = append(parents, parent{kind: "status_change_event", id: strconv.FormatInt(priorEventID, 10)})
	}
	evidence := map[string]any{}
	_ = json.Unmarshal([]byte(event.EvidenceJSON), &evidence)
	seenEvidence := map[int64]bool{}
	for _, rawID := range anySlice(evidence["direct_evidence_ids"]) {
		id, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(rawID)), 10, 64)
		if err != nil || id <= 0 || seenEvidence[id] {
			continue
		}
		seenEvidence[id] = true
		parents = append(parents, parent{kind: "direct_evidence", id: strconv.FormatInt(id, 10)})
	}
	statements := make([]D1Statement, 0, len(parents))
	for _, p := range parents {
		statements = append(statements, D1Statement{
			SQL: d1StatusTransitionDependencyInsert,
			Args: []any{
				MemoryDerivationDependencyContract, event.ChatSessionID, sourceRevision,
				"source_revision:" + sourceRevision,
				event.ChatSessionID, sourceRevision, sourceUnitID,
				p.kind, p.id, "status_transition.v1", "source_observation", "not_materialized",
				now, now,
			},
		})
	}
	// sourceUnitID is bound into the idempotency subquery, not used as a parent
	// id: it identifies a source unit, not a row.
	_ = sourceUnitID
	return statements, nil
}

// ---------------------------------------------------------------------------
// reversible status reads
// ---------------------------------------------------------------------------

// d1StatusTransitionScanEvent is scanStatusChangeEvent over the D1 transport.
// Absence maps to ErrNotFound, exactly as sql.ErrNoRows does on the reference
// path, because routes branch on that identity and a raw errD1NoRows would
// surface as an unknown 500.
func d1StatusTransitionScanEvent(row d1Scanner) (StatusChangeEvent, error) {
	var item StatusChangeEvent
	var statusValueID, sourceTurn *int64
	var previousValueJSON, newValueJSON, storyClockJSON *string
	if err := row.Scan(
		&item.ID, &item.ChatSessionID, &item.RegistryID, &statusValueID, &item.StatusKey, &item.OwnerScope, &item.OwnerID,
		&item.EventKind, &previousValueJSON, &newValueJSON, &item.EvidenceJSON, &sourceTurn,
		&storyClockJSON, &item.EventState, &item.CreatedAt,
	); err != nil {
		if errors.Is(err, errD1NoRows) {
			return StatusChangeEvent{}, ErrNotFound
		}
		return StatusChangeEvent{}, err
	}
	item.StatusValueID = d1DerefInt64(statusValueID)
	item.PreviousValueJSON = d1DerefString(previousValueJSON)
	item.NewValueJSON = d1DerefString(newValueJSON)
	item.StoryClockJSON = d1DerefString(storyClockJSON)
	item.SourceTurn = int(d1DerefInt64(sourceTurn))
	return item, nil
}

// d1StatusTransitionScanCurrent reads one current-value row. owner_label and
// source_turn are nullable in the canonical schema and are read through
// pointer destinations so NULL stays distinct from the empty string and from 0.
func d1StatusTransitionScanCurrent(row d1Scanner) (StatusCurrentValue, error) {
	var item StatusCurrentValue
	var ownerLabel *string
	var sourceTurn *int64
	if err := row.Scan(
		&item.ID, &item.ChatSessionID, &item.RegistryID, &item.StatusKey, &item.OwnerScope, &item.OwnerID,
		&ownerLabel, &item.ValueKind, &item.ValueJSON, &item.EvidenceJSON, &sourceTurn,
		&item.WriteState, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return StatusCurrentValue{}, err
	}
	item.OwnerLabel = d1DerefString(ownerLabel)
	item.SourceTurn = int(d1DerefInt64(sourceTurn))
	return item, nil
}

// d1StatusTransitionListCurrentValues is listStatusCurrentValuesWithExecutor.
// The result is a non-nil empty slice on no match so the replay branch's
// len() test and the callers' JSON encoding agree with the reference.
func (s *d1Store) d1StatusTransitionListCurrentValues(ctx context.Context, chatSessionID, ownerScope, ownerID, statusKey string) ([]StatusCurrentValue, error) {
	rows, err := s.conn.Query(ctx, d1StatusTransitionCurrentValueForOwner,
		chatSessionID, ownerScope, ownerID, statusKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StatusCurrentValue{}
	for rows.Next() {
		item, scanErr := d1StatusTransitionScanCurrent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// GetReversibleStatusEventBySourceUnit resolves the single event a source unit
// produced, newest first. The (revision, unit) pair is read out of evidence_json
// because those columns do not exist; the ORDER BY id DESC is what makes a
// re-emitted unit resolve to its latest observation rather than its first.
func (s *d1Store) GetReversibleStatusEventBySourceUnit(ctx context.Context, chatSessionID, sourceRevision, sourceUnitID string) (StatusChangeEvent, error) {
	return d1StatusTransitionScanEvent(s.conn.QueryRow(ctx,
		d1StatusTransitionEventLookup, chatSessionID, sourceRevision, sourceUnitID))
}

// ListReversibleStatusCurrentValues reads the complete current projection for
// one owner scope across a set of status keys.
//
// There is deliberately NO limit and no fallback default: this is the "exact,
// uncapped rebuild" read the capability is named for, and a bounded default
// would silently truncate a rebuild into an apparently complete but wrong
// state. An empty key list short-circuits to a non-nil empty result rather than
// running `status_key IN ()`, which is a syntax error on both dialects.
func (s *d1Store) ListReversibleStatusCurrentValues(ctx context.Context, chatSessionID, ownerScope string, statusKeys []string) ([]StatusCurrentValue, error) {
	keys := normalizedNonEmptyStrings(statusKeys)
	if len(keys) == 0 {
		return []StatusCurrentValue{}, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(keys)), ",")
	query := d1StatusTransitionCurrentValueProjection + `
	WHERE current_value.chat_session_id = ?` +
		d1StatusTransitionCurrentValueEligible + `
		  AND current_value.owner_scope = ?
		  AND current_value.status_key IN (` + placeholders + `)
		ORDER BY current_value.status_key ASC, current_value.owner_scope ASC, current_value.owner_id ASC`
	args := make([]any, 0, len(keys)+2)
	args = append(args, chatSessionID, strings.TrimSpace(ownerScope))
	for _, key := range keys {
		args = append(args, key)
	}
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StatusCurrentValue{}
	for rows.Next() {
		item, scanErr := d1StatusTransitionScanCurrent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// d1StatusTransitionLatestProjectionEvents is the "one current observation per
// owner slot" anti-join.
//
// The winner is selected by the NOT EXISTS comparison, not by the trailing
// ORDER BY. The comparison is (observation turn, id): a newer event with an
// older source turn does not win, and among equal observation turns the higher
// id wins. The observation turn is repair_recorded_turn when it is a positive
// JSON integer and source_turn otherwise, which is what lets an explicit repair
// recorded now order AFTER the history it corrects without backdating its
// authority to the historical cause turn. The trailing ORDER BY only fixes the
// presentation order of the winners, by (status_key, owner_scope, owner_id).
var d1StatusTransitionLatestProjectionEvents = `
	SELECT e.id, e.chat_session_id, e.registry_id, e.status_value_id, e.status_key, e.owner_scope, e.owner_id,
	       e.event_kind, e.previous_value_json, e.new_value_json, e.evidence_json, e.source_turn,
	       e.story_clock_json, e.event_state, e.created_at
	FROM status_change_events e
	LEFT JOIN memory_source_revisions source_revision
	  ON source_revision.chat_session_id = e.chat_session_id
	 AND source_revision.source_revision = json_extract(e.evidence_json, '$."source_revision"')
	 AND source_revision.lifecycle_state = 'active'
	WHERE e.chat_session_id = ?
	  AND ` + d1StatusProjectionSourceSQL("e", "source_revision") + `
	  AND e.status_key IN (%s)
	  AND ` + d1CurrentProjectionPredicate("e.evidence_json") + `
	  AND NOT EXISTS (
		SELECT 1
		FROM status_change_events newer
		LEFT JOIN memory_source_revisions newer_source
		  ON newer_source.chat_session_id = newer.chat_session_id
		 AND newer_source.source_revision = json_extract(newer.evidence_json, '$."source_revision"')
		 AND newer_source.lifecycle_state = 'active'
		WHERE newer.chat_session_id = e.chat_session_id
		  AND ` + d1StatusProjectionSourceSQL("newer", "newer_source") + `
		  AND newer.status_key = e.status_key
		  AND newer.owner_scope = e.owner_scope
		  AND newer.owner_id = e.owner_id
		  AND ` + d1CurrentProjectionPredicate("newer.evidence_json") + `
		  AND (` + d1StatusTransitionTurnSQLNewer + ` > ` + d1StatusTransitionTurnSQLE + `
		       OR (` + d1StatusTransitionTurnSQLNewer + ` = ` + d1StatusTransitionTurnSQLE + ` AND newer.id > e.id))
	  )
	ORDER BY e.status_key ASC, e.owner_scope ASC, e.owner_id ASC`

// d1StatusTransitionTurnSQLE is substituted at package initialisation
// by d1StatusObservationTurnSQL("e").
var d1StatusTransitionTurnSQLE = d1StatusObservationTurnSQL("e")

// ListLatestReversibleCurrentProjectionEvents returns the newest current-
// projection observation per (status_key, owner_scope, owner_id) for a session.
//
// Like ListReversibleStatusCurrentValues it is uncapped: it is the rebuild read
// that proves the ledger and the current projection agree.
func (s *d1Store) ListLatestReversibleCurrentProjectionEvents(ctx context.Context, chatSessionID string, statusKeys []string) ([]StatusChangeEvent, error) {
	keys := normalizedNonEmptyStrings(statusKeys)
	if len(keys) == 0 {
		return []StatusChangeEvent{}, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(keys)), ",")
	rows, err := s.conn.Query(ctx, fmt.Sprintf(d1StatusTransitionLatestProjectionEvents, placeholders),
		append([]any{chatSessionID}, d1StatusTransitionKeyArgs(keys)...)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StatusChangeEvent{}
	for rows.Next() {
		item, scanErr := d1StatusTransitionScanEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// d1StatusTransitionKeyArgs binds an IN list in the order the placeholders were
// rendered. normalizedNonEmptyStrings already dropped blanks and duplicates, so
// a caller passing [" a ", "a", ""] gets two placeholders, not three ??the
// placeholder count and the argument count can never disagree.
func d1StatusTransitionKeyArgs(keys []string) []any {
	args := make([]any, 0, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	return args
}

// ---------------------------------------------------------------------------
// narrative and repair pending projection
// ---------------------------------------------------------------------------

// d1StatusTransitionProjectPendingThread is projectNarrativePendingThreadTx: it
// consumes the pending projection recorded with the accepted narrative
// transition.
//
// The occurrence lookup is read in phase 1 because it decides the statement:
// a thread key that already exists is UPDATEd by id, a new one is INSERTed. A
// deleted row id is not a new occurrence and may be allocated again, hence
// ORDER BY id DESC rather than "any row under this key".
func (s *d1Store) d1StatusTransitionProjectPendingThread(ctx context.Context, chatSessionID, valueJSON string) ([]D1Statement, error) {
	var value struct {
		PendingThread *PendingThread `json:"pending_thread"`
	}
	if json.Unmarshal([]byte(valueJSON), &value) != nil || value.PendingThread == nil {
		return nil, nil
	}
	p := value.PendingThread
	p.ChatSessionID = chatSessionID
	if strings.TrimSpace(p.ThreadKey) == "" {
		return nil, nil
	}
	var existingID int64
	err := s.conn.QueryRow(ctx, d1StatusTransitionPendingThreadID, chatSessionID, p.ThreadKey).Scan(&existingID)
	if err == nil {
		p.ID = existingID
		// An identified lifecycle update may reopen a resolved occurrence. The
		// row's creation and manual trust fields belong to that occurrence and
		// stay intact, so they are absent from the SET list; every present field
		// is COALESCEd so a blank snapshot cannot blank the stored value.
		return []D1Statement{{
			SQL: `
			UPDATE pending_threads
			SET description = COALESCE(?, description),
			    status = COALESCE(?, status),
			    resolved_turn = NULLIF(?, 0),
			    source_turn = NULLIF(?, 0),
			    priority = NULLIF(?, 0),
			    hook_type = COALESCE(?, hook_type),
			    hook_metadata_json = COALESCE(?, hook_metadata_json),
			    updated_at = ?
			WHERE id = ?`,
			Args: []any{
				d1NullableString(p.Description), d1NullableString(firstNonEmptyString(p.Status, "open")),
				p.ResolvedTurn, p.SourceTurn, p.Priority, d1NullableString(p.HookType),
				d1NullableString(firstNonEmptyString(p.HookMetadataJSON, p.DetailsJSON)),
				d1TimeValue(p.UpdatedAt), p.ID,
			},
		}}, nil
	}
	if !errors.Is(err, errD1NoRows) {
		return nil, err
	}
	return []D1Statement{{
		SQL: `
		INSERT INTO pending_threads (
			chat_session_id, thread_key, description, status, created_turn,
			resolved_turn, source_turn, priority, hook_type, hook_metadata_json,
			pinned, suppressed, user_corrected, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		Args: []any{
			chatSessionID, p.ThreadKey, d1NullableString(p.Description),
			firstNonEmptyString(p.Status, "open"), p.CreatedTurn, p.ResolvedTurn, p.SourceTurn,
			p.Priority, d1NullableString(p.HookType),
			d1NullableString(firstNonEmptyString(p.HookMetadataJSON, p.DetailsJSON)),
			d1BoolValue(p.Pinned), d1BoolValue(p.Suppressed), d1BoolValue(p.UserCorrected),
			d1TimeValue(p.CreatedAt), d1TimeValue(p.UpdatedAt),
		},
	}}, nil
}

// d1StatusTransitionRestorePendingSnapshot is restoreRepairPendingSnapshotTx:
// the reverse half of the reversible contract.
//
// It first normalises the thread through the ordinary projection, then writes
// the EXACT observed before-snapshot over it. The difference is the point: a
// normal lifecycle projection preserves manual flags and origin metadata,
// whereas an explicit undo reinstates what was seen, including pinned,
// suppressed, user_corrected and created_at. That is why this is a full-column
// UPDATE and not the COALESCE update above.
func (s *d1Store) d1StatusTransitionRestorePendingSnapshot(ctx context.Context, sid string, snapshot *PendingThread) ([]D1Statement, error) {
	value, err := json.Marshal(map[string]any{"pending_thread": snapshot})
	if err != nil {
		return nil, err
	}
	statements, err := s.d1StatusTransitionProjectPendingThread(ctx, sid, string(value))
	if err != nil {
		return nil, err
	}
	return append(statements, D1Statement{
		SQL: `
		UPDATE pending_threads
		SET description = ?, status = ?, created_turn = ?, resolved_turn = NULLIF(?, 0),
		    source_turn = NULLIF(?, 0), priority = NULLIF(?, 0), hook_type = ?, hook_metadata_json = ?,
		    pinned = ?, suppressed = ?, user_corrected = ?, created_at = ?, updated_at = ?
		WHERE chat_session_id = ? AND thread_key = ?`,
		Args: []any{
			d1NullableString(snapshot.Description), firstNonEmptyString(snapshot.Status, "open"),
			snapshot.CreatedTurn, snapshot.ResolvedTurn, snapshot.SourceTurn, snapshot.Priority,
			d1NullableString(snapshot.HookType),
			d1NullableString(firstNonEmptyString(snapshot.HookMetadataJSON, snapshot.DetailsJSON)),
			d1BoolValue(snapshot.Pinned), d1BoolValue(snapshot.Suppressed), d1BoolValue(snapshot.UserCorrected),
			d1TimeValue(snapshot.CreatedAt), d1TimeValue(snapshot.UpdatedAt), sid, snapshot.ThreadKey,
		},
	}), nil
}

// ---------------------------------------------------------------------------
// explicit repair artifacts
// ---------------------------------------------------------------------------

// d1StatusTransitionApplyRepairArtifacts is applyStateRepairArtifactsTx without
// the transaction: the same per-row SELECT, the same rebase of the recorded
// delta onto the live row, and the same vector invalidation plus re-enqueue.
//
// Two details are load-bearing. The SELECT has to read the LIVE row because
// change.Before is rebuilt from it: the undo path in admin_state_repair replays
// the recorded before/after pair, and a before captured at plan time is only
// correct if nothing else touched the row in between. And
// bodyRepairApplyJSONDelta runs only for partial tables (character_states,
// active_states, status_current_values, ...), where a row holds unrelated fields
// and a whole-value overwrite would erase state the repair never mentioned.
func (s *d1Store) d1StatusTransitionApplyRepairArtifacts(ctx context.Context, sid, operation string, changes []StateRepairArtifactChange, now time.Time) ([]StateRepairArtifactChange, []D1Statement, error) {
	result := make([]StateRepairArtifactChange, 0, len(changes))
	statements := make([]D1Statement, 0, len(changes)+2)
	for _, change := range changes {
		spec, err := bodyRepairSpec(change.Table)
		if err != nil {
			return nil, nil, err
		}
		columns := bodyRepairColumns(spec)
		values := make([]*string, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		err = s.conn.QueryRow(ctx,
			`SELECT `+d1QuoteIdentList(columns)+` FROM `+d1QuoteIdent(spec.table)+
				` WHERE `+d1QuoteIdent(spec.sessionColumn)+` = ? AND id = ?`,
			sid, change.ID).Scan(dest...)
		if errors.Is(err, errD1NoRows) {
			// Existing source deletion owns removed rows.
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		plannedBefore := change.Before
		change.Before = map[string]*string{}
		set := make([]string, 0, len(columns))
		params := make([]any, 0, len(columns)+2)
		for i, key := range columns {
			if _, exists := change.After[key]; !exists {
				continue
			}
			if values[i] != nil {
				stored := *values[i]
				change.Before[key] = &stored
			} else {
				change.Before[key] = nil
			}
			value, exists := change.After[key]
			if !exists {
				continue
			}
			if bodyRepairPartialTable(spec.table) && values[i] != nil && plannedBefore[key] != nil && value != nil {
				merged := bodyRepairApplyJSONDelta(*values[i], *plannedBefore[key], *value)
				value = &merged
				change.After[key] = value
			}
			var arg any
			if value != nil {
				arg = *value
			}
			set = append(set, d1QuoteIdent(key)+" = ?")
			params = append(params, arg)
		}
		if len(set) == 0 {
			continue
		}
		params = append(params, sid, change.ID)
		statements = append(statements, D1Statement{
			SQL:  `UPDATE ` + d1QuoteIdent(spec.table) + ` SET ` + strings.Join(set, ",") + ` WHERE ` + d1QuoteIdent(spec.sessionColumn) + ` = ? AND id = ?`,
			Args: params,
		})
		if spec.vectorTier != "" {
			if len(change.VectorIDs) > 0 {
				ids, err := s.d1StatusTransitionVectorIDs(ctx, sid, spec, change.ID)
				if err != nil {
					return nil, nil, err
				}
				change.VectorIDs = ids
			}
			vectorStatements, err := s.d1StatusTransitionRepairArtifactVector(ctx, sid, operation, spec, change, now)
			if err != nil {
				return nil, nil, err
			}
			statements = append(statements, vectorStatements...)
		}
		result = append(result, change)
	}
	return result, statements, nil
}

// d1StatusTransitionVectorIDs is stateRepairVectorIDs over the D1 transport.
// precise_memory_units is addressed by its unit_id rather than its row id,
// because that is the identity the vector document was built from.
func (s *d1Store) d1StatusTransitionVectorIDs(ctx context.Context, sid string, spec bodyRepairTable, id int64) ([]string, error) {
	key := strconv.FormatInt(id, 10)
	if spec.table == "precise_memory_units" {
		if err := s.conn.QueryRow(ctx,
			`SELECT unit_id FROM precise_memory_units WHERE chat_session_id = ? AND id = ?`,
			sid, id).Scan(&key); err != nil {
			return nil, err
		}
	}
	return []string{spec.vectorTier + ":" + sid + ":" + key, spec.vectorTier + ":" + key}, nil
}

// d1StatusTransitionRepairArtifactVector is repairArtifactVectorTx: it retires
// the in-flight outbox work for a repaired document and enqueues exactly one
// replacement.
//
// The JOIN to memory_source_revisions is what decides required_source_state: a
// document whose source has been rolled back must be re-enqueued as an inactive
// delete, or the worker would re-embed content the user discarded. A document
// whose repair cleared every repairable field becomes a delete rather than an
// upsert, which is why `cleared` is computed from change.After against the
// table's clear values.
func (s *d1Store) d1StatusTransitionRepairArtifactVector(ctx context.Context, sid, operation string, spec bodyRepairTable, change StateRepairArtifactChange, now time.Time) ([]D1Statement, error) {
	ids, err := s.d1StatusTransitionVectorIDs(ctx, sid, spec, change.ID)
	if err != nil {
		return nil, err
	}
	documentID := ids[0]
	var revision, state, document string
	var ready bool
	err = s.conn.QueryRow(ctx, `
		SELECT o.source_revision, s.lifecycle_state, COALESCE(o.document_json, ''), o.embedding_ready
		FROM memory_vector_outbox o
		JOIN memory_source_revisions s
		  ON s.chat_session_id = o.chat_session_id AND s.source_revision = o.source_revision
		WHERE o.chat_session_id = ? AND o.document_id = ? AND o.operation = 'upsert'
		ORDER BY o.id DESC LIMIT 1`, sid, documentID).Scan(&revision, &state, &document, &ready)
	if errors.Is(err, errD1NoRows) {
		// No outbox work exists for this document, so there is nothing to
		// retire and nothing to re-index. The reference returns here too.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cleared := true
	for key, value := range change.After {
		if value != nil && *value != spec.clear[key] {
			cleared = false
		}
	}
	stamp := d1TimeValue(now)
	statements := []D1Statement{{
		SQL: `
		UPDATE memory_vector_outbox
		SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL, retry_after = NULL,
		    last_error = 'explicit_body_data_repair', updated_at = ?
		WHERE chat_session_id = ? AND document_id = ? AND status IN ('pending','retryable','needs_embedding','leased')`,
		Args: []any{stamp, sid, documentID},
	}}
	item := &MemoryVectorOutboxItem{
		OperationKey:        memoryVectorOperationKey("repair:"+operation, sid, revision, documentID),
		ChatSessionID:       sid,
		SourceRevision:      revision,
		DocumentID:          documentID,
		Status:              "pending",
		CreatedAt:           now,
		UpdatedAt:           now,
		RequiredSourceState: "active",
		EmbeddingReady:      ready,
	}
	if state != "active" {
		item.RequiredSourceState = "inactive"
	}
	if cleared {
		item.Operation = "delete"
		item.DocumentJSON = memoryVectorDeleteAuditJSON("explicit_body_data_repair")
		item.EmbeddingReady = true
	} else {
		item.Operation = "upsert"
		item.DocumentJSON = document
	}
	enqueue, include, err := s.d1StatusTransitionEnqueueVectorStatement(ctx, item)
	if err != nil {
		return nil, err
	}
	if include {
		statements = append(statements, enqueue)
	}
	return statements, nil
}

// d1StatusTransitionEnqueueVectorStatement turns the reference's insert-then-
// resolve-duplicate enqueue into a pre-check plus an optional statement.
//
// A D1 batch cannot report which statement it suppressed, so the duplicate
// resolution has to happen before the batch, exactly as the precise-memory
// writer's outbox path already does. The outcomes are unchanged: an identical
// existing row means the work is already queued and no statement is added, and
// a different document under the same operation key is an idempotency conflict
// rather than a silent overwrite.
func (s *d1Store) d1StatusTransitionEnqueueVectorStatement(ctx context.Context, item *MemoryVectorOutboxItem) (D1Statement, bool, error) {
	statement, err := d1EnqueueMemoryVectorStatement(item)
	if err != nil {
		return D1Statement{}, false, err
	}
	var operation, chatSessionID, revision, documentID, documentJSON, requiredSourceState string
	var embeddingReady bool
	err = s.conn.QueryRow(ctx, `
		SELECT operation, chat_session_id, source_revision, document_id,
		       COALESCE(document_json, ''), embedding_ready, required_source_state
		FROM memory_vector_outbox
		WHERE operation_key = ?`, item.OperationKey).Scan(
		&operation, &chatSessionID, &revision, &documentID, &documentJSON,
		&embeddingReady, &requiredSourceState)
	if err == nil {
		if operation != item.Operation || chatSessionID != item.ChatSessionID ||
			revision != item.SourceRevision || documentID != item.DocumentID ||
			(item.Operation == "upsert" && strings.TrimSpace(documentJSON) != strings.TrimSpace(item.DocumentJSON)) ||
			embeddingReady != item.EmbeddingReady || requiredSourceState != item.RequiredSourceState {
			return D1Statement{}, false, errors.New("memory vector operation idempotency conflict")
		}
		return D1Statement{}, false, nil
	}
	if !errors.Is(err, errD1NoRows) {
		return D1Statement{}, false, err
	}
	return statement, true, nil
}

// ---------------------------------------------------------------------------
// SupersessionResolutionStore
// ---------------------------------------------------------------------------

// SaveSupersessionResolution records the decision in audit_logs and applies the
// matching target-state write, as one atomic batch.
//
// The audit row is FIRST so a reader that finds the decision can always find
// the state change; the batch is what makes that ordering a guarantee instead
// of a hope. MariaDB got the same guarantee from one transaction, and the audit
// id there came from LastInsertId ??here it is read back after the commit by
// the audit row's own natural key (created_at + event type + target), newest
// first, which is the same "the write landed, the handle did not" shape
// LastInsertId failure has on the reference path.
func (s *d1Store) SaveSupersessionResolution(ctx context.Context, d *SupersessionResolutionDecision) (*SupersessionResolutionRecord, error) {
	decision, err := normalizeSupersessionResolutionDecision(d)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	detailsJSON := mariaSupersessionResolutionDetailsJSON(decision)
	summary := mariaSupersessionResolutionSummary(decision)
	source := firstNonEmptyString(decision.Operator, "critic")
	stamp := d1TimeValue(now)

	statements := []D1Statement{{
		SQL: `
		INSERT INTO audit_logs (created_at, event_type, chat_session_id, target_type, target_id, summary, details_json, source)
		VALUES (?, 'supersession_resolution', ?, ?, ?, ?, ?, ?)`,
		Args: []any{
			stamp, decision.ChatSessionID, decision.TargetType, decision.TargetID,
			summary, detailsJSON, source,
		},
	}}
	stateStatements, err := s.d1SupersessionResolutionStateStatements(decision, now)
	if err != nil {
		return nil, err
	}
	statements = append(statements, stateStatements...)

	if err := s.conn.Batch(ctx, statements...); err != nil {
		return nil, err
	}

	record := &SupersessionResolutionRecord{
		CreatedAt:       now,
		ChatSessionID:   decision.ChatSessionID,
		TargetType:      decision.TargetType,
		TargetID:        decision.TargetID,
		SourceTurn:      decision.SourceTurn,
		ResolutionClass: decision.ResolutionClass,
		NewTargetType:   decision.NewTargetType,
		NewTargetID:     decision.NewTargetID,
		RelationshipKey: decision.RelationshipKey,
		Reason:          decision.Reason,
		DetailsJSON:     detailsJSON,
		Source:          source,
	}
	var id int64
	if err := s.conn.QueryRow(ctx, `
		SELECT id FROM audit_logs
		WHERE created_at = ? AND event_type = 'supersession_resolution'
		  AND chat_session_id = ? AND target_type = ? AND target_id = ?
		ORDER BY id DESC LIMIT 1`,
		stamp, decision.ChatSessionID, decision.TargetType, decision.TargetID).Scan(&id); err == nil {
		record.ID = id
	}
	return record, nil
}

// d1SupersessionResolutionStateStatements is mariaApplySupersessionResolutionState.
// An unknown target type contributes no statement, which is the reference's
// `default: return nil` ??a resolution is still recorded in the audit trail
// even when the target is a table this provider does not mutate.
func (s *d1Store) d1SupersessionResolutionStateStatements(d SupersessionResolutionDecision, now time.Time) ([]D1Statement, error) {
	switch d.TargetType {
	case "direct_evidence":
		return d1SupersessionDirectEvidenceState(d), nil
	case "kg_triple":
		return d1SupersessionKGTripleState(d), nil
	case "pending_thread":
		return d1SupersessionPendingThreadState(d, now), nil
	default:
		return nil, nil
	}
}

// d1SupersessionDirectEvidenceState closes or supersedes an evidence record.
//
// repair_needed is cleared on every branch: a record the critic has already
// adjudicated is no longer awaiting repair, and leaving the flag set would keep
// it in the repair queue forever. superseded_by_id is bound as NULL when the
// decision names no replacement, so "superseded with no successor" stays
// distinguishable from "never superseded".
func d1SupersessionDirectEvidenceState(d SupersessionResolutionDecision) []D1Statement {
	switch d.ResolutionClass {
	case "close":
		return []D1Statement{{
			SQL: `
			UPDATE direct_evidence_records
			SET archive_state = ?, capture_verification = ?, committed_gate = ?, repair_needed = 0
			WHERE id = ? AND chat_session_id = ?`,
			Args: []any{"closed_archive", "closed", "closed_by_resolution", d.TargetID, d.ChatSessionID},
		}}
	case "supersede", "refine", "reverse":
		var supersededBy any
		if d.NewTargetID > 0 {
			supersededBy = d.NewTargetID
		}
		return []D1Statement{{
			SQL: `
			UPDATE direct_evidence_records
			SET archive_state = ?, capture_verification = ?, committed_gate = ?, repair_needed = 0,
			    superseded_by_id = ?
			WHERE id = ? AND chat_session_id = ?`,
			Args: []any{
				"superseded_archive", d.ResolutionClass, d.ResolutionClass + "_by_resolution",
				supersededBy, d.TargetID, d.ChatSessionID,
			},
		}}
	default:
		// soft_demote and stale_demote deliberately leave the record readable;
		// they lower foreground priority without deleting source history.
		return nil
	}
}

// d1SupersessionKGTripleState closes the validity interval of a triple.
//
// The guard `valid_to IS NULL OR valid_to = 0 OR valid_to > ?` is what makes
// the write safe to repeat: a triple already closed at or before this turn is
// left alone, so re-resolving the same relationship cannot drag its end date
// backwards.
func d1SupersessionKGTripleState(d SupersessionResolutionDecision) []D1Statement {
	if d.SourceTurn <= 0 {
		return nil
	}
	switch d.ResolutionClass {
	case "close", "supersede", "refine", "reverse", "stale_demote":
		return []D1Statement{{
			SQL: `
			UPDATE kg_triples
			SET valid_to = ?
			WHERE id = ? AND chat_session_id = ? AND (valid_to IS NULL OR valid_to = 0 OR valid_to > ?)`,
			Args: []any{d.SourceTurn, d.TargetID, d.ChatSessionID, d.SourceTurn},
		}}
	default:
		return nil
	}
}

// d1SupersessionPendingThreadState resolves a pending thread at the decision's
// turn. The status is a literal, not a caller value: a resolution always ends
// the thread, and a class that means "keep it in the background" is handled by
// leaving the row untouched above.
func d1SupersessionPendingThreadState(d SupersessionResolutionDecision, now time.Time) []D1Statement {
	switch d.ResolutionClass {
	case "close", "supersede", "refine", "reverse":
		return []D1Statement{{
			SQL: `
			UPDATE pending_threads
			SET status = ?, resolved_turn = ?, updated_at = ?
			WHERE id = ? AND chat_session_id = ?`,
			Args: []any{"resolved", d.SourceTurn, d1TimeValue(now), d.TargetID, d.ChatSessionID},
		}}
	default:
		return nil
	}
}

// ListSupersessionResolutions reads the resolution trail for a session.
//
// It is the same audit-log read on both providers, with the same recency
// ordering (created_at DESC, id DESC) and the same limit semantics: a
// non-positive limit means no LIMIT clause at all, i.e. the complete trail. The
// projection back out of details_json is the shared helper, because the record
// type is a VIEW over the audit row, not a second table.
func (s *d1Store) ListSupersessionResolutions(ctx context.Context, chatSessionID string, limit int) ([]SupersessionResolutionRecord, error) {
	logs, err := s.ListAuditLogs(ctx, chatSessionID, "supersession_resolution", limit)
	if err != nil {
		return nil, err
	}
	out := make([]SupersessionResolutionRecord, 0, len(logs))
	for _, item := range logs {
		out = append(out, mariaSupersessionResolutionRecordFromAudit(item))
	}
	return out, nil
}
