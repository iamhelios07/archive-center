package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// D1 logical-turn replacement and session-stitch capability.
//
// Two capabilities share this file because both are whole-narrative writes
// rather than row writes: each one decides, in one atomic step, that a stretch
// of stored story is no longer the story, and then makes every derived row
// agree.
//
//   - LogicalTurnReplacementStore is the rollback-adjacent path.
//     ReplaceLogicalTurn swaps one canonical turn for a regenerated one and
//     RollbackCanonicalTail drops the tail outright. Both must invalidate, not
//     merely delete.
//   - SessionStitchStore joins two sessions into one narrative by copying every
//     manifest table under renumbered turn coordinates.
//
// ---------------------------------------------------------------------------
// What the transaction boundary became
// ---------------------------------------------------------------------------
//
// The MariaDB methods run inside a *sql.Tx and take SELECT ... FOR UPDATE on the
// canonical tail and on every source revision they are about to invalidate.
// D1 has no row locks and no readable transaction: D1Conn.Batch is the atomic
// boundary, it returns only an error, and a batch cannot be read from the
// middle of one. So each method is split exactly where the reference stops
// reading and starts writing:
//
//	phase 1 (reads, before the batch)  -> the FOR UPDATE probes
//	phase 2 (one D1Conn.Batch)         -> every INSERT/UPDATE/DELETE
//
// The reads that had to move are named at their call sites. What is lost is
// serialisation BETWEEN the phases, which is the same unresolved D1 locking
// boundary every other capability in this provider records: the in-process half
// is closed by memoryDerivationWriteMu, and a cross-Container race between the
// probe and the batch is not. The batch atomicity plus the natural keys mean
// the loser conflicts rather than duplicates, so the failure is loud.
//
// ---------------------------------------------------------------------------
// Why the invalidation set is the point, not the deletion
// ---------------------------------------------------------------------------
//
// A derived row that SURVIVES a replacement is the failure this capability
// exists to prevent. memories, direct_evidence_records, kg_triples, entities,
// status_change_events and the rest are projection rows whose provenance points
// at a canonical turn. If the turn is replaced and the projection is merely left
// in place, the next recall serves text the user explicitly threw away,
// attributed to a turn that no longer says that. So every statement in
// d1LogicalTurnCanonicalTailCommands is a DELETE or a RESET that reaches the
// replaced turn, and the tests assert the surviving row set rather than the
// fact that a replacement happened.

var _ LogicalTurnReplacementStore = (*d1Store)(nil)

// ---------------------------------------------------------------------------
// error classification
// ---------------------------------------------------------------------------

// d1LogicalTurnError builds the typed error the HTTP layer decodes into a
// response code. It is the reference's typedLogicalTurnReplacementError, so the
// Code, Stage, Retryable and CommitState contract is identical.
func d1LogicalTurnError(code, stage string, retryable bool, commitState string, cause error) error {
	return &LogicalTurnReplacementError{
		Code:        code,
		Stage:       stage,
		Retryable:   retryable,
		CommitState: commitState,
		Cause:       cause,
	}
}

// d1LogicalTurnClassifyError is classifyLogicalTurnReplacementStoreError for a
// transport that has no MySQL error numbers.
//
// The reference reads numeric codes off *mysql.MySQLError. D1 returns SQLite
// text through the Worker bridge, so there is no number to switch on. The two
// constraint-shaped codes are recovered from the engine's own stable message,
// which is the same text production sees because the bridge forwards it
// verbatim, and the same text the local SQLite harness produces, so the tests
// exercise the real classification. The permission code has no counterpart at
// all: D1 authorizes the binding rather than a per-statement grant, and matching
// a fabricated error string to invent one would be guessing at a contract this
// provider does not have. That code is therefore simply unreachable here.
func d1LogicalTurnClassifyError(err error, stage string, commitAttempted bool) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotEnabled) {
		return d1LogicalTurnError("logical_turn_store_unavailable", stage, false, "not_committed", err)
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "UNIQUE constraint failed"):
		// MySQL 1062: a natural key this write owns already exists.
		return d1LogicalTurnError("logical_turn_revision_conflict", stage, false, "not_committed", err)
	case strings.Contains(text, "FOREIGN KEY constraint failed"):
		// MySQL 1451/1452: a projection still points at a row the batch removed.
		return d1LogicalTurnError("logical_turn_constraint_conflict", stage, false, "not_committed", err)
	}
	if commitAttempted {
		return d1LogicalTurnError("logical_turn_commit_outcome_unknown", stage, false, "unknown", err)
	}
	return d1LogicalTurnError("logical_turn_transaction_failed", stage, true, "not_committed", err)
}

// ---------------------------------------------------------------------------
// ReplaceLogicalTurn
// ---------------------------------------------------------------------------

// ReplaceLogicalTurn is intentionally limited to the canonical tail. It also
// accepts the immediately missing tail (latest == requested-1): RisuAI can
// delete the old assistant turn before committing its regenerated result. In
// that case the replacement must recreate the same logical turn, not fail or
// allocate a new turn. Historical turns and gaps remain rejected.
func (s *d1Store) ReplaceLogicalTurn(ctx context.Context, replacement LogicalTurnReplacement) error {
	if s == nil || s.conn == nil {
		return d1LogicalTurnError("logical_turn_store_unavailable", "preflight", false, "not_committed", ErrNotEnabled)
	}
	sid := strings.TrimSpace(replacement.ChatSessionID)
	if sid == "" || replacement.TurnIndex <= 0 || strings.TrimSpace(replacement.UserContent) == "" || strings.TrimSpace(replacement.AssistantContent) == "" {
		return d1LogicalTurnError("logical_turn_request_invalid", "preflight", false, "not_committed",
			fmt.Errorf("invalid logical turn replacement"))
	}
	if replacement.SourceRevision != nil {
		source := replacement.SourceRevision
		if source.ChatSessionID != sid || source.TurnIndex != replacement.TurnIndex ||
			source.UserContent != replacement.UserContent ||
			source.AssistantContent != replacement.AssistantContent {
			return d1LogicalTurnError("logical_turn_revision_conflict", "source_revision", false, "not_committed",
				fmt.Errorf("logical turn replacement source revision mismatch"))
		}
		if err := validateMemorySourceRevision(source); err != nil {
			return d1LogicalTurnError("logical_turn_revision_invalid", "source_revision", false, "not_committed", err)
		}
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	latestTurn, err := s.d1LogicalTurnCanonicalTail(ctx, sid)
	if err != nil {
		return d1LogicalTurnClassifyError(err, "canonical_tail_read", false)
	}
	if latestTurn != replacement.TurnIndex && latestTurn != replacement.TurnIndex-1 {
		return d1LogicalTurnError("logical_turn_not_current_tail", "canonical_tail_check", false, "not_committed",
			fmt.Errorf("logical turn replacement requires current canonical tail: latest=%d requested=%d", latestTurn, replacement.TurnIndex))
	}

	createdAt := d1TimeValue(replacement.CreatedAt)
	statements := make([]D1Statement, 0, 48)
	if replacement.SourceRevision != nil {
		successor, err := s.d1LogicalTurnSuccessorStatements(ctx, sid, replacement.TurnIndex, replacement.SourceRevision, createdAt)
		if err != nil {
			return d1LogicalTurnClassifyError(err, "source_revision_register", false)
		}
		statements = append(statements, successor...)
	}
	statements = append(statements, d1LogicalTurnCanonicalTailCommands(sid, replacement.TurnIndex, replacement.SourceRevision == nil, false)...)
	restore, err := s.d1LogicalTurnStatusCurrentRestore(ctx, sid)
	if err != nil {
		return d1LogicalTurnClassifyError(err, "status_current_restore", false)
	}
	statements = append(statements, restore...)
	// The two inserts are LAST on purpose. The tail sweep deletes
	// chat_logs WHERE turn_index = t, so an insert placed before it would be
	// deleted by its own replacement; an insert placed before the sweeps of the
	// derived tables would leave those tables describing the discarded turn.
	statements = append(statements,
		D1Statement{SQL: `INSERT INTO chat_logs (chat_session_id, turn_index, role, content, created_at) VALUES (?, ?, 'user', ?, ?)`,
			Args: []any{sid, replacement.TurnIndex, replacement.UserContent, createdAt}},
		D1Statement{SQL: `INSERT INTO chat_logs (chat_session_id, turn_index, role, content, created_at) VALUES (?, ?, 'assistant', ?, ?)`,
			Args: []any{sid, replacement.TurnIndex, replacement.AssistantContent, createdAt}},
	)
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return d1LogicalTurnClassifyError(err, "canonical_replace", true)
	}
	if replacement.SourceRevision != nil {
		// LastInsertId has no D1 equivalent, so the assigned id is read back from
		// the committed row by the same natural key the insert bound. A failed
		// readback leaves ID zero, which is the outcome the reference produces
		// when LastInsertId itself fails: the write landed, the handle did not.
		var id int64
		if err := s.conn.QueryRow(ctx, `SELECT id FROM memory_source_revisions WHERE chat_session_id = ? AND source_revision = ?`,
			sid, replacement.SourceRevision.SourceRevision).Scan(&id); err == nil {
			replacement.SourceRevision.ID = id
		}
	}
	return nil
}

// d1LogicalTurnCanonicalTail reads the newest canonical turn. The reference
// appends FOR UPDATE here; D1 has no row lock, so the statement is the same
// read without the lock and the atomicity comes from the batch that follows.
//
// ORDER BY turn_index DESC, id DESC is load-bearing, not decoration: the tail
// must be the row the engine considers last at the highest turn, and the id
// tiebreak is what decides that when a turn has more than one row.
func (s *d1Store) d1LogicalTurnCanonicalTail(ctx context.Context, sid string) (int, error) {
	var turn int
	err := s.conn.QueryRow(ctx, `SELECT turn_index FROM chat_logs WHERE chat_session_id = ? ORDER BY turn_index DESC, id DESC LIMIT 1`, sid).Scan(&turn)
	if errors.Is(err, errD1NoRows) {
		// An empty session has tail 0, which is the reference's sql.NullInt64
		// with Valid=false and which makes turn 1 the recreatable first turn.
		return 0, nil
	}
	return turn, err
}

// d1LogicalTurnSuccessorStatements builds the whole source-revision block: the
// invalidation of the turn's prior accepted source, the binding of every
// retained prior revision to the accepted successor, and the successor's own
// insert.
//
// The successor is excluded from its own invalidation. On a first call it does
// not exist yet, so this matches the reference exactly. On a replay it is the
// whole point: without the exclusion the replay would supersede the very
// revision it is re-registering and then fail to insert it again, so a retried
// host request could never converge. The insert is skipped on that same replay,
// which is what makes the replacement idempotent by its natural key
// (chat_session_id, source_revision).
func (s *d1Store) d1LogicalTurnSuccessorStatements(
	ctx context.Context,
	sid string,
	turn int,
	source *MemorySourceRevision,
	createdAt string,
) ([]D1Statement, error) {
	replay, err := s.d1LogicalTurnSourceRevisionFence(ctx, sid, source)
	if err != nil {
		return nil, err
	}
	revisions, err := s.d1LogicalTurnActiveRevisions(ctx, sid, turn, source.SourceRevision)
	if err != nil {
		return nil, err
	}
	deletes, err := s.d1KnownVectorDeletes(ctx, sid, revisions, turn)
	if err != nil {
		return nil, err
	}
	statements := d1LogicalTurnVectorDeleteStatements(sid, deletes, "logical_turn_replaced", createdAt)
	statements = append(statements, d1LogicalTurnInvalidationStatements(sid, revisions, "superseded", "logical_turn_replaced", createdAt, source.SourceRevision)...)
	// A host-observed replacement may have invalidated the old source before the
	// new accepted final arrived. Bind every retained non-deleted prior revision
	// at this logical turn to the accepted successor now.
	//
	// COALESCE(invalidated_at, ?) is kept and is not a convenience: a prior
	// revision that was already invalidated must keep the moment its text was
	// discarded, because createdAt here is when the SUCCESSOR was observed, not
	// when the prior text stopped being canonical.
	statements = append(statements, D1Statement{
		SQL: `
		UPDATE memory_source_revisions
		SET lifecycle_state = 'superseded', superseded_by_revision = ?,
		    invalidation_reason = 'logical_turn_replaced',
		    invalidated_at = COALESCE(invalidated_at, ?), updated_at = ?
		WHERE chat_session_id = ? AND turn_index = ?
		  AND source_revision <> ? AND lifecycle_state <> 'deleted'`,
		Args: []any{source.SourceRevision, createdAt, createdAt, sid, turn, source.SourceRevision},
	})
	if replay {
		return statements, nil
	}
	return append(statements, d1LogicalTurnSourceRevisionInsert(source, createdAt)), nil
}

// d1LogicalTurnSourceRevisionFence reports whether the successor revision is
// already stored as the live acceptance for this turn, and refuses when it is
// stored as anything else.
//
// This is the source-revision fence for the replacement. The write carries a
// source-derived projection: the claim that the turn's canonical text is the
// text that revision accepted. A stored row under the same natural key that is
// not this exact acceptance is a collision and is refused, never silently
// overwritten, because last-writer-wins here would erase one of the two
// observations of what the turn said. A stored row that has since been
// superseded or deleted is stale in the other direction: replaying it would
// resurrect what the user discarded.
func (s *d1Store) d1LogicalTurnSourceRevisionFence(ctx context.Context, sid string, source *MemorySourceRevision) (bool, error) {
	var lifecycle, turn, logicalTurnID, user, assistant, hash string
	err := s.conn.QueryRow(ctx, `
		SELECT lifecycle_state, CAST(turn_index AS TEXT), logical_turn_id,
		       raw_user_content, raw_assistant_content, combined_content_hash
		FROM memory_source_revisions
		WHERE chat_session_id = ? AND source_revision = ?
	`, sid, source.SourceRevision).Scan(&lifecycle, &turn, &logicalTurnID, &user, &assistant, &hash)
	if errors.Is(err, errD1NoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	identical := turn == fmt.Sprintf("%d", source.TurnIndex) &&
		logicalTurnID == source.LogicalTurnID &&
		user == source.UserContent &&
		assistant == source.AssistantContent &&
		hash == source.CombinedContentHash
	if !identical {
		return false, fmt.Errorf("logical turn source revision %q already records a different acceptance", source.SourceRevision)
	}
	if !strings.EqualFold(lifecycle, "active") {
		return false, fmt.Errorf("logical turn source revision %q is no longer active", source.SourceRevision)
	}
	return true, nil
}

// d1LogicalTurnActiveRevisions is invalidateMemorySourcesTx's FOR UPDATE probe:
// the active revisions at exactly this turn, in the reference's (turn_index, id)
// order, minus the successor being registered. Pass an empty keep for a
// rollback, where there is no successor and the clause is omitted rather than
// compared against the empty string, so no stored revision can be excluded by
// accident.
func (s *d1Store) d1LogicalTurnActiveRevisions(ctx context.Context, sid string, turn int, keep string) ([]string, error) {
	query := `
		SELECT source_revision
		FROM memory_source_revisions
		WHERE chat_session_id = ? AND turn_index = ? AND lifecycle_state = 'active'`
	args := []any{sid, turn}
	if keep != "" {
		query += ` AND source_revision <> ?`
		args = append(args, keep)
	}
	query += ` ORDER BY turn_index, id`
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var revisions []string
	for rows.Next() {
		var revision string
		if err := rows.Scan(&revision); err != nil {
			return nil, err
		}
		revisions = append(revisions, revision)
	}
	return revisions, rows.Err()
}

// d1LogicalTurnRetirableRevisions is the probe for a DELETED rollback. A delete
// retires revisions a previous pass already invalidated, which the active-only
// probe cannot see; including only active rows would leave a deleted tail with
// live-looking history behind it.
func (s *d1Store) d1LogicalTurnRetirableRevisions(ctx context.Context, sid string, turn int) ([]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT source_revision FROM memory_source_revisions
		WHERE chat_session_id = ? AND turn_index = ? AND lifecycle_state <> 'deleted'
		ORDER BY turn_index, id
	`, sid, turn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var revisions []string
	for rows.Next() {
		var revision string
		if err := rows.Scan(&revision); err != nil {
			return nil, err
		}
		revisions = append(revisions, revision)
	}
	return revisions, rows.Err()
}

// d1LogicalTurnVectorDeleteStatements builds the durable vector-delete receipts.
//
// The operation key is a SHA-256 over (operation, session, revision, document)
// and SQLite has no SHA-256, so it is bound from Go exactly as the reference
// computes it. ON CONFLICT DO NOTHING is what makes a replay of the same
// invalidation idempotent: the same document invalidated twice must leave one
// pending delete, not two racing ones.
func d1LogicalTurnVectorDeleteStatements(sid string, deletes []d1VectorDelete, reason, now string) []D1Statement {
	statements := make([]D1Statement, 0, len(deletes))
	for _, delete := range deletes {
		statements = append(statements, D1Statement{
			SQL: `
			INSERT INTO memory_vector_outbox (
				contract_version, operation_key, operation, chat_session_id,
				source_revision, document_id, document_json, embedding_ready,
				required_source_state, status, attempts, created_at, updated_at
			) VALUES (?, ?, 'delete', ?, ?, ?, ?, 1, 'inactive', 'pending', 0, ?, ?)
			ON CONFLICT (operation_key) DO NOTHING`,
			Args: []any{
				MemoryVectorOutboxContract,
				memoryVectorOperationKey("delete:inactive", sid, delete.sourceRevision, delete.documentID),
				sid, delete.sourceRevision, delete.documentID,
				d1NullableString(memoryVectorDeleteAuditJSON(reason)),
				now, now,
			},
		})
	}
	return statements
}

// d1LogicalTurnInvalidationStatements is invalidateMemorySourcesTx's write
// half, statement for statement.
//
// The CASE WHEN ? = 'deleted' shapes are kept verbatim, including the bound
// parameter, because they are the difference between "this source is no longer
// the live text of the turn" and "this source is gone forever". A deleted
// revision must surrender its raw content, its Critic extraction and its
// evidence links so nothing downstream can mistake them for live data, while an
// invalidated or superseded one keeps them for the reversible path. The
// lifecycle_state filter on precise_memory_units is dropped for the same reason:
// a delete reaches every unit the revision ever produced, not only the live one.
func d1LogicalTurnInvalidationStatements(
	sid string,
	revisions []string,
	lifecycleState string,
	reason string,
	now string,
	supersededBy string,
) []D1Statement {
	preciseStatePredicate := ` AND lifecycle_state = 'active'`
	if lifecycleState == "deleted" {
		preciseStatePredicate = ""
	}
	statements := make([]D1Statement, 0, len(revisions)*6)
	for _, revision := range revisions {
		statements = append(statements,
			D1Statement{SQL: `
				UPDATE memory_derivation_dependencies
				SET lifecycle_state = 'invalidated', invalidated_at = ?, updated_at = ?
				WHERE chat_session_id = ? AND source_revision = ? AND lifecycle_state = 'active'`,
				Args: []any{now, now, sid, revision}},
			D1Statement{SQL: `
				UPDATE precise_memory_units
				SET lifecycle_state = 'invalidated',
				    evidence_excerpt = CASE WHEN ? = 'deleted' THEN '' ELSE evidence_excerpt END,
				    direct_evidence_ids_json = CASE WHEN ? = 'deleted' THEN json_array() ELSE direct_evidence_ids_json END,
				    payload_json = CASE WHEN ? = 'deleted' THEN json_object() ELSE payload_json END,
				    relationship_key = CASE WHEN ? = 'deleted' THEN NULL ELSE relationship_key END,
				    reveal_condition = CASE WHEN ? = 'deleted' THEN NULL ELSE reveal_condition END,
				    updated_at = ?
				WHERE chat_session_id = ? AND source_revision = ?` + preciseStatePredicate,
				Args: []any{lifecycleState, lifecycleState, lifecycleState, lifecycleState,
					lifecycleState, now, sid, revision}},
			D1Statement{SQL: `
				UPDATE memory_reprocessing_jobs
				SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL,
				    last_error = ?, updated_at = ?
				WHERE chat_session_id = ? AND source_revision = ?
				  AND status IN ('pending', 'leased', 'retryable')`,
				Args: []any{reason, now, sid, revision}},
			D1Statement{SQL: `
				UPDATE memory_vector_outbox
				SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL,
				    document_json = CASE WHEN ? = 'deleted' THEN NULL ELSE document_json END,
				    last_error = ?, updated_at = ?
				WHERE chat_session_id = ? AND source_revision = ? AND operation = 'upsert'
				  AND status IN ('pending', 'leased', 'retryable', 'needs_embedding')`,
				Args: []any{lifecycleState, reason, now, sid, revision}},
		)
		if lifecycleState == "deleted" {
			statements = append(statements, D1Statement{SQL: `
				UPDATE memory_vector_outbox
				SET document_json = NULL, updated_at = ?
				WHERE chat_session_id = ? AND source_revision = ? AND operation = 'upsert'`,
				Args: []any{now, sid, revision}})
		}
		statements = append(statements, D1Statement{SQL: `
			UPDATE memory_source_revisions
			SET lifecycle_state = ?, superseded_by_revision = ?,
			    invalidation_reason = ?, invalidated_at = ?, updated_at = ?
			    , raw_user_content = CASE WHEN ? = 'deleted' THEN '' ELSE raw_user_content END
			    , raw_assistant_content = CASE WHEN ? = 'deleted' THEN '' ELSE raw_assistant_content END
			    , source_message_id = CASE WHEN ? = 'deleted' THEN NULL ELSE source_message_id END
			    , source_generation_id = CASE WHEN ? = 'deleted' THEN NULL ELSE source_generation_id END
			    , derived_result_json = CASE WHEN ? = 'deleted' THEN NULL ELSE derived_result_json END
			    , critic_input_snapshot_json = CASE WHEN ? = 'deleted' THEN NULL ELSE critic_input_snapshot_json END
			    , critic_input_snapshot_hash = CASE WHEN ? = 'deleted' THEN NULL ELSE critic_input_snapshot_hash END
			WHERE chat_session_id = ? AND source_revision = ?`,
			Args: []any{lifecycleState, d1NullableString(supersededBy), d1NullableString(reason),
				now, now, lifecycleState, lifecycleState, lifecycleState,
				lifecycleState, lifecycleState, lifecycleState, lifecycleState,
				sid, revision}})
	}
	return statements
}

// d1LogicalTurnSourceRevisionInsert is insertMemorySourceRevisionTx without
// LastInsertId; the assigned id is read back after the batch.
//
// The defaults are the reference's validateMemorySourceRevision defaults.
// derived_admission_state defaults to pending, which is what makes the
// replacement's new source visible to the admission lane as work still to be
// done rather than as an already-projected turn.
func d1LogicalTurnSourceRevisionInsert(source *MemorySourceRevision, createdAt string) D1Statement {
	updatedAt := createdAt
	if !source.UpdatedAt.IsZero() {
		updatedAt = d1TimeValue(source.UpdatedAt)
	}
	return D1Statement{
		SQL: `
		INSERT INTO memory_source_revisions (
			contract_version, source_revision, chat_session_id, logical_turn_id,
			turn_index, source_message_id, source_generation_id, branch_id,
			branch_state, raw_user_content, raw_assistant_content,
			combined_content_hash, user_observed_content_hash,
			assistant_observed_content_hash, hash_algorithm, host_observed_at_ms,
			lifecycle_state, superseded_by_revision, invalidation_reason,
			derived_admission_state, derived_admission_version,
			derived_extractor_version, derived_index_version,
			derived_result_hash, derived_result_json, derived_admitted_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		Args: []any{
			firstNonEmptyString(source.ContractVersion, MemorySourceRevisionContract),
			source.SourceRevision, source.ChatSessionID, source.LogicalTurnID, source.TurnIndex,
			d1NullableString(source.SourceMessageID), d1NullableString(source.SourceGenerationID),
			d1NullableString(source.BranchID),
			firstNonEmptyString(source.BranchState, "not_exposed"),
			source.UserContent, source.AssistantContent, source.CombinedContentHash,
			d1NullableString(source.UserObservedContentHash),
			d1NullableString(source.AssistantObservedContentHash), source.HashAlgorithm,
			source.HostObservedAtMS, firstNonEmptyString(source.LifecycleState, "active"),
			d1NullableString(source.SupersededByRevision), d1NullableString(source.InvalidationReason),
			firstNonEmptyString(source.DerivedAdmissionState, "pending"),
			source.DerivedAdmissionVersion, source.DerivedExtractorVersion,
			source.DerivedIndexVersion, d1NullableString(source.DerivedResultHash),
			d1NullableString(source.DerivedResultJSON), d1NullableTime(source.DerivedAdmittedAt),
			createdAt, updatedAt,
		},
	}
}

// ---------------------------------------------------------------------------
// the canonical tail deletion set
// ---------------------------------------------------------------------------

// d1LogicalTailCommand is one statement of the canonical tail sweep: a query
// plus its bound arguments, in the reference's execution order. It exists so a
// test can assert the sweep's shape without reaching into the builder that
// produces it.
type d1LogicalTailCommand struct {
	query string
	args  []any
}

// d1LogicalTurnPreserveManualEditsSQL is preserveLegacyCharacterManualEditsSQL.
//
// JSON_CONTAINS_PATH(x, 'one', '$.manual_overrides') becomes
// json_type(x, '$.manual_overrides') IS NOT NULL, which is the same test: it
// counts a path that exists even when its value is an explicit JSON null. The
// simpler json_extract(...) IS NOT NULL would answer false for
// {"manual_overrides": null} and silently drop an operator's saved voice
// override on the very replacement that was meant to preserve it.
//
// The two NOT EXISTS clauses are the "only the newest state, and only if no
// override was ever recorded" rule. Without the second one the insert would
// re-record the same override on every later replacement, and a reader that
// takes the latest manual_character_override would keep replaying an edit the
// operator already superseded by editing the state directly.
func d1LogicalTurnPreserveManualEditsSQL() string {
	return `
	INSERT INTO character_events
		(chat_session_id, character_name, turn_index, event_type, details_json, created_at)
	SELECT state.chat_session_id, state.character_name, 0, 'manual_character_override',
		json_object('source', 'manual_patch', 'recorded_turn', state.turn_index,
		'edits', json_array(json_object('path', json_array('speech_style', 'manual_overrides'),
		'value', json_extract(state.speech_style_json, '$."manual_overrides"')))), ` + d1NowExpression + `
	FROM character_states state WHERE state.chat_session_id = ? AND state.turn_index >= ?
		AND json_type(state.speech_style_json, '$."manual_overrides"') IS NOT NULL
		AND NOT EXISTS (SELECT 1 FROM character_states newer WHERE newer.chat_session_id = state.chat_session_id
			AND newer.character_name = state.character_name AND (newer.turn_index > state.turn_index OR (newer.turn_index = state.turn_index AND newer.id > state.id)))
		AND NOT EXISTS (SELECT 1 FROM character_events edits WHERE edits.chat_session_id = state.chat_session_id
			AND edits.character_name = state.character_name AND edits.event_type = 'manual_character_override')`
}

// d1LogicalTurnCanonicalTailCommands is canonicalTailDeleteCommands on SQLite.
//
// Three dialect moves are load-bearing:
//
//   - GREATEST(a, b) is MAX(a, b) with two arguments. SQLite has no GREATEST,
//     and its many-argument MAX would pick the NULL if either side were NULL,
//     blanking a last_seen_turn instead of clamping it.
//   - CURRENT_TIMESTAMP(3) is d1NowExpression, the fixed-millisecond TEXT form
//     the schema defaults use, so a touched row still sorts with rows that were
//     written by a default.
//   - the status_change_events sweep uses the shared d1JSONStringBlankPredicate
//     rather than a second hand-written NULLIF(TRIM(...)) form. That predicate
//     is what decides whether an event names a source revision at all: an event
//     that does is deleted with the turn, and one that does not is kept because
//     it is repair history with no turn left to point at.
func d1LogicalTurnCanonicalTailCommands(sid string, t int, legacyPhysicalCleanup, deleteTailChat bool) []D1Statement {
	commands := []d1LogicalTailCommand{
		{d1LogicalTurnPreserveManualEditsSQL(), []any{sid, t}},
		{`DELETE FROM effective_input_logs WHERE chat_session_id = ? AND turn_index >= ?`, []any{sid, t}},
	}
	if legacyPhysicalCleanup {
		commands = append(commands, d1LogicalTailCommand{`DELETE FROM precise_memory_units WHERE chat_session_id = ? AND source_turn_end >= ?`, []any{sid, t}})
	}
	commands = append(commands,
		d1LogicalTailCommand{`DELETE FROM memories WHERE chat_session_id = ? AND turn_index >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM direct_evidence_records WHERE chat_session_id = ? AND source_turn_end >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM kg_triples WHERE chat_session_id = ? AND (source_turn >= ? OR valid_from >= ?)`, []any{sid, t, t}},
		d1LogicalTailCommand{`DELETE FROM critic_feedback WHERE chat_session_id = ? AND target_type = 'turn' AND target_id >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM character_events WHERE chat_session_id = ? AND turn_index >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM speaker_attributions WHERE chat_session_id = ? AND source_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM entity_identity_artifact_bindings WHERE chat_session_id = ? AND source_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM entity_identity_surfaces WHERE chat_session_id = ? AND source_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`UPDATE entity_identities AS identity_row SET last_seen_turn = MAX(identity_row.first_seen_turn, COALESCE((SELECT MAX(surface.source_turn) FROM entity_identity_surfaces AS surface WHERE surface.chat_session_id = identity_row.chat_session_id AND surface.stable_entity_id = identity_row.stable_entity_id), identity_row.first_seen_turn)), updated_at = ` + d1NowExpression + ` WHERE identity_row.chat_session_id = ? AND identity_row.source_turn < ? AND identity_row.last_seen_turn >= ?`, []any{sid, t, t}},
		d1LogicalTailCommand{`DELETE FROM entity_identity_links WHERE chat_session_id = ? AND (source_entity_id IN (SELECT stable_entity_id FROM entity_identities WHERE chat_session_id = ? AND source_turn >= ?) OR target_entity_id IN (SELECT stable_entity_id FROM entity_identities WHERE chat_session_id = ? AND source_turn >= ?))`, []any{sid, sid, t, sid, t}},
		d1LogicalTailCommand{`DELETE FROM entity_identities WHERE chat_session_id = ? AND source_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`UPDATE entities SET last_seen_turn = ?, updated_at = ` + d1NowExpression + ` WHERE chat_session_id = ? AND (first_seen_turn IS NULL OR first_seen_turn < ?) AND last_seen_turn >= ?`, []any{t - 1, sid, t, t}},
		d1LogicalTailCommand{`DELETE FROM entities WHERE chat_session_id = ? AND first_seen_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM trust_states WHERE chat_session_id = ? AND source_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM storylines WHERE chat_session_id = ? AND (last_turn >= ? OR first_turn >= ?)`, []any{sid, t, t}},
		d1LogicalTailCommand{`DELETE FROM world_rules WHERE chat_session_id = ? AND source_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM character_states WHERE chat_session_id = ? AND turn_index >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM pending_threads WHERE chat_session_id = ? AND (source_turn >= ? OR created_turn >= ? OR resolved_turn >= ?)`, []any{sid, t, t, t}},
		d1LogicalTailCommand{`DELETE FROM active_states WHERE chat_session_id = ? AND turn_index >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM canonical_state_layers WHERE chat_session_id = ? AND turn_index >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM episode_summaries WHERE chat_session_id = ? AND (to_turn >= ? OR from_turn >= ?)`, []any{sid, t, t}},
		d1LogicalTailCommand{`UPDATE guidance_plan_states SET story_plan_json = NULL, director_json = NULL, warnings_json = NULL, state_status = 'empty', last_turn = -1, updated_at = ` + d1NowExpression + ` WHERE chat_session_id = ? AND last_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM chapter_summaries WHERE chat_session_id = ? AND (to_turn >= ? OR from_turn >= ?)`, []any{sid, t, t}},
		d1LogicalTailCommand{`DELETE FROM arc_summaries WHERE chat_session_id = ? AND (to_turn >= ? OR from_turn >= ?)`, []any{sid, t, t}},
		d1LogicalTailCommand{`DELETE FROM saga_digests WHERE chat_session_id = ? AND (to_turn >= ? OR from_turn >= ?)`, []any{sid, t, t}},
		d1LogicalTailCommand{`DELETE FROM session_active_scopes WHERE chat_session_id = ?`, []any{sid}},
		d1LogicalTailCommand{`DELETE FROM protagonist_entity_memories WHERE source_chat_session_id = ? AND source_turn_index >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM consequence_records WHERE chat_session_id = ? AND source_turn_end >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM psychology_branches WHERE chat_session_id = ? AND source_turn_end >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM theme_offscreen_carries WHERE chat_session_id = ? AND source_turn_end >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM capture_verification_records WHERE chat_session_id = ? AND turn_index >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM status_current_values WHERE chat_session_id = ? AND source_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM status_change_events WHERE chat_session_id = ? AND source_turn >= ? AND ` + d1JSONStringBlankPredicate("evidence_json", d1JSONPath("source_revision")), []any{sid, t}},
		d1LogicalTailCommand{`UPDATE status_effects SET effect_state = 'active', cleared_evidence_json = NULL, cleared_turn = NULL, updated_at = ` + d1NowExpression + ` WHERE chat_session_id = ? AND cleared_turn >= ?`, []any{sid, t}},
		d1LogicalTailCommand{`DELETE FROM status_effects WHERE chat_session_id = ? AND source_turn >= ?`, []any{sid, t}},
	)
	if legacyPhysicalCleanup {
		commands = append(commands, d1LogicalTailCommand{`DELETE FROM status_change_events WHERE chat_session_id = ? AND source_turn >= ?`, []any{sid, t}})
	}
	if deleteTailChat {
		commands = append(commands, d1LogicalTailCommand{`DELETE FROM chat_logs WHERE chat_session_id = ? AND turn_index >= ?`, []any{sid, t}})
	} else {
		commands = append(commands, d1LogicalTailCommand{`DELETE FROM chat_logs WHERE chat_session_id = ? AND turn_index = ?`, []any{sid, t}})
	}
	statements := make([]D1Statement, 0, len(commands))
	for _, command := range commands {
		statements = append(statements, D1Statement{SQL: command.query, Args: command.args})
	}
	return statements
}

// ---------------------------------------------------------------------------
// current status projection restore
// ---------------------------------------------------------------------------

// d1LogicalTurnStatusCurrentRestore is restoreActiveStatusCurrentValuesTx.
//
// It runs in the same batch as the tail sweep so a reader can never observe
// the moment where the tail is gone but the current status values still
// describe it. Three statements, in the reference's order:
//
//  1. re-upsert every current value still justified by surviving accepted
//     source history,
//  2. delete every current value whose newest surviving observation is an
//     explicit removal,
//  3. reproject the narrative pending thread from the surviving snapshots.
//
// Statement 3 needs a read (an occurrence lookup that decides UPDATE-by-id
// versus INSERT), so the read is taken here and only the writes are handed to
// the batch. That is the one place where a concurrent writer can be observed
// between the reference's read and the batch, and it is the same
// cross-Container boundary this file records for the tail probe.
func (s *d1Store) d1LogicalTurnStatusCurrentRestore(ctx context.Context, sid string) ([]D1Statement, error) {
	statements := []D1Statement{
		{SQL: d1LogicalTurnStatusCurrentUpsert, Args: []any{sid}},
		{SQL: d1LogicalTurnStatusCurrentDelete, Args: []any{sid}},
	}
	snapshots, err := s.d1LogicalTurnNarrativePendingSnapshots(ctx, sid)
	if err != nil {
		return nil, err
	}
	for _, snapshot := range snapshots {
		var value struct {
			Exact   bool           `json:"repair_pending_snapshot"`
			Pending *PendingThread `json:"pending_thread"`
		}
		if json.Unmarshal([]byte(snapshot), &value) == nil && value.Exact && value.Pending != nil {
			restored, err := s.d1StatusTransitionRestorePendingSnapshot(ctx, sid, value.Pending)
			if err != nil {
				return nil, err
			}
			statements = append(statements, restored...)
			continue
		}
		projected, err := s.d1StatusTransitionProjectPendingThread(ctx, sid, snapshot)
		if err != nil {
			return nil, err
		}
		statements = append(statements, projected...)
	}
	return statements, nil
}

// d1LogicalTurnStatusCurrentUpsert is the reference's
// INSERT ... SELECT ... ON DUPLICATE KEY UPDATE.
//
// ON DUPLICATE KEY UPDATE becomes ON CONFLICT over the schema's own
// UNIQUE (chat_session_id, registry_id, owner_scope, owner_id), which is the
// same conflict the MariaDB form resolves. The SELECT carries a WHERE, which
// SQLite requires before it will parse an upsert attached to a SELECT. created_at
// comes from the event rather than the column default so a rematerialised value
// keeps the moment its evidence was observed, and updated_at uses the inline now
// expression because the reference uses CURRENT_TIMESTAMP(3) there: binding the
// event's created_at would move updated_at backwards on every rematerialisation.
//
// d1JSONTextOrEmpty supplies subject_label. It is the shared helper precisely
// because the COALESCE exists to fall back to the registry label: a subject
// label that is not a JSON string cannot be an owner label, and MariaDB's
// coercion of a JSON null into the four characters "null" would put that text
// in owner_label exactly where the COALESCE intended the registry's own label.
var d1LogicalTurnStatusCurrentUpsert = `
	INSERT INTO status_current_values (
		chat_session_id, registry_id, status_key, owner_scope, owner_id,
		owner_label, value_kind, value_json, evidence_json, source_turn,
		write_state, created_at, updated_at
	)
	SELECT event.chat_session_id, event.registry_id, event.status_key,
	       event.owner_scope, event.owner_id,
	       COALESCE(NULLIF(` + d1JSONTextOrEmpty("event.new_value_json", d1JSONPath("subject_label")) + `, ''), registry.label),
	       registry.value_kind, event.new_value_json, event.evidence_json,
	       event.source_turn, 'current', event.created_at, ` + d1NowExpression + `
	FROM status_change_events event
	JOIN status_schema_registry registry ON registry.id = event.registry_id
	LEFT JOIN memory_source_revisions source_revision
	  ON source_revision.chat_session_id = event.chat_session_id
	 AND source_revision.source_revision = json_extract(event.evidence_json, '$."source_revision"')
	 AND source_revision.lifecycle_state = 'active'
	WHERE event.chat_session_id = ?
	  AND ` + d1StatusProjectionSourceSQL("event", "source_revision") + `
	  AND event.new_value_json IS NOT NULL
	  AND ` + d1CurrentProjectionPredicate("event.evidence_json") + `
	  AND COALESCE(json_extract(event.evidence_json, '$."projection_action"'), '') <> 'remove'
	  AND NOT EXISTS (
		SELECT 1
		FROM status_change_events newer
		LEFT JOIN memory_source_revisions newer_source
		  ON newer_source.chat_session_id = newer.chat_session_id
		 AND newer_source.source_revision = json_extract(newer.evidence_json, '$."source_revision"')
		 AND newer_source.lifecycle_state = 'active'
		WHERE newer.chat_session_id = event.chat_session_id
		  AND ` + d1StatusProjectionSourceSQL("newer", "newer_source") + `
		  AND newer.status_key = event.status_key
		  AND newer.owner_scope = event.owner_scope
		  AND newer.owner_id = event.owner_id
		  AND ` + d1CurrentProjectionPredicate("newer.evidence_json") + `
		  AND (` + d1StatusObservationTurnSQL("newer") + ` > ` + d1StatusObservationTurnSQL("event") + `
		       OR (` + d1StatusObservationTurnSQL("newer") + ` = ` + d1StatusObservationTurnSQL("event") + ` AND newer.id > event.id))
	  )
	ON CONFLICT (chat_session_id, registry_id, owner_scope, owner_id) DO UPDATE SET
		owner_label = excluded.owner_label, value_kind = excluded.value_kind,
		value_json = excluded.value_json, evidence_json = excluded.evidence_json,
		source_turn = excluded.source_turn, write_state = 'current',
		updated_at = ` + d1NowExpression

// d1LogicalTurnStatusCurrentDelete removes the current values whose newest
// surviving observation is an explicit removal.
//
// The correlated EXISTS names the same (status_key, owner_scope, owner_id) slot
// as the candidate row, which is why the DELETE carries the row alias: the
// anti-join must be per-slot, not per-session. Without that, one removal
// anywhere in the session would evict every current value in it.
var d1LogicalTurnStatusCurrentDelete = `
	DELETE FROM status_current_values AS current_value
	WHERE current_value.chat_session_id = ? AND EXISTS (
		SELECT 1 FROM status_change_events event
		LEFT JOIN memory_source_revisions source_revision
		  ON source_revision.chat_session_id = event.chat_session_id
		 AND source_revision.source_revision = json_extract(event.evidence_json, '$."source_revision"')
		WHERE event.chat_session_id = current_value.chat_session_id
		  AND event.status_key = current_value.status_key
		  AND event.owner_scope = current_value.owner_scope
		  AND event.owner_id = current_value.owner_id
		  AND ` + d1StatusProjectionSourceSQL("event", "source_revision") + `
		  AND ` + d1CurrentProjectionPredicate("event.evidence_json") + `
		  AND ` + d1JSONTextEquals("event.evidence_json", d1JSONPath("projection_action"), "remove") + `
		  AND NOT EXISTS (
			SELECT 1 FROM status_change_events newer
			LEFT JOIN memory_source_revisions newer_source
			  ON newer_source.chat_session_id = newer.chat_session_id
			 AND newer_source.source_revision = json_extract(newer.evidence_json, '$."source_revision"')
			 AND newer_source.lifecycle_state = 'active'
			WHERE newer.chat_session_id = event.chat_session_id
			  AND newer.status_key = event.status_key
			  AND newer.owner_scope = event.owner_scope
			  AND newer.owner_id = event.owner_id
			  AND ` + d1StatusProjectionSourceSQL("newer", "newer_source") + `
			  AND ` + d1CurrentProjectionPredicate("newer.evidence_json") + `
			  AND (` + d1StatusObservationTurnSQL("newer") + ` > ` + d1StatusObservationTurnSQL("event") + `
			       OR (` + d1StatusObservationTurnSQL("newer") + ` = ` + d1StatusObservationTurnSQL("event") + ` AND newer.id > event.id))
		  )
	)`

// d1LogicalTurnNarrativePendingSnapshots is the reference's narrative snapshot
// read. It must stay a UNION ALL rather than a UNION: duplicate snapshots are
// two observations of the same slot, and deduplicating them would silently drop
// one and let the earlier text win.
func (s *d1Store) d1LogicalTurnNarrativePendingSnapshots(ctx context.Context, sid string) ([]string, error) {
	rows, err := s.conn.Query(ctx, `
	SELECT current_value.value_json
	FROM status_current_values current_value
	LEFT JOIN memory_source_revisions source_revision
	  ON source_revision.chat_session_id = current_value.chat_session_id
	 AND source_revision.source_revision = json_extract(current_value.evidence_json, '$."source_revision"')
	 AND source_revision.lifecycle_state = 'active'
	WHERE current_value.chat_session_id = ?
	  AND `+d1StatusProjectionSourceSQL("current_value", "source_revision")+`
	  AND current_value.status_key = 'narrative_state'
	  AND current_value.write_state = 'current'
	  AND `+d1JSONPathPresent("current_value.value_json", d1JSONPath("pending_thread"))+`
	UNION ALL
	SELECT event.new_value_json FROM status_change_events event
	LEFT JOIN memory_source_revisions source_revision
	  ON source_revision.chat_session_id = event.chat_session_id
	 AND source_revision.source_revision = json_extract(event.evidence_json, '$."source_revision"')
	WHERE event.chat_session_id = ? AND event.status_key = 'narrative_state'
	  AND `+d1StatusProjectionSourceSQL("event", "source_revision")+`
	  AND `+d1CurrentProjectionPredicate("event.evidence_json")+`
	  AND `+d1JSONTextEquals("event.evidence_json", d1JSONPath("projection_action"), "remove")+`
	  AND `+d1JSONPathPresent("event.new_value_json", d1JSONPath("pending_thread"))+`
	  AND NOT EXISTS (
		SELECT 1 FROM status_change_events newer
		LEFT JOIN memory_source_revisions newer_source
		  ON newer_source.chat_session_id = newer.chat_session_id
		 AND newer_source.source_revision = json_extract(newer.evidence_json, '$."source_revision"')
		WHERE newer.chat_session_id = event.chat_session_id
		  AND newer.status_key = event.status_key
		  AND newer.owner_scope = event.owner_scope
		  AND newer.owner_id = event.owner_id
		  AND `+d1StatusProjectionSourceSQL("newer", "newer_source")+`
		  AND `+d1CurrentProjectionPredicate("newer.evidence_json")+`
		  AND (`+d1StatusObservationTurnSQL("newer")+` > `+d1StatusObservationTurnSQL("event")+`
		       OR (`+d1StatusObservationTurnSQL("newer")+` = `+d1StatusObservationTurnSQL("event")+` AND newer.id > event.id))
	  )`, sid, sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var snapshots []string
	for rows.Next() {
		var snapshot string
		if err := rows.Scan(&snapshot); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, rows.Err()
}

// ---------------------------------------------------------------------------
// RollbackCanonicalTail
// ---------------------------------------------------------------------------

// RollbackCanonicalTail performs source invalidation, durable vector-delete
// enqueueing, raw deletion, and derived cleanup in one atomic batch. Vector
// provider I/O intentionally happens after this method returns.
func (s *d1Store) RollbackCanonicalTail(ctx context.Context, rollback LogicalTurnRollback) error {
	if s == nil || s.conn == nil {
		return d1LogicalTurnError("logical_turn_store_unavailable", "preflight", false, "not_committed", ErrNotEnabled)
	}
	sid := strings.TrimSpace(rollback.ChatSessionID)
	if sid == "" || rollback.TurnIndex <= 0 {
		return d1LogicalTurnError("logical_turn_request_invalid", "preflight", false, "not_committed",
			fmt.Errorf("invalid logical turn rollback"))
	}
	lifecycleAction := strings.ToLower(strings.TrimSpace(rollback.LifecycleAction))
	switch lifecycleAction {
	case "":
		lifecycleAction = LogicalTurnLifecycleInvalidated
	case LogicalTurnLifecycleInvalidated, LogicalTurnLifecycleSuperseded, LogicalTurnLifecycleDeleted:
	default:
		return d1LogicalTurnError("logical_turn_lifecycle_action_invalid", "preflight", false, "not_committed",
			fmt.Errorf("unsupported logical turn lifecycle action %q", rollback.LifecycleAction))
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	latestTurn, err := s.d1LogicalTurnCanonicalTail(ctx, sid)
	if err != nil {
		return d1LogicalTurnClassifyError(err, "canonical_tail_read", false)
	}
	// Unlike the replacement, a rollback MAY start past the tail: the host is
	// reporting a turn it already saw is gone. It may not start before it. An
	// empty session has tail 0, and the reference's latest.Valid=false skips the
	// check entirely there, so turn 1 is accepted in an empty session too.
	if latestTurn > 0 && rollback.TurnIndex > latestTurn+1 {
		return d1LogicalTurnError("logical_turn_not_current_tail", "canonical_tail_check", false, "not_committed",
			fmt.Errorf("logical turn rollback begins after canonical tail: latest=%d requested=%d", latestTurn, rollback.TurnIndex))
	}
	now := d1TimeValue(rollback.CreatedAt)
	reason := firstNonEmptyString(rollback.Reason, "turn_rollback")

	revisions, err := s.d1LogicalTurnRetirableRevisions(ctx, sid, rollback.TurnIndex)
	if err != nil {
		return d1LogicalTurnClassifyError(err, "source_revision_invalidate", false)
	}
	if lifecycleAction != LogicalTurnLifecycleDeleted {
		revisions, err = s.d1LogicalTurnActiveRevisions(ctx, sid, rollback.TurnIndex, "")
		if err != nil {
			return d1LogicalTurnClassifyError(err, "source_revision_invalidate", false)
		}
	}
	deletes, err := s.d1KnownVectorDeletes(ctx, sid, revisions, rollback.TurnIndex)
	if err != nil {
		return d1LogicalTurnClassifyError(err, "source_revision_invalidate", false)
	}
	statements := d1LogicalTurnVectorDeleteStatements(sid, deletes, reason, now)
	statements = append(statements, d1LogicalTurnInvalidationStatements(sid, revisions, lifecycleAction, reason, now, "")...)
	statements = append(statements, d1LogicalTurnCanonicalTailCommands(sid, rollback.TurnIndex, false, true)...)
	restore, err := s.d1LogicalTurnStatusCurrentRestore(ctx, sid)
	if err != nil {
		return d1LogicalTurnClassifyError(err, "status_current_restore", false)
	}
	statements = append(statements, restore...)
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return d1LogicalTurnClassifyError(err, "canonical_rollback", true)
	}
	return nil
}

// ---------------------------------------------------------------------------
// SessionStitchStore
// ---------------------------------------------------------------------------

// The request order is the author's story order. The active chat is always the
// last segment. Existing source rows are read in one pass and never updated,
// locked for future use, or deleted by this operation.
//
// The reference serialises concurrent stitches of the same request with
// SELECT GET_LOCK on the deterministic target session id. D1 has no advisory
// locks, so the ledger row is the claim instead: the target id is a pure
// function of (operation id, ordered session ids), so two concurrent stitches
// of the same request write the same target, and the second one finds the
// first one's ledger row. A row still in status copying is the observable
// equivalent of "the lock is held", and it produces the same answer the
// reference produces under contention: the retry error, not a half-written
// result.
//
// The stitch order itself is the request order, and it is the whole content of
// the operation: the segments carry the ordinal and the turn offset each
// session was imported at, and a later reader reconstructs the narrative from
// those numbers. Nothing re-sorts them, so the ORDER BY the ledger probe
// carries is the only sort key in the path and it is pinned by a test.
func (s *d1Store) StitchSessions(ctx context.Context, req SessionStitchRequest) (*SessionStitchResult, error) {
	if s == nil || s.conn == nil {
		return nil, ErrNotEnabled
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	current := strings.TrimSpace(req.CurrentSessionID)
	if current == "" || strings.TrimSpace(req.OperationID) == "" {
		return nil, errors.New("current_session_id and operation_id are required")
	}
	ids := []string{}
	seen := map[string]bool{current: true}
	for _, id := range req.SourceSessionIDs {
		id = strings.TrimSpace(id)
		if id != "" && !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	if len(ids) == 0 {
		return nil, errors.New("select at least one previous session")
	}
	// The active chat is appended last and is never accepted from the source
	// list: a stitch that put the live chat first would renumber the live
	// session's own turns and move the conversation the user is looking at.
	ids = append(ids, current)
	target := "stitch_" + sessionMigrationStringHash("session-stitch.v1", req.OperationID, strings.Join(ids, "\x1f"))[:40]

	// The existing migration ledger is the retry and restart record.
	migrationID, note, status, found, err := s.d1SessionStitchLedger(ctx, target)
	if err != nil {
		return nil, err
	}
	if found {
		if status == "copying" {
			return nil, errors.New("session stitch is still processing; retry this operation")
		}
		var result SessionStitchResult
		if err := json.Unmarshal([]byte(note), &result); err != nil {
			return nil, err
		}
		result.MigrationID, result.TargetSessionID = migrationID, target
		result.EntityIDMap, err = s.d1SessionStitchEntityMap(ctx, migrationID)
		return &result, err
	}

	manifest := SessionMigrationManifest()
	if err := s.d1SessionMigrationValidateManifestSchema(ctx); err != nil {
		return nil, err
	}
	snapshots := make([]map[string][]sessionMigrationRow, len(ids))
	targetRows := make(map[string][]sessionMigrationRow, len(manifest))
	for _, entry := range manifest {
		plan, ok := SessionMigrationExecutionPlanFor(entry.Table)
		if !ok {
			return nil, fmt.Errorf("session migration manifest plan missing for %s", entry.Table)
		}
		for i, id := range ids {
			if snapshots[i] == nil {
				snapshots[i] = map[string][]sessionMigrationRow{}
			}
			rows, err := s.d1SessionMigrationReadRows(ctx, entry, plan, id)
			if err != nil {
				return nil, err
			}
			snapshots[i][entry.Table] = rows
		}
		rows, err := s.d1SessionMigrationReadRows(ctx, entry, plan, target)
		if err != nil {
			return nil, err
		}
		targetRows[entry.Table] = rows
	}

	// A stitched current chat still displays only its own tail, not the earlier
	// segments already imported into it. Keep that boundary through another join.
	previous := SessionStitchResult{CurrentSourceSessionID: current}
	if _, previousNote, _, found, err := s.d1SessionStitchLedger(ctx, current); err != nil {
		return nil, err
	} else if found {
		if err := json.Unmarshal([]byte(previousNote), &previous); err != nil {
			return nil, err
		}
	}

	rows, segments, err := sessionStitchSnapshot(ids, snapshots, current, req.RebuildPublicProjection, previous.CurrentOffset)
	if err != nil {
		return nil, err
	}
	result := &SessionStitchResult{
		TargetSessionID:        target,
		Segments:               segments,
		CurrentOffset:          segments[len(segments)-1].Offset + previous.CurrentOffset,
		CurrentSourceSessionID: previous.CurrentSourceSessionID,
	}
	result.CurrentSourceSessionIDs = append([]string{current}, previous.CurrentSourceSessionIDs...)
	result.CurrentInputGroupAliases = sessionStitchInputGroupAliases(snapshots[len(snapshots)-1]["audit_logs"], previous.CurrentInputGroupAliases)
	encoded, _ := json.Marshal(result)

	// The ordered snapshot goes through the same copy path the plain migration
	// uses, so the row map, the deferred foreign keys, the parity proof and the
	// vector expected-ID ledger are produced by one implementation rather than a
	// second one that could drift from it.
	execution, err := s.d1SessionMigrationCopySnapshot(ctx, SessionMigrationCompleteRequest{
		SourceSessionID: current,
		TargetSessionID: target,
		Mode:            SessionMigrationModeStitch,
		OperatorNote:    string(encoded),
	}, manifest, rows, targetRows)
	if err != nil {
		return nil, err
	}
	result.MigrationID, result.EntityIDMap = execution.MigrationID, execution.EntityIDMap
	return result, nil
}

// d1SessionStitchLedger reads the durable operation note for a stitch target.
//
// ORDER BY id DESC LIMIT 1 is the newest-wins rule and it is load-bearing. A
// target can carry more than one non-reverted ledger row when an operation was
// retried after a partial failure, and the note of the LATEST attempt is the
// one whose segment offsets describe the target as it is now. Reading the
// oldest would hand a caller offsets for a copy that was later superseded.
func (s *d1Store) d1SessionStitchLedger(ctx context.Context, sessionID string) (int64, string, string, bool, error) {
	var id int64
	var note, status string
	err := s.conn.QueryRow(ctx, `
		SELECT id, COALESCE(operator_note, ''), status
		FROM session_migrations
		WHERE target_session_id = ? AND mode = ?
		  AND status NOT IN ('rolled_back', 'rollback_partial')
		ORDER BY id DESC
		LIMIT 1
	`, sessionID, SessionMigrationModeStitch).Scan(&id, &note, &status)
	if errors.Is(err, errD1NoRows) {
		return 0, "", "", false, nil
	}
	if err != nil {
		return 0, "", "", false, err
	}
	return id, note, status, true, nil
}

// d1SessionStitchEntityMap reads the stable-entity identities a completed
// stitch assigned, so a replayed request reports the same identity map the
// original did. The row map is the record, not the target table: a later
// migration of the same target would rewrite the target identities, and a
// replay must still report what THIS operation produced.
func (s *d1Store) d1SessionStitchEntityMap(ctx context.Context, migrationID int64) (map[string]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT source_key, target_key
		FROM session_migration_artifact_row_map
		WHERE migration_id = ? AND table_name = 'entity_identities' AND key_column_name = 'stable_entity_id'
	`, migrationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var source, target string
		if err := rows.Scan(&source, &target); err != nil {
			return nil, err
		}
		out[source] = target
	}
	return out, rows.Err()
}
