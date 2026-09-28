package store

import (
	"context"
	"strings"
)

// D1 typed-lane reads over precise_memory_units.
//
// CharacterPerspectiveMemoryReader, ActiveInteractionMemoryReader, and
// GeneralVectorPreciseMemoryReader are the three prepare-turn lanes that read
// atomic precise units instead of the legacy aggregate Memory row. They are
// implemented together because they share one table, one fence, and one row
// shape, and because prepare-turn discovers them by the same type assertion
// pattern: a provider that implements two of the three would render a
// character's point of view from an empty projection while reporting the other
// lanes as available.
//
// What is translated rather than re-derived
//
// Every statement below is the MariaDB statement from mariadb_precise_memory.go
// with the dialect-level adjustments D1 needs, and nothing else. The load-bearing
// details that are easy to lose in a rewrite:
//
//   - The active-source-revision INNER JOIN is the fence. A unit whose accepted
//     source revision has been superseded, invalidated, or deleted is no longer
//     part of the current scene, even while the unit row itself is still
//     'active'. Lifecycle invalidation updates the units (d1
//     InvalidateSourceRevisions, the rollback path, and the logical-turn
//     replacement path all do), so on a well-formed database the two conditions
//     agree. The join is kept anyway: it is the second, independent fence, and
//     it is what stops a unit written against a revision that was never accepted
//     from being rendered as current fact.
//   - The join cannot fan out. memory_source_revisions.source_revision is UNIQUE
//     in both schemas, so one unit matches at most one revision row on either
//     provider and no DISTINCT is needed. A de-duplicating DISTINCT would hide a
//     schema regression rather than report it.
//   - The two ordered lanes order by source_turn_start ASC, unit_id ASC: the
//     scene's own chronology, with the unit id as a deterministic tie-break for
//     several units committed in the same turn. The general-vector lane orders
//     by id ASC instead, because it feeds a candidate inventory rather than a
//     narrative, and a stable insertion order is what lets the caller's
//     eligibility filter and budget assembly be reproducible.
//   - No limit is applied. The interfaces take no limit, and prepare-turn
//     applies its own history-scope filter and per-session candidate budget
//     after the read. A page here would silently truncate the candidate set
//     rather than bound a response.
//   - Review metadata is not a filter. admission_state and review_state are
//     returned verbatim and are not filtered on by the interaction lane, so a
//     unit still awaiting review keeps describing the occurrence. Privacy
//     (visibility, knowledge_holder_entity_id) and epistemic scope are the
//     exclusion axes; audit state is not.
//
// Argument and error parity
//
// A blank or whitespace-only session id is ErrNotFound on the MariaDB path, and
// the perspective read treats a blank knowledge holder the same way. Callers
// depend on this: prepare-turn filters those two sentinel errors out of its read
// diagnostics, while any other error becomes a degraded prepare-turn trace. The
// same guards run before any statement is sent, so a malformed request cannot
// reach SQLite.
//
// SQLite notes
//
//   - COALESCE(col, '') is kept instead of being replaced by nullable scans.
//     The MariaDB SELECT list already collapses NULL to empty text, so keeping
//     the expression is what makes the D1 scan identical instead of merely
//     equivalent, and the typed columns stay non-NULL for the caller.
//   - created_at and updated_at are TEXT in the canonical schema and are parsed
//     by the shared D1 time conversion, exactly as on the other D1 readers.
//   - chat_session_id and knowledge_holder_entity_id are compared with SQLite's
//     binary equality, where the MariaDB columns are utf8mb4_unicode_ci and so
//     case-insensitive. This matches every other D1 reader of the same columns
//     (the entity-identity reads already compare stable_entity_id the same way),
//     and it is safe for this lane in particular: entity ids are deterministic
//     lowercase hex UUIDs produced by entityIdentityStableID, so the value
//     prepare-turn passes as the current knowledge holder is the stored value
//     byte for byte. Making only this reader case-insensitive would disagree
//     with the resolver that produced the argument.
//
// An empty result is a non-nil empty slice on both providers, so a session with
// no eligible units encodes identically whichever backend served it.

var _ CharacterPerspectiveMemoryReader = (*d1Store)(nil)
var _ ActiveInteractionMemoryReader = (*d1Store)(nil)
var _ GeneralVectorPreciseMemoryReader = (*d1Store)(nil)

// d1PreciseMemorySourceFence is the INNER JOIN that keeps only units whose
// accepted source revision is still active. It is the D1 spelling of the join in
// mariadb_precise_memory.go and is shared by all three lanes so the fence can
// never drift between them.
const d1PreciseMemorySourceFence = `
		FROM precise_memory_units unit
		JOIN memory_source_revisions source_revision
		  ON source_revision.chat_session_id = unit.chat_session_id
		 AND source_revision.source_revision = unit.source_revision
		 AND source_revision.lifecycle_state = 'active'`

// d1PerspectiveUnitSelect is the MariaDB perspective column list, in the same
// order, so the scan and the returned struct are the same on both providers.
// The perspective lane reads no relationship or location identity: those belong
// to the interaction lane and to the general-vector inventory.
const d1PerspectiveUnitSelect = `
		SELECT
			unit.unit_id, unit.chat_session_id, unit.source_turn_start,
			unit.source_turn_end, unit.source_revision, unit.memory_kind,
			COALESCE(unit.memory_subtype, ''), unit.payload_json,
			COALESCE(unit.actor_entity_id, ''),
			COALESCE(unit.subject_entity_id, ''),
			unit.truth_scope, unit.epistemic_mode, unit.authority_class,
			unit.admission_state, unit.review_state, unit.visibility,
			COALESCE(unit.knowledge_holder_entity_id, ''),
			COALESCE(unit.reveal_condition, ''), unit.lifecycle_state,
			unit.created_at, unit.updated_at`

// d1InteractionUnitSelect is the MariaDB interaction column list. It adds the
// relationship identities (affected, object, relationship_key) that prepare-turn
// uses to describe who did what to whom.
const d1InteractionUnitSelect = `
		SELECT
			unit.unit_id, unit.chat_session_id, unit.source_turn_start,
			unit.source_turn_end, unit.source_revision, unit.memory_kind,
			COALESCE(unit.memory_subtype, ''), unit.payload_json,
			COALESCE(unit.actor_entity_id, ''),
			COALESCE(unit.subject_entity_id, ''),
			COALESCE(unit.affected_entity_id, ''),
			COALESCE(unit.object_entity_id, ''),
			COALESCE(unit.relationship_key, ''),
			unit.truth_scope, unit.epistemic_mode, unit.authority_class,
			unit.admission_state, unit.review_state, unit.visibility,
			COALESCE(unit.knowledge_holder_entity_id, ''),
			COALESCE(unit.reveal_condition, ''), unit.lifecycle_state,
			unit.created_at, unit.updated_at`

// d1GeneralVectorUnitSelect is the MariaDB general-vector column list. It is the
// only lane that reads unit.id and unit.confidence, and the only one that also
// carries location_entity_id, because a vector-hit hydration has to reconstruct
// the fact and its provenance rather than render a character view.
const d1GeneralVectorUnitSelect = `
		SELECT
			unit.id, unit.unit_id, unit.chat_session_id,
			unit.source_turn_start, unit.source_turn_end, unit.source_revision,
			unit.memory_kind, COALESCE(unit.memory_subtype, ''), unit.payload_json,
			COALESCE(unit.actor_entity_id, ''), COALESCE(unit.subject_entity_id, ''),
			COALESCE(unit.affected_entity_id, ''), COALESCE(unit.location_entity_id, ''),
			COALESCE(unit.object_entity_id, ''), COALESCE(unit.relationship_key, ''),
			unit.truth_scope, unit.authority_class,
			unit.admission_state, unit.review_state, unit.visibility,
			COALESCE(unit.knowledge_holder_entity_id, ''),
			unit.epistemic_mode, COALESCE(unit.reveal_condition, ''),
			unit.confidence, unit.lifecycle_state, unit.created_at, unit.updated_at`

// d1PerspectiveEpistemicModes is the closed set a knowledge-holder unit may
// carry. A unit outside it is not a perspective at all (for example a 'direct'
// observation of the objective world), so it belongs to the general-vector
// inventory and must not be rendered as one character's private knowledge.
const d1PerspectiveEpistemicModes = `('known', 'suspected', 'unknown', 'misinformed', 'hidden', 'revealed')`

// d1PreciseMemoryReadSession trims a session id and rejects a blank one with
// ErrNotFound, matching the MariaDB guard. It runs before any statement so a
// malformed request is answered without touching the database.
func d1PreciseMemoryReadSession(chatSessionID string) (string, error) {
	chatSessionID = strings.TrimSpace(chatSessionID)
	if chatSessionID == "" {
		return "", ErrNotFound
	}
	return chatSessionID, nil
}

// ListCharacterPerspectiveMemoryUnits returns the source-active perspective
// units held by exactly one knowledge holder, oldest turn first.
//
// The holder predicate is an exact equality and not a scope or prefix match: a
// unit written about what one character knows must never be rendered as what
// another character knows, and the caller still re-checks the current stable
// knowledge-holder identity before rendering any payload.
func (s *d1Store) ListCharacterPerspectiveMemoryUnits(ctx context.Context, chatSessionID, knowledgeHolderEntityID string) ([]PreciseMemoryUnit, error) {
	chatSessionID, err := d1PreciseMemoryReadSession(chatSessionID)
	if err != nil {
		return nil, err
	}
	knowledgeHolderEntityID = strings.TrimSpace(knowledgeHolderEntityID)
	if knowledgeHolderEntityID == "" {
		return nil, ErrNotFound
	}

	rows, err := s.conn.Query(ctx, d1PerspectiveUnitSelect+d1PreciseMemorySourceFence+`
		WHERE unit.chat_session_id = ?
		  AND unit.memory_kind = 'observation'
		  AND unit.knowledge_holder_entity_id = ?
		  AND unit.epistemic_mode IN `+d1PerspectiveEpistemicModes+`
		  AND unit.lifecycle_state = 'active'
		ORDER BY unit.source_turn_start ASC, unit.unit_id ASC
	`, chatSessionID, knowledgeHolderEntityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []PreciseMemoryUnit{}
	for rows.Next() {
		item, err := d1ScanPerspectiveMemoryUnit(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListActiveInteractionMemoryUnits returns the source-active relationship
// observations and interaction boundaries of one session, oldest turn first.
//
// Review metadata does not erase the occurrence, so a unit still awaiting review
// is returned with its admission_state and review_state intact. The caller applies
// visibility and current-turn selection for the request being assembled.
func (s *d1Store) ListActiveInteractionMemoryUnits(ctx context.Context, chatSessionID string) ([]PreciseMemoryUnit, error) {
	chatSessionID, err := d1PreciseMemoryReadSession(chatSessionID)
	if err != nil {
		return nil, err
	}

	rows, err := s.conn.Query(ctx, d1InteractionUnitSelect+d1PreciseMemorySourceFence+`
		WHERE unit.chat_session_id = ?
		  AND unit.memory_kind IN ('observation', 'boundary')
		  AND unit.lifecycle_state = 'active'
		ORDER BY unit.source_turn_start ASC, unit.unit_id ASC
	`, chatSessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []PreciseMemoryUnit{}
	for rows.Next() {
		item, err := d1ScanInteractionMemoryUnit(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// ListGeneralVectorPreciseMemoryUnits returns the source-active units eligible
// for the general vector index, in stable insertion order, with their canonical
// fact text and provenance for request-time vector-hit hydration.
//
// The statement is fenced and filtered to active units in SQL, and the privacy
// and epistemic eligibility rules are then applied through the shared
// PreciseMemoryGeneralVectorEligible, exactly as the MariaDB path does. Reusing
// that function rather than restating the rules in SQL is the point: the writer
// side decides eligibility with the same function when it enqueues the durable
// outbox row, so the indexed set and the read-back inventory cannot disagree.
// Perspective-scoped units stay on their typed lanes because that function
// rejects a unit that carries a knowledge holder, a restricted visibility, or a
// holder-relative epistemic mode.
func (s *d1Store) ListGeneralVectorPreciseMemoryUnits(ctx context.Context, chatSessionID string) ([]PreciseMemoryUnit, error) {
	chatSessionID, err := d1PreciseMemoryReadSession(chatSessionID)
	if err != nil {
		return nil, err
	}

	rows, err := s.conn.Query(ctx, d1GeneralVectorUnitSelect+d1PreciseMemorySourceFence+`
		WHERE unit.chat_session_id = ?
		  AND unit.lifecycle_state = 'active'
		ORDER BY unit.id ASC
	`, chatSessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []PreciseMemoryUnit{}
	for rows.Next() {
		item, err := d1ScanGeneralVectorMemoryUnit(rows)
		if err != nil {
			return nil, err
		}
		if PreciseMemoryGeneralVectorEligible(&item) {
			out = append(out, item)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// The three scan helpers take D1Rows rather than a scan-only interface so they
// cannot be handed a result handle the caller still owns, and so a future
// single-row variant of one of these statements has to widen them deliberately.

func d1ScanPerspectiveMemoryUnit(row D1Rows) (PreciseMemoryUnit, error) {
	var item PreciseMemoryUnit
	if err := row.Scan(
		&item.UnitID, &item.ChatSessionID, &item.SourceTurnStart,
		&item.SourceTurnEnd, &item.SourceRevision, &item.Kind,
		&item.Subtype, &item.PayloadJSON, &item.ActorEntityID,
		&item.SubjectEntityID, &item.TruthScope, &item.EpistemicMode,
		&item.AuthorityClass, &item.AdmissionState, &item.ReviewState,
		&item.Visibility, &item.KnowledgeHolderEntityID,
		&item.RevealCondition, &item.LifecycleState, &item.CreatedAt,
		&item.UpdatedAt,
	); err != nil {
		return PreciseMemoryUnit{}, err
	}
	return item, nil
}

func d1ScanInteractionMemoryUnit(row D1Rows) (PreciseMemoryUnit, error) {
	var item PreciseMemoryUnit
	if err := row.Scan(
		&item.UnitID, &item.ChatSessionID, &item.SourceTurnStart,
		&item.SourceTurnEnd, &item.SourceRevision, &item.Kind,
		&item.Subtype, &item.PayloadJSON, &item.ActorEntityID,
		&item.SubjectEntityID, &item.AffectedEntityID, &item.ObjectEntityID,
		&item.RelationshipKey, &item.TruthScope, &item.EpistemicMode,
		&item.AuthorityClass, &item.AdmissionState, &item.ReviewState,
		&item.Visibility, &item.KnowledgeHolderEntityID,
		&item.RevealCondition, &item.LifecycleState, &item.CreatedAt,
		&item.UpdatedAt,
	); err != nil {
		return PreciseMemoryUnit{}, err
	}
	return item, nil
}

func d1ScanGeneralVectorMemoryUnit(row D1Rows) (PreciseMemoryUnit, error) {
	var item PreciseMemoryUnit
	if err := row.Scan(
		&item.ID, &item.UnitID, &item.ChatSessionID,
		&item.SourceTurnStart, &item.SourceTurnEnd, &item.SourceRevision,
		&item.Kind, &item.Subtype, &item.PayloadJSON,
		&item.ActorEntityID, &item.SubjectEntityID, &item.AffectedEntityID,
		&item.LocationEntityID, &item.ObjectEntityID, &item.RelationshipKey,
		&item.TruthScope, &item.AuthorityClass,
		&item.AdmissionState, &item.ReviewState, &item.Visibility,
		&item.KnowledgeHolderEntityID, &item.EpistemicMode, &item.RevealCondition,
		&item.Confidence, &item.LifecycleState, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return PreciseMemoryUnit{}, err
	}
	return item, nil
}
