package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Rollback mutations are canonical D1 operations.  Batch is deliberately used
// where MariaDB uses a transaction: the Worker bridge executes a D1 batch as
// one SQLite transaction, so no partially-rewound entity or status projection
// can become visible.
var _ RollbackStore = (*d1Store)(nil)

func (s *d1Store) d1RollbackExec(ctx context.Context, query string, args ...any) error {
	_, err := s.conn.Exec(ctx, query, args...)
	return err
}

func (s *d1Store) DeleteChatLogs(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM chat_logs WHERE chat_session_id = ? AND turn_index >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteEffectiveInputs(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM effective_input_logs WHERE chat_session_id = ? AND turn_index >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteMemories(ctx context.Context, sid string, fromTurn int) error {
	return s.conn.Batch(ctx,
		D1Statement{SQL: `UPDATE precise_memory_units SET lifecycle_state = 'invalidated', updated_at = ? WHERE chat_session_id = ? AND source_turn_end >= ? AND lifecycle_state = 'active'`, Args: []any{d1TimeValue(time.Now().UTC()), sid, fromTurn}},
		D1Statement{SQL: `DELETE FROM memories WHERE chat_session_id = ? AND turn_index >= ?`, Args: []any{sid, fromTurn}},
	)
}
func (s *d1Store) DeleteEvidence(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM direct_evidence_records WHERE chat_session_id = ? AND source_turn_end >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteKGTriples(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM kg_triples WHERE chat_session_id = ? AND (source_turn >= ? OR valid_from >= ?)`, sid, fromTurn, fromTurn)
}
func (s *d1Store) DeleteCriticFeedback(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM critic_feedback WHERE chat_session_id = ? AND target_type = 'turn' AND target_id >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteCharacterEvents(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM character_events WHERE chat_session_id = ? AND turn_index >= ?`, sid, fromTurn)
}

func (s *d1Store) DeleteEntities(ctx context.Context, sid string, fromTurn int) error {
	now := d1TimeValue(time.Now().UTC())
	return s.conn.Batch(ctx,
		D1Statement{SQL: `UPDATE precise_memory_units SET lifecycle_state = 'invalidated', actor_entity_id = NULL, subject_entity_id = NULL, affected_entity_id = NULL, location_entity_id = NULL, object_entity_id = NULL, knowledge_holder_entity_id = NULL, updated_at = ? WHERE chat_session_id = ? AND source_turn_end >= ?`, Args: []any{now, sid, fromTurn}},
		D1Statement{SQL: `DELETE FROM speaker_attributions WHERE chat_session_id = ? AND source_turn >= ?`, Args: []any{sid, fromTurn}},
		D1Statement{SQL: `DELETE FROM entity_identity_artifact_bindings WHERE chat_session_id = ? AND source_turn >= ?`, Args: []any{sid, fromTurn}},
		D1Statement{SQL: `DELETE FROM entity_identity_surfaces WHERE chat_session_id = ? AND source_turn >= ?`, Args: []any{sid, fromTurn}},
		D1Statement{SQL: `UPDATE entity_identities AS identity_row SET last_seen_turn = MAX(identity_row.first_seen_turn, COALESCE((SELECT MAX(surface.source_turn) FROM entity_identity_surfaces AS surface WHERE surface.chat_session_id = identity_row.chat_session_id AND surface.stable_entity_id = identity_row.stable_entity_id), identity_row.first_seen_turn)), updated_at = ? WHERE identity_row.chat_session_id = ? AND identity_row.source_turn < ? AND identity_row.last_seen_turn >= ?`, Args: []any{now, sid, fromTurn, fromTurn}},
		D1Statement{SQL: `DELETE FROM entity_identity_links WHERE chat_session_id = ? AND (source_entity_id IN (SELECT stable_entity_id FROM entity_identities WHERE chat_session_id = ? AND source_turn >= ?) OR target_entity_id IN (SELECT stable_entity_id FROM entity_identities WHERE chat_session_id = ? AND source_turn >= ?))`, Args: []any{sid, sid, fromTurn, sid, fromTurn}},
		D1Statement{SQL: `DELETE FROM entity_identities WHERE chat_session_id = ? AND source_turn >= ?`, Args: []any{sid, fromTurn}},
		D1Statement{SQL: `UPDATE entities SET last_seen_turn = ?, updated_at = ? WHERE chat_session_id = ? AND (first_seen_turn IS NULL OR first_seen_turn < ?) AND last_seen_turn >= ?`, Args: []any{fromTurn - 1, now, sid, fromTurn, fromTurn}},
		D1Statement{SQL: `DELETE FROM entities WHERE chat_session_id = ? AND first_seen_turn >= ?`, Args: []any{sid, fromTurn}},
	)
}

func (s *d1Store) DeleteTrustStates(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM trust_states WHERE chat_session_id = ? AND source_turn >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteStorylines(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM storylines WHERE chat_session_id = ? AND (last_turn >= ? OR first_turn >= ?)`, sid, fromTurn, fromTurn)
}
func (s *d1Store) DeleteWorldRules(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM world_rules WHERE chat_session_id = ? AND source_turn >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteCharacterStates(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM character_states WHERE chat_session_id = ? AND turn_index >= ?`, sid, fromTurn)
}
func (s *d1Store) DeletePendingThreads(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM pending_threads WHERE chat_session_id = ? AND (source_turn >= ? OR created_turn >= ? OR resolved_turn >= ?)`, sid, fromTurn, fromTurn, fromTurn)
}
func (s *d1Store) DeleteActiveStates(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM active_states WHERE chat_session_id = ? AND turn_index >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteCanonicalStateLayers(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM canonical_state_layers WHERE chat_session_id = ? AND turn_index >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteEpisodeSummaries(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM episode_summaries WHERE chat_session_id = ? AND (to_turn >= ? OR from_turn >= ?)`, sid, fromTurn, fromTurn)
}
func (s *d1Store) DeleteGuidancePlanState(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `UPDATE guidance_plan_states SET story_plan_json = NULL, director_json = NULL, warnings_json = NULL, state_status = 'empty', last_turn = -1, updated_at = ? WHERE chat_session_id = ? AND last_turn >= ?`, d1TimeValue(time.Now().UTC()), sid, fromTurn)
}
func (s *d1Store) DeleteChapterSummaries(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM chapter_summaries WHERE chat_session_id = ? AND (to_turn >= ? OR from_turn >= ?)`, sid, fromTurn, fromTurn)
}
func (s *d1Store) DeleteArcSummaries(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM arc_summaries WHERE chat_session_id = ? AND (to_turn >= ? OR from_turn >= ?)`, sid, fromTurn, fromTurn)
}
func (s *d1Store) DeleteSagaDigests(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM saga_digests WHERE chat_session_id = ? AND (to_turn >= ? OR from_turn >= ?)`, sid, fromTurn, fromTurn)
}
func (s *d1Store) DeleteSessionActiveScopes(ctx context.Context, sid string, _ int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM session_active_scopes WHERE chat_session_id = ?`, sid)
}
func (s *d1Store) DeleteProtagonistEntityMemories(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM protagonist_entity_memories WHERE source_chat_session_id = ? AND source_turn_index >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteConsequenceRecords(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM consequence_records WHERE chat_session_id = ? AND source_turn_end >= ?`, sid, fromTurn)
}
func (s *d1Store) DeletePsychologyBranches(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM psychology_branches WHERE chat_session_id = ? AND source_turn_end >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteThemeOffscreenCarries(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM theme_offscreen_carries WHERE chat_session_id = ? AND source_turn_end >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteCaptureVerificationRecords(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM capture_verification_records WHERE chat_session_id = ? AND turn_index >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteStatusCurrentValues(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM status_current_values WHERE chat_session_id = ? AND source_turn >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteStatusChangeEvents(ctx context.Context, sid string, fromTurn int) error {
	return s.d1RollbackExec(ctx, `DELETE FROM status_change_events WHERE chat_session_id = ? AND source_turn >= ?`, sid, fromTurn)
}
func (s *d1Store) DeleteStatusEffects(ctx context.Context, sid string, fromTurn int) error {
	return s.conn.Batch(ctx,
		D1Statement{SQL: `UPDATE status_effects SET effect_state = 'active', cleared_evidence_json = NULL, cleared_turn = NULL, updated_at = ? WHERE chat_session_id = ? AND cleared_turn >= ?`, Args: []any{d1TimeValue(time.Now().UTC()), sid, fromTurn}},
		D1Statement{SQL: `DELETE FROM status_effects WHERE chat_session_id = ? AND source_turn >= ?`, Args: []any{sid, fromTurn}},
	)
}

// DeleteSession preserves derivation/audit rows while making their source
// revisions permanently inactive.  The vector outbox delete requests remain so
// the separate Vectorize worker can remove corresponding accelerator entries.
func (s *d1Store) DeleteSession(ctx context.Context, sid string) error {
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	rows, err := s.conn.Query(ctx, `SELECT source_revision FROM memory_source_revisions WHERE chat_session_id = ? AND lifecycle_state <> 'deleted' ORDER BY turn_index, id`, sid)
	if err != nil {
		return err
	}
	var revisions []string
	for rows.Next() {
		var revision string
		if err := rows.Scan(&revision); err != nil {
			_ = rows.Close()
			return err
		}
		revisions = append(revisions, revision)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	now := d1TimeValue(time.Now().UTC())
	vectorDeletes, err := s.d1KnownVectorDeletes(ctx, sid, revisions, 1)
	if err != nil {
		return err
	}
	statements := make([]D1Statement, 0, 10+len(revisions)*5+len(vectorDeletes))
	for _, delete := range vectorDeletes {
		statements = append(statements, D1Statement{SQL: `INSERT INTO memory_vector_outbox (
			contract_version, operation_key, operation, chat_session_id, source_revision,
			document_id, document_json, embedding_ready, required_source_state, status,
			attempts, created_at, updated_at
		) VALUES (?, ?, 'delete', ?, ?, ?, ?, 1, 'inactive', 'pending', 0, ?, ?)
		ON CONFLICT (operation_key) DO NOTHING`, Args: []any{
			MemoryVectorOutboxContract,
			memoryVectorOperationKey("delete:inactive", sid, delete.sourceRevision, delete.documentID),
			sid, delete.sourceRevision, delete.documentID, memoryVectorDeleteAuditJSON("session_deleted"), now, now,
		}})
	}
	for _, revision := range revisions {
		statements = append(statements,
			D1Statement{SQL: `UPDATE memory_derivation_dependencies SET lifecycle_state = 'invalidated', invalidated_at = ?, updated_at = ? WHERE chat_session_id = ? AND source_revision = ? AND lifecycle_state = 'active'`, Args: []any{now, now, sid, revision}},
			D1Statement{SQL: `UPDATE precise_memory_units SET lifecycle_state = 'invalidated', evidence_excerpt = '', direct_evidence_ids_json = '[]', payload_json = '{}', relationship_key = NULL, reveal_condition = NULL, updated_at = ? WHERE chat_session_id = ? AND source_revision = ?`, Args: []any{now, sid, revision}},
			D1Statement{SQL: `UPDATE memory_reprocessing_jobs SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL, last_error = ?, updated_at = ? WHERE chat_session_id = ? AND source_revision = ? AND status IN ('pending', 'leased', 'retryable')`, Args: []any{"session_deleted", now, sid, revision}},
			D1Statement{SQL: `UPDATE memory_vector_outbox SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL, document_json = NULL, last_error = ?, updated_at = ? WHERE chat_session_id = ? AND source_revision = ? AND operation = 'upsert' AND status IN ('pending', 'leased', 'retryable', 'needs_embedding')`, Args: []any{"session_deleted", now, sid, revision}},
			D1Statement{SQL: `UPDATE memory_vector_outbox SET document_json = NULL, updated_at = ? WHERE chat_session_id = ? AND source_revision = ? AND operation = 'upsert'`, Args: []any{now, sid, revision}},
			D1Statement{SQL: `UPDATE memory_source_revisions SET lifecycle_state = 'deleted', superseded_by_revision = NULL, invalidation_reason = ?, invalidated_at = ?, updated_at = ?, raw_user_content = '', raw_assistant_content = '', source_message_id = NULL, source_generation_id = NULL, derived_result_json = NULL, critic_input_snapshot_json = NULL, critic_input_snapshot_hash = NULL WHERE chat_session_id = ? AND source_revision = ? AND lifecycle_state <> 'deleted'`, Args: []any{"session_deleted", now, now, sid, revision}},
		)
	}
	statements = append(statements, d1SessionDeleteStatements(sid)...)
	return s.conn.Batch(ctx, statements...)
}

type d1VectorDelete struct {
	documentID     string
	sourceRevision string
}

// d1SessionVectorDeletes is the SQLite/D1 equivalent of MariaDB's known-vector
// discovery.  It runs before the session batch deletes canonical rows, then the
// batch atomically makes the delete operations durable with the invalidation.
func (s *d1Store) d1KnownVectorDeletes(ctx context.Context, sid string, revisions []string, fromTurn int) ([]d1VectorDelete, error) {
	if len(revisions) == 0 {
		return nil, nil
	}
	deletes := make([]d1VectorDelete, 0)
	indexes := make(map[string]int)
	add := func(documentID, sourceRevision string) {
		documentID = strings.TrimSpace(documentID)
		if documentID == "" {
			return
		}
		if index, exists := indexes[documentID]; exists {
			deletes[index].sourceRevision = sourceRevision
			return
		}
		indexes[documentID] = len(deletes)
		deletes = append(deletes, d1VectorDelete{documentID: documentID, sourceRevision: sourceRevision})
	}
	for _, revision := range revisions {
		rows, err := s.conn.Query(ctx, `SELECT DISTINCT document_id FROM memory_vector_outbox WHERE chat_session_id = ? AND source_revision = ? AND operation = 'upsert' AND document_id <> '' ORDER BY document_id`, sid, revision)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var documentID string
			if err := rows.Scan(&documentID); err != nil {
				_ = rows.Close()
				return nil, err
			}
			add(documentID, revision)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	for _, candidate := range []struct{ tier, query string }{
		{"memory", `SELECT id FROM memories WHERE chat_session_id = ? AND turn_index >= ?`},
		{"evidence", `SELECT id FROM direct_evidence_records WHERE chat_session_id = ? AND source_turn_end >= ?`},
		{"world_rule", `SELECT id FROM world_rules WHERE chat_session_id = ? AND source_turn >= ?`},
	} {
		rows, err := s.conn.Query(ctx, candidate.query, sid, fromTurn)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return nil, err
			}
			add(fmt.Sprintf("%s:%s:%d", candidate.tier, sid, id), revisions[len(revisions)-1])
			add(fmt.Sprintf("%s:%d", candidate.tier, id), revisions[len(revisions)-1])
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return deletes, nil
}

func d1SessionDeleteStatements(sid string) []D1Statement {
	// Keep this list in the same dependency order as MariaDB DeleteSession.
	tables := []string{
		"lorebook_reference_entries", "lorebook_reference_snapshots", "lorebook_reference_scopes", "lorebook_reference_session_locks",
		"session_reference_bindings", "persona_capsule_attachments", "protagonist_entity_memories",
		"chat_logs", "effective_input_logs", "memories", "direct_evidence_records", "kg_triples", "character_events", "storylines", "world_rules", "character_states", "pending_threads", "active_states", "canonical_state_layers", "episode_summaries", "chapter_summaries", "arc_summaries", "saga_digests", "session_active_scopes", "guidance_plan_states", "speaker_attributions", "entity_identity_artifact_bindings", "entity_identity_links", "entity_identity_surfaces", "entity_identities", "entities", "trust_states", "consequence_records", "psychology_branches", "session_fork_lineage", "theme_offscreen_carries", "capture_verification_records", "status_effects", "status_change_events", "status_current_values", "status_schema_registry", "status_schema_proposals", "critic_feedback",
	}
	statements := make([]D1Statement, 0, len(tables)+2)
	// The first two child tables use scope_id rather than chat_session_id.
	statements = append(statements,
		D1Statement{SQL: `DELETE FROM lorebook_reference_entries WHERE scope_id IN (SELECT scope_id FROM lorebook_reference_scopes WHERE chat_session_id = ?)`, Args: []any{sid}},
		D1Statement{SQL: `DELETE FROM lorebook_reference_snapshots WHERE scope_id IN (SELECT scope_id FROM lorebook_reference_scopes WHERE chat_session_id = ?)`, Args: []any{sid}},
	)
	for _, table := range tables {
		if table == "lorebook_reference_entries" || table == "lorebook_reference_snapshots" || table == "persona_capsule_attachments" || table == "protagonist_entity_memories" {
			continue
		}
		statements = append(statements, D1Statement{SQL: fmt.Sprintf(`DELETE FROM %s WHERE chat_session_id = ?`, d1QuoteIdent(table)), Args: []any{sid}})
	}
	statements = append(statements,
		D1Statement{SQL: `DELETE FROM persona_capsule_attachments WHERE target_chat_session_id = ?`, Args: []any{sid}},
		D1Statement{SQL: `DELETE FROM protagonist_entity_memories WHERE source_chat_session_id = ?`, Args: []any{sid}},
	)
	return statements
}
