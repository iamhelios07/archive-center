package store

import (
	"context"
	"testing"
)

// Every optional rollback entry point is executed against the canonical SQLite
// schema.  This guards the easily missed dialect/schema drift in a long list of
// otherwise simple predicates (D1 is SQLite, not MariaDB).
func TestD1RollbackStoreAllOperationsExecute(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	const sid = "empty-session"
	operations := []struct {
		name string
		call func() error
	}{
		{"chat", func() error { return st.DeleteChatLogs(ctx, sid, 4) }},
		{"effective", func() error { return st.DeleteEffectiveInputs(ctx, sid, 4) }},
		{"memory", func() error { return st.DeleteMemories(ctx, sid, 4) }},
		{"evidence", func() error { return st.DeleteEvidence(ctx, sid, 4) }},
		{"kg", func() error { return st.DeleteKGTriples(ctx, sid, 4) }},
		{"critic", func() error { return st.DeleteCriticFeedback(ctx, sid, 4) }},
		{"events", func() error { return st.DeleteCharacterEvents(ctx, sid, 4) }},
		{"entities", func() error { return st.DeleteEntities(ctx, sid, 4) }},
		{"trust", func() error { return st.DeleteTrustStates(ctx, sid, 4) }},
		{"storyline", func() error { return st.DeleteStorylines(ctx, sid, 4) }},
		{"rules", func() error { return st.DeleteWorldRules(ctx, sid, 4) }},
		{"character state", func() error { return st.DeleteCharacterStates(ctx, sid, 4) }},
		{"threads", func() error { return st.DeletePendingThreads(ctx, sid, 4) }},
		{"active state", func() error { return st.DeleteActiveStates(ctx, sid, 4) }},
		{"canonical", func() error { return st.DeleteCanonicalStateLayers(ctx, sid, 4) }},
		{"episode", func() error { return st.DeleteEpisodeSummaries(ctx, sid, 4) }},
		{"guidance", func() error { return st.DeleteGuidancePlanState(ctx, sid, 4) }},
		{"chapter", func() error { return st.DeleteChapterSummaries(ctx, sid, 4) }},
		{"arc", func() error { return st.DeleteArcSummaries(ctx, sid, 4) }},
		{"saga", func() error { return st.DeleteSagaDigests(ctx, sid, 4) }},
		{"scope", func() error { return st.DeleteSessionActiveScopes(ctx, sid, 4) }},
		{"protagonist", func() error { return st.DeleteProtagonistEntityMemories(ctx, sid, 4) }},
		{"consequence", func() error { return st.DeleteConsequenceRecords(ctx, sid, 4) }},
		{"psychology", func() error { return st.DeletePsychologyBranches(ctx, sid, 4) }},
		{"theme", func() error { return st.DeleteThemeOffscreenCarries(ctx, sid, 4) }},
		{"capture", func() error { return st.DeleteCaptureVerificationRecords(ctx, sid, 4) }},
		{"status current", func() error { return st.DeleteStatusCurrentValues(ctx, sid, 4) }},
		{"status changes", func() error { return st.DeleteStatusChangeEvents(ctx, sid, 4) }},
		{"status effects", func() error { return st.DeleteStatusEffects(ctx, sid, 4) }},
		{"session", func() error { return st.DeleteSession(ctx, sid) }},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.call(); err != nil {
				t.Fatalf("%s: %v", operation.name, err)
			}
		})
	}
}

func TestD1RollbackTailAndEffectSemantics(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	if err := conn.Batch(ctx,
		D1Statement{SQL: `INSERT INTO chat_logs (chat_session_id, turn_index, role, content) VALUES ('s1', 3, 'user', 'old'), ('s1', 4, 'user', 'tail'), ('other', 4, 'user', 'other')`},
		D1Statement{SQL: `INSERT INTO effective_input_logs (chat_session_id, turn_index, effective_input) VALUES ('s1', 3, 'old'), ('s1', 4, 'tail')`},
		D1Statement{SQL: `INSERT INTO direct_evidence_records (chat_session_id, evidence_text, source_turn_start, source_turn_end) VALUES ('s1', 'old', 2, 3), ('s1', 'overlap', 3, 4)`},
	); err != nil {
		t.Fatalf("seed tail records: %v", err)
	}
	if err := st.DeleteChatLogs(ctx, "s1", 4); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteEffectiveInputs(ctx, "s1", 4); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteEvidence(ctx, "s1", 4); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		query string
		want  int
	}{
		{`SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 's1'`, 1},
		{`SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'other'`, 1},
		{`SELECT COUNT(*) FROM effective_input_logs WHERE chat_session_id = 's1'`, 1},
		{`SELECT COUNT(*) FROM direct_evidence_records WHERE chat_session_id = 's1'`, 1},
	} {
		var got int
		if err := conn.QueryRow(ctx, check.query).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != check.want {
			t.Errorf("%s = %d, want %d", check.query, got, check.want)
		}
	}

	registryID := d1SeedStatusRegistry(t, conn, "s1", "hp")
	if _, err := conn.Exec(ctx, `INSERT INTO status_effects (chat_session_id, registry_id, status_key, owner_scope, owner_id, effect_kind, evidence_json, start_clock_json, source_turn, cleared_turn, effect_state, cleared_evidence_json) VALUES ('s1', ?, 'hp', 'character', 'old', 'buff', '{}', '{}', 2, 4, 'cleared', '{"why":"tail"}'), ('s1', ?, 'hp', 'character', 'tail', 'buff', '{}', '{}', 4, NULL, 'active', NULL)`, registryID, registryID); err != nil {
		t.Fatalf("seed effects: %v", err)
	}
	if err := st.DeleteStatusEffects(ctx, "s1", 4); err != nil {
		t.Fatalf("delete effects: %v", err)
	}
	var state string
	var clearedTurn *int64
	if err := conn.QueryRow(ctx, `SELECT effect_state, cleared_turn FROM status_effects WHERE chat_session_id = 's1' AND owner_id = 'old'`).Scan(&state, &clearedTurn); err != nil {
		t.Fatal(err)
	}
	if state != "active" || clearedTurn != nil {
		t.Errorf("reopened effect = %q/%v, want active/NULL", state, clearedTurn)
	}
	var tailCount int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM status_effects WHERE chat_session_id = 's1' AND owner_id = 'tail'`).Scan(&tailCount); err != nil {
		t.Fatal(err)
	}
	if tailCount != 0 {
		t.Errorf("tail effect count = %d, want 0", tailCount)
	}
}

func TestD1DeleteEntitiesPreservesPriorIdentityAndRewindsRanges(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	if err := conn.Batch(ctx,
		D1Statement{SQL: `INSERT INTO entities (chat_session_id, name, first_seen_turn, last_seen_turn) VALUES ('s1', 'old', 1, 7), ('s1', 'tail', 4, 4)`},
		D1Statement{SQL: `INSERT INTO entity_identities (stable_entity_id, chat_session_id, identity_namespace, entity_kind, canonical_label, source_contract, source_revision, source_content_hash, source_turn, source_index, idempotency_key, first_seen_turn, last_seen_turn) VALUES ('old-id', 's1', 'main', 'character', 'old', 'test', 'rev-1', 'hash', 1, 0, 'old-key', 1, 7), ('tail-id', 's1', 'main', 'character', 'tail', 'test', 'rev-4', 'hash', 4, 0, 'tail-key', 4, 4)`},
		D1Statement{SQL: `INSERT INTO entity_identity_surfaces (surface_id, stable_entity_id, chat_session_id, identity_namespace, surface_kind, surface_text, normalized_surface, valid_from_turn, source_contract, source_revision, source_turn, idempotency_key) VALUES ('old-tail-surface', 'old-id', 's1', 'main', 'name', 'old', 'old', 4, 'test', 'rev-4', 4, 'surface-old'), ('tail-surface', 'tail-id', 's1', 'main', 'name', 'tail', 'tail', 4, 'test', 'rev-4', 4, 'surface-tail')`},
		D1Statement{SQL: `INSERT INTO entity_identity_links (link_id, chat_session_id, source_entity_id, target_entity_id, link_kind, evidence_json) VALUES ('old-to-tail', 's1', 'old-id', 'tail-id', 'alias', '{}')`},
	); err != nil {
		t.Fatalf("seed entities: %v", err)
	}
	if err := st.DeleteEntities(ctx, "s1", 4); err != nil {
		t.Fatalf("DeleteEntities: %v", err)
	}
	var oldLast, oldIdentityCount, tailIdentityCount, tailEntityCount, linkCount int
	if err := conn.QueryRow(ctx, `SELECT last_seen_turn FROM entities WHERE chat_session_id = 's1' AND name = 'old'`).Scan(&oldLast); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM entity_identities WHERE stable_entity_id = 'old-id'`).Scan(&oldIdentityCount); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM entity_identities WHERE stable_entity_id = 'tail-id'`).Scan(&tailIdentityCount); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM entities WHERE chat_session_id = 's1' AND name = 'tail'`).Scan(&tailEntityCount); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM entity_identity_links WHERE chat_session_id = 's1'`).Scan(&linkCount); err != nil {
		t.Fatal(err)
	}
	if oldLast != 3 || oldIdentityCount != 1 || tailIdentityCount != 0 || tailEntityCount != 0 || linkCount != 0 {
		t.Errorf("entity rollback oldLast=%d old/tail identities=%d/%d tail entities=%d links=%d", oldLast, oldIdentityCount, tailIdentityCount, tailEntityCount, linkCount)
	}
}

func TestD1DeleteSessionInvalidatesSourcesAndQueuesVectorDeletes(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSourceRevision(t, conn, "s1", "rev-1", "active")
	if err := conn.Batch(ctx,
		D1Statement{SQL: `INSERT INTO memories (chat_session_id, turn_index) VALUES ('s1', 1)`},
		D1Statement{SQL: `INSERT INTO memory_vector_outbox (operation_key, operation, chat_session_id, source_revision, document_id, document_json, embedding_ready, required_source_state, status) VALUES ('upsert-key', 'upsert', 's1', 'rev-1', 'existing-doc', '{}', 1, 'active', 'completed')`},
	); err != nil {
		t.Fatalf("seed session rows: %v", err)
	}
	if err := st.DeleteSession(ctx, "s1"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	var lifecycle, rawUser, rawAssistant string
	if err := conn.QueryRow(ctx, `SELECT lifecycle_state, raw_user_content, raw_assistant_content FROM memory_source_revisions WHERE source_revision = 'rev-1'`).Scan(&lifecycle, &rawUser, &rawAssistant); err != nil {
		t.Fatal(err)
	}
	if lifecycle != "deleted" || rawUser != "" || rawAssistant != "" {
		t.Errorf("source after delete = %q/%q/%q, want deleted and scrubbed", lifecycle, rawUser, rawAssistant)
	}
	var memoryCount, deleteCount int
	var completedUpsertDoc *string
	if err := conn.QueryRow(ctx, `SELECT document_json FROM memory_vector_outbox WHERE operation_key = 'upsert-key'`).Scan(&completedUpsertDoc); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM memories WHERE chat_session_id = 's1'`).Scan(&memoryCount); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM memory_vector_outbox WHERE chat_session_id = 's1' AND operation = 'delete' AND status = 'pending' AND required_source_state = 'inactive'`).Scan(&deleteCount); err != nil {
		t.Fatal(err)
	}
	if memoryCount != 0 {
		t.Errorf("session memories = %d, want 0", memoryCount)
	}
	if completedUpsertDoc != nil {
		t.Errorf("completed upsert document_json = %q, want scrubbed NULL", *completedUpsertDoc)
	}
	// Existing document plus session-qualified and legacy document identities.
	if deleteCount != 3 {
		t.Errorf("delete outbox rows = %d, want 3", deleteCount)
	}
}
