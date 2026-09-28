package store

import (
	"context"
	"testing"
)

// d1SeedSourceRevision inserts a minimal valid memory_source_revisions row so the
// eligibility join has a target.
func d1SeedSourceRevision(t *testing.T, conn *sqliteD1Conn, sessionID, revision, lifecycle string) {
	t.Helper()
	ctx := context.Background()
	if _, err := conn.Exec(ctx, `INSERT INTO memory_source_revisions (
		source_revision, chat_session_id, logical_turn_id, turn_index,
		raw_user_content, raw_assistant_content, combined_content_hash,
		hash_algorithm, host_observed_at_ms, lifecycle_state
	) VALUES (?, ?, ?, 1, 'u', 'a', 'hash', 'sha256', 1, ?)`,
		revision, sessionID, "turn-1", lifecycle); err != nil {
		t.Fatalf("seed source revision %s: %v", revision, err)
	}
}

// d1SeedStatusRegistry inserts the registry row a current value must reference.
func d1SeedStatusRegistry(t *testing.T, conn *sqliteD1Conn, sessionID, statusKey string) int64 {
	t.Helper()
	ctx := context.Background()
	var id int64
	if err := conn.QueryRow(ctx, `INSERT INTO status_schema_registry
		(chat_session_id, status_key, label, owner_scope, value_kind)
		VALUES (?, ?, ?, 'character', 'number')
		RETURNING id`, sessionID, statusKey, statusKey).Scan(&id); err != nil {
		t.Fatalf("seed status registry %s: %v", statusKey, err)
	}
	return id
}

func d1SeedCurrentValue(t *testing.T, conn *sqliteD1Conn, sessionID string, registryID int64, ownerID, statusKey, evidence, writeState string) int64 {
	t.Helper()
	ctx := context.Background()
	var id int64
	if _, err := conn.Exec(ctx, `INSERT INTO status_current_values
		(chat_session_id, registry_id, status_key, owner_scope, owner_id, value_kind, value_json, evidence_json, write_state)
		VALUES (?, ?, ?, 'character', ?, 'number', '1', ?, ?)`,
		sessionID, registryID, statusKey, ownerID, evidence, writeState); err != nil {
		t.Fatalf("seed current value %s/%s: %v", ownerID, statusKey, err)
	}
	if err := conn.QueryRow(ctx, `SELECT id FROM status_current_values WHERE chat_session_id = ? AND owner_id = ? AND status_key = ?`,
		sessionID, ownerID, statusKey).Scan(&id); err != nil {
		t.Fatalf("read seeded id: %v", err)
	}
	return id
}

// TestD1ListStatusCurrentValuesEligibility pins the current-projection domain
// rule: a materialised current row is eligible only when it names no source
// revision, or names one that resolves to an active revision.
func TestD1ListStatusCurrentValuesEligibility(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	registryID := d1SeedStatusRegistry(t, conn, "s1", "hp")
	d1SeedSourceRevision(t, conn, "s1", "rev-active", "active")
	d1SeedSourceRevision(t, conn, "s1", "rev-superseded", "superseded")

	// Eligible: no revision named (a state_repair-style correction).
	d1SeedCurrentValue(t, conn, "s1", registryID, "no-revision", "hp", `{}`, "current")
	// Eligible: names an active revision.
	d1SeedCurrentValue(t, conn, "s1", registryID, "active-revision", "hp", `{"source_revision":"rev-active"}`, "current")
	// Eligible: an explicit empty revision is treated as "none named".
	d1SeedCurrentValue(t, conn, "s1", registryID, "empty-revision", "hp", `{"source_revision":""}`, "current")
	// Not eligible: the named revision is superseded.
	d1SeedCurrentValue(t, conn, "s1", registryID, "superseded-revision", "hp", `{"source_revision":"rev-superseded"}`, "current")
	// Not eligible: the named revision does not exist.
	d1SeedCurrentValue(t, conn, "s1", registryID, "unknown-revision", "hp", `{"source_revision":"rev-missing"}`, "current")
	// Not eligible: the row is not the materialised current state.
	d1SeedCurrentValue(t, conn, "s1", registryID, "not-current", "hp", `{"source_revision":"rev-active"}`, "superseded")
	// Not eligible: a non-string revision must not be coerced into a match.
	d1SeedCurrentValue(t, conn, "s1", registryID, "numeric-revision", "hp", `{"source_revision":123}`, "current")
	// Another session must not contribute.
	d1SeedCurrentValue(t, conn, "s2", registryID, "other-session", "hp", `{}`, "current")

	values, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "", 0)
	if err != nil {
		t.Fatalf("ListStatusCurrentValues: %v", err)
	}

	eligible := map[string]bool{}
	for _, value := range values {
		eligible[value.OwnerID] = true
	}
	for _, want := range []string{"no-revision", "active-revision", "empty-revision"} {
		if !eligible[want] {
			t.Errorf("%s must be eligible; got %v", want, eligible)
		}
	}
	for _, unwanted := range []string{"superseded-revision", "unknown-revision", "not-current", "numeric-revision", "other-session"} {
		if eligible[unwanted] {
			t.Errorf("%s must not be eligible; got %v", unwanted, eligible)
		}
	}
	if len(values) != 3 {
		t.Errorf("eligible rows = %d, want exactly 3: %v", len(values), eligible)
	}
}

func TestD1ListStatusCurrentValuesFiltersAndOrder(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	hpRegistry := d1SeedStatusRegistry(t, conn, "s1", "hp")
	mpRegistry := d1SeedStatusRegistry(t, conn, "s1", "mp")

	d1SeedCurrentValue(t, conn, "s1", hpRegistry, "bob", "hp", `{}`, "current")
	d1SeedCurrentValue(t, conn, "s1", hpRegistry, "alice", "hp", `{}`, "current")
	d1SeedCurrentValue(t, conn, "s1", mpRegistry, "alice", "mp", `{}`, "current")

	all, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "", 0)
	if err != nil {
		t.Fatalf("ListStatusCurrentValues: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("rows = %d, want 3", len(all))
	}
	// owner_scope, then owner_id, then status_key.
	if all[0].OwnerID != "alice" || all[0].StatusKey != "hp" {
		t.Errorf("first row = %s/%s, want alice/hp", all[0].OwnerID, all[0].StatusKey)
	}

	byOwner, err := st.ListStatusCurrentValues(ctx, "s1", "", "bob", "", 0)
	if err != nil {
		t.Fatalf("filter by owner: %v", err)
	}
	if len(byOwner) != 1 || byOwner[0].OwnerID != "bob" {
		t.Errorf("owner filter = %+v", byOwner)
	}

	byKey, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "mp", 0)
	if err != nil {
		t.Fatalf("filter by status key: %v", err)
	}
	if len(byKey) != 1 || byKey[0].StatusKey != "mp" {
		t.Errorf("status key filter = %+v", byKey)
	}

	byScope, err := st.ListStatusCurrentValues(ctx, "s1", "character", "", "", 0)
	if err != nil {
		t.Fatalf("filter by owner scope: %v", err)
	}
	if len(byScope) != 3 {
		t.Errorf("owner scope filter = %d rows, want 3", len(byScope))
	}
	none, err := st.ListStatusCurrentValues(ctx, "s1", "absent-scope", "", "", 0)
	if err != nil {
		t.Fatalf("filter by absent scope: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("absent scope filter = %d rows, want 0", len(none))
	}

	limited, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "", 1)
	if err != nil {
		t.Fatalf("limited read: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("limit = %d rows, want 1", len(limited))
	}
}

// TestD1SaveStatusCurrentValueUpsert verifies the MariaDB upsert contract: a
// repeat write of the same (session, registry, scope, owner) key updates the
// existing row and reports its id rather than inserting a duplicate.
func TestD1SaveStatusCurrentValueUpsert(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	registryID := d1SeedStatusRegistry(t, conn, "s1", "hp")

	first, err := st.SaveStatusCurrentValue(ctx, StatusCurrentValue{
		ChatSessionID: "s1", RegistryID: registryID, StatusKey: "hp", OwnerScope: "character",
		OwnerID: "alice", ValueKind: "number", ValueJSON: "10", EvidenceJSON: `{}`,
	})
	if err != nil {
		t.Fatalf("SaveStatusCurrentValue: %v", err)
	}
	if first.ID == 0 {
		t.Fatal("the first save must report an id")
	}
	if first.WriteState != "current" {
		t.Errorf("write_state = %q, want the default current", first.WriteState)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_current_values`); got != 1 {
		t.Fatalf("rows = %d, want 1", got)
	}

	second, err := st.SaveStatusCurrentValue(ctx, StatusCurrentValue{
		ChatSessionID: "s1", RegistryID: registryID, StatusKey: "hp", OwnerScope: "character",
		OwnerID: "alice", ValueKind: "number", ValueJSON: "7", EvidenceJSON: `{"source_revision":"rev-1"}`,
	})
	if err != nil {
		t.Fatalf("second SaveStatusCurrentValue: %v", err)
	}
	if second.ID != first.ID {
		t.Errorf("upsert id = %d, want the existing id %d", second.ID, first.ID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_current_values`); got != 1 {
		t.Errorf("rows = %d after an upsert, want 1 (no duplicate)", got)
	}

	var valueJSON, evidenceJSON string
	if err := conn.QueryRow(ctx, `SELECT value_json, evidence_json FROM status_current_values WHERE id = ?`, first.ID).
		Scan(&valueJSON, &evidenceJSON); err != nil {
		t.Fatalf("read updated row: %v", err)
	}
	if valueJSON != "7" {
		t.Errorf("value_json = %q, want the updated 7", valueJSON)
	}
	if evidenceJSON != `{"source_revision":"rev-1"}` {
		t.Errorf("evidence_json = %q, want the updated evidence", evidenceJSON)
	}

	// source_turn 0 must be stored as NULL, matching MariaDB's NULLIF(?, 0).
	var sourceTurn *int64
	if err := conn.QueryRow(ctx, `SELECT source_turn FROM status_current_values WHERE id = ?`, first.ID).
		Scan(&sourceTurn); err != nil {
		t.Fatalf("read source_turn: %v", err)
	}
	if sourceTurn != nil {
		t.Errorf("source_turn = %d, want NULL for a zero turn", *sourceTurn)
	}

	// A non-zero source turn must be stored, and a zero registry reference must be
	// rejected by the schema's foreign key rather than silently accepted.
	turned, err := st.SaveStatusCurrentValue(ctx, StatusCurrentValue{
		ChatSessionID: "s1", RegistryID: registryID, StatusKey: "hp", OwnerScope: "character",
		OwnerID: "carol", ValueKind: "number", ValueJSON: "3", EvidenceJSON: `{}`, SourceTurn: 12,
	})
	if err != nil {
		t.Fatalf("save with a source turn: %v", err)
	}
	var storedTurn int64
	if err := conn.QueryRow(ctx, `SELECT source_turn FROM status_current_values WHERE id = ?`, turned.ID).Scan(&storedTurn); err != nil {
		t.Fatalf("read stored turn: %v", err)
	}
	if storedTurn != 12 {
		t.Errorf("source_turn = %d, want 12", storedTurn)
	}

	if _, err := st.SaveStatusCurrentValue(ctx, StatusCurrentValue{
		ChatSessionID: "s1", RegistryID: 999999, StatusKey: "hp", OwnerScope: "character",
		OwnerID: "dangling", ValueKind: "number", ValueJSON: "1", EvidenceJSON: `{}`,
	}); err == nil {
		t.Error("a registry reference that does not exist must be rejected by the foreign key")
	}
}

// TestD1SavedValueParticipatesInEligibility ties the write and read paths: a row
// saved against an active revision must be readable, and one saved against a
// superseded revision must not be.
func TestD1SavedValueParticipatesInEligibility(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	registryID := d1SeedStatusRegistry(t, conn, "s1", "hp")
	d1SeedSourceRevision(t, conn, "s1", "rev-live", "active")
	d1SeedSourceRevision(t, conn, "s1", "rev-dead", "superseded")

	if _, err := st.SaveStatusCurrentValue(ctx, StatusCurrentValue{
		ChatSessionID: "s1", RegistryID: registryID, StatusKey: "hp", OwnerScope: "character",
		OwnerID: "live", ValueKind: "number", ValueJSON: "1", EvidenceJSON: `{"source_revision":"rev-live"}`,
	}); err != nil {
		t.Fatalf("save live: %v", err)
	}
	if _, err := st.SaveStatusCurrentValue(ctx, StatusCurrentValue{
		ChatSessionID: "s1", RegistryID: registryID, StatusKey: "hp", OwnerScope: "character",
		OwnerID: "dead", ValueKind: "number", ValueJSON: "1", EvidenceJSON: `{"source_revision":"rev-dead"}`,
	}); err != nil {
		t.Fatalf("save dead: %v", err)
	}

	values, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "", 0)
	if err != nil {
		t.Fatalf("ListStatusCurrentValues: %v", err)
	}
	if len(values) != 1 || values[0].OwnerID != "live" {
		t.Errorf("eligible after save = %+v, want only the live-revision row", values)
	}

	// A superseded revision must also not be reachable by name.
	named, err := st.ListStatusCurrentValues(ctx, "s1", "", "dead", "", 0)
	if err != nil {
		t.Fatalf("named read: %v", err)
	}
	if len(named) != 0 {
		t.Errorf("a superseded revision must stay ineligible: %+v", named)
	}
}
