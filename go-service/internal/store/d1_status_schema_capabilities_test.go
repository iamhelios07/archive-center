package store

import (
	"context"
	"errors"
	"testing"
)

// TestD1ListStatusCurrentValuesLimitSemantics pins the limit rule that a careless
// translation gets wrong: -1 means the complete projection, any other
// non-positive value falls back to 100, and an oversized request is capped at
// 1000. Only a positive limit emits a LIMIT clause.
func TestD1ListStatusCurrentValuesLimitSemantics(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registryID := d1SeedStatusRegistry(t, conn, "s1", "hp")

	// Seed more rows than the default limit so unbounded and defaulted reads are
	// distinguishable.
	const seeded = 120
	stmts := make([]D1Statement, 0, seeded)
	for i := 0; i < seeded; i++ {
		stmts = append(stmts, D1Statement{
			SQL: `INSERT INTO status_current_values
				(chat_session_id, registry_id, status_key, owner_scope, owner_id, value_kind, value_json, evidence_json, write_state)
				VALUES ('s1', ?, 'hp', 'character', ?, 'number', '1', '{}', 'current')`,
			Args: []any{registryID, d1OwnerID(i)},
		})
	}
	if err := conn.Batch(ctx, stmts...); err != nil {
		t.Fatalf("seed %d current values: %v", seeded, err)
	}

	unbounded, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "", -1)
	if err != nil {
		t.Fatalf("limit -1: %v", err)
	}
	if len(unbounded) != seeded {
		t.Errorf("limit -1 returned %d rows, want all %d", len(unbounded), seeded)
	}

	defaulted, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "", 0)
	if err != nil {
		t.Fatalf("limit 0: %v", err)
	}
	if len(defaulted) != 100 {
		t.Errorf("limit 0 returned %d rows, want the bounded default of 100", len(defaulted))
	}

	negative, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "", -5)
	if err != nil {
		t.Fatalf("limit -5: %v", err)
	}
	if len(negative) != 100 {
		t.Errorf("limit -5 returned %d rows, want the bounded default of 100", len(negative))
	}

	capped, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "", 5000)
	if err != nil {
		t.Fatalf("limit 5000: %v", err)
	}
	if len(capped) != seeded {
		t.Errorf("limit 5000 returned %d rows, want the %d seeded rows (the cap must not exceed the data)", len(capped), seeded)
	}

	small, err := st.ListStatusCurrentValues(ctx, "s1", "", "", "", 7)
	if err != nil {
		t.Fatalf("limit 7: %v", err)
	}
	if len(small) != 7 {
		t.Errorf("limit 7 returned %d rows, want 7", len(small))
	}
}

// d1OwnerID renders a stable, sortable owner id.
func d1OwnerID(i int) string {
	return "owner-" + string(rune('a'+i%26)) + "-" + itoaPadded(i)
}

func itoaPadded(i int) string {
	digits := []byte{}
	if i == 0 {
		return "000"
	}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	for len(digits) < 3 {
		digits = append([]byte{'0'}, digits...)
	}
	return string(digits)
}

func TestD1StatusSchemaProposalStore(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	saved, err := st.SaveStatusSchemaProposal(ctx, StatusSchemaProposal{
		ChatSessionID: "s1", SchemaJSON: `{"keys":["hp"]}`,
	})
	if err != nil {
		t.Fatalf("SaveStatusSchemaProposal: %v", err)
	}
	if saved.ID == 0 {
		t.Fatal("save must report an id")
	}
	// Defaults mirror the MariaDB path.
	if saved.InputChannel != "bootstrap" || saved.ProposalState != "pending_review" || saved.SchemaName != "status_schema" {
		t.Errorf("defaults = %+v, want bootstrap/pending_review/status_schema", saved)
	}

	second, err := st.SaveStatusSchemaProposal(ctx, StatusSchemaProposal{
		ChatSessionID: "s1", InputChannel: "operator", ProposalState: "approved", SchemaName: "other",
	})
	if err != nil {
		t.Fatalf("second save: %v", err)
	}
	if _, err := st.SaveStatusSchemaProposal(ctx, StatusSchemaProposal{ChatSessionID: "s2"}); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	all, err := st.ListStatusSchemaProposals(ctx, "s1", "", 10)
	if err != nil {
		t.Fatalf("ListStatusSchemaProposals: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("proposals = %d, want 2 (session isolation)", len(all))
	}
	approved, err := st.ListStatusSchemaProposals(ctx, "s1", "approved", 10)
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	if len(approved) != 1 || approved[0].ID != second.ID {
		t.Errorf("state filter = %+v, want only the approved proposal", approved)
	}

	fetched, err := st.GetStatusSchemaProposal(ctx, saved.ID)
	if err != nil {
		t.Fatalf("GetStatusSchemaProposal: %v", err)
	}
	if fetched.SchemaJSON != `{"keys":["hp"]}` {
		t.Errorf("schema_json = %q", fetched.SchemaJSON)
	}
	if !fetched.ReviewedAt.IsZero() {
		t.Errorf("an unreviewed proposal must read a zero reviewed_at, got %v", fetched.ReviewedAt)
	}
	if _, err := st.GetStatusSchemaProposal(ctx, 999999); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing proposal = %v, want ErrNotFound", err)
	}

	if err := st.UpdateStatusSchemaProposalReview(ctx, saved.ID, "approved", "looks good", "operator"); err != nil {
		t.Fatalf("UpdateStatusSchemaProposalReview: %v", err)
	}
	reviewed, err := st.GetStatusSchemaProposal(ctx, saved.ID)
	if err != nil {
		t.Fatalf("read reviewed: %v", err)
	}
	if reviewed.ProposalState != "approved" || reviewed.ReviewNote != "looks good" || reviewed.Reviewer != "operator" {
		t.Errorf("review = %+v", reviewed)
	}
	if reviewed.ReviewedAt.IsZero() {
		t.Error("a review must stamp reviewed_at")
	}

	if err := st.UpdateStatusSchemaProposalReview(ctx, saved.ID, "  ", "", ""); err == nil {
		t.Error("an empty proposal_state must be rejected")
	}
	if err := st.UpdateStatusSchemaProposalReview(ctx, 999999, "approved", "", ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing proposal = %v, want ErrNotFound", err)
	}
}

func TestD1StatusSchemaRegistryStore(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	defs := []StatusSchemaDefinition{
		{ChatSessionID: "s1", StatusKey: "hp", OwnerScope: "character", ValueKind: "number"},
		{ChatSessionID: "s1", SchemaName: "other", StatusKey: "mp", OwnerScope: "character", ValueKind: "number", RegistryState: "retired", Label: "Mana"},
	}
	saved, err := st.SaveStatusSchemaDefinitions(ctx, defs)
	if err != nil {
		t.Fatalf("SaveStatusSchemaDefinitions: %v", err)
	}
	if len(saved) != 2 {
		t.Fatalf("saved = %d, want 2", len(saved))
	}
	// Defaults: schema_name falls back, label falls back to status_key, state to active.
	if saved[0].SchemaName != "status_schema" || saved[0].Label != "hp" || saved[0].RegistryState != "active" {
		t.Errorf("defaults = %+v", saved[0])
	}
	if saved[1].Label != "Mana" || saved[1].RegistryState != "retired" {
		t.Errorf("explicit values must be preserved: %+v", saved[1])
	}
	if saved[0].ID == 0 || saved[1].ID == 0 {
		t.Error("each definition must report an id")
	}

	all, err := st.ListStatusSchemaDefinitions(ctx, "s1", "", 0)
	if err != nil {
		t.Fatalf("ListStatusSchemaDefinitions: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("definitions = %d, want 2", len(all))
	}
	active, err := st.ListStatusSchemaDefinitions(ctx, "s1", "active", 0)
	if err != nil {
		t.Fatalf("active filter: %v", err)
	}
	if len(active) != 1 || active[0].StatusKey != "hp" {
		t.Errorf("active definitions = %+v, want only hp", active)
	}

	// Only an active definition resolves by key.
	found, err := st.GetStatusSchemaDefinitionByKey(ctx, "s1", "hp", "character")
	if err != nil {
		t.Fatalf("GetStatusSchemaDefinitionByKey: %v", err)
	}
	if found.Label != "hp" {
		t.Errorf("resolved label = %q, want hp", found.Label)
	}
	if _, err := st.GetStatusSchemaDefinitionByKey(ctx, "s1", "mp", "character"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a retired definition must not resolve, got %v", err)
	}
	if _, err := st.GetStatusSchemaDefinitionByKey(ctx, "s1", "absent", "character"); !errors.Is(err, ErrNotFound) {
		t.Errorf("an unknown key = %v, want ErrNotFound", err)
	}

	// A duplicate of the same unique tuple fails partway; the batch reports the
	// rows it already accepted instead of discarding them.
	partial, err := st.SaveStatusSchemaDefinitions(ctx, []StatusSchemaDefinition{
		{ChatSessionID: "s1", StatusKey: "sp", OwnerScope: "character", ValueKind: "number"},
		{ChatSessionID: "s1", StatusKey: "hp", OwnerScope: "character", ValueKind: "number"},
	})
	if err == nil {
		t.Error("a duplicate registry tuple must be rejected")
	}
	if len(partial) != 1 || partial[0].StatusKey != "sp" {
		t.Errorf("partial result = %+v, want the one definition that succeeded", partial)
	}
	if got := d1Count(t, conn2(t, st), `SELECT COUNT(*) FROM status_schema_registry`); got != 3 {
		t.Errorf("registry rows = %d, want 3 (two initial plus the accepted partial)", got)
	}
}

// conn2 exposes the transport behind a test store so a count can be asserted.
func conn2(t *testing.T, st *d1Store) *sqliteD1Conn {
	t.Helper()
	return st.conn.(*sqliteD1Conn)
}
