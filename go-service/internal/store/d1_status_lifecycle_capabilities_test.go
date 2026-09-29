package store

import (
	"context"
	"errors"
	"testing"
)

// TestD1StatusLifecycleListLimitRules pins the two different limit rules that a
// well-meaning unification would break: the change-event ledger treats -1 as
// "everything", while the effect ledger has no unbounded mode and clamps -1 to
// its bounded default.
func TestD1StatusLifecycleListLimitRules(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registryID := d1SeedStatusRegistry(t, conn, "s1", "hp")

	const seeded = 105
	eventStmts := make([]D1Statement, 0, seeded)
	effectStmts := make([]D1Statement, 0, seeded)
	for i := 0; i < seeded; i++ {
		eventStmts = append(eventStmts, D1Statement{
			SQL: `INSERT INTO status_change_events
				(chat_session_id, registry_id, status_key, owner_scope, owner_id, event_kind, evidence_json, event_state)
				VALUES ('s1', ?, 'hp', 'character', ?, 'set', '{}', 'recorded')`,
			Args: []any{registryID, d1OwnerID(i)},
		})
		effectStmts = append(effectStmts, D1Statement{
			SQL: `INSERT INTO status_effects
				(chat_session_id, registry_id, status_key, owner_scope, owner_id, effect_kind, evidence_json, start_clock_json, effect_state)
				VALUES ('s1', ?, 'hp', 'character', ?, 'buff', '{}', '{}', 'active')`,
			Args: []any{registryID, d1OwnerID(i)},
		})
	}
	if err := conn.Batch(ctx, eventStmts...); err != nil {
		t.Fatalf("seed change events: %v", err)
	}
	if err := conn.Batch(ctx, effectStmts...); err != nil {
		t.Fatalf("seed effects: %v", err)
	}

	// Change events: -1 is the complete ledger.
	allEvents, err := st.ListStatusChangeEvents(ctx, "s1", "", "", "", -1)
	if err != nil {
		t.Fatalf("events limit -1: %v", err)
	}
	if len(allEvents) != seeded {
		t.Errorf("change events with -1 = %d, want all %d", len(allEvents), seeded)
	}
	defaultEvents, err := st.ListStatusChangeEvents(ctx, "s1", "", "", "", 0)
	if err != nil {
		t.Fatalf("events limit 0: %v", err)
	}
	if len(defaultEvents) != 100 {
		t.Errorf("change events with 0 = %d, want the bounded default 100", len(defaultEvents))
	}
	negativeEvents, err := st.ListStatusChangeEvents(ctx, "s1", "", "", "", -9)
	if err != nil {
		t.Fatalf("events limit -9: %v", err)
	}
	if len(negativeEvents) != 100 {
		t.Errorf("change events with -9 = %d, want 100", len(negativeEvents))
	}

	// Effects: -1 is NOT unbounded, it clamps to the same default.
	allEffects, err := st.ListStatusEffects(ctx, "s1", "", "", "", -1)
	if err != nil {
		t.Fatalf("effects limit -1: %v", err)
	}
	if len(allEffects) != 100 {
		t.Errorf("effects with -1 = %d, want 100 (no unbounded mode)", len(allEffects))
	}
	cappedEffects, err := st.ListStatusEffects(ctx, "s1", "", "", "", 5000)
	if err != nil {
		t.Fatalf("effects limit 5000: %v", err)
	}
	if len(cappedEffects) != seeded {
		t.Errorf("effects with 5000 = %d, want the %d seeded rows", len(cappedEffects), seeded)
	}
	smallEffects, err := st.ListStatusEffects(ctx, "s1", "", "", "", 4)
	if err != nil {
		t.Fatalf("effects limit 4: %v", err)
	}
	if len(smallEffects) != 4 {
		t.Errorf("effects with 4 = %d, want 4", len(smallEffects))
	}
}

func TestD1StatusChangeEventSaveAndFilters(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registryID := d1SeedStatusRegistry(t, conn, "s1", "hp")

	event, err := st.SaveStatusChangeEvent(ctx, StatusChangeEvent{
		ChatSessionID: "s1", RegistryID: registryID, StatusKey: "hp", OwnerScope: "character",
		OwnerID: "alice", EventKind: "set", EvidenceJSON: `{}`, PreviousValueJSON: "1", NewValueJSON: "2",
	})
	if err != nil {
		t.Fatalf("SaveStatusChangeEvent: %v", err)
	}
	if event.ID == 0 {
		t.Fatal("save must report an id")
	}
	if event.EventState != "recorded" {
		t.Errorf("event_state = %q, want the default recorded", event.EventState)
	}
	if event.CreatedAt.IsZero() {
		t.Error("the returned event must carry a timestamp")
	}

	// A zero status_value_id and zero source_turn must be NULL.
	var statusValueID, sourceTurn *int64
	if err := conn.QueryRow(ctx, `SELECT status_value_id, source_turn FROM status_change_events WHERE id = ?`, event.ID).
		Scan(&statusValueID, &sourceTurn); err != nil {
		t.Fatalf("read nullables: %v", err)
	}
	if statusValueID != nil || sourceTurn != nil {
		t.Errorf("zero id/turn must be NULL, got %v/%v", statusValueID, sourceTurn)
	}

	other, err := st.SaveStatusChangeEvent(ctx, StatusChangeEvent{
		ChatSessionID: "s1", RegistryID: registryID, StatusKey: "mp", OwnerScope: "character",
		OwnerID: "bob", EventKind: "delta", EvidenceJSON: `{}`, SourceTurn: 12, EventState: "superseded",
	})
	if err != nil {
		t.Fatalf("second event: %v", err)
	}
	if other.SourceTurn != 12 {
		t.Errorf("source turn = %d, want 12", other.SourceTurn)
	}
	if other.EventState != "superseded" {
		t.Errorf("explicit state = %q, want superseded", other.EventState)
	}
	if _, err := st.SaveStatusChangeEvent(ctx, StatusChangeEvent{
		ChatSessionID: "s2", RegistryID: registryID, StatusKey: "hp", OwnerScope: "character", OwnerID: "x", EventKind: "set", EvidenceJSON: `{}`,
	}); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	byKey, err := st.ListStatusChangeEvents(ctx, "s1", "", "", "mp", 0)
	if err != nil {
		t.Fatalf("filter by key: %v", err)
	}
	if len(byKey) != 1 || byKey[0].ID != other.ID {
		t.Errorf("status key filter = %+v", byKey)
	}
	byOwner, err := st.ListStatusChangeEvents(ctx, "s1", "", "bob", "", 0)
	if err != nil {
		t.Fatalf("filter by owner: %v", err)
	}
	if len(byOwner) != 1 {
		t.Errorf("owner filter = %+v", byOwner)
	}
	isolated, err := st.ListStatusChangeEvents(ctx, "s1", "", "", "", -1)
	if err != nil {
		t.Fatalf("unbounded list: %v", err)
	}
	if len(isolated) != 2 {
		t.Errorf("session isolation: %d rows, want 2", len(isolated))
	}
}

func TestD1StatusEffectLifecycle(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registryID := d1SeedStatusRegistry(t, conn, "s1", "hp")

	effect, err := st.SaveStatusEffect(ctx, StatusEffect{
		ChatSessionID: "s1", RegistryID: registryID, StatusKey: "hp", OwnerScope: "character",
		OwnerID: "alice", EffectKind: "buff", EvidenceJSON: `{}`,
	})
	if err != nil {
		t.Fatalf("SaveStatusEffect: %v", err)
	}
	if effect.ID == 0 || effect.EffectState != "active" {
		t.Errorf("effect = %+v, want an id and the default active state", effect)
	}

	var sourceTurn *int64
	if err := conn.QueryRow(ctx, `SELECT source_turn FROM status_effects WHERE id = ?`, effect.ID).Scan(&sourceTurn); err != nil {
		t.Fatalf("read source turn: %v", err)
	}
	if sourceTurn != nil {
		t.Errorf("zero source turn must be NULL, got %d", *sourceTurn)
	}

	// Clearing records evidence and a turn; a zero turn becomes NULL again.
	if err := st.UpdateStatusEffectState(ctx, effect.ID, "cleared", `{"why":"resolved"}`, 9); err != nil {
		t.Fatalf("UpdateStatusEffectState: %v", err)
	}
	var state string
	var clearedEvidence *string
	var clearedTurn *int64
	if err := conn.QueryRow(ctx, `SELECT effect_state, cleared_evidence_json, cleared_turn
		FROM status_effects WHERE id = ?`, effect.ID).Scan(&state, &clearedEvidence, &clearedTurn); err != nil {
		t.Fatalf("read cleared effect: %v", err)
	}
	if state != "cleared" {
		t.Errorf("effect_state = %q, want cleared", state)
	}
	if clearedEvidence == nil || *clearedEvidence != `{"why":"resolved"}` {
		t.Errorf("cleared_evidence_json = %v", clearedEvidence)
	}
	if clearedTurn == nil || *clearedTurn != 9 {
		t.Errorf("cleared_turn = %v, want 9", clearedTurn)
	}

	if err := st.UpdateStatusEffectState(ctx, effect.ID, "expired", "", 0); err != nil {
		t.Fatalf("second update: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT cleared_evidence_json, cleared_turn FROM status_effects WHERE id = ?`, effect.ID).
		Scan(&clearedEvidence, &clearedTurn); err != nil {
		t.Fatalf("read second update: %v", err)
	}
	if clearedEvidence != nil || clearedTurn != nil {
		t.Errorf("empty evidence and zero turn must be NULL, got %v/%v", clearedEvidence, clearedTurn)
	}

	active, err := st.ListStatusEffects(ctx, "s1", "", "", "expired", 0)
	if err != nil {
		t.Fatalf("filter by state: %v", err)
	}
	if len(active) != 1 || active[0].ID != effect.ID {
		t.Errorf("state filter = %+v", active)
	}
	none, err := st.ListStatusEffects(ctx, "s1", "", "", "active", 0)
	if err != nil {
		t.Fatalf("filter by absent state: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("absent state = %d rows, want 0", len(none))
	}

	if err := st.UpdateStatusEffectState(ctx, 999999, "cleared", "", 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing effect = %v, want ErrNotFound", err)
	}
}
