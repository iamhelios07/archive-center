package store

import (
	"context"
	"strings"
	"testing"
)

func TestD1LatestSessionTurnIndex(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if got, err := st.LatestSessionTurnIndex(ctx, "s1"); err != nil || got != 0 {
		t.Fatalf("empty session = %d, %v; want 0, nil", got, err)
	}
	for _, turn := range []int{2, 7, 4} {
		if _, err := conn.Exec(ctx, `INSERT INTO chat_logs (chat_session_id, turn_index, role, content)
			VALUES ('s1', ?, 'user', 'x')`, turn); err != nil {
			t.Fatalf("seed turn %d: %v", turn, err)
		}
	}
	if _, err := conn.Exec(ctx, `INSERT INTO chat_logs (chat_session_id, turn_index, role, content) VALUES ('s2', 99, 'user', 'x')`); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	got, err := st.LatestSessionTurnIndex(ctx, "s1")
	if err != nil {
		t.Fatalf("LatestSessionTurnIndex: %v", err)
	}
	if got != 7 {
		t.Errorf("latest turn = %d, want 7", got)
	}
}

func TestD1ListMemoriesRangeWindowAndIncludeIDs(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	seed := func(turn int, marker string) int64 {
		t.Helper()
		if _, err := conn.Exec(ctx, `INSERT INTO memories (chat_session_id, turn_index, summary_json)
			VALUES ('s1', ?, ?)`, turn, marker); err != nil {
			t.Fatalf("seed memory turn %d: %v", turn, err)
		}
		var id int64
		if err := conn.QueryRow(ctx, `SELECT id FROM memories WHERE summary_json = ?`, marker).Scan(&id); err != nil {
			t.Fatalf("read id: %v", err)
		}
		return id
	}

	seed(2, "two")
	outsideID := seed(20, "twenty")
	pinnedID := seed(-1, "pinned")
	if _, err := conn.Exec(ctx, `INSERT INTO memories (chat_session_id, turn_index, summary_json) VALUES ('s2', 3, 'other')`); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	window, err := st.ListMemoriesRange(ctx, "s1", 1, 5, nil)
	if err != nil {
		t.Fatalf("ListMemoriesRange: %v", err)
	}
	// The negative-turn "pinned" memory is always included, the in-window memory
	// is included, and the out-of-window one is not.
	if len(window) != 2 {
		t.Fatalf("window rows = %d, want 2: %+v", len(window), window)
	}
	if window[0].SummaryJSON != "pinned" {
		t.Errorf("turn_index ASC ordering broken: first = %q", window[0].SummaryJSON)
	}

	withInclude, err := st.ListMemoriesRange(ctx, "s1", 1, 5, []int64{outsideID})
	if err != nil {
		t.Fatalf("ListMemoriesRange with includeIDs: %v", err)
	}
	if len(withInclude) != 3 {
		t.Errorf("includeIDs rows = %d, want 3 (window plus the explicitly requested id)", len(withInclude))
	}

	// Non-positive ids are skipped rather than matching anything.
	ignored, err := st.ListMemoriesRange(ctx, "s1", 1, 5, []int64{0, -5})
	if err != nil {
		t.Fatalf("ListMemoriesRange with ignored ids: %v", err)
	}
	if len(ignored) != 2 {
		t.Errorf("rows with only non-positive ids = %d, want the plain window of 2", len(ignored))
	}

	// A non-positive window means unbounded on that side.
	unbounded, err := st.ListMemoriesRange(ctx, "s1", 0, 0, nil)
	if err != nil {
		t.Fatalf("unbounded range: %v", err)
	}
	if len(unbounded) != 3 {
		t.Errorf("unbounded rows = %d, want 3", len(unbounded))
	}
	if pinnedID == 0 {
		t.Error("sanity: pinned memory must have an id")
	}
}

func TestD1ListEvidenceRangeEffectiveTurnAndExclusions(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	insert := func(anchor any, start, end int, tombstoned int, superseded any, marker string) int64 {
		t.Helper()
		if _, err := conn.Exec(ctx, `INSERT INTO direct_evidence_records
			(chat_session_id, evidence_kind, evidence_text, source_turn_start, source_turn_end,
			 turn_anchor, tombstoned, superseded_by_id)
			VALUES ('s1', 'fact_event', ?, ?, ?, ?, ?, ?)`,
			marker, start, end, anchor, tombstoned, superseded); err != nil {
			t.Fatalf("seed evidence %s: %v", marker, err)
		}
		var id int64
		if err := conn.QueryRow(ctx, `SELECT id FROM direct_evidence_records WHERE evidence_text = ?`, marker).Scan(&id); err != nil {
			t.Fatalf("read id: %v", err)
		}
		return id
	}

	// Effective turn comes from the greatest of start, end, and a non-null anchor.
	insert(9, 3, 4, 0, nil, "anchor-wins")
	insert(nil, 6, 7, 0, nil, "in-window")
	// Excluded: tombstoned, or already superseded.
	insert(nil, 6, 7, 1, nil, "tombstoned")
	insert(nil, 6, 7, 0, 4, "superseded")
	// Always included regardless of window.
	insert(nil, -5, -5, 0, nil, "negative-start")
	// Outside the window and with no anchor to rescue it.
	insert(nil, 50, 50, 0, nil, "far-outside")

	rows, err := st.ListEvidenceRange(ctx, "s1", 1, 10, nil)
	if err != nil {
		t.Fatalf("ListEvidenceRange: %v", err)
	}
	got := map[string]bool{}
	for _, row := range rows {
		got[row.EvidenceText] = true
	}
	for _, want := range []string{"anchor-wins", "in-window", "negative-start"} {
		if !got[want] {
			t.Errorf("%s must be in range; got %v", want, got)
		}
	}
	for _, unwanted := range []string{"tombstoned", "superseded", "far-outside"} {
		if got[unwanted] {
			t.Errorf("%s must be excluded; got %v", unwanted, got)
		}
	}
	// The anchor (9) rather than the start (3) must have decided eligibility.
	for _, row := range rows {
		if row.EvidenceText == "anchor-wins" && row.TurnAnchor != 9 {
			t.Errorf("anchor = %d, want 9", row.TurnAnchor)
		}
	}

	outsideID := insert(nil, 50, 50, 0, nil, "explicitly-requested")
	withInclude, err := st.ListEvidenceRange(ctx, "s1", 1, 10, []int64{outsideID})
	if err != nil {
		t.Fatalf("ListEvidenceRange with includeIDs: %v", err)
	}
	found := false
	for _, row := range withInclude {
		if row.EvidenceText == "explicitly-requested" {
			found = true
		}
	}
	if !found {
		t.Error("an explicitly requested evidence id must be returned even outside the window")
	}
}

func TestD1ListKGTriplesRangeKeepsOpenTriples(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	insert := func(sourceTurn any, validTo any, marker string) {
		t.Helper()
		if _, err := conn.Exec(ctx, `INSERT INTO kg_triples
			(chat_session_id, subject, predicate, object, source_turn, valid_to)
			VALUES ('s1', ?, 'p', 'o', ?, ?)`, marker, sourceTurn, validTo); err != nil {
			t.Fatalf("seed triple %s: %v", marker, err)
		}
	}

	insert(3, nil, "open-null")
	insert(3, 0, "open-zero")
	insert(3, 5, "closed-in-window")
	// The window applies to source_turn, not valid_to: a triple that has closed
	// but whose source turn is inside the window stays included, while one whose
	// source turn is outside the window and which has already closed is excluded.
	insert(3, 1, "closed-but-source-in-window")
	insert(90, 5, "closed-far-outside")
	insert(-2, 9, "negative-source")

	rows, err := st.ListKGTriplesRange(ctx, "s1", 1, 4)
	if err != nil {
		t.Fatalf("ListKGTriplesRange: %v", err)
	}
	got := map[string]bool{}
	for _, row := range rows {
		got[row.Subject] = true
	}
	for _, want := range []string{"open-null", "open-zero", "closed-in-window", "closed-but-source-in-window", "negative-source"} {
		if !got[want] {
			t.Errorf("%s must be included (open, or source turn in range); got %v", want, got)
		}
	}
	if got["closed-far-outside"] {
		t.Errorf("a closed triple whose source turn is outside the window must be excluded; got %v", got)
	}
}

func TestD1ListActiveStatesAndLayersRangeKeepsNewestPerType(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for _, spec := range []struct {
		stateType string
		turn      int
	}{
		{"mood", 2}, {"mood", 30}, {"location", 3}, {"location", 40},
	} {
		if _, err := conn.Exec(ctx, `INSERT INTO active_states
			(chat_session_id, state_type, content, turn_index) VALUES ('s1', ?, ?, ?)`,
			spec.stateType, spec.stateType, spec.turn); err != nil {
			t.Fatalf("seed active state: %v", err)
		}
	}

	states, err := st.ListActiveStatesRange(ctx, "s1", 1, 5)
	if err != nil {
		t.Fatalf("ListActiveStatesRange: %v", err)
	}
	// mood@2 and location@3 are in the window; the newest per type (mood@30,
	// location@40) must also be present so prompt assembly sees current state.
	turnsByType := map[string][]int{}
	for _, state := range states {
		turnsByType[state.StateType] = append(turnsByType[state.StateType], state.TurnIndex)
	}
	for _, stateType := range []string{"mood", "location"} {
		turns := turnsByType[stateType]
		if len(turns) != 2 {
			t.Errorf("%s turns = %v, want both the in-window and the newest", stateType, turns)
			continue
		}
		if turns[0] != 30 && turns[0] != 40 {
			t.Errorf("%s ordering must be turn DESC, got %v", stateType, turns)
		}
	}

	for _, spec := range []struct {
		layerType string
		turn      int
	}{{"summary", 2}, {"summary", 25}, {"detail", 3}} {
		if _, err := conn.Exec(ctx, `INSERT INTO canonical_state_layers
			(chat_session_id, layer_type, content, turn_index) VALUES ('s1', ?, 'c', ?)`,
			spec.layerType, spec.turn); err != nil {
			t.Fatalf("seed layer: %v", err)
		}
	}
	layers, err := st.ListCanonicalStateLayersRange(ctx, "s1", 1, 5)
	if err != nil {
		t.Fatalf("ListCanonicalStateLayersRange: %v", err)
	}
	layerTurns := map[string][]int{}
	for _, layer := range layers {
		layerTurns[layer.LayerType] = append(layerTurns[layer.LayerType], layer.TurnIndex)
	}
	if len(layerTurns["summary"]) != 2 {
		t.Errorf("summary layers = %v, want the in-window and newest rows", layerTurns["summary"])
	}
	if len(layerTurns["detail"]) != 1 {
		t.Errorf("detail layers = %v, want 1", layerTurns["detail"])
	}
}

func TestD1ListCharacterStatesCurrentBefore(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for _, spec := range []struct {
		name   string
		turn   int
		appear string
	}{
		{"hero", 1, `{"hair":"black"}`},
		{"hero", 5, `{"hair":"brown"}`},
		{"rival", 2, `{"hair":"blond"}`},
	} {
		if _, err := conn.Exec(ctx, `INSERT INTO character_states
			(chat_session_id, character_name, appearance_json, turn_index) VALUES ('s1', ?, ?, ?)`,
			spec.name, spec.appear, spec.turn); err != nil {
			t.Fatalf("seed character state: %v", err)
		}
	}

	// Strictly before turn 3: hero resolves to its turn-1 snapshot, rival to its
	// turn-2 one.
	before, err := st.ListCharacterStatesCurrentBefore(ctx, "s1", 3)
	if err != nil {
		t.Fatalf("ListCharacterStatesCurrentBefore: %v", err)
	}
	byName := map[string]CharacterState{}
	for _, state := range before {
		byName[state.CharacterName] = state
	}
	if len(before) != 2 {
		t.Fatalf("rows before turn 3 = %d, want 2", len(before))
	}
	if byName["hero"].TurnIndex != 1 {
		t.Errorf("hero turn = %d, want the snapshot before turn 3 (1)", byName["hero"].TurnIndex)
	}
	if byName["rival"].TurnIndex != 2 {
		t.Errorf("rival turn = %d, want 2", byName["rival"].TurnIndex)
	}

	// A non-positive bound means the latest across the whole session.
	latest, err := st.ListCharacterStatesCurrentBefore(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("latest character states: %v", err)
	}
	for _, state := range latest {
		if state.CharacterName == "hero" && state.TurnIndex != 5 {
			t.Errorf("latest hero turn = %d, want 5", state.TurnIndex)
		}
	}

	// A manual override still applies to a historical read.
	if _, err := conn.Exec(ctx, `INSERT INTO character_events
		(chat_session_id, character_name, turn_index, event_type, details_json)
		VALUES ('s1', 'hero', 6, 'manual_character_override', '{"edits":[{"path":["appearance","hair"],"value":"red"}]}')`); err != nil {
		t.Fatalf("seed override: %v", err)
	}
	overlaid, err := st.ListCharacterStatesCurrentBefore(ctx, "s1", 3)
	if err != nil {
		t.Fatalf("historical read with override: %v", err)
	}
	found := false
	for _, state := range overlaid {
		if state.CharacterName == "hero" {
			found = true
			if !strings.Contains(state.AppearanceJSON, "red") {
				t.Errorf("override must apply to the historical read: %s", state.AppearanceJSON)
			}
		}
	}
	if !found {
		t.Error("hero must still be present")
	}

	// Before turn 1 nothing is eligible.
	none, err := st.ListCharacterStatesCurrentBefore(ctx, "s1", 1)
	if err != nil {
		t.Fatalf("read before turn 1: %v", err)
	}
	for _, state := range none {
		if state.TurnIndex >= 1 {
			t.Errorf("a snapshot at turn %d must not satisfy beforeTurn = 1", state.TurnIndex)
		}
	}
}
