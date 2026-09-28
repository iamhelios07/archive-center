package store

import (
	"context"
	"errors"
	"testing"
)

// TestD1ListCharacterStateHistoryReturnsNewestSnapshotsFirst pins the ordering the
// history contract promises: newest turn first, and within one turn the
// highest-id row first, because a same-turn tie is otherwise non-deterministic and
// the provenance rebuild reads the first row of a turn as the surviving one.
func TestD1ListCharacterStateHistoryReturnsNewestSnapshotsFirst(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for _, spec := range []struct {
		turn   any
		appear string
	}{
		{1, `{"hair":"black"}`},
		{3, `{"hair":"brown"}`},
		{3, `{"hair":"blonde"}`},
		{nil, `{"hair":"grey"}`},
	} {
		if _, err := conn.Exec(ctx, `INSERT INTO character_states
			(chat_session_id, character_name, appearance_json, turn_index)
			VALUES ('s1', 'Chloe', ?, ?)`, spec.appear, spec.turn); err != nil {
			t.Fatalf("seed Chloe turn %v: %v", spec.turn, err)
		}
	}

	history, err := st.ListCharacterStateHistory(ctx, "s1", "Chloe", 50, 0)
	if err != nil {
		t.Fatalf("ListCharacterStateHistory: %v", err)
	}
	if len(history) != 4 {
		t.Fatalf("history rows = %d, want 4: %+v", len(history), history)
	}

	wantAppear := []string{`{"hair":"blonde"}`, `{"hair":"brown"}`, `{"hair":"black"}`, `{"hair":"grey"}`}
	wantTurn := []int{3, 3, 1, 0}
	for i := range history {
		if history[i].AppearanceJSON != wantAppear[i] {
			t.Errorf("row %d appearance = %s, want %s", i, history[i].AppearanceJSON, wantAppear[i])
		}
		if history[i].TurnIndex != wantTurn[i] {
			t.Errorf("row %d turn_index = %d, want %d", i, history[i].TurnIndex, wantTurn[i])
		}
	}

	// The same-turn tie is resolved by id, the NULL turn is read as 0 but still
	// sorts last because the column is ordered as stored, and the id tie-break is
	// what makes a same-turn page deterministic.
	if history[0].ID <= history[1].ID {
		t.Errorf("same-turn tie must order by id DESC, got %d then %d", history[0].ID, history[1].ID)
	}
	if history[1].ID <= history[2].ID {
		t.Errorf("turn DESC ordering broken: ids %d, %d, %d", history[0].ID, history[1].ID, history[2].ID)
	}
}

// TestD1ListCharacterStateHistoryStaysInsideOneSessionAndCharacter confirms the
// two equality predicates really are two: neither another character in the same
// session nor the same character in another session may appear in a page.
func TestD1ListCharacterStateHistoryStaysInsideOneSessionAndCharacter(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	seed := func(sid, name string, turn int) {
		t.Helper()
		if _, err := conn.Exec(ctx, `INSERT INTO character_states
			(chat_session_id, character_name, appearance_json, turn_index)
			VALUES (?, ?, '{"hair":"same"}', ?)`, sid, name, turn); err != nil {
			t.Fatalf("seed %s/%s: %v", sid, name, err)
		}
	}
	seed("s1", "Chloe", 1)
	seed("s1", "Chloe", 2)
	seed("s1", "Mina", 9)
	seed("s2", "Chloe", 9)

	history, err := st.ListCharacterStateHistory(ctx, "s1", "Chloe", 50, 0)
	if err != nil {
		t.Fatalf("ListCharacterStateHistory: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("history rows = %d, want 2 (only s1/Chloe): %+v", len(history), history)
	}
	for _, state := range history {
		if state.ChatSessionID != "s1" || state.CharacterName != "Chloe" {
			t.Errorf("row leaked scope: session %q character %q", state.ChatSessionID, state.CharacterName)
		}
	}

	// A character that never had a snapshot in this session is an empty page,
	// not another character's rows and not an error.
	empty, err := st.ListCharacterStateHistory(ctx, "s1", "nobody", 50, 0)
	if err != nil {
		t.Fatalf("unknown character: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("unknown character returned %d rows: %+v", len(empty), empty)
	}
}

// TestD1ListCharacterStateHistoryClampsLimitAndOffset pins the argument handling.
// The admin repair preview pages with 200 and stops on a short page, so a limit of
// 200 must serve a full page and an oversized limit must not silently become the
// ceiling, which would let that loop stop early.
func TestD1ListCharacterStateHistoryClampsLimitAndOffset(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	const total = 210
	for turn := 1; turn <= total; turn++ {
		if _, err := conn.Exec(ctx, `INSERT INTO character_states
			(chat_session_id, character_name, appearance_json, turn_index)
			VALUES ('s1', 'Chloe', '{"hair":"black"}', ?)`, turn); err != nil {
			t.Fatalf("seed turn %d: %v", turn, err)
		}
	}

	ids := func(t *testing.T, limit, offset int) []int64 {
		t.Helper()
		rows, err := st.ListCharacterStateHistory(ctx, "s1", "Chloe", limit, offset)
		if err != nil {
			t.Fatalf("ListCharacterStateHistory(limit=%d, offset=%d): %v", limit, offset, err)
		}
		out := make([]int64, 0, len(rows))
		for _, row := range rows {
			out = append(out, row.ID)
		}
		return out
	}

	// A non-positive limit and an oversized limit both fall back to 50.
	for _, limit := range []int{0, -5, 201, 5000} {
		if got := ids(t, limit, 0); len(got) != d1CharacterHistoryDefaultLimit {
			t.Errorf("limit %d returned %d rows, want the %d-row default", limit, len(got), d1CharacterHistoryDefaultLimit)
		}
	}
	// The ceiling itself serves a full page, which is what keeps the caller's
	// paging loop going instead of treating a truncated page as the tail.
	if got := ids(t, d1CharacterHistoryPageLimit, 0); len(got) != d1CharacterHistoryPageLimit {
		t.Errorf("limit %d returned %d rows, want a full page of %d",
			d1CharacterHistoryPageLimit, len(got), d1CharacterHistoryPageLimit)
	}

	// A negative offset is the first page, not an error.
	first := ids(t, 10, 0)
	negative := ids(t, 10, -3)
	if len(negative) != len(first) {
		t.Fatalf("negative offset returned %d rows, want the first page of %d", len(negative), len(first))
	}
	for i := range first {
		if negative[i] != first[i] {
			t.Fatalf("negative offset page differs from the first page: %+v vs %+v", negative, first)
		}
	}

	// Offset walks the same ordering the first page established.
	tail := ids(t, 10, 205)
	if len(tail) != total-205 {
		t.Errorf("offset 205 returned %d rows, want %d", len(tail), total-205)
	}
	if past := ids(t, 10, total); len(past) != 0 {
		t.Errorf("offset past the end returned %d rows, want 0: %+v", len(past), past)
	}

	// The caller's own paging loop must cover every row exactly once.
	seen := map[int64]int{}
	pages := 0
	for offset := 0; ; offset += d1CharacterHistoryPageLimit {
		page, err := st.ListCharacterStateHistory(ctx, "s1", "Chloe", d1CharacterHistoryPageLimit, offset)
		if err != nil {
			t.Fatalf("paged read at offset %d: %v", offset, err)
		}
		for _, row := range page {
			seen[row.ID]++
		}
		if len(page) < d1CharacterHistoryPageLimit {
			break
		}
		pages++
		if pages > 10 {
			t.Fatal("paging does not terminate")
		}
	}
	if len(seen) != total {
		t.Errorf("paged read covered %d of %d snapshots", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Errorf("snapshot %d appeared %d times across pages, want once", id, count)
		}
	}
}

// TestD1ListCharacterStateHistoryReadsNullableColumnsAsEmptyAndZero pins the NULL
// handling the repair preview depends on: a snapshot written before a field
// existed reads as empty text and turn 0 rather than failing the scan, and
// provenance metadata is carried through untouched.
func TestD1ListCharacterStateHistoryReadsNullableColumnsAsEmptyAndZero(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	metadata := `{"fields":{"/status/coins":{"source_turn":2}}}`
	if _, err := conn.Exec(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, turn_index)
		VALUES ('s1', 'Chloe', 4)`); err != nil {
		t.Fatalf("seed all-null snapshot: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, status_json, field_provenance_json, turn_index)
		VALUES ('s1', 'Chloe', ?, ?, 5)`, `{"coins":4}`, metadata); err != nil {
		t.Fatalf("seed snapshot with provenance: %v", err)
	}

	history, err := st.ListCharacterStateHistory(ctx, "s1", "Chloe", 50, 0)
	if err != nil {
		t.Fatalf("ListCharacterStateHistory: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("history rows = %d, want 2: %+v", len(history), history)
	}

	newest := history[0]
	if newest.StatusJSON != `{"coins":4}` {
		t.Errorf("newest status = %q", newest.StatusJSON)
	}
	if newest.FieldProvenanceJSON != metadata {
		t.Errorf("newest provenance = %q, want %q", newest.FieldProvenanceJSON, metadata)
	}
	if newest.AppearanceJSON != "" || newest.PersonalityJSON != "" || newest.RelationshipsJSON != "" || newest.SpeechStyleJSON != "" {
		t.Errorf("absent columns must read as empty text: %+v", newest)
	}
	if newest.CreatedAt.IsZero() || newest.UpdatedAt.IsZero() {
		t.Errorf("timestamps must be parsed, not left zero: %+v", newest)
	}

	oldest := history[1]
	if oldest.StatusJSON != "" || oldest.FieldProvenanceJSON != "" {
		t.Errorf("NULL columns must read as empty text: %+v", oldest)
	}
	if oldest.TurnIndex != 4 {
		t.Errorf("NULL turn column must not be confused here: %+v", oldest)
	}
}

// TestD1ListCharacterStateHistoryDoesNotOverlayManualEdits pins the one place the
// history read deliberately differs from the current-state readers. MariaDB
// applies no manual-edit overlay to this query, and applying one here would
// rewrite a past snapshot and invent a later origin for a field that never
// changed. The current-state reader is asserted here too, so the contrast is
// recorded rather than looking like a missing feature.
func TestD1ListCharacterStateHistoryDoesNotOverlayManualEdits(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if _, err := conn.Exec(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, appearance_json, turn_index)
		VALUES ('s1', 'Chloe', '{"hair":"brown"}', 1)`); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO character_events
		(chat_session_id, character_name, turn_index, event_type, details_json)
		VALUES ('s1', 'Chloe', 5, 'manual_character_override', '{"edits":[{"path":["appearance","hair"],"value":"red"}]}')`); err != nil {
		t.Fatalf("seed manual override: %v", err)
	}

	history, err := st.ListCharacterStateHistory(ctx, "s1", "Chloe", 50, 0)
	if err != nil {
		t.Fatalf("ListCharacterStateHistory: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("history rows = %d, want 1: %+v", len(history), history)
	}
	if history[0].AppearanceJSON != `{"hair":"brown"}` {
		t.Errorf("history must keep the snapshot as written, got %s", history[0].AppearanceJSON)
	}

	// The current-state read is where the durable operator edit belongs.
	current, err := st.ListCharacterStates(ctx, "s1")
	if err != nil {
		t.Fatalf("ListCharacterStates: %v", err)
	}
	if len(current) != 1 || current[0].AppearanceJSON != `{"hair":"red"}` {
		t.Errorf("current-state read must overlay the durable edit: %+v", current)
	}
}

// TestD1ListCharacterStateHistoryUsesTheSameNameMatchAsGetCharacterState records
// the one provider difference a caller can observe. MariaDB compares
// character_name under utf8mb4_unicode_ci and therefore ignores case; SQLite
// compares TEXT bytes. Every D1 character reader shares that binary equality, so
// the history read matches the stored name exactly instead of making this one
// route lenient and disagreeing with GetCharacterState on the same input.
func TestD1ListCharacterStateHistoryUsesTheSameNameMatchAsGetCharacterState(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if _, err := conn.Exec(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, appearance_json, turn_index)
		VALUES ('s1', 'Chloe', '{"hair":"brown"}', 1)`); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	exact, err := st.ListCharacterStateHistory(ctx, "s1", "Chloe", 50, 0)
	if err != nil {
		t.Fatalf("exact name: %v", err)
	}
	if len(exact) != 1 {
		t.Fatalf("exact name returned %d rows, want 1", len(exact))
	}

	different, err := st.ListCharacterStateHistory(ctx, "s1", "chloe", 50, 0)
	if err != nil {
		t.Fatalf("differently cased name: %v", err)
	}
	if len(different) != 0 {
		t.Errorf("history matched a name the current-state reader does not: %+v", different)
	}
	if _, err := st.GetCharacterState(ctx, "s1", "chloe"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetCharacterState error = %v, want ErrNotFound for the same input", err)
	}
}

// TestD1ListCharacterStateHistoryIsAdvertisedAsCapability checks the consequence
// the routes actually depend on: the history route and the admin repair preview
// gate on a type assertion, so a provider that implements the method but is not
// discoverable through the capability manifest would still answer
// "history_store_not_available".
func TestD1ListCharacterStateHistoryIsAdvertisedAsCapability(t *testing.T) {
	st, _ := newD1TestStore(t)
	var asStore Store = st

	if _, ok := asStore.(CharacterStateHistoryStore); !ok {
		t.Fatal("D1 store must satisfy CharacterStateHistoryStore")
	}
	for _, status := range CapabilityReport(asStore) {
		if status.Name != "CharacterStateHistoryStore" {
			continue
		}
		if !status.Implemented {
			t.Fatal("capability manifest reports CharacterStateHistoryStore as missing")
		}
		return
	}
	t.Fatal("capability manifest does not probe CharacterStateHistoryStore")
}
