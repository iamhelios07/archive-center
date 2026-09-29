package store

import (
	"context"
	"errors"
	"testing"
)

func TestD1ExplorerDeletesAreSessionScoped(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	own := &Memory{ChatSessionID: "delete-s1", TurnIndex: 1, SummaryJSON: `{}`}
	other := &Memory{ChatSessionID: "delete-s2", TurnIndex: 1, SummaryJSON: `{}`}
	if err := st.SaveMemory(ctx, own); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveMemory(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteMemoryByID(ctx, "delete-s1", other.ID); err != nil {
		t.Fatalf("cross-session delete: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memories WHERE id = ?`, other.ID); got != 1 {
		t.Fatalf("cross-session memory count = %d, want 1", got)
	}
	if err := st.DeleteMemoryByID(ctx, "delete-s1", own.ID); err != nil {
		t.Fatalf("own delete: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memories WHERE id = ?`, own.ID); got != 0 {
		t.Fatalf("own memory count = %d, want 0", got)
	}
}

func TestD1DeleteCharacterByNameDeletesOnlyCharacterRows(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	if err := conn.Batch(ctx,
		D1Statement{SQL: `INSERT INTO character_states (chat_session_id, character_name, appearance_json) VALUES ('character-s', 'Alex', '{}'), ('character-s', 'Item', '{}')`},
		D1Statement{SQL: `INSERT INTO character_events (chat_session_id, character_name, event_type) VALUES ('character-s', 'Alex', 'manual_edit'), ('character-s', 'Item', 'manual_edit')`},
		D1Statement{SQL: `INSERT INTO entities (chat_session_id, name, entity_type, first_seen_turn, last_seen_turn) VALUES ('character-s', 'Alex', 'npc', 1, 1), ('character-s', 'Alex', 'location', 1, 1), ('character-s', 'Item', 'item', 1, 1)`},
	); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteCharacterByName(ctx, "character-s", " Alex "); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		query string
		want  int
	}{
		{`SELECT COUNT(*) FROM character_states WHERE chat_session_id = 'character-s' AND character_name = 'Alex'`, 0},
		{`SELECT COUNT(*) FROM character_events WHERE chat_session_id = 'character-s' AND character_name = 'Alex'`, 0},
		{`SELECT COUNT(*) FROM entities WHERE chat_session_id = 'character-s' AND name = 'Alex' AND entity_type = 'npc'`, 0},
		{`SELECT COUNT(*) FROM entities WHERE chat_session_id = 'character-s' AND name = 'Alex' AND entity_type = 'location'`, 1},
		{`SELECT COUNT(*) FROM character_states WHERE chat_session_id = 'character-s' AND character_name = 'Item'`, 1},
	} {
		if got := d1Count(t, conn, check.query); got != check.want {
			t.Errorf("%s = %d, want %d", check.query, got, check.want)
		}
	}
	if err := st.DeleteCharacterByName(ctx, "character-s", " \t"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("blank character error = %v, want ErrNotFound", err)
	}
}
