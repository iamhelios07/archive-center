package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// sqliteD1Conn implements D1Conn over a real SQLite engine. Because D1 runs
// SQLite, this transport exercises the same dialect the Worker will execute, so
// these tests validate the D1 statements rather than a bespoke mock.
//
// Scanning delegates to the shared d1AssignRow conversion, so the SQLite and
// bridge transports interpret columns identically.
type sqliteD1Conn struct {
	db *sql.DB
}

func (c *sqliteD1Conn) QueryRow(ctx context.Context, query string, args ...any) D1Row {
	return &sqliteD1Row{db: c.db, ctx: ctx, query: query, args: args}
}

func (c *sqliteD1Conn) Query(ctx context.Context, query string, args ...any) (D1Rows, error) {
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &sqliteD1Rows{rows: rows}, nil
}

func (c *sqliteD1Conn) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, nil
	}
	return n, nil
}

// Batch mirrors D1's transactional batch: any failure rolls the whole sequence
// back, so a partially applied batch is never left behind.
func (c *sqliteD1Conn) Batch(ctx context.Context, statements ...D1Statement) error {
	if len(statements) == 0 {
		return nil
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt.SQL, stmt.Args...); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

type sqliteD1Row struct {
	db    *sql.DB
	ctx   context.Context
	query string
	args  []any
}

func (r *sqliteD1Row) Scan(dest ...any) error {
	rows, err := r.db.QueryContext(r.ctx, r.query, r.args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return errD1NoRows
	}
	return sqliteScanInto(rows, dest...)
}

type sqliteD1Rows struct {
	rows *sql.Rows
}

func (r *sqliteD1Rows) Next() bool             { return r.rows.Next() }
func (r *sqliteD1Rows) Err() error             { return r.rows.Err() }
func (r *sqliteD1Rows) Close() error           { return r.rows.Close() }
func (r *sqliteD1Rows) Scan(dest ...any) error { return sqliteScanInto(r.rows, dest...) }

// sqliteScanInto reads the current row into generic values and then applies the
// shared D1 conversion, keeping both transports consistent.
func sqliteScanInto(rows *sql.Rows, dest ...any) error {
	values := make([]any, len(dest))
	ptrs := make([]any, len(dest))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return err
	}
	return d1AssignRow(dest, values)
}

func newD1TestStore(t *testing.T) (*d1Store, *sqliteD1Conn) {
	t.Helper()
	db := d1ApplyAllMigrations(t)
	conn := &sqliteD1Conn{db: db}
	st, err := NewD1Store(conn)
	if err != nil {
		t.Fatalf("NewD1Store: %v", err)
	}
	d1, ok := st.(*d1Store)
	if !ok {
		t.Fatalf("NewD1Store returned %T, want *d1Store", st)
	}
	return d1, conn
}

func d1Count(t *testing.T, conn *sqliteD1Conn, query string, args ...any) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

// TestD1StoreChatLogReplayIsIdempotent pins the MariaDB parity for a repeated
// write of the same (session, turn, role) key: one row, stable id.
func TestD1StoreChatLogReplayIsIdempotent(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	first := &ChatLog{ChatSessionID: "s1", TurnIndex: 3, Role: "User", Content: "hello", CreatedAt: created}
	if err := st.SaveChatLog(ctx, first); err != nil {
		t.Fatalf("first SaveChatLog: %v", err)
	}
	if first.ID == 0 {
		t.Fatal("SaveChatLog must report the canonical id")
	}
	if first.Role != "user" {
		t.Errorf("role = %q, want the normalised %q", first.Role, "user")
	}

	replay := &ChatLog{ChatSessionID: "s1", TurnIndex: 3, Role: "user", Content: "hello", CreatedAt: created}
	if err := st.SaveChatLog(ctx, replay); err != nil {
		t.Fatalf("replayed SaveChatLog: %v", err)
	}
	if replay.ID != first.ID {
		t.Errorf("replay id = %d, want the existing id %d", replay.ID, first.ID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs`); got != 1 {
		t.Errorf("chat_logs rows = %d, want 1 after an idempotent replay", got)
	}
}

// TestD1StoreChatLogRejectsConflictingPayload pins the other half of the MariaDB
// contract: a same-key write with different content must fail rather than
// overwrite. SQLite accepts such an upsert, so the gate is application-side.
func TestD1StoreChatLogRejectsConflictingPayload(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if err := st.SaveChatLog(ctx, &ChatLog{ChatSessionID: "s1", TurnIndex: 1, Role: "user", Content: "original"}); err != nil {
		t.Fatalf("SaveChatLog: %v", err)
	}
	err := st.SaveChatLog(ctx, &ChatLog{ChatSessionID: "s1", TurnIndex: 1, Role: "user", Content: "different"})
	if err == nil {
		t.Fatal("a same-key write with different content must be rejected")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs`); got != 1 {
		t.Errorf("chat_logs rows = %d, want 1", got)
	}
	var content string
	if err := conn.QueryRow(ctx, `SELECT content FROM chat_logs`).Scan(&content); err != nil {
		t.Fatalf("read content: %v", err)
	}
	if content != "original" {
		t.Errorf("content = %q, want the original to be preserved", content)
	}
}

func TestD1StoreListChatLogsRangeAndOrder(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	for _, turn := range []int{3, 1, 2, 5} {
		if err := st.SaveChatLog(ctx, &ChatLog{ChatSessionID: "s1", TurnIndex: turn, Role: "user", Content: "u"}); err != nil {
			t.Fatalf("seed turn %d: %v", turn, err)
		}
	}
	if err := st.SaveChatLog(ctx, &ChatLog{ChatSessionID: "s2", TurnIndex: 1, Role: "user", Content: "other"}); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	all, err := st.ListChatLogs(ctx, "s1", 0, 0)
	if err != nil {
		t.Fatalf("ListChatLogs: %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("all rows = %d, want 4", len(all))
	}
	for i, want := range []int{1, 2, 3, 5} {
		if all[i].TurnIndex != want {
			t.Errorf("row %d turn = %d, want %d (ordering is turn then id)", i, all[i].TurnIndex, want)
		}
	}

	bounded, err := st.ListChatLogs(ctx, "s1", 2, 3)
	if err != nil {
		t.Fatalf("bounded ListChatLogs: %v", err)
	}
	if len(bounded) != 2 || bounded[0].TurnIndex != 2 || bounded[1].TurnIndex != 3 {
		t.Fatalf("bounded rows = %+v, want turns 2 and 3", bounded)
	}
}

func TestD1StoreEffectiveInputRoundTrip(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 1, 2, 3, 4, 5, 123000000, time.UTC)

	if err := st.SaveEffectiveInput(ctx, &EffectiveInput{
		ChatSessionID: "s1", TurnIndex: 1, EffectiveInput: "processed intent", CreatedAt: created,
	}); err != nil {
		t.Fatalf("SaveEffectiveInput: %v", err)
	}

	got, err := st.GetEffectiveInput(ctx, "s1", 1)
	if err != nil {
		t.Fatalf("GetEffectiveInput: %v", err)
	}
	if got.EffectiveInput != "processed intent" {
		t.Errorf("effective input = %q", got.EffectiveInput)
	}
	if !got.CreatedAt.UTC().Equal(created) {
		t.Errorf("created_at = %s, want %s (millisecond precision must round-trip)", got.CreatedAt.UTC(), created)
	}

	if _, err := st.GetEffectiveInput(ctx, "s1", 99); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing turn error = %v, want ErrNotFound", err)
	}
}

func TestD1StoreMemoryReturningIDAndNullableColumns(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	full := &Memory{
		ChatSessionID: "s1", TurnIndex: 1, SummaryJSON: `{"a":1}`, Embedding: `[0.1,0.2]`,
		EmbeddingModel: "m1", Importance: 0.5, EmotionalBoost: 0.25, Evidence: `{"e":1}`,
		EmotionalIntensity: 0.75, NarrativeSignificance: 0.6, PlaceWing: "w", PlaceRoom: "r",
	}
	if err := st.SaveMemory(ctx, full); err != nil {
		t.Fatalf("SaveMemory: %v", err)
	}
	if full.ID == 0 {
		t.Fatal("SaveMemory must report the inserted id via RETURNING")
	}

	sparse := &Memory{ChatSessionID: "s1", TurnIndex: 2, Importance: 0.1}
	if err := st.SaveMemory(ctx, sparse); err != nil {
		t.Fatalf("SaveMemory sparse: %v", err)
	}
	if sparse.ID == full.ID {
		t.Fatal("each inserted memory must receive its own id")
	}

	rows, err := st.ListMemories(ctx, "s1", 0, 0)
	if err != nil {
		t.Fatalf("ListMemories: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].SummaryJSON != `{"a":1}` || rows[0].Embedding != `[0.1,0.2]` || rows[0].PlaceWing != "w" {
		t.Errorf("full row did not round-trip: %+v", rows[0])
	}
	if rows[1].SummaryJSON != "" || rows[1].Evidence != "" || rows[1].PlaceRoom != "" {
		t.Errorf("NULL text columns must read back as empty strings: %+v", rows[1])
	}
	if rows[1].Importance != 0.1 {
		t.Errorf("sparse importance = %v, want 0.1", rows[1].Importance)
	}
}

// TestD1StoreBatchIsAtomic pins the D1 batch contract the reset and canonical
// transactions depend on: a failing statement rolls the whole batch back.
func TestD1StoreBatchIsAtomic(t *testing.T) {
	_, conn := newD1TestStore(t)
	ctx := context.Background()

	err := conn.Batch(ctx,
		D1Statement{SQL: `INSERT INTO chat_logs (chat_session_id, turn_index, role, content) VALUES ('s', 1, 'user', 'ok')`},
		D1Statement{SQL: `INSERT INTO chat_logs (chat_session_id, turn_index, role, content) VALUES ('s', 2, 'user', 'ok')`},
		D1Statement{SQL: `INSERT INTO no_such_table (x) VALUES (1)`},
	)
	if err == nil {
		t.Fatal("a batch containing an invalid statement must fail")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs`); got != 0 {
		t.Errorf("chat_logs rows = %d after a failed batch, want 0 (batch must roll back)", got)
	}

	if err := conn.Batch(ctx,
		D1Statement{SQL: `INSERT INTO chat_logs (chat_session_id, turn_index, role, content) VALUES ('s', 1, 'user', 'ok')`},
	); err != nil {
		t.Fatalf("valid batch: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs`); got != 1 {
		t.Errorf("chat_logs rows = %d after a valid batch, want 1", got)
	}
}

// TestD1StoreFullCanonicalSurface re-verifies that the D1 provider now answers
// the whole base Store contract and that an empty database yields empty reads
// and a well-formed empty resume pack rather than an unimplemented error.
func TestD1StoreFullCanonicalSurface(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	var base Store = st
	_ = base

	pack, err := st.GetResumePack(ctx, "s1", "manual")
	if err != nil {
		t.Fatalf("GetResumePack on an empty session must succeed with an empty pack: %v", err)
	}
	if pack.PackStatus != "empty" || pack.LayerCount != 0 || pack.Trigger != "manual" {
		t.Errorf("empty resume pack = %+v, want an empty pack carrying the trigger", pack)
	}
	if pack.SourcesUsed == nil || len(pack.SourcesUsed) != 0 {
		t.Errorf("empty pack sources = %#v, want an empty non-nil list", pack.SourcesUsed)
	}

	// Representative reads across the surface must all succeed on empty data.
	if rows, err := st.ListChatLogs(ctx, "s1", 0, 0); err != nil || len(rows) != 0 {
		t.Errorf("ListChatLogs on empty data = %v, %v", rows, err)
	}
	if rows, err := st.ListStorylines(ctx, "s1"); err != nil || len(rows) != 0 {
		t.Errorf("ListStorylines on empty data = %v, %v", rows, err)
	}
	if rows, err := st.ListWorldRules(ctx, "s1"); err != nil || len(rows) != 0 {
		t.Errorf("ListWorldRules on empty data = %v, %v", rows, err)
	}
	if rows, err := st.ListCharacterStates(ctx, "s1"); err != nil || len(rows) != 0 {
		t.Errorf("ListCharacterStates on empty data = %v, %v", rows, err)
	}
	if rows, err := st.ListPendingThreads(ctx, "s1", ""); err != nil || len(rows) != 0 {
		t.Errorf("ListPendingThreads on empty data = %v, %v", rows, err)
	}
	if rows, err := st.ListEpisodeSummaries(ctx, "s1", 0, 0, 0); err != nil || len(rows) != 0 {
		t.Errorf("ListEpisodeSummaries on empty data = %v, %v", rows, err)
	}
	if rows, err := st.ListAuditLogs(ctx, "s1", "", 10); err != nil || len(rows) != 0 {
		t.Errorf("ListAuditLogs on empty data = %v, %v", rows, err)
	}
	if rows, err := st.ListSessions(ctx); err != nil || len(rows) != 0 {
		t.Errorf("ListSessions on empty data = %v, %v", rows, err)
	}
	if stats, err := st.Stats(ctx); err != nil || stats.ChatLogs != 0 || stats.Memories != 0 || stats.KgTriples != 0 {
		t.Errorf("Stats on empty data = %+v, %v", stats, err)
	}
}

// TestD1StoreResumePackAssembly pins the saga/arc/chapter assembly order, the
// text prefixes, and the newest-row selection the resume path depends on.
func TestD1StoreResumePackAssembly(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	// Two saga digests: the newest to_turn must win.
	if _, err := conn.Exec(ctx, `INSERT INTO saga_digests
		(chat_session_id, from_turn, to_turn, saga_summary) VALUES ('s1', 1, 5, 'old saga')`); err != nil {
		t.Fatalf("seed old saga: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO saga_digests
		(chat_session_id, from_turn, to_turn, saga_summary, resume_pack_text)
		VALUES ('s1', 6, 20, 'new saga', 'resume text')`); err != nil {
		t.Fatalf("seed new saga: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO arc_summaries
		(chat_session_id, from_turn, to_turn, arc_index, core_conflict) VALUES ('s1', 1, 20, 1, 'conflict A')`); err != nil {
		t.Fatalf("seed arc: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO chapter_summaries
		(chat_session_id, from_turn, to_turn, chapter_index, chapter_title, summary_text, resume_text)
		VALUES ('s1', 1, 20, 1, 'Chapter One', 'summary body', 'chapter resume')`); err != nil {
		t.Fatalf("seed chapter: %v", err)
	}

	pack, err := st.GetResumePack(ctx, "s1", "auto")
	if err != nil {
		t.Fatalf("GetResumePack: %v", err)
	}
	if pack.PackStatus != "ready" {
		t.Fatalf("pack status = %q, want ready", pack.PackStatus)
	}
	if pack.Trigger != "auto" {
		t.Errorf("trigger = %q, want auto", pack.Trigger)
	}
	if pack.LayerCount != 3 {
		t.Errorf("layer count = %d, want 3", pack.LayerCount)
	}
	wantSources := []string{"saga_digests", "arc_summaries", "chapter_summaries"}
	for i, want := range wantSources {
		if pack.SourcesUsed[i] != want {
			t.Errorf("source %d = %q, want %q", i, pack.SourcesUsed[i], want)
		}
	}

	// The resume-pack text takes precedence over the summary for the saga, and
	// the text order is saga, arc, chapter.
	if !strings.Contains(pack.AssembledText, "Saga: resume text") {
		t.Errorf("saga must prefer resume_pack_text: %q", pack.AssembledText)
	}
	if strings.Contains(pack.AssembledText, "old saga") {
		t.Errorf("a stale saga must not appear: %q", pack.AssembledText)
	}
	if !strings.Contains(pack.AssembledText, "Arc: conflict A") {
		t.Errorf("arc must fall back to core_conflict: %q", pack.AssembledText)
	}
	if !strings.Contains(pack.AssembledText, "Chapter: Chapter One") ||
		!strings.Contains(pack.AssembledText, "Resume: chapter resume") ||
		!strings.Contains(pack.AssembledText, "Summary: summary body") {
		t.Errorf("chapter lines missing: %q", pack.AssembledText)
	}
	if strings.Index(pack.AssembledText, "Saga:") > strings.Index(pack.AssembledText, "Arc:") ||
		strings.Index(pack.AssembledText, "Arc:") > strings.Index(pack.AssembledText, "Chapter:") {
		t.Errorf("assembly order must be saga, arc, chapter: %q", pack.AssembledText)
	}

	// The nested rows must be attached, not just their text.
	if pack.Saga == nil || pack.Saga.ToTurn != 20 {
		t.Errorf("newest saga row not selected: %+v", pack.Saga)
	}
	if pack.Arc == nil || pack.Arc.CoreConflict != "conflict A" {
		t.Errorf("arc row not selected: %+v", pack.Arc)
	}
	if pack.Chapter == nil || pack.Chapter.ChapterTitle != "Chapter One" {
		t.Errorf("chapter row not selected: %+v", pack.Chapter)
	}

	// A session other than s1 has none of these rows.
	other, err := st.GetResumePack(ctx, "s2", "auto")
	if err != nil {
		t.Fatalf("GetResumePack for another session: %v", err)
	}
	if other.PackStatus != "empty" {
		t.Errorf("other session pack status = %q, want empty (session isolation)", other.PackStatus)
	}
}

func TestNewD1StoreRejectsNilConn(t *testing.T) {
	if _, err := NewD1Store(nil); err == nil {
		t.Error("NewD1Store(nil) must fail")
	}
}

func TestD1StoreCharacterStatesNewestPerCharacter(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	// Two snapshots for "hero" and one for "rival". Only the newest hero snapshot
	// is current, and turn_index DESC decides that.
	if _, err := conn.Exec(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, appearance_json, turn_index)
		VALUES ('s1', 'hero', '{"hair":"black"}', 1)`); err != nil {
		t.Fatalf("seed hero turn 1: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, appearance_json, turn_index)
		VALUES ('s1', 'hero', '{"hair":"brown"}', 3)`); err != nil {
		t.Fatalf("seed hero turn 3: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, status_json, turn_index)
		VALUES ('s1', 'rival', '{"mood":"calm"}', 2)`); err != nil {
		t.Fatalf("seed rival: %v", err)
	}
	// A different session must not leak in.
	if _, err := conn.Exec(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, turn_index) VALUES ('s2', 'other', 9)`); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	states, err := st.ListCharacterStates(ctx, "s1")
	if err != nil {
		t.Fatalf("ListCharacterStates: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("states = %d, want 2 (one per character)", len(states))
	}
	byName := map[string]CharacterState{}
	for _, state := range states {
		byName[state.CharacterName] = state
	}
	if !strings.Contains(byName["hero"].AppearanceJSON, "brown") {
		t.Errorf("hero must resolve to the newest snapshot: %s", byName["hero"].AppearanceJSON)
	}
	if byName["hero"].TurnIndex != 3 {
		t.Errorf("hero turn_index = %d, want 3", byName["hero"].TurnIndex)
	}
	if byName["rival"].StatusJSON != `{"mood":"calm"}` {
		t.Errorf("rival status = %q", byName["rival"].StatusJSON)
	}

	fetched, err := st.GetCharacterState(ctx, "s1", "hero")
	if err != nil {
		t.Fatalf("GetCharacterState: %v", err)
	}
	if fetched.TurnIndex != 3 {
		t.Errorf("GetCharacterState turn_index = %d, want 3", fetched.TurnIndex)
	}
	if _, err := st.GetCharacterState(ctx, "s1", "nobody"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing character error = %v, want ErrNotFound", err)
	}
}

func TestD1StoreCharacterManualOverrideOverlay(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if _, err := conn.Exec(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, appearance_json, turn_index)
		VALUES ('s1', 'hero', '{"hair":"brown"}', 1)`); err != nil {
		t.Fatalf("seed hero: %v", err)
	}

	// Durable operator edits live in character_events, not in the derived
	// snapshot, so the overlay is applied on read. An older override followed by
	// a newer one must resolve to the newer edit only.
	if _, err := conn.Exec(ctx, `INSERT INTO character_events
		(chat_session_id, character_name, turn_index, event_type, details_json)
		VALUES ('s1', 'hero', 2, 'manual_character_override', '{"edits":[{"path":["appearance","hair"],"value":"blonde"}]}')`); err != nil {
		t.Fatalf("seed first override: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO character_events
		(chat_session_id, character_name, turn_index, event_type, details_json)
		VALUES ('s1', 'hero', 5, 'manual_character_override', '{"edits":[{"path":["appearance","hair"],"value":"red"}]}')`); err != nil {
		t.Fatalf("seed second override: %v", err)
	}
	// A non-override event must be ignored entirely.
	if _, err := conn.Exec(ctx, `INSERT INTO character_events
		(chat_session_id, character_name, turn_index, event_type, details_json)
		VALUES ('s1', 'hero', 6, 'state_change', '{"edits":[{"path":["appearance","hair"],"value":"green"}]}')`); err != nil {
		t.Fatalf("seed unrelated event: %v", err)
	}

	fetched, err := st.GetCharacterState(ctx, "s1", "hero")
	if err != nil {
		t.Fatalf("GetCharacterState: %v", err)
	}
	if !strings.Contains(fetched.AppearanceJSON, "red") {
		t.Errorf("the newest override must win: %s", fetched.AppearanceJSON)
	}
	if strings.Contains(fetched.AppearanceJSON, "blonde") || strings.Contains(fetched.AppearanceJSON, "green") {
		t.Errorf("stale or unrelated edits leaked into the overlay: %s", fetched.AppearanceJSON)
	}

	// The same overlay must apply through the list read.
	states, err := st.ListCharacterStates(ctx, "s1")
	if err != nil {
		t.Fatalf("ListCharacterStates: %v", err)
	}
	if len(states) != 1 || !strings.Contains(states[0].AppearanceJSON, "red") {
		t.Errorf("list read must apply the same overlay: %+v", states)
	}

	// An override for a character with no derived snapshot still surfaces, which
	// is how an operator-only character remains visible.
	if _, err := conn.Exec(ctx, `INSERT INTO character_events
		(chat_session_id, character_name, turn_index, event_type, details_json)
		VALUES ('s1', 'ghost', 7, 'manual_character_override', '{"edits":[{"path":["status","mood"],"value":"uneasy"}]}')`); err != nil {
		t.Fatalf("seed ghost override: %v", err)
	}
	afterGhost, err := st.ListCharacterStates(ctx, "s1")
	if err != nil {
		t.Fatalf("ListCharacterStates after ghost: %v", err)
	}
	found := false
	for _, state := range afterGhost {
		if state.CharacterName == "ghost" {
			found = true
			if !strings.Contains(state.StatusJSON, "uneasy") {
				t.Errorf("ghost override not applied: %s", state.StatusJSON)
			}
		}
	}
	if !found {
		t.Error("an override for a character without a snapshot must still appear")
	}

	// A NULL details_json carries no edits and must not fail the read.
	if _, err := conn.Exec(ctx, `INSERT INTO character_events
		(chat_session_id, character_name, turn_index, event_type, details_json)
		VALUES ('s1', 'nulled', 8, 'manual_character_override', NULL)`); err != nil {
		t.Fatalf("seed null override: %v", err)
	}
	if _, err := st.ListCharacterStates(ctx, "s1"); err != nil {
		t.Fatalf("a NULL override payload must not break the read: %v", err)
	}
}

func TestD1StoreStorylines(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for _, spec := range []struct {
		name     string
		lastTurn int
	}{
		{"early", 1},
		{"late", 5},
		{"middle", 3},
	} {
		if _, err := conn.Exec(ctx, `INSERT INTO storylines
			(chat_session_id, name, last_turn, first_turn) VALUES ('s1', ?, ?, 0)`,
			spec.name, spec.lastTurn); err != nil {
			t.Fatalf("seed %s: %v", spec.name, err)
		}
	}
	if _, err := conn.Exec(ctx, `UPDATE storylines
		SET entities_json = '["a"]', confidence = 0.75, evidence_count = 4
		WHERE chat_session_id = 's1' AND name = 'late'`); err != nil {
		t.Fatalf("populate nullable columns: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO storylines (chat_session_id, name, last_turn) VALUES ('s2', 'other', 9)`); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	rows, err := st.ListStorylines(ctx, "s1")
	if err != nil {
		t.Fatalf("ListStorylines: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("storylines = %d, want 3 (session isolation)", len(rows))
	}
	for i, want := range []string{"late", "middle", "early"} {
		if rows[i].Name != want {
			t.Errorf("row %d = %q, want %q (last_turn DESC)", i, rows[i].Name, want)
		}
	}
	if rows[0].EntitiesJSON != `["a"]` || rows[0].Confidence != 0.75 || rows[0].EvidenceCount != 4 {
		t.Errorf("nullable columns did not round-trip: %+v", rows[0])
	}
	if rows[2].EntitiesJSON != "" || rows[2].Confidence != 0 || rows[2].EvidenceCount != 0 {
		t.Errorf("NULL columns must read as zero values: %+v", rows[2])
	}
}

func TestD1StoreListWorldRulesKeepsLatestPerGroup(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	// Two revisions of the same (scope, key, scope_name) group: only the newest
	// source_turn survives.
	if _, err := conn.Exec(ctx, `INSERT INTO world_rules
		(chat_session_id, scope, scope_name, category, "key", value_json, source_turn)
		VALUES ('s1', 'root', NULL, 'custom', 'k1', 'v1', 1)`); err != nil {
		t.Fatalf("seed k1 v1: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO world_rules
		(chat_session_id, scope, scope_name, category, "key", value_json, source_turn)
		VALUES ('s1', 'root', NULL, 'custom', 'k1', 'v2', 2)`); err != nil {
		t.Fatalf("seed k1 v2: %v", err)
	}
	// A named scope_name is a different group even with the same scope and key,
	// which is what MariaDB's <=> null-safe comparison expresses.
	if _, err := conn.Exec(ctx, `INSERT INTO world_rules
		(chat_session_id, scope, scope_name, category, "key", value_json, source_turn)
		VALUES ('s1', 'root', 'north', 'custom', 'k1', 'v-named', 1)`); err != nil {
		t.Fatalf("seed named: %v", err)
	}

	rules, err := st.ListWorldRules(ctx, "s1")
	if err != nil {
		t.Fatalf("ListWorldRules: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("rules = %d, want 2 (one per null-safe group)", len(rules))
	}
	byName := map[string]string{}
	for _, rule := range rules {
		byName[rule.ScopeName] = rule.ValueJSON
	}
	if byName[""] != "v2" {
		t.Errorf("unnamed group value = %q, want the newest revision v2", byName[""])
	}
	if byName["north"] != "v-named" {
		t.Errorf("named group value = %q, want v-named", byName["north"])
	}
}

func TestD1StoreListInheritedWorldRulesChainAndFilters(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	seed := func(scope, scopeName, key string, sourceTurn int, suppressed int, pinned int) {
		t.Helper()
		if _, err := conn.Exec(ctx, `INSERT INTO world_rules
			(chat_session_id, scope, scope_name, category, "key", value_json, source_turn, suppressed, pinned)
			VALUES ('s1', ?, ?, 'custom', ?, 'v', ?, ?, ?)`,
			scope, d1NullableString(scopeName), key, sourceTurn, suppressed, pinned); err != nil {
			t.Fatalf("seed %s/%s/%s: %v", scope, scopeName, key, err)
		}
	}
	seed("location", "", "loc", 1, 0, 0)
	seed("region", "", "reg", 1, 0, 0)
	seed("root", "", "roots", 1, 0, 0)
	seed("session", "", "sess", 1, 0, 0)
	seed("faction", "", "out-of-chain", 1, 0, 0)
	seed("location", "", "suppressed", 1, 1, 0)
	seed("location", "north", "named", 1, 0, 0)
	seed("location", "", "pinned", 1, 0, 1)

	// The chain for location is location -> region -> root -> session.
	rules, err := st.ListInheritedWorldRules(ctx, "s1", "location", "")
	if err != nil {
		t.Fatalf("ListInheritedWorldRules: %v", err)
	}
	keys := make([]string, 0, len(rules))
	for _, rule := range rules {
		keys = append(keys, rule.Key)
	}
	contains := func(list []string, want string) bool {
		for _, item := range list {
			if item == want {
				return true
			}
		}
		return false
	}
	if contains(keys, "out-of-chain") {
		t.Errorf("faction rule must be excluded from the location chain: %v", keys)
	}
	if contains(keys, "suppressed") {
		t.Errorf("suppressed rule must be excluded: %v", keys)
	}
	if contains(keys, "named") {
		t.Errorf("named scope_name must be excluded when no scope name is requested: %v", keys)
	}
	for _, want := range []string{"pinned", "loc", "reg", "roots", "sess"} {
		if !contains(keys, want) {
			t.Errorf("missing %q in inherited rules: %v", want, keys)
		}
	}
	// location comes before its ancestors, and a pinned rule leads its scope.
	if len(rules) == 0 || rules[0].Key != "pinned" {
		t.Fatalf("pinned rule must lead the chain order: %v", keys)
	}
	scopeOrder := map[string]int{"location": 0, "region": 1, "root": 2, "session": 3}
	previous := -1
	for _, rule := range rules {
		position := scopeOrder[rule.Scope]
		if position < previous {
			t.Errorf("chain proximity ordering broken at %q: %v", rule.Scope, keys)
		}
		previous = position
	}

	// An explicit scope name narrows the active scope only.
	named, err := st.ListInheritedWorldRules(ctx, "s1", "location", "north")
	if err != nil {
		t.Fatalf("ListInheritedWorldRules named: %v", err)
	}
	for _, rule := range named {
		if rule.Scope == "location" && rule.ScopeName != "north" {
			t.Errorf("named request leaked an unnamed location rule: %+v", rule)
		}
		if rule.Scope != "location" && rule.ScopeName != "" {
			t.Errorf("ancestor scopes must stay unnamed: %+v", rule)
		}
	}
}

func TestD1StoreActiveScopeFallbackAndUpsert(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if _, err := st.GetActiveScope(ctx, "s1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing active scope error = %v, want ErrNotFound", err)
	}

	if err := st.UpsertActiveScope(ctx, &SessionActiveScope{
		ChatSessionID: "s1", ActiveScope: "location", ScopeName: "north",
	}); err != nil {
		t.Fatalf("UpsertActiveScope: %v", err)
	}
	saved, err := st.GetActiveScope(ctx, "s1")
	if err != nil {
		t.Fatalf("GetActiveScope: %v", err)
	}
	if saved.ActiveScope != "location" || saved.ScopeName != "north" {
		t.Errorf("active scope = %+v", saved)
	}

	// A second upsert must update in place, not insert a duplicate row.
	if err := st.UpsertActiveScope(ctx, &SessionActiveScope{
		ChatSessionID: "s1", ActiveScope: "region",
	}); err != nil {
		t.Fatalf("second UpsertActiveScope: %v", err)
	}
	updated, err := st.GetActiveScope(ctx, "s1")
	if err != nil {
		t.Fatalf("GetActiveScope after update: %v", err)
	}
	if updated.ID != saved.ID || updated.ActiveScope != "region" || updated.ScopeName != "" {
		t.Errorf("upsert must update the same row: before=%+v after=%+v", saved, updated)
	}

	if err := st.UpsertActiveScope(ctx, nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("nil upsert error = %v, want ErrNotFound", err)
	}
}

// TestD1CanonicalSchemaIndexesAndKeys guards four defects that an independent
// cross-check of the generated schema found. Each one is silent at generation
// time and only appears as missing behaviour at runtime.
func TestD1CanonicalSchemaIndexesAndKeys(t *testing.T) {
	db := d1ApplyAllMigrations(t)
	ctx := context.Background()

	// 1. SQLite index names are database-global while MariaDB index names are
	//    per-table, so a repeated name plus IF NOT EXISTS silently dropped 27
	//    indexes and left tables such as memories unindexed.
	//    The count spans every tracked migration because sqlite_master aggregates
	//    them all, including the reset control plane.
	paths, err := filepath.Glob(filepath.Join(d1MigrationDir(t), "*.sql"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no migration files found: %v", err)
	}
	declared := 0
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, line := range strings.Split(strings.ReplaceAll(string(content), "\r\n", "\n"), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "CREATE INDEX") ||
				strings.HasPrefix(strings.TrimSpace(line), "CREATE UNIQUE INDEX") {
				declared++
			}
		}
	}
	var actual int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'",
	).Scan(&actual); err != nil {
		t.Fatalf("count indexes: %v", err)
	}
	if declared == 0 {
		t.Fatal("no CREATE INDEX statements found in the canonical schema")
	}
	if declared != actual {
		t.Errorf("schema declares %d indexes but SQLite created %d; a name collision is silently dropping indexes", declared, actual)
	}

	info, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	var tableNames []string
	for info.Next() {
		var name string
		if err := info.Scan(&name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		tableNames = append(tableNames, name)
	}
	info.Close()

	// 2. SQLite does not make a non-INTEGER column-level PRIMARY KEY implicitly
	//    NOT NULL, so a NULL key would be accepted where MariaDB rejects it.
	var nonIntegerPKTable, nonIntegerPKColumn string
	for _, table := range tableNames {
		columns, err := db.QueryContext(ctx, "PRAGMA table_info("+d1QuoteIdent(table)+")")
		if err != nil {
			t.Fatalf("table_info %s: %v", table, err)
		}
		for columns.Next() {
			var cid, notNull, pk int
			var name, columnType string
			var defaultValue any
			if err := columns.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
				columns.Close()
				t.Fatalf("scan table_info %s: %v", table, err)
			}
			if pk == 1 && columnType != "INTEGER" {
				if nonIntegerPKTable == "" {
					nonIntegerPKTable, nonIntegerPKColumn = table, name
				}
				if notNull == 0 {
					t.Errorf("column %s.%s is a %s PRIMARY KEY without NOT NULL; MariaDB implies NOT NULL", table, name, columnType)
				}
			}
		}
		columns.Close()
	}

	// The NOT NULL must actually reject a NULL key, not just be declared.
	if nonIntegerPKTable != "" {
		if _, err := db.ExecContext(ctx, "INSERT INTO "+d1QuoteIdent(nonIntegerPKTable)+
			" ("+d1QuoteIdent(nonIntegerPKColumn)+") VALUES (NULL)"); err == nil {
			t.Errorf("inserting NULL into %s.%s succeeded; the primary key must be NOT NULL", nonIntegerPKTable, nonIntegerPKColumn)
		}
	}

	// 3. A database-generated timestamp must be readable by RFC3339-only callers,
	//    which MariaDB's DATETIME(3) default satisfies and SQLite's
	//    CURRENT_TIMESTAMP does not.
	if _, err := db.ExecContext(ctx, `CREATE TABLE ts_probe (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')))`); err != nil {
		t.Fatalf("create timestamp probe: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO ts_probe DEFAULT VALUES"); err != nil {
		t.Fatalf("insert timestamp probe: %v", err)
	}
	var generated string
	if err := db.QueryRowContext(ctx, "SELECT created_at FROM ts_probe").Scan(&generated); err != nil {
		t.Fatalf("read generated timestamp: %v", err)
	}
	if _, err := time.Parse(time.RFC3339, generated); err != nil {
		t.Errorf("database-generated timestamp %q is not RFC3339: %v", generated, err)
	}

	// 4. The canonical schema must not carry a duplicate UNIQUE column set, which
	//    would create two identical autoindexes. Index metadata is gathered in
	//    two phases because the test pool holds a single connection, so a nested
	//    query inside an open cursor would deadlock.
	for _, table := range tableNames {
		indexes, err := db.QueryContext(ctx, "PRAGMA index_list("+d1QuoteIdent(table)+")")
		if err != nil {
			t.Fatalf("index_list %s: %v", table, err)
		}
		var uniqueIndexNames []string
		for indexes.Next() {
			var seq, unique, partial int
			var name, origin string
			if err := indexes.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
				indexes.Close()
				t.Fatalf("scan index_list %s: %v", table, err)
			}
			if origin == "u" {
				uniqueIndexNames = append(uniqueIndexNames, name)
			}
		}
		indexes.Close()

		seen := map[string]bool{}
		for _, name := range uniqueIndexNames {
			columns, err := db.QueryContext(ctx, "PRAGMA index_info("+d1QuoteIdent(name)+")")
			if err != nil {
				t.Fatalf("index_info %s: %v", name, err)
			}
			var cols []string
			for columns.Next() {
				var seqNo, cid int
				var colName string
				if err := columns.Scan(&seqNo, &cid, &colName); err != nil {
					columns.Close()
					t.Fatalf("scan index_info %s: %v", name, err)
				}
				cols = append(cols, colName)
			}
			columns.Close()
			key := strings.Join(cols, ",")
			if seen[key] {
				t.Errorf("table %s has a duplicate UNIQUE constraint on (%s)", table, key)
			}
			seen[key] = true
		}
	}
}

// TestD1CanonicalSchemaTimestampDefaultMatchesStoreFormat ties the schema default
// shape to the format the D1 store writes, so a DB-defaulted row and an
// application-written row stay interchangeable.
func TestD1CanonicalSchemaTimestampDefaultMatchesStoreFormat(t *testing.T) {
	fixed := time.Date(2026, 3, 4, 5, 6, 7, 123000000, time.UTC).Format(d1TimeLayout)
	if fixed != "2026-03-04T05:06:07.123Z" {
		t.Errorf("d1TimeLayout output = %q, want millisecond RFC3339", fixed)
	}
	if _, err := parseD1Time(fixed); err != nil {
		t.Errorf("d1TimeLayout output must round-trip: %v", err)
	}
	if written := d1TimeValue(time.Time{}); written == "" {
		t.Error("d1TimeValue must render a zero time as the current UTC time")
	}
}

// TestD1StoreTimeParsing covers both the RFC3339 representation the store writes
// and the space-separated form a SQLite CURRENT_TIMESTAMP default produces.
func TestD1StoreTimeParsing(t *testing.T) {
	cases := []struct {
		input string
		want  time.Time
	}{
		{"2026-03-04T05:06:07.123Z", time.Date(2026, 3, 4, 5, 6, 7, 123000000, time.UTC)},
		{"2026-03-04 05:06:07", time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)},
	}
	for _, tc := range cases {
		got, err := parseD1Time(tc.input)
		if err != nil {
			t.Errorf("parseD1Time(%q): %v", tc.input, err)
			continue
		}
		if !got.Equal(tc.want) {
			t.Errorf("parseD1Time(%q) = %s, want %s", tc.input, got, tc.want)
		}
	}
	if _, err := parseD1Time("not-a-time"); err == nil {
		t.Error("parseD1Time must reject an unparseable timestamp")
	}
}

// TestD1StorePendingThreadStatusFiltering pins the three status lanes the MariaDB
// path exposes: an explicit status, "all", and the default open/paused set.
func TestD1StorePendingThreadStatusFiltering(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	rows := []struct {
		key    string
		status string
		source int
		pinned int
	}{
		{"open-1", "open", 2, 0},
		{"paused-1", "paused", 3, 0},
		{"resolved-1", "resolved", 4, 0},
		{"closed-1", "closed", 5, 0},
		{"pinned-1", "open", 1, 1},
	}
	for _, row := range rows {
		if _, err := conn.Exec(ctx, `INSERT INTO pending_threads
			(chat_session_id, thread_key, status, source_turn, pinned)
			VALUES ('s1', ?, ?, ?, ?)`, row.key, row.status, row.source, row.pinned); err != nil {
			t.Fatalf("seed %s: %v", row.key, err)
		}
	}

	defaultLanes, err := st.ListPendingThreads(ctx, "s1", "")
	if err != nil {
		t.Fatalf("ListPendingThreads default: %v", err)
	}
	if len(defaultLanes) != 3 {
		t.Fatalf("default lanes = %d, want 3 (open/paused)", len(defaultLanes))
	}
	// pinned DESC dominates, then source_turn DESC.
	if defaultLanes[0].ThreadKey != "pinned-1" {
		t.Errorf("first row = %q, want the pinned thread first", defaultLanes[0].ThreadKey)
	}
	if defaultLanes[1].ThreadKey != "paused-1" || defaultLanes[2].ThreadKey != "open-1" {
		t.Errorf("source_turn DESC ordering broken: %q, %q", defaultLanes[1].ThreadKey, defaultLanes[2].ThreadKey)
	}

	all, err := st.ListPendingThreads(ctx, "s1", "all")
	if err != nil {
		t.Fatalf("ListPendingThreads all: %v", err)
	}
	if len(all) != 5 {
		t.Errorf("all = %d, want 5", len(all))
	}

	explicit, err := st.ListPendingThreads(ctx, "s1", "resolved")
	if err != nil {
		t.Fatalf("ListPendingThreads resolved: %v", err)
	}
	if len(explicit) != 1 || explicit[0].ThreadKey != "resolved-1" {
		t.Errorf("explicit status filter = %+v", explicit)
	}
}

func TestD1StoreActiveStatesAndCanonicalLayers(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for i, spec := range []struct {
		stateType string
		turn      int
	}{{"mood", 1}, {"mood", 3}, {"location", 2}} {
		if _, err := conn.Exec(ctx, `INSERT INTO active_states
			(chat_session_id, state_type, content, turn_index) VALUES ('s1', ?, ?, ?)`,
			spec.stateType, "content", spec.turn); err != nil {
			t.Fatalf("seed active state %d: %v", i, err)
		}
	}

	allStates, err := st.ListActiveStates(ctx, "s1", "")
	if err != nil {
		t.Fatalf("ListActiveStates all: %v", err)
	}
	if len(allStates) != 3 {
		t.Fatalf("active states = %d, want 3", len(allStates))
	}
	if allStates[0].TurnIndex != 3 {
		t.Errorf("turn_index DESC ordering broken: first turn = %d", allStates[0].TurnIndex)
	}

	moods, err := st.ListActiveStates(ctx, "s1", "mood")
	if err != nil {
		t.Fatalf("ListActiveStates mood: %v", err)
	}
	if len(moods) != 2 {
		t.Errorf("mood states = %d, want 2", len(moods))
	}

	// canonical_state_layers carries nullable numeric columns that must read as
	// zero rather than failing the scan.
	if _, err := conn.Exec(ctx, `INSERT INTO canonical_state_layers
		(chat_session_id, layer_type, content, turn_index) VALUES ('s1', 'summary', 'c', 7)`); err != nil {
		t.Fatalf("seed layer: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO canonical_state_layers
		(chat_session_id, layer_type, content, source_state_type, turn_index, source_turn, source_record, last_verified_turn, confidence)
		VALUES ('s1', 'detail', 'c', 'mood', 8, 4, 42, 5, 0.75)`); err != nil {
		t.Fatalf("seed detailed layer: %v", err)
	}

	layers, err := st.ListCanonicalStateLayers(ctx, "s1", "")
	if err != nil {
		t.Fatalf("ListCanonicalStateLayers: %v", err)
	}
	if len(layers) != 2 {
		t.Fatalf("layers = %d, want 2", len(layers))
	}
	if layers[0].TurnIndex != 8 || layers[0].SourceRecord != 42 || layers[0].Confidence != 0.75 {
		t.Errorf("detailed layer did not round-trip: %+v", layers[0])
	}
	if layers[1].SourceStateType != "" || layers[1].SourceRecord != 0 || layers[1].Confidence != 0 {
		t.Errorf("NULL numeric layer columns must read as zero: %+v", layers[1])
	}

	details, err := st.ListCanonicalStateLayers(ctx, "s1", "detail")
	if err != nil {
		t.Fatalf("ListCanonicalStateLayers detail: %v", err)
	}
	if len(details) != 1 || details[0].LayerType != "detail" {
		t.Errorf("layer filter = %+v", details)
	}
}

func TestD1StoreEpisodeSummaries(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for _, spec := range []struct {
		from, to int
	}{{1, 5}, {6, 10}, {11, 20}} {
		if _, err := conn.Exec(ctx, `INSERT INTO episode_summaries
			(chat_session_id, from_turn, to_turn, summary_text, key_entities, embedding_model)
			VALUES ('s1', ?, ?, ?, '["a"]', 'm1')`, spec.from, spec.to, "summary"); err != nil {
			t.Fatalf("seed episode: %v", err)
		}
	}
	if _, err := conn.Exec(ctx, `INSERT INTO episode_summaries
		(chat_session_id, from_turn, to_turn, summary_text) VALUES ('s2', 1, 2, 'other')`); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	all, err := st.ListEpisodeSummaries(ctx, "s1", 0, 0, 0)
	if err != nil {
		t.Fatalf("ListEpisodeSummaries: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("episodes = %d, want 3", len(all))
	}
	if all[0].ToTurn != 20 {
		t.Errorf("to_turn DESC ordering broken: first = %d", all[0].ToTurn)
	}
	if all[0].KeyEntities != `["a"]` || all[0].EmbeddingModel != "m1" {
		t.Errorf("nullable columns did not round-trip: %+v", all[0])
	}
	if all[2].OpenLoopsJSON != "" || all[2].EmbeddingVector != "" {
		t.Errorf("NULL text columns must read as empty: %+v", all[2])
	}

	limited, err := st.ListEpisodeSummaries(ctx, "s1", 2, 0, 0)
	if err != nil {
		t.Fatalf("limited ListEpisodeSummaries: %v", err)
	}
	if len(limited) != 2 {
		t.Errorf("limit = %d, want 2", len(limited))
	}

	bounded, err := st.ListEpisodeSummaries(ctx, "s1", 0, 6, 15)
	if err != nil {
		t.Fatalf("bounded ListEpisodeSummaries: %v", err)
	}
	if len(bounded) != 1 || bounded[0].FromTurn != 6 {
		t.Errorf("range filter = %+v", bounded)
	}

	fetched, err := st.GetEpisodeSummary(ctx, all[0].ID)
	if err != nil {
		t.Fatalf("GetEpisodeSummary: %v", err)
	}
	if fetched.ID != all[0].ID || fetched.SummaryText != "summary" {
		t.Errorf("GetEpisodeSummary = %+v", fetched)
	}
	if _, err := st.GetEpisodeSummary(ctx, 999999); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing episode error = %v, want ErrNotFound", err)
	}
}
