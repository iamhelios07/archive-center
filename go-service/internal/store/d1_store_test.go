package store

import (
	"context"
	"database/sql"
	"errors"
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

// TestD1StoreUnimplementedMethodsFailLoudly guards the no-silent-success rule:
// an unfinished canonical path must return an error, never a nil result that a
// caller could mistake for a completed write.
func TestD1StoreUnimplementedMethodsFailLoudly(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if err := st.SaveEvidence(ctx, &DirectEvidence{}); !errors.Is(err, errD1Unimplemented) {
		t.Errorf("SaveEvidence error = %v, want errD1Unimplemented", err)
	}
	if _, err := st.ListStorylines(ctx, "s1"); !errors.Is(err, errD1Unimplemented) {
		t.Errorf("ListStorylines error = %v, want errD1Unimplemented", err)
	}
	if _, err := st.Stats(ctx); !errors.Is(err, errD1Unimplemented) {
		t.Errorf("Stats error = %v, want errD1Unimplemented", err)
	}
}

func TestNewD1StoreRejectsNilConn(t *testing.T) {
	if _, err := NewD1Store(nil); err == nil {
		t.Error("NewD1Store(nil) must fail")
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
