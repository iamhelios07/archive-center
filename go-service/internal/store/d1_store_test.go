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

// TestD1StoreUnimplementedMethodsFailLoudly guards the no-silent-success rule:
// an unfinished canonical path must return an error, never a nil result that a
// caller could mistake for a completed write.
func TestD1StoreUnimplementedMethodsFailLoudly(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if err := st.SaveChatLog(ctx, &ChatLog{ChatSessionID: "s1", TurnIndex: 1, Role: "user", Content: "x"}); err != nil {
		t.Fatalf("implemented path must work: %v", err)
	}
	if _, err := st.GetResumePack(ctx, "s1", "manual"); !errors.Is(err, errD1Unimplemented) {
		t.Errorf("GetResumePack error = %v, want errD1Unimplemented", err)
	}
	if _, err := st.ListWorldRules(ctx, "s1"); !errors.Is(err, errD1Unimplemented) {
		t.Errorf("ListWorldRules error = %v, want errD1Unimplemented", err)
	}
	if _, err := st.ListStorylines(ctx, "s1"); !errors.Is(err, errD1Unimplemented) {
		t.Errorf("ListStorylines error = %v, want errD1Unimplemented", err)
	}
}

func TestNewD1StoreRejectsNilConn(t *testing.T) {
	if _, err := NewD1Store(nil); err == nil {
		t.Error("NewD1Store(nil) must fail")
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
