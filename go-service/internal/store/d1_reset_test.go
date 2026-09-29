package store

import (
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// d1MigrationDir locates the tracked D1 migration directory from the store
// package (go-service/internal/store -> repository root).
func d1MigrationDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join("..", "..", "..", "deploy", "cloudflare", "migrations")
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("D1 migration directory missing at %s: %v", dir, err)
	}
	return dir
}

// d1SplitStatements splits a SQL script on top-level semicolons, dropping line
// comments and respecting single-quoted strings. It is deliberately small: the
// D1 migrations contain no triggers or stored procedures.
func d1SplitStatements(script string) []string {
	var out []string
	var b strings.Builder
	inQuote := false
	for _, line := range strings.Split(script, "\n") {
		if !inQuote {
			if idx := strings.Index(line, "--"); idx >= 0 {
				line = line[:idx]
			}
		}
		for i := 0; i < len(line); i++ {
			ch := line[i]
			if ch == '\'' {
				if inQuote && i+1 < len(line) && line[i+1] == '\'' {
					b.WriteByte(ch)
					b.WriteByte(line[i+1])
					i++
					continue
				}
				inQuote = !inQuote
			}
			if ch == ';' && !inQuote {
				if stmt := strings.TrimSpace(b.String()); stmt != "" {
					out = append(out, stmt)
				}
				b.Reset()
				continue
			}
			b.WriteByte(ch)
		}
		b.WriteByte('\n')
	}
	if stmt := strings.TrimSpace(b.String()); stmt != "" {
		out = append(out, stmt)
	}
	return out
}

func d1ApplyScript(t *testing.T, db *sql.DB, label, script string) {
	t.Helper()
	for i, stmt := range d1SplitStatements(script) {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s statement %d failed: %v\n%s", label, i+1, err, stmt)
		}
	}
}

// d1ApplyAllMigrations applies every tracked D1 migration in filename order,
// matching how wrangler D1 applies them.
func d1ApplyAllMigrations(t *testing.T) *sql.DB {
	t.Helper()
	db := d1TestDB(t)
	paths, err := filepath.Glob(filepath.Join(d1MigrationDir(t), "*.sql"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no D1 migrations found: %v", err)
	}
	sort.Strings(paths)
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		d1ApplyScript(t, db, filepath.Base(path), string(content))
	}
	return db
}

// TestD1ResetAllowlistMatchesMariaDB pins the two providers to one registry. A
// silent divergence would reset different data on Cloudflare than locally.
func TestD1ResetAllowlistMatchesMariaDB(t *testing.T) {
	if len(d1AdminResetTables) != len(mariaAdminResetTables) {
		t.Fatalf("D1 reset allowlist has %d tables, MariaDB has %d", len(d1AdminResetTables), len(mariaAdminResetTables))
	}
	for i := range mariaAdminResetTables {
		if d1AdminResetTables[i] != mariaAdminResetTables[i] {
			t.Errorf("allowlist position %d: D1=%q MariaDB=%q", i, d1AdminResetTables[i], mariaAdminResetTables[i])
		}
	}
}

// TestD1ResetAllowlistExcludesControlPlaneAndReservedNames guards the reset
// scope: control-plane tables own reset progress, and sqlite_* objects hold
// AUTOINCREMENT counters and schema that a reset must not delete.
func TestD1ResetAllowlistExcludesControlPlaneAndReservedNames(t *testing.T) {
	inAllowlist := map[string]bool{}
	for _, name := range d1AdminResetTables {
		if inAllowlist[name] {
			t.Errorf("duplicate allowlist entry %q", name)
		}
		inAllowlist[name] = true
	}
	for _, reserved := range d1ResetControlPlaneTables {
		if inAllowlist[reserved] {
			t.Errorf("control-plane table %q must not be in the reset allowlist", reserved)
		}
		if !d1ResetReservedName(reserved) && !strings.HasPrefix(reserved, "d1_") {
			t.Errorf("control-plane table %q is not recognisably control plane", reserved)
		}
	}
	for _, name := range d1AdminResetTables {
		if d1ResetReservedName(name) {
			t.Errorf("allowlist entry %q targets a SQLite internal object", name)
		}
	}
	if d1ResetReservedName("sqlite_sequence") != true || d1ResetReservedName("sqlite_schema") != true {
		t.Error("d1ResetReservedName must reject sqlite_* internals")
	}
	if d1ResetReservedName("chat_logs") {
		t.Error("d1ResetReservedName must accept application tables")
	}
}

// TestD1ResetControlPlaneEnforcesSingleActiveRun verifies the partial unique
// index blocks a second concurrent reset while completed and failed runs remain
// as history.
func TestD1ResetControlPlaneEnforcesSingleActiveRun(t *testing.T) {
	db := d1TestDB(t)
	content, err := os.ReadFile(filepath.Join(d1MigrationDir(t), "002_reset_control_plane.sql"))
	if err != nil {
		t.Fatalf("read control plane migration: %v", err)
	}
	d1ApplyScript(t, db, "002_reset_control_plane.sql", string(content))

	// The migration seeds one lease row and one epoch row.
	var leases, epochs int
	if err := db.QueryRow("SELECT COUNT(*) FROM d1_maintenance_lease").Scan(&leases); err != nil {
		t.Fatalf("count leases: %v", err)
	}
	if leases != 1 {
		t.Errorf("seeded lease rows = %d, want 1", leases)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM d1_reset_epoch").Scan(&epochs); err != nil {
		t.Fatalf("count epochs: %v", err)
	}
	if epochs != 1 {
		t.Errorf("seeded epoch rows = %d, want 1", epochs)
	}

	insert := `INSERT INTO d1_reset_runs
		(reset_run_id, epoch, status, started_at, updated_at, fencing_token, confirmation)
		VALUES (?, ?, ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 1, 'RESET_ARCHIVE_CENTER_DB')`
	if _, err := db.Exec(insert, "run-1", 1, "running"); err != nil {
		t.Fatalf("insert first running run: %v", err)
	}
	if _, err := db.Exec(insert, "run-2", 2, "running"); err == nil {
		t.Error("a second concurrent running reset must be rejected")
	}
	// purging_vectors is also in-flight, so it must be blocked too. A unique
	// index on "status" alone would wrongly allow this.
	if _, err := db.Exec(insert, "run-3", 3, "purging_vectors"); err == nil {
		t.Error("a purging_vectors run must also block another active reset")
	}
	// Once the active run reaches a terminal state, history and a new run are
	// both allowed.
	if _, err := db.Exec("UPDATE d1_reset_runs SET status = 'completed' WHERE reset_run_id = 'run-1'"); err != nil {
		t.Fatalf("terminal transition: %v", err)
	}
	if _, err := db.Exec(insert, "run-4", 4, "failed"); err != nil {
		t.Fatalf("failed history must be allowed once no run is active: %v", err)
	}
	if _, err := db.Exec(insert, "run-5", 5, "running"); err != nil {
		t.Fatalf("a new run must be allowed once the previous one is terminal: %v", err)
	}
}

// TestD1ResetChunkedDeleteIsResumable exercises the reset cursor contract on a
// real SQLite engine: an interrupted reset resumes from its committed cursor,
// never double-deletes, and preserves AUTOINCREMENT monotonicity.
func TestD1ResetChunkedDeleteIsResumable(t *testing.T) {
	db := d1TestDB(t)
	if _, err := db.Exec(`CREATE TABLE "chat_logs" (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin seed: %v", err)
	}
	stmt, err := tx.Prepare(`INSERT INTO "chat_logs" (v) VALUES (?)`)
	if err != nil {
		t.Fatalf("prepare seed: %v", err)
	}
	const totalRows = 1000
	for i := 0; i < totalRows; i++ {
		if _, err := stmt.Exec("row"); err != nil {
			t.Fatalf("seed row %d: %v", i, err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seed: %v", err)
	}

	// runChunk deletes at most d1ResetChunkSize rows after the cursor and
	// returns the new cursor plus the number of deleted rows.
	runChunk := func(cursor int64) (int64, int) {
		t.Helper()
		rows, err := db.Query(d1ResetChunkSelectSQL("chat_logs"), cursor, d1ResetChunkSize)
		if err != nil {
			t.Fatalf("cursor select: %v", err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan rowid: %v", err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		if len(ids) == 0 {
			return cursor, 0
		}
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin chunk: %v", err)
		}
		res, err := tx.Exec(d1ResetChunkDeleteSQL("chat_logs"), cursor, d1ResetChunkSize)
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("chunk delete: %v", err)
		}
		deleted, _ := res.RowsAffected()
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit chunk: %v", err)
		}
		return ids[len(ids)-1], int(deleted)
	}

	countRows := func() int {
		t.Helper()
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM "chat_logs"`).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	// Simulate an interruption after two committed chunks.
	var cursor int64
	deleted := 0
	for i := 0; i < 2; i++ {
		var n int
		cursor, n = runChunk(cursor)
		deleted += n
	}
	if deleted != 2*d1ResetChunkSize {
		t.Fatalf("two chunks deleted %d rows, want %d", deleted, 2*d1ResetChunkSize)
	}
	if got := countRows(); got != totalRows-deleted {
		t.Fatalf("after interruption remaining = %d, want %d", got, totalRows-deleted)
	}

	// Resume from the persisted cursor.
	for {
		var n int
		cursor, n = runChunk(cursor)
		if n == 0 {
			break
		}
		deleted += n
	}
	if deleted != totalRows {
		t.Fatalf("resumed reset deleted %d rows total, want %d (no double delete)", deleted, totalRows)
	}
	if got := countRows(); got != 0 {
		t.Fatalf("remaining after full reset = %d, want 0", got)
	}

	// DELETE (not TRUNCATE) must preserve AUTOINCREMENT monotonicity, matching
	// MariaDB's DELETE-based ResetAll.
	if _, err := db.Exec(`INSERT INTO "chat_logs" (v) VALUES ('after-reset')`); err != nil {
		t.Fatalf("insert after reset: %v", err)
	}
	var nextID int64
	if err := db.QueryRow(`SELECT id FROM "chat_logs"`).Scan(&nextID); err != nil {
		t.Fatalf("read id: %v", err)
	}
	if nextID <= totalRows {
		t.Errorf("post-reset id = %d, want > %d so ids stay monotonic", nextID, totalRows)
	}
}

// d1CanonicalSchemaPath returns the canonical D1 migration path.
func d1CanonicalSchemaPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(d1MigrationDir(t), "001_canonical_schema.sql")
}

// TestD1CanonicalSchemaCoversResetAllowlist verifies the tracked canonical D1
// schema loads on the same SQLite engine, enforces foreign keys, and keeps every
// reset allowlist table deletable by the reset's rowid cursor.
//
// The canonical migration is generated from migrations/001..014 and is a
// blocking Stage 3 deliverable. Until it exists this test skips loudly instead
// of passing silently.
func TestD1CanonicalSchemaCoversResetAllowlist(t *testing.T) {
	if _, err := os.Stat(d1CanonicalSchemaPath(t)); err != nil {
		t.Skip("BLOCKING Stage 3 deliverable missing: deploy/cloudflare/migrations/001_canonical_schema.sql has not been generated yet")
	}
	db := d1ApplyAllMigrations(t)

	var fkEnabled int
	if err := db.QueryRow("PRAGMA foreign_keys").Scan(&fkEnabled); err != nil {
		t.Fatalf("read foreign_keys pragma: %v", err)
	}
	if fkEnabled != 1 {
		t.Fatal("test harness must run with foreign keys enabled, matching D1")
	}

	// Every allowlist table must exist and be addressable by rowid.
	for _, table := range d1AdminResetTables {
		var name string
		err := db.QueryRow("SELECT name FROM sqlite_schema WHERE type = 'table' AND name = ?", table).Scan(&name)
		if err == sql.ErrNoRows {
			t.Errorf("reset allowlist table %q is missing from the D1 canonical schema", table)
			continue
		}
		if err != nil {
			t.Fatalf("lookup table %q: %v", table, err)
		}
		var rowid int64
		if err := db.QueryRow("SELECT rowid FROM " + d1QuoteIdent(table) + " LIMIT 1").Scan(&rowid); err != nil && err != sql.ErrNoRows {
			t.Errorf("table %q is not rowid-addressable: %v", table, err)
		}
	}

	// No application table may be declared WITHOUT ROWID, or the reset cursor
	// could not use rowid.
	var withoutRowID int
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND sql LIKE '%WITHOUT ROWID%'",
	).Scan(&withoutRowID); err != nil {
		t.Fatalf("count WITHOUT ROWID tables: %v", err)
	}
	if withoutRowID != 0 {
		t.Errorf("found %d WITHOUT ROWID tables; the reset rowid cursor requires rowid tables", withoutRowID)
	}

	// The reset must never target a SQLite internal object.
	for _, table := range d1AdminResetTables {
		if d1ResetReservedName(table) {
			t.Errorf("allowlist table %q is a SQLite internal object", table)
		}
	}

	var tableCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").Scan(&tableCount); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tableCount < len(d1AdminResetTables) {
		t.Errorf("schema defines %d application tables, fewer than the %d allowlist entries", tableCount, len(d1AdminResetTables))
	}
}
