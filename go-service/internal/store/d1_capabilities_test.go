package store

import (
	"context"
	"strconv"
	"testing"
)

// d1SeedAllowlistRows inserts rows into a few allowlist tables so a reset has
// real work to delete.
func d1SeedAllowlistRows(t *testing.T, conn *sqliteD1Conn, sessionID string, count int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < count; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO chat_logs
			(chat_session_id, turn_index, role, content) VALUES (?, ?, 'user', 'body')`, sessionID, i+1); err != nil {
			t.Fatalf("seed chat_log: %v", err)
		}
	}
	if _, err := conn.Exec(ctx, `INSERT INTO memories (chat_session_id, turn_index, importance) VALUES (?, 1, 0.5)`, sessionID); err != nil {
		t.Fatalf("seed memory: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO kg_triples (chat_session_id, subject, predicate, object) VALUES (?, 'a', 'b', 'c')`, sessionID); err != nil {
		t.Fatalf("seed kg triple: %v", err)
	}
}

// TestD1ResetAllClearsApplicationDataAndPreservesControlPlane verifies the
// operator reset deletes application rows, keeps the control plane it needs to
// be resumable, and leaves AUTOINCREMENT ids monotonic.
func TestD1ResetAllClearsApplicationDataAndPreservesControlPlane(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1SeedAllowlistRows(t, conn, "s1", 12)
	// A completed history row must survive; the reset owns only application data.
	if _, err := conn.Exec(ctx, `INSERT INTO d1_reset_runs
		(reset_run_id, epoch, status, started_at, updated_at, fencing_token, confirmation)
		VALUES ('history-run', 0, 'completed', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 0, 'x')`); err != nil {
		t.Fatalf("seed history run: %v", err)
	}
	var epochBefore int64
	if err := conn.QueryRow(ctx, `SELECT current_epoch FROM d1_reset_epoch WHERE epoch_id = 1`).Scan(&epochBefore); err != nil {
		t.Fatalf("read epoch: %v", err)
	}
	var maxIDBefore int64
	if err := conn.QueryRow(ctx, `SELECT MAX(id) FROM chat_logs`).Scan(&maxIDBefore); err != nil {
		t.Fatalf("read max id: %v", err)
	}

	result, err := st.ResetAll(ctx)
	if err != nil {
		t.Fatalf("ResetAll: %v", err)
	}
	if result.RowsDeleted < 14 {
		t.Errorf("rows deleted = %d, want at least the 14 seeded rows", result.RowsDeleted)
	}
	if result.TablesCleared != len(d1AdminResetTables) {
		t.Errorf("tables cleared = %d, want the whole %d-table allowlist", result.TablesCleared, len(d1AdminResetTables))
	}

	for _, table := range []string{"chat_logs", "memories", "kg_triples", "effective_input_logs"} {
		if got := d1Count(t, conn, `SELECT COUNT(*) FROM `+d1QuoteIdent(table)); got != 0 {
			t.Errorf("%s rows = %d after reset, want 0", table, got)
		}
	}

	// The control plane must survive, or the reset could not report itself or be
	// resumed.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM d1_reset_runs WHERE reset_run_id = 'history-run'`); got != 1 {
		t.Errorf("reset history row was deleted; the control plane is not application data")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM d1_maintenance_lease`); got != 1 {
		t.Errorf("maintenance lease rows = %d, want 1", got)
	}
	var epochAfter int64
	if err := conn.QueryRow(ctx, `SELECT current_epoch FROM d1_reset_epoch WHERE epoch_id = 1`).Scan(&epochAfter); err != nil {
		t.Fatalf("read epoch after reset: %v", err)
	}
	if epochAfter != epochBefore+1 {
		t.Errorf("epoch = %d, want %d", epochAfter, epochBefore+1)
	}
	var purgedEpoch int64
	if err := conn.QueryRow(ctx, `SELECT vector_purged_epoch FROM d1_reset_epoch WHERE epoch_id = 1`).Scan(&purgedEpoch); err != nil {
		t.Fatalf("read purged epoch: %v", err)
	}
	if purgedEpoch >= epochAfter {
		t.Errorf("vector_purged_epoch = %d must stay behind current_epoch %d until Stage 4 purges vectors", purgedEpoch, epochAfter)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM d1_reset_runs WHERE status = 'running'`); got != 0 {
		t.Errorf("running reset rows = %d, want 0 after completion", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM d1_reset_runs WHERE status = 'completed'`); got != 2 {
		t.Errorf("completed reset rows = %d, want 2 (history plus this run)", got)
	}
	// The lease must be released so a later reset is not blocked.
	var holder *string
	if err := conn.QueryRow(ctx, `SELECT holder FROM d1_maintenance_lease WHERE lease_name = 'admin_reset'`).Scan(&holder); err != nil {
		t.Fatalf("read lease holder: %v", err)
	}
	if holder != nil {
		t.Errorf("lease holder = %q, want released", *holder)
	}

	// DELETE, not TRUNCATE: AUTOINCREMENT stays monotonic, matching MariaDB.
	if _, err := conn.Exec(ctx, `INSERT INTO chat_logs (chat_session_id, turn_index, role, content)
		VALUES ('s1', 99, 'user', 'after reset')`); err != nil {
		t.Fatalf("insert after reset: %v", err)
	}
	var newID int64
	if err := conn.QueryRow(ctx, `SELECT id FROM chat_logs`).Scan(&newID); err != nil {
		t.Fatalf("read new id: %v", err)
	}
	if newID <= maxIDBefore {
		t.Errorf("post-reset id = %d, want > %d so ids stay monotonic", newID, maxIDBefore)
	}
}

// TestD1ResetAllResumesInterruptedRun proves the reset continues a persisted
// cursor instead of restarting, which is the durability contract a stateless
// Container needs.
func TestD1ResetAllResumesInterruptedRun(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1SeedAllowlistRows(t, conn, "s1", 20)
	var firstChunkMax int64
	if err := conn.QueryRow(ctx, `SELECT MIN(id) + 4 FROM chat_logs`).Scan(&firstChunkMax); err != nil {
		t.Fatalf("compute cursor: %v", err)
	}
	// Simulate an interrupted run: a 'running' row whose cursor points past the
	// rows an earlier process already committed, with those rows actually gone.
	if _, err := conn.Exec(ctx, `DELETE FROM chat_logs WHERE id <= ?`, firstChunkMax); err != nil {
		t.Fatalf("simulate committed chunk: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO d1_reset_runs
		(reset_run_id, epoch, status, table_index, table_name, last_key, rows_deleted, tables_cleared,
		 started_at, updated_at, fencing_token, confirmation)
		VALUES ('interrupted-run', 7, 'running', 0, 'chat_logs', ?, 5, 0,
		        '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 3, 'RESET_ARCHIVE_CENTER_DB')`,
		strconv.FormatInt(firstChunkMax, 10)); err != nil {
		t.Fatalf("seed interrupted run: %v", err)
	}
	var chatLogsBefore int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM chat_logs`).Scan(&chatLogsBefore); err != nil {
		t.Fatalf("count before: %v", err)
	}

	result, err := st.ResetAll(ctx)
	if err != nil {
		t.Fatalf("ResetAll: %v", err)
	}

	// The interrupted run must be the one that completed, not a replacement.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM d1_reset_runs WHERE reset_run_id = 'interrupted-run' AND status = 'completed'`); got != 1 {
		t.Errorf("the interrupted run must be resumed and completed, not replaced")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM d1_reset_runs`); got != 1 {
		t.Errorf("reset runs = %d, want 1 (a resume must not open a second run)", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs`); got != 0 {
		t.Errorf("chat_logs = %d after resumed reset, want 0", got)
	}
	if result.TablesCleared != len(d1AdminResetTables) {
		t.Errorf("tables cleared = %d, want the whole allowlist", result.TablesCleared)
	}
	if result.RowsDeleted < int64(chatLogsBefore+2) {
		t.Errorf("rows deleted = %d, want at least the %d remaining chat logs plus the other seeded rows",
			result.RowsDeleted, chatLogsBefore)
	}
}

// TestD1ResetAllExcludesSQLiteInternals verifies the allowlist never targets a
// SQLite internal object and the schema survives.
func TestD1ResetAllExcludesSQLiteInternals(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1SeedAllowlistRows(t, conn, "s1", 2)
	if _, err := st.ResetAll(ctx); err != nil {
		t.Fatalf("ResetAll: %v", err)
	}

	// The schema must still be present and usable after a reset.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`); got < len(d1AdminResetTables) {
		t.Errorf("tables after reset = %d, want at least %d; the schema must be preserved", got, len(d1AdminResetTables))
	}
	if _, err := conn.Exec(ctx, `INSERT INTO chat_logs (chat_session_id, turn_index, role, content)
		VALUES ('s1', 1, 'user', 'usable')`); err != nil {
		t.Errorf("the schema must stay usable after a reset: %v", err)
	}
	for _, table := range d1AdminResetTables {
		if d1ResetReservedName(table) {
			t.Fatalf("allowlist contains SQLite internal object %q", table)
		}
	}
}

func TestD1LatestTimelineTurnIndex(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if got, err := st.LatestTimelineTurnIndex(ctx, "s1"); err != nil || got != 0 {
		t.Fatalf("empty session timeline = %d, %v; want 0, nil", got, err)
	}

	if _, err := conn.Exec(ctx, `INSERT INTO chat_logs (chat_session_id, turn_index, role, content) VALUES ('s1', 3, 'user', 'x')`); err != nil {
		t.Fatalf("seed chat log: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO memories (chat_session_id, turn_index) VALUES ('s1', 5)`); err != nil {
		t.Fatalf("seed memory: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO direct_evidence_records
		(chat_session_id, evidence_text, source_turn_start, source_turn_end, turn_anchor) VALUES ('s1', 't', 1, 2, 7)`); err != nil {
		t.Fatalf("seed evidence: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO kg_triples (chat_session_id, subject, predicate, object, source_turn) VALUES ('s1','a','b','c', 4)`); err != nil {
		t.Fatalf("seed triple: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO episode_summaries (chat_session_id, from_turn, to_turn, summary_text) VALUES ('s1', 1, 11, 's')`); err != nil {
		t.Fatalf("seed episode: %v", err)
	}
	// Another session must not contribute.
	if _, err := conn.Exec(ctx, `INSERT INTO chat_logs (chat_session_id, turn_index, role, content) VALUES ('s2', 99, 'user', 'x')`); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	got, err := st.LatestTimelineTurnIndex(ctx, "s1")
	if err != nil {
		t.Fatalf("LatestTimelineTurnIndex: %v", err)
	}
	if got != 11 {
		t.Errorf("timeline turn = %d, want 11 (the newest across all ledgers)", got)
	}
}

func TestD1CountAuditLogsAndListEffectiveInputs(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := conn.Exec(ctx, `INSERT INTO audit_logs (event_type, chat_session_id, summary)
			VALUES ('turn_complete', 's1', 'a')`); err != nil {
			t.Fatalf("seed audit: %v", err)
		}
	}
	if _, err := conn.Exec(ctx, `INSERT INTO audit_logs (event_type, chat_session_id, summary)
		VALUES ('admin_reset', 's1', 'b')`); err != nil {
		t.Fatalf("seed other audit: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO audit_logs (event_type, chat_session_id, summary)
		VALUES ('turn_complete', 's2', 'c')`); err != nil {
		t.Fatalf("seed other session audit: %v", err)
	}

	total, err := st.CountAuditLogs(ctx, "", "")
	if err != nil {
		t.Fatalf("CountAuditLogs all: %v", err)
	}
	if total != 5 {
		t.Errorf("total audit logs = %d, want 5", total)
	}
	byEvent, err := st.CountAuditLogs(ctx, "", "turn_complete")
	if err != nil {
		t.Fatalf("CountAuditLogs by event: %v", err)
	}
	// An empty session filter counts every session: three for s1 plus one for s2.
	if byEvent != 4 {
		t.Errorf("turn_complete count = %d, want 4", byEvent)
	}
	bySessionAndEvent, err := st.CountAuditLogs(ctx, "s1", "turn_complete")
	if err != nil {
		t.Fatalf("CountAuditLogs by session and event: %v", err)
	}
	if bySessionAndEvent != 3 {
		t.Errorf("s1 turn_complete count = %d, want 3", bySessionAndEvent)
	}
	bySession, err := st.CountAuditLogs(ctx, "s2", "")
	if err != nil {
		t.Fatalf("CountAuditLogs by session: %v", err)
	}
	if bySession != 1 {
		t.Errorf("s2 count = %d, want 1", bySession)
	}

	// The counter must agree with the page it accompanies.
	listed, err := st.ListAuditLogs(ctx, "", "turn_complete", 0)
	if err != nil {
		t.Fatalf("ListAuditLogs: %v", err)
	}
	if len(listed) != byEvent {
		t.Errorf("ListAuditLogs returned %d rows but the counter says %d", len(listed), byEvent)
	}

	for _, turn := range []int{1, 2, 4} {
		if err := st.SaveEffectiveInput(ctx, &EffectiveInput{
			ChatSessionID: "s1", TurnIndex: turn, EffectiveInput: "intent",
		}); err != nil {
			t.Fatalf("seed effective input %d: %v", turn, err)
		}
	}
	inputs, err := st.ListEffectiveInputs(ctx, "s1", 0, 0)
	if err != nil {
		t.Fatalf("ListEffectiveInputs: %v", err)
	}
	if len(inputs) != 3 {
		t.Fatalf("effective inputs = %d, want 3", len(inputs))
	}
	for i, want := range []int{1, 2, 4} {
		if inputs[i].TurnIndex != want {
			t.Errorf("input %d turn = %d, want %d (ascending)", i, inputs[i].TurnIndex, want)
		}
	}
	bounded, err := st.ListEffectiveInputs(ctx, "s1", 2, 4)
	if err != nil {
		t.Fatalf("bounded ListEffectiveInputs: %v", err)
	}
	if len(bounded) != 2 || bounded[0].TurnIndex != 2 {
		t.Errorf("bounded inputs = %+v, want turns 2 and 4", bounded)
	}
}
