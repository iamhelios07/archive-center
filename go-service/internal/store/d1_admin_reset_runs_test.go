package store

import (
	"context"
	"testing"
)

// seedAdminResetRun writes one row of the reset control plane the way ResetAll
// does, so these tests read what the reset actually persists rather than a shape
// invented here.
func seedAdminResetRun(t *testing.T, conn *sqliteD1Conn, id, status string, epoch, rows, tables int64) {
	t.Helper()
	completed := interface{}(nil)
	if status == "completed" {
		completed = "2026-02-01T00:00:00Z"
	}
	if _, err := conn.Exec(context.Background(), `
		INSERT INTO d1_reset_runs
			(reset_run_id, epoch, status, rows_deleted, tables_cleared,
			 started_at, updated_at, completed_at, retry_count, fencing_token, confirmation)
		VALUES (?, ?, ?, ?, ?, '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z', ?, 2, 7, 'RESET_ARCHIVE_CENTER_DB')`,
		id, epoch, status, rows, tables, completed); err != nil {
		t.Fatalf("seed reset run %q: %v", id, err)
	}
}

// TestD1AdminResetRunReaderReadsBackWhatTheResetPersisted is the point of the
// capability: an operator polls the job list and has to see what the reset
// actually did, including from a process that did not do it.
func TestD1AdminResetRunReaderReadsBackWhatTheResetPersisted(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	seedAdminResetRun(t, conn, "run-completed", "completed", 3, 4200, 72)
	seedAdminResetRun(t, conn, "run-running", "running", 3, 120, 8)

	run, found, err := st.GetAdminResetRun(ctx, "run-completed")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found {
		t.Fatal("get reported found=false for a row the reset wrote")
	}
	if run.Epoch != 3 || run.RowsDeleted != 4200 || run.TablesCleared != 72 {
		t.Errorf("run = %+v; the counts the reset reported must be the counts read back", run)
	}
	if run.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2", run.RetryCount)
	}
	if run.CompletedAt == "" {
		t.Error("a completed run reported no completed_at")
	}
	if !run.Durable {
		t.Error("Durable = false for a row read back from D1; the whole point is that it outlives the process")
	}
	if run.StartedAt != "2026-01-01T00:00:00Z" || run.UpdatedAt != "2026-01-01T00:01:00Z" {
		t.Errorf("timestamps = %q / %q", run.StartedAt, run.UpdatedAt)
	}
}

// TestD1AdminResetRunReaderReportsAMissingRunAsAbsent covers the stale bookmark.
// An operator who kept a run id from yesterday gets 404, not a failure, and
// certainly not a fabricated row.
func TestD1AdminResetRunReaderReportsAMissingRunAsAbsent(t *testing.T) {
	st, _ := newD1TestStore(t)
	run, found, err := st.GetAdminResetRun(context.Background(), "never-existed")
	if err != nil {
		t.Errorf("get of an unknown id returned an error, not found=false: %v", err)
	}
	if found {
		t.Errorf("get of an unknown id reported found with %+v", run)
	}
}

// TestD1AdminResetRunListingIsBoundedAndNewestFirst pins the two properties an
// operator console depends on during an incident. The table only grows, so an
// unbounded read is a way to make the console slow exactly when it is being
// used, and "newest first" is what makes the top of the list the thing that just
// happened.
func TestD1AdminResetRunListingIsBoundedAndNewestFirst(t *testing.T) {
	st, conn := newD1TestStore(t)
	for i := 0; i < 8; i++ {
		id := "run-" + string(rune('a'+i))
		if _, err := conn.Exec(context.Background(), `
			INSERT INTO d1_reset_runs
				(reset_run_id, epoch, status, started_at, updated_at, retry_count, fencing_token, confirmation)
			VALUES (?, 1, 'completed', ?, ?, 0, 1, 'x')`,
			id, "2026-01-0"+string(rune('1'+i))+"T00:00:00Z", "2026-01-01T00:00:00Z"); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
	ctx := context.Background()

	all, err := st.ListAdminResetRuns(ctx, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 8 {
		t.Fatalf("list with no limit returned %d runs, want all 8", len(all))
	}
	if all[0].ResetRunID != "run-h" {
		t.Errorf("first run = %q, want the newest (run-h)", all[0].ResetRunID)
	}

	limited, err := st.ListAdminResetRuns(ctx, 3)
	if err != nil {
		t.Fatalf("list with a limit: %v", err)
	}
	if len(limited) != 3 {
		t.Errorf("limit=3 returned %d runs", len(limited))
	}

	// A caller asking for the whole table is capped rather than obeyed. This is a
	// console endpoint polled during an incident.
	capped, err := st.ListAdminResetRuns(ctx, 100000)
	if err != nil {
		t.Fatalf("list with an oversized limit: %v", err)
	}
	if len(capped) > 200 {
		t.Errorf("limit=100000 returned %d runs; the cap was not applied", len(capped))
	}
}

// TestD1AdminResetRunsAreNotDeletedByReset is the property that makes the control
// plane worth having: a reset that erased its own record would be a reset whose
// completion nobody could verify.
func TestD1AdminResetRunsAreNotDeletedByReset(t *testing.T) {
	st, conn := newD1TestStore(t)
	seedAdminResetRun(t, conn, "run-before-reset", "completed", 1, 10, 2)
	d1SeedAllowlistRows(t, conn, "s1", 5)

	if _, err := st.ResetAll(context.Background()); err != nil {
		t.Fatalf("ResetAll: %v", err)
	}
	if _, found, err := st.GetAdminResetRun(context.Background(), "run-before-reset"); err != nil {
		t.Fatalf("get after reset: %v", err)
	} else if !found {
		t.Error("the reset deleted its own run record; a reset whose completion cannot be verified is not auditable")
	}
}
