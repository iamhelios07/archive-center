package store

import (
	"context"
	"encoding/json"
	"testing"
)

// d1AdminJobFixture builds a snapshot shaped like the one the HTTP layer writes,
// so these tests read what a real job produces rather than a shape invented here.
func d1AdminJobFixture(jobID, kind, status string, terminal bool) AdminJobSnapshot {
	wire := map[string]any{
		"contract_version": "admin_background_job.v1",
		"job_id":           jobID,
		"kind":             kind,
		"status":           status,
		"chat_session_id":  "sess-1",
		"started_at":       "2026-02-01T00:00:00Z",
		"updated_at":       "2026-02-01T00:02:00Z",
		"progress":         map[string]any{"status": status, "processed": 12},
	}
	encoded, _ := json.Marshal(wire)
	return AdminJobSnapshot{
		JobID:        jobID,
		Kind:         kind,
		SessionID:    "sess-1",
		Status:       status,
		SnapshotJSON: encoded,
		StartedAt:    "2026-02-01T00:00:00Z",
		UpdatedAt:    "2026-02-01T00:02:00Z",
		Terminal:     terminal,
	}
}

// TestD1AdminJobSnapshotRoundTrip is the plain write/read path.
func TestD1AdminJobSnapshotRoundTrip(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if err := st.SaveAdminJobSnapshot(ctx, d1AdminJobFixture("reindex-1", "reindex", "running", false)); err != nil {
		t.Fatalf("save: %v", err)
	}
	snapshot, found, err := st.GetAdminJobSnapshot(ctx, "reindex-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !found {
		t.Fatal("get reported found=false for a job that was written")
	}
	if snapshot.Kind != "reindex" || snapshot.Status != "running" || snapshot.SessionID != "sess-1" {
		t.Errorf("snapshot = %+v; the columns that queries use must survive the write", snapshot)
	}
	if snapshot.Terminal {
		t.Error("Terminal = true for a running job; the restore path would then never look at it")
	}

	// A later write replaces rather than duplicating. One job is one row, or the
	// table would grow with every progress tick.
	if err := st.SaveAdminJobSnapshot(ctx, d1AdminJobFixture("reindex-1", "reindex", "completed", true)); err != nil {
		t.Fatalf("second save: %v", err)
	}
	all, err := st.ListAdminJobSnapshots(ctx, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("two writes produced %d rows for one job", len(all))
	}
	if all[0].Status != "completed" || !all[0].Terminal {
		t.Errorf("snapshot = %+v, want the replacement state", all[0])
	}
}

// TestD1AdminJobOpenSnapshotsAreExactlyTheUnfinishedOnes covers the query the
// restore path runs. Its name is the contract: a caller must not have to filter,
// because a caller that forgot would restore finished jobs as live ones.
func TestD1AdminJobOpenSnapshotsAreExactlyTheUnfinishedOnes(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if err := st.SaveAdminJobSnapshot(ctx, d1AdminJobFixture("a", "reindex", "running", false)); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := st.SaveAdminJobSnapshot(ctx, d1AdminJobFixture("b", "rescan", "queued", false)); err != nil {
		t.Fatalf("save b: %v", err)
	}
	for _, terminal := range []string{"completed", "failed", "cancelled", "interrupted"} {
		if err := st.SaveAdminJobSnapshot(ctx, d1AdminJobFixture("done-"+terminal, "reindex", terminal, true)); err != nil {
			t.Fatalf("save %s: %v", terminal, err)
		}
	}

	open, err := st.ListOpenAdminJobSnapshots(ctx)
	if err != nil {
		t.Fatalf("list open: %v", err)
	}
	if len(open) != 2 {
		t.Fatalf("open = %d, want 2; a terminal job that still reports open would be re-interrupted on every restart", len(open))
	}
	ids := map[string]bool{}
	for _, snapshot := range open {
		ids[snapshot.JobID] = true
	}
	if !ids["a"] || !ids["b"] {
		t.Errorf("open ids = %v, want a and b", ids)
	}
}

// TestD1AdminJobSnapshotRejectsUndecodableJSON guards the one way this record can
// become permanently useless. A snapshot that cannot be parsed can never be
// presented to an operator, so refusing the write turns a silent loss into a
// visible failure at the moment of the write.
func TestD1AdminJobSnapshotRejectsUndecodableJSON(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	valid := d1AdminJobFixture("reindex-1", "reindex", "running", false)
	if err := st.SaveAdminJobSnapshot(ctx, valid); err != nil {
		t.Fatalf("save valid: %v", err)
	}

	broken := valid
	broken.SnapshotJSON = []byte(`{"job_id":`)
	if err := st.SaveAdminJobSnapshot(ctx, broken); err == nil {
		t.Error("an undecodable snapshot was accepted; it could never be presented again")
	}
	// The rejection must not have disturbed the good record.
	snapshot, found, err := st.GetAdminJobSnapshot(ctx, "reindex-1")
	if err != nil || !found {
		t.Fatalf("the rejected write damaged the stored snapshot: found=%t err=%v", found, err)
	}
	if string(snapshot.SnapshotJSON) != string(valid.SnapshotJSON) {
		t.Error("the rejected write replaced the previous snapshot")
	}
}

// TestD1AdminJobSnapshotRequiresAnID is the other precondition. A row with no id
// could never be looked up, so it would be a record of a job nobody can ask about.
func TestD1AdminJobSnapshotRequiresAnID(t *testing.T) {
	st, _ := newD1TestStore(t)
	snapshot := d1AdminJobFixture("", "reindex", "running", false)
	if err := st.SaveAdminJobSnapshot(context.Background(), snapshot); err == nil {
		t.Error("a snapshot with no job id was accepted")
	}
}

// TestD1AdminJobSnapshotListingIsBounded keeps the console endpoint from asking
// the database for a table scan during an incident, which is the moment it is
// most likely to be asked for.
func TestD1AdminJobSnapshotListingIsBounded(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if err := st.SaveAdminJobSnapshot(ctx, d1AdminJobFixture(
			"job-"+string(rune('a'+i)), "reindex", "completed", true)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	limited, err := st.ListAdminJobSnapshots(ctx, 3)
	if err != nil {
		t.Fatalf("list with a limit: %v", err)
	}
	if len(limited) != 3 {
		t.Errorf("limit=3 returned %d", len(limited))
	}
	capped, err := st.ListAdminJobSnapshots(ctx, 100000)
	if err != nil {
		t.Fatalf("list with an oversized limit: %v", err)
	}
	if len(capped) > 200 {
		t.Errorf("limit=100000 returned %d; the cap was not applied", len(capped))
	}
}
