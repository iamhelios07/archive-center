package store

import (
	"context"
	"strings"
	"testing"
)

// The reset run had a requested_by column that nothing ever wrote.
//
// So a completed reset could report what it deleted and never who asked for it.
// The plan calls for the reset's confirmation, authorization and audit semantics
// to be preserved, and a declared-but-unfilled column is the audit half of that
// left undone: it reads as if the actor were recorded somewhere.
//
// These tests pin the whole path: the actor reaches the row, an empty actor stays
// empty rather than becoming a placeholder, a resume does not overwrite the
// original requester, and the value is bounded and sanitised because it can come
// from a request body.

func TestD1ResetRecordsTheRequestedActor(t *testing.T) {
	st, conn := newD1TestStore(t)
	d1SeedAllowlistRows(t, conn, "s1", 3)

	result, err := st.ResetAllAs(context.Background(), "  operator@example.com  ")
	if err != nil {
		t.Fatalf("ResetAllAs: %v", err)
	}
	if result.TablesCleared == 0 {
		t.Fatal("the reset did nothing, so this test proves nothing about the record")
	}

	run, found, err := st.GetAdminResetRun(context.Background(), latestResetRunID(t, st))
	if err != nil || !found {
		t.Fatalf("read back the run: found=%t err=%v", found, err)
	}
	// Trimmed, because a padded label and the same label are the same operator.
	if run.RequestedBy != "operator@example.com" {
		t.Errorf("RequestedBy = %q, want the trimmed label", run.RequestedBy)
	}
}

// latestResetRunID returns the most recent run id, so a test does not have to
// guess the generated identifier.
func latestResetRunID(t *testing.T, st *d1Store) string {
	t.Helper()
	runs, err := st.ListAdminResetRuns(context.Background(), 1)
	if err != nil || len(runs) == 0 {
		t.Fatalf("list runs: %v (%d runs)", err, len(runs))
	}
	return runs[0].ResetRunID
}

// TestD1ResetLeavesAnUnnamedActorEmpty is the difference between an honest audit
// column and a misleading one. A placeholder is indistinguishable from a real
// identity when someone reads the row later, so "nobody was named" has to be
// representable.
func TestD1ResetLeavesAnUnnamedActorEmpty(t *testing.T) {
	st, conn := newD1TestStore(t)
	d1SeedAllowlistRows(t, conn, "s1", 3)

	// The plain ResetAll has no way to carry an actor, which is exactly the case
	// this must not paper over.
	if _, err := st.ResetAll(context.Background()); err != nil {
		t.Fatalf("ResetAll: %v", err)
	}
	run, found, err := st.GetAdminResetRun(context.Background(), latestResetRunID(t, st))
	if err != nil || !found {
		t.Fatalf("read back the run: found=%t err=%v", found, err)
	}
	if run.RequestedBy != "" {
		t.Errorf("RequestedBy = %q for a reset nobody was named on; a placeholder here cannot be told apart from a real identity later", run.RequestedBy)
	}
}

// TestD1ResetResumeKeepsTheOriginalRequester is the subtle one.
//
// A reset is resumable: a second call picks up the interrupted run's cursor. That
// second call was not the request — it is a continuation of the first — so the
// recorded actor must stay the one who originally asked. Overwriting it would make
// the audit say that whoever happened to click resume is responsible for a
// deletion somebody else authorised.
func TestD1ResetResumeKeepsTheOriginalRequester(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedAllowlistRows(t, conn, "s1", 400)

	// Start a reset and leave it in flight, the way a Container replacement does.
	if _, err := st.ResetAllAs(ctx, "first-operator"); err != nil {
		t.Fatalf("first reset: %v", err)
	}
	runID := latestResetRunID(t, st)
	if _, err := conn.Exec(ctx,
		`UPDATE d1_reset_runs SET status = 'running' WHERE reset_run_id = ?`, runID); err != nil {
		t.Fatalf("leave the run in flight: %v", err)
	}

	// A different operator resumes it.
	if _, err := st.ResetAllAs(ctx, "second-operator"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	run, found, err := st.GetAdminResetRun(ctx, runID)
	if err != nil || !found {
		t.Fatalf("read back the resumed run: found=%t err=%v", found, err)
	}
	if run.RequestedBy != "first-operator" {
		t.Errorf("RequestedBy = %q after a resume, want the original requester; a resume is a continuation of the first request, not a new one", run.RequestedBy)
	}
}

// TestD1ResetFillsAnEmptyActorOnResume is the other direction: the first call may
// have had no actor because it came through the plain ResetAll, and a later
// attributed resume should fill it in rather than leaving a recoverable fact
// unrecorded.
func TestD1ResetFillsAnEmptyActorOnResume(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedAllowlistRows(t, conn, "s1", 400)

	if _, err := st.ResetAll(ctx); err != nil {
		t.Fatalf("first reset: %v", err)
	}
	runID := latestResetRunID(t, st)
	if _, err := conn.Exec(ctx,
		`UPDATE d1_reset_runs SET status = 'running' WHERE reset_run_id = ?`, runID); err != nil {
		t.Fatalf("leave the run in flight: %v", err)
	}
	if _, err := st.ResetAllAs(ctx, "later-operator"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	run, found, err := st.GetAdminResetRun(ctx, runID)
	if err != nil || !found {
		t.Fatalf("read back: found=%t err=%v", found, err)
	}
	if run.RequestedBy != "later-operator" {
		t.Errorf("RequestedBy = %q, want the attributed resume to fill an empty actor", run.RequestedBy)
	}
}

// TestNormalizeAdminResetActorBoundsAndSanitises covers the input handling. The
// value can come from a request body, so it is attacker-controlled until proven
// otherwise, and it is echoed back by /admin/jobs and written into log lines.
func TestNormalizeAdminResetActorBoundsAndSanitises(t *testing.T) {
	cases := map[string]struct {
		input string
		want  string
	}{
		"trimmed":           {"  alice  ", "alice"},
		"empty stays empty": {"", ""},
		"whitespace only":   {"   ", ""},
		"newline stripped":  {"ali\nce", "alice"},
		"tab stripped":      {"ali\tce", "alice"},
		"escape stripped":   {"ali\x1bce", "alice"},
		"delete stripped":   {"ali\x7fce", "alice"},
		"unicode preserved": {"운영자", "운영자"},
	}
	for name, tc := range cases {
		if got := NormalizeAdminResetActor(tc.input); got != tc.want {
			t.Errorf("%s: NormalizeAdminResetActor(%q) = %q, want %q", name, tc.input, got, tc.want)
		}
	}

	// An unbounded audit column is a way to grow the control plane without limit.
	long := strings.Repeat("x", AdminResetActorLimit*4)
	got := NormalizeAdminResetActor(long)
	if len(got) != AdminResetActorLimit {
		t.Errorf("a %d-character actor became %d characters, want it truncated to %d", len(long), len(got), AdminResetActorLimit)
	}
}
