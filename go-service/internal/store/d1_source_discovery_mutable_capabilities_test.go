package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// D1 source discovery mutable and query capability tests.
//
// The workflow these cover is the operator's: start a job, advance it through
// the pipeline, resume it after a failure, and find the latest attempt for a
// (work, continuity) binding. Every one of those routes is gated on the two
// capabilities under test, so a failure here is a 503 on a route that works
// locally.

// d1DiscoveryJobInput is a request bound to a reference work and a continuity.
func d1DiscoveryJobInput(workID, continuityID string) SourceDiscoveryInput {
	return SourceDiscoveryInput{
		WorkQuery:     "the bridge at Ilse",
		OriginalTitle: "The Bridge at Ilse",
		Language:      "en",
		WorkID:        workID,
		ContinuityID:  continuityID,
	}
}

// TestD1SourceDiscoveryMutableAndQueryAreAdvertised pins the delivery state
// through the manifest a deployment reports.
func TestD1SourceDiscoveryMutableAndQueryAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var store Store = st
	if _, ok := store.(SourceDiscoveryMutableStore); !ok {
		t.Fatal("the D1 provider must expose SourceDiscoveryMutableStore")
	}
	if _, ok := store.(SourceDiscoveryQueryStore); !ok {
		t.Fatal("the D1 provider must expose SourceDiscoveryQueryStore")
	}
	for _, capability := range []string{"SourceDiscoveryMutableStore", "SourceDiscoveryQueryStore"} {
		reported := false
		for _, status := range CapabilityReport(st) {
			if status.Name != capability {
				continue
			}
			reported = true
			if !status.Implemented {
				t.Errorf("the capability manifest reports %s as missing", capability)
			}
		}
		if !reported {
			t.Errorf("%s is not probed by the capability manifest", capability)
		}
	}
}

// TestD1UpdateSourceDiscoveryJobAdvancesAndBumpsRevision pins the update: the
// state and payloads land, the revision moves, and the caller gets the stored
// row back rather than its own request echoed.
func TestD1UpdateSourceDiscoveryJobAdvancesAndBumpsRevision(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	created, err := st.SaveSourceDiscoveryJob(ctx, d1DiscoveryJobInput("work-1", "cont-1"), "created",
		map[string]any{}, map[string]any{})
	if err != nil {
		t.Fatalf("SaveSourceDiscoveryJob: %v", err)
	}
	if created.Revision != 1 {
		t.Errorf("created revision = %d, want 1", created.Revision)
	}

	updated, err := st.UpdateSourceDiscoveryJob(ctx, created.JobID, "extracting",
		map[string]any{"notes": "fetching"}, map[string]any{"covered": 3})
	if err != nil {
		t.Fatalf("UpdateSourceDiscoveryJob: %v", err)
	}
	if updated.State != "extracting" {
		t.Errorf("state = %q, want %q", updated.State, "extracting")
	}
	if updated.Revision != created.Revision+1 {
		t.Errorf("revision = %d, want %d", updated.Revision, created.Revision+1)
	}
	if got := updated.Result["notes"]; got != "fetching" {
		t.Errorf("result payload = %v, want the supplied one", updated.Result)
	}
	if got := updated.CoverageReport["covered"]; got != float64(3) {
		t.Errorf("coverage payload = %v, want the supplied one", updated.CoverageReport)
	}
	// The durable row agrees with the returned record.
	reread, err := st.GetSourceDiscoveryJob(ctx, created.JobID)
	if err != nil {
		t.Fatalf("GetSourceDiscoveryJob: %v", err)
	}
	if reread.Revision != updated.Revision || reread.State != "extracting" {
		t.Errorf("stored row = revision %d state %q, want revision %d state %q",
			reread.Revision, reread.State, updated.Revision, "extracting")
	}
	// Repeated advances keep counting, which is what a caller compares against to
	// detect a concurrent writer.
	final, err := st.UpdateSourceDiscoveryJob(ctx, created.JobID, "ready_for_admission", nil, nil)
	if err != nil {
		t.Fatalf("second UpdateSourceDiscoveryJob: %v", err)
	}
	if final.Revision != 3 {
		t.Errorf("revision after two updates = %d, want 3", final.Revision)
	}
}

// TestD1UpdateSourceDiscoveryJobMovesUpdatedAt is the regression the earlier
// provenance pass flagged and could not fix, because the method is not in the
// base capability.
//
// MariaDB bumps updated_at implicitly through ON UPDATE CURRENT_TIMESTAMP(3).
// SQLite has no such clause on this table, so the update has to name the column
// itself. If it does not, updated_at stays frozen at the insert time and
// FindLatestSourceDiscoveryJob — which orders by updated_at DESC — returns the
// OLDEST attempt for a work instead of the newest. The resume path would then
// silently restart a stale attempt, which is invisible: every call succeeds.
func TestD1UpdateSourceDiscoveryJobMovesUpdatedAt(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	created, err := st.SaveSourceDiscoveryJob(ctx, d1DiscoveryJobInput("work-move", "cont-move"), "created",
		map[string]any{}, map[string]any{})
	if err != nil {
		t.Fatalf("SaveSourceDiscoveryJob: %v", err)
	}
	var before string
	if err := conn.QueryRow(ctx,
		`SELECT updated_at FROM source_discovery_jobs WHERE job_id = ?`, created.JobID).Scan(&before); err != nil {
		t.Fatalf("read updated_at before: %v", err)
	}
	// Move the stored timestamp into the past so a frozen column is unambiguous
	// rather than a same-instant coincidence.
	past := "2000-01-01T00:00:00.000Z"
	if _, err := conn.Exec(ctx,
		`UPDATE source_discovery_jobs SET updated_at = ? WHERE job_id = ?`, past, created.JobID); err != nil {
		t.Fatalf("age the row: %v", err)
	}

	after, err := st.UpdateSourceDiscoveryJob(ctx, created.JobID, "discovering", nil, nil)
	if err != nil {
		t.Fatalf("UpdateSourceDiscoveryJob: %v", err)
	}
	var updatedAt string
	if err := conn.QueryRow(ctx,
		`SELECT updated_at FROM source_discovery_jobs WHERE job_id = ?`, created.JobID).Scan(&updatedAt); err != nil {
		t.Fatalf("read updated_at after: %v", err)
	}
	if updatedAt <= past {
		t.Errorf("updated_at = %q, want it to move past %q; a frozen column makes the latest-job lookup return the oldest attempt",
			updatedAt, past)
	}
	// The returned record carries the stored value, at the stored precision, so a
	// caller comparing it against the row is comparing like with like.
	if !after.UpdatedAt.Equal(d1TimeInstantFromText(t, updatedAt)) {
		t.Errorf("returned updated_at = %v, want the stored %v", after.UpdatedAt, updatedAt)
	}
}

// TestD1FindLatestSourceDiscoveryJobReturnsTheNewestAttempt pins the lookup the
// resume path depends on.
//
// The rows differ only in their timestamps, so an ordering that ignores updated_at
// returns the wrong job and the operator resumes a discarded attempt.
func TestD1FindLatestSourceDiscoveryJobReturnsTheNewestAttempt(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	oldest, err := st.SaveSourceDiscoveryJob(ctx, d1DiscoveryJobInput("work-order", "cont-order"), "created",
		map[string]any{}, map[string]any{})
	if err != nil {
		t.Fatalf("first SaveSourceDiscoveryJob: %v", err)
	}
	middle, err := st.SaveSourceDiscoveryJob(ctx, d1DiscoveryJobInput("work-order", "cont-order"), "created",
		map[string]any{}, map[string]any{})
	if err != nil {
		t.Fatalf("second SaveSourceDiscoveryJob: %v", err)
	}
	newest, err := st.SaveSourceDiscoveryJob(ctx, d1DiscoveryJobInput("work-order", "cont-order"), "created",
		map[string]any{}, map[string]any{})
	if err != nil {
		t.Fatalf("third SaveSourceDiscoveryJob: %v", err)
	}
	// Force a known order instead of relying on how fast the inserts ran.
	stamp := map[string]string{
		oldest.JobID: "2026-01-01T00:00:00.000Z",
		middle.JobID: "2026-02-01T00:00:00.000Z",
		newest.JobID: "2026-03-01T00:00:00.000Z",
	}
	for jobID, at := range stamp {
		if _, err := conn.Exec(ctx,
			`UPDATE source_discovery_jobs SET updated_at = ? WHERE job_id = ?`, at, jobID); err != nil {
			t.Fatalf("stamp %s: %v", jobID, err)
		}
	}

	found, err := st.FindLatestSourceDiscoveryJob(ctx, "work-order", "cont-order")
	if err != nil {
		t.Fatalf("FindLatestSourceDiscoveryJob: %v", err)
	}
	if found.JobID != newest.JobID {
		t.Errorf("found job = %s, want the newest attempt %s", found.JobID, newest.JobID)
	}

	// Advancing a NON-latest attempt makes it the latest, which is the rule a
	// resume depends on. The state is a real pipeline state because the vocabulary
	// check is applied before any statement.
	if _, err := st.UpdateSourceDiscoveryJob(ctx, oldest.JobID, "discovering", nil, nil); err != nil {
		t.Fatalf("advance the oldest attempt: %v", err)
	}
	found, err = st.FindLatestSourceDiscoveryJob(ctx, "work-order", "cont-order")
	if err != nil {
		t.Fatalf("FindLatestSourceDiscoveryJob after the advance: %v", err)
	}
	if found.JobID != oldest.JobID {
		t.Errorf("found job = %s, want the just-advanced %s", found.JobID, oldest.JobID)
	}
	if found.State != "discovering" {
		t.Errorf("state = %q, want %q", found.State, "discovering")
	}
}

// TestD1FindLatestSourceDiscoveryJobScopesToBothIdentifiers pins the binding.
//
// A job is bound to a work AND a continuity. Matching on either alone would
// return a job for a different scope that happens to share the other id, and the
// operator would resume an unrelated discovery.
func TestD1FindLatestSourceDiscoveryJobScopesToBothIdentifiers(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if _, err := st.SaveSourceDiscoveryJob(ctx, d1DiscoveryJobInput("work-a", "cont-a"), "created",
		map[string]any{}, map[string]any{}); err != nil {
		t.Fatalf("SaveSourceDiscoveryJob: %v", err)
	}

	// Right work, wrong continuity.
	if _, err := st.FindLatestSourceDiscoveryJob(ctx, "work-a", "cont-b"); !errors.Is(err, ErrNotFound) {
		t.Errorf("mismatched continuity = %v, want ErrNotFound", err)
	}
	// Wrong work, right continuity.
	if _, err := st.FindLatestSourceDiscoveryJob(ctx, "work-b", "cont-a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("mismatched work = %v, want ErrNotFound", err)
	}
	// Neither exists.
	if _, err := st.FindLatestSourceDiscoveryJob(ctx, "work-none", "cont-none"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown binding = %v, want ErrNotFound", err)
	}
	// Both blank is a caller error, not an empty result.
	if _, err := st.FindLatestSourceDiscoveryJob(ctx, "  ", "cont-a"); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank work = %v, want ErrInvalidReference", err)
	}
	if _, err := st.FindLatestSourceDiscoveryJob(ctx, "work-a", "  "); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank continuity = %v, want ErrInvalidReference", err)
	}
}

// TestD1UpdateSourceDiscoveryJobRejectsBadInput pins the argument contract: a
// blank id or a state outside the pipeline vocabulary must be refused before any
// statement, so a caller typo cannot advance a real job.
func TestD1UpdateSourceDiscoveryJobRejectsBadInput(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	created, err := st.SaveSourceDiscoveryJob(ctx, d1DiscoveryJobInput("work-guard", "cont-guard"), "created",
		map[string]any{}, map[string]any{})
	if err != nil {
		t.Fatalf("SaveSourceDiscoveryJob: %v", err)
	}

	if _, err := st.UpdateSourceDiscoveryJob(ctx, "  ", "extracting", nil, nil); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank job id = %v, want ErrInvalidReference", err)
	}
	if _, err := st.UpdateSourceDiscoveryJob(ctx, created.JobID, "not_a_state", nil, nil); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("unknown state = %v, want ErrInvalidReference", err)
	}
	// Nothing may have moved.
	if got := d1Count(t, conn, `SELECT revision FROM source_discovery_jobs WHERE job_id = ?`, created.JobID); got != 1 {
		t.Errorf("revision = %d, want it left at 1", got)
	}
}

// TestD1UpdateSourceDiscoveryJobRejectsUnknownJob pins the not-found contract.
// A caller that believed it had advanced a job would otherwise wait for a
// progress tick that never comes.
func TestD1UpdateSourceDiscoveryJobRejectsUnknownJob(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if _, err := st.UpdateSourceDiscoveryJob(ctx, "no-such-job", "extracting", nil, nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown job = %v, want ErrNotFound", err)
	}
}

// TestD1SourceDiscoveryJobLifecycleIsReplaySafe walks the shape a real operator
// session takes: create, discover, review, admit. The point is that the whole
// lifecycle works on this provider, because without these two capabilities every
// step after the first answers 503.
func TestD1SourceDiscoveryJobLifecycleIsReplaySafe(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	job, err := st.SaveSourceDiscoveryJob(ctx, d1DiscoveryJobInput("work-life", "cont-life"), "created",
		map[string]any{}, map[string]any{})
	if err != nil {
		t.Fatalf("SaveSourceDiscoveryJob: %v", err)
	}
	for _, state := range []string{"scope_ready", "discovering", "fetching", "extracting", "reconciling", "coverage_review", "ready_for_admission"} {
		job, err = st.UpdateSourceDiscoveryJob(ctx, job.JobID, state,
			map[string]any{"state": state}, map[string]any{"covered": 1})
		if err != nil {
			t.Fatalf("advance to %s: %v", state, err)
		}
		if job.State != state {
			t.Fatalf("state = %q, want %q", job.State, state)
		}
	}
	if job.Revision != 8 {
		t.Errorf("revision after the lifecycle = %d, want 8", job.Revision)
	}
	// The binding is still resolvable at the end, which is what a later resume or
	// a re-run depends on.
	found, err := st.FindLatestSourceDiscoveryJob(ctx, "work-life", "cont-life")
	if err != nil {
		t.Fatalf("FindLatestSourceDiscoveryJob: %v", err)
	}
	if found.JobID != job.JobID || found.State != "ready_for_admission" {
		t.Errorf("found %s in state %q, want %s in state %q",
			found.JobID, found.State, job.JobID, "ready_for_admission")
	}
}

// d1TimeInstantFromText parses a stored timestamp for a comparison against a
// returned time.Time.
func d1TimeInstantFromText(t *testing.T, text string) time.Time {
	t.Helper()
	parsed, err := parseD1Time(text)
	if err != nil {
		t.Fatalf("parse stored timestamp %q: %v", text, err)
	}
	return parsed
}
