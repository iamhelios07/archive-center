package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/config"
	"github.com/risulongmemory/archive-center-go/internal/store"
)

// The job list and the job detail must agree.
//
// They did not. A job restored as interrupted appeared in GET /admin/jobs, and
// GET /admin/jobs/{id} then answered 404 for that same id. That is worse than
// either omitting the job or showing it, because a 404 says "that id was wrong"
// when the truth is "that job is over" — and an operator following the id they
// just read out of the list concludes they mistyped it, or that the list lied.
//
// The event stream had the same disagreement, which is worse there: the 404
// arrives at exactly the moment someone is watching a job, so it reads as a
// broken endpoint rather than as a finished one.

// bothDurableStore answers as a reset run reader and a job snapshot store, which
// is what a D1-backed deployment is.
type bothDurableStore struct {
	store.Store
	resetRuns map[string]store.AdminResetRun
	snapshots map[string]store.AdminJobSnapshot
}

func (b *bothDurableStore) GetAdminResetRun(_ context.Context, id string) (store.AdminResetRun, bool, error) {
	run, ok := b.resetRuns[id]
	return run, ok, nil
}

func (b *bothDurableStore) ListAdminResetRuns(context.Context, int) ([]store.AdminResetRun, error) {
	out := []store.AdminResetRun{}
	for _, run := range b.resetRuns {
		out = append(out, run)
	}
	return out, nil
}

func (b *bothDurableStore) GetAdminJobSnapshot(_ context.Context, id string) (store.AdminJobSnapshot, bool, error) {
	snapshot, ok := b.snapshots[id]
	return snapshot, ok, nil
}

func (b *bothDurableStore) ListAdminJobSnapshots(context.Context, int) ([]store.AdminJobSnapshot, error) {
	out := []store.AdminJobSnapshot{}
	for _, snapshot := range b.snapshots {
		out = append(out, snapshot)
	}
	return out, nil
}

func (b *bothDurableStore) ListOpenAdminJobSnapshots(context.Context) ([]store.AdminJobSnapshot, error) {
	return nil, nil
}

func (b *bothDurableStore) SaveAdminJobSnapshot(context.Context, store.AdminJobSnapshot) error {
	return nil
}

func interruptedSnapshot(jobID, kind string) store.AdminJobSnapshot {
	wire := map[string]any{
		"contract_version":   "admin_background_job.v1",
		"job_id":             jobID,
		"kind":               kind,
		"status":             adminJobInterruptedStatus,
		"chat_session_id":    "sess-1",
		"started_at":         "2026-02-01T00:00:00Z",
		"updated_at":         "2026-02-01T00:06:00Z",
		"finished_at":        "2026-02-01T00:06:00Z",
		"terminal":           true,
		"resumable":          false,
		"interrupted_reason": "the instance running this job was replaced; the work was not resumed and must be re-run",
		"progress":           map[string]any{"status": adminJobInterruptedStatus, "processed": 12},
	}
	encoded, _ := json.Marshal(wire)
	return store.AdminJobSnapshot{
		JobID: jobID, Kind: kind, SessionID: "sess-1", Status: adminJobInterruptedStatus,
		SnapshotJSON: encoded, StartedAt: "2026-02-01T00:00:00Z",
		UpdatedAt: "2026-02-01T00:06:00Z", Terminal: true,
	}
}

func durableJobServer(storeUnderTest store.Store) *Server {
	return &Server{Store: storeUnderTest, AdminJobs: newAdminJobManager(), Cfg: config.Default()}
}

func serveJobDetail(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/jobs/{job_id}", s.handleAdminJob)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/jobs/"+id, nil))
	return recorder
}

// TestJobDetailResolvesAnInterruptedJobFromTheSnapshot is the regression: the id
// an operator just read out of the list must resolve.
func TestJobDetailResolvesAnInterruptedJobFromTheSnapshot(t *testing.T) {
	durable := &bothDurableStore{
		snapshots: map[string]store.AdminJobSnapshot{
			"reindex-1": interruptedSnapshot("reindex-1", "reindex"),
		},
	}
	recorder := serveJobDetail(t, durableJobServer(durable), "reindex-1")

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET an id the list just showed = %d, want 200; a 404 here says the id was wrong when the job is simply over. body %q", recorder.Code, recorder.Body.String())
	}
	var job map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &job); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if job["status"] != adminJobInterruptedStatus {
		t.Errorf("status = %v, want %q", job["status"], adminJobInterruptedStatus)
	}
	if job["durable"] != true || job["source"] != "d1_admin_jobs" {
		t.Errorf("durable = %v, source = %v; the detail view must mark it the same way the list does", job["durable"], job["source"])
	}
	if job["resumable"] != false {
		t.Errorf("resumable = %v, want false", job["resumable"])
	}
}

// TestJobDetailStillPrefersALiveJobOverItsStoredCopy keeps the ordering honest. A
// job this process is running must be answered from memory, not from a snapshot
// that was written moments ago and is now behind.
func TestJobDetailStillPrefersALiveJobOverItsStoredCopy(t *testing.T) {
	durable := &bothDurableStore{
		snapshots: map[string]store.AdminJobSnapshot{
			"reindex-live": interruptedSnapshot("reindex-live", "reindex"),
		},
	}
	manager := newAdminJobManager()
	manager.start("reindex", "sess-1", map[string]any{}, func(context.Context, adminJobProgressFunc) (map[string]any, error) {
		return map[string]any{"status": "completed"}, nil
	})
	manager.mu.RLock()
	var liveID string
	for id := range manager.jobs {
		liveID = id
	}
	manager.mu.RUnlock()
	// Give the stored copy a colliding id so the ordering is what is being tested.
	durable.snapshots[liveID] = interruptedSnapshot(liveID, "reindex")
	manager.mu.Lock()
	manager.jobs[liveID].Status = "running"
	manager.mu.Unlock()

	s := durableJobServer(durable)
	s.AdminJobs = manager
	recorder := serveJobDetail(t, s, liveID)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var job map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &job); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if job["status"] == adminJobInterruptedStatus {
		t.Error("a job this process is running was answered from its stored snapshot; the live state is behind by design")
	}
}

// TestJobDetailStillAnswersUnknownIdsAsNotFound keeps the fix from turning every
// unknown id into something.
func TestJobDetailStillAnswersUnknownIdsAsNotFound(t *testing.T) {
	durable := &bothDurableStore{snapshots: map[string]store.AdminJobSnapshot{}}
	recorder := serveJobDetail(t, durableJobServer(durable), "never-existed")
	if recorder.Code != http.StatusNotFound {
		t.Errorf("GET an unknown id = %d, want 404", recorder.Code)
	}
}

// serveJobEvents opens the NDJSON stream and returns the recorder.
func serveJobEvents(t *testing.T, s *Server, id string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/jobs/{job_id}/events", s.handleAdminJobEvents)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/jobs/"+id+"/events", nil))
	return recorder
}

// TestEventStreamForAnInterruptedJobEmitsItsFinalStateAndCloses is the SSE half.
//
// A stored job has no future updates because nothing is executing it, so the
// stream answers once and ends. That is what a client watching a terminal job
// would have seen anyway, and it is strictly better than the 404 this used to
// return — which arrived at the exact moment someone was watching a job.
func TestEventStreamForAnInterruptedJobEmitsItsFinalStateAndCloses(t *testing.T) {
	durable := &bothDurableStore{
		snapshots: map[string]store.AdminJobSnapshot{
			"reindex-1": interruptedSnapshot("reindex-1", "reindex"),
		},
	}
	recorder := serveJobEvents(t, durableJobServer(durable), "reindex-1")

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET the event stream of a stored job = %d, want 200; body %q", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/x-ndjson; charset=utf-8" {
		t.Errorf("Content-Type = %q", got)
	}

	var events []map[string]any
	scanner := bufio.NewScanner(recorder.Body)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("stream line %q is not JSON: %v", line, err)
		}
		events = append(events, event)
	}
	if len(events) != 1 {
		t.Fatalf("the stream emitted %d events, want exactly 1; a finished job has no second update", len(events))
	}
	if events[0]["status"] != adminJobInterruptedStatus {
		t.Errorf("status = %v, want %q", events[0]["status"], adminJobInterruptedStatus)
	}
	if events[0]["terminal"] != true {
		t.Error("the final event is not marked terminal; a client would wait for an update that can never arrive")
	}
}

// TestEventStreamForAResetRunExplainsWhyItCannotStream keeps the two kinds of
// stored record apart.
//
// A reset run is not a finished job. Its progress advances because a worker is
// checkpointing into D1, not because this process is executing it, so it has no
// event stream at all. Answering 409 with the reason is more useful than an
// empty stream or a 404, both of which read as a broken endpoint.
func TestEventStreamForAResetRunExplainsWhyItCannotStream(t *testing.T) {
	durable := &bothDurableStore{
		resetRuns: map[string]store.AdminResetRun{
			"reset-abc": completedResetRun("reset-abc"),
		},
		snapshots: map[string]store.AdminJobSnapshot{},
	}
	recorder := serveJobEvents(t, durableJobServer(durable), "reset-abc")

	if recorder.Code != http.StatusConflict {
		t.Fatalf("GET the event stream of a reset run = %d, want 409; body %q", recorder.Code, recorder.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload["reason_code"] != "reset_run_has_no_event_stream" {
		t.Errorf("reason_code = %v; the response has to say why rather than only refusing", payload["reason_code"])
	}
}

// TestEventStreamForAnUnknownIdIsStillNotFound keeps the fix bounded.
func TestEventStreamForAnUnknownIdIsStillNotFound(t *testing.T) {
	durable := &bothDurableStore{snapshots: map[string]store.AdminJobSnapshot{}}
	recorder := serveJobEvents(t, durableJobServer(durable), "never-existed")
	if recorder.Code != http.StatusNotFound {
		t.Errorf("GET the event stream of an unknown id = %d, want 404", recorder.Code)
	}
}
