package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/config"
	"github.com/risulongmemory/archive-center-go/internal/store"
)

// /admin/jobs read from a process-memory manager, so on a stateless Container it
// came back empty after every restart. The operator polling it concluded the
// reset had never run — while the reset may have completed, or be part way
// through a resumable chunk, with the truth sitting in a D1 row the endpoint
// never looked at.
//
// The reset is the destructive operator action: it has a confirmation token, a
// maintenance lease and an epoch fence. It is also the one whose disappearance
// from this list misleads most, because "no jobs" and "the job list was lost"
// look identical.

// durableResetStore is a Store that can read reset runs and nothing else. It
// embeds the no-op store rather than a real D1 store so the test states the
// property directly: a server with a durable reader and no job manager still
// answers with the reset.
type durableResetStore struct {
	store.Store
	runs      []store.AdminResetRun
	listCalls int
	getCalls  int
	listErr   error
}

func (d *durableResetStore) ListAdminResetRuns(_ context.Context, _ int) ([]store.AdminResetRun, error) {
	d.listCalls++
	if d.listErr != nil {
		return nil, d.listErr
	}
	return d.runs, nil
}

func (d *durableResetStore) GetAdminResetRun(_ context.Context, id string) (store.AdminResetRun, bool, error) {
	d.getCalls++
	if d.listErr != nil {
		return store.AdminResetRun{}, false, d.listErr
	}
	for _, run := range d.runs {
		if run.ResetRunID == id {
			return run, true, nil
		}
	}
	return store.AdminResetRun{}, false, nil
}

func completedResetRun(id string) store.AdminResetRun {
	return store.AdminResetRun{
		ResetRunID:    id,
		Epoch:         4,
		Status:        "completed",
		RowsDeleted:   9100,
		TablesCleared: 72,
		StartedAt:     "2026-02-01T00:00:00Z",
		UpdatedAt:     "2026-02-01T00:04:00Z",
		CompletedAt:   "2026-02-01T00:04:00Z",
		Durable:       true,
	}
}

func serveAdminJobs(t *testing.T, s *Server) map[string]any {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/jobs", s.handleAdminJobs)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/jobs", nil))

	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode admin jobs %q: %v", recorder.Body.String(), err)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /admin/jobs = %d, want 200; body %q", recorder.Code, recorder.Body.String())
	}
	return payload
}

// jobByID finds a job by id.
//
// The in-process manager calls the field "job_id" and mints ids of the form
// "<kind>-<nanos>-<sequence>", while the durable presentation uses "id" and the
// run's own identifier. Both are matched so a test can ask for a job by either
// its exact id or its kind, without every case having to know which producer made
// it.
func jobByID(t *testing.T, payload map[string]any, id string) map[string]any {
	t.Helper()
	jobs, _ := payload["jobs"].([]any)
	for _, entry := range jobs {
		job, _ := entry.(map[string]any)
		if job == nil {
			continue
		}
		for _, field := range []string{"id", "job_id"} {
			if value, _ := job[field].(string); value == id {
				return job
			}
		}
		if value, _ := job["job_id"].(string); strings.HasPrefix(value, id+"-") {
			return job
		}
	}
	return nil
}

// TestAdminJobsSurviveARestartWithNoJobManager is the regression, stated as the
// deployment it describes: a fresh process, no in-memory history, and a reset
// that a previous instance performed.
func TestAdminJobsSurviveARestartWithNoJobManager(t *testing.T) {
	durable := &durableResetStore{runs: []store.AdminResetRun{completedResetRun("reset-abc123")}}

	// No AdminJobs at all. This is what a Container looks like after it is
	// replaced, and it is also why the previous behaviour was an empty list.
	s := &Server{Store: durable, Cfg: config.Default()}

	payload := serveAdminJobs(t, s)
	job := jobByID(t, payload, "reset-abc123")
	if job == nil {
		t.Fatalf("the completed reset is not in the job list after a restart: %v", payload["jobs"])
	}
	if job["status"] != "completed" {
		t.Errorf("status = %v, want completed", job["status"])
	}
	if job["durable"] != true {
		t.Errorf("durable = %v; an operator cannot tell a surviving job from a lost one without this", job["durable"])
	}
	if job["source"] != "d1_reset_runs" {
		t.Errorf("source = %v, want d1_reset_runs so the operator knows where the truth is", job["source"])
	}
	if job["rows_deleted"] != float64(9100) {
		t.Errorf("rows_deleted = %v, want the count the reset reported", job["rows_deleted"])
	}
	if payload["durable_source"] != true {
		t.Errorf("durable_source = %v, want true when the store can record reset runs", payload["durable_source"])
	}

	// A progress field is NOT invented. processed: 0 would read as "the reset did
	// nothing", which is the opposite of what a completed reset means.
	if _, present := job["processed"]; present {
		t.Error("the presentation invented a processed field; a zero there reads as a reset that did nothing")
	}
}

// TestAdminJobsMarkInProcessJobsAsNotDurable is the other half. The non-reset
// jobs are still process memory and are still lost; the flag says so instead of
// letting the list look equally trustworthy.
func TestAdminJobsMarkInProcessJobsAsNotDurable(t *testing.T) {
	durable := &durableResetStore{runs: []store.AdminResetRun{completedResetRun("reset-abc123")}}
	manager := newAdminJobManager()
	manager.start("reindex", "sess-1", map[string]any{}, func(context.Context, adminJobProgressFunc) (map[string]any, error) {
		return map[string]any{}, nil
	})

	s := &Server{Store: durable, AdminJobs: manager, Cfg: config.Default()}
	payload := serveAdminJobs(t, s)

	inProcess := jobByKind(t, payload, "reindex")
	if inProcess == nil {
		t.Fatalf("the in-process job is missing: %v", payload["jobs"])
	}
	if inProcess["durable"] != false {
		t.Errorf("durable = %v, want false; an in-process job does not survive a Container restart", inProcess["durable"])
	}
	if inProcess["source"] != "process_memory" {
		t.Errorf("source = %v, want process_memory", inProcess["source"])
	}
	if jobByID(t, payload, "reset-abc123") == nil {
		t.Error("the durable reset was dropped when an in-process job was present; the two are merged, not exclusive")
	}
}

// TestAdminJobsOnAStoreWithoutDurableRuns stays truthful rather than failing. A
// local runtime's in-process list is the whole truth there, so the endpoint
// answers normally and says the reset source is not durable.
func TestAdminJobsOnAStoreWithoutDurableRuns(t *testing.T) {
	manager := newAdminJobManager()
	manager.start("reindex", "sess-1", map[string]any{}, func(context.Context, adminJobProgressFunc) (map[string]any, error) {
		return map[string]any{}, nil
	})
	s := &Server{Store: &noResetRunReaderStore{}, AdminJobs: manager, Cfg: config.Default()}

	payload := serveAdminJobs(t, s)
	if payload["durable_source"] != false {
		t.Errorf("durable_source = %v, want false", payload["durable_source"])
	}
	if jobByKind(t, payload, "reindex") == nil {
		t.Error("the local runtime's in-process job list stopped working")
	}
}

// TestAdminJobsReportAReaderFailureInsteadOfAnEmptyList is why the durable read
// returns an error rather than nil on failure. A store that claims the capability
// and then fails would otherwise be indistinguishable from a deployment that has
// never had a reset, which is the silence that hid this in the first place.
func TestAdminJobsReportAReaderFailureInsteadOfAnEmptyList(t *testing.T) {
	durable := &durableResetStore{listErr: fmt.Errorf("d1 unavailable")}
	s := &Server{Store: durable, Cfg: config.Default()}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/jobs", s.handleAdminJobs)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/jobs", nil))

	if recorder.Code == http.StatusOK {
		t.Fatalf("a failing durable read returned 200 with %q; an operator would read that as 'no reset has run'", recorder.Body.String())
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", recorder.Code)
	}
}

// TestAdminJobLookupResolvesADurableRunAfterARestart covers the bookmarked URL.
// An operator who kept the id expects it to keep working, and the id is durable
// precisely because the process that created it is gone.
func TestAdminJobLookupResolvesADurableRunAfterARestart(t *testing.T) {
	durable := &durableResetStore{runs: []store.AdminResetRun{completedResetRun("reset-abc123")}}
	s := &Server{Store: durable, Cfg: config.Default()}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /admin/jobs/{job_id}", s.handleAdminJob)
	mux.HandleFunc("DELETE /admin/jobs/{job_id}", s.handleAdminJob)

	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/jobs/reset-abc123", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET of a durable run = %d, want 200; body %q", recorder.Code, recorder.Body.String())
	}
	var job map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &job); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if job["id"] != "reset-abc123" || job["durable"] != true {
		t.Errorf("job = %+v", job)
	}

	missing := httptest.NewRecorder()
	mux.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/admin/jobs/never-existed", nil))
	if missing.Code != http.StatusNotFound {
		t.Errorf("GET of an unknown id = %d, want 404", missing.Code)
	}
}

// noResetRunReaderStore is a Store WITHOUT the capability, which is the local
// runtime's case. It must not be mistaken for a store that can report "no runs".
type noResetRunReaderStore struct{ store.Store }

var _ = time.Second

// jobByKind finds a job by its kind. The in-process manager mints ids from a
// timestamp and a sequence counter, so "the reindex job" is the stable way to
// name one; the durable presentation uses a fixed id and matches by that.
func jobByKind(t *testing.T, payload map[string]any, kind string) map[string]any {
	t.Helper()
	jobs, _ := payload["jobs"].([]any)
	for _, entry := range jobs {
		job, _ := entry.(map[string]any)
		if job == nil {
			continue
		}
		if value, _ := job["kind"].(string); value == kind {
			return job
		}
	}
	return nil
}
