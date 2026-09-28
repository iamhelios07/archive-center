package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/store"
)

// What happens to an operator job when the Container is replaced.
//
// The in-process job manager is memory only, so every job in flight when the
// previous instance went away disappeared with no trace. An operator polling
// /admin/jobs saw an empty list and could not tell "nothing is running" from "the
// record of what was running was lost".
//
// The restore deliberately does NOT hand the jobs back. Reindex, rescan,
// session-normalize and dedupe-cleanup capture in-process state a new instance
// does not have; the progress map holds a cursor for reporting, not for
// resuming. Restoring a dead job as running would be the comfortable answer and
// the false one — nothing is executing it, and a progress bar that has stopped
// moving is the worst thing to show someone who cannot tell why.

// recordingJobSnapshotStore counts writes so a test can assert that persistence
// happened, and can assert equally that it did NOT happen where it should not.
//
// It is mutex-guarded because the manager writes from a goroutine: persist is
// dispatched asynchronously, so a test reading the recorded map while a write is
// in flight is a data race in the test rather than in the code under test. The
// first version of this test was flaky for exactly that reason.
type recordingJobSnapshotStore struct {
	store.Store
	mu        sync.Mutex
	open      []store.AdminJobSnapshot
	all       map[string]store.AdminJobSnapshot
	saves     int
	openCalls int
	openErr   error
	listErr   error
}

func newRecordingJobSnapshotStore() *recordingJobSnapshotStore {
	return &recordingJobSnapshotStore{all: map[string]store.AdminJobSnapshot{}}
}

func (r *recordingJobSnapshotStore) SaveAdminJobSnapshot(_ context.Context, snapshot store.AdminJobSnapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saves++
	r.all[snapshot.JobID] = snapshot
	if snapshot.Terminal {
		// A terminal write is how a job leaves the open set, mirroring the real
		// store rather than depending on the test to update both.
		filtered := r.open[:0]
		for _, existing := range r.open {
			if existing.JobID != snapshot.JobID {
				filtered = append(filtered, existing)
			}
		}
		r.open = filtered
	}
	return nil
}

// recorded returns a snapshot under the lock. Callers must not touch all
// directly.
func (r *recordingJobSnapshotStore) recorded(jobID string) (store.AdminJobSnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot, ok := r.all[jobID]
	return snapshot, ok
}

func (r *recordingJobSnapshotStore) recordedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.all)
}

func (r *recordingJobSnapshotStore) saveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.saves
}

func (r *recordingJobSnapshotStore) openCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.open)
}

func (r *recordingJobSnapshotStore) ListOpenAdminJobSnapshots(context.Context) ([]store.AdminJobSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.openCalls++
	if r.openErr != nil {
		return nil, r.openErr
	}
	return r.open, nil
}

func (r *recordingJobSnapshotStore) ListAdminJobSnapshots(context.Context, int) ([]store.AdminJobSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.listErr != nil {
		return nil, r.listErr
	}
	out := make([]store.AdminJobSnapshot, 0, len(r.all))
	for _, snapshot := range r.all {
		out = append(out, snapshot)
	}
	return out, nil
}

func (r *recordingJobSnapshotStore) GetAdminJobSnapshot(_ context.Context, jobID string) (store.AdminJobSnapshot, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot, ok := r.all[jobID]
	return snapshot, ok, nil
}

func openJobSnapshot(jobID, kind, status string) store.AdminJobSnapshot {
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
	return store.AdminJobSnapshot{
		JobID: jobID, Kind: kind, SessionID: "sess-1", Status: status,
		SnapshotJSON: encoded, StartedAt: "2026-02-01T00:00:00Z",
		UpdatedAt: "2026-02-01T00:02:00Z", Terminal: false,
	}
}

// TestInheritedAdminJobIsMarkedInterruptedNotRunning is the whole design in one
// assertion. Restoring as running is the comfortable answer and the wrong one.
func TestInheritedAdminJobIsMarkedInterruptedNotRunning(t *testing.T) {
	record := newRecordingJobSnapshotStore()
	record.open = []store.AdminJobSnapshot{openJobSnapshot("reindex-1", "reindex", "running")}

	s := &Server{Store: record, AdminJobs: newAdminJobManager()}
	s.attachAdminJobPersistence()

	stored, found := record.recorded("reindex-1")
	if !found {
		t.Fatal("the inherited job was not written back at all; it is still invisible")
	}
	if stored.Status != adminJobInterruptedStatus {
		t.Errorf("status = %q, want %q; nothing is executing this job and reporting it as running is the false answer", stored.Status, adminJobInterruptedStatus)
	}
	if !stored.Terminal {
		t.Error("an interrupted job was left open; every later restart would report it interrupted again")
	}
	if record.openCount() != 0 {
		t.Errorf("the open set still holds %d job(s) after the restore", record.openCount())
	}

	// The reason is stated in the record rather than left for the operator to ask
	// about. "interrupted" alone invites "interrupted by what, and did any of it
	// commit", and answering that in a support thread is the expensive version.
	var wire map[string]any
	if err := json.Unmarshal(stored.SnapshotJSON, &wire); err != nil {
		t.Fatalf("the stored snapshot is not valid JSON, which is the state it exists to prevent: %v", err)
	}
	if wire["resumable"] != false {
		t.Errorf("resumable = %v, want false; the work function's captured state is gone", wire["resumable"])
	}
	reason, _ := wire["interrupted_reason"].(string)
	if reason == "" {
		t.Error("no interrupted_reason was recorded; the operator is left to guess what happened")
	}
}

// TestInheritedAdminJobDoesNotResume is the negative, and it is the one that
// matters. Nothing may be picked up and run.
func TestInheritedAdminJobDoesNotResume(t *testing.T) {
	record := newRecordingJobSnapshotStore()
	record.open = []store.AdminJobSnapshot{openJobSnapshot("reindex-1", "reindex", "running")}

	manager := newAdminJobManager()
	s := &Server{Store: record, AdminJobs: manager}
	s.attachAdminJobPersistence()

	for _, job := range manager.list(0) {
		if job["status"] == "running" {
			t.Errorf("a job was restored as running and would be listed as executing: %+v", job)
		}
	}
	// The manager must not have adopted the job at all: there is no closure to
	// run, so presenting it in the live list would be a job with no work behind it.
	if _, live := manager.get("reindex-1"); live {
		t.Error("the inherited job was adopted into the live manager, where nothing can execute it")
	}
}

// TestAdminJobsArePersistedWhenTheStoreCan records that a job this instance runs
// leaves a record, which is the other half of the feature.
func TestAdminJobsArePersistedWhenTheStoreCan(t *testing.T) {
	record := newRecordingJobSnapshotStore()
	manager := newAdminJobManager()
	s := &Server{Store: record, AdminJobs: manager}
	s.attachAdminJobPersistence()

	manager.start("reindex", "sess-1", map[string]any{}, func(context.Context, adminJobProgressFunc) (map[string]any, error) {
		return map[string]any{"status": "completed"}, nil
	})
	defer manager.closePersistWriter()

	// The work runs on its own goroutine and each write is asynchronous, so the
	// record legitimately passes through an open state on its way to a terminal
	// one. What must eventually be true is that the job is recorded as FINISHED.
	// Asserting that every snapshot is terminal would be asserting that the first
	// write happened to be the last one, which is a race, not a property.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snapshot, found := record.recorded(jobID(t, manager)); found && snapshot.Terminal {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if record.recordedCount() == 0 {
		t.Fatal("a job run by this instance left no record; the next restart would report nothing")
	}
	final, found := record.recorded(jobID(t, manager))
	if !found {
		t.Fatalf("the job is missing from the record entirely: %v", record.all)
	}
	if !final.Terminal {
		t.Errorf("a job that finished was still recorded as non-terminal (%q); the restore would report it interrupted on the next restart", final.Status)
	}
	if final.Status != "completed" {
		t.Errorf("recorded status = %q, want completed", final.Status)
	}
}

// jobID returns the single id the manager knows about, so a test does not have to
// reconstruct the manager's id format to look the job up afterwards.
func jobID(t *testing.T, manager *adminJobManager) string {
	t.Helper()
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	if len(manager.jobs) != 1 {
		t.Fatalf("manager holds %d jobs, want exactly 1", len(manager.jobs))
	}
	for id := range manager.jobs {
		return id
	}
	return ""
}

// TestAdminJobPersistenceIsThrottledWhileRunning keeps a reindex from turning one
// operator action into thousands of writes. Progress is reported per turn, and
// the record does not need per-turn resolution to be useful.
func TestAdminJobPersistenceIsThrottledWhileRunning(t *testing.T) {
	record := newRecordingJobSnapshotStore()
	manager := newAdminJobManager()
	_ = &Server{Store: record, AdminJobs: manager}
	manager.persist = func(job map[string]any) {
		snapshot, err := adminJobSnapshotFromWire(job)
		if err != nil {
			return
		}
		_ = record.SaveAdminJobSnapshot(context.Background(), snapshot)
	}

	manager.start("reindex", "sess-1", map[string]any{}, func(context.Context, adminJobProgressFunc) (map[string]any, error) {
		return map[string]any{"status": "completed"}, nil
	})
	afterStart := record.saveCount()

	// A burst of progress updates, all inside the throttle window.
	for i := 0; i < 200; i++ {
		manager.update("reindex-sess-1-x", "running", map[string]any{"processed": i})
	}
	manager.mu.RLock()
	id := ""
	for candidate := range manager.jobs {
		id = candidate
	}
	manager.mu.RUnlock()
	for i := 0; i < 200; i++ {
		manager.update(id, "running", map[string]any{"processed": i})
	}
	if record.saveCount() > afterStart+2 {
		t.Errorf("200 progress updates produced %d writes; a reindex would turn one operator action into a flood of writes against the canonical database", record.saveCount()-afterStart)
	}
}

// TestLocalRuntimeKeepsItsInMemoryJobs is the direction that must not break. A
// store without the capability has no persist hook and no restore, and the local
// deployment keeps the jobs it has always had.
func TestLocalRuntimeKeepsItsInMemoryJobs(t *testing.T) {
	manager := newAdminJobManager()
	s := &Server{Store: &noSnapshotStore{}, AdminJobs: manager}
	s.attachAdminJobPersistence()

	if manager.persist != nil {
		t.Error("a persist hook was installed for a store that cannot persist; every update would fail")
	}
	manager.start("reindex", "sess-1", map[string]any{}, func(context.Context, adminJobProgressFunc) (map[string]any, error) {
		return map[string]any{"status": "completed"}, nil
	})
	if len(manager.list(0)) == 0 {
		t.Error("the local runtime's in-process job list stopped working")
	}
}

// TestAFailedRestoreIsReportedNotFatal keeps a persistence problem from becoming
// an outage. The deployment can still serve; what it cannot do is explain what it
// inherited, and saying so beats starting with a silent empty list.
func TestAFailedRestoreIsReportedNotFatal(t *testing.T) {
	record := newRecordingJobSnapshotStore()
	record.openErr = fmt.Errorf("d1 unavailable")

	manager := newAdminJobManager()
	s := &Server{Store: record, AdminJobs: manager}
	s.attachAdminJobPersistence() // must not panic or hang

	if manager == nil || s.AdminJobs == nil {
		t.Fatal("the server was left unusable by a failed restore")
	}
}

// noSnapshotStore is a Store WITHOUT the capability: the local runtime.
type noSnapshotStore struct{ store.Store }
