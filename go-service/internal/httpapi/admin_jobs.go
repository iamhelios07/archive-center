package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/risulongmemory/archive-center-go/internal/store"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type adminJobProgressFunc func(map[string]any)

type adminBackgroundJob struct {
	ID              string
	Kind            string
	SessionID       string
	Status          string
	Request         map[string]any
	Progress        map[string]any
	Result          map[string]any
	Error           string
	StartedAt       time.Time
	UpdatedAt       time.Time
	FinishedAt      *time.Time
	Revision        uint64
	changed         chan struct{}
	lastPersistedAt time.Time
	cancel          context.CancelFunc
}

type adminJobManager struct {
	mu      sync.RWMutex
	nextID  uint64
	jobs    map[string]*adminBackgroundJob
	order   []string
	maxJobs int

	// persist writes a job's current state somewhere that outlives this process.
	// It is nil on a deployment with no durable store, which is the local
	// runtime and is correct there: the process that owns the jobs is the process
	// that has them.
	//
	// It is never called while the manager lock is held, and never concurrently
	// with itself: see persistCh.
	persist func(job map[string]any)

	// persistCh serialises writes to persist.
	//
	// Dispatching each write as its own goroutine loses the ORDER of the states,
	// and the order is the whole point. A terminal write that a start write
	// overtakes leaves a completed job recorded as still running, which is the
	// exact confusion the durable record exists to remove. That bug was real: it
	// passed in isolation and failed only under the load of the full suite, which
	// is the worst way for a data-loss bug to present itself.
	//
	// One goroutine draining one channel keeps the order the manager produced and
	// keeps the manager lock free of I/O.
	persistCh chan map[string]any
	persistWG sync.WaitGroup
}

// startPersistWriter begins draining persistCh.
//
// It is started on the first enqueue, so a manager on the local runtime never
// runs a goroutine that has nothing to do.
func (m *adminJobManager) startPersistWriter() {
	if m.persist == nil || m.persistCh != nil {
		return
	}
	m.persistCh = make(chan map[string]any, 64)
	m.persistWG.Add(1)
	go func() {
		defer m.persistWG.Done()
		for job := range m.persistCh {
			m.persist(job)
		}
	}()
}

// enqueuePersist hands a snapshot to the writer.
//
// A full queue drops an IN-FLIGHT snapshot, because the next one supersedes it
// within seconds and this is a recent history rather than a log. A terminal
// snapshot is never dropped: it is the state the record exists for, and blocking
// here rather than losing it is correct, since the manager lock is not held.
func (m *adminJobManager) enqueuePersist(job map[string]any, terminal bool) {
	if m == nil || m.persist == nil {
		return
	}
	m.startPersistWriter()
	if terminal {
		m.persistCh <- job
		return
	}
	select {
	case m.persistCh <- job:
	default:
	}
}

// closePersistWriter stops the writer once the queued writes have been applied.
func (m *adminJobManager) closePersistWriter() {
	if m == nil || m.persistCh == nil {
		return
	}
	close(m.persistCh)
	m.persistWG.Wait()
	m.persistCh = nil
}

// adminJobPersistInterval throttles writes of an in-flight job.
//
// A reindex reports progress per turn, and persisting each one would turn a
// single operator action into thousands of writes against the canonical database
// — to save a status line that changes every few seconds anyway. Terminal states
// are always written, because those are the ones the record exists for.
const adminJobPersistInterval = 5 * time.Second

// persistJobLocked queues a snapshot if it is worth writing.
//
// Terminal transitions always persist: a completed, failed, cancelled or
// deferred job is the thing an operator comes back for, and it will never be
// written again. In-flight jobs are throttled, and a job that has never been
// written yet is always written once, so a job that is later lost still leaves a
// record of having existed.
func (m *adminJobManager) persistJobLocked(job *adminBackgroundJob) {
	if m == nil || m.persist == nil || job == nil {
		return
	}
	terminal := adminJobTerminal(job.Status)
	if !terminal && time.Since(job.lastPersistedAt) < adminJobPersistInterval {
		return
	}
	snapshot := job.snapshot()
	// The timestamp is taken when the snapshot is queued, so a slow backend does
	// not cause every subsequent update to queue as well.
	job.lastPersistedAt = time.Now()
	m.enqueuePersist(snapshot, terminal)
}

func newAdminJobManager() *adminJobManager {
	return &adminJobManager{
		jobs:    map[string]*adminBackgroundJob{},
		order:   []string{},
		maxJobs: 80,
	}
}

func (m *adminJobManager) start(kind, sid string, request map[string]any, work func(context.Context, adminJobProgressFunc) (map[string]any, error)) map[string]any {
	if m == nil {
		m = newAdminJobManager()
	}
	normalizedKind := strings.TrimSpace(kind)
	normalizedSID := strings.TrimSpace(sid)
	m.mu.Lock()
	for i := len(m.order) - 1; i >= 0; i-- {
		existing := m.jobs[m.order[i]]
		if existing == nil || !strings.EqualFold(strings.TrimSpace(existing.Kind), normalizedKind) || strings.TrimSpace(existing.SessionID) != normalizedSID {
			continue
		}
		if existing.Status != "queued" && existing.Status != "running" && existing.Status != "cancelling" {
			continue
		}
		snapshot := existing.snapshot()
		snapshot["reused_running_job"] = true
		m.mu.Unlock()
		return snapshot
	}
	now := time.Now().UTC()
	jobCtx, cancelJob := context.WithCancel(context.Background())
	id := fmt.Sprintf("%s-%d-%06d", strings.ToLower(normalizedKind), now.UnixNano(), atomic.AddUint64(&m.nextID, 1))
	job := &adminBackgroundJob{
		ID:        id,
		Kind:      normalizedKind,
		SessionID: normalizedSID,
		Status:    "queued",
		Request:   sanitizeAdminJobRequest(request),
		Progress: map[string]any{
			"status":             "queued",
			"processed":          0,
			"candidate_count":    0,
			"display_total":      0,
			"failed_count":       0,
			"skipped_count":      0,
			"progress_percent":   0,
			"processed_turns":    []int{},
			"failed_turns":       []map[string]any{},
			"failed_ids":         []int64{},
			"last_processed":     nil,
			"foreground_timeout": false,
		},
		StartedAt: now,
		UpdatedAt: now,
		Revision:  1,
		changed:   make(chan struct{}),
		cancel:    cancelJob,
	}
	m.jobs[id] = job
	m.order = append(m.order, id)
	m.pruneLocked()
	// Written immediately, and unconditionally, because a job that is lost
	// entirely leaves nothing behind. If the instance is replaced before this job
	// reaches a terminal state, the operator needs to know it existed and what
	// kind it was, even if the throttled in-flight writes never happened.
	m.persistJobLocked(job)
	snapshot := job.snapshot()
	m.mu.Unlock()

	go func() {
		defer cancelJob()
		m.update(id, "running", map[string]any{"status": "running", "started": true})
		result, err := work(jobCtx, func(progress map[string]any) {
			m.update(id, "running", progress)
		})
		if err != nil {
			if errors.Is(err, context.Canceled) {
				m.finish(id, "cancelled", result, err.Error())
				return
			}
			m.finish(id, "failed", result, err.Error())
			return
		}
		terminalStatus := "completed"
		if strings.EqualFold(normalizedKind, "session_normalize") {
			switch strings.ToLower(strings.TrimSpace(stringFromAny(result["status"]))) {
			case "deferred", "partial_deferred":
				terminalStatus = "deferred"
			case "partial_error":
				terminalStatus = "partial_error"
			case "failed", "error", "blocked":
				terminalStatus = "failed"
			}
		}
		m.finish(id, terminalStatus, result, "")
	}()

	return snapshot
}

func (m *adminJobManager) get(id string) (map[string]any, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	job, ok := m.jobs[strings.TrimSpace(id)]
	if !ok || job == nil {
		return nil, false
	}
	return job.snapshot(), true
}

// exists answers "is this job in this process", without handing out a snapshot.
//
// It exists for the event stream, which has to choose between streaming a live
// job and answering about a stored one. Using get for that would build a whole
// snapshot just to compare its key, and a caller that then discarded the result
// would be doing the expensive part for nothing.
func (m *adminJobManager) exists(id string) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.jobs[strings.TrimSpace(id)] != nil
}

func (m *adminJobManager) list(limit int) []map[string]any {
	if m == nil {
		return []map[string]any{}
	}
	if limit <= 0 {
		limit = 20
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []map[string]any{}
	for i := len(m.order) - 1; i >= 0 && len(out) < limit; i-- {
		if job := m.jobs[m.order[i]]; job != nil {
			out = append(out, job.snapshot())
		}
	}
	return out
}

func (m *adminJobManager) update(id, status string, progress map[string]any) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[id]
	if job == nil || adminJobTerminal(job.Status) {
		return
	}
	if strings.TrimSpace(status) != "" {
		job.Status = strings.TrimSpace(status)
	}
	if job.Progress == nil {
		job.Progress = map[string]any{}
	}
	previousStage := strings.TrimSpace(stringFromAny(job.Progress["stage"]))
	incomingStage := strings.TrimSpace(stringFromAny(progress["stage"]))
	if incomingStage != "" && incomingStage != previousStage {
		if _, ok := progress["display_total"]; !ok {
			if _, candidateOK := progress["candidate_count"]; !candidateOK {
				if _, totalOK := progress["total_candidates"]; !totalOK {
					if _, genericTotalOK := progress["total"]; !genericTotalOK {
						job.Progress["display_total"] = 0
					}
				}
			}
		}
	}
	for k, v := range progress {
		job.Progress[k] = v
	}
	if _, ok := progress["display_total"]; !ok {
		for _, key := range []string{"candidate_count", "total_candidates", "total"} {
			if value, found := progress[key]; found {
				job.Progress["display_total"] = intFromAny(value, 0)
				break
			}
		}
	}
	job.UpdatedAt = time.Now().UTC()
	job.publishChangeLocked()
	m.persistJobLocked(job)
}

func (m *adminJobManager) finish(id, status string, result map[string]any, errText string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	job := m.jobs[id]
	if job == nil || adminJobTerminal(job.Status) {
		return
	}
	now := time.Now().UTC()
	job.Status = status
	job.UpdatedAt = now
	job.FinishedAt = &now
	job.Result = result
	job.Error = strings.TrimSpace(errText)
	if job.Error != "" {
		slog.Error("background job failed", "job_id", id, "status", status, "error", scrubCriticFailureText(job.Error, ""))
	}
	job.cancel = nil
	if job.Progress == nil {
		job.Progress = map[string]any{}
	}
	job.Progress["status"] = status
	job.Progress["done"] = status == "completed"
	job.Progress["error"] = nilIfEmpty(job.Error)
	job.Progress["finished_at"] = now.Format(time.RFC3339)
	if _, ok := job.Progress["progress_percent"]; !ok {
		job.Progress["progress_percent"] = 100
	}
	if status == "completed" {
		job.Progress["progress_percent"] = 100
	} else if status == "deferred" && intFromAny(job.Progress["progress_percent"], 0) >= 100 {
		job.Progress["progress_percent"] = 99
	}
	job.publishChangeLocked()
	m.persistJobLocked(job)
}

func (m *adminJobManager) cancelJob(id string) (map[string]any, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.Lock()
	job := m.jobs[strings.TrimSpace(id)]
	if job == nil {
		m.mu.Unlock()
		return nil, false
	}
	if adminJobTerminal(job.Status) {
		snapshot := job.snapshot()
		m.mu.Unlock()
		return snapshot, true
	}
	cancel := job.cancel
	now := time.Now().UTC()
	job.Status = "cancelled"
	job.UpdatedAt = now
	job.FinishedAt = &now
	job.Error = context.Canceled.Error()
	job.cancel = nil
	if job.Progress == nil {
		job.Progress = map[string]any{}
	}
	job.Progress["status"] = "cancelled"
	job.Progress["done"] = false
	job.Progress["error"] = job.Error
	job.Progress["finished_at"] = now.Format(time.RFC3339)
	job.publishChangeLocked()
	m.persistJobLocked(job)
	snapshot := job.snapshot()
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return snapshot, true
}

func (j *adminBackgroundJob) publishChangeLocked() {
	if j == nil {
		return
	}
	j.Revision++
	if j.changed != nil {
		close(j.changed)
	}
	j.changed = make(chan struct{})
}

func adminJobTerminal(status string) bool {
	switch strings.TrimSpace(status) {
	case "completed", "deferred", "partial_error", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func (m *adminJobManager) pruneLocked() {
	if m.maxJobs <= 0 {
		m.maxJobs = 80
	}
	if len(m.order) <= m.maxJobs {
		return
	}
	remainingToRemove := len(m.order) - m.maxJobs
	kept := make([]string, 0, len(m.order))
	for _, id := range m.order {
		job := m.jobs[id]
		if remainingToRemove > 0 && job != nil && adminJobTerminal(job.Status) {
			delete(m.jobs, id)
			remainingToRemove--
			continue
		}
		kept = append(kept, id)
	}
	m.order = kept
}

func (j *adminBackgroundJob) snapshot() map[string]any {
	if j == nil {
		return map[string]any{}
	}
	out := map[string]any{
		"job_id":           j.ID,
		"kind":             j.Kind,
		"chat_session_id":  j.SessionID,
		"status":           j.Status,
		"request":          cloneMapAny(j.Request),
		"progress":         cloneMapAny(j.Progress),
		"result":           cloneMapAny(j.Result),
		"error":            nilIfEmpty(j.Error),
		"started_at":       j.StartedAt.Format(time.RFC3339),
		"updated_at":       j.UpdatedAt.Format(time.RFC3339),
		"background":       true,
		"contract_version": "admin_background_job.v1",
		"revision":         j.Revision,
		"terminal":         adminJobTerminal(j.Status),
	}
	if j.FinishedAt != nil {
		out["finished_at"] = j.FinishedAt.Format(time.RFC3339)
	}
	return out
}

func (m *adminJobManager) observe(id string, afterRevision uint64) (map[string]any, <-chan struct{}, bool) {
	if m == nil {
		return nil, nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	job := m.jobs[strings.TrimSpace(id)]
	if job == nil {
		return nil, nil, false
	}
	var snapshot map[string]any
	if job.Revision > afterRevision || adminJobTerminal(job.Status) {
		snapshot = job.snapshot()
	}
	return snapshot, job.changed, true
}

func cloneMapAny(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func sanitizeAdminJobRequest(req map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range req {
		lower := strings.ToLower(k)
		if strings.Contains(lower, "key") || strings.Contains(lower, "secret") || strings.Contains(lower, "password") || strings.Contains(lower, "token") {
			out[k] = "[redacted]"
			continue
		}
		out[k] = v
	}
	return out
}

func adminJobProgressPercent(processed, total int) int {
	if total <= 0 {
		if processed > 0 {
			return 100
		}
		return 0
	}
	pct := processed * 100 / total
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// adminResetRunJob presents a durable D1 reset run in the same shape as an
// in-process job, so one endpoint can answer for both.
//
// The mapping is deliberately lossy in one direction only: a reset run has no
// per-session identity and no progress bar, so those are absent rather than
// invented as zeros. An operator reading `processed: 0` would conclude the reset
// did nothing, which is the opposite of what a completed reset means.
func adminResetRunJob(run store.AdminResetRun) map[string]any {
	job := map[string]any{
		"id":             run.ResetRunID,
		"kind":           "database_reset",
		"status":         run.Status,
		"epoch":          run.Epoch,
		"rows_deleted":   run.RowsDeleted,
		"tables_cleared": run.TablesCleared,
		"retry_count":    run.RetryCount,
		"started_at":     run.StartedAt,
		"updated_at":     run.UpdatedAt,
		// The whole point: this row outlives the process that wrote it.
		"durable": true,
		"source":  "d1_reset_runs",
	}
	if run.CompletedAt != "" {
		job["completed_at"] = run.CompletedAt
	}
	if run.LastError != "" {
		job["last_error"] = run.LastError
	}
	return job
}

// durableAdminResetRuns reads the reset control plane when the store can.
//
// It returns nil rather than an error when the capability is absent, because a
// store that cannot persist a reset run is the local runtime, and its in-process
// job list is the whole truth there. An error is reserved for a store that
// claims the capability and then fails, since that would otherwise be reported as
// an empty list — the same silence that made this gap invisible.
func (s *Server) durableAdminResetRuns(r *http.Request, limit int) ([]map[string]any, error) {
	reader, ok := s.Store.(store.AdminResetRunReader)
	if !ok {
		return nil, nil
	}
	runs, err := reader.ListAdminResetRuns(r.Context(), limit)
	if err != nil {
		return nil, err
	}
	jobs := make([]map[string]any, 0, len(runs))
	for _, run := range runs {
		jobs = append(jobs, adminResetRunJob(run))
	}
	return jobs, nil
}

func (s *Server) handleAdminJobs(w http.ResponseWriter, r *http.Request) {
	limit := intFromAny(r.URL.Query().Get("limit"), 20)
	jobs := []map[string]any{}
	if s.AdminJobs != nil {
		jobs = s.AdminJobs.list(limit)
	}
	// Every in-process job is explicitly marked as not durable. Before this, an
	// operator could not tell a job that would survive a Container restart from
	// one that would vanish, and the list looked equally trustworthy either way.
	for _, job := range jobs {
		if _, present := job["durable"]; !present {
			job["durable"] = false
			job["source"] = "process_memory"
		}
	}

	// The reset control plane is merged rather than swapped in. A reset is the
	// destructive operator action, the one with a confirmation token and an epoch
	// fence, and it is the one whose absence from this list after a restart would
	// most mislead. The other jobs stay in memory and are still lost; the flag
	// says so instead of pretending otherwise.
	durable, err := s.durableAdminResetRuns(r, limit)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "admin_jobs_unavailable", err.Error())
		return
	}
	jobs = append(jobs, durable...)

	// Job snapshots are merged in for the same reason, and are marked so a
	// restored job is distinguishable from a live one. A job the previous
	// instance left open is presented as interrupted, never as running: nothing
	// is executing it, and a progress bar that has stopped moving is the worst
	// thing to show an operator who cannot tell why.
	persisted, err := s.durableAdminJobSnapshots(r, limit)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "admin_jobs_unavailable", err.Error())
		return
	}
	jobs = append(jobs, persisted...)

	sort.SliceStable(jobs, func(i, j int) bool {
		return strings.Compare(fmt.Sprint(jobs[i]["started_at"]), fmt.Sprint(jobs[j]["started_at"])) > 0
	})
	if limit > 0 && len(jobs) > limit {
		jobs = jobs[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"jobs":   jobs,
		// Stated once, rather than left for the reader to infer from the flags:
		// on a stateless Container the in-process half of this list is empty after
		// a restart, and that is the design, not a fault.
		"durable_source":        s.resetRunsAreDurable(),
		"job_snapshots_durable": s.adminJobSnapshotsAreDurable(),
	})
}

// durableAdminJobSnapshots reads persisted job records, newest first.
//
// A snapshot is only presented when this process is not already presenting the
// same job from memory, so a job that is running here appears once rather than
// twice with two different statuses.
func (s *Server) durableAdminJobSnapshots(r *http.Request, limit int) ([]map[string]any, error) {
	snaps, ok := s.Store.(store.AdminJobSnapshotStore)
	if !ok {
		return nil, nil
	}
	snapshots, err := snaps.ListAdminJobSnapshots(r.Context(), limit)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(snapshots))
	for _, snapshot := range snapshots {
		var job map[string]any
		if len(snapshot.SnapshotJSON) == 0 || json.Unmarshal(snapshot.SnapshotJSON, &job) != nil {
			// A snapshot that does not parse is still a record that something
			// happened, and the columns carry its identity. Omitting it would make
			// the job look like it never existed.
			job = map[string]any{
				"job_id": snapshot.JobID,
				"kind":   snapshot.Kind,
				"status": snapshot.Status,
				"error":  "the stored snapshot could not be decoded; the job record itself is intact",
			}
		}
		if _, live := s.AdminJobs.get(snapshot.JobID); live {
			continue
		}
		job["durable"] = true
		job["source"] = "d1_admin_jobs"
		out = append(out, job)
	}
	return out, nil
}

func (s *Server) adminJobSnapshotsAreDurable() bool {
	_, ok := s.Store.(store.AdminJobSnapshotStore)
	return ok
}

// adminResetRunJobByID reads one durable reset run.
//
// The event stream needs it to tell apart two kinds of stored record. A reset run
// is a presentation over the control plane: its progress advances because a
// worker is checkpointing into D1, not because this process is executing it, so
// it has no event stream at all. An interrupted job is different — it is simply
// over, and the operator should be shown that once and told the stream is done.
func (s *Server) adminResetRunJobByID(ctx context.Context, id string) (map[string]any, bool, error) {
	reader, ok := s.Store.(store.AdminResetRunReader)
	if !ok {
		return nil, false, nil
	}
	run, found, err := reader.GetAdminResetRun(ctx, id)
	if err != nil || !found {
		return nil, found, err
	}
	return adminResetRunJob(run), true, nil
}

// resetRunsAreDurable reports whether the reset control plane survives a restart,
// so an operator can tell "no reset has run" from "this deployment cannot record
// that one ran".
func (s *Server) resetRunsAreDurable() bool {
	_, ok := s.Store.(store.AdminResetRunReader)
	return ok
}

func (s *Server) handleAdminJob(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("job_id"))
	if id == "" {
		writeBadRequest(w, "job_id is required")
		return
	}
	if s.AdminJobs != nil {
		if r.Method == http.MethodDelete {
			job, ok := s.AdminJobs.cancelJob(id)
			if !ok {
				writeJSON(w, http.StatusNotFound, map[string]any{"status": "not_found", "job_id": id})
				return
			}
			writeJSON(w, http.StatusOK, job)
			return
		}
		if job, ok := s.AdminJobs.get(id); ok {
			if _, present := job["durable"]; !present {
				job["durable"] = false
				job["source"] = "process_memory"
			}
			writeJSON(w, http.StatusOK, job)
			return
		}
	}
	// A reset run is looked up after the in-process list, not instead of it. An
	// operator who bookmarked a reset url expects it to keep working across a
	// Container restart, and the id is the durable one precisely because the
	// process that created it is gone.
	if reader, ok := s.Store.(store.AdminResetRunReader); ok {
		run, found, err := reader.GetAdminResetRun(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "admin_job_unavailable", err.Error())
			return
		}
		if found {
			writeJSON(w, http.StatusOK, adminResetRunJob(run))
			return
		}
	}
	// The persisted snapshots, last. They are checked after the live manager and
	// after the reset control plane so a job this process is actually running is
	// never shadowed by a stale copy of itself.
	//
	// Without this the list and the detail disagreed: a job restored as
	// interrupted appeared in /admin/jobs and then 404'd on its own url, which is
	// a worse answer than either omitting it or showing it, because it suggests
	// the id was wrong rather than that the job is over.
	if job, found, err := s.persistedAdminJob(r.Context(), id); err != nil {
		writeError(w, http.StatusServiceUnavailable, "admin_job_unavailable", err.Error())
		return
	} else if found {
		writeJSON(w, http.StatusOK, job)
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]any{"status": "not_found", "job_id": id})
}

// persistedAdminJob reads one stored job snapshot and shapes it for the wire the
// same way the list does, so a job looks the same however it was asked for.
func (s *Server) persistedAdminJob(ctx context.Context, id string) (map[string]any, bool, error) {
	snaps, ok := s.Store.(store.AdminJobSnapshotStore)
	if !ok {
		return nil, false, nil
	}
	snapshot, found, err := snaps.GetAdminJobSnapshot(ctx, id)
	if err != nil || !found {
		return nil, found, err
	}
	return snapshotJobForWire(snapshot), true, nil
}

// snapshotJobForWire decodes a stored snapshot, falling back to the columns when
// the document cannot be decoded. A record that does not parse is still a record
// that something happened, and the columns carry its identity.
func snapshotJobForWire(snapshot store.AdminJobSnapshot) map[string]any {
	var job map[string]any
	if len(snapshot.SnapshotJSON) == 0 || json.Unmarshal(snapshot.SnapshotJSON, &job) != nil || job == nil {
		job = map[string]any{
			"job_id": snapshot.JobID,
			"kind":   snapshot.Kind,
			"status": snapshot.Status,
			"error":  "the stored snapshot could not be decoded; the job record itself is intact",
		}
	}
	job["durable"] = true
	job["source"] = "d1_admin_jobs"
	return job
}

func (s *Server) handleAdminJobEvents(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("job_id"))
	if id == "" {
		writeBadRequest(w, "job_id is required")
		return
	}
	afterRevision, err := strconv.ParseUint(strings.TrimSpace(r.URL.Query().Get("after_revision")), 10, 64)
	if err != nil && strings.TrimSpace(r.URL.Query().Get("after_revision")) != "" {
		writeBadRequest(w, "after_revision must be a non-negative integer")
		return
	}

	// A job this process is not running has no live stream, but that is not the
	// same as not existing.
	//
	// It used to answer 404, so an operator who opened the stream for a job that
	// was interrupted by a Container replacement was told the job did not exist —
	// while /admin/jobs was listing it. Worse, the 404 arrived at exactly the
	// moment the stream was being used to watch a job, so it read as a broken
	// endpoint rather than as a finished job.
	//
	// A persisted job has no future updates: nothing is executing it. So the
	// stream emits its final state once and closes, which is what a client
	// watching a terminal job would have seen anyway.
	if s.AdminJobs == nil || !s.AdminJobs.exists(id) {
		job, found, lookupErr := s.persistedAdminJob(r.Context(), id)
		if lookupErr != nil {
			writeError(w, http.StatusServiceUnavailable, "admin_job_unavailable", lookupErr.Error())
			return
		}
		if found {
			w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache, no-store")
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(http.StatusOK)
			// Marked terminal for the client, because there will be no second
			// line: this is the whole answer.
			job["terminal"] = true
			_ = json.NewEncoder(w).Encode(job)
			return
		}
		// A reset run is not a finished job. Its progress advances because a
		// worker is checkpointing into D1, not because this process is executing
		// it, so it has no event stream at all. Saying so is more useful than an
		// empty stream or a bare 404, both of which read as a broken endpoint.
		if _, resetFound, resetErr := s.adminResetRunJobByID(r.Context(), id); resetErr != nil {
			writeError(w, http.StatusServiceUnavailable, "admin_job_unavailable", resetErr.Error())
			return
		} else if resetFound {
			writeJSON(w, http.StatusConflict, map[string]any{
				"status":      "not_streamable",
				"reason_code": "reset_run_has_no_event_stream",
				"job_id":      id,
				"detail":      "reset progress is read from /admin/jobs/{id}, not streamed",
			})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]any{"status": "not_found", "job_id": id})
		return
	}

	if _, _, ok := s.AdminJobs.observe(id, afterRevision); !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"status": "not_found", "job_id": id})
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"status":      "stream_transport_unavailable",
			"reason_code": "http_flusher_unavailable",
			"job_id":      id,
		})
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	currentRevision := afterRevision
	for {
		snapshot, changed, found := s.AdminJobs.observe(id, currentRevision)
		if !found {
			return
		}
		if snapshot != nil {
			if err := encoder.Encode(snapshot); err != nil {
				return
			}
			flusher.Flush()
			currentRevision = uint64FromAny(snapshot["revision"])
			if boolFromAny(snapshot["terminal"]) {
				return
			}
			continue
		}
		select {
		case <-r.Context().Done():
			return
		case <-changed:
		}
	}
}

func uint64FromAny(v any) uint64 {
	switch value := v.(type) {
	case uint64:
		return value
	case uint:
		return uint64(value)
	case int:
		if value > 0 {
			return uint64(value)
		}
	case int64:
		if value > 0 {
			return uint64(value)
		}
	case float64:
		if value > 0 {
			return uint64(value)
		}
	case string:
		parsed, _ := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		return parsed
	}
	return 0
}
