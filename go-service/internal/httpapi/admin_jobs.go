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
	ID         string
	Kind       string
	SessionID  string
	Status     string
	Request    map[string]any
	Progress   map[string]any
	Result     map[string]any
	Error      string
	StartedAt  time.Time
	UpdatedAt  time.Time
	FinishedAt *time.Time
	Revision   uint64
	changed    chan struct{}
	cancel     context.CancelFunc
}

type adminJobManager struct {
	mu      sync.RWMutex
	nextID  uint64
	jobs    map[string]*adminBackgroundJob
	order   []string
	maxJobs int
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
		"durable_source": s.resetRunsAreDurable(),
	})
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
	writeJSON(w, http.StatusNotFound, map[string]any{"status": "not_found", "job_id": id})
}

func (s *Server) handleAdminJobEvents(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("job_id"))
	if id == "" {
		writeBadRequest(w, "job_id is required")
		return
	}
	if s.AdminJobs == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"status": "not_found", "job_id": id})
		return
	}
	afterRevision, err := strconv.ParseUint(strings.TrimSpace(r.URL.Query().Get("after_revision")), 10, 64)
	if err != nil && strings.TrimSpace(r.URL.Query().Get("after_revision")) != "" {
		writeBadRequest(w, "after_revision must be a non-negative integer")
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
