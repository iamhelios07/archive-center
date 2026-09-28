package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/store"
)

// Restoring operator jobs after a Container restart.
//
// The in-process job manager is memory only, and a Cloudflare Container is
// replaced rather than repaired, so every job was in flight when the previous
// instance went away disappeared with no trace. An operator polling /admin/jobs
// saw an empty list and could not tell "nothing is running" from "the record of
// what was running was lost".
//
// What the restore does NOT do is hand the jobs back. There is no resume: the
// closures that perform reindex, rescan, session-normalize and dedupe-cleanup
// capture in-process state that a new instance does not have, and the progress
// map holds a cursor for reporting, not for resuming. Restoring such a job as
// running would be the more comfortable answer and the false one — nothing is
// executing it, and an operator watching a progress bar that has stopped moving
// has no way to know that.
//
// So a job found open at startup is written back as INTERRUPTED, with the reason
// stated. The operator re-runs it. That is a visible cost, chosen over a resume
// that would be a guess about what the previous instance already committed.

// adminJobInterruptedStatus is what an open job becomes when a new instance
// starts and finds it.
//
// It is deliberately not one of the statuses the running job writes, and it is
// terminal: the work did not finish and will not be picked up, so leaving it open
// would make the next restart report it interrupted again and make the
// "list open jobs" query return it forever.
const adminJobInterruptedStatus = "interrupted"

// attachAdminJobPersistence wires the manager to a durable store when one is
// available, and restores what the previous instance left behind.
//
// It is called once during server construction, before any route serves, so a
// job is never presented as if it had just been started by this instance.
func (s *Server) attachAdminJobPersistence() {
	if s == nil || s.AdminJobs == nil || s.Store == nil {
		return
	}
	snaps, ok := s.Store.(store.AdminJobSnapshotStore)
	if !ok {
		// The local runtime. Its job list is the whole truth, and it keeps it.
		return
	}

	s.AdminJobs.persist = func(job map[string]any) {
		snapshot, err := adminJobSnapshotFromWire(job)
		if err != nil {
			slog.Warn("admin job snapshot could not be prepared", "error", err)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := snaps.SaveAdminJobSnapshot(ctx, snapshot); err != nil {
			// A failed write must not fail the job. The work is real whether or
			// not its record survives, and turning a persistence problem into a
			// job failure would abandon work that was about to succeed.
			slog.Warn("admin job snapshot could not be saved", "job_id", snapshot.JobID, "error", err)
		}
	}

	s.restoreInterruptedAdminJobs(context.Background())
}

// restoreInterruptedAdminJobs marks every job the previous instance left open as
// interrupted, and reports how many it found.
//
// It runs synchronously at startup. An operator polling immediately after a
// deploy must see the interrupted state on the first request, not a window where
// the list is briefly empty.
func (s *Server) restoreInterruptedAdminJobs(ctx context.Context) {
	snaps, ok := s.Store.(store.AdminJobSnapshotStore)
	if !ok {
		return
	}
	open, err := snaps.ListOpenAdminJobSnapshots(ctx)
	if err != nil {
		// Reported rather than fatal. The deployment can still serve; what it
		// cannot do is tell the operator about jobs it inherited. Saying so in
		// /admin/jobs is better than starting with a lie and no explanation.
		slog.Warn("inherited admin jobs could not be read", "error", err)
		return
	}
	for _, snapshot := range open {
		if err := s.markAdminJobInterrupted(ctx, snaps, snapshot); err != nil {
			slog.Warn("inherited admin job could not be marked interrupted",
				"job_id", snapshot.JobID, "error", err)
		}
	}
}

func (s *Server) markAdminJobInterrupted(ctx context.Context, snaps store.AdminJobSnapshotStore, snapshot store.AdminJobSnapshot) error {
	var wire map[string]any
	if len(snapshot.SnapshotJSON) > 0 {
		// A snapshot that does not parse is still worth reporting; the columns
		// carry the identity even when the document does not.
		if err := json.Unmarshal(snapshot.SnapshotJSON, &wire); err != nil {
			wire = map[string]any{}
		}
	}
	now := time.Now().UTC()
	wire["job_id"] = snapshot.JobID
	wire["kind"] = snapshot.Kind
	wire["status"] = adminJobInterruptedStatus
	wire["updated_at"] = now.Format(time.RFC3339)
	if snapshot.SessionID != "" {
		wire["chat_session_id"] = snapshot.SessionID
	}
	if progress, _ := wire["progress"].(map[string]any); progress != nil {
		progress["status"] = adminJobInterruptedStatus
		progress["done"] = false
	}
	// The reason is stated rather than implied. "interrupted" alone invites the
	// question "interrupted by what, and did any of it commit", and answering it
	// in the record is cheaper than answering it in a support thread.
	wire["interrupted_reason"] = "the instance running this job was replaced; the work was not resumed and must be re-run"
	wire["resumable"] = false
	wire["finished_at"] = now.Format(time.RFC3339)
	wire["durable"] = true
	wire["source"] = "d1_admin_jobs"

	prepared, err := adminJobSnapshotFromWire(wire)
	if err != nil {
		return err
	}
	// Terminal on the way in: the job is no longer open, so the next restart does
	// not report it interrupted a second time.
	prepared.Terminal = true
	return snaps.SaveAdminJobSnapshot(ctx, prepared)
}

// adminJobSnapshotFromWire converts a job presentation into a storable snapshot.
//
// The identity is taken from the wire rather than from a struct on purpose: the
// presentation is what the HTTP layer already produces, and deriving the stored
// row from it keeps one definition of what a job is rather than two that drift.
func adminJobSnapshotFromWire(job map[string]any) (store.AdminJobSnapshot, error) {
	jobID := strings.TrimSpace(stringFromAny(job["job_id"]))
	if jobID == "" {
		jobID = strings.TrimSpace(stringFromAny(job["id"]))
	}
	if jobID == "" {
		return store.AdminJobSnapshot{}, errAdminJobSnapshotWithoutID
	}
	status := strings.TrimSpace(stringFromAny(job["status"]))
	encoded, err := json.Marshal(job)
	if err != nil {
		return store.AdminJobSnapshot{}, err
	}
	return store.AdminJobSnapshot{
		JobID:        jobID,
		Kind:         strings.TrimSpace(stringFromAny(job["kind"])),
		SessionID:    strings.TrimSpace(stringFromAny(job["chat_session_id"])),
		Status:       status,
		SnapshotJSON: encoded,
		StartedAt:    strings.TrimSpace(stringFromAny(job["started_at"])),
		UpdatedAt:    strings.TrimSpace(stringFromAny(job["updated_at"])),
		Terminal:     adminJobTerminal(status),
	}, nil
}

var errAdminJobSnapshotWithoutID = &adminJobSnapshotError{"admin job snapshot has no job id"}

type adminJobSnapshotError struct{ message string }

func (e *adminJobSnapshotError) Error() string { return e.message }
