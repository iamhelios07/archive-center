package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// AdminJobSnapshot is one administrative job as the operator last saw it.
//
// It is the whole snapshot rather than a set of named columns. The snapshot is
// already a versioned document (contract_version admin_background_job.v1) that
// the HTTP layer owns, and decomposing it here would create a second shape to
// keep in step with a document that changes when a job kind grows a field. The
// few values that are promoted to columns are the ones queries need: kind and
// status for filtering, the timestamps for ordering, and terminal because the
// restore path selects on it.
type AdminJobSnapshot struct {
	JobID        string
	Kind         string
	SessionID    string
	Status       string
	SnapshotJSON []byte
	StartedAt    string
	UpdatedAt    string
	Terminal     bool
}

// AdminJobSnapshotStore persists operator job state across a Container restart.
//
// It is a record of what happened, not a work queue. Nothing here can be handed
// to a worker and resumed: the closures that perform reindex, rescan,
// session-normalize and dedupe-cleanup capture in-process state that a new
// instance does not have. A store that implied resume would be promising more
// than the Go layer can deliver.
type AdminJobSnapshotStore interface {
	// SaveAdminJobSnapshot writes the current state of a job, replacing any
	// previous snapshot for the same id.
	SaveAdminJobSnapshot(ctx context.Context, snapshot AdminJobSnapshot) error

	// ListAdminJobSnapshots returns the most recent snapshots, newest first.
	ListAdminJobSnapshots(ctx context.Context, limit int) ([]AdminJobSnapshot, error)

	// GetAdminJobSnapshot returns one snapshot. found=false is the normal answer
	// for an unknown id.
	GetAdminJobSnapshot(ctx context.Context, jobID string) (snapshot AdminJobSnapshot, found bool, err error)

	// ListOpenAdminJobSnapshots returns every snapshot still marked non-terminal.
	//
	// The restore path uses it, and the name says what it is for: a Container
	// starting up has to decide what to do about jobs that were in flight when
	// the previous instance went away. It cannot resume them, so it marks them
	// interrupted. Returning "all" instead would force the caller to filter, and
	// a caller that forgot would restore finished jobs as live ones.
	ListOpenAdminJobSnapshots(ctx context.Context) ([]AdminJobSnapshot, error)
}

// d1AdminJobDefaultLimit and d1AdminJobMaxLimit bound the listing. This backs a
// console an operator polls during an incident, and the table grows with every
// job, so an unbounded read is a way to make the console slow exactly when it is
// being used.
const (
	d1AdminJobDefaultLimit = 20
	d1AdminJobMaxLimit     = 200
)

var _ AdminJobSnapshotStore = (*d1Store)(nil)

// NormalizeAdminJobSnapshot validates a snapshot and fills in what a caller may
// reasonably have left empty.
//
// The JSON is checked here rather than at the read path on purpose: a snapshot
// that cannot be parsed is a job record that can never be presented, and finding
// that out when an operator asks for the list is much worse than refusing the
// write that produced it.
func NormalizeAdminJobSnapshot(snapshot AdminJobSnapshot, now time.Time) (AdminJobSnapshot, error) {
	jobID := strings.TrimSpace(snapshot.JobID)
	if jobID == "" {
		return snapshot, fmt.Errorf("store: admin job snapshot requires a job id")
	}
	if !json.Valid(snapshot.SnapshotJSON) {
		return snapshot, fmt.Errorf("store: admin job %q snapshot is not valid JSON", jobID)
	}
	stamp := func(value string) string {
		if strings.TrimSpace(value) != "" {
			return value
		}
		return d1TimeValue(now)
	}
	return AdminJobSnapshot{
		JobID:        jobID,
		Kind:         strings.TrimSpace(snapshot.Kind),
		SessionID:    strings.TrimSpace(snapshot.SessionID),
		Status:       strings.TrimSpace(snapshot.Status),
		SnapshotJSON: snapshot.SnapshotJSON,
		StartedAt:    stamp(snapshot.StartedAt),
		UpdatedAt:    stamp(snapshot.UpdatedAt),
		Terminal:     snapshot.Terminal,
	}, nil
}

func (s *d1Store) SaveAdminJobSnapshot(ctx context.Context, snapshot AdminJobSnapshot) error {
	normalized, err := NormalizeAdminJobSnapshot(snapshot, time.Time{})
	if err != nil {
		return err
	}
	if _, err := s.conn.Exec(ctx, `
		INSERT INTO d1_admin_jobs
			(job_id, kind, session_id, status, snapshot_json, started_at, updated_at, terminal)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (job_id) DO UPDATE SET
			kind = excluded.kind,
			session_id = excluded.session_id,
			status = excluded.status,
			snapshot_json = excluded.snapshot_json,
			updated_at = excluded.updated_at,
			terminal = excluded.terminal`,
		normalized.JobID, normalized.Kind, normalized.SessionID, normalized.Status,
		string(normalized.SnapshotJSON), normalized.StartedAt, normalized.UpdatedAt,
		boolToInt(normalized.Terminal)); err != nil {
		return fmt.Errorf("store: save admin job snapshot %q: %w", normalized.JobID, err)
	}
	return nil
}

func (s *d1Store) ListAdminJobSnapshots(ctx context.Context, limit int) ([]AdminJobSnapshot, error) {
	if limit <= 0 {
		limit = d1AdminJobDefaultLimit
	}
	if limit > d1AdminJobMaxLimit {
		limit = d1AdminJobMaxLimit
	}
	rows, err := s.conn.Query(ctx, d1AdminJobSelect+`
		ORDER BY updated_at DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list admin job snapshots: %w", err)
	}
	defer rows.Close()
	return d1CollectAdminJobSnapshots(rows)
}

func (s *d1Store) GetAdminJobSnapshot(ctx context.Context, jobID string) (AdminJobSnapshot, bool, error) {
	row := s.conn.QueryRow(ctx, d1AdminJobSelect+` WHERE job_id = ?`, strings.TrimSpace(jobID))
	snapshot, err := d1ScanAdminJobSnapshot(row)
	if errors.Is(err, errD1NoRows) {
		return AdminJobSnapshot{}, false, nil
	}
	if err != nil {
		return AdminJobSnapshot{}, false, fmt.Errorf("store: get admin job snapshot %q: %w", jobID, err)
	}
	return snapshot, true, nil
}

func (s *d1Store) ListOpenAdminJobSnapshots(ctx context.Context) ([]AdminJobSnapshot, error) {
	rows, err := s.conn.Query(ctx, d1AdminJobSelect+`
		WHERE terminal = 0
		ORDER BY updated_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("store: list open admin job snapshots: %w", err)
	}
	defer rows.Close()
	return d1CollectAdminJobSnapshots(rows)
}

// d1AdminJobSelect is shared by every read so the column order cannot drift
// between the listing, the single get and the restore.
const d1AdminJobSelect = `SELECT job_id, kind, session_id, status, snapshot_json, started_at, updated_at, terminal FROM d1_admin_jobs`

func d1CollectAdminJobSnapshots(rows D1Rows) ([]AdminJobSnapshot, error) {
	snapshots := make([]AdminJobSnapshot, 0, 8)
	for rows.Next() {
		snapshot, err := d1ScanAdminJobSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate admin job snapshots: %w", err)
	}
	return snapshots, nil
}

func d1ScanAdminJobSnapshot(source interface{ Scan(...any) error }) (AdminJobSnapshot, error) {
	var (
		snapshot    AdminJobSnapshot
		snapshotRaw string
		terminal    any
	)
	if err := source.Scan(&snapshot.JobID, &snapshot.Kind, &snapshot.SessionID, &snapshot.Status,
		&snapshotRaw, &snapshot.StartedAt, &snapshot.UpdatedAt, &terminal); err != nil {
		if errors.Is(err, errD1NoRows) {
			return AdminJobSnapshot{}, err
		}
		return AdminJobSnapshot{}, fmt.Errorf("store: scan admin job snapshot: %w", err)
	}
	value, err := d1AsInt64(terminal)
	if err != nil {
		return AdminJobSnapshot{}, fmt.Errorf("store: admin job terminal flag: %w", err)
	}
	snapshot.Terminal = value != 0
	snapshot.SnapshotJSON = []byte(snapshotRaw)
	return snapshot, nil
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
