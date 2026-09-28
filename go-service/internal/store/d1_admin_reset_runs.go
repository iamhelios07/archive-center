package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

// d1DefaultAdminResetRunLimit bounds an unparameterised listing. The reset table
// only grows, and this backs a console an operator polls during an incident, so
// the unbounded read is the failure mode to avoid.
const d1DefaultAdminResetRunLimit = 20

// d1MaxAdminResetRunLimit caps what a caller may ask for, so a single request
// cannot ask the database for a table scan during a recovery.
const d1MaxAdminResetRunLimit = 200

var _ AdminResetRunReader = (*d1Store)(nil)

// ListAdminResetRuns reads the reset control plane back for presentation.
//
// The rows are the same ones ResetAll wrote as it worked, so this reports what
// actually happened rather than what the process that happened to be serving this
// request believes happened. On a stateless Container those are different things:
// the worker can be a different instance from the one that did the work, or no
// instance at all since it did.
func (s *d1Store) ListAdminResetRuns(ctx context.Context, limit int) ([]AdminResetRun, error) {
	if limit <= 0 {
		limit = d1DefaultAdminResetRunLimit
	}
	if limit > d1MaxAdminResetRunLimit {
		limit = d1MaxAdminResetRunLimit
	}

	rows, err := s.conn.Query(ctx, `
		SELECT reset_run_id, epoch, status, rows_deleted, tables_cleared,
		       started_at, updated_at, COALESCE(completed_at, ''),
		       COALESCE(last_error, ''), retry_count
		FROM d1_reset_runs
		ORDER BY started_at DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list admin reset runs: %w", err)
	}
	defer rows.Close()

	runs := make([]AdminResetRun, 0, 8)
	for rows.Next() {
		run, err := d1ScanAdminResetRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate admin reset runs: %w", err)
	}
	return runs, nil
}

// GetAdminResetRun returns one run. A missing row is found=false rather than an
// error, so the HTTP layer can answer 404 for a stale bookmark without reporting
// a failure on a read-only path.
func (s *d1Store) GetAdminResetRun(ctx context.Context, resetRunID string) (AdminResetRun, bool, error) {
	row := s.conn.QueryRow(ctx, `
		SELECT reset_run_id, epoch, status, rows_deleted, tables_cleared,
		       started_at, updated_at, COALESCE(completed_at, ''),
		       COALESCE(last_error, ''), retry_count
		FROM d1_reset_runs WHERE reset_run_id = ?`, resetRunID)

	run, err := d1ScanAdminResetRun(row)
	if errors.Is(err, errD1NoRows) {
		return AdminResetRun{}, false, nil
	}
	if err != nil {
		return AdminResetRun{}, false, fmt.Errorf("store: get admin reset run %q: %w", resetRunID, err)
	}
	return run, true, nil
}

// d1ScanAdminResetRun reads one row through either a cursor or a single-row
// handle, so the list and the get share one column order and cannot drift.
//
// The bridge transports JSON, so integers arrive as float64; the conversion is
// explicit here rather than left to a driver's coercion.
func d1ScanAdminResetRun(source interface{ Scan(...any) error }) (AdminResetRun, error) {
	var (
		run           AdminResetRun
		epoch         any
		rowsDeleted   any
		tablesCleared any
		retryCount    any
	)
	if err := source.Scan(&run.ResetRunID, &epoch, &run.Status, &rowsDeleted, &tablesCleared,
		&run.StartedAt, &run.UpdatedAt, &run.CompletedAt, &run.LastError, &retryCount); err != nil {
		if errors.Is(err, errD1NoRows) {
			return AdminResetRun{}, err
		}
		return AdminResetRun{}, fmt.Errorf("store: scan admin reset run: %w", err)
	}
	var err error
	if run.Epoch, err = d1AsInt64(epoch); err != nil {
		return AdminResetRun{}, fmt.Errorf("store: reset run epoch: %w", err)
	}
	if run.RowsDeleted, err = d1AsInt64(rowsDeleted); err != nil {
		return AdminResetRun{}, fmt.Errorf("store: reset run rows_deleted: %w", err)
	}
	if run.TablesCleared, err = d1AsInt64(tablesCleared); err != nil {
		return AdminResetRun{}, fmt.Errorf("store: reset run tables_cleared: %w", err)
	}
	if run.RetryCount, err = d1AsInt64(retryCount); err != nil {
		return AdminResetRun{}, fmt.Errorf("store: reset run retry_count: %w", err)
	}
	// True by construction: a row was read back, so this state outlives the
	// process that wrote it.
	run.Durable = true
	return run, nil
}

// d1AsInt64 accepts either an int64 or a JSON float64. SQLite hands back
// integers over the direct driver and numbers over the bridge, and the same code
// has to read both without a caller knowing which one it is talking to.
func d1AsInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case float64:
		return int64(typed), nil
	case int:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	default:
		return 0, fmt.Errorf("store: expected an integer, got %T", value)
	}
}
