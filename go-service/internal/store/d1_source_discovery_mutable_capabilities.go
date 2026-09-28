package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

// D1 source discovery mutable and query capabilities.
//
// The base SourceDiscoveryStore (save and read one job) was ported alongside the
// provenance-repair capability. These two are the rest of the surface, and they
// are the ones the HTTP layer actually gates the operator workflow on:
//
//	discoveryStore, ok := s.sourceDiscoveryAuthorityStore(w)
//	mutable, ok := discoveryStore.(store.SourceDiscoveryMutableStore)
//	if !ok {
//	    writeError(w, http.StatusServiceUnavailable, "source_discovery_repair_unavailable", ...)
//	}
//
// Without them a Cloudflare deployment can create and read a source discovery
// job but cannot advance, resume, complete, admit, or structurally repair one —
// every route on the job lifecycle answers 503 while the same routes work on
// local MariaDB. That is a P capability reduced to unavailable, which the Stage 1
// capability inventory forbids, and it is why these two are implemented even
// though the base store already had coverage.
//
// The revision counter is the optimistic-concurrency handle
//
// Every update bumps revision and returns the re-read row, so a caller that
// captured a revision can tell its own write from a concurrent one. That is the
// only protection this table has: there is no lease and no compare-and-swap in
// the reference, so two operators advancing the same job last-write-wins here
// exactly as it does there. Adding a guard would make D1 reject a write MariaDB
// accepts.

var _ SourceDiscoveryMutableStore = (*d1Store)(nil)
var _ SourceDiscoveryQueryStore = (*d1Store)(nil)

// UpdateSourceDiscoveryJob advances one job and returns it as stored.
//
// The re-read is not a convenience. The caller receives the row the database
// holds, so the revision it acts on next is the one that was actually committed
// rather than the one it hoped it wrote — which is the whole point of returning
// the job rather than only an error.
func (s *d1Store) UpdateSourceDiscoveryJob(
	ctx context.Context,
	jobID, state string,
	result, coverage map[string]any,
) (*SourceDiscoveryJob, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" || !sourceDiscoveryState(state) {
		// Rejected before any statement, so a caller typo cannot advance a real job
		// to a state outside the pipeline vocabulary. The schema's CHECK constraint
		// would catch it too, but as a constraint violation naming an internal
		// table rather than as a clean sentinel.
		return nil, ErrInvalidReference
	}
	resultJSON, _ := json.Marshal(result)
	coverageJSON, _ := json.Marshal(coverage)

	// updated_at is named here explicitly, and that is a real difference from the
	// reference statement.
	//
	// MariaDB bumps the column implicitly through ON UPDATE CURRENT_TIMESTAMP(3).
	// SQLite has no such column clause and no trigger on this table, so omitting
	// it would leave updated_at frozen at the insert time. FindLatestSourceDiscoveryJob
	// orders by "updated_at DESC, created_at DESC" and idx_source_discovery_state
	// is built on (job_state, updated_at), so a frozen updated_at would make the
	// "latest job for this work" lookup return the OLDEST job for that work — the
	// resume path would silently resume a stale attempt.
	//
	// d1NowExpression is the same shape the schema default writes, so the stored
	// value keeps ordering against a row created by the default.
	changed, err := s.conn.Exec(ctx, `
		UPDATE source_discovery_jobs
		SET job_state = ?, result_json = ?, coverage_report_json = ?,
		    revision = revision + 1, updated_at = `+d1NowExpression+`
		WHERE job_id = ?
	`, state, string(resultJSON), string(coverageJSON), jobID)
	if err != nil {
		return nil, err
	}
	// A zero count means the job id addresses nothing. It is reported as
	// ErrNotFound rather than silently succeeding, because a caller that believed
	// it had advanced a job would then wait for a progress tick that never comes.
	if changed == 0 {
		return nil, ErrNotFound
	}
	return s.GetSourceDiscoveryJob(ctx, jobID)
}

// FindLatestSourceDiscoveryJob returns the most recently touched job for a
// (work, continuity) pair.
//
// The lookup goes through the request payload rather than through a column,
// because the payload is where the reference records the binding: the same job
// row is the record of one discovery attempt, and the work and continuity it
// serves live in its request. The newest-wins rule is the ORDER BY, and the
// created_at tiebreak matters because a single instant can carry two updates.
func (s *d1Store) FindLatestSourceDiscoveryJob(ctx context.Context, workID, continuityID string) (*SourceDiscoveryJob, error) {
	workID = strings.TrimSpace(workID)
	continuityID = strings.TrimSpace(continuityID)
	if workID == "" || continuityID == "" {
		// Both halves are required. A job is bound to a reference work AND a
		// continuity, and matching on either alone would return a job for a
		// different scope that happens to share the id.
		return nil, ErrInvalidReference
	}
	var jobID string
	// json_extract is SQLite's JSON_UNQUOTE(JSON_EXTRACT(...)): it returns the
	// SQL text of a JSON string with the quotes already removed, so the comparison
	// is a plain string equality against the stored value.
	err := s.conn.QueryRow(ctx, `
		SELECT job_id
		FROM source_discovery_jobs
		WHERE json_extract(request_json, '$.work_id') = ?
		  AND json_extract(request_json, '$.continuity_id') = ?
		ORDER BY updated_at DESC, created_at DESC
		LIMIT 1
	`, workID, continuityID).Scan(&jobID)
	if errors.Is(err, errD1NoRows) {
		// No attempt has ever been made for this binding. That is a normal answer
		// for a first run, and the reference reports it the same way, so the route
		// can start one instead of reporting a failure.
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.GetSourceDiscoveryJob(ctx, jobID)
}
