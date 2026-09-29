package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// D1 memory reprocessing capabilities.
//
// memory_reprocessing_jobs is the durable revision-aware worker queue that sits
// between an accepted source turn and its derived memory. Three capabilities
// live on this table and they are ported here together because they are one
// mechanism:
//
//   - MemoryReprocessingJobStore         enqueue, claim, complete, fail
//   - MemoryReprocessingJobReopener      operator recovery for an abandoned job
//   - MemoryReprocessingWakeScheduleStore the one restart-recoverable wake cursor
//
// The write path already exists on D1 — d1_source_revision_capabilities.go marks
// a job stale_rejected when a revision is invalidated — but nothing could ever
// put a job in the queue, and the worker therefore woke up to an empty table and
// never reprocessed anything. A Cloudflare deployment accepted turns, stored the
// raw source, and silently produced no derived memory at all.
//
// Why the claim is a guarded UPDATE and not SELECT ... FOR UPDATE
//
// The reference claims inside a transaction:
//
//	BEGIN
//	  UPDATE memory_reprocessing_jobs j JOIN memory_source_revisions s ...   (sweep)
//	  SELECT j.* ... ORDER BY j.created_at, j.id LIMIT 1 FOR UPDATE
//	  UPDATE memory_reprocessing_jobs SET status='leased', ... WHERE id = ?
//	COMMIT
//
// D1 has no row locks that survive a bridge call, and FOR UPDATE is not SQLite
// syntax at all, so the lock cannot be reproduced. The guarantee it provided —
// "the row I selected is still the row I am about to lease, and nobody else has
// leased it in between" — is instead carried by the WHERE clause of the lease
// UPDATE itself:
//
//	WHERE id = ? AND status = ? AND lease_owner IS ?
//
// `status` and `lease_owner` are the two values the SELECT read. SQLite evaluates
// a single UPDATE under a write lock, so re-asserting them turns the lease into a
// compare-and-swap: if another worker claimed the job in the window between the
// SELECT and the UPDATE, this worker's UPDATE matches zero rows and it must not
// believe it owns the job. `lease_owner IS ?` is what closes the narrow window
// the status check alone leaves open — two workers recovering the SAME expired
// lease both read status='leased', so status alone would let the second overwrite
// the first worker's fresh lease; re-asserting the owner it read does not.
//
// That is why d1ReprocessingJobClaimRounds exists. The reference does not need a
// retry loop because a losing worker blocks on the row lock and then re-reads a
// committed state. Here a losing worker re-runs the whole select-and-lease pair,
// which is the same thing without the blocking. The bound exists so a pathological
// queue cannot spin; exhausting it reports ErrNotFound, which the worker already
// treats as "nothing to do this wake" (memory_reprocessing_worker.go), so the
// worst case is one idle wake, never a double-lease.
//
// Why the reopen is a Batch with a readback, not a transaction with counts
//
// ReopenMemoryReprocessingJob resets two rows — the revision's committed
// admission snapshot and the job's lease state — and the reference asserts
// RowsAffected() == 1 on each before committing, so a lost race rolls the whole
// thing back. D1Conn.Batch is the transaction boundary but reports only an error,
// not per-statement affected counts, and splitting the two writes into separate
// Exec calls to recover the counts would reintroduce exactly the partial state the
// reference forbids: a reset admission snapshot with no re-queued job is a turn
// that can never be reprocessed again, and nothing reports it.
//
// The batch keeps the writes atomic and the reference's two assertions are
// reproduced as a post-commit readback. That is not the same guarantee and the
// difference is stated rather than hidden: the pre-checks below still refuse with
// the reference's error values BEFORE anything is written, and the two post-write
// races are the only cases where D1 commits and then reports. Their blast
// radius is deliberately the benign direction — if the revision goes inactive
// between the read and the batch, the guarded source UPDATE matches nothing, the
// job is re-queued, and the very next claim sweep marks that job stale_rejected.
// The dangerous direction (snapshot reset, job not re-queued) is the one the batch
// makes unreachable.
//
// The reopener's lease check is a Go comparison, not a text comparison
//
// The reference reads lease_until into sql.NullTime and compares with
// `leaseUntil.Time.After(now)` in Go. That is reproduced literally: the TEXT
// column is parsed by parseD1Time and compared as a time.Time. The SQL-side
// comparisons in the claim and wake statements do compare TEXT against TEXT, and
// that is safe there only because both sides are rendered by d1TimeValue. Doing
// it in Go costs nothing here and is immune to the rendering edge documented in
// d1_session_route_capabilities.go, where RFC3339 trims trailing fractional
// zeros and a raw string order can disagree with the clock.
//
// Why the enqueue is deliberately NOT source-fenced
//
// Parity rule: a write that accepts source-derived projections must refuse an
// inactive revision. The enqueue is the exception, and it is an exception in the
// reference too: EnqueueMemoryReprocessingJob checks nothing but its own identity
// columns, and a job for a superseded revision is accepted and later swept to
// stale_rejected by the claim. Adding a fence here would drop a job the reference
// accepts, and the caller (complete_turn_source_revision.go) treats a missing job
// as "nothing to reprocess" rather than as an error — so a fence would turn a
// rolled-back turn into a silently un-reprocessed one. The fence for this table
// is the claim-time lifecycle join plus the sweep, and both are ported.
//
// Text comparison
//
// idempotency_key, chat_session_id, source_revision, and the version triple are
// SQLite BINARY equality where the MariaDB columns are utf8mb4_unicode_ci. The
// conflict resolution compares the values it SCANNED against the caller's Go
// values, which is the same Go-side comparison the reference performs, so the
// divergence is confined to the WHERE that finds the row and is consistent with
// every other D1 reader of these columns.
//
// A known, deliberate divergence: rows affected on a no-op update.
//
// MariaDB reports rows whose stored bytes actually changed because no DSN here
// sets clientFoundRows, so re-writing a job with its own values returns 0. Every
// affected-row check below is a CORRECTNESS gate (did the claim land? did the
// reopen lose its row?), never a "did anything change?" report, and SQLite
// reports 1 for the matched row — which is the answer those gates want. The
// reference's own tests depend on this: its reopen and lease-fence cases mock
// affected=1.

var _ MemoryReprocessingJobStore = (*d1Store)(nil)
var _ MemoryReprocessingJobReopener = (*d1Store)(nil)
var _ MemoryReprocessingWakeScheduleStore = (*d1Store)(nil)

// d1ReprocessingIdempotencyConflict is the reference's collision error. Both the
// enqueue and the reopener raise it, and both must stay the same message: the
// HTTP layer surfaces it verbatim in its persistence diagnostics
// (group_turn_complete.go).
var d1ReprocessingIdempotencyConflict = errors.New("memory reprocessing idempotency conflict")

// d1ReprocessingReopenLost reproduces the reference's assertion that the reopen
// UPDATE still matched its job row.
var d1ReprocessingReopenLost = errors.New("memory reprocessing job reopen lost")

// d1ReprocessingJobClaimRounds bounds how many select-and-lease rounds one claim
// call will attempt before reporting ErrNotFound. One round is the common case
// (the mutex already serializes workers inside a Container); the extra rounds
// exist so a worker that lost the compare-and-swap re-reads and leases the NEXT
// job the way the reference's blocked worker would, instead of dropping a tick.
const d1ReprocessingJobClaimRounds = 4

// d1ReprocessingJobColumns is the MariaDB projection of a claimed job, in the
// same order, so the statement and the scan cannot drift apart.
const d1ReprocessingJobColumns = `j.id, j.contract_version, j.idempotency_key, j.chat_session_id,
		       j.source_revision, j.source_contract, j.derivation_version,
		       j.extractor_version, j.index_version, j.status, j.attempts,
		       j.retry_after, j.lease_owner, j.lease_until, j.last_error,
		       j.created_at, j.updated_at`

// d1ReprocessingJobScanner is the single-row shape both transports satisfy.
type d1ReprocessingJobScanner interface{ Scan(dest ...any) error }

// d1ReprocessingMigrationLockedPredicate is the MariaDB session-migration fence,
// repeated in the claim and the wake schedule. A session that is mid-migration
// must not have its jobs leased, because the migration is about to delete them
// (session_migration_manifest.go lists memory_reprocessing_jobs as
// delete-after-verified); leasing work that is about to be deleted would let a
// worker spend a full admission pass on rows that are then removed.
//
// `locked = 1` is the SQLite spelling of MariaDB's `locked = TRUE`: the canonical
// D1 schema stores the flag as INTEGER, and the boolean literal is not portable
// here. The same predicate appears in d1_session_route_capabilities.go.
const d1ReprocessingMigrationLockedPredicate = `
		  AND NOT EXISTS (
		    SELECT 1
		    FROM session_migration_locks migration_lock
		    WHERE migration_lock.source_session_id = j.chat_session_id
		      AND migration_lock.locked = 1
		      AND migration_lock.unlocked_at IS NULL
		  )`

// d1ReprocessingStaleSweep retires every claimable job whose source revision is
// no longer active. It is the MariaDB multi-table UPDATE, written as a correlated
// subquery because SQLite has no UPDATE ... JOIN.
//
// The translation is exact: the reference joins on s.source_revision only (NOT on
// chat_session_id), and source_revision is the UNIQUE key of
// memory_source_revisions, so "exists an inactive revision with this key" selects
// precisely the rows the join would have matched. Widening the join to include
// chat_session_id would silently stop retiring a job whose session id and
// revision have drifted apart.
//
// This runs as its own statement rather than inside the claim batch, which is the
// one place D1 and MariaDB genuinely differ: the reference rolls the sweep back
// when the claim finds nothing. Committing it anyway is safe because the sweep
// only writes the TERMINAL status stale_rejected to rows the claim was never going
// to select (the claim's own join already requires lifecycle_state = 'active'),
// and it writes the same terminal status InvalidateSourceRevisions writes for the
// same rows. A job cannot be left claimable by a sweep that committed.
const d1ReprocessingStaleSweep = `
		UPDATE memory_reprocessing_jobs
		SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL,
		    last_error = 'source_revision_not_active', updated_at = ?
		WHERE status IN ('pending', 'leased', 'retryable')
		  AND source_revision IN (
		    SELECT source_revision
		    FROM memory_source_revisions
		    WHERE lifecycle_state <> 'active'
		  )`

// d1ReprocessingJobLease is the compare-and-swap that replaces FOR UPDATE.
//
// The SET clause is the reference statement unchanged. The WHERE clause is the
// reference's `WHERE id = ?` plus the two values the preceding SELECT observed,
// and it is the entire exclusivity argument on this provider: SQLite runs one
// UPDATE under a write lock, so a worker whose candidate was taken while it was
// reading matches nothing and learns it does not hold the lease.
//
// `attempts = attempts + 1` is kept as the column expression rather than
// `attempts = ?`. Binding the Go-side `job.Attempts++` value would be a lost
// update if a concurrent claim had bumped the counter between the SELECT and the
// UPDATE, and the returned job would then disagree with the stored row.
const d1ReprocessingJobLease = `
		UPDATE memory_reprocessing_jobs
		SET status = 'leased', attempts = attempts + 1, lease_owner = ?,
		    lease_until = ?, updated_at = ?
		WHERE id = ?
		  AND status = ?
		  AND lease_owner IS ?`

// d1ReprocessingJobFinish is the completion/failure write. It carries the same
// two-value compare-and-swap as the lease: without it, a worker whose lease was
// stolen between the read and the write would stamp 'completed' over a job another
// worker is running right now, and the loser's own completion would then find
// status='completed' and report ErrLeaseExpired with no way to tell the two apart.
const d1ReprocessingJobFinish = `
		UPDATE memory_reprocessing_jobs
		SET status = ?, retry_after = ?, lease_owner = NULL, lease_until = NULL,
		    last_error = ?, updated_at = ?
		WHERE id = ?
		  AND status = 'leased'
		  AND lease_owner IS ?`

// d1ReprocessingReopenSourceReset clears ONLY the committed admission snapshot.
// The raw user and assistant content, the content hashes, and the critic input
// snapshot are deliberately untouched: the reopener is an operator recovery tool
// for re-running the DERIVATION, and destroying the observed source would make
// the retry non-reproducible.
//
// The `lifecycle_state = 'active'` guard is the source-revision fence. It stays
// in the statement (rather than only in the Go pre-check) so a revision that goes
// inactive between the pre-check and the batch makes this a no-op instead of
// resetting the admission of a revision the user has already rolled back.
const d1ReprocessingReopenSourceReset = `
		UPDATE memory_source_revisions
		SET derived_admission_state = 'pending',
		    derived_admission_version = '',
		    derived_extractor_version = '',
		    derived_index_version = '',
		    derived_result_hash = NULL,
		    derived_result_json = NULL,
		    derived_admitted_at = NULL,
		    updated_at = ?
		WHERE chat_session_id = ?
		  AND source_revision = ?
		  AND lifecycle_state = 'active'`

// d1ReprocessingReopenJobReset returns the job to the queue with a clean lease.
// attempts is reset to 0 because the operator asked for the work to be redone
// from scratch; a permanent failure that is reopened must not be immediately
// re-failed by the worker's attempt ceiling on the first pass.
const d1ReprocessingReopenJobReset = `
		UPDATE memory_reprocessing_jobs
		SET status = 'pending',
		    attempts = 0,
		    retry_after = NULL,
		    lease_owner = NULL,
		    lease_until = NULL,
		    last_error = NULL,
		    updated_at = ?
		WHERE id = ?
		  AND idempotency_key = ?`

// d1ReprocessingNow is nonZeroTime: a zero instant becomes the current UTC time
// so a caller that does not care about the clock still writes a usable value.
func d1ReprocessingNow(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t.UTC()
}

// d1ReprocessingTime dereferences a nullable timestamp the way the reference's
// timeFromNull does: absent stays the zero time rather than becoming "now".
func d1ReprocessingTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// d1ReprocessingNullableText binds a scanned nullable string for a later
// comparison. It preserves the distinction between NULL and the empty string,
// which d1NullableString would erase: the lease compare-and-swap must be able to
// assert "this row had no owner" as NULL rather than matching a row that
// genuinely stores an empty owner.
func d1ReprocessingNullableText(v *string) any {
	if v == nil {
		return nil
	}
	return *v
}

// d1ScanReprocessingJob mirrors the reference scan. retry_after, lease_owner,
// lease_until, and last_error are nullable, and the lease is decided on them, so
// they are read through pointers: the returned job collapses them to the zero
// value exactly as the reference's NullTime.Time / NullString.String do, while
// the raw lease_owner pointer is returned alongside so the caller can re-assert
// precisely what it read in the compare-and-swap.
func d1ScanReprocessingJob(scanner d1ReprocessingJobScanner) (*MemoryReprocessingJob, *string, error) {
	job := &MemoryReprocessingJob{}
	var retryAfter, leaseUntil *time.Time
	var leaseOwner, lastError *string
	if err := scanner.Scan(&job.ID, &job.ContractVersion, &job.IdempotencyKey,
		&job.ChatSessionID, &job.SourceRevision, &job.SourceContract,
		&job.DerivationVersion, &job.ExtractorVersion, &job.IndexVersion,
		&job.Status, &job.Attempts, &retryAfter, &leaseOwner, &leaseUntil,
		&lastError, &job.CreatedAt, &job.UpdatedAt); err != nil {
		return nil, nil, err
	}
	job.RetryAfter = d1ReprocessingTime(retryAfter)
	job.LeaseOwner = d1DerefString(leaseOwner)
	job.LeaseUntil = d1ReprocessingTime(leaseUntil)
	job.LastError = d1DerefString(lastError)
	return job, leaseOwner, nil
}

// EnqueueMemoryReprocessingJob records one unit of reprocessing work.
//
// It reports whether a NEW row was written. An exact replay — same
// idempotency_key AND same session, revision, and version triple — reports false
// and writes nothing, which is what makes a retried complete-turn safe. A replay
// that collides on the key but carries a different identity is a conflict error,
// never a silent overwrite: the key is the caller's claim that "this exact
// derivation is the one to run", and honouring a mismatched payload under it would
// run a derivation the caller never asked for.
//
// The collision is resolved BEFORE the insert rather than after a caught error
// 1062, because D1Conn.Batch and the bridge's Exec report only success or
// failure and cannot distinguish a suppressed write from a written one. The
// observable contract is identical — the same statement, the same two outcomes —
// and it is safe to reorder because s.memoryDerivationWriteMu is held across the
// check and the insert, so nothing in this process can create the key in between.
// Across Containers the race surfaces as the unique-constraint error from the
// insert rather than as a silent overwrite, which is the safe direction.
func (s *d1Store) EnqueueMemoryReprocessingJob(ctx context.Context, job *MemoryReprocessingJob) (bool, error) {
	if job == nil || strings.TrimSpace(job.IdempotencyKey) == "" ||
		strings.TrimSpace(job.ChatSessionID) == "" || strings.TrimSpace(job.SourceRevision) == "" {
		return false, fmt.Errorf("invalid memory reprocessing job")
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	// The defaults are applied before the insert AND before the collision check,
	// exactly as the reference does, so a replay is compared against the values
	// that would have been stored rather than against the raw request.
	if strings.TrimSpace(job.ContractVersion) == "" {
		job.ContractVersion = MemoryReprocessingJobContract
	}
	if strings.TrimSpace(job.Status) == "" {
		job.Status = "pending"
	}

	var sid, revision, sourceContract, derivationVersion, extractorVersion, indexVersion string
	err := s.conn.QueryRow(ctx, `
		SELECT chat_session_id, source_revision, source_contract,
		       derivation_version, extractor_version, index_version
		FROM memory_reprocessing_jobs
		WHERE idempotency_key = ?`, job.IdempotencyKey).Scan(&sid, &revision,
		&sourceContract, &derivationVersion, &extractorVersion, &indexVersion)
	if err == nil {
		if sid != job.ChatSessionID || revision != job.SourceRevision ||
			sourceContract != job.SourceContract ||
			derivationVersion != job.DerivationVersion ||
			extractorVersion != job.ExtractorVersion ||
			indexVersion != job.IndexVersion {
			return false, d1ReprocessingIdempotencyConflict
		}
		return false, nil
	}
	if !errors.Is(err, errD1NoRows) {
		return false, err
	}

	// retry_after, lease_owner, lease_until, and last_error go through the
	// nullable bindings so an unset lease is stored as NULL. That is not
	// tidiness: the claim predicate reads `retry_after IS NULL` as "claimable
	// now", and an empty string there would compare as greater than every
	// timestamp and park the job until a lease expired.
	_, err = s.conn.Exec(ctx, `
		INSERT INTO memory_reprocessing_jobs (
			contract_version, idempotency_key, chat_session_id, source_revision,
			source_contract, derivation_version, extractor_version, index_version,
			status, attempts, retry_after, lease_owner, lease_until, last_error,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.ContractVersion, job.IdempotencyKey, job.ChatSessionID,
		job.SourceRevision, job.SourceContract, job.DerivationVersion,
		job.ExtractorVersion, job.IndexVersion, job.Status, job.Attempts,
		d1NullableTime(job.RetryAfter), d1NullableString(job.LeaseOwner),
		d1NullableTime(job.LeaseUntil), d1NullableString(job.LastError),
		d1TimeValue(job.CreatedAt), d1TimeValue(job.UpdatedAt))
	if err != nil {
		return false, err
	}
	return true, nil
}

// ClaimMemoryReprocessingJob leases at most one job for leaseOwner.
//
// It returns ErrNotFound when nothing is claimable, which the worker treats as an
// empty queue rather than a failure. A job is claimable when its source revision
// is still active, its session is not mid-migration, and either it is
// pending/retryable with no retry_after or a retry_after strictly in the past, or
// it is leased with a lease_until strictly in the past — the abandoned-worker
// recovery path. The two comparison arguments are rendered by d1TimeValue, the
// same function that writes the columns, so the text comparison is like-for-like.
func (s *d1Store) ClaimMemoryReprocessingJob(ctx context.Context, leaseOwner string, now time.Time, leaseDuration time.Duration) (*MemoryReprocessingJob, error) {
	if strings.TrimSpace(leaseOwner) == "" || leaseDuration <= 0 {
		return nil, fmt.Errorf("invalid memory reprocessing lease")
	}
	// The mutex is held for the whole claim. Inside one Container it serializes
	// workers, and it is also what makes the select-then-compare-and-swap window
	// safe: no other goroutine in this process can claim between the two
	// statements. Across Containers the compare-and-swap is what holds.
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()
	now = d1ReprocessingNow(now)

	if _, err := s.conn.Exec(ctx, d1ReprocessingStaleSweep, d1TimeValue(now)); err != nil {
		return nil, err
	}

	for round := 0; round < d1ReprocessingJobClaimRounds; round++ {
		job, observedOwner, err := d1ReprocessingJobForLease(ctx, s, now)
		if err != nil {
			return nil, err
		}
		observedStatus := job.Status
		leaseUntil := now.Add(leaseDuration)

		affected, err := s.conn.Exec(ctx, d1ReprocessingJobLease,
			leaseOwner, d1TimeValue(leaseUntil), d1TimeValue(now),
			job.ID, observedStatus, d1ReprocessingNullableText(observedOwner))
		if err != nil {
			return nil, err
		}
		if affected == 0 {
			// Another worker leased this row first. Re-read rather than report
			// a claim we do not hold: the job is still there, just not ours.
			continue
		}
		// The lease landed, so the row now holds exactly these values. The
		// returned job mirrors the stored row, including the incremented
		// attempt count, because the worker reports result.Attempt to the HUD
		// and uses it against the failure ceiling.
		job.LeaseOwner = leaseOwner
		job.LeaseUntil = leaseUntil
		job.Status = "leased"
		job.Attempts++
		return job, nil
	}
	return nil, ErrNotFound
}

// d1ReprocessingJobForLease is the MariaDB selectMemoryReprocessingJobForLease
// statement with FOR UPDATE removed and nothing else changed.
//
// retry_after is treated as an EXCLUSIVE wake cursor: a job failed during this
// wake has retry_after == now, which is not "< now", so this same drain cannot
// immediately re-lease it. Without that strictness a permanently failing job
// would be re-claimed the instant Fail returned, and the worker would spin on it
// instead of backing off to the retry delay the failure recorded.
//
// ORDER BY j.created_at, j.id is the FIFO the reference uses, and the id
// tiebreak is what makes two jobs enqueued in the same instant claim in a
// deterministic order across providers. created_at is TEXT, so it orders
// lexicographically, which for the d1TimeValue rendering is chronological.
func d1ReprocessingJobForLease(ctx context.Context, s *d1Store, now time.Time) (*MemoryReprocessingJob, *string, error) {
	nowText := d1TimeValue(now)
	row := s.conn.QueryRow(ctx, `
		SELECT `+d1ReprocessingJobColumns+`
		FROM memory_reprocessing_jobs j
		JOIN memory_source_revisions s ON s.source_revision = j.source_revision
		WHERE s.lifecycle_state = 'active'`+d1ReprocessingMigrationLockedPredicate+`
		  AND (
		    (j.status IN ('pending', 'retryable') AND (j.retry_after IS NULL OR j.retry_after < ?))
		    OR (j.status = 'leased' AND j.lease_until < ?)
		  )
		ORDER BY j.created_at, j.id
		LIMIT 1`, nowText, nowText)
	job, leaseOwner, err := d1ScanReprocessingJob(row)
	if errors.Is(err, errD1NoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	return job, leaseOwner, nil
}

// NextMemoryReprocessingWakeAt reports when the worker must next wake, so a
// restarted Container restores its one-shot timer instead of polling.
//
// The MIN is over a CASE that picks lease_until for a leased job and retry_after
// for everything else, because those are the two columns that decide when a row
// becomes claimable; taking the minimum of the wrong column would either wake the
// worker while every job is still leased (busy loop) or sleep through a retryable
// job whose retry_after has already passed. Rows with neither column set are
// excluded by the trailing predicate rather than by NULL handling in Go, which
// keeps this a single statement exactly as the reference is.
//
// An empty queue is ErrNotFound, and the worker reads that as "no schedule" and
// keeps its existing timer instead of treating it as a failure.
func (s *d1Store) NextMemoryReprocessingWakeAt(ctx context.Context) (time.Time, error) {
	var next *time.Time
	if err := s.conn.QueryRow(ctx, `
		SELECT MIN(CASE
		         WHEN j.status = 'leased' THEN j.lease_until
		         ELSE j.retry_after
		       END)
		FROM memory_reprocessing_jobs j
		JOIN memory_source_revisions s ON s.source_revision = j.source_revision
		WHERE s.lifecycle_state = 'active'`+d1ReprocessingMigrationLockedPredicate+`
		  AND (
		    (j.status IN ('pending', 'retryable') AND j.retry_after IS NOT NULL)
		    OR (j.status = 'leased' AND j.lease_until IS NOT NULL)
		  )`).Scan(&next); err != nil {
		return time.Time{}, err
	}
	// MIN over an empty set is NULL, and MIN never returns the zero time from a
	// stored value, so both collapse to "no schedule" exactly as the reference's
	// `!next.Valid || next.Time.IsZero()` check does.
	if next == nil || next.IsZero() {
		return time.Time{}, ErrNotFound
	}
	return *next, nil
}

// CompleteMemoryReprocessingJob releases a claimed job as successfully finished.
func (s *d1Store) CompleteMemoryReprocessingJob(ctx context.Context, jobID int64, leaseOwner string, now time.Time) error {
	return s.d1FinishMemoryReprocessingJob(ctx, jobID, leaseOwner, now, time.Time{}, false, false, "")
}

// FailMemoryReprocessingJob releases a claimed job as failed. A permanent failure
// parks the job; otherwise it goes back to retryable with the caller's retryAfter
// as its wake cursor.
func (s *d1Store) FailMemoryReprocessingJob(ctx context.Context, jobID int64, leaseOwner string, now, retryAfter time.Time, permanent bool, failure string) error {
	return s.d1FinishMemoryReprocessingJob(ctx, jobID, leaseOwner, now, retryAfter, true, permanent, failure)
}

// d1FinishMemoryReprocessingJob is the shared complete/fail path.
//
// The lease is verified before anything is written, and the failure modes are
// distinct on purpose:
//
//   - the row is gone, or is no longer 'leased'  -> ErrLeaseExpired
//   - a different worker owns the lease           -> ErrLeaseExpired
//   - the lease has already expired                -> ErrLeaseExpired
//   - the lease is ours but the revision died     -> the job is marked
//     stale_rejected, the write is COMMITTED, and ErrSourceRevisionStale is
//     returned
//
// The fourth case commits even though it returns an error, in the reference and
// here. That is the point of it: a worker that finished a job whose revision was
// rolled back must not leave the job leased, because the lease would block
// re-queueing it and the stale_rejected status is the durable record that the work
// was correctly abandoned. The caller (memory_reprocessing_worker.go) reads
// ErrSourceRevisionStale as "stale_rejected, not an error" and records it on the
// turn.
//
// A leased row with a NULL lease_until is refused here as ErrLeaseExpired. The
// reference scans that column into a non-nullable time.Time and would fail the
// scan; refusing is the same outcome with a defined sentinel, and it is the safe
// reading because only the claim writes status='leased' and it always writes the
// lease with it.
func (s *d1Store) d1FinishMemoryReprocessingJob(ctx context.Context, jobID int64, leaseOwner string, now, retryAfter time.Time, failed, permanent bool, failure string) error {
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()
	now = d1ReprocessingNow(now)

	var leaseOwnerValue *string
	var leaseUntil *time.Time
	var sourceState string
	err := s.conn.QueryRow(ctx, `
		SELECT j.lease_owner, j.lease_until, s.lifecycle_state
		FROM memory_reprocessing_jobs j
		JOIN memory_source_revisions s ON s.source_revision = j.source_revision
		WHERE j.id = ? AND j.status = 'leased'`, jobID).Scan(
		&leaseOwnerValue, &leaseUntil, &sourceState)
	if errors.Is(err, errD1NoRows) {
		return ErrLeaseExpired
	}
	if err != nil {
		return err
	}
	// The owner comparison is an exact string match, not a trimmed one: lease
	// owners are worker identities, and trimming here would let " worker-1"
	// finish "worker-1"'s work. An absent owner is "" and therefore matches
	// only a caller that also passed "".
	if d1DerefString(leaseOwnerValue) != leaseOwner ||
		d1ReprocessingTime(leaseUntil).Before(now) {
		return ErrLeaseExpired
	}

	if sourceState != "active" {
		if _, err := s.conn.Exec(ctx, `
			UPDATE memory_reprocessing_jobs
			SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL,
			    last_error = 'source_revision_not_active', updated_at = ?
			WHERE id = ?`, d1TimeValue(now), jobID); err != nil {
			return err
		}
		return ErrSourceRevisionStale
	}

	status := "completed"
	if failed {
		status = "retryable"
		if permanent {
			status = "permanent"
		}
	}
	// retry_after is the CALLER's cursor and is written unconditionally,
	// including for a permanent failure — that is what the reference statement
	// binds, and what the reference's own sqlmock assertion for a permanent
	// failure expects. What keeps a terminal row out of
	// NextMemoryReprocessingWakeAt is that statement's status predicate, not an
	// empty column. A completion passes the zero time, which d1NullableTime
	// renders as NULL; that is how success clears the cursor. Forcing NULL for a
	// permanent failure here instead would be a silent divergence from the
	// reference for no behavioural gain.
	affected, err := s.conn.Exec(ctx, d1ReprocessingJobFinish,
		status, d1NullableTime(retryAfter), d1NullableString(failure),
		d1TimeValue(now), jobID, d1ReprocessingNullableText(leaseOwnerValue))
	if err != nil {
		return err
	}
	if affected != 1 {
		// The row stopped matching between the read and the write: another
		// worker finished it, or the lease was stolen. Reporting success would
		// tell the caller its result was recorded when it was not.
		return ErrLeaseExpired
	}
	return nil
}

// ReopenMemoryReprocessingJob re-queues an abandoned job for an operator-driven
// retry and resets the active revision's committed admission snapshot so the
// admission actually re-runs instead of short-circuiting on a snapshot that
// claims the derivation already happened.
//
// Every refusal happens before any write and returns the reference's value:
//
//   - unknown idempotency key, or one whose job has no source revision to join
//   - ErrNotFound
//   - the key resolves to a different session or revision -> idempotency conflict
//   - the revision is not active                    -> ErrSourceRevisionStale
//   - a live worker still holds the lease           -> ErrMemoryReprocessingLeased
//
// The last one is the interesting refusal. Reopening under an active lease would
// reset attempts to 0 and clear the lease out from under a worker that is
// mid-admission; that worker would then finish, find its lease gone, and its
// derived writes would race a fresh re-run of the same turn. So the check is
// kept even though it costs an operator the retry they asked for — they are told
// the job is running, which is the truth.
//
// The two resets are one atomic batch. See the file header for why the reference's
// per-statement affected-row assertions are reproduced by a post-commit readback
// rather than by splitting the batch.
func (s *d1Store) ReopenMemoryReprocessingJob(ctx context.Context, idempotencyKey, chatSessionID, sourceRevision string, now time.Time) (bool, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	chatSessionID = strings.TrimSpace(chatSessionID)
	sourceRevision = strings.TrimSpace(sourceRevision)
	if idempotencyKey == "" || chatSessionID == "" || sourceRevision == "" {
		return false, fmt.Errorf("invalid memory reprocessing reopen request")
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	var (
		jobID       int64
		sid         string
		revision    string
		status      string
		leaseUntil  *time.Time
		sourceState string
	)
	// The join is on BOTH chat_session_id and source_revision here, unlike the
	// claim's revision-only join. That is the reference's asymmetry: the reopener
	// must refuse a key whose row has drifted away from the session the operator
	// named, and a revision-only join would silently reopen a job belonging to
	// another session. A job with no matching revision produces no row, which is
	// ErrNotFound — the route falls back to enqueueing instead.
	err := s.conn.QueryRow(ctx, `
		SELECT j.id, j.chat_session_id, j.source_revision,
		       j.status, j.lease_until, s.lifecycle_state
		FROM memory_reprocessing_jobs j
		JOIN memory_source_revisions s
		  ON s.chat_session_id = j.chat_session_id
		 AND s.source_revision = j.source_revision
		WHERE j.idempotency_key = ?`, idempotencyKey).Scan(
		&jobID, &sid, &revision, &status, &leaseUntil, &sourceState)
	if errors.Is(err, errD1NoRows) {
		return false, ErrNotFound
	}
	if err != nil {
		return false, err
	}
	if sid != chatSessionID || revision != sourceRevision {
		return false, d1ReprocessingIdempotencyConflict
	}
	if sourceState != "active" {
		return false, ErrSourceRevisionStale
	}
	now = d1ReprocessingNow(now)
	// A Go comparison, not a text one: the reference compares a parsed DATETIME
	// with a Go time, and parsing the TEXT here keeps that exact semantics. The
	// lease owner is not re-checked, only the expiry, because a stale owner with a
	// future lease_until still means somebody is running the job right now.
	if status == "leased" && leaseUntil != nil && leaseUntil.After(now) {
		return false, ErrMemoryReprocessingLeased
	}

	nowText := d1TimeValue(now)
	if err := s.conn.Batch(ctx,
		D1Statement{SQL: d1ReprocessingReopenSourceReset, Args: []any{nowText, sid, revision}},
		D1Statement{SQL: d1ReprocessingReopenJobReset, Args: []any{nowText, jobID, idempotencyKey}},
	); err != nil {
		return false, err
	}

	// The reference asserts affected == 1 on both statements before committing.
	// A batch cannot report per-statement counts, so the assertions are made
	// against the committed state instead. Both are races with a concurrent
	// operator or a session migration, and both are checked in the order that
	// keeps the source-revision guarantee first.
	var lifecycle string
	lifecycleErr := s.conn.QueryRow(ctx, `
		SELECT lifecycle_state FROM memory_source_revisions
		WHERE chat_session_id = ? AND source_revision = ?`, sid, revision).Scan(&lifecycle)
	if errors.Is(lifecycleErr, errD1NoRows) || (lifecycleErr == nil && lifecycle != "active") {
		return false, ErrSourceRevisionStale
	}
	if lifecycleErr != nil {
		return false, lifecycleErr
	}
	var reopenedID int64
	if err := s.conn.QueryRow(ctx, `
		SELECT id FROM memory_reprocessing_jobs WHERE id = ? AND idempotency_key = ?`,
		jobID, idempotencyKey).Scan(&reopenedID); err != nil {
		if errors.Is(err, errD1NoRows) {
			return false, d1ReprocessingReopenLost
		}
		return false, err
	}
	return true, nil
}
