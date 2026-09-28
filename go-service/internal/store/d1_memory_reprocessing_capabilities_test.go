package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// D1 memory reprocessing tests.
//
// Every test runs against the real SQLite engine through the same D1 transport
// the Worker uses, with foreign keys enabled exactly as D1 enforces them, so
// these assert the statements D1 actually executes rather than a mock's idea of
// them.
//
// The seeds are chosen around the failure modes that are SILENT rather than
// loud. A claim that hands the same job to two workers produces two concurrent
// admissions of one turn, and the second write is not an error — it is a
// duplicate memory the user then sees twice. A reopener that resets the
// admission snapshot without re-queueing the job leaves a turn that can never be
// derived again, and again nothing reports it. A wake schedule that reports the
// wrong column produces a worker that either busy-loops while every job is still
// leased or sleeps through a retryable job. So these tests count rows and read
// stored columns; they do not settle for "the call returned nil".

// ---------------------------------------------------------------------------
// seeds
// ---------------------------------------------------------------------------

// d1ReprocessingScene is one session with a live revision and a superseded one.
// The superseded revision is what the sweep, the claim join, and the reopener
// fence have to see, so it is seeded everywhere rather than faked per test.
type d1ReprocessingScene struct {
	SessionID  string
	LiveRev    string
	DeadRev    string
	LockedLive string
}

func d1ReprocessingSeedScene(t *testing.T, conn *sqliteD1Conn, sessionID string) d1ReprocessingScene {
	t.Helper()
	scene := d1ReprocessingScene{
		SessionID:  sessionID,
		LiveRev:    sessionID + "-rev-live",
		DeadRev:    sessionID + "-rev-superseded",
		LockedLive: sessionID + "-rev-locked",
	}
	// Two live revisions need two logical turns: the canonical schema allows only
	// one ACTIVE revision per (session, logical turn).
	d1SeedIdentityRevision(t, conn, sessionID, scene.LiveRev, "turn-1", "active")
	d1SeedIdentityRevision(t, conn, sessionID, scene.DeadRev, "turn-2", "superseded")
	d1SeedIdentityRevision(t, conn, sessionID, scene.LockedLive, "turn-3", "active")
	return scene
}

// d1ReprocessingJobSeed describes one queue row. The zero value of every field
// is a row the enqueue would produce, so a test states only what it is about.
// leaseOwner/retryAfter/leaseUntil are written as NULL when empty, which is what
// makes a pending job claimable and a leased job recoverable.
type d1ReprocessingJobSeed struct {
	idempotencyKey string
	sessionID      string
	sourceRevision string
	sourceContract string
	derivation     string
	extractor      string
	index          string
	status         string
	attempts       int
	retryAfter     string
	leaseOwner     string
	leaseUntil     string
	lastError      string
	createdAt      string
	updatedAt      string
}

func d1ReprocessingJobDefaults(seed d1ReprocessingJobSeed) d1ReprocessingJobSeed {
	if seed.sourceContract == "" {
		seed.sourceContract = "source_acceptance_observation.v1"
	}
	if seed.derivation == "" {
		seed.derivation = PreciseMemoryUnitContract
	}
	if seed.extractor == "" {
		seed.extractor = "complete_turn.configured_critic_extract"
	}
	if seed.index == "" {
		seed.index = "not_materialized"
	}
	if seed.status == "" {
		seed.status = "pending"
	}
	if seed.createdAt == "" {
		seed.createdAt = "2026-07-28T02:00:00Z"
	}
	if seed.updatedAt == "" {
		seed.updatedAt = seed.createdAt
	}
	return seed
}

// d1ReprocessingNullableText keeps a seeded optional column NULL when it is
// absent, which is the state the claim predicate reads for "no lease".
func d1ReprocessingSeedText(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// d1ReprocessingSeedJob inserts one queue row directly, bypassing the enqueue, so
// a test can place a job in a state the enqueue cannot produce (leased, retryable,
// permanent, completed).
func d1ReprocessingSeedJob(t *testing.T, conn *sqliteD1Conn, seed d1ReprocessingJobSeed) int64 {
	t.Helper()
	seed = d1ReprocessingJobDefaults(seed)
	var id int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO memory_reprocessing_jobs (
			contract_version, idempotency_key, chat_session_id, source_revision,
			source_contract, derivation_version, extractor_version, index_version,
			status, attempts, retry_after, lease_owner, lease_until, last_error,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`,
		MemoryReprocessingJobContract, seed.idempotencyKey, seed.sessionID,
		seed.sourceRevision, seed.sourceContract, seed.derivation, seed.extractor,
		seed.index, seed.status, seed.attempts,
		d1ReprocessingSeedText(seed.retryAfter), d1ReprocessingSeedText(seed.leaseOwner),
		d1ReprocessingSeedText(seed.leaseUntil), d1ReprocessingSeedText(seed.lastError),
		seed.createdAt, seed.updatedAt).Scan(&id); err != nil {
		t.Fatalf("seed reprocessing job %s: %v", seed.idempotencyKey, err)
	}
	return id
}

// d1ReprocessingJobRow is the stored queue state a test asserts against.
type d1ReprocessingJobRow struct {
	Status     string
	Attempts   int
	LeaseOwner string
	LeaseUntil string
	RetryAfter string
	LastError  string
}

func d1ReprocessingJobState(t *testing.T, conn *sqliteD1Conn, id int64) d1ReprocessingJobRow {
	t.Helper()
	var owner, leaseUntil, retryAfter, lastError *string
	var row d1ReprocessingJobRow
	if err := conn.QueryRow(context.Background(), `
		SELECT status, attempts, lease_owner, lease_until, retry_after, last_error
		FROM memory_reprocessing_jobs WHERE id = ?`, id).Scan(
		&row.Status, &row.Attempts, &owner, &leaseUntil, &retryAfter, &lastError); err != nil {
		t.Fatalf("read reprocessing job %d: %v", id, err)
	}
	row.LeaseOwner = d1DerefString(owner)
	row.LeaseUntil = d1DerefString(leaseUntil)
	row.RetryAfter = d1DerefString(retryAfter)
	row.LastError = d1DerefString(lastError)
	return row
}

// d1ReprocessingAdmissionState is the source revision's committed admission
// snapshot, which the reopener must reset and everything else must leave alone.
func d1ReprocessingAdmissionState(t *testing.T, conn *sqliteD1Conn, revision string) (string, string, string, string, string) {
	t.Helper()
	var state, admissionVersion, extractorVersion, indexVersion string
	var resultHash *string
	if err := conn.QueryRow(context.Background(), `
		SELECT derived_admission_state, derived_admission_version,
		       derived_extractor_version, derived_index_version, derived_result_hash
		FROM memory_source_revisions WHERE source_revision = ?`, revision).Scan(
		&state, &admissionVersion, &extractorVersion, &indexVersion, &resultHash); err != nil {
		t.Fatalf("read admission state for %s: %v", revision, err)
	}
	return state, admissionVersion, extractorVersion, indexVersion, d1DerefString(resultHash)
}

// d1ReprocessingCommitAdmission marks a revision's admission as committed, the
// state a previously successful run leaves behind. The reopener has to clear it
// or the retry would short-circuit on a snapshot claiming the work already
// happened.
func d1ReprocessingCommitAdmission(t *testing.T, conn *sqliteD1Conn, revision string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `
		UPDATE memory_source_revisions
		SET derived_admission_state = 'committed',
		    derived_admission_version = ?,
		    derived_extractor_version = ?,
		    derived_index_version = ?,
		    derived_result_hash = ?,
		    derived_result_json = ?,
		    derived_admitted_at = ?
		WHERE source_revision = ?`,
		"admission.v1", "extractor.v9", "index.v9", "hash-committed",
		`{"admitted":true}`, "2026-07-28T02:30:00Z", revision); err != nil {
		t.Fatalf("commit admission for %s: %v", revision, err)
	}
}

// d1ReprocessingJob builds the enqueue request for a scene. Tests vary only the
// idempotency key and the version triple, which are the identity the conflict
// check compares.
func d1ReprocessingJob(scene d1ReprocessingScene, key string) *MemoryReprocessingJob {
	return &MemoryReprocessingJob{
		IdempotencyKey:    key,
		ChatSessionID:     scene.SessionID,
		SourceRevision:    scene.LiveRev,
		SourceContract:    "source_acceptance_observation.v1",
		DerivationVersion: PreciseMemoryUnitContract,
		ExtractorVersion:  "complete_turn.configured_critic_extract",
		IndexVersion:      "not_materialized",
		CreatedAt:         time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC),
		UpdatedAt:         time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC),
	}
}

// ---------------------------------------------------------------------------
// capability advertisement
// ---------------------------------------------------------------------------

// TestD1ReprocessingCapabilitiesAreAdvertised pins the delivery state. A
// capability that is implemented but not advertised through the manifest makes
// the worker silently skip the queue: memory_reprocessing_worker.go type-asserts
// the store and returns ErrNotEnabled when the assertion fails, which is
// indistinguishable from "there was no work".
func TestD1ReprocessingCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var store Store = st
	if _, ok := store.(MemoryReprocessingJobStore); !ok {
		t.Fatal("the D1 provider must expose MemoryReprocessingJobStore")
	}
	if _, ok := store.(MemoryReprocessingJobReopener); !ok {
		t.Fatal("the D1 provider must expose MemoryReprocessingJobReopener")
	}
	if _, ok := store.(MemoryReprocessingWakeScheduleStore); !ok {
		t.Fatal("the D1 provider must expose MemoryReprocessingWakeScheduleStore")
	}
	for _, capability := range []string{
		"MemoryReprocessingJobStore",
		"MemoryReprocessingJobReopener",
		"MemoryReprocessingWakeScheduleStore",
	} {
		reported := false
		for _, status := range CapabilityReport(store) {
			if status.Name != capability {
				continue
			}
			reported = true
			if !status.Implemented {
				t.Errorf("the capability manifest reports %s as missing", capability)
			}
		}
		if !reported {
			t.Errorf("%s is not probed by the capability manifest", capability)
		}
	}
}

// ---------------------------------------------------------------------------
// enqueue
// ---------------------------------------------------------------------------

// TestD1ReprocessingEnqueueIsIdempotentAndFenced pins rule 6 from both sides: an
// exact replay must not duplicate the row, and a collision that is not an exact
// replay must be a conflict rather than a silent overwrite.
func TestD1ReprocessingEnqueueIsIdempotentAndFenced(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-enqueue")
	ctx := context.Background()
	key := strings.Repeat("a", 64)

	job := d1ReprocessingJob(scene, key)
	inserted, err := st.EnqueueMemoryReprocessingJob(ctx, job)
	if err != nil || !inserted {
		t.Fatalf("first enqueue inserted=%v err=%v", inserted, err)
	}
	// The contract default is applied to the CALLER's job, not just the row: the
	// HTTP layer logs the returned contract version and a blank one reads as a
	// missing contract.
	if job.ContractVersion != MemoryReprocessingJobContract || job.Status != "pending" {
		t.Fatalf("enqueue defaults not applied: contract=%q status=%q", job.ContractVersion, job.Status)
	}

	replay := d1ReprocessingJob(scene, key)
	inserted, err = st.EnqueueMemoryReprocessingJob(ctx, replay)
	if err != nil || inserted {
		t.Fatalf("exact replay inserted=%v err=%v", inserted, err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_reprocessing_jobs WHERE idempotency_key = ?`, key); got != 1 {
		t.Fatalf("replay duplicated the queue row: %d rows", got)
	}

	// A different derivation under the same key is a different unit of work. The
	// stored row must survive untouched: overwriting it would make the key stop
	// meaning "this exact derivation", and a completed-turn retry would then
	// silently run a derivation nobody asked for.
	conflicting := d1ReprocessingJob(scene, key)
	conflicting.ExtractorVersion = "critic.v2"
	inserted, err = st.EnqueueMemoryReprocessingJob(ctx, conflicting)
	if inserted || !errors.Is(err, d1ReprocessingIdempotencyConflict) {
		t.Fatalf("version collision inserted=%v err=%v", inserted, err)
	}
	if got := d1Count(t, conn,
		`SELECT COUNT(*) FROM memory_reprocessing_jobs WHERE idempotency_key = ? AND extractor_version = ?`,
		key, "complete_turn.configured_critic_extract"); got != 1 {
		t.Fatalf("conflicting replay overwrote the stored row: %d matching rows", got)
	}

	// A replay under a different SESSION is the same conflict class, and it is
	// the one an operator actually hits after a session migration: the key is
	// global, the identity is not.
	otherKey := strings.Repeat("b", 64)
	otherScene := d1ReprocessingScene{SessionID: "s-other", LiveRev: "s-other-rev-live"}
	d1SeedIdentityRevision(t, conn, otherScene.SessionID, otherScene.LiveRev, "turn-1", "active")
	crossed := d1ReprocessingJob(scene, otherKey)
	crossed.ChatSessionID = otherScene.SessionID
	crossed.SourceRevision = otherScene.LiveRev
	if inserted, err = st.EnqueueMemoryReprocessingJob(ctx, crossed); !inserted || err != nil {
		t.Fatalf("cross-session enqueue inserted=%v err=%v", inserted, err)
	}
	stealing := d1ReprocessingJob(scene, otherKey)
	if inserted, err = st.EnqueueMemoryReprocessingJob(ctx, stealing); inserted || !errors.Is(err, d1ReprocessingIdempotencyConflict) {
		t.Fatalf("session collision inserted=%v err=%v", inserted, err)
	}
}

// TestD1ReprocessingEnqueueRejectsIncompleteJobAndUnknownRevision pins the two
// ways the enqueue must refuse before the worker ever sees the row.
func TestD1ReprocessingEnqueueRejectsIncompleteJobAndUnknownRevision(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-enqueue-guard")
	ctx := context.Background()

	for name, job := range map[string]*MemoryReprocessingJob{
		"nil":                 nil,
		"blank key":           {IdempotencyKey: "  ", ChatSessionID: "s", SourceRevision: "r"},
		"blank session":       {IdempotencyKey: "k", ChatSessionID: " ", SourceRevision: "r"},
		"blank source":        {IdempotencyKey: "k", ChatSessionID: "s", SourceRevision: " "},
		"whitespace-only key": {IdempotencyKey: "\t\n", ChatSessionID: "s", SourceRevision: "r"},
	} {
		inserted, err := st.EnqueueMemoryReprocessingJob(ctx, job)
		if inserted || err == nil || !strings.Contains(err.Error(), "invalid memory reprocessing job") {
			t.Errorf("%s: inserted=%v err=%v, want the reference validation error", name, inserted, err)
		}
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_reprocessing_jobs`); got != 0 {
		t.Fatalf("rejected enqueues wrote %d rows", got)
	}

	// An unknown source revision is refused by the canonical foreign key, not by
	// a Go check. That is deliberate: the enqueue is NOT lifecycle-fenced (see
	// the implementation header), so existence is the only thing the schema can
	// enforce, and it must still be enforced — a job with no revision can never
	// be joined by the claim and would sit in the queue forever.
	orphan := d1ReprocessingJob(scene, strings.Repeat("c", 64))
	orphan.SourceRevision = scene.SessionID + "-rev-missing"
	if inserted, err := st.EnqueueMemoryReprocessingJob(ctx, orphan); inserted || err == nil {
		t.Fatalf("orphan enqueue inserted=%v err=%v, want a foreign-key failure", inserted, err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_reprocessing_jobs`); got != 0 {
		t.Fatalf("orphan enqueue wrote %d rows", got)
	}
}

// TestD1ReprocessingEnqueueAcceptsSupersededRevision pins the deliberate absence
// of a lifecycle fence. A job queued for a revision that has just been rolled back
// is legal here: the claim sweep retires it. Refusing at enqueue time would make
// a rolled-back turn permanently underivable, because the caller treats a missing
// job as "nothing to reprocess" rather than as an error.
func TestD1ReprocessingEnqueueAcceptsSupersededRevision(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-enqueue-dead")
	ctx := context.Background()

	job := d1ReprocessingJob(scene, strings.Repeat("d", 64))
	job.SourceRevision = scene.DeadRev
	inserted, err := st.EnqueueMemoryReprocessingJob(ctx, job)
	if err != nil || !inserted {
		t.Fatalf("superseded enqueue inserted=%v err=%v", inserted, err)
	}
	id := d1Count(t, conn, `SELECT id FROM memory_reprocessing_jobs WHERE idempotency_key = ?`, job.IdempotencyKey)

	// The sweep is the fence, and it must retire the job without ever leasing it.
	claimed, err := st.ClaimMemoryReprocessingJob(ctx, "worker", time.Date(2026, 7, 28, 3, 0, 0, 0, time.UTC), time.Minute)
	if !errors.Is(err, ErrNotFound) || claimed != nil {
		t.Fatalf("claim over a retired job: %+v err=%v, want ErrNotFound", claimed, err)
	}
	state := d1ReprocessingJobState(t, conn, int64(id))
	if state.Status != "stale_rejected" || state.LastError != "source_revision_not_active" {
		t.Fatalf("swept job state=%+v, want stale_rejected/source_revision_not_active", state)
	}
}

// ---------------------------------------------------------------------------
// claim
// ---------------------------------------------------------------------------

// TestD1ReprocessingClaimLeasesExactlyOneJobAndProvesExclusivity is the central
// lease test. It walks the whole lifecycle through the real statements: a pending
// job is leased with its attempt counter incremented, a second worker with no
// work left gets ErrNotFound rather than the same job, and the completed job
// releases the lease.
func TestD1ReprocessingClaimLeasesExactlyOneJobAndProvesExclusivity(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-claim")
	ctx := context.Background()
	now := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

	id := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("e", 64),
		sessionID:      scene.SessionID,
		sourceRevision: scene.LiveRev,
		attempts:       2,
		createdAt:      "2026-07-28T01:00:00Z",
	})

	claimed, err := st.ClaimMemoryReprocessingJob(ctx, "worker-1", now, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != int64(id) || claimed.Status != "leased" || claimed.LeaseOwner != "worker-1" {
		t.Fatalf("claimed=%+v, want id=%d leased to worker-1", claimed, id)
	}
	// The returned attempt count must match the STORED one, because the worker
	// compares it against its failure ceiling. A stale value here turns a job
	// that should be abandoned into one that is retried forever.
	if claimed.Attempts != 3 {
		t.Fatalf("claimed attempts=%d, want 3", claimed.Attempts)
	}
	if !claimed.LeaseUntil.Equal(now.Add(time.Minute)) {
		t.Fatalf("claimed lease until=%v, want %v", claimed.LeaseUntil, now.Add(time.Minute))
	}
	state := d1ReprocessingJobState(t, conn, int64(id))
	if state.Status != "leased" || state.Attempts != 3 || state.LeaseOwner != "worker-1" {
		t.Fatalf("stored lease state=%+v", state)
	}
	if state.RetryAfter != "" || state.LastError != "" {
		t.Fatalf("claim must not invent a wake cursor or an error: %+v", state)
	}

	// A second worker must find nothing. This is the assertion that matters: the
	// compare-and-swap is what makes it true, and a plain "SELECT then UPDATE by
	// id" would hand the same job to both workers.
	second, err := st.ClaimMemoryReprocessingJob(ctx, "worker-2", now, time.Minute)
	if !errors.Is(err, ErrNotFound) || second != nil {
		t.Fatalf("second claim=%+v err=%v, want ErrNotFound", second, err)
	}
	if after := d1ReprocessingJobState(t, conn, int64(id)); after.LeaseOwner != "worker-1" || after.Attempts != 3 {
		t.Fatalf("second claim stole the lease: %+v", after)
	}

	if err := st.CompleteMemoryReprocessingJob(ctx, int64(id), "worker-1", now.Add(30*time.Second)); err != nil {
		t.Fatalf("complete: %v", err)
	}
	done := d1ReprocessingJobState(t, conn, int64(id))
	if done.Status != "completed" || done.LeaseOwner != "" || done.LeaseUntil != "" || done.RetryAfter != "" {
		t.Fatalf("completed state=%+v, want completed with a released lease and no wake cursor", done)
	}

	// A completed job is terminal: it must not be claimable again.
	third, err := st.ClaimMemoryReprocessingJob(ctx, "worker-3", now.Add(time.Hour), time.Minute)
	if !errors.Is(err, ErrNotFound) || third != nil {
		t.Fatalf("claim over a completed job=%+v err=%v", third, err)
	}
}

// TestD1ReprocessingClaimRecoversAbandonedLeaseAndHonoursRetryCursor pins the
// two timing rules the wake scheduler depends on. Both are text comparisons on
// the D1 side, so they are asserted with values that differ from `now` by more
// than a rounding artefact.
func TestD1ReprocessingClaimRecoversAbandonedLeaseAndHonoursRetryCursor(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-timing")
	ctx := context.Background()
	now := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

	// A lease that expired an hour ago belongs to a worker that died. It must be
	// recoverable, or the turn is never reprocessed again.
	abandoned := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("f", 64),
		sessionID:      scene.SessionID,
		sourceRevision: scene.LiveRev,
		status:         "leased",
		leaseOwner:     "dead-worker",
		leaseUntil:     "2026-07-28T01:00:00Z",
		attempts:       1,
		createdAt:      "2026-07-28T00:00:00Z",
	})
	// retry_after is an EXCLUSIVE cursor: a job that failed in this very wake has
	// retry_after == now and must not be re-claimed by the same drain, or a
	// permanently failing turn spins instead of backing off.
	waiting := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("1", 64),
		sessionID:      scene.SessionID,
		sourceRevision: scene.LiveRev,
		status:         "retryable",
		retryAfter:     "2026-07-28T02:00:00Z",
		attempts:       4,
		createdAt:      "2026-07-28T00:00:00Z",
	})

	claimed, err := st.ClaimMemoryReprocessingJob(ctx, "worker-live", now, 2*time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// FIFO by created_at, id tiebreak: the abandoned job was created first.
	if claimed.ID != int64(abandoned) {
		t.Fatalf("claimed id=%d, want the abandoned lease %d", claimed.ID, abandoned)
	}
	if claimed.Attempts != 2 {
		t.Fatalf("recovered attempts=%d, want 2", claimed.Attempts)
	}

	// The retryable job's wake cursor is exactly `now`, so it is not claimable at
	// `now`...
	if again, err := st.ClaimMemoryReprocessingJob(ctx, "worker-live", now, time.Minute); !errors.Is(err, ErrNotFound) || again != nil {
		t.Fatalf("claim at the exclusive cursor boundary=%+v err=%v", again, err)
	}
	// ...but it becomes claimable the moment the cursor is behind us.
	after, err := st.ClaimMemoryReprocessingJob(ctx, "worker-live", now.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatalf("claim after the cursor passed: %v", err)
	}
	if after.ID != int64(waiting) {
		t.Fatalf("claimed id=%d, want the due retryable job %d", after.ID, waiting)
	}

	// A job whose lease is still live must never be stolen, even by a later
	// drain with a different owner.
	live := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("2", 64),
		sessionID:      scene.SessionID,
		sourceRevision: scene.LiveRev,
		status:         "leased",
		leaseOwner:     "busy-worker",
		leaseUntil:     "2026-07-28T09:00:00Z",
		attempts:       1,
		createdAt:      "2026-07-28T00:00:00Z",
	})
	if _, err := st.ClaimMemoryReprocessingJob(ctx, "thief", now, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("claim over a live lease err=%v, want ErrNotFound", err)
	}
	if state := d1ReprocessingJobState(t, conn, int64(live)); state.LeaseOwner != "busy-worker" {
		t.Fatalf("live lease was stolen: %+v", state)
	}
}

// TestD1ReprocessingClaimFencesMigrationLockedSession pins the migration fence.
// A session that is mid-migration is about to have its jobs deleted; leasing one
// would spend a whole admission pass on rows that are removed moments later.
func TestD1ReprocessingClaimFencesMigrationLockedSession(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-locked")
	ctx := context.Background()
	now := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

	locked := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("3", 64),
		sessionID:      scene.SessionID,
		sourceRevision: scene.LockedLive,
		createdAt:      "2026-07-28T00:00:00Z",
	})
	// A lock that is flagged but already released must NOT fence anything: the
	// reference requires BOTH locked and unlocked_at IS NULL, and a lock that
	// ignores the release time would strand every job of a finished migration.
	d1SeedSessionMigration(t, conn, 900, "s-released", "s-target", "copy", "completed", "")
	d1SeedSessionMigrationLock(t, conn, 900, "s-released", "s-target", "migrated_away", "2026-07-28T00:00:00Z", 1, "2026-07-28T01:00:00Z")
	d1SeedSessionMigration(t, conn, 901, scene.SessionID, "s-target", "copy", "in_progress", "")
	d1SeedSessionMigrationLock(t, conn, 901, scene.SessionID, "s-target", "migrating_away", "2026-07-28T00:00:00Z", 1, nil)

	// Only the released session has work, and it is claimable.
	d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("4", 64),
		sessionID:      "s-released",
		sourceRevision: scene.LiveRev,
		createdAt:      "2026-07-28T00:00:00Z",
	})
	claimed, err := st.ClaimMemoryReprocessingJob(ctx, "worker", now, time.Minute)
	if err != nil || claimed.ChatSessionID != "s-released" {
		t.Fatalf("claim=%+v err=%v, want the released session's job", claimed, err)
	}
	if state := d1ReprocessingJobState(t, conn, int64(locked)); state.Status != "pending" {
		t.Fatalf("migration-locked job state=%+v, want pending", state)
	}
}

// TestD1ReprocessingClaimSweepsStaleSourceRevisions pins the sweep that the
// MariaDB multi-table UPDATE expresses. Only CLAIMABLE statuses are swept: a
// permanent failure and a completed job keep their terminal record.
func TestD1ReprocessingClaimSweepsStaleSourceRevisions(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-sweep")
	ctx := context.Background()

	pending := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("5", 64), sessionID: scene.SessionID,
		sourceRevision: scene.DeadRev, status: "pending", createdAt: "2026-07-28T00:00:00Z",
	})
	leased := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("6", 64), sessionID: scene.SessionID,
		sourceRevision: scene.DeadRev, status: "leased", leaseOwner: "worker",
		leaseUntil: "2026-07-28T09:00:00Z", createdAt: "2026-07-28T00:00:00Z",
	})
	retryable := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("7", 64), sessionID: scene.SessionID,
		sourceRevision: scene.DeadRev, status: "retryable", createdAt: "2026-07-28T00:00:00Z",
	})
	permanent := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("8", 64), sessionID: scene.SessionID,
		sourceRevision: scene.DeadRev, status: "permanent", createdAt: "2026-07-28T00:00:00Z",
	})
	completed := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("9", 64), sessionID: scene.SessionID,
		sourceRevision: scene.DeadRev, status: "completed", createdAt: "2026-07-28T00:00:00Z",
	})
	live := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("0", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, createdAt: "2026-07-28T00:00:00Z",
	})

	if _, err := st.ClaimMemoryReprocessingJob(ctx, "worker", time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC), time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}

	for name, id := range map[string]int64{
		"pending": pending, "leased": leased, "retryable": retryable,
	} {
		state := d1ReprocessingJobState(t, conn, id)
		if state.Status != "stale_rejected" || state.LastError != "source_revision_not_active" ||
			state.LeaseOwner != "" || state.LeaseUntil != "" {
			t.Errorf("%s job was not retired: %+v", name, state)
		}
	}
	// Terminal rows are not claimable, so the sweep must leave their record
	// alone: rewriting a permanent failure to stale_rejected would erase the
	// distinction the operator HUD renders.
	for name, id := range map[string]int64{"permanent": permanent, "completed": completed} {
		if state := d1ReprocessingJobState(t, conn, id); state.Status == "stale_rejected" {
			t.Errorf("%s job was swept: %+v", name, state)
		}
	}
	if state := d1ReprocessingJobState(t, conn, live); state.Status != "leased" {
		t.Fatalf("live-source job was swept or lost: %+v", state)
	}
}

// TestD1ReprocessingClaimRejectsInvalidLease pins the reference's argument
// validation. A zero or negative lease duration would produce a lease_until in
// the past, which the next drain would immediately treat as abandoned and steal —
// so refusing it is the only way to keep a worker from losing its own work.
func TestD1ReprocessingClaimRejectsInvalidLease(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-lease-guard")
	ctx := context.Background()
	id := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("a1", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev,
	})
	now := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

	for name, tc := range map[string]struct {
		owner string
		lease time.Duration
	}{
		"blank owner":       {"  ", time.Minute},
		"zero lease":        {"worker", 0},
		"negative lease":    {"worker", -time.Minute},
		"blank owner zero":  {"", 0},
		"negative and only": {"\n", -time.Second},
	} {
		job, err := st.ClaimMemoryReprocessingJob(ctx, tc.owner, now, tc.lease)
		if job != nil || err == nil || !strings.Contains(err.Error(), "invalid memory reprocessing lease") {
			t.Errorf("%s: job=%+v err=%v, want the reference validation error", name, job, err)
		}
	}
	if state := d1ReprocessingJobState(t, conn, int64(id)); state.Status != "pending" || state.LeaseOwner != "" {
		t.Fatalf("a rejected claim mutated the queue: %+v", state)
	}
}

// ---------------------------------------------------------------------------
// complete / fail
// ---------------------------------------------------------------------------

// TestD1ReprocessingFinishRequiresTheLiveLease pins the ErrLeaseExpired cases.
// Every one of them must leave the row untouched: a completion that lands on a
// lease somebody else holds reports a success the caller did not earn, and the
// real owner's result is then overwritten.
func TestD1ReprocessingFinishRequiresTheLiveLease(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-finish")
	ctx := context.Background()
	now := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

	// A leased job the caller does not own.
	notMine := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("b1", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, status: "leased", leaseOwner: "owner-1",
		leaseUntil: "2026-07-28T03:00:00Z", attempts: 1,
	})
	// A leased job whose lease already expired: the owner may no longer finish it,
	// because a later drain is entitled to take it over at any moment.
	expired := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("b2", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, status: "leased", leaseOwner: "owner-2",
		leaseUntil: "2026-07-28T01:00:00Z", attempts: 1,
	})
	// A job that is not leased at all.
	pending := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("b3", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, status: "pending",
	})
	// An id that addresses nothing.
	missing := int64(999999)

	if err := st.CompleteMemoryReprocessingJob(ctx, int64(notMine), "owner-2", now); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("foreign lease completion err=%v, want ErrLeaseExpired", err)
	}
	if err := st.FailMemoryReprocessingJob(ctx, int64(expired), "owner-2", now, now.Add(time.Minute), false, "boom"); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("expired lease failure err=%v, want ErrLeaseExpired", err)
	}
	if err := st.CompleteMemoryReprocessingJob(ctx, int64(pending), "owner-1", now); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("unleased completion err=%v, want ErrLeaseExpired", err)
	}
	if err := st.CompleteMemoryReprocessingJob(ctx, missing, "owner-1", now); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("missing job completion err=%v, want ErrLeaseExpired", err)
	}
	if err := st.FailMemoryReprocessingJob(ctx, missing, "owner-1", now, now.Add(time.Minute), true, "boom"); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("missing job failure err=%v, want ErrLeaseExpired", err)
	}

	for name, id := range map[string]int64{"not mine": notMine, "expired": expired} {
		state := d1ReprocessingJobState(t, conn, id)
		if state.Status != "leased" || state.LeaseOwner == "" || state.RetryAfter != "" {
			t.Errorf("%s job was mutated by a refused finish: %+v", name, state)
		}
	}
	if state := d1ReprocessingJobState(t, conn, int64(pending)); state.Status != "pending" || state.LeaseOwner != "" {
		t.Fatalf("unleased job was mutated by a refused finish: %+v", state)
	}
}

// TestD1ReprocessingFailRecordsRetryAndPermanentOutcomes pins the two failure
// shapes. A permanent failure must leave NO retry_after: a terminal job with a
// wake cursor would sit in NextMemoryReprocessingWakeAt forever, and the worker
// would wake repeatedly for a job it can never run.
func TestD1ReprocessingFailRecordsRetryAndPermanentOutcomes(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-fail")
	ctx := context.Background()
	now := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)
	retryAt := now.Add(5 * time.Minute)

	retryable := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("c1", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, status: "leased", leaseOwner: "worker",
		leaseUntil: "2026-07-28T03:00:00Z", attempts: 1,
	})
	permanent := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("c2", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, status: "leased", leaseOwner: "worker",
		leaseUntil: "2026-07-28T03:00:00Z", attempts: 9,
	})

	if err := st.FailMemoryReprocessingJob(ctx, int64(retryable), "worker", now, retryAt, false, "critic_timeout"); err != nil {
		t.Fatalf("retryable failure: %v", err)
	}
	state := d1ReprocessingJobState(t, conn, int64(retryable))
	if state.Status != "retryable" || state.LastError != "critic_timeout" ||
		state.LeaseOwner != "" || state.LeaseUntil != "" {
		t.Fatalf("retryable state=%+v", state)
	}
	if state.RetryAfter != d1TimeValue(retryAt) {
		t.Fatalf("retry_after=%q, want %q", state.RetryAfter, d1TimeValue(retryAt))
	}
	// The attempt counter is NOT reset by a failure — that is the worker's own
	// failure ceiling, and a reset here would retry a poison turn forever.
	if state.Attempts != 1 {
		t.Fatalf("attempts=%d, want the failure to preserve the counter", state.Attempts)
	}

	if err := st.FailMemoryReprocessingJob(ctx, int64(permanent), "worker", now, retryAt, true, "schema_unsupported"); err != nil {
		t.Fatalf("permanent failure: %v", err)
	}
	stopped := d1ReprocessingJobState(t, conn, int64(permanent))
	if stopped.Status != "permanent" || stopped.LastError != "schema_unsupported" {
		t.Fatalf("permanent state=%+v", stopped)
	}
	// The reference binds the caller's cursor for EVERY finish, permanent
	// included, so the leftover retry_after is correct here. What must keep this
	// row from ever being scheduled or leased is the STATUS predicate in the two
	// read statements, and the two assertions below exercise exactly that.
	if stopped.RetryAfter != d1TimeValue(retryAt) {
		t.Fatalf("retry_after=%q, want the reference's %q", stopped.RetryAfter, d1TimeValue(retryAt))
	}
	if stopped.Attempts != 9 {
		t.Fatalf("attempts=%d, want 9", stopped.Attempts)
	}

	// A day later the retryable job's cursor has long passed, so it is legitimately
	// claimable again — and the permanent one must never be, even though its
	// cursor has passed too. Claiming the wrong row here would resurrect a turn
	// the worker already gave up on.
	claimed, err := st.ClaimMemoryReprocessingJob(ctx, "worker", now.Add(24*time.Hour), time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != int64(retryable) {
		t.Fatalf("claimed id=%d, want the retryable job %d", claimed.ID, retryable)
	}
	if err := st.CompleteMemoryReprocessingJob(ctx, int64(retryable), "worker", now.Add(24*time.Hour)); err != nil {
		t.Fatalf("complete the recovered job: %v", err)
	}
	if again, err := st.ClaimMemoryReprocessingJob(ctx, "worker", now.Add(48*time.Hour), time.Minute); !errors.Is(err, ErrNotFound) || again != nil {
		t.Fatalf("claim over the remaining permanent job=%+v err=%v, want ErrNotFound", again, err)
	}
	// Same for the schedule: only the permanent row is left, and its leftover
	// cursor must not produce a wake the worker can never act on.
	if _, err := st.NextMemoryReprocessingWakeAt(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a permanent failure must not schedule a wake: err=%v", err)
	}
}

// TestD1ReprocessingFinishOnSupersededRevisionCommitsStaleRejection pins the one
// path that returns an error AFTER committing. A worker that finished a job whose
// revision was rolled back must leave a durable stale_rejected record; leaving the
// job leased would block re-queueing it and lose the record that the work was
// correctly abandoned.
func TestD1ReprocessingFinishOnSupersededRevisionCommitsStaleRejection(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-stale-finish")
	ctx := context.Background()
	now := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

	id := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("d1", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, status: "leased", leaseOwner: "worker",
		leaseUntil: "2026-07-28T03:00:00Z", attempts: 1,
	})
	// The revision dies while the worker is mid-admission.
	if _, err := conn.Exec(ctx, `UPDATE memory_source_revisions SET lifecycle_state = 'superseded'
		WHERE source_revision = ?`, scene.LiveRev); err != nil {
		t.Fatalf("supersede revision: %v", err)
	}

	if err := st.CompleteMemoryReprocessingJob(ctx, int64(id), "worker", now); !errors.Is(err, ErrSourceRevisionStale) {
		t.Fatalf("stale completion err=%v, want ErrSourceRevisionStale", err)
	}
	state := d1ReprocessingJobState(t, conn, int64(id))
	if state.Status != "stale_rejected" || state.LastError != "source_revision_not_active" ||
		state.LeaseOwner != "" || state.LeaseUntil != "" {
		t.Fatalf("stale state=%+v", state)
	}
	// The same fence applies to a failure: a failed admission on a dead revision
	// must be retired, not re-queued.
	id2 := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("d2", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, status: "leased", leaseOwner: "worker",
		leaseUntil: "2026-07-28T03:00:00Z", attempts: 1,
	})
	if err := st.FailMemoryReprocessingJob(ctx, int64(id2), "worker", now, now.Add(time.Minute), false, "boom"); !errors.Is(err, ErrSourceRevisionStale) {
		t.Fatalf("stale failure err=%v, want ErrSourceRevisionStale", err)
	}
	if state := d1ReprocessingJobState(t, conn, int64(id2)); state.Status != "stale_rejected" {
		t.Fatalf("stale failure state=%+v", state)
	}
}

// ---------------------------------------------------------------------------
// wake schedule
// ---------------------------------------------------------------------------

// TestD1ReprocessingNextWakeAtUsesTheRightColumn pins the MIN-over-CASE. The
// value it returns is the one the worker arms its one-shot timer with, so taking
// the wrong column is a silent scheduling bug: a wake driven by retry_after while
// every job is still leased busy-loops, and one driven by lease_until sleeps
// through a retryable job that is already due.
func TestD1ReprocessingNextWakeAtUsesTheRightColumn(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-wake")
	ctx := context.Background()

	if _, err := st.NextMemoryReprocessingWakeAt(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty queue err=%v, want ErrNotFound", err)
	}

	// Only a pending job with a retry_after participates; a pending job with none
	// is claimable immediately and has no future wake to schedule.
	d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("e1", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, status: "pending",
	})
	if _, err := st.NextMemoryReprocessingWakeAt(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pending job with no cursor err=%v, want ErrNotFound", err)
	}
	// A dead revision and a migration-locked session are both invisible to the
	// schedule: neither will ever be claimed, so neither may delay a wake.
	d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("e2", 64), sessionID: scene.SessionID,
		sourceRevision: scene.DeadRev, status: "retryable", retryAfter: "2026-07-28T02:01:00Z",
	})
	if _, err := st.NextMemoryReprocessingWakeAt(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale-source job must not schedule a wake: err=%v", err)
	}

	// The migration fence compares the LOCK's source_session_id against the
	// JOB's chat_session_id, so this job must sit in the locked session while
	// drawing its source revision from elsewhere.
	d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("e3", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LockedLive, status: "retryable", retryAfter: "2026-07-28T02:02:00Z",
	})
	d1SeedSessionMigration(t, conn, 950, scene.SessionID, "s-target", "copy", "in_progress", "")
	d1SeedSessionMigrationLock(t, conn, 950, scene.SessionID, "s-target", "migrating_away", "2026-07-28T00:00:00Z", 1, nil)
	if _, err := st.NextMemoryReprocessingWakeAt(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("migration-locked job must not schedule a wake: err=%v", err)
	}

	// The minimum across both columns: an expired lease is due sooner than the
	// retryable job's cursor, and the lease column must win.
	d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("e4", 64), sessionID: "s-released",
		sourceRevision: scene.LiveRev, status: "leased", leaseOwner: "dead",
		leaseUntil: "2026-07-28T02:05:00Z", attempts: 1,
	})
	d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("e5", 64), sessionID: "s-released",
		sourceRevision: scene.LiveRev, status: "retryable", retryAfter: "2026-07-28T03:00:00Z",
		attempts: 2,
	})
	next, err := st.NextMemoryReprocessingWakeAt(ctx)
	if err != nil {
		t.Fatalf("next wake: %v", err)
	}
	want, err := parseD1Time("2026-07-28T02:05:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if !next.Equal(want) {
		t.Fatalf("next wake=%v, want %v (the lease_until branch of the CASE)", next, want)
	}

	// A completed job contributes nothing even when it still carries a cursor, so
	// a terminal row can never delay or pull forward the schedule.
	if _, err := conn.Exec(ctx, `UPDATE memory_reprocessing_jobs
		SET status = 'completed', retry_after = '2026-07-28T00:30:00Z'
		WHERE idempotency_key = ?`, strings.Repeat("e5", 64)); err != nil {
		t.Fatalf("complete the retryable job: %v", err)
	}
	next, err = st.NextMemoryReprocessingWakeAt(ctx)
	if err != nil {
		t.Fatalf("next wake after completing: %v", err)
	}
	if !next.Equal(want) {
		t.Fatalf("a completed job changed the schedule: %v, want %v", next, want)
	}
	// With every row terminal the schedule is empty again, which the worker reads
	// as "keep the existing timer" rather than as a failure.
	if _, err := conn.Exec(ctx, `UPDATE memory_reprocessing_jobs SET status = 'completed'
		WHERE idempotency_key = ?`, strings.Repeat("e4", 64)); err != nil {
		t.Fatalf("complete the leased job: %v", err)
	}
	if _, err := st.NextMemoryReprocessingWakeAt(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("drained schedule err=%v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// reopener
// ---------------------------------------------------------------------------

// TestD1ReprocessingReopenResetsAdmissionAndRequeuesTheJob is the reopener's core
// promise. It resets the admission snapshot AND re-queues the job, and it leaves
// the raw observed source alone — the retry has to be reproducible, so a reopener
// that cleared the user content would make the second attempt unverifiable.
func TestD1ReprocessingReopenResetsAdmissionAndRequeuesTheJob(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-reopen")
	ctx := context.Background()
	now := time.Date(2026, 8, 3, 3, 4, 5, 0, time.UTC)
	key := strings.Repeat("f1", 64)

	id := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: key, sessionID: scene.SessionID, sourceRevision: scene.LiveRev,
		status: "completed", attempts: 3, lastError: "critic_timeout",
		retryAfter: "2026-07-28T09:00:00Z", leaseOwner: "worker", leaseUntil: "2026-07-28T09:00:00Z",
	})
	d1ReprocessingCommitAdmission(t, conn, scene.LiveRev)
	rawBefore := d1Count(t, conn, `SELECT COUNT(*) FROM memory_source_revisions
		WHERE source_revision = ? AND raw_user_content = 'u' AND raw_assistant_content = 'a'`, scene.LiveRev)
	if rawBefore != 1 {
		t.Fatalf("raw source seed is wrong: %d matching rows", rawBefore)
	}

	reopened, err := st.ReopenMemoryReprocessingJob(ctx, key, scene.SessionID, scene.LiveRev, now)
	if err != nil || !reopened {
		t.Fatalf("reopen=%v err=%v", reopened, err)
	}

	// The admission snapshot is cleared so the next admission actually re-runs.
	state, admissionVersion, extractorVersion, indexVersion, resultHash :=
		d1ReprocessingAdmissionState(t, conn, scene.LiveRev)
	if state != "pending" || admissionVersion != "" || extractorVersion != "" || indexVersion != "" || resultHash != "" {
		t.Fatalf("admission not reset: state=%q versions=(%q,%q,%q) hash=%q",
			state, admissionVersion, extractorVersion, indexVersion, resultHash)
	}
	// The raw observed source survives: the retry must be reproducible.
	if after := d1Count(t, conn, `SELECT COUNT(*) FROM memory_source_revisions
		WHERE source_revision = ? AND raw_user_content = 'u' AND raw_assistant_content = 'a'`, scene.LiveRev); after != 1 {
		t.Fatalf("reopen disturbed the raw source: %d matching rows", after)
	}
	// The job is back in the queue with a clean lease and a reset counter.
	row := d1ReprocessingJobState(t, conn, int64(id))
	if row.Status != "pending" || row.Attempts != 0 || row.LeaseOwner != "" ||
		row.LeaseUntil != "" || row.RetryAfter != "" || row.LastError != "" {
		t.Fatalf("reopened job state=%+v", row)
	}
	// And it is genuinely claimable again — the point of the whole operation.
	claimed, err := st.ClaimMemoryReprocessingJob(ctx, "worker", now, time.Minute)
	if err != nil || claimed.ID != int64(id) {
		t.Fatalf("reopened job is not claimable: %+v err=%v", claimed, err)
	}
}

// TestD1ReprocessingReopenRefusals pins every path that must write nothing. Each
// of them is a place where a half-applied reopen would leave a turn that can
// never be derived again, so the queue and the admission snapshot are both
// re-read after each refusal.
func TestD1ReprocessingReopenRefusals(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-reopen-refuse")
	ctx := context.Background()
	now := time.Date(2026, 8, 3, 3, 4, 5, 0, time.UTC)
	key := strings.Repeat("f2", 64)

	id := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: key, sessionID: scene.SessionID, sourceRevision: scene.LiveRev,
		status: "completed", attempts: 3,
	})
	d1ReprocessingCommitAdmission(t, conn, scene.LiveRev)

	// Unknown key: the route falls back to enqueueing, so ErrNotFound is the
	// signal it branches on.
	if reopened, err := st.ReopenMemoryReprocessingJob(ctx, strings.Repeat("ff", 64),
		scene.SessionID, scene.LiveRev, now); reopened || !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown key reopen=%v err=%v, want ErrNotFound", reopened, err)
	}
	// Identity mismatch: the key resolves to a different session or revision.
	if reopened, err := st.ReopenMemoryReprocessingJob(ctx, key, "s-somewhere-else", scene.LiveRev, now); reopened ||
		!errors.Is(err, d1ReprocessingIdempotencyConflict) {
		t.Fatalf("session mismatch reopen=%v err=%v, want an idempotency conflict", reopened, err)
	}
	if reopened, err := st.ReopenMemoryReprocessingJob(ctx, key, scene.SessionID, scene.DeadRev, now); reopened ||
		!errors.Is(err, d1ReprocessingIdempotencyConflict) {
		t.Fatalf("revision mismatch reopen=%v err=%v, want an idempotency conflict", reopened, err)
	}
	// Blank arguments.
	for name, args := range map[string][3]string{
		"blank key":         {"  ", scene.SessionID, scene.LiveRev},
		"blank session":     {key, " ", scene.LiveRev},
		"blank revision":    {key, scene.SessionID, "\t"},
		"all blank":         {"", "", ""},
		"whitespace around": {"\n" + key + "\n", " " + scene.SessionID + " ", " " + scene.LiveRev + " "},
	} {
		reopened, err := st.ReopenMemoryReprocessingJob(ctx, args[0], args[1], args[2], now)
		if name == "whitespace around" {
			// Trimming is part of the contract: a padded key addresses the same
			// row, and this one DOES reopen. Verified separately below.
			if err != nil || !reopened {
				t.Errorf("%s: reopened=%v err=%v, want the trimmed key to resolve", name, reopened, err)
			}
			continue
		}
		if reopened || err == nil || !strings.Contains(err.Error(), "invalid memory reprocessing reopen request") {
			t.Errorf("%s: reopened=%v err=%v, want the reference validation error", name, reopened, err)
		}
	}
	// The trimmed reopen above consumed the reopen, so re-seed for the rest.
	if _, err := conn.Exec(ctx, `UPDATE memory_reprocessing_jobs
		SET status = 'completed', lease_owner = NULL, lease_until = NULL, attempts = 3
		WHERE id = ?`, id); err != nil {
		t.Fatalf("restore job: %v", err)
	}
	d1ReprocessingCommitAdmission(t, conn, scene.LiveRev)

	// An active lease is refused. Reopening under it would reset attempts and
	// clear the lease out from under a worker mid-admission, so that worker's
	// writes would race a fresh re-run of the same turn.
	if _, err := conn.Exec(ctx, `UPDATE memory_reprocessing_jobs
		SET status = 'leased', lease_owner = 'busy', lease_until = ? WHERE id = ?`,
		d1TimeValue(now.Add(time.Minute)), id); err != nil {
		t.Fatalf("lease the job: %v", err)
	}
	reopened, err := st.ReopenMemoryReprocessingJob(ctx, key, scene.SessionID, scene.LiveRev, now)
	if reopened || !errors.Is(err, ErrMemoryReprocessingLeased) {
		t.Fatalf("leased reopen=%v err=%v, want ErrMemoryReprocessingLeased", reopened, err)
	}
	if row := d1ReprocessingJobState(t, conn, int64(id)); row.Status != "leased" || row.LeaseOwner != "busy" || row.Attempts != 3 {
		t.Fatalf("a refused reopen mutated the lease: %+v", row)
	}

	// A lease that has EXPIRED is reopenable: that is the abandoned-job case the
	// reopener exists for.
	if _, err := conn.Exec(ctx, `UPDATE memory_reprocessing_jobs
		SET lease_until = ? WHERE id = ?`, d1TimeValue(now.Add(-time.Minute)), id); err != nil {
		t.Fatalf("expire the lease: %v", err)
	}
	if reopened, err = st.ReopenMemoryReprocessingJob(ctx, key, scene.SessionID, scene.LiveRev, now); !reopened || err != nil {
		t.Fatalf("expired-lease reopen=%v err=%v", reopened, err)
	}

	// A dead source revision is refused with the source fence, and nothing is
	// written.
	if _, err := conn.Exec(ctx, `UPDATE memory_reprocessing_jobs SET status = 'completed' WHERE id = ?`, id); err != nil {
		t.Fatalf("reset job: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE memory_source_revisions SET lifecycle_state = 'invalidated'
		WHERE source_revision = ?`, scene.LiveRev); err != nil {
		t.Fatalf("invalidate revision: %v", err)
	}
	d1ReprocessingCommitAdmission(t, conn, scene.LiveRev)
	reopened, err = st.ReopenMemoryReprocessingJob(ctx, key, scene.SessionID, scene.LiveRev, now)
	if reopened || !errors.Is(err, ErrSourceRevisionStale) {
		t.Fatalf("dead-source reopen=%v err=%v, want ErrSourceRevisionStale", reopened, err)
	}
	if state, admissionVersion, _, _, resultHash := d1ReprocessingAdmissionState(t, conn, scene.LiveRev); state != "committed" ||
		admissionVersion != "admission.v1" || resultHash != "hash-committed" {
		t.Fatalf("a refused reopen reset the admission snapshot: state=%q version=%q hash=%q", state, admissionVersion, resultHash)
	}
	if row := d1ReprocessingJobState(t, conn, int64(id)); row.Status != "completed" {
		t.Fatalf("a refused reopen mutated the job: %+v", row)
	}
}

// TestD1ReprocessingReopenRequiresAResolvableSourceRevision pins the INNER JOIN.
// A job whose source revision is missing produces no row, which is ErrNotFound
// rather than a silent reopen: the route's ErrNotFound branch enqueues a fresh job
// and wakes the workers, and a "reopened" report for a revision that cannot be
// admitted would leave the turn with no work queued at all.
func TestD1ReprocessingReopenRequiresAResolvableSourceRevision(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-reopen-join")
	ctx := context.Background()
	now := time.Date(2026, 8, 3, 3, 4, 5, 0, time.UTC)
	key := strings.Repeat("f3", 64)

	id := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: key, sessionID: scene.SessionID, sourceRevision: scene.LiveRev,
		status: "completed", attempts: 2,
	})
	d1ReprocessingCommitAdmission(t, conn, scene.LiveRev)

	// Reopen it first so the job and the admission are in a known clean state.
	if reopened, err := st.ReopenMemoryReprocessingJob(ctx, key, scene.SessionID, scene.LiveRev, now); !reopened || err != nil {
		t.Fatalf("seed reopen=%v err=%v", reopened, err)
	}
	if _, err := conn.Exec(ctx, `UPDATE memory_reprocessing_jobs SET status = 'completed' WHERE id = ?`, id); err != nil {
		t.Fatalf("reset job: %v", err)
	}
	d1ReprocessingCommitAdmission(t, conn, scene.LiveRev)

	// Deleting the revision is blocked by the canonical foreign key, which is
	// itself the point: the job cannot outlive the revision it is joined to.
	if _, err := conn.Exec(ctx, `DELETE FROM memory_source_revisions WHERE source_revision = ?`,
		scene.LiveRev); err == nil {
		t.Fatal("the canonical schema must refuse to orphan a reprocessing job")
	}
	reopened, err := st.ReopenMemoryReprocessingJob(ctx, key, scene.SessionID, scene.LiveRev, now)
	if !reopened || err != nil {
		t.Fatalf("reopen after the orphan delete was refused: reopened=%v err=%v", reopened, err)
	}
}

// ---------------------------------------------------------------------------
// text-column ordering
// ---------------------------------------------------------------------------

// TestD1ReprocessingTextTimestampsOrderChronologically pins the accepted
// divergence (c) this file depends on: the D1 schema stores timestamps as
// zero-padded RFC3339 UTC TEXT, so ORDER BY and the MIN aggregate read them
// lexicographically, and that ordering agrees with the clock for the values
// d1TimeValue produces. The claim's FIFO and the wake schedule are both wrong if
// it ever stopped agreeing, so it is asserted rather than assumed.
func TestD1ReprocessingTextTimestampsOrderChronologically(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1ReprocessingSeedScene(t, conn, "s-order")
	ctx := context.Background()
	base := time.Date(2026, 7, 28, 2, 0, 0, 0, time.UTC)

	// Newest created_at first in the insert order, so a FIFO claim can only pass
	// if the text comparison really is chronological.
	third := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("11", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, createdAt: d1TimeValue(base.Add(2 * time.Hour)),
	})
	first := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("12", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, createdAt: d1TimeValue(base),
	})
	// Two jobs in the same instant: the id tiebreak is what makes their order
	// deterministic across providers.
	second := d1ReprocessingSeedJob(t, conn, d1ReprocessingJobSeed{
		idempotencyKey: strings.Repeat("13", 64), sessionID: scene.SessionID,
		sourceRevision: scene.LiveRev, createdAt: d1TimeValue(base),
	})

	claimed, err := st.ClaimMemoryReprocessingJob(ctx, "worker", base.Add(24*time.Hour), time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != int64(first) {
		t.Fatalf("claimed id=%d, want the oldest job %d", claimed.ID, first)
	}
	claimed, err = st.ClaimMemoryReprocessingJob(ctx, "worker", base.Add(24*time.Hour), time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != int64(second) {
		t.Fatalf("claimed id=%d, want the id tiebreak winner %d", claimed.ID, second)
	}
	claimed, err = st.ClaimMemoryReprocessingJob(ctx, "worker", base.Add(24*time.Hour), time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != int64(third) {
		t.Fatalf("claimed id=%d, want the newest job %d", claimed.ID, third)
	}
	if _, err := st.ClaimMemoryReprocessingJob(ctx, "worker", base.Add(24*time.Hour), time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("drained queue err=%v, want ErrNotFound", err)
	}
}
