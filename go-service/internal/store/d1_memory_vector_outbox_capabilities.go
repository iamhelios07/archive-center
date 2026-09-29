package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// D1 vector outbox capabilities.
//
// memory_vector_outbox is the revision-fenced, idempotent work queue between the
// canonical store and the vector index (ChromaDB on the local runtime, the
// Worker-side collection on Cloudflare). Three capabilities live on the table
// and all three are ported here, because a queue with only an enqueue is a
// queue nothing drains:
//
//   - MemoryVectorOutboxStore              enqueue, claim, complete, fail
//   - MemoryVectorOutboxLaneStore          the operation lane the bounded
//     authority worker uses to reserve fair service for deletes
//   - MemoryVectorOutboxMaintenanceStore   duplicate delete coalescing
//
// The three statements that carry the contract
//
//  1. operation_key is the outbox's only identity, and it is UNIQUE. Every
//     enqueue is therefore either a first write or a replay, and the two are
//     told apart by comparing the STORED row against the request. On MariaDB
//     that comparison happens after a caught 1062; on D1 it happens before the
//     insert, because D1Conn.Batch reports only an error and a statement that
//     silently does nothing would be indistinguishable from one that wrote a
//     row. The observable contract is unchanged: an exact replay reports
//     (false, nil) and writes nothing, a collision that is not an exact replay
//     is an idempotency conflict, and the stored row is never overwritten.
//     s.memoryDerivationWriteMu is held across the check and the write, so
//     nothing in this process can insert the same key in between.
//
//  2. The claim is a two-step read-then-write, and the read is where the whole
//     lease contract lives:
//
//       (embedding_ready = 1 AND status IN ('pending','retryable')
//         AND (retry_after IS NULL OR retry_after < ?))
//     OR (operation = 'upsert' AND embedding_ready = 0
//         AND status IN ('needs_embedding','retryable')
//         AND (retry_after IS NULL OR retry_after < ?))
//     OR (status = 'leased' AND lease_until < ?)
//
//     A leased row is claimable only while its lease_until is in the PAST. That
//     single clause is what stops the same operation being handed to two
//     owners: a worker that holds a live lease is invisible to every other
//     claim, and only a crashed worker's operation becomes visible again. Note
//     that lease_until IS NULL on a leased row does not qualify either (NULL < ?
//     is NULL, not true), which matches MariaDB: a lease with no deadline is
//     not a reclaimable lease.
//
//  3. The fence is a disjunction, never a conjunction, because the two required
//     source states are opposites:
//
//       (required_source_state = 'active'   AND lifecycle_state =  'active')
//       OR (required_source_state = 'inactive' AND lifecycle_state <> 'active')
//
//     An upsert of a document whose source the user has already rolled back, or
//     a delete of a document whose source is still current, are BOTH rejected.
//     Collapsing the two arms into one rule would let one of them through.
//
// Timestamps are TEXT, so the lease comparison is a text comparison
//
// lease_until and retry_after are RFC3339 UTC text, and every value compared
// against them is rendered by d1TimeValue(now) first, so both sides of the
// comparison use one representation. This file never adds CAST or julianday to
// a *_at comparison: lexicographic order over zero-padded UTC RFC3339 is
// chronological order, which is the same ordering the MariaDB DATETIME(3)
// columns give, and the timestamps this file writes are the ones d1TimeValue
// renders. Callers that pass a `now` with a sub-second component therefore get
// that same representation on both sides of the comparison.
//
// MariaDB-only constructs and what was done instead
//
//   - SELECT ... FOR UPDATE. D1 has no row locks. The claim's read is a plain
//     SELECT and its writes go through D1Conn.Batch, which is the atomic
//     boundary. The one place the reference relied on the row lock to make a
//     read-modify-write safe is the batch itself, and Batch keeps that
//     property; the cross-call window between the SELECT and the Batch is
//     closed inside one process by s.memoryDerivationWriteMu, and across
//     processes it fails as a lease the second claimant can see, never as a
//     silent double hand-out.
//   - UPDATE ... JOIN (multi-table update). SQLite has no multi-table UPDATE, so
//     the fenced row set is computed by an id subquery inside the same
//     statement. The set is identical and the statement is still atomic.
//   - STRAIGHT_JOIN / FORCE INDEX. MySQL planner hints with no SQLite meaning;
//     they are dropped. The predicates they steered are kept verbatim.
//   - LAST_INSERT_ID. Not needed: the outbox is addressed by its own
//     operation_key, never by a generated id at insert time.
//   - Batching across a prepared read. D1Conn.Batch only carries writes, so the
//     claim is: fence UPDATE (one atomic statement), seed SELECT, sibling
//     SELECT, then the lease UPDATEs as one Batch. The statement ORDER is the
//     reference's order and it matters: the fence must be applied before the
//     selection, because the selection is what decides which rows still block
//     later operations of the same document.

var _ MemoryVectorOutboxStore = (*d1Store)(nil)
var _ MemoryVectorOutboxLaneStore = (*d1Store)(nil)
var _ MemoryVectorOutboxMaintenanceStore = (*d1Store)(nil)

// d1OutboxFenceRejectedLastError is the reference's marker written onto a row
// the source-revision fence took away. It is a constant, not a formatted
// string, because a worker greps for it when it reports why a queued document
// was abandoned.
const d1OutboxFenceRejectedLastError = "source_revision_fence_rejected"

// d1OutboxDuplicateDeleteLastError is the reference's marker written onto a
// delete that a newer delete for the same document already supersedes.
const d1OutboxDuplicateDeleteLastError = "duplicate_delete_operation_key_4_0_4"

// d1OutboxRequiredSourceStateDefault is the source state an operation demands
// when the caller states none. It is derived from the operation rather than
// fixed: an upsert publishes a document that is only correct while its source
// is current, and a delete removes a document that only exists because its
// source is gone.
func d1OutboxRequiredSourceStateDefault(operation string) string {
	if operation == "upsert" {
		return "active"
	}
	return "inactive"
}

// d1OutboxScanItem mirrors scanMemoryVectorOutboxItem. The four nullable
// columns are read into pointer destinations so NULL stays distinguishable
// from a stored empty string: document_json is COALESCEd by the enqueue's
// identity check, and last_error/lease_owner/retry_after/lease_until are what
// tell a "never leased" row apart from a "leased and then released" one.
//
// Attempts, status, and the lease fields on the returned item are the values as
// they were STORED; the caller that claimed it overwrites the lease fields
// itself, exactly as the reference does, so a caller can never see a lease the
// database does not hold.
func d1OutboxScanItem(scanner interface{ Scan(dest ...any) error }) (*MemoryVectorOutboxItem, error) {
	item := &MemoryVectorOutboxItem{}
	var documentJSON, leaseOwner, lastError *string
	var retryAfter, leaseUntil *time.Time
	if err := scanner.Scan(
		&item.ID, &item.ContractVersion, &item.OperationKey, &item.Operation,
		&item.ChatSessionID, &item.SourceRevision, &item.DocumentID,
		&documentJSON, &item.EmbeddingReady, &item.RequiredSourceState,
		&item.Status, &item.Attempts, &retryAfter, &leaseOwner,
		&leaseUntil, &lastError, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return nil, err
	}
	item.DocumentJSON = d1DerefString(documentJSON)
	if retryAfter != nil {
		item.RetryAfter = *retryAfter
	}
	item.LeaseOwner = d1DerefString(leaseOwner)
	if leaseUntil != nil {
		item.LeaseUntil = *leaseUntil
	}
	item.LastError = d1DerefString(lastError)
	return item, nil
}

// d1OutboxEnqueue validates and writes one outbox operation, resolving the
// UNIQUE operation_key collision against the stored row before the insert.
//
// The comparison set is the reference's, unchanged: operation, session,
// revision, document id, and — for an upsert only — the document payload.
// embedding_ready and required_source_state are part of it too, because a
// replay that arrives with a different embedding readiness is a different
// operation that happens to hash to the same key, and keeping the stored row
// would silently drop the caller's work. The document comparison is skipped
// for a delete because a delete's document_json is an audit note, not the
// payload being published.
func d1OutboxEnqueue(ctx context.Context, s *d1Store, item *MemoryVectorOutboxItem) (bool, error) {
	statement, err := d1EnqueueMemoryVectorStatement(item)
	if err != nil {
		return false, err
	}
	// The reference normalizes the item in place before inserting, and a caller
	// that inspects the item afterwards reads the values that were persisted.
	// The shared builder validates without mutating, so the same defaults are
	// applied here; they are idempotent, so running both is harmless.
	item.ContractVersion = firstNonEmptyString(item.ContractVersion, MemoryVectorOutboxContract)
	item.RequiredSourceState = firstNonEmptyString(item.RequiredSourceState, d1OutboxRequiredSourceStateDefault(item.Operation))
	item.Status = firstNonEmptyString(item.Status, "pending")

	documentJSON := strings.TrimSpace(item.DocumentJSON)
	var existingOperation, existingSession, existingRevision, existingDocumentID string
	var existingJSON, existingRequiredSourceState string
	var existingEmbeddingReady bool
	err = s.conn.QueryRow(ctx, `
		SELECT operation, chat_session_id, source_revision, document_id,
		       COALESCE(document_json, ''), embedding_ready, required_source_state
		FROM memory_vector_outbox
		WHERE operation_key = ?`, item.OperationKey).Scan(
		&existingOperation, &existingSession, &existingRevision, &existingDocumentID,
		&existingJSON, &existingEmbeddingReady, &existingRequiredSourceState)
	if err == nil {
		if existingOperation != item.Operation ||
			existingSession != item.ChatSessionID ||
			existingRevision != item.SourceRevision ||
			existingDocumentID != item.DocumentID ||
			(existingOperation == "upsert" && strings.TrimSpace(existingJSON) != documentJSON) ||
			existingEmbeddingReady != item.EmbeddingReady ||
			existingRequiredSourceState != item.RequiredSourceState {
			return false, errors.New("memory vector operation idempotency conflict")
		}
		return false, nil
	}
	if !errors.Is(err, errD1NoRows) {
		return false, err
	}
	if _, err := s.conn.Exec(ctx, statement.SQL, statement.Args...); err != nil {
		return false, err
	}
	return true, nil
}

// EnqueueMemoryVectorOperation records one vector operation, or resolves the
// replay against the operation that is already recorded.
//
// The write mutex is taken before the collision check so the check and the
// insert are one critical section: without it a concurrent replay could read
// "no row", both writers would insert, and the loser's document would depend on
// which batch committed first.
func (s *d1Store) EnqueueMemoryVectorOperation(ctx context.Context, item *MemoryVectorOutboxItem) (bool, error) {
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()
	return d1OutboxEnqueue(ctx, s, item)
}

// d1OutboxEnqueueAdmissionVectorOperation is the lease-aware enqueue the
// admission path uses, and it exists here because it is the only caller that
// honours store.WithMemoryAdmissionVectorReplay.
//
// Outside a replay it is exactly the plain enqueue. Inside a replay it takes a
// different branch for an operation key that already exists, and that branch is
// the reason this function is not just d1OutboxEnqueue:
//
//   - the stored identity must match the request exactly, or the key is
//     describing two different things and this is a conflict, not a refresh;
//   - the source-revision fence still applies, so a replay may not resurrect an
//     operation whose source the user has rolled back;
//   - a LIVE lease is refused with ErrMemoryReprocessingLeased, because the
//     replay is about to overwrite document_json and reset the attempt counter
//     of a document another worker is applying right now;
//   - an operation that already has a NEWER sibling for the same document is
//     refused, because refreshing the older one would resurrect a superseded
//     payload in front of the newer operation that replaced it.
//
// The refresh itself resets attempts and clears the retry and lease columns.
// That is the point of the administrative replay: it re-queues a document that
// a partial failure left stuck, and leaving the old attempt count would make the
// next failure look like the hundredth.
//
// It returns true when it refreshed an existing row, which is what the caller
// counts as "queued", exactly as the reference does.
//
// The caller must hold s.memoryDerivationWriteMu across this call, the way the
// reference holds its transaction across the same statements. The refresh is a
// read followed by a write on the same row, so without that critical section two
// replays could both read "no live lease" and both overwrite the payload.
func d1OutboxEnqueueAdmissionVectorOperation(ctx context.Context, s *d1Store, item *MemoryVectorOutboxItem) (bool, error) {
	if !memoryAdmissionVectorReplayFromContext(ctx).Refresh {
		return d1OutboxEnqueue(ctx, s, item)
	}
	if item == nil || strings.TrimSpace(item.OperationKey) == "" {
		return false, errors.New("invalid memory vector outbox item")
	}
	documentJSON := strings.TrimSpace(item.DocumentJSON)
	if documentJSON != "" && !json.Valid([]byte(documentJSON)) {
		return false, errors.New("invalid memory vector document JSON")
	}
	var outboxID int64
	var operation, sid, revision, documentID, requiredSourceState, status, sourceState string
	var leaseUntil *time.Time
	err := s.conn.QueryRow(ctx, `
		SELECT o.id, o.operation, o.chat_session_id, o.source_revision,
		       o.document_id, o.required_source_state, o.status,
		       o.lease_until, s.lifecycle_state
		FROM memory_vector_outbox o
		JOIN memory_source_revisions s
		  ON s.chat_session_id = o.chat_session_id
		 AND s.source_revision = o.source_revision
		WHERE o.operation_key = ?`, item.OperationKey).Scan(
		&outboxID, &operation, &sid, &revision, &documentID,
		&requiredSourceState, &status, &leaseUntil, &sourceState)
	if errors.Is(err, errD1NoRows) {
		return d1OutboxEnqueue(ctx, s, item)
	}
	if err != nil {
		return false, err
	}
	if operation != item.Operation || sid != item.ChatSessionID ||
		revision != item.SourceRevision || documentID != item.DocumentID ||
		requiredSourceState != item.RequiredSourceState {
		return false, errors.New("memory vector operation idempotency conflict")
	}
	if (requiredSourceState == "active" && sourceState != "active") ||
		(requiredSourceState == "inactive" && sourceState == "active") {
		return false, ErrSourceRevisionStale
	}
	now := time.Now().UTC()
	if status == "leased" && leaseUntil != nil && leaseUntil.After(now) {
		return false, ErrMemoryReprocessingLeased
	}
	var newer int
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM memory_vector_outbox
		WHERE chat_session_id = ? AND document_id = ? AND id > ?`,
		sid, documentID, outboxID).Scan(&newer); err != nil {
		return false, err
	}
	if newer > 0 {
		return false, errors.New("memory vector refresh is superseded by a newer operation")
	}
	targetStatus := firstNonEmptyString(item.Status, "pending")
	// The lease guard is repeated in the statement so the refresh cannot land
	// on a row another worker leased between the read above and this write.
	affected, err := s.conn.Exec(ctx, `
		UPDATE memory_vector_outbox
		SET document_json = ?, embedding_ready = ?, status = ?, attempts = 0,
		    retry_after = NULL, lease_owner = NULL, lease_until = NULL,
		    last_error = NULL, updated_at = ?
		WHERE operation_key = ?
		  AND (status <> 'leased' OR lease_until IS NULL OR lease_until <= ?)`,
		d1NullableString(documentJSON), d1BoolValue(item.EmbeddingReady), targetStatus,
		d1TimeValue(nonZeroTime(item.UpdatedAt)), item.OperationKey, d1TimeValue(now))
	if err != nil {
		return false, err
	}
	if affected != 1 {
		return false, ErrMemoryReprocessingLeased
	}
	return true, nil
}

// ClaimMemoryVectorOperations leases the oldest eligible operation, and with it
// the sibling group the reference bundles into the same lease.
func (s *d1Store) ClaimMemoryVectorOperations(
	ctx context.Context,
	leaseOwner string,
	now time.Time,
	leaseDuration time.Duration,
) ([]*MemoryVectorOutboxItem, error) {
	return s.d1OutboxClaim(ctx, leaseOwner, now, leaseDuration, "")
}

// ClaimMemoryVectorOperationsByOperation is the same claim restricted to one
// operation lane, so the bounded authority worker can reserve fair service for
// deletes instead of starving them behind a continuous upsert backlog.
//
// An unknown lane is refused rather than silently claiming everything: a typo in
// the worker's lane name would otherwise read as "claim the whole queue".
func (s *d1Store) ClaimMemoryVectorOperationsByOperation(
	ctx context.Context,
	leaseOwner string,
	now time.Time,
	leaseDuration time.Duration,
	operation string,
) ([]*MemoryVectorOutboxItem, error) {
	operation = strings.ToLower(strings.TrimSpace(operation))
	if operation != "upsert" && operation != "delete" {
		return nil, fmt.Errorf("invalid vector outbox operation lane")
	}
	return s.d1OutboxClaim(ctx, leaseOwner, now, leaseDuration, operation)
}

// d1OutboxClaim is the reference claim statement for statement.
//
// The order of the four phases is the contract:
//
//  1. the fence UPDATE retires every queued operation whose required source
//     state no longer matches its revision, so those rows stop blocking later
//     operations of the same document;
//  2. the seed SELECT picks the single oldest eligible operation, honouring the
//     per-document causal order and the session-migration hold;
//  3. the sibling SELECT widens the lease to the group that shares the seed's
//     embedding dependency or its delete lane — one round trip of embedding
//     work, or one batch of deletions, not a trickle of single operations;
//  4. the lease UPDATEs go out as ONE Batch, so a failure cannot leave half a
//     lease group marked leased with the other half still claimable.
func (s *d1Store) d1OutboxClaim(
	ctx context.Context,
	leaseOwner string,
	now time.Time,
	leaseDuration time.Duration,
	operation string,
) ([]*MemoryVectorOutboxItem, error) {
	if strings.TrimSpace(leaseOwner) == "" || leaseDuration <= 0 {
		return nil, fmt.Errorf("invalid vector outbox lease")
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	now = nonZeroTime(now)
	// The multi-table UPDATE becomes an id subquery over the same join. The
	// subquery is materialized before the UPDATE runs, so a row is never
	// re-evaluated against its own new value.
	if _, err := s.conn.Exec(ctx, `
		UPDATE memory_vector_outbox
		SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL,
		    last_error = ?, updated_at = ?
		WHERE status IN ('pending', 'leased', 'retryable', 'needs_embedding')
		  AND id IN (
		    SELECT fenced.id
		    FROM memory_vector_outbox fenced
		    JOIN memory_source_revisions s ON s.source_revision = fenced.source_revision
		    WHERE (fenced.required_source_state = 'active' AND s.lifecycle_state <> 'active')
		       OR (fenced.required_source_state = 'inactive' AND s.lifecycle_state = 'active')
		  )`, d1OutboxFenceRejectedLastError, d1TimeValue(now)); err != nil {
		return nil, err
	}

	seed, err := d1OutboxSelectForLease(ctx, s, now, operation)
	if err != nil {
		return nil, err
	}
	items := []*MemoryVectorOutboxItem{seed}
	// Only two seeds are worth widening: a delete, and an upsert still waiting
	// for its embedding. A ready upsert is already a single finished vector to
	// push, so batching its siblings would only delay them behind a lease they
	// did not need.
	if seed.Operation == "delete" || (seed.Operation == "upsert" && !seed.EmbeddingReady) {
		siblings, err := d1OutboxSelectSiblingsForLease(ctx, s, seed, now)
		if err != nil {
			return nil, err
		}
		items = append(items, siblings...)
	}

	// The lease deadline is computed once in Go, exactly as the reference does,
	// so every member of the group is leased to the SAME instant. Computing it
	// per row would make a 128-row delete group span a measurable window in
	// which the earliest rows have already expired.
	leaseUntilTime := now.Add(leaseDuration)
	leaseUntil := d1TimeValue(leaseUntilTime)
	statements := make([]D1Statement, 0, len(items))
	for _, claimed := range items {
		claimed.LeaseOwner = leaseOwner
		claimed.LeaseUntil = leaseUntilTime
		claimed.Status = "leased"
		claimed.Attempts++
		statements = append(statements, D1Statement{
			SQL: `
			UPDATE memory_vector_outbox
			SET status = 'leased', attempts = attempts + 1, lease_owner = ?,
			    lease_until = ?, updated_at = ?
			WHERE id = ?`,
			Args: []any{leaseOwner, leaseUntil, d1TimeValue(now), claimed.ID},
		})
	}
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return nil, err
	}
	return items, nil
}

// d1OutboxSelectForLease is the seed SELECT. The lane clause is appended rather
// than templated into the predicate so an unfiltered claim is byte-identical to
// the reference's statement.
func d1OutboxSelectForLease(
	ctx context.Context,
	s *d1Store,
	now time.Time,
	operation string,
) (*MemoryVectorOutboxItem, error) {
	operationClause := ""
	args := []any{d1TimeValue(now), d1TimeValue(now), d1TimeValue(now)}
	if operation != "" {
		operationClause = " AND o.operation = ?"
		args = append(args, operation)
	}
	item, err := d1OutboxScanItem(s.conn.QueryRow(ctx, `
		SELECT o.id, o.contract_version, o.operation_key, o.operation,
		       o.chat_session_id, o.source_revision, o.document_id,
		       o.document_json, o.embedding_ready, o.required_source_state,
		       o.status, o.attempts, o.retry_after, o.lease_owner,
		       o.lease_until, o.last_error, o.created_at, o.updated_at
		FROM memory_vector_outbox o
		JOIN memory_source_revisions s ON s.source_revision = o.source_revision
		WHERE (
		    (
		      o.embedding_ready = 1
		      AND o.status IN ('pending', 'retryable')
		      AND (o.retry_after IS NULL OR o.retry_after < ?)
		    )
		    OR (
		      o.operation = 'upsert'
		      AND o.embedding_ready = 0
		      AND o.status IN ('needs_embedding', 'retryable')
		      AND (o.retry_after IS NULL OR o.retry_after < ?)
		    )
		    OR (o.status = 'leased' AND o.lease_until < ?)
		  )
		  AND (
		    (o.required_source_state = 'active' AND s.lifecycle_state = 'active')
		    OR (o.required_source_state = 'inactive' AND s.lifecycle_state <> 'active')
		  )
		  AND NOT EXISTS (
		    SELECT 1
		    FROM session_migration_locks migration_lock
		    WHERE migration_lock.source_session_id = o.chat_session_id
		      AND migration_lock.locked = 1
		      AND migration_lock.unlocked_at IS NULL
		  )
		  AND NOT EXISTS (
		    SELECT 1
		    FROM memory_vector_outbox prior
		    WHERE prior.chat_session_id = o.chat_session_id
		      AND prior.document_id = o.document_id
		      AND prior.id < o.id
		      AND prior.status IN ('pending', 'leased', 'retryable', 'needs_embedding')
		  )`+operationClause+`
		ORDER BY o.created_at, o.id
		LIMIT 1`, args...))
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

// d1OutboxSelectSiblingsForLease widens a seed lease to its group.
//
// The two shapes are not interchangeable and the difference is load-bearing:
//
//   - the upsert sibling is scoped to the seed's source revision AND session.
//     Those operations are all waiting on the same embedding round trip, so
//     leasing them together is the point of the batch size of 32: one embedding
//     request amortizes over 31 documents instead of 1.
//   - the delete sibling is scoped to the session only, and it spans revisions.
//     Deletes exist precisely to clean up after a revision that is no longer
//     active, so restricting them to one revision would leave every document
//     from an older rollback stranded forever.
//
// Both shapes keep the two NOT EXISTS guards: a migration-locked session is
// not claimable at all, and a document still has an earlier open operation, so
// a later one must not overtake it.
func d1OutboxSelectSiblingsForLease(
	ctx context.Context,
	s *d1Store,
	seed *MemoryVectorOutboxItem,
	now time.Time,
) ([]*MemoryVectorOutboxItem, error) {
	where := `o.source_revision = ?
		  AND o.chat_session_id = ?
		  AND o.id <> ?
		  AND o.operation = 'upsert'
		  AND o.embedding_ready = 0
		  AND (
		    (
		      o.status IN ('needs_embedding', 'retryable')
		      AND (o.retry_after IS NULL OR o.retry_after < ?)
		    )
		    OR (o.status = 'leased' AND o.lease_until < ?)
		  )
		  AND o.required_source_state = 'active'
		  AND s.lifecycle_state = 'active'`
	args := []any{seed.SourceRevision, seed.ChatSessionID, seed.ID, d1TimeValue(now), d1TimeValue(now)}
	limit := memoryVectorUpsertClaimBatchSize - 1
	if seed.Operation == "delete" {
		where = `o.chat_session_id = ?
		  AND o.id <> ?
		  AND o.operation = 'delete'
		  AND o.embedding_ready = 1
		  AND (
		    (
		      o.status IN ('pending', 'retryable')
		      AND (o.retry_after IS NULL OR o.retry_after < ?)
		    )
		    OR (o.status = 'leased' AND o.lease_until < ?)
		  )
		  AND o.required_source_state = 'inactive'
		  AND s.lifecycle_state <> 'active'`
		args = []any{seed.ChatSessionID, seed.ID, d1TimeValue(now), d1TimeValue(now)}
		limit = memoryVectorDeleteClaimBatchSize - 1
	}
	rows, err := s.conn.Query(ctx, `
		SELECT o.id, o.contract_version, o.operation_key, o.operation,
		       o.chat_session_id, o.source_revision, o.document_id,
		       o.document_json, o.embedding_ready, o.required_source_state,
		       o.status, o.attempts, o.retry_after, o.lease_owner,
		       o.lease_until, o.last_error, o.created_at, o.updated_at
		FROM memory_vector_outbox o
		JOIN memory_source_revisions s ON s.source_revision = o.source_revision
		WHERE `+where+`
		  AND NOT EXISTS (
		    SELECT 1
		    FROM session_migration_locks migration_lock
		    WHERE migration_lock.source_session_id = o.chat_session_id
		      AND migration_lock.locked = 1
		      AND migration_lock.unlocked_at IS NULL
		  )
		  AND NOT EXISTS (
		    SELECT 1
		    FROM memory_vector_outbox prior
		    WHERE prior.chat_session_id = o.chat_session_id
		      AND prior.document_id = o.document_id
		      AND prior.id < o.id
		      AND prior.status IN ('pending', 'leased', 'retryable', 'needs_embedding')
		  )
		ORDER BY o.created_at, o.id
		LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []*MemoryVectorOutboxItem{}
	for rows.Next() {
		item, err := d1OutboxScanItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// CompleteMemoryVectorOperation retires one leased operation successfully.
func (s *d1Store) CompleteMemoryVectorOperation(
	ctx context.Context,
	outboxID int64,
	leaseOwner string,
	now time.Time,
) error {
	return s.d1OutboxFinish(ctx, outboxID, leaseOwner, now, time.Time{}, false, false, "")
}

// DeferMemoryVectorVisibility releases an acknowledged upsert while Vectorize
// propagates it. retryable plus the explicit marker is a durable pending state
// that fits the existing v1 schema; attempts is restored because no provider
// operation failed.
func (s *d1Store) DeferMemoryVectorVisibility(ctx context.Context, outboxID int64, leaseOwner string, now, retryAfter time.Time, documentJSON string) error {
	now = nonZeroTime(now)
	if retryAfter.Before(now) {
		retryAfter = now
	}
	affected, err := s.conn.Exec(ctx, `
		UPDATE memory_vector_outbox
		SET status = 'retryable', attempts = CASE WHEN attempts > 0 THEN attempts - 1 ELSE 0 END,
		    document_json = CASE WHEN ? <> '' THEN ? ELSE document_json END,
		    retry_after = ?, lease_owner = NULL, lease_until = NULL, last_error = ?, updated_at = ?
		WHERE id = ? AND status = 'leased' AND lease_owner = ?`,
		documentJSON, documentJSON, d1TimeValue(retryAfter), MemoryVectorVisibilityPendingMarker, d1TimeValue(now), outboxID, leaseOwner)
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrLeaseExpired
	}
	return nil
}

// FailMemoryVectorOperation returns a leased operation to the queue, either as
// retryable with a wake time or as permanently failed.
func (s *d1Store) FailMemoryVectorOperation(
	ctx context.Context,
	outboxID int64,
	leaseOwner string,
	now time.Time,
	retryAfter time.Time,
	permanent bool,
	failure string,
) error {
	return s.d1OutboxFinish(ctx, outboxID, leaseOwner, now, retryAfter, true, permanent, failure)
}

// d1OutboxFinish is the shared completion and failure path.
//
// The pre-read is the authority, and it is checked in this order because each
// step refines the previous one:
//
//   - no row at all, or a join that found no source revision, is
//     ErrLeaseExpired: the worker is addressing work this store does not have.
//   - a row the fence already retired is ErrSourceRevisionStale, NOT
//     ErrLeaseExpired. A worker whose document was retracted must be told the
//     document was retracted; reporting "your lease expired" would send it back
//     to claim the next operation and it would never learn why its work was
//     thrown away.
//   - anything other than a live lease held by this owner is
//     ErrLeaseExpired. The lease comparison is exact: an expired lease is not
//     honoured, so a slow worker whose lease was stolen cannot overwrite the
//     result the new owner is about to write.
//   - only then is the source fence re-checked. A lease taken while the source
//     was current can outlive a rollback, and completing it would publish a
//     document the user has already discarded. The row is marked stale_rejected
//     AND ErrSourceRevisionStale is returned, so the rejection is durable and
//     the next claim cannot pick it up.
func (s *d1Store) d1OutboxFinish(
	ctx context.Context,
	outboxID int64,
	leaseOwner string,
	now time.Time,
	retryAfter time.Time,
	failed bool,
	permanent bool,
	failure string,
) error {
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	now = nonZeroTime(now)
	var currentStatus, requiredState, sourceState string
	var currentOwner *string
	var leaseUntil *time.Time
	err := s.conn.QueryRow(ctx, `
		SELECT o.status, o.lease_owner, o.lease_until, o.required_source_state,
		       s.lifecycle_state
		FROM memory_vector_outbox o
		JOIN memory_source_revisions s ON s.source_revision = o.source_revision
		WHERE o.id = ?`, outboxID).Scan(
		&currentStatus, &currentOwner, &leaseUntil, &requiredState, &sourceState)
	if errors.Is(err, errD1NoRows) {
		return ErrLeaseExpired
	}
	if err != nil {
		return err
	}
	if currentStatus == "stale_rejected" {
		return ErrSourceRevisionStale
	}
	if currentStatus != "leased" || leaseUntil == nil ||
		d1DerefString(currentOwner) != leaseOwner || leaseUntil.Before(now) {
		return ErrLeaseExpired
	}
	sourceFenceSatisfied := (requiredState == "active" && sourceState == "active") ||
		(requiredState == "inactive" && sourceState != "active")
	if !sourceFenceSatisfied {
		if _, err := s.conn.Exec(ctx, `
			UPDATE memory_vector_outbox
			SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL,
			    last_error = ?, updated_at = ?
			WHERE id = ?`, d1OutboxFenceRejectedLastError, d1TimeValue(now), outboxID); err != nil {
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
	_, err = s.conn.Exec(ctx, `
		UPDATE memory_vector_outbox
		SET status = ?, retry_after = ?, lease_owner = NULL, lease_until = NULL,
		    last_error = ?, updated_at = ?
		WHERE id = ?`,
		status, d1NullableTime(retryAfter), d1NullableString(failure), d1TimeValue(now), outboxID)
	return err
}

// CoalesceInactiveMemoryVectorDeleteOperations retires duplicate deletes for one
// session and reports how many rows it retired.
//
// A rollback can queue the same document delete several times — once per
// re-invalidation, once per migration pass — and every duplicate is a separate
// index round trip for a document that is already gone. The rule is causal
// order, not recency: the EARLIEST open delete for a (revision, document) pair
// survives and every later one is retired, because the earlier operation is the
// one whose position in the document's history the index has to respect.
func (s *d1Store) CoalesceInactiveMemoryVectorDeleteOperations(
	ctx context.Context,
	chatSessionID string,
	now time.Time,
) (int64, error) {
	chatSessionID = strings.TrimSpace(chatSessionID)
	if chatSessionID == "" {
		return 0, fmt.Errorf("chat session id is required")
	}
	now = nonZeroTime(now)
	var total, afterID int64
	for {
		staleRejected, err := s.d1OutboxCoalesceDeleteBatch(ctx, chatSessionID, now, &afterID)
		if err != nil {
			return total, err
		}
		total += staleRejected
		// A short batch means the backlog is drained. The alternative — looping
		// until an empty batch — would re-scan the historical outbox once more
		// after every full batch for no possible gain.
		if staleRejected < memoryVectorDeleteCoalesceBatchSize {
			return total, nil
		}
	}
}

// d1OutboxCoalesceDeleteBatch retires one batch of duplicate deletes.
//
// The mutex is taken per batch rather than per call, so a large historical
// backlog cannot monopolize the writer for the whole pass: the reference takes
// the same decision, and the alternative is a single D1 binding starved of
// every other derivation write for the length of a full-table cleanup.
//
// The id subquery keeps the candidate set frozen before the UPDATE, which is
// what the reference's FOR UPDATE did: without it a row retired by this batch
// could be re-selected by the next one and counted twice.
func (s *d1Store) d1OutboxCoalesceDeleteBatch(
	ctx context.Context,
	chatSessionID string,
	now time.Time,
	afterID *int64,
) (int64, error) {
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	stamp := d1TimeValue(now)
	rows, err := s.conn.Query(ctx, `
		SELECT o.id
		FROM memory_vector_outbox o
		JOIN memory_source_revisions s
		  ON s.chat_session_id = o.chat_session_id
		 AND s.source_revision = o.source_revision
		WHERE o.chat_session_id = ? AND o.id > ?
		  AND o.operation = 'delete'
		  AND o.required_source_state = 'inactive'
		  AND s.lifecycle_state <> 'active'
		  AND o.status IN ('pending', 'retryable', 'leased', 'needs_embedding')
		  AND (o.status <> 'leased' OR o.lease_until IS NULL OR o.lease_until <= ?)
		  AND NOT EXISTS (
		    SELECT 1
		    FROM memory_vector_outbox active_lease
		    WHERE active_lease.chat_session_id = o.chat_session_id
		      AND active_lease.source_revision = o.source_revision
		      AND active_lease.document_id = o.document_id
		      AND active_lease.operation = 'delete'
		      AND active_lease.required_source_state = 'inactive'
		      AND active_lease.status = 'leased'
		      AND active_lease.lease_until > ?
		  )
		  AND (
		    SELECT MIN(keep.id)
		    FROM memory_vector_outbox keep
		    WHERE keep.chat_session_id = o.chat_session_id
		      AND keep.source_revision = o.source_revision
		      AND keep.document_id = o.document_id
		      AND keep.operation = 'delete'
		      AND keep.required_source_state = 'inactive'
		      AND (
		        (keep.status IN ('pending', 'retryable') AND (keep.retry_after IS NULL OR keep.retry_after < ?))
		        OR (keep.status = 'leased' AND keep.lease_until < ?)
		      )
		      AND (
		        o.status = 'needs_embedding'
		        OR (o.status IN ('pending', 'retryable') AND o.retry_after IS NOT NULL AND o.retry_after >= ?)
		        OR (o.status = 'leased' AND (o.lease_until IS NULL OR o.lease_until >= ?))
		        OR keep.created_at < o.created_at
		        OR (keep.created_at = o.created_at AND keep.id < o.id)
		      )
		  ) IS NOT NULL
		ORDER BY o.id
		LIMIT ?`,
		chatSessionID, *afterID, stamp, stamp, stamp, stamp, stamp, stamp, memoryVectorDeleteCoalesceBatchSize)
	if err != nil {
		return 0, err
	}
	ids := make([]int64, 0, memoryVectorDeleteCoalesceBatchSize)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids)+3)
	args = append(args, d1OutboxDuplicateDeleteLastError, stamp)
	for index, id := range ids {
		placeholders[index] = "?"
		args = append(args, id)
	}
	args = append(args, stamp)
	affected, err := s.conn.Exec(ctx, `
		UPDATE memory_vector_outbox
		SET status = 'stale_rejected', retry_after = NULL,
		    lease_owner = NULL, lease_until = NULL, last_error = ?, updated_at = ?
		WHERE id IN (`+strings.Join(placeholders, ",")+`)
		  AND status IN ('pending', 'retryable', 'leased', 'needs_embedding')
		  AND (status <> 'leased' OR lease_until IS NULL OR lease_until <= ?)`, args...)
	if err != nil {
		return 0, err
	}
	// The cursor advances to the last row this batch CONSIDERED, not the last
	// it changed. A row whose status a concurrent writer already moved must not
	// make the pass re-read the range before it.
	*afterID = ids[len(ids)-1]
	return affected, nil
}
