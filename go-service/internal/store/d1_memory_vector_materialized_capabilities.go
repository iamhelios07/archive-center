package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// D1 materialized memory-vector completion.
//
// CompleteMemoryVectorMaterializedOperation is the one place in the vector
// pipeline where the external index and the canonical database are made to agree,
// and it is the reason it exists as a separate capability from the plain
// outbox completion. A worker has already pushed the document into the vector
// index when it calls this; the two writes below are the canonical half of that
// same operation:
//
//	UPDATE memories                SET embedding, embedding_model
//	UPDATE memory_vector_outbox    SET status = completed
//
// The first is the vector stored on the canonical row. The second is the queue
// entry that says the document is published. An outbox row marked completed
// without the first write is an index that silently lost a document: the queue
// believes the work is done, nothing will ever retry it, and the only symptom is
// a memory that never becomes searchable. That is why the two writes are one
// atomic unit here and are not two calls.
//
// What the completion verifies, in the reference order
//
//  1. The materialization argument is well formed. This happens before any
//     statement and before the writer is taken, because a caller that hands over
//     a row id that does not exist must be told so without a rollback to undo.
//
//  2. The outbox operation is still a LIVE LEASE held by this owner, and it is
//     an UPSERT. A delete lane operation has no memories row to converge, so
//     completing one through this path would write a vector onto a document the
//     user asked to remove. A worker whose lease expired, or whose row was
//     already retired, is told ErrLeaseExpired so it stops and re-claims rather
//     than overwriting the result the new owner is writing.
//
//  3. The materialization names the SAME document as the leased operation. This
//     is the check that stops a worker from publishing one document vector onto
//     another documents row: without it, a mismatched (session, revision,
//     document) triple would satisfy every other guard and silently overwrite a
//     memory the caller never meant to touch.
//
//  4. The source revision is still ACTIVE, and the operation still REQUIRES an
//     active source. Note that this is a conjunction, not the disjunction the
//     plain outbox finish uses: a materialized completion is by definition an
//     upsert, so a row that only satisfies the inactive arm is a delete wearing
//     an upserts lease and must be refused rather than materialised. The refusal
//     is durable: the row is marked stale_rejected before the error is returned,
//     so the next claim cannot pick it up.
//
//  5. The memory row exists, and its turn_index equals the source revisions
//     turn_index. This is the cross-table verification that makes the completion
//     a verification rather than a write. A memory row whose turn has drifted
//     from the revision that produced it is a row the caller did not mean: the
//     row id was resolved from the outbox document, and if the turn disagrees
//     then the document now points at a different memory than the one that was
//     leased.
//
// FOR UPDATE, and what replaced it
//
// The reference holds two row locks across all of the above: the outbox row
// (from the first probe) and the memories row (from the turn probe), and the two
// UPDATEs run in the same transaction. D1 has no row locks. What is used
// instead is the same substitute the outbox capability already documents:
//
//   - The probes are plain SELECTs, and they still run. They are not dead
//     weight: every one of them branches on its result and decides which error
//     the caller gets, which is the part of the lock that was carrying
//     information. Only the mutual exclusion they also provided is gone.
//   - The two UPDATEs go out as ONE D1Conn.Batch, which is the D1 transaction
//     boundary. This is the guarantee that actually mattered for the capability:
//     the vector and the queue entry commit together or not at all.
//   - The window BETWEEN the probes and the batch is closed inside one process
//     by s.memoryDerivationWriteMu, the same mutex the outbox claim, the plain
//     outbox finish, and the source-revision fence all take. No in-process
//     writer can move the outbox row or delete the memory row in between.
//
// The residual gap is the one every D1 write capability in this provider
// records rather than hides: across Cloudflare Containers there is no shared
// lock, so a second Container can act on the same outbox row or memory row in
// the window between the probes and the batch. D1 is single-writer, so that
// second writer is serialised against the batch rather than interleaved with it,
// and both writers act on the same rows by id, so the outcome is a lost update
// that the next claim re-drives. It is not a completion of a document that was
// never materialised, because the batch is atomic.
//
// The statements themselves are the reference statements. No predicate was
// moved into Go and no guard was added to an UPDATE that the reference does not
// carry: adding a lease predicate to the completing UPDATE would look safer but
// would make D1 refuse a completion MariaDB performs, which is a divergence in
// the other direction.

var _ MemoryVectorMaterializedCompletionStore = (*d1Store)(nil)

// d1VectorMaterializedOperationProbe reads the leased operation together with the
// lifecycle facts the completion decides on.
//
// The join carries BOTH the session and the revision. That is the reference join
// and it is not redundant: the outbox table is keyed on the revision alone, so
// dropping chat_session_id would let an operation whose stored session disagrees
// with its revision still join, and a completion would then materialise a
// vector into a session the operation was never queued for. The sibling outbox
// capability joins on source_revision alone, and this statement must not be made
// to look like it.
//
// lease_owner and lease_until are read through pointer destinations because both
// columns are nullable, and NULL is not the same answer as an empty string here:
// a leased row with a NULL lease_until is a lease with no deadline, which the
// reference refuses (a NullTime that is not Valid). turn_index is NOT NULL in
// the canonical schema on both tables, so it scans as a plain integer.
const d1VectorMaterializedOperationProbe = `
	SELECT o.status, o.operation, o.chat_session_id, o.source_revision,
	       o.document_id, o.lease_owner, o.lease_until,
	       o.required_source_state, s.lifecycle_state, s.turn_index
	FROM memory_vector_outbox o
	JOIN memory_source_revisions s
	  ON s.chat_session_id = o.chat_session_id
	 AND s.source_revision = o.source_revision
	WHERE o.id = ?`

// d1VectorMaterializedMemoryTurnProbe is the cross-table verification read. It
// runs AFTER the source fence, never before: a document the user has rolled back
// must be reported as retracted, not as a missing memory row.
const d1VectorMaterializedMemoryTurnProbe = `
	SELECT turn_index
	FROM memories
	WHERE id = ? AND chat_session_id = ?`

// d1VectorMaterializedStaleReject retires an operation whose source revision no
// longer supports it. retry_after is deliberately absent from the SET list,
// matching the reference: a retired operation is not waiting for a wake time,
// and leaving a stale one behind would make it look schedulable to a reader.
const d1VectorMaterializedStaleReject = `
	UPDATE memory_vector_outbox
	SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL,
	    last_error = ?, updated_at = ?
	WHERE id = ?`

// d1VectorMaterializedStoreVector is the canonical half of the completion. There
// is no updated_at on memories in either schema, so the write leaves the row
// otherwise untouched: memories is the append-only canonical table and the
// embedding columns are the only thing a vector operation is allowed to fill in.
const d1VectorMaterializedStoreVector = `
	UPDATE memories
	SET embedding = ?, embedding_model = ?
	WHERE id = ? AND chat_session_id = ?`

// d1VectorMaterializedCompleteOutbox is the queue half of the completion. It is
// unconditional on status and lease for the same reason the reference UPDATE is:
// the probe above already established that this owner holds a live lease, and
// re-stating the lease here would reject a completion MariaDB performs. Every
// column the claim set is cleared here, because a completed row that still
// carries a lease owner is a row the next claim reads as ambiguous.
const d1VectorMaterializedCompleteOutbox = `
	UPDATE memory_vector_outbox
	SET status = 'completed', retry_after = NULL,
	    lease_owner = NULL, lease_until = NULL, last_error = NULL, updated_at = ?
	WHERE id = ?`

// d1VectorMaterializedOperation is the probe result. The nullable lease columns
// stay nullable here rather than being flattened: the difference between a row
// with no lease owner and a row whose lease owner is the empty string decides
// whether the caller owns the operation, so flattening it in the reader would
// hide a defect the writer has to reject.
type d1VectorMaterializedOperation struct {
	status             string
	operation          string
	chatSessionID      string
	sourceRevision     string
	documentID         string
	leaseOwner         *string
	leaseUntil         *time.Time
	requiredSourceStat string
	lifecycleState     string
	sourceTurn         int
}

// d1VectorMaterializedReadOperation issues the probe and maps an empty result the
// way the reference maps sql.ErrNoRows: the worker is addressing work this
// store does not have, which is a lease it no longer owns.
//
// This is deliberately NOT ErrNotFound. ErrNotFound is the read-path identity
// that routes translate into a 404 for a document the caller named; here the
// caller named an outbox id that a worker is mid-flight on, and reporting "not
// found" would send a repair tool looking for a document that may exist.
func d1VectorMaterializedReadOperation(
	ctx context.Context,
	s *d1Store,
	outboxID int64,
) (d1VectorMaterializedOperation, error) {
	var found d1VectorMaterializedOperation
	err := s.conn.QueryRow(ctx, d1VectorMaterializedOperationProbe, outboxID).Scan(
		&found.status, &found.operation, &found.chatSessionID, &found.sourceRevision,
		&found.documentID, &found.leaseOwner, &found.leaseUntil,
		&found.requiredSourceStat, &found.lifecycleState, &found.sourceTurn,
	)
	if errors.Is(err, errD1NoRows) {
		return d1VectorMaterializedOperation{}, ErrLeaseExpired
	}
	if err != nil {
		return d1VectorMaterializedOperation{}, err
	}
	return found, nil
}

// CompleteMemoryVectorMaterializedOperation stores the verified public vector on
// its canonical memory row and completes the outbox operation that produced it,
// as one atomic D1 batch.
//
// The verification is the capability. Every guard below refuses the completion
// rather than writing a vector the index and the database would then disagree
// about, and the two refusals that a worker can act on keep the reference's
// exact error identities: ErrLeaseExpired means stop and re-claim, and
// ErrSourceRevisionStale means the document was retracted and the caller should
// drop it from the vector index.
func (s *d1Store) CompleteMemoryVectorMaterializedOperation(
	ctx context.Context,
	outboxID int64,
	leaseOwner string,
	now time.Time,
	materialization MemoryVectorMaterialization,
) error {
	if strings.TrimSpace(materialization.ChatSessionID) == "" ||
		strings.TrimSpace(materialization.SourceRevision) == "" ||
		strings.TrimSpace(materialization.DocumentID) == "" ||
		materialization.SourceRowID <= 0 ||
		strings.TrimSpace(materialization.EmbeddingModel) == "" {
		return errors.New("invalid memory vector materialization")
	}
	// The embedding is validated as a NON-EMPTY float array, which is a stronger
	// check than "is valid JSON": an empty array, a JSON null and a JSON object
	// all satisfy a syntax check and none of them is a vector. Storing any of
	// them would leave a memory row whose embedding column cannot be embedded
	// with, and the failure would only surface at the next search.
	var embedding []float64
	if err := json.Unmarshal([]byte(strings.TrimSpace(materialization.EmbeddingJSON)), &embedding); err != nil || len(embedding) == 0 {
		return errors.New("invalid memory vector materialization embedding")
	}

	// The reference holds this across the probes and both writes, because its
	// transaction does. The mutex is what stands in for the row locks, and every
	// other writer of these two tables on this provider takes it, so no
	// in-process writer can interleave between the probes and the batch.
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	now = nonZeroTime(now)

	found, err := d1VectorMaterializedReadOperation(ctx, s, outboxID)
	if err != nil {
		return err
	}

	// A row the fence already retired is reported as retracted rather than as an
	// expired lease. A worker that is told its lease expired goes back to claim
	// the next operation and never learns that its document was thrown away; the
	// distinction is what lets the processor delete the document it just pushed.
	if found.status == "stale_rejected" {
		return ErrSourceRevisionStale
	}
	// The lease comparison happens here, in Go, on the same two values the
	// reference compares: the stored deadline parsed from TEXT and the caller
	// supplied now. It is never pushed into SQL, because a SQL comparison would
	// have to render now as text, and a caller may pass a time with a
	// sub-millisecond component that the fixed-width stored form truncates.
	// A NULL lease_until is refused rather than treated as unbounded: a lease
	// with no deadline is not a reclaimable lease, and honouring it would let
	// any worker complete an operation nobody can prove it still owns.
	if found.status != "leased" || found.operation != "upsert" ||
		found.leaseUntil == nil ||
		d1DerefString(found.leaseOwner) != leaseOwner ||
		found.leaseUntil.Before(now) {
		return ErrLeaseExpired
	}

	if found.chatSessionID != strings.TrimSpace(materialization.ChatSessionID) ||
		found.sourceRevision != strings.TrimSpace(materialization.SourceRevision) ||
		found.documentID != strings.TrimSpace(materialization.DocumentID) {
		return errors.New("memory vector materialization identity mismatch")
	}

	// A conjunction, not the outbox fence disjunction: a materialized completion
	// publishes a document that is only correct while its source is current, and
	// an operation that only requires an inactive source is a delete. Marking it
	// stale_rejected makes the refusal durable so the next claim stops offering
	// it, and the marker is the shared one the processor greps for when it
	// reports why a queued document was abandoned.
	if found.requiredSourceStat != "active" || found.lifecycleState != "active" {
		if _, err := s.conn.Exec(ctx, d1VectorMaterializedStaleReject,
			d1OutboxFenceRejectedLastError, d1TimeValue(now), outboxID); err != nil {
			return err
		}
		return ErrSourceRevisionStale
	}

	var memoryTurn int
	err = s.conn.QueryRow(ctx, d1VectorMaterializedMemoryTurnProbe,
		materialization.SourceRowID, found.chatSessionID).Scan(&memoryTurn)
	if errors.Is(err, errD1NoRows) {
		return errors.New("materialized memory row is missing")
	}
	if err != nil {
		return err
	}
	if memoryTurn != found.sourceTurn {
		return errors.New("materialized memory row source turn mismatch")
	}

	// The batch is the transaction. The reference order is kept so a reader
	// comparing the two providers reads them in the same order, and because
	// inside one atomic batch the order carries no durability meaning either way:
	// if the outbox statement fails the memory write is rolled back with it, and
	// that is the whole guarantee.
	//
	// EmbeddingJSON is bound exactly as the reference binds it, untrimmed. The
	// only producer marshals it, so the trimming difference is unobservable, and
	// binding the caller value verbatim keeps the two statements identical.
	err = s.conn.Batch(ctx,
		D1Statement{
			SQL: d1VectorMaterializedStoreVector,
			Args: []any{
				materialization.EmbeddingJSON,
				materialization.EmbeddingModel,
				materialization.SourceRowID,
				found.chatSessionID,
			},
		},
		D1Statement{
			SQL:  d1VectorMaterializedCompleteOutbox,
			Args: []any{d1TimeValue(now), outboxID},
		},
	)
	if err != nil {
		return err
	}
	return nil
}
