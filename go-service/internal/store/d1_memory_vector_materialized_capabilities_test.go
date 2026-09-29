package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// D1 materialized memory-vector completion tests.
//
// These run against the real SQLite engine through the same D1 transport the
// Worker uses, with foreign keys enforced, because this capability is almost
// entirely a question of what the SQL permits. Every assertion below is made
// against the STORED row, not against a return value: the failure this
// capability exists to prevent is a queue row that says a document is published
// while the memory row never received the vector, and that state returns nil.
//
// The seeds are arranged around the refusals that are silent. A completion with
// a mismatched identity, a completion onto a memory row whose turn has drifted
// from its source revision, a completion whose source was rolled back while the
// lease was held - each of them would write a vector nobody asked for, and each
// is asserted here by proving the two columns that must stay untouched.

// d1VectorMaterializedFixture is one session with an active source revision, a
// memory row on that revision turn, and a leased upsert operation naming both.
type d1VectorMaterializedFixture struct {
	sessionID  string
	revision   string
	turn       int
	documentID string
	memoryID   int64
	outboxID   int64
}

// d1VectorMaterializedMemorySeed describes one canonical memories row.
type d1VectorMaterializedMemorySeed struct {
	sessionID  string
	turn       int
	summary    string
	embedding  *string
	embedModel *string
}

// d1VectorMaterializedSeedMemory inserts one memories row through the D1
// transport. The nullable columns are bound through the same helpers the store
// uses so a NULL stays NULL, which is what lets a later assertion tell "the
// embedding was never written" apart from "the embedding was written empty".
func d1VectorMaterializedSeedMemory(
	t *testing.T,
	conn *sqliteD1Conn,
	seed d1VectorMaterializedMemorySeed,
) int64 {
	t.Helper()
	var id int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO memories (chat_session_id, turn_index, summary_json, embedding, embedding_model)
		VALUES (?, ?, ?, ?, ?) RETURNING id`,
		seed.sessionID, seed.turn, seed.summary, seed.embedding, seed.embedModel).Scan(&id); err != nil {
		t.Fatalf("seed memories row (session=%q turn=%d): %v", seed.sessionID, seed.turn, err)
	}
	return id
}

// d1VectorMaterializedStoredVector is the persisted form of the two columns this
// capability is allowed to write, plus the row turn that the verification reads.
// hasEmbedding distinguishes NULL from the empty string, which matters: a
// completion that stored an empty vector would look identical to one that stored
// nothing if the test only compared strings.
type d1VectorMaterializedStoredVector struct {
	present      bool
	turn         int
	hasEmbedding bool
	embedding    string
	hasModel     bool
	embedModel   string
	summary      string
}

func d1VectorMaterializedReadVector(
	t *testing.T,
	conn *sqliteD1Conn,
	memoryID int64,
) d1VectorMaterializedStoredVector {
	t.Helper()
	var row d1VectorMaterializedStoredVector
	var embedding, embedModel, summary *string
	err := conn.QueryRow(context.Background(),
		`SELECT turn_index, embedding, embedding_model, summary_json FROM memories WHERE id = ?`,
		memoryID).Scan(&row.turn, &embedding, &embedModel, &summary)
	if errors.Is(err, errD1NoRows) {
		return row
	}
	if err != nil {
		t.Fatalf("read memories row %d: %v", memoryID, err)
	}
	row.present = true
	if embedding != nil {
		row.hasEmbedding = true
		row.embedding = *embedding
	}
	if embedModel != nil {
		row.hasModel = true
		row.embedModel = *embedModel
	}
	row.summary = d1DerefString(summary)
	return row
}

// d1VectorMaterializedNow is the fixed instant every test completes at. It has no
// sub-millisecond component on purpose: the completion writes updated_at through
// d1TimeValue, which truncates to milliseconds, and a nanosecond in the fixture
// would make the stored text differ from what the test asked for.
var d1VectorMaterializedNow = time.Date(2026, 8, 30, 2, 30, 0, 0, time.UTC)

// d1VectorMaterializedLeaseOwner is the owner holding the fixture lease.
const d1VectorMaterializedLeaseOwner = "worker-a"

// d1VectorMaterializedMaterialization is the completion request the fixture is
// meant to satisfy. A test overrides only the field it is about, and every
// override is one of the identity members or the payload, because those are the
// only things the reference compares.
func d1VectorMaterializedMaterialization(
	fixture d1VectorMaterializedFixture,
) MemoryVectorMaterialization {
	return MemoryVectorMaterialization{
		ChatSessionID:  fixture.sessionID,
		SourceRevision: fixture.revision,
		DocumentID:     fixture.documentID,
		SourceRowID:    fixture.memoryID,
		EmbeddingJSON:  `[0.2,0.4,0.6]`,
		EmbeddingModel: "embedding-resolved",
	}
}

// d1VectorMaterializedNewFixture builds a completable fixture: an ACTIVE source
// revision, a memory row on that revision turn, and a LEASED upsert operation
// whose lease is live at d1VectorMaterializedNow.
//
// The lease is expressed through the same seed the outbox capability uses, so the
// stored lease_owner/lease_until are rendered by the same helper the claim path
// writes with. A fixture that hand-wrote its own timestamp format would compare
// two representations instead of two instants.
func d1VectorMaterializedNewFixture(t *testing.T, conn *sqliteD1Conn) d1VectorMaterializedFixture {
	t.Helper()
	leaseUntil := d1VectorMaterializedNow.Add(time.Minute)
	fixture := d1VectorMaterializedFixture{
		sessionID:  "vm-s1",
		revision:   "vm-s1-rev-live",
		turn:       5,
		documentID: "memory:vm-s1:17",
	}
	d1SeedAdmissionSource(t, conn, fixture.sessionID, fixture.revision, fixture.turn, "active")
	fixture.memoryID = d1VectorMaterializedSeedMemory(t, conn, d1VectorMaterializedMemorySeed{
		sessionID: fixture.sessionID,
		turn:      fixture.turn,
		summary:   `{"summary":"kept"}`,
	})
	// attempts is seeded NON-ZERO on purpose. The completion is not allowed to
	// touch it, and an assertion against a seeded zero would pass just as well if
	// the statement had reset it.
	fixture.outboxID = d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		operation:      "upsert",
		sessionID:      fixture.sessionID,
		sourceRevision: fixture.revision,
		documentID:     fixture.documentID,
		status:         "leased",
		attempts:       3,
		leaseOwner:     d1VectorMaterializedString(d1VectorMaterializedLeaseOwner),
		leaseUntil:     &leaseUntil,
		createdAt:      d1VectorMaterializedNow.Add(-time.Minute),
	})
	return fixture
}

// d1VectorMaterializedString returns a pointer to a copy of the text, for the
// nullable seed columns.
func d1VectorMaterializedString(value string) *string {
	copied := value
	return &copied
}

// d1VectorMaterializedAssertUntouched proves the refusal wrote nothing to the
// memory row. It is the assertion that carries the weight in this file: every
// refusal path must leave the vector columns exactly as they were, because a
// refusal that had already written an embedding would publish a document the
// caller was told was not published.
func d1VectorMaterializedAssertUntouched(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) {
	t.Helper()
	row := d1VectorMaterializedReadVector(t, conn, fixture.memoryID)
	if !row.present {
		t.Fatal("the memory row disappeared; a refusal must never delete one")
	}
	if row.hasEmbedding || row.hasModel {
		t.Errorf("a refused completion wrote a vector: embedding=%q model=%q",
			row.embedding, row.embedModel)
	}
	if row.summary != `{"summary":"kept"}` {
		t.Errorf("summary_json = %q, want the untouched seed value", row.summary)
	}
}

// TestD1VectorMaterializedCompletionStoresTheVectorAndCompletesTheOutbox is the
// happy path, and it asserts both halves of the write because a capability that
// completed only one of them would pass a test that checked a single table.
func TestD1VectorMaterializedCompletionStoresTheVectorAndCompletesTheOutbox(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	fixture := d1VectorMaterializedNewFixture(t, conn)
	materialization := d1VectorMaterializedMaterialization(fixture)

	if err := st.CompleteMemoryVectorMaterializedOperation(
		ctx, fixture.outboxID, d1VectorMaterializedLeaseOwner,
		d1VectorMaterializedNow, materialization,
	); err != nil {
		t.Fatalf("CompleteMemoryVectorMaterializedOperation: %v", err)
	}

	stored := d1VectorMaterializedReadVector(t, conn, fixture.memoryID)
	if !stored.hasEmbedding || stored.embedding != materialization.EmbeddingJSON {
		t.Errorf("stored embedding = %q (present=%t), want %q",
			stored.embedding, stored.hasEmbedding, materialization.EmbeddingJSON)
	}
	if !stored.hasModel || stored.embedModel != materialization.EmbeddingModel {
		t.Errorf("stored embedding_model = %q (present=%t), want %q",
			stored.embedModel, stored.hasModel, materialization.EmbeddingModel)
	}
	// The completion is not allowed to touch anything else on the row, because
	// memories is the append-only canonical table.
	if stored.summary != `{"summary":"kept"}` || stored.turn != fixture.turn {
		t.Errorf("the completion changed an unrelated column: %+v", stored)
	}

	outbox := d1OutboxRead(t, conn, fixture.outboxID)
	if outbox.status != "completed" {
		t.Errorf("outbox status = %q, want completed", outbox.status)
	}
	// Every column the claim set must be cleared: a completed row that still
	// carries a lease owner reads as ambiguous to the next claim and to a repair
	// tool that counts live leases.
	if outbox.leaseOwner != "" || !outbox.leaseUntil.IsZero() ||
		!outbox.retryAfter.IsZero() || outbox.lastError != "" {
		t.Errorf("a completed row kept lease/retry state: %+v", outbox)
	}
	var updatedAt string
	if err := conn.QueryRow(ctx,
		`SELECT updated_at FROM memory_vector_outbox WHERE id = ?`, fixture.outboxID).Scan(&updatedAt); err != nil {
		t.Fatalf("read outbox updated_at: %v", err)
	}
	if want := d1TimeValue(d1VectorMaterializedNow); updatedAt != want {
		t.Errorf("outbox updated_at = %q, want %q", updatedAt, want)
	}
	// The attempt counter is history and must survive the completion; a worker
	// that reads attempts = 0 here would look like a freshly queued operation,
	// and a repair tool counting retries would lose the record of how hard this
	// document was to materialise.
	if outbox.attempts != 3 {
		t.Errorf("outbox attempts = %d, want the seeded 3 preserved", outbox.attempts)
	}
}

// TestD1VectorMaterializedCompletionValidatesTheMaterialization pins the argument
// contract. Every case must be refused BEFORE any statement, because the two
// writes travel in one batch and a late failure would roll back a good write for
// a caller that simply passed nonsense.
func TestD1VectorMaterializedCompletionValidatesTheMaterialization(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	fixture := d1VectorMaterializedNewFixture(t, conn)
	base := d1VectorMaterializedMaterialization(fixture)

	cases := []struct {
		name   string
		mutate func(*MemoryVectorMaterialization)
		want   string
	}{
		{"blank session", func(m *MemoryVectorMaterialization) { m.ChatSessionID = "   " }, "invalid memory vector materialization"},
		{"blank revision", func(m *MemoryVectorMaterialization) { m.SourceRevision = "" }, "invalid memory vector materialization"},
		{"blank document", func(m *MemoryVectorMaterialization) { m.DocumentID = "\t" }, "invalid memory vector materialization"},
		{"zero source row", func(m *MemoryVectorMaterialization) { m.SourceRowID = 0 }, "invalid memory vector materialization"},
		{"negative source row", func(m *MemoryVectorMaterialization) { m.SourceRowID = -3 }, "invalid memory vector materialization"},
		{"blank model", func(m *MemoryVectorMaterialization) { m.EmbeddingModel = " " }, "invalid memory vector materialization"},
		{"blank embedding", func(m *MemoryVectorMaterialization) { m.EmbeddingJSON = "" }, "invalid memory vector materialization embedding"},
		{"broken json", func(m *MemoryVectorMaterialization) { m.EmbeddingJSON = `[0.2,` }, "invalid memory vector materialization embedding"},
		// The three that matter most: each is well formed JSON, so a check that
		// only asked "is this valid JSON" would accept it, and none of them is a
		// vector. Storing a JSON null would leave a memory row that no embedding
		// call can turn into a vector, and the failure would only surface at the
		// next search rather than here.
		{"empty array", func(m *MemoryVectorMaterialization) { m.EmbeddingJSON = `[]` }, "invalid memory vector materialization embedding"},
		{"json null", func(m *MemoryVectorMaterialization) { m.EmbeddingJSON = `null` }, "invalid memory vector materialization embedding"},
		{"json object", func(m *MemoryVectorMaterialization) { m.EmbeddingJSON = `{"vector":[0.2]}` }, "invalid memory vector materialization embedding"},
		{"non numeric array", func(m *MemoryVectorMaterialization) { m.EmbeddingJSON = `["a","b"]` }, "invalid memory vector materialization embedding"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := base
			tc.mutate(&request)
			err := st.CompleteMemoryVectorMaterializedOperation(
				ctx, fixture.outboxID, d1VectorMaterializedLeaseOwner,
				d1VectorMaterializedNow, request)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to name %q", err, tc.want)
			}
			d1VectorMaterializedAssertUntouched(t, conn, fixture)
		})
	}

	// Not one of those refusals may have retired the operation either: a bad
	// argument is the caller's bug, not a retraction of the document.
	if outbox := d1OutboxRead(t, conn, fixture.outboxID); outbox.status != "leased" {
		t.Errorf("outbox status = %q, want the lease left untouched by a rejected argument", outbox.status)
	}
}

// TestD1VectorMaterializedCompletionRequiresTheClaimedUpsertLease walks every
// shape of "this worker does not own this operation right now". Each one is
// ErrLeaseExpired or ErrSourceRevisionStale, and the distinction is the
// difference between a worker that re-claims and a worker that deletes the
// document it just pushed, so both identities are asserted.
func TestD1VectorMaterializedCompletionRequiresTheClaimedUpsertLease(t *testing.T) {
	cases := []struct {
		name     string
		want     error
		mutate   func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture)
		now      time.Time
		readBack func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) string
	}{
		{
			name: "unknown operation",
			want: ErrLeaseExpired,
			mutate: func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) {
				if _, err := conn.Exec(context.Background(),
					`DELETE FROM memory_vector_outbox WHERE id = ?`, fixture.outboxID); err != nil {
					t.Fatalf("remove outbox row: %v", err)
				}
			},
			now: d1VectorMaterializedNow,
		},
		{
			// A row the fence already retired. Reporting "your lease expired"
			// here would send the worker back to claim the next operation and it
			// would never learn that its document was thrown away.
			name: "already retired by the fence",
			want: ErrSourceRevisionStale,
			mutate: func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) {
				if _, err := conn.Exec(context.Background(),
					`UPDATE memory_vector_outbox
					 SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL,
					     last_error = 'source_revision_fence_rejected'
					 WHERE id = ?`, fixture.outboxID); err != nil {
					t.Fatalf("retire outbox row: %v", err)
				}
			},
			now: d1VectorMaterializedNow,
		},
		{
			name: "not leased",
			want: ErrLeaseExpired,
			mutate: func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) {
				if _, err := conn.Exec(context.Background(),
					`UPDATE memory_vector_outbox SET status = 'pending', lease_owner = NULL, lease_until = NULL
					 WHERE id = ?`, fixture.outboxID); err != nil {
					t.Fatalf("release outbox row: %v", err)
				}
			},
			now: d1VectorMaterializedNow,
		},
		{
			// A delete lane operation has no memories row to converge. Completing
			// one here would write a vector onto a document the user asked to
			// remove, which is the exact inverse of the delete.
			name: "delete lane operation",
			want: ErrLeaseExpired,
			mutate: func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) {
				if _, err := conn.Exec(context.Background(),
					`UPDATE memory_vector_outbox
					 SET operation = 'delete', required_source_state = 'inactive'
					 WHERE id = ?`, fixture.outboxID); err != nil {
					t.Fatalf("retag outbox row as a delete: %v", err)
				}
			},
			now: d1VectorMaterializedNow,
		},
		{
			name: "another owner holds the lease",
			want: ErrLeaseExpired,
			mutate: func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) {
				if _, err := conn.Exec(context.Background(),
					`UPDATE memory_vector_outbox SET lease_owner = 'worker-b' WHERE id = ?`,
					fixture.outboxID); err != nil {
					t.Fatalf("reassign lease: %v", err)
				}
			},
			now: d1VectorMaterializedNow,
		},
		{
			// An expired lease is not honoured even by the owner that took it: the
			// row is claimable again, and a slow worker that overwrote the result
			// the new owner is writing would corrupt the document silently.
			name: "lease already expired",
			want: ErrLeaseExpired,
			now:  d1VectorMaterializedNow.Add(2 * time.Minute),
		},
		{
			// A lease with no deadline is not a reclaimable lease, and honouring
			// it would let any worker complete an operation nobody can prove it
			// still owns.
			name: "lease without a deadline",
			want: ErrLeaseExpired,
			mutate: func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) {
				if _, err := conn.Exec(context.Background(),
					`UPDATE memory_vector_outbox SET lease_until = NULL WHERE id = ?`,
					fixture.outboxID); err != nil {
					t.Fatalf("clear lease deadline: %v", err)
				}
			},
			now: d1VectorMaterializedNow,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			ctx := context.Background()
			fixture := d1VectorMaterializedNewFixture(t, conn)
			if tc.mutate != nil {
				tc.mutate(t, conn, fixture)
			}
			err := st.CompleteMemoryVectorMaterializedOperation(
				ctx, fixture.outboxID, d1VectorMaterializedLeaseOwner,
				tc.now, d1VectorMaterializedMaterialization(fixture))
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			d1VectorMaterializedAssertUntouched(t, conn, fixture)
			// A refusal is not a write: the operation must be exactly as the
			// setup left it, in whatever state the setup put it.
			if tc.mutate == nil {
				if outbox := d1OutboxRead(t, conn, fixture.outboxID); outbox.status != "leased" {
					t.Errorf("outbox status = %q, want the lease preserved by a refusal", outbox.status)
				}
			}
		})
	}
}

// TestD1VectorMaterializedCompletionRejectsAMismatchedIdentity is the check that
// stops one document's vector landing on another document's row. Without it every
// other guard passes and a memory the caller never named is overwritten.
func TestD1VectorMaterializedCompletionRejectsAMismatchedIdentity(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*MemoryVectorMaterialization)
	}{
		{"another session", func(m *MemoryVectorMaterialization) { m.ChatSessionID = "vm-s2" }},
		{"another revision", func(m *MemoryVectorMaterialization) { m.SourceRevision = "vm-s1-rev-other" }},
		{"another document", func(m *MemoryVectorMaterialization) { m.DocumentID = "memory:vm-s1:99" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			ctx := context.Background()
			fixture := d1VectorMaterializedNewFixture(t, conn)
			request := d1VectorMaterializedMaterialization(fixture)
			tc.mutate(&request)

			err := st.CompleteMemoryVectorMaterializedOperation(
				ctx, fixture.outboxID, d1VectorMaterializedLeaseOwner,
				d1VectorMaterializedNow, request)
			if err == nil || !strings.Contains(err.Error(), "materialization identity mismatch") {
				t.Fatalf("error = %v, want the identity mismatch error", err)
			}
			d1VectorMaterializedAssertUntouched(t, conn, fixture)
			if outbox := d1OutboxRead(t, conn, fixture.outboxID); outbox.status != "leased" {
				t.Errorf("outbox status = %q, want a mismatch to leave the lease alone", outbox.status)
			}
		})
	}
}

// TestD1VectorMaterializedCompletionRetiresAnOperationWhoseSourceIsNotActive
// covers both arms of the source fence. The point that distinguishes this
// capability from the plain outbox finish is the SECOND case: a row that
// satisfies only the inactive arm is a delete wearing an upserts lease, and the
// outbox fence disjunction would let it through. A materialized completion must
// refuse it, because the vector it would write belongs to a document the user
// has discarded.
func TestD1VectorMaterializedCompletionRetiresAnOperationWhoseSourceIsNotActive(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture)
	}{
		{
			name: "source revision rolled back",
			mutate: func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) {
				if _, err := conn.Exec(context.Background(),
					`UPDATE memory_source_revisions SET lifecycle_state = 'invalidated'
					 WHERE source_revision = ?`, fixture.revision); err != nil {
					t.Fatalf("invalidate source revision: %v", err)
				}
			},
		},
		{
			// This is the arm the disjunction fence accepts. The upsert still
			// requires an ACTIVE source, and its source is active, so the
			// requirement column is what disqualifies it.
			name: "operation requires an inactive source",
			mutate: func(t *testing.T, conn *sqliteD1Conn, fixture d1VectorMaterializedFixture) {
				if _, err := conn.Exec(context.Background(),
					`UPDATE memory_vector_outbox SET required_source_state = 'inactive'
					 WHERE id = ?`, fixture.outboxID); err != nil {
					t.Fatalf("lower the required source state: %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			ctx := context.Background()
			fixture := d1VectorMaterializedNewFixture(t, conn)
			tc.mutate(t, conn, fixture)

			err := st.CompleteMemoryVectorMaterializedOperation(
				ctx, fixture.outboxID, d1VectorMaterializedLeaseOwner,
				d1VectorMaterializedNow, d1VectorMaterializedMaterialization(fixture))
			if !errors.Is(err, ErrSourceRevisionStale) {
				t.Fatalf("error = %v, want ErrSourceRevisionStale", err)
			}
			d1VectorMaterializedAssertUntouched(t, conn, fixture)

			// The refusal is durable. Without the retirement the row would stay
			// claimable and the next worker would push the same retracted document
			// into the index again, which is the loop the fence exists to stop.
			outbox := d1OutboxRead(t, conn, fixture.outboxID)
			if outbox.status != "stale_rejected" {
				t.Errorf("outbox status = %q, want stale_rejected", outbox.status)
			}
			if outbox.lastError != "source_revision_fence_rejected" {
				t.Errorf("outbox last_error = %q, want the fence marker the processor greps for", outbox.lastError)
			}
			if outbox.leaseOwner != "" || !outbox.leaseUntil.IsZero() {
				t.Errorf("a retired row kept its lease: %+v", outbox)
			}
		})
	}
}

// TestD1VectorMaterializedCompletionVerifiesTheMemoryRow is the cross-table half
// of the verification. The row id came out of the outbox document, so if the turn
// on the memory row disagrees with the turn on the source revision, the document
// now names a different memory than the one that was leased, and writing a
// vector onto it would publish an embedding that belongs to a different turn of
// the conversation.
func TestD1VectorMaterializedCompletionVerifiesTheMemoryRow(t *testing.T) {
	t.Run("row is missing", func(t *testing.T) {
		st, conn := newD1TestStore(t)
		ctx := context.Background()
		fixture := d1VectorMaterializedNewFixture(t, conn)
		request := d1VectorMaterializedMaterialization(fixture)
		request.SourceRowID = 999999

		err := st.CompleteMemoryVectorMaterializedOperation(
			ctx, fixture.outboxID, d1VectorMaterializedLeaseOwner, d1VectorMaterializedNow, request)
		if err == nil || !strings.Contains(err.Error(), "materialized memory row is missing") {
			t.Fatalf("error = %v, want the missing memory row error", err)
		}
		d1VectorMaterializedAssertUntouched(t, conn, fixture)
		if outbox := d1OutboxRead(t, conn, fixture.outboxID); outbox.status != "leased" {
			t.Errorf("outbox status = %q, want the operation still leased", outbox.status)
		}
	})

	t.Run("row belongs to another session", func(t *testing.T) {
		st, conn := newD1TestStore(t)
		ctx := context.Background()
		fixture := d1VectorMaterializedNewFixture(t, conn)
		// A memory row that exists, in the right shape, but in a different
		// session. The probe is keyed on BOTH id and session precisely so this
		// row cannot be mistaken for the leased one.
		otherID := d1VectorMaterializedSeedMemory(t, conn, d1VectorMaterializedMemorySeed{
			sessionID: "vm-s2",
			turn:      fixture.turn,
		})
		request := d1VectorMaterializedMaterialization(fixture)
		request.SourceRowID = otherID

		err := st.CompleteMemoryVectorMaterializedOperation(
			ctx, fixture.outboxID, d1VectorMaterializedLeaseOwner, d1VectorMaterializedNow, request)
		if err == nil || !strings.Contains(err.Error(), "materialized memory row is missing") {
			t.Fatalf("error = %v, want the missing memory row error", err)
		}
		d1VectorMaterializedAssertUntouched(t, conn, fixture)
	})

	t.Run("row turn drifted from the source revision", func(t *testing.T) {
		st, conn := newD1TestStore(t)
		ctx := context.Background()
		fixture := d1VectorMaterializedNewFixture(t, conn)
		if _, err := conn.Exec(ctx,
			`UPDATE memories SET turn_index = ? WHERE id = ?`,
			fixture.turn+3, fixture.memoryID); err != nil {
			t.Fatalf("drift the memory turn: %v", err)
		}

		err := st.CompleteMemoryVectorMaterializedOperation(
			ctx, fixture.outboxID, d1VectorMaterializedLeaseOwner,
			d1VectorMaterializedNow, d1VectorMaterializedMaterialization(fixture))
		if err == nil || !strings.Contains(err.Error(), "materialized memory row source turn mismatch") {
			t.Fatalf("error = %v, want the source turn mismatch error", err)
		}
		d1VectorMaterializedAssertUntouched(t, conn, fixture)
		if outbox := d1OutboxRead(t, conn, fixture.outboxID); outbox.status != "leased" {
			t.Errorf("outbox status = %q, want the operation still leased so the mismatch is retried, not lost", outbox.status)
		}
	})
}

// d1VectorMaterializedFaultConn wraps a D1 transport and poisons the batch.
//
// It exists to pin the atomicity claim rather than assert it in prose. A
// completion whose two writes were sent as two independent calls would not touch
// Batch at all, so batchCalls would stay zero and the poisoned statement would
// never run: the first write would already be committed and the memory row would
// carry a vector for an operation that is still leased. If instead the two writes
// travel in one Batch, the poison runs after them inside the same transaction and
// the rollback takes the vector write with it.
type d1VectorMaterializedFaultConn struct {
	D1Conn
	batchCalls   int
	statementLen int
}

// Batch counts the call and appends a statement that cannot execute, so the
// underlying transactional Batch fails and rolls back everything before it.
func (c *d1VectorMaterializedFaultConn) Batch(ctx context.Context, statements ...D1Statement) error {
	c.batchCalls++
	c.statementLen = len(statements)
	poisoned := append(append([]D1Statement{}, statements...),
		D1Statement{SQL: `INSERT INTO d1_vector_materialized_missing_table (x) VALUES (1)`})
	return c.D1Conn.Batch(ctx, poisoned...)
}

// TestD1VectorMaterializedCompletionWritesBothHalvesInOneBatch is the atomicity
// pin: the vector write and the queue completion must be one transaction, because
// an outbox row marked completed without the vector existing is an index that
// silently lost a document.
func TestD1VectorMaterializedCompletionWritesBothHalvesInOneBatch(t *testing.T) {
	_, conn := newD1TestStore(t)
	ctx := context.Background()
	fixture := d1VectorMaterializedNewFixture(t, conn)

	fault := &d1VectorMaterializedFaultConn{D1Conn: conn}
	wrapped, err := NewD1Store(fault)
	if err != nil {
		t.Fatalf("NewD1Store over the faulting transport: %v", err)
	}
	st, ok := wrapped.(*d1Store)
	if !ok {
		t.Fatalf("NewD1Store returned %T, want *d1Store", wrapped)
	}

	// The batch fails, so the whole completion fails.
	if err := st.CompleteMemoryVectorMaterializedOperation(
		ctx, fixture.outboxID, d1VectorMaterializedLeaseOwner,
		d1VectorMaterializedNow, d1VectorMaterializedMaterialization(fixture)); err == nil {
		t.Fatal("a batch that fails must fail the completion")
	}
	if fault.batchCalls != 1 {
		t.Fatalf("batch calls = %d, want exactly 1: the two writes must travel in one transaction", fault.batchCalls)
	}
	if fault.statementLen != 2 {
		t.Errorf("batch statement count = %d, want 2 (the memories write and the outbox completion)", fault.statementLen)
	}
	// The rollback is the guarantee: the memory row must not carry the vector,
	// because the queue entry that would have claimed it is still leased.
	d1VectorMaterializedAssertUntouched(t, conn, fixture)
	outbox := d1OutboxRead(t, conn, fixture.outboxID)
	if outbox.status != "leased" {
		t.Errorf("outbox status = %q, want leased after a rolled back batch", outbox.status)
	}
}

// TestD1VectorMaterializedCapabilityIsAdvertised pins the interface assertion the
// processor type-asserts on. If this capability were missing, the worker would
// report "materialized completion is not supported" after it had already pushed
// the document, which is a failure that looks like a misconfiguration.
func TestD1VectorMaterializedCapabilityIsAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)
	if _, ok := any(st).(MemoryVectorMaterializedCompletionStore); !ok {
		t.Fatal("the D1 store must satisfy MemoryVectorMaterializedCompletionStore")
	}
}
