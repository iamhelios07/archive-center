package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// D1 vector outbox tests.
//
// Every test runs against the real SQLite engine through the same D1 transport
// the Worker uses, with foreign keys on exactly as D1 enforces them. That is the
// point of this file: the outbox is a concurrency contract expressed almost
// entirely in SQL, and a mock would only prove that the Go code called the
// statements the test expected.
//
// The seeds are arranged around the failures that are silent. A claim that
// hands the same document to two owners, an enqueue that overwrites a stored
// operation with a different payload, a completion that publishes a document
// whose source the user already rolled back - none of them returns an error, and
// each of them is asserted here by the STORED row, not by the return value.

// ---------------------------------------------------------------------------
// seeds
// ---------------------------------------------------------------------------

// d1OutboxItemSeed is one memory_vector_outbox row. Every field that the schema
// constrains has a default taken from the reference defaults, so a test states
// only the column it is actually about.
type d1OutboxItemSeed struct {
	operationKey        string
	operation           string
	sessionID           string
	sourceRevision      string
	documentID          string
	documentJSON        *string
	embeddingReady      bool
	requiredSourceState string
	status              string
	attempts            int
	retryAfter          *time.Time
	leaseOwner          *string
	leaseUntil          *time.Time
	lastError           *string
	createdAt           time.Time
	updatedAt           time.Time
}

// d1OutboxItem fills in the defaults the reference enqueue would have applied,
// and derives the operation key from the PRODUCTION key helper. Using the real
// hash keeps the fixture honest: if this file computed keys differently from the
// store, the unique key under test would not be the one production enforces.
//
// embeddingReady defaults to TRUE, which is what makes a pending operation
// claimable at all: the claim admits a ready operation in the 'pending' or
// 'retryable' lane, and a not-ready operation only in the 'needs_embedding' or
// 'retryable' lane. A fixture that left the flag false and the status 'pending'
// would describe a row no worker can ever take, and every claim assertion in
// this file would pass for the wrong reason. The embedding-waiting case is
// therefore always stated explicitly, together with its status.
func d1OutboxItem(seed d1OutboxItemSeed) d1OutboxItemSeed {
	if seed.operation == "" {
		seed.operation = "upsert"
	}
	if seed.sessionID == "" {
		seed.sessionID = "s1"
	}
	if seed.sourceRevision == "" {
		seed.sourceRevision = "s1-rev-live"
	}
	if seed.documentID == "" {
		seed.documentID = "memory:" + seed.sessionID + ":1"
	}
	if seed.operationKey == "" {
		// A delete's key is derived from the OPERATION PLUS THE REQUIRED SOURCE
		// STATE, which is what both production delete paths do:
		// memoryVectorOperationKey("delete:inactive", ...) for an invalidation and
		// memoryAdmissionVectorOperationKey("delete:"+state, ...) for the
		// admission replay. Deriving it from the bare operation name would
		// produce a key no writer ever emits, and an assertion on it would pass
		// against a string the store can never produce.
		keyOperation := seed.operation
		if seed.operation == "delete" {
			keyOperation = "delete:" + d1OutboxRequiredSourceStateDefault(seed.operation)
		}
		seed.operationKey = memoryVectorOperationKey(keyOperation, seed.sessionID, seed.sourceRevision, seed.documentID)
	}
	if seed.requiredSourceState == "" {
		seed.requiredSourceState = d1OutboxRequiredSourceStateDefault(seed.operation)
	}
	if seed.status == "" {
		seed.status = "pending"
	}
	if !seed.embeddingReady && seed.status == "pending" {
		seed.embeddingReady = true
	}
	if seed.createdAt.IsZero() {
		seed.createdAt = time.Date(2026, 8, 30, 1, 0, 0, 0, time.UTC)
	}
	if seed.updatedAt.IsZero() {
		seed.updatedAt = seed.createdAt
	}
	return seed
}

// d1OutboxSeedItem inserts one outbox row through the D1 transport, so a seeded
// row is written by the same dialect the store uses.
//
// The nullable columns are rendered through the same helpers the store uses
// (d1TimeValue for a timestamp, NULL for an absent value). Binding a Go time
// straight to the driver would store the driver's own text form instead of the
// canonical RFC3339 the D1 bridge sends, and the lease comparison under test
// would then be comparing two different formats — a test that passes for the
// wrong reason.
func d1OutboxSeedItem(t *testing.T, conn *sqliteD1Conn, seed d1OutboxItemSeed) int64 {
	t.Helper()
	seed = d1OutboxItem(seed)
	if _, err := conn.Exec(context.Background(), `
		INSERT INTO memory_vector_outbox (
			contract_version, operation_key, operation, chat_session_id,
			source_revision, document_id, document_json, embedding_ready,
			required_source_state, status, attempts, retry_after, lease_owner,
			lease_until, last_error, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		MemoryVectorOutboxContract, seed.operationKey, seed.operation, seed.sessionID,
		seed.sourceRevision, seed.documentID, seed.documentJSON, d1BoolValue(seed.embeddingReady),
		seed.requiredSourceState, seed.status, seed.attempts, d1OutboxSeedTime(seed.retryAfter),
		d1OutboxSeedString(seed.leaseOwner), d1OutboxSeedTime(seed.leaseUntil),
		d1OutboxSeedString(seed.lastError),
		d1TimeValue(seed.createdAt), d1TimeValue(seed.updatedAt)); err != nil {
		t.Fatalf("seed outbox operation %s/%s: %v", seed.operation, seed.documentID, err)
	}
	var id int64
	if err := conn.QueryRow(context.Background(),
		`SELECT id FROM memory_vector_outbox WHERE operation_key = ?`,
		seed.operationKey).Scan(&id); err != nil {
		t.Fatalf("read seeded outbox id: %v", err)
	}
	return id
}

// d1OutboxSeedTime renders an optional timestamp the way the store stores it.
func d1OutboxSeedTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return d1TimeValue(*value)
}

// d1OutboxSeedString renders an optional text column, keeping NULL distinct from
// the empty string exactly as d1NullableString does on the write path.
func d1OutboxSeedString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

// d1OutboxStored is the persisted form of one outbox row, read back through the
// D1 transport. Asserting on this rather than on a returned struct is the whole
// point: the caller can be handed a projection the database does not hold.
type d1OutboxStored struct {
	id             int64
	status         string
	attempts       int
	leaseOwner     string
	leaseUntil     time.Time
	retryAfter     time.Time
	lastError      string
	documentJSON   string
	documentID     string
	requiredState  string
	embeddingReady bool
	operationKey   string
}

func d1OutboxRead(t *testing.T, conn *sqliteD1Conn, id int64) d1OutboxStored {
	t.Helper()
	var row d1OutboxStored
	var leaseOwner, lastError, documentJSON *string
	var leaseUntil, retryAfter *time.Time
	if err := conn.QueryRow(context.Background(), `
		SELECT id, status, attempts, lease_owner, lease_until, retry_after, last_error,
		       document_json, document_id, required_source_state, embedding_ready, operation_key
		FROM memory_vector_outbox WHERE id = ?`, id).Scan(
		&row.id, &row.status, &row.attempts, &leaseOwner, &leaseUntil, &retryAfter, &lastError,
		&documentJSON, &row.documentID, &row.requiredState, &row.embeddingReady, &row.operationKey); err != nil {
		t.Fatalf("read outbox row %d: %v", id, err)
	}
	row.leaseOwner = d1DerefString(leaseOwner)
	row.lastError = d1DerefString(lastError)
	row.documentJSON = d1DerefString(documentJSON)
	if leaseUntil != nil {
		row.leaseUntil = *leaseUntil
	}
	if retryAfter != nil {
		row.retryAfter = *retryAfter
	}
	return row
}

// d1OutboxSeedRevisions registers one live and one retired revision in a
// session. The retired one is what the fence has something to reject: an
// rollback that never happened leaves the fence untested.
func d1OutboxSeedRevisions(t *testing.T, conn *sqliteD1Conn, sessionID string) (live string, dead string) {
	t.Helper()
	live = sessionID + "-rev-live"
	dead = sessionID + "-rev-dead"
	d1SeedIdentityRevision(t, conn, sessionID, live, "turn-1", "active")
	d1SeedIdentityRevision(t, conn, sessionID, dead, "turn-2", "superseded")
	return live, dead
}

// d1OutboxLockSession marks a session as migrated away, which is the hold the
// claim must respect. The lock row has a foreign key onto session_migrations,
// so the parent row is written too: a fixture that skipped it would fail for a
// reason that has nothing to do with the claim.
func d1OutboxLockSession(t *testing.T, conn *sqliteD1Conn, sourceSessionID string, locked int) {
	t.Helper()
	ctx := context.Background()
	var migrationID int64
	if err := conn.QueryRow(ctx, `
		INSERT INTO session_migrations (source_session_id, target_session_id, status)
		VALUES (?, ?, 'migrated') RETURNING id`, sourceSessionID, sourceSessionID+"-canonical").Scan(&migrationID); err != nil {
		t.Fatalf("seed session migration: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO session_migration_locks (migration_id, source_session_id, target_session_id, lock_status, locked)
		VALUES (?, ?, ?, 'migrated_away', ?)`, migrationID, sourceSessionID, sourceSessionID+"-canonical", locked); err != nil {
		t.Fatalf("seed session migration lock: %v", err)
	}
}

// d1OutboxClaimKeys reduces a claim to its operation keys, which is what a
// caller acts on: the ids are assigned by the database and the keys are the
// identity the caller asked for.
func d1OutboxClaimKeys(items []*MemoryVectorOutboxItem) []string {
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.OperationKey)
	}
	return keys
}

// d1OutboxItem builds a valid enqueue request. A test overrides only the field
// it is about.
func d1OutboxItemRequest(sessionID, revision, documentID string) *MemoryVectorOutboxItem {
	return &MemoryVectorOutboxItem{
		OperationKey:        memoryVectorOperationKey("upsert", sessionID, revision, documentID),
		Operation:           "upsert",
		ChatSessionID:       sessionID,
		SourceRevision:      revision,
		DocumentID:          documentID,
		DocumentJSON:        `{"ID":"` + documentID + `"}`,
		EmbeddingReady:      true,
		RequiredSourceState: "active",
		Status:              "pending",
		CreatedAt:           time.Date(2026, 8, 30, 1, 0, 0, 0, time.UTC),
		UpdatedAt:           time.Date(2026, 8, 30, 1, 0, 0, 0, time.UTC),
	}
}

// ---------------------------------------------------------------------------
// enqueue
// ---------------------------------------------------------------------------

// TestD1EnqueueMemoryVectorOperationValidatesAndNormalizes pins the argument
// contract and the defaults.
//
// Each rejection must happen BEFORE any statement is sent, because the canonical
// schema enforces operation, required_source_state and status with CHECK
// constraints: an unvalidated item would fail the whole statement rather than
// one row, and on a batch it would roll back an otherwise good write.
func TestD1EnqueueMemoryVectorOperationValidatesAndNormalizes(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, _ := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := context.Background()

	cases := []struct {
		name  string
		apply func(*MemoryVectorOutboxItem)
		want  string
	}{
		{"no operation key", func(i *MemoryVectorOutboxItem) { i.OperationKey = "  " }, "invalid memory vector outbox item"},
		{"no session", func(i *MemoryVectorOutboxItem) { i.ChatSessionID = "" }, "invalid memory vector outbox item"},
		{"no revision", func(i *MemoryVectorOutboxItem) { i.SourceRevision = "" }, "invalid memory vector outbox item"},
		{"no document", func(i *MemoryVectorOutboxItem) { i.DocumentID = "" }, "invalid memory vector outbox item"},
		{"unknown operation", func(i *MemoryVectorOutboxItem) { i.Operation = "merge" }, "invalid vector outbox operation"},
		{"broken document json", func(i *MemoryVectorOutboxItem) { i.DocumentJSON = `{"broken":` }, "invalid memory vector document JSON"},
		{"upsert without a document", func(i *MemoryVectorOutboxItem) { i.DocumentJSON = "" }, "memory vector upsert document is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			item := d1OutboxItemRequest("s1", live, "memory:s1:"+strings.ReplaceAll(tc.name, " ", "-"))
			tc.apply(item)
			inserted, err := st.EnqueueMemoryVectorOperation(ctx, item)
			if inserted {
				t.Error("a rejected enqueue must not report inserted")
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to name %q", err, tc.want)
			}
		})
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != 0 {
		t.Fatalf("outbox rows = %d, want 0: a rejected enqueue must write nothing", got)
	}
	if _, err := st.EnqueueMemoryVectorOperation(ctx, nil); err == nil {
		t.Error("a nil item must be rejected")
	}

	// An upsert with nothing stated is published against the ACTIVE source and
	// starts pending: those are the reference's defaults and the worker reads
	// them, so a different default would queue a document the fence rejects.
	upsert := &MemoryVectorOutboxItem{
		OperationKey: memoryVectorOperationKey("upsert", "s1", live, "memory:s1:1"),
		Operation:    "upsert", ChatSessionID: "s1", SourceRevision: live,
		DocumentID: "memory:s1:1", DocumentJSON: `{"ID":"memory:s1:1"}`, EmbeddingReady: true,
	}
	inserted, err := st.EnqueueMemoryVectorOperation(ctx, upsert)
	if err != nil {
		t.Fatalf("EnqueueMemoryVectorOperation: %v", err)
	}
	if !inserted {
		t.Fatal("a first enqueue must report inserted")
	}
	if upsert.ContractVersion != MemoryVectorOutboxContract {
		t.Errorf("contract version = %q, want %q", upsert.ContractVersion, MemoryVectorOutboxContract)
	}
	if upsert.RequiredSourceState != "active" || upsert.Status != "pending" {
		t.Errorf("upsert defaults = %q/%q, want active/pending", upsert.RequiredSourceState, upsert.Status)
	}
	stored := d1OutboxRead(t, conn, 1)
	if stored.requiredState != "active" || stored.status != "pending" {
		t.Errorf("stored defaults = %q/%q, want active/pending", stored.requiredState, stored.status)
	}
	// A zero created_at is filled with the current instant, exactly as
	// nonZeroTime does: a stored empty string would sort before every other row
	// and be claimed first by an empty ORDER BY.
	if stored.status == "" {
		t.Error("the stored row is empty")
	}
	var createdAt string
	if err := conn.QueryRow(ctx,
		`SELECT created_at FROM memory_vector_outbox WHERE id = 1`).Scan(&createdAt); err != nil {
		t.Fatalf("read created_at: %v", err)
	}
	if createdAt == "" || strings.HasPrefix(createdAt, "0001-01-01") {
		t.Errorf("created_at = %q, want the current instant", createdAt)
	}

	// A delete defaults to the INACTIVE source state. The two defaults are
	// opposites and a worker that treated them as one would delete a live
	// document.
	del := &MemoryVectorOutboxItem{
		OperationKey: memoryVectorOperationKey("delete", "s1", "s1-rev-dead", "memory:s1:1"),
		Operation:    "delete", ChatSessionID: "s1", SourceRevision: "s1-rev-dead",
		DocumentID: "memory:s1:1", EmbeddingReady: true,
	}
	if _, err := st.EnqueueMemoryVectorOperation(ctx, del); err != nil {
		t.Fatalf("delete enqueue: %v", err)
	}
	if del.RequiredSourceState != "inactive" {
		t.Errorf("delete required state = %q, want %q", del.RequiredSourceState, "inactive")
	}
	// A delete carries no document: the audit JSON is optional, and requiring
	// one would make the invalidation path build a payload it does not have.
	deleteRow := d1OutboxRead(t, conn, 2)
	if deleteRow.documentJSON != "" {
		t.Errorf("a delete without a document must store NULL, got %q", deleteRow.documentJSON)
	}
}

// TestD1EnqueueMemoryVectorOperationReplaysIdempotentlyAndRejectsConflicts
// pins the idempotency half of the unique key.
//
// A replay must be recognised and must write nothing. A collision that is NOT an
// exact replay must be an error, never a silent overwrite: the key is a hash of
// (operation, session, revision, document), so a differing payload means two
// different operations collided, and keeping the stored row would drop one of
// them with no trace.
func TestD1EnqueueMemoryVectorOperationReplaysIdempotentlyAndRejectsConflicts(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, _ := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := context.Background()

	first := d1OutboxItemRequest("s1", live, "memory:s1:1")
	if inserted, err := st.EnqueueMemoryVectorOperation(ctx, first); !inserted || err != nil {
		t.Fatalf("first enqueue: inserted=%v err=%v", inserted, err)
	}

	// A retry rebuilds the item from scratch, exactly as an interrupted turn
	// would.
	replay := d1OutboxItemRequest("s1", live, "memory:s1:1")
	inserted, err := st.EnqueueMemoryVectorOperation(ctx, replay)
	if err != nil {
		t.Fatalf("replayed enqueue: %v", err)
	}
	if inserted {
		t.Error("a replay must report inserted=false")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != 1 {
		t.Fatalf("outbox rows after a replay = %d, want 1", got)
	}
	if replay.DocumentJSON != first.DocumentJSON {
		t.Error("a replay must not rewrite the stored document")
	}
	if stored := d1OutboxRead(t, conn, 1); stored.status != "pending" || stored.attempts != 0 {
		t.Errorf("a replay must not touch the stored row: %+v", stored)
	}

	// Every identity column is part of the collision. The document payload is
	// the one that matters most: two different documents hashed to the same key
	// would leave the index with one and the store claiming the other.
	conflicts := []struct {
		name  string
		apply func(*MemoryVectorOutboxItem)
	}{
		{"different document payload", func(i *MemoryVectorOutboxItem) { i.DocumentJSON = `{"ID":"memory:s1:999"}` }},
		{"different embedding readiness", func(i *MemoryVectorOutboxItem) { i.EmbeddingReady = false }},
		{"different required source state", func(i *MemoryVectorOutboxItem) { i.RequiredSourceState = "inactive" }},
		{"different operation", func(i *MemoryVectorOutboxItem) { i.Operation = "delete"; i.DocumentJSON = "" }},
	}
	for _, tc := range conflicts {
		t.Run(tc.name, func(t *testing.T) {
			item := d1OutboxItemRequest("s1", live, "memory:s1:1")
			tc.apply(item)
			inserted, err := st.EnqueueMemoryVectorOperation(ctx, item)
			if inserted {
				t.Error("a conflicting enqueue must not report inserted")
			}
			if err == nil || !strings.Contains(err.Error(), "idempotency conflict") {
				t.Fatalf("error = %v, want an idempotency conflict", err)
			}
			if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != 1 {
				t.Errorf("outbox rows = %d, want 1; a conflict must not write", got)
			}
			if stored := d1OutboxRead(t, conn, 1); stored.status != "pending" {
				t.Errorf("a conflict must leave the stored row alone: %+v", stored)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// claim
// ---------------------------------------------------------------------------

// TestD1VectorOutboxClaimNeverHandsALeasedOperationToASecondOwner is the
// concurrency contract, pinned end to end.
//
// A second worker must not receive an operation whose lease is still in the
// future. If it did, both workers would write the same document, and the
// durable row would record only one of the two outcomes: the loser's result is
// invisible. The fixture therefore has TWO claimable operations, so the second
// claim can only pass by handing back the wrong one. An empty second claim
// would also prove the predicate but would not prove the queue still drains.
func TestD1VectorOutboxClaimNeverHandsALeasedOperationToASecondOwner(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, _ := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := context.Background()
	now := time.Date(2026, 8, 30, 1, 10, 0, 0, time.UTC)

	firstID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sourceRevision: live, documentID: "memory:s1:1", createdAt: now, updatedAt: now,
	})
	secondID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sourceRevision: live, documentID: "memory:s1:2",
		createdAt: now.Add(time.Second), updatedAt: now,
	})

	claimed, err := st.ClaimMemoryVectorOperations(ctx, "worker-a", now, time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("first claim returned %d operations, want 1", len(claimed))
	}
	firstKey := memoryVectorOperationKey("upsert", "s1", live, "memory:s1:1")
	if claimed[0].OperationKey != firstKey {
		t.Fatalf("first claim took %q, want the oldest operation %q", claimed[0].OperationKey, firstKey)
	}
	if claimed[0].Status != "leased" || claimed[0].LeaseOwner != "worker-a" || claimed[0].Attempts != 1 {
		t.Errorf("claimed item = %+v, want a leased first attempt owned by worker-a", claimed[0])
	}
	if !claimed[0].LeaseUntil.Equal(now.Add(time.Minute)) {
		t.Errorf("lease_until = %s, want %s", claimed[0].LeaseUntil, now.Add(time.Minute))
	}
	// The lease must be DURABLE, not just returned: the returned struct is the
	// caller's copy and the next worker reads the column.
	stored := d1OutboxRead(t, conn, firstID)
	if stored.status != "leased" || stored.leaseOwner != "worker-a" || stored.attempts != 1 {
		t.Errorf("stored lease = %+v, want the row itself leased to worker-a", stored)
	}
	if !stored.leaseUntil.Equal(now.Add(time.Minute)) {
		t.Errorf("stored lease_until = %s, want %s", stored.leaseUntil, now.Add(time.Minute))
	}

	// A second worker claiming at the SAME instant must be given the other
	// operation, never the leased one.
	second, err := st.ClaimMemoryVectorOperations(ctx, "worker-b", now, time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("second claim returned %d operations, want exactly 1", len(second))
	}
	if second[0].ID == firstID {
		t.Fatal("a live lease was handed to a second owner")
	}
	if second[0].ID != secondID {
		t.Errorf("second claim took id %d, want the remaining operation %d", second[0].ID, secondID)
	}

	// With nothing left, the queue reports ErrNotFound rather than an empty
	// slice: a caller polling a drained queue must be able to tell "nothing to
	// do" from "the claim silently returned nothing".
	if _, err := st.ClaimMemoryVectorOperations(ctx, "worker-c", now, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Errorf("claim on a drained queue = %v, want ErrNotFound", err)
	}

	// The lease is a deadline, not a permanent claim: once it has passed, the
	// crashed owner's operation becomes claimable again and its attempt counter
	// advances, which is what the retry accounting reads. A ready upsert is not
	// batched with siblings, so the recovery claim returns one operation at a
	// time and the second expired lease is recovered on the next call.
	recovered, err := st.ClaimMemoryVectorOperations(ctx, "worker-d", now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("recovery claim: %v", err)
	}
	if len(recovered) != 1 || recovered[0].ID != firstID {
		t.Fatalf("recovery claim = %+v, want the oldest expired lease %d", d1OutboxClaimKeys(recovered), firstID)
	}
	if recovered[0].LeaseOwner != "worker-d" || recovered[0].Attempts != 2 {
		t.Errorf("recovered operation = %+v, want a second attempt owned by worker-d", recovered[0])
	}
	if got := d1OutboxRead(t, conn, firstID); got.attempts != 2 || got.leaseOwner != "worker-d" {
		t.Errorf("stored recovery = %+v, want the second attempt durable", got)
	}
	secondRecovered, err := st.ClaimMemoryVectorOperations(ctx, "worker-d", now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatalf("second recovery claim: %v", err)
	}
	if len(secondRecovered) != 1 || secondRecovered[0].ID != secondID || secondRecovered[0].Attempts != 2 {
		t.Errorf("second recovery = %+v, want %d on its second attempt", d1OutboxClaimKeys(secondRecovered), secondID)
	}
}

// TestD1VectorOutboxClaimLeasesTheWholeSiblingGroup pins the batching that makes
// the claim worth having.
//
// An upsert still waiting for its embedding is leased together with every other
// embedding-waiting operation of the SAME revision, so one embedding round trip
// amortizes over the whole batch. A ready upsert is not batched: it is already a
// finished vector to push, and leasing its siblings would only delay them
// behind a lease they do not need.
func TestD1VectorOutboxClaimLeasesTheWholeSiblingGroup(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, _ := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := context.Background()
	now := time.Date(2026, 8, 30, 2, 0, 0, 0, time.UTC)

	var ids []int64
	for i := 1; i <= 3; i++ {
		ids = append(ids, d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
			sourceRevision: live, documentID: "precise_memory:s1:u" + string(rune('0'+i)),
			documentJSON:   d1OutboxStrPtr(`{"ID":"precise_memory:s1:u` + string(rune('0'+i)) + `"}`),
			embeddingReady: false, status: "needs_embedding",
			createdAt: now.Add(time.Duration(i) * time.Second), updatedAt: now,
		}))
	}
	// A READY upsert of the same revision is not part of the group.
	readyID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sourceRevision: live, documentID: "precise_memory:s1:ready", embeddingReady: true,
		createdAt: now.Add(10 * time.Second), updatedAt: now,
	})

	claimed, err := st.ClaimMemoryVectorOperations(ctx, "worker", now, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed %d operations, want the 3-operation embedding group", len(claimed))
	}
	// ORDER BY created_at, id: the batch is returned in claim order so a worker
	// can report progress against it.
	for i, item := range claimed {
		if item.ID != ids[i] {
			t.Errorf("claim position %d = id %d, want %d (created_at, id order)", i, item.ID, ids[i])
		}
		if item.Status != "leased" || item.LeaseOwner != "worker" || item.Attempts != 1 {
			t.Errorf("claimed %d = %+v, want a leased first attempt", i, item)
		}
	}
	for _, id := range ids {
		if got := d1OutboxRead(t, conn, id); got.status != "leased" || got.leaseOwner != "worker" {
			t.Errorf("row %d = %+v, want the lease to be durable", id, got)
		}
	}
	// The ready upsert stays claimable: it was not swept into the group.
	if got := d1OutboxRead(t, conn, readyID); got.status != "pending" {
		t.Errorf("the ready upsert = %+v, want it left pending", got)
	}
}

// TestD1VectorOutboxClaimDeleteLaneBypassesUnrelatedUpserts pins the lane
// capability. A continuous upsert backlog must not be able to starve deletes
// forever, so the bounded worker claims the delete lane directly.
//
// A typo in the lane name is refused rather than treated as "claim everything":
// otherwise a misconfigured worker would quietly take the whole queue and the
// starvation it was added to fix would come back.
func TestD1VectorOutboxClaimDeleteLaneBypassesUnrelatedUpserts(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, dead := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := context.Background()
	now := time.Date(2026, 8, 30, 3, 0, 0, 0, time.UTC)

	// An older upsert of one document, and a newer delete of a DIFFERENT one.
	// The documents must differ: an earlier open operation for the same document
	// blocks a later one, so a delete queued behind its own upsert is correctly
	// unclaimable and would prove nothing about the lane.
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sourceRevision: live, documentID: "memory:s1:1", createdAt: now, updatedAt: now,
	})
	deleteKey := memoryVectorOperationKey("delete:inactive", "s1", dead, "memory:s1:2")
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		operation: "delete", sourceRevision: dead, documentID: "memory:s1:2",
		embeddingReady: true, requiredSourceState: "inactive",
		createdAt: now.Add(time.Second), updatedAt: now,
	})

	// A lane name that is neither lane is refused rather than treated as "claim
	// everything": otherwise a misconfigured worker would quietly take the whole
	// queue and the starvation the lane was added to fix would come back. A PADDED
	// or differently cased lane name is normalized instead, because that is the
	// same worker's spelling of a real lane and refusing it would stop the queue
	// draining for a reason the caller cannot see.
	for _, lane := range []string{"", "  ", "merge", "upsert-x", "delete,upsert"} {
		if _, err := st.ClaimMemoryVectorOperationsByOperation(ctx, "worker", now, time.Minute, lane); err == nil {
			t.Errorf("lane %q was accepted, want it refused", lane)
		}
	}

	// The delete is the NEWER row, so an unfiltered claim would take the upsert
	// first. The lane is what lets the bounded worker reach the delete anyway.
	claimed, err := st.ClaimMemoryVectorOperationsByOperation(ctx, "worker", now, time.Minute, "  DeLeTe  ")
	if err != nil {
		t.Fatalf("delete lane claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].OperationKey != deleteKey {
		t.Fatalf("delete lane claimed %v, want only the delete %q", d1OutboxClaimKeys(claimed), deleteKey)
	}
	if claimed[0].Operation != "delete" || claimed[0].RequiredSourceState != "inactive" {
		t.Errorf("claimed delete = %+v, want the inactive-required lane", claimed[0])
	}

	// The upsert lane is the mirror image and must not see the delete.
	upserts, err := st.ClaimMemoryVectorOperationsByOperation(ctx, "worker", now, time.Minute, "upsert")
	if err != nil {
		t.Fatalf("upsert lane claim: %v", err)
	}
	if len(upserts) != 1 || upserts[0].Operation != "upsert" {
		t.Errorf("upsert lane claimed %v, want only the upsert", d1OutboxClaimKeys(upserts))
	}
}

// TestD1VectorOutboxClaimRejectsRowsTheSourceFenceRetires pins the fence, in
// BOTH directions, and pins that the rejection is durable.
//
// An upsert of a document whose source has been rolled back, and a delete of a
// document whose source is still current, are both wrong, and they are wrong
// for opposite reasons. Checking only one arm would publish retracted content or
// remove live content from the index, and both failures are invisible: the
// document simply is or is not there.
func TestD1VectorOutboxClaimRejectsRowsTheSourceFenceRetires(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, dead := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := context.Background()
	now := time.Date(2026, 8, 30, 4, 0, 0, 0, time.UTC)

	upsertID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sourceRevision: dead, documentID: "memory:s1:rolled-back",
		documentJSON: d1OutboxStrPtr(`{"ID":"memory:s1:rolled-back"}`), createdAt: now, updatedAt: now,
	})
	deleteID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		operation: "delete", sourceRevision: live, documentID: "memory:s1:current",
		requiredSourceState: "inactive", embeddingReady: true,
		createdAt: now.Add(time.Second), updatedAt: now,
	})

	if _, err := st.ClaimMemoryVectorOperations(ctx, "worker", now, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Fatalf("claim over fenced rows = %v, want ErrNotFound", err)
	}
	for _, id := range []int64{upsertID, deleteID} {
		stored := d1OutboxRead(t, conn, id)
		if stored.status != "stale_rejected" {
			t.Errorf("row %d status = %q, want stale_rejected", id, stored.status)
		}
		if stored.lastError != d1OutboxFenceRejectedLastError {
			t.Errorf("row %d last_error = %q, want %q", id, stored.lastError, d1OutboxFenceRejectedLastError)
		}
		if stored.leaseOwner != "" || !stored.leaseUntil.IsZero() {
			t.Errorf("row %d = %+v, want the lease cleared", id, stored)
		}
	}

	// A retired row stays retired: the fence is applied on every claim, so a
	// later poll cannot resurrect it.
	if _, err := st.ClaimMemoryVectorOperations(ctx, "worker", now.Add(time.Hour), time.Minute); !errors.Is(err, ErrNotFound) {
		t.Errorf("second claim = %v, want ErrNotFound", err)
	}
	if got := d1OutboxRead(t, conn, upsertID); got.status != "stale_rejected" {
		t.Errorf("a retired row was resurrected: %+v", got)
	}
}

// TestD1VectorOutboxClaimPreservesPerDocumentCausalOrder pins the guard that
// stops a later operation overtaking an earlier one for the same document.
//
// The index applies a document's operations in the order it was given them. A
// delete that lands before the upsert of the same document would leave the
// document permanently in the index with nothing to remove it, and nothing in
// the durable state would record that the two ran out of order.
func TestD1VectorOutboxClaimPreservesPerDocumentCausalOrder(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, dead := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := context.Background()
	now := time.Date(2026, 8, 30, 5, 0, 0, 0, time.UTC)
	documentID := "memory:s1:ordered"

	// An upsert and a later delete of the SAME document. The guard is not about
	// two copies of one operation: the unique operation key already prevents
	// that. It is about a document's history, so the fixture has to use two
	// different operations on one document, which is exactly the shape a
	// rollback produces: the document is published, then retracted.
	olderID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sourceRevision: live, documentID: documentID,
		documentJSON: d1OutboxStrPtr(`{"ID":"` + documentID + `"}`),
		createdAt:    now, updatedAt: now,
	})
	newerID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		operation: "delete", sourceRevision: dead, documentID: documentID,
		requiredSourceState: "inactive", embeddingReady: true,
		createdAt: now.Add(time.Second), updatedAt: now,
	})

	claimed, err := st.ClaimMemoryVectorOperations(ctx, "worker", now, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != olderID {
		t.Fatalf("claim took %+v, want only the older operation %d", d1OutboxClaimKeys(claimed), olderID)
	}
	// The later operation is blocked by the leased earlier one, so the document
	// cannot be published twice out of order while the first is in flight.
	if _, err := st.ClaimMemoryVectorOperations(ctx, "worker", now, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Errorf("claim of the later operation = %v, want ErrNotFound while the earlier one is open", err)
	}
	if got := d1OutboxRead(t, conn, newerID); got.status != "pending" {
		t.Errorf("the later operation = %+v, want it left pending", got)
	}
}

// TestD1VectorOutboxClaimSkipsAMigrationLockedSession pins the session-migration
// hold. A session that has been migrated away must not have its outbox drained:
// the canonical rows have moved, so applying a document here would resurrect
// content in a session the user no longer reads.
func TestD1VectorOutboxClaimSkipsAMigrationLockedSession(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, _ := d1OutboxSeedRevisions(t, conn, "s1")
	d1OutboxSeedRevisions(t, conn, "s2")
	d1OutboxSeedRevisions(t, conn, "s3")
	ctx := context.Background()
	now := time.Date(2026, 8, 30, 6, 0, 0, 0, time.UTC)
	d1OutboxLockSession(t, conn, "s1", 1)
	// A lock that was never taken is not a hold. Treating the mere PRESENCE of a
	// lock row as a hold would strand a session whose migration never completed,
	// and the hold would never lift.
	d1OutboxLockSession(t, conn, "s3", 0)

	lockedID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s1", sourceRevision: live, documentID: "memory:s1:1",
		createdAt: now, updatedAt: now,
	})
	untakenLockID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s3", sourceRevision: "s3-rev-live", documentID: "memory:s3:1",
		createdAt: now, updatedAt: now,
	})
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s2", sourceRevision: "s2-rev-live", documentID: "memory:s2:1",
		createdAt: now.Add(time.Second), updatedAt: now,
	})

	claimed, err := st.ClaimMemoryVectorOperations(ctx, "worker", now, time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// created_at ordering puts the held session first, so the claim can only pass
	// by skipping it.
	if len(claimed) != 1 || claimed[0].ID != untakenLockID {
		t.Fatalf("claim = %+v, want the session whose lock was never taken", d1OutboxClaimKeys(claimed))
	}
	if got := d1OutboxRead(t, conn, lockedID); got.status != "pending" {
		t.Errorf("the held session's operation = %+v, want it left pending", got)
	}

	// Releasing the hold makes the session claimable again. A hold that has been
	// released is not a hold, and reading it as one would strand the session's
	// queue forever behind a migration that already completed.
	if _, err := conn.Exec(ctx, `
		UPDATE session_migration_locks SET unlocked_at = ?
		WHERE source_session_id = ? AND locked = 1`, d1TimeValue(now), "s1"); err != nil {
		t.Fatalf("release the session hold: %v", err)
	}
	reclaimed, err := st.ClaimMemoryVectorOperations(ctx, "worker", now, time.Minute)
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != lockedID {
		t.Errorf("claim after release = %+v, want the previously held operation", d1OutboxClaimKeys(reclaimed))
	}
}

// ---------------------------------------------------------------------------
// completion and failure
// ---------------------------------------------------------------------------

// TestD1VectorOutboxFinishUsesTheClaimedLeaseAndTheSourceFence pins the whole
// completion contract: the lease, the owner, the source fence, and the two
// different sentinels.
//
// The two sentinels are not interchangeable. ErrLeaseExpired tells a worker its
// lease is gone and it should stop; ErrSourceRevisionStale tells it the document
// was retracted and the work must not be retried. Reporting the wrong one makes
// a worker retry a document the user deliberately erased, which is how a
// retracted memory comes back.
func TestD1VectorOutboxFinishUsesTheClaimedLeaseAndTheSourceFence(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, dead := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := context.Background()
	now := time.Date(2026, 8, 30, 7, 0, 0, 0, time.UTC)

	claim := func(documentID string) int64 {
		t.Helper()
		id := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
			sourceRevision: live, documentID: documentID, createdAt: now, updatedAt: now,
		})
		claimed, err := st.ClaimMemoryVectorOperations(ctx, "worker", now, time.Minute)
		if err != nil {
			t.Fatalf("claim %s: %v", documentID, err)
		}
		if len(claimed) != 1 || claimed[0].ID != id {
			t.Fatalf("claim %s returned %+v", documentID, d1OutboxClaimKeys(claimed))
		}
		return id
	}

	// A leased operation completes only for its owner.
	completedID := claim("memory:s1:complete")
	if err := st.CompleteMemoryVectorOperation(ctx, completedID, "someone-else", now); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("completion by another owner = %v, want ErrLeaseExpired", err)
	}
	if got := d1OutboxRead(t, conn, completedID); got.status != "leased" {
		t.Errorf("a rejected completion changed the row: %+v", got)
	}
	if err := st.CompleteMemoryVectorOperation(ctx, completedID, "worker", now); err != nil {
		t.Fatalf("completion: %v", err)
	}
	stored := d1OutboxRead(t, conn, completedID)
	if stored.status != "completed" {
		t.Errorf("status = %q, want completed", stored.status)
	}
	if stored.leaseOwner != "" || !stored.leaseUntil.IsZero() {
		t.Errorf("a completed row must release its lease: %+v", stored)
	}
	if stored.lastError != "" {
		t.Errorf("a completed row must clear last_error, got %q", stored.lastError)
	}
	// A completed row is not reclaimable, or the worker would publish the same
	// document forever.
	if _, err := st.ClaimMemoryVectorOperations(ctx, "worker", now, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Errorf("claim after completion = %v, want ErrNotFound", err)
	}

	// A failure returns the operation to the queue with a wake time, and a
	// permanent failure retires it.
	failedID := claim("memory:s1:retryable")
	retryAt := now.Add(2 * time.Minute)
	if err := st.FailMemoryVectorOperation(ctx, failedID, "worker", now, retryAt, false, "chroma_unreachable"); err != nil {
		t.Fatalf("failure: %v", err)
	}
	stored = d1OutboxRead(t, conn, failedID)
	if stored.status != "retryable" || stored.lastError != "chroma_unreachable" {
		t.Errorf("failed row = %+v, want retryable with the failure recorded", stored)
	}
	if !stored.retryAfter.Equal(retryAt) {
		t.Errorf("retry_after = %s, want %s", stored.retryAfter, retryAt)
	}
	if stored.leaseOwner != "" {
		t.Errorf("a failed row must release its lease: %+v", stored)
	}
	// The wake time is honoured in both directions. Before it passes the row is
	// invisible, which is what stops a failing document from being retried in a
	// tight loop; after it passes the row is claimable again, which is the whole
	// point of recording a wake time at all.
	if _, err := st.ClaimMemoryVectorOperations(ctx, "worker", now, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Errorf("claim before retry_after = %v, want ErrNotFound", err)
	}
	// The wake time itself is not yet past: the comparison is strict, so a claim
	// at exactly retry_after still finds nothing.
	if _, err := st.ClaimMemoryVectorOperations(ctx, "worker", retryAt, time.Minute); !errors.Is(err, ErrNotFound) {
		t.Errorf("claim at exactly retry_after = %v, want ErrNotFound", err)
	}
	retryClaim, err := st.ClaimMemoryVectorOperations(ctx, "worker", retryAt.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatalf("claim after retry_after: %v", err)
	}
	if len(retryClaim) != 1 || retryClaim[0].ID != failedID {
		t.Fatalf("claim after retry_after = %+v, want the backed-off operation", d1OutboxClaimKeys(retryClaim))
	}
	if retryClaim[0].Attempts != 2 {
		t.Errorf("recovered attempt = %d, want 2; the counter is what the backoff reads", retryClaim[0].Attempts)
	}
	// Completing it retires the row so it stops competing with the rest of the
	// cases in this test.
	if err := st.CompleteMemoryVectorOperation(ctx, failedID, "worker", retryAt.Add(time.Second)); err != nil {
		t.Fatalf("completion of the recovered operation: %v", err)
	}

	permanentID := claim("memory:s1:permanent")
	if err := st.FailMemoryVectorOperation(ctx, permanentID, "worker", now, time.Time{}, true, "unrecoverable"); err != nil {
		t.Fatalf("permanent failure: %v", err)
	}
	stored = d1OutboxRead(t, conn, permanentID)
	if stored.status != "permanent" || !stored.retryAfter.IsZero() {
		t.Errorf("permanent row = %+v, want permanent with no wake time", stored)
	}
	if _, err := st.ClaimMemoryVectorOperations(ctx, "worker", now.Add(24*time.Hour), time.Minute); !errors.Is(err, ErrNotFound) {
		t.Error("a permanently failed operation must never be reclaimed")
	}

	// A lease taken while the source was current, finished after the source was
	// rolled back. Completing it would publish a document the user discarded.
	rolledBackID := claim("memory:s1:rolled-back-later")
	if _, err := conn.Exec(ctx,
		`UPDATE memory_source_revisions SET lifecycle_state = 'superseded' WHERE source_revision = ?`,
		live); err != nil {
		t.Fatalf("roll the source revision back: %v", err)
	}
	err = st.CompleteMemoryVectorOperation(ctx, rolledBackID, "worker", now)
	if !errors.Is(err, ErrSourceRevisionStale) {
		t.Fatalf("completion after a rollback = %v, want ErrSourceRevisionStale", err)
	}
	stored = d1OutboxRead(t, conn, rolledBackID)
	if stored.status != "stale_rejected" || stored.lastError != d1OutboxFenceRejectedLastError {
		t.Errorf("row after the fence = %+v, want it retired with the fence marker", stored)
	}
	if stored.leaseOwner != "" {
		t.Errorf("a retired row must release its lease: %+v", stored)
	}

	// An expired lease is not honoured, even for the original owner: its work
	// may already have been re-leased to somebody else, and completing it now
	// would overwrite the result that owner is about to write. This case is
	// seeded last on purpose: an expired lease is claimable again, so leaving it
	// in the queue would make every earlier claim assertion in this test
	// ambiguous.
	expiredID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sourceRevision: dead, documentID: "memory:s1:expired", status: "leased",
		leaseOwner: d1OutboxStrPtr("dead-worker"),
		leaseUntil: d1OutboxTimePtr(now.Add(-time.Second)),
		createdAt:  now, updatedAt: now,
	})
	if err := st.CompleteMemoryVectorOperation(ctx, expiredID, "dead-worker", now); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("completion on an expired lease = %v, want ErrLeaseExpired", err)
	}
	if got := d1OutboxRead(t, conn, expiredID); got.status != "leased" || got.leaseOwner != "dead-worker" {
		t.Errorf("a rejected completion changed the row: %+v", got)
	}

	// A row the fence already retired reports the source fence, not the lease.
	// A worker whose document was retracted must be able to tell that from a
	// lease problem, or it will retry work that must never run again.
	fencedID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sourceRevision: dead, documentID: "memory:s1:fenced", status: "stale_rejected",
		lastError: d1OutboxStrPtr(d1OutboxFenceRejectedLastError), createdAt: now, updatedAt: now,
	})
	if err := st.CompleteMemoryVectorOperation(ctx, fencedID, "worker", now); !errors.Is(err, ErrSourceRevisionStale) {
		t.Errorf("completion of a retired row = %v, want ErrSourceRevisionStale", err)
	}
	// An id that addresses nothing at all is a lease the worker never held, not a
	// source problem: nothing about the source can be concluded from a row that
	// does not exist.
	if err := st.CompleteMemoryVectorOperation(ctx, 999999, "worker", now); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("completion of an unknown id = %v, want ErrLeaseExpired", err)
	}
}

// ---------------------------------------------------------------------------
// maintenance
// ---------------------------------------------------------------------------

// TestD1VectorOutboxCoalescesOnlyDuplicateInactiveDeletes pins the maintenance
// pass.
//
// The rule is causal order, not recency: the EARLIEST open delete for a
// (revision, document) pair survives and every later one is retired. Retiring
// the earliest instead would leave the document in the index with no operation
// left to remove it: a leak that no further pass would repair, because the
// retired row would already be gone from the candidate set.
func TestD1VectorOutboxCoalescesOnlyDuplicateInactiveDeletes(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, dead := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := context.Background()
	now := time.Date(2026, 8, 30, 8, 0, 0, 0, time.UTC)

	deleteSeed := func(documentID, revision string, createdAt time.Time) int64 {
		t.Helper()
		return d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
			operation: "delete", sourceRevision: revision, documentID: documentID,
			operationKey:        memoryVectorOperationKey("delete:inactive", "s1", revision, documentID) + "-" + createdAt.Format("150405"),
			requiredSourceState: "inactive", embeddingReady: true,
			createdAt: createdAt, updatedAt: now,
		})
	}
	// Two deletes of the same document from the same retired revision: the older
	// survives, the newer is the duplicate.
	keepID := deleteSeed("memory:s1:duplicated", dead, now)
	duplicateID := deleteSeed("memory:s1:duplicated", dead, now.Add(time.Minute))
	// A delete of a document whose source is STILL ACTIVE must never be
	// coalesced: retiring it would silently cancel a pending cleanup that a
	// later invalidation still needs to run.
	activeID := deleteSeed("memory:s1:current", live, now.Add(2*time.Minute))
	// A live lease on a document's delete is a claim in progress; retiring it
	// would make the worker's completion land on a row it no longer owns.
	leasedID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		operation: "delete", sourceRevision: dead, documentID: "memory:s1:leased",
		operationKey:        memoryVectorOperationKey("delete:inactive", "s1", dead, "memory:s1:leased") + "-leased",
		requiredSourceState: "inactive", embeddingReady: true, status: "leased",
		leaseOwner: d1OutboxStrPtr("worker"),
		leaseUntil: d1OutboxTimePtr(now.Add(time.Hour)),
		createdAt:  now, updatedAt: now,
	})
	// A different document is not a duplicate of anything.
	otherID := deleteSeed("memory:s1:other", dead, now.Add(3*time.Minute))

	if _, err := st.CoalesceInactiveMemoryVectorDeleteOperations(ctx, "  ", now); err == nil {
		t.Error("a blank session must be refused")
	}

	retired, err := st.CoalesceInactiveMemoryVectorDeleteOperations(ctx, "s1", now)
	if err != nil {
		t.Fatalf("CoalesceInactiveMemoryVectorDeleteOperations: %v", err)
	}
	if retired != 1 {
		t.Errorf("retired = %d, want exactly the one later duplicate", retired)
	}
	if got := d1OutboxRead(t, conn, duplicateID); got.status != "stale_rejected" ||
		got.lastError != d1OutboxDuplicateDeleteLastError {
		t.Errorf("the later duplicate = %+v, want it retired with the duplicate marker", got)
	}
	if got := d1OutboxRead(t, conn, keepID); got.status != "pending" {
		t.Errorf("the earliest delete = %+v, want it kept", got)
	}
	if got := d1OutboxRead(t, conn, activeID); got.status != "pending" {
		t.Errorf("a delete of a current source = %+v, want it kept", got)
	}
	if got := d1OutboxRead(t, conn, leasedID); got.status != "leased" || got.leaseOwner != "worker" {
		t.Errorf("a leased delete = %+v, want its lease untouched", got)
	}
	if got := d1OutboxRead(t, conn, otherID); got.status != "pending" {
		t.Errorf("an unrelated delete = %+v, want it kept", got)
	}

	// The pass is idempotent: a second run finds nothing, so a maintenance route
	// that is retried cannot inflate the reported count.
	again, err := st.CoalesceInactiveMemoryVectorDeleteOperations(ctx, "s1", now)
	if err != nil {
		t.Fatalf("second coalesce pass: %v", err)
	}
	if again != 0 {
		t.Errorf("second pass retired %d, want 0", again)
	}

	// A session the caller never asked about is untouched.
	other, err := st.CoalesceInactiveMemoryVectorDeleteOperations(ctx, "s-unknown", now)
	if err != nil {
		t.Fatalf("coalesce for an unknown session: %v", err)
	}
	if other != 0 {
		t.Errorf("an unknown session retired %d rows, want 0", other)
	}
}

// ---------------------------------------------------------------------------
// admission replay
// ---------------------------------------------------------------------------

// TestD1VectorOutboxAdmissionReplayRefreshesOnlyWhatItOwns pins the
// lease-aware replay branch, which is the one enqueue path that may OVERWRITE an
// existing row instead of refusing to.
//
// Every refusal below protects a document another actor owns: a live lease
// belongs to a worker applying the document right now, a newer sibling is a
// payload that has already replaced this one, and a retracted source means the
// document must not exist at all. Without those the replay would be a silent
// data loss.
func TestD1VectorOutboxAdmissionReplayRefreshesOnlyWhatItOwns(t *testing.T) {
	st, conn := newD1TestStore(t)
	live, dead := d1OutboxSeedRevisions(t, conn, "s1")
	ctx := WithMemoryAdmissionVectorReplay(context.Background(), true, false)
	plain := context.Background()
	now := time.Date(2026, 8, 30, 9, 0, 0, 0, time.UTC)
	documentID := "memory:s1:replayed"

	// Outside a replay the admission enqueue IS the plain enqueue, including
	// its idempotency: a second identical request must not write a second row.
	first := d1OutboxItemRequest("s1", live, documentID)
	if inserted, err := d1OutboxEnqueueAdmissionVectorOperation(plain, st, first); !inserted || err != nil {
		t.Fatalf("plain admission enqueue: inserted=%v err=%v", inserted, err)
	}
	replayOfSame := d1OutboxItemRequest("s1", live, documentID)
	if inserted, err := d1OutboxEnqueueAdmissionVectorOperation(plain, st, replayOfSame); inserted || err != nil {
		t.Fatalf("plain admission replay must behave as a plain replay: inserted=%v err=%v", inserted, err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != 1 {
		t.Fatalf("outbox rows = %d, want 1", got)
	}

	// A stuck operation is refreshed: the payload is replaced, the attempt
	// counter is reset, and the retry/lease/error columns are cleared. Leaving
	// the old attempt count would make the next failure look like the hundredth
	// and trip any backoff the worker derives from it.
	stuckID := d1OutboxRead(t, conn, 1).id
	if _, err := conn.Exec(context.Background(), `
		UPDATE memory_vector_outbox
		SET status = 'retryable', attempts = 7, last_error = 'chroma_unreachable',
		    document_json = '{"ID":"stale"}'
		WHERE id = ?`, stuckID); err != nil {
		t.Fatalf("mark the operation stuck: %v", err)
	}
	refresh := d1OutboxItemRequest("s1", live, documentID)
	refresh.DocumentJSON = `{"ID":"` + documentID + `","v":2}`
	refresh.UpdatedAt = now
	refreshed, err := d1OutboxEnqueueAdmissionVectorOperation(ctx, st, refresh)
	if err != nil {
		t.Fatalf("replay refresh: %v", err)
	}
	if !refreshed {
		t.Error("refreshing an existing operation must report true, as the reference does")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != 1 {
		t.Errorf("a refresh must not insert a row: %d rows", got)
	}
	stored := d1OutboxRead(t, conn, stuckID)
	if stored.documentJSON != refresh.DocumentJSON {
		t.Errorf("document = %q, want the refreshed payload", stored.documentJSON)
	}
	if stored.status != "pending" || stored.attempts != 0 || stored.lastError != "" {
		t.Errorf("refreshed row = %+v, want a clean pending first attempt", stored)
	}

	// A live lease is refused. Overwriting the document of an operation a worker
	// is applying right now would make that worker's completion land on content
	// it never sent.
	leasedID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s1", sourceRevision: live, documentID: "memory:s1:leased",
		documentJSON: d1OutboxStrPtr(`{"ID":"memory:s1:leased"}`),
		status:       "leased", leaseOwner: d1OutboxStrPtr("worker"),
		leaseUntil: d1OutboxTimePtr(time.Now().UTC().Add(time.Hour)),
		createdAt:  now, updatedAt: now,
	})
	leased := d1OutboxItemRequest("s1", live, "memory:s1:leased")
	if _, err := d1OutboxEnqueueAdmissionVectorOperation(ctx, st, leased); !errors.Is(err, ErrMemoryReprocessingLeased) {
		t.Errorf("replay onto a live lease = %v, want ErrMemoryReprocessingLeased", err)
	}
	if got := d1OutboxRead(t, conn, leasedID); got.status != "leased" || got.documentJSON != `{"ID":"memory:s1:leased"}` {
		t.Errorf("the leased operation was modified: %+v", got)
	}

	// A retracted source refuses the refresh outright, and the stored operation
	// survives untouched: a replay that un-retracts a document is exactly the
	// failure the source fence exists to prevent.
	deadID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s1", sourceRevision: dead, documentID: "memory:s1:dead",
		documentJSON: d1OutboxStrPtr(`{"ID":"memory:s1:dead"}`),
		createdAt:    now, updatedAt: now,
	})
	_, err = d1OutboxEnqueueAdmissionVectorOperation(ctx, st, d1OutboxItemRequest("s1", dead, "memory:s1:dead"))
	if !errors.Is(err, ErrSourceRevisionStale) {
		t.Errorf("replay against a retracted source = %v, want ErrSourceRevisionStale", err)
	}
	if got := d1OutboxRead(t, conn, deadID); got.status != "pending" ||
		got.documentJSON != `{"ID":"memory:s1:dead"}` {
		t.Errorf("the retracted operation was modified: %+v", got)
	}

	// A newer operation for the same document supersedes the older one, so the
	// older must not be refreshed in front of it.
	olderID := d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s1", sourceRevision: live, documentID: "memory:s1:ordered",
		documentJSON: d1OutboxStrPtr(`{"ID":"memory:s1:ordered","v":1}`),
		createdAt:    now, updatedAt: now,
	})
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s1", sourceRevision: live, documentID: "memory:s1:ordered",
		operationKey: memoryVectorOperationKey("upsert", "s1", live, "memory:s1:ordered") + "-v2",
		documentJSON: d1OutboxStrPtr(`{"ID":"memory:s1:ordered","v":2}`),
		createdAt:    now.Add(time.Second), updatedAt: now,
	})
	superseded := d1OutboxItemRequest("s1", live, "memory:s1:ordered")
	superseded.DocumentJSON = `{"ID":"memory:s1:ordered","v":99}`
	if _, err := d1OutboxEnqueueAdmissionVectorOperation(ctx, st, superseded); err == nil ||
		!strings.Contains(err.Error(), "superseded by a newer operation") {
		t.Errorf("replay of a superseded operation = %v, want the superseded refusal", err)
	}
	if got := d1OutboxRead(t, conn, olderID); got.documentJSON != `{"ID":"memory:s1:ordered","v":1}` {
		t.Errorf("the superseded payload was rewritten: %+v", got)
	}

	// A key that describes two different operations is a conflict, not a
	// refresh: refreshing it would attach this document to an operation that
	// was queued for another one.
	if _, err := d1OutboxEnqueueAdmissionVectorOperation(ctx, st, &MemoryVectorOutboxItem{
		OperationKey: memoryVectorOperationKey("upsert", "s1", live, "memory:s1:replayed"),
		Operation:    "upsert", ChatSessionID: "s1", SourceRevision: live,
		DocumentID: "memory:s1:somewhere-else", DocumentJSON: `{"ID":"elsewhere"}`,
		RequiredSourceState: "active",
	}); err == nil || !strings.Contains(err.Error(), "idempotency conflict") {
		t.Errorf("replay with a mismatched identity = %v, want an idempotency conflict", err)
	}

	// The replay still validates before it touches anything, exactly as the
	// plain enqueue does.
	if _, err := d1OutboxEnqueueAdmissionVectorOperation(ctx, st, &MemoryVectorOutboxItem{
		OperationKey: "  ", Operation: "upsert",
	}); err == nil {
		t.Error("a replay with no operation key must be refused")
	}
	if _, err := d1OutboxEnqueueAdmissionVectorOperation(ctx, st, &MemoryVectorOutboxItem{
		OperationKey: memoryVectorOperationKey("upsert", "s1", live, "memory:s1:broken"),
		Operation:    "upsert", ChatSessionID: "s1", SourceRevision: live,
		DocumentID: "memory:s1:broken", DocumentJSON: `{"broken":`,
	}); err == nil {
		t.Error("a replay with an invalid document payload must be refused")
	}
}

// ---------------------------------------------------------------------------
// capability advertisement
// ---------------------------------------------------------------------------

// TestD1VectorOutboxCapabilitiesAreAdvertised pins the delivery state through
// the manifest. These three are discovered by type assertion, so a provider that
// implemented the methods but was not discoverable would still answer
// "vector outbox unavailable" in a route trace, and the queue would silently
// stop draining.
func TestD1VectorOutboxCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var asStore Store = st
	if _, ok := asStore.(MemoryVectorOutboxStore); !ok {
		t.Error("the D1 provider must expose MemoryVectorOutboxStore")
	}
	if _, ok := asStore.(MemoryVectorOutboxLaneStore); !ok {
		t.Error("the D1 provider must expose MemoryVectorOutboxLaneStore")
	}
	if _, ok := asStore.(MemoryVectorOutboxMaintenanceStore); !ok {
		t.Error("the D1 provider must expose MemoryVectorOutboxMaintenanceStore")
	}
	implemented := map[string]bool{}
	for _, status := range CapabilityReport(asStore) {
		implemented[status.Name] = status.Implemented
	}
	for _, name := range []string{
		"MemoryVectorOutboxStore", "MemoryVectorOutboxLaneStore", "MemoryVectorOutboxMaintenanceStore",
	} {
		if !implemented[name] {
			t.Errorf("the capability manifest reports %s as missing", name)
		}
	}
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

// d1OutboxStrPtr and d1OutboxTimePtr address the nullable outbox columns from a
// seed. They exist because a fixture that wants "this column was never set" must
// say so explicitly: passing a zero value would store an empty string where the
// reference stores NULL, and the COALESCE in the enqueue's identity check would
// then compare an empty document against a real one.
func d1OutboxStrPtr(value string) *string { return &value }

func d1OutboxTimePtr(value time.Time) *time.Time { return &value }
