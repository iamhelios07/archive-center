package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/store"
	"github.com/risulongmemory/archive-center-go/internal/vector"
)

// These tests pin the interaction between the memory vector outbox and a vector
// store whose writes become visible AFTER the write is acknowledged.
//
// The outbox verifies an upsert by reading the document back immediately and
// requiring an exact count of one. That verification assumes a synchronously
// applied write. On an eventually consistent index — which is what Vectorize is —
// the readback legitimately returns nothing for a short window, and the
// consequences are not cosmetic:
//
//	failed readback   -> one attempt consumed
//	attempts exhausted -> MEMORY_VECTOR_RETRY_LIMIT_REACHED, parked PERMANENTLY
//
// so a write that actually succeeded ends with its document missing from the
// index forever, plus an operator-visible failure. The fix is
// vector.VectorVisibilityWaiter: a store that applies writes asynchronously is
// given a bounded wait before the readback, and a store that does not implement
// it is read back immediately exactly as before.

// eventualVectorStore wraps a synchronous store and can be told to hide a write
// until the visibility waiter asks for it.
type eventualVectorStore struct {
	*memoryVectorProcessorVector

	mu sync.Mutex
	// hidden holds document ids the store has accepted but not yet applied.
	hidden map[string]bool
	// waitBudgets records the budgets the processor offered, so a test can prove
	// the waiter was consulted at all rather than merely that the read succeeded.
	waitBudgets []time.Duration
	// hideWaiterSet removes the VectorVisibilityWaiter capability, which is how
	// the unfixed path is reproduced.
	hideWaiterSet bool
}

func newEventualVectorStore() *eventualVectorStore {
	return &eventualVectorStore{
		memoryVectorProcessorVector: &memoryVectorProcessorVector{VectorStore: vector.NewFakeVectorStore()},
		hidden:                      map[string]bool{},
	}
}

// withoutWaiter returns a view that still HIDES pending writes but does not
// expose vector.VectorVisibilityWaiter.
//
// The hiding behaviour has to survive the wrapper or the test would prove
// nothing: a view that read through to the inner store would make the readback
// succeed and the missing waiter invisible.
func (s *eventualVectorStore) withoutWaiter() vector.VectorStore {
	return struct {
		vector.VectorStore
		vector.DocumentDeleter
		vector.ExactDocumentReader
	}{s.memoryVectorProcessorVector, s.memoryVectorProcessorVector, s}
}

func (s *eventualVectorStore) hide(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hidden[id] = true
}

func (s *eventualVectorStore) GetDocuments(ctx context.Context, ids []string) ([]vector.VectorDocument, error) {
	s.mu.Lock()
	visible := make([]string, 0, len(ids))
	for _, id := range ids {
		if s.hidden[id] {
			continue
		}
		visible = append(visible, id)
	}
	s.mu.Unlock()
	return s.memoryVectorProcessorVector.GetDocuments(ctx, visible)
}

func (s *eventualVectorStore) AwaitVisible(_ context.Context, ids []string, budget time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waitBudgets = append(s.waitBudgets, budget)
	for _, id := range ids {
		delete(s.hidden, id)
	}
	return nil
}

func (s *eventualVectorStore) budgetsOffered() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.waitBudgets...)
}

// eventualVectorOutboxItem builds a ready-to-process upsert item whose document
// passes readback verification: the verification requires a source revision, a
// contract, an index identity, and a content fingerprint over the document text.
// It carries a materialisation model so the operation can reach the completed
// state rather than stopping at the materialisation contract.
func eventualVectorOutboxItem(t *testing.T, id int64, documentID, revision, sessionID string) *store.MemoryVectorOutboxItem {
	t.Helper()
	document := verifiedMemoryVectorProcessorDocument(vector.VectorDocument{
		ID: documentID, ChatSessionID: sessionID, SourceTable: "memories", SourceRowID: "1",
		SchemaVersion: "memory.v1", DocumentText: "an accepted write",
		Embedding: []float32{0.1, 0.2},
	}, revision)
	document.Metadata["embedding_model"] = "test-model"
	documentJSON, err := materializedMemoryVectorDocumentJSON(document)
	if err != nil {
		t.Fatalf("materializedMemoryVectorDocumentJSON: %v", err)
	}
	return &store.MemoryVectorOutboxItem{
		ID: id, Operation: "upsert", ChatSessionID: sessionID, SourceRevision: revision,
		DocumentID: documentID, DocumentJSON: documentJSON, EmbeddingReady: true,
		RequiredSourceState: "active", Status: "pending",
	}
}

func newEventualVectorServer(t *testing.T, vec vector.VectorStore, items ...*store.MemoryVectorOutboxItem) *Server {
	t.Helper()
	st := &memoryVectorProcessorStore{Store: store.NewNoopStore(), items: items}
	return &Server{
		Store:  st,
		Vector: vec,
		RuntimeConfig: RuntimeConfig{
			Synced: true, FailedQueueMaxAttempts: 4,
		},
	}
}

// TestOutboxUpsertWaitsForVisibilityOnAnAsynchronousStore is the regression.
//
// Without the waiter, an upsert the store has accepted but not applied fails its
// readback, consumes an attempt, and at the limit is parked permanently with its
// document missing from the index. The operation must complete on the first
// attempt, and the store must have been given a bounded wait.
func TestOutboxUpsertWaitsForVisibilityOnAnAsynchronousStore(t *testing.T) {
	inner := newEventualVectorStore()
	inner.hide("memory:sess-v:1")
	item := eventualVectorOutboxItem(t, 1, "memory:sess-v:1", "revision-v-1", "sess-v")
	server := newEventualVectorServer(t, inner, item)

	result, err := server.processMemoryVectorOutboxOnce(context.Background(), "worker", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("processMemoryVectorOutboxOnce: %v", err)
	}
	if result.CanonicalState != "completed" {
		t.Fatalf("canonical state = %q (failure %q), want completed; an accepted asynchronous write must not be parked",
			result.CanonicalState, result.Failure)
	}
	budgets := inner.budgetsOffered()
	if len(budgets) != 1 {
		t.Fatalf("visibility budgets offered = %v, want exactly one", budgets)
	}
	if budgets[0] <= 0 {
		t.Errorf("visibility budget = %s, want a positive bound", budgets[0])
	}
	if budgets[0] > memoryVectorRetryDelay {
		t.Errorf("visibility budget = %s, want no longer than the retry delay %s; a longer wait holds the lease for longer than the caller would have waited anyway",
			budgets[0], memoryVectorRetryDelay)
	}
}

// TestOutboxUpsertWithoutAVisibilityWaiterIsUnchanged pins that the synchronous
// path is untouched. A store that does not implement the waiter must not be
// offered a budget at all, and must be read back immediately as before.
func TestOutboxUpsertWithoutAVisibilityWaiterIsUnchanged(t *testing.T) {
	inner := newEventualVectorStore()
	// No wait is offered, so the hidden write stays hidden and the readback fails
	// exactly as it did before the waiter existed. That is the point: the
	// synchronous path is untouched, including its failure mode.
	inner.hide("memory:sess-v:2")
	item := eventualVectorOutboxItem(t, 2, "memory:sess-v:2", "revision-v-2", "sess-v")
	server := newEventualVectorServer(t, inner.withoutWaiter(), item)

	result, err := server.processMemoryVectorOutboxOnce(context.Background(), "worker", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("processMemoryVectorOutboxOnce: %v", err)
	}
	if budgets := inner.budgetsOffered(); len(budgets) != 0 {
		t.Errorf("a store without the waiter was offered a budget %v; the synchronous path must be unchanged", budgets)
	}
	if result.CanonicalState == "completed" {
		t.Fatal("a hidden write read back as absent must not be reported as completed")
	}
	if result.Failure == "" {
		t.Error("a failed readback must say why")
	}
}

// TestOutboxUpsertRepeatedVisibilityFailureConsumesAttemptsAndParks pins the
// consequence the waiter exists to prevent, so the fix is known to be load
// bearing rather than cosmetic: a store that never becomes visible drives the
// outbox to its attempt limit and then parks permanently.
// visibilityPendingProcessorStore opts a test outbox into the durable pending
// capability. Keeping it separate from memoryVectorProcessorStore preserves the
// legacy-path tests above, which intentionally assert the old retry behaviour.
type visibilityPendingProcessorStore struct {
	*memoryVectorProcessorStore
	deferredPayloads []string
}

func (s *visibilityPendingProcessorStore) DeferMemoryVectorVisibility(_ context.Context, _ int64, _ string, _ time.Time, _ time.Time, documentJSON string) error {
	s.deferredPayloads = append(s.deferredPayloads, documentJSON)
	return nil
}

// acknowledgedInvisibleVectorStore models the Vectorize observation from Gate
// 2: the mutation and visibility probe succeed, yet the immediately following
// exact read remains empty. The processor must persist that as visibility-pending
// instead of re-upserting it or consuming a retry attempt.
type acknowledgedInvisibleVectorStore struct {
	*memoryVectorProcessorVector
	awaitCalls int
	awaitErr   error
}

func (s *acknowledgedInvisibleVectorStore) AwaitVisible(context.Context, []string, time.Duration) error {
	s.awaitCalls++
	return s.awaitErr
}

func (s *acknowledgedInvisibleVectorStore) GetDocuments(context.Context, []string) ([]vector.VectorDocument, error) {
	return nil, nil
}

func TestOutboxUpsertDefersAcknowledgedButUnreadableVectorizeWrite(t *testing.T) {
	item := eventualVectorOutboxItem(t, 11, "memory:sess-v:pending", "revision-v-pending", "sess-v")
	base := &memoryVectorProcessorStore{Store: store.NewNoopStore(), items: []*store.MemoryVectorOutboxItem{item}}
	outbox := &visibilityPendingProcessorStore{memoryVectorProcessorStore: base}
	accelerator := &acknowledgedInvisibleVectorStore{
		memoryVectorProcessorVector: &memoryVectorProcessorVector{VectorStore: vector.NewFakeVectorStore()},
	}
	server := &Server{Store: outbox, Vector: accelerator, RuntimeConfig: RuntimeConfig{Synced: true, FailedQueueMaxAttempts: 4}}

	result, err := server.processMemoryVectorOutboxOnce(context.Background(), "worker", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("processMemoryVectorOutboxOnce: %v", err)
	}
	if result.CanonicalState != "visibility_pending" || !result.VectorApplied {
		t.Fatalf("result = %+v, want accepted visibility_pending state", result)
	}
	if len(base.failed) != 0 {
		t.Fatalf("visibility pending called FailMemoryVectorOperation for %v", base.failed)
	}
	if len(outbox.deferredPayloads) != 1 {
		t.Fatalf("deferred payloads = %d, want 1", len(outbox.deferredPayloads))
	}
	if len(accelerator.upserts) != 1 || accelerator.awaitCalls != 1 {
		t.Fatalf("upserts=%d awaits=%d, want one accepted write and one probe", len(accelerator.upserts), accelerator.awaitCalls)
	}

	// A later lease retains the marker. It must probe visibility only: replaying
	// the same mutation would compound Vectorize's propagation window.
	retry := *item
	retry.Status = "retryable"
	retry.LastError = store.MemoryVectorVisibilityPendingMarker
	outbox.items = []*store.MemoryVectorOutboxItem{&retry}
	result, err = server.processMemoryVectorOutboxOnce(context.Background(), "worker", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("second processMemoryVectorOutboxOnce: %v", err)
	}
	if result.CanonicalState != "visibility_pending" {
		t.Fatalf("second result state = %q, want visibility_pending", result.CanonicalState)
	}
	if len(accelerator.upserts) != 1 {
		t.Fatalf("visibility-pending lease replayed Upsert %d times, want 1", len(accelerator.upserts))
	}
}

func TestOutboxBlockingFacadeDefersVisibilityTimeoutWithoutRetryingMutation(t *testing.T) {
	item := eventualVectorOutboxItem(t, 12, "memory:sess-v:blocking", "revision-v-blocking", "sess-v")
	base := &memoryVectorProcessorStore{Store: store.NewNoopStore(), items: []*store.MemoryVectorOutboxItem{item}}
	outbox := &visibilityPendingProcessorStore{memoryVectorProcessorStore: base}
	accelerator := &acknowledgedInvisibleVectorStore{
		memoryVectorProcessorVector: &memoryVectorProcessorVector{VectorStore: vector.NewFakeVectorStore()},
		awaitErr:                    context.DeadlineExceeded,
	}
	server := &Server{
		Store:         outbox,
		Vector:        vector.NewVisibilityBlockingVectorStore(accelerator),
		RuntimeConfig: RuntimeConfig{Synced: true, FailedQueueMaxAttempts: 4},
	}

	result, err := server.processMemoryVectorOutboxOnce(context.Background(), "worker", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("processMemoryVectorOutboxOnce: %v", err)
	}
	if result.CanonicalState != "visibility_pending" || !result.VectorApplied {
		t.Fatalf("result = %+v, want accepted visibility_pending state", result)
	}
	if len(base.failed) != 0 || len(outbox.deferredPayloads) != 1 {
		t.Fatalf("failed=%v deferred=%d, want no retry consumption and one durable deferral", base.failed, len(outbox.deferredPayloads))
	}
	if len(accelerator.upserts) != 1 || accelerator.awaitCalls != 1 {
		t.Fatalf("upserts=%d awaits=%d, want one mutation and one blocking wait", len(accelerator.upserts), accelerator.awaitCalls)
	}
}

// canonicalReadbackProvider models the D1 manifest being present before a
// Vectorize replica exposes the same document. The outbox must not treat this
// canonical row as accelerator proof after all production wrappers are applied.
type canonicalReadbackProvider struct {
	docs map[string]vector.VectorDocument
}

func (p canonicalReadbackProvider) ListCanonicalVectorDocuments(_ context.Context, sessionID string) ([]vector.VectorDocument, error) {
	var out []vector.VectorDocument
	for _, doc := range p.docs {
		if doc.ChatSessionID == sessionID {
			out = append(out, doc)
		}
	}
	return out, nil
}

func (p canonicalReadbackProvider) GetCanonicalVectorDocuments(_ context.Context, ids []string) ([]vector.VectorDocument, error) {
	out := make([]vector.VectorDocument, 0, len(ids))
	for _, id := range ids {
		if doc, ok := p.docs[id]; ok {
			out = append(out, doc)
		}
	}
	return out, nil
}

func TestOutboxFencedCanonicalVectorizeReadbackBypassesCanonicalFallback(t *testing.T) {
	item := eventualVectorOutboxItem(t, 13, "memory:sess-v:fenced", "revision-v-fenced", "sess-v")
	var canonicalDoc vector.VectorDocument
	if err := json.Unmarshal([]byte(item.DocumentJSON), &canonicalDoc); err != nil {
		t.Fatalf("unmarshal canonical document: %v", err)
	}
	base := &memoryVectorProcessorStore{Store: store.NewNoopStore(), items: []*store.MemoryVectorOutboxItem{item}}
	outbox := &visibilityPendingProcessorStore{memoryVectorProcessorStore: base}
	raw := &acknowledgedInvisibleVectorStore{
		memoryVectorProcessorVector: &memoryVectorProcessorVector{VectorStore: vector.NewFakeVectorStore()},
	}
	blocking := vector.NewVisibilityBlockingVectorStore(raw)
	canonical := vector.NewCanonicalVectorStore(blocking, canonicalReadbackProvider{docs: map[string]vector.VectorDocument{canonicalDoc.ID: canonicalDoc}})
	server := &Server{
		Store:         outbox,
		Vector:        vector.NewMutationFencedStore(canonical),
		RuntimeConfig: RuntimeConfig{Synced: true, FailedQueueMaxAttempts: 4},
	}

	result, err := server.processMemoryVectorOutboxOnce(context.Background(), "worker", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("processMemoryVectorOutboxOnce: %v", err)
	}
	if result.CanonicalState != "visibility_pending" || !result.VectorApplied {
		t.Fatalf("result = %+v, want accepted visibility_pending despite D1 canonical readback", result)
	}
	if len(base.failed) != 0 || len(outbox.deferredPayloads) != 1 {
		t.Fatalf("failed=%v deferred=%d, want no retry consumption and one durable deferral", base.failed, len(outbox.deferredPayloads))
	}
	if len(raw.upserts) != 1 || raw.awaitCalls != 2 {
		t.Fatalf("upserts=%d awaits=%d, want one mutation and blocking plus outbox visibility checks", len(raw.upserts), raw.awaitCalls)
	}
}

func TestOutboxUpsertRepeatedVisibilityFailureConsumesAttemptsAndParks(t *testing.T) {
	neverVisible := &neverVisibleVectorStore{VectorStore: vector.NewFakeVectorStore()}
	document := verifiedMemoryVectorProcessorDocument(vector.VectorDocument{
		ID: "memory:sess-v:3", ChatSessionID: "sess-v", SourceTable: "memories", SourceRowID: "1",
		SchemaVersion: "memory.v1", DocumentText: "never visible",
		Embedding: []float32{0.1, 0.2},
	}, "revision-v-3")
	documentJSON, err := materializedMemoryVectorDocumentJSON(document)
	if err != nil {
		t.Fatalf("materializedMemoryVectorDocumentJSON: %v", err)
	}

	states := []string{}
	for attempt := int64(0); attempt < 4; attempt++ {
		item := &store.MemoryVectorOutboxItem{
			ID: int64(attempt + 1), Operation: "upsert", ChatSessionID: "sess-v",
			SourceRevision: "revision-v-3", DocumentID: document.ID, DocumentJSON: documentJSON,
			EmbeddingReady: true, RequiredSourceState: "active", Status: "pending", Attempts: int(attempt),
		}
		st := &memoryVectorProcessorStore{Store: store.NewNoopStore(), items: []*store.MemoryVectorOutboxItem{item}}
		server := &Server{
			Store: st, Vector: neverVisible,
			RuntimeConfig: RuntimeConfig{Synced: true, FailedQueueMaxAttempts: 4},
		}
		result, err := server.processMemoryVectorOutboxOnce(context.Background(), "worker", time.Now().UTC(), time.Minute)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		states = append(states, result.CanonicalState)
	}
	for i, state := range states {
		if state == "completed" {
			t.Fatalf("attempt %d completed against a store that never becomes visible", i)
		}
	}
	last := states[len(states)-1]
	if last != "permanent" {
		t.Errorf("final canonical state = %q, want permanent once the attempt budget is exhausted; got %v", last, states)
	}
}

// neverVisibleVectorStore acknowledges writes and never makes them readable. It
// deliberately does NOT implement the waiter, so the wait cannot rescue it: this
// models a genuinely stuck mutation.
type neverVisibleVectorStore struct {
	vector.VectorStore
}

func (s *neverVisibleVectorStore) Upsert(_ context.Context, _ string, _ []vector.VectorDocument) error {
	return nil
}

func (s *neverVisibleVectorStore) GetDocuments(_ context.Context, _ []string) ([]vector.VectorDocument, error) {
	return nil, nil
}

func (s *neverVisibleVectorStore) DeleteDocuments(_ context.Context, _ []string) error { return nil }

// TestOutboxReportsARejectedDeleteLoudly keeps the other half of the contract: a
// delete the store refuses must fail visibly rather than be reported complete.
func TestOutboxReportsARejectedDeleteLoudly(t *testing.T) {
	vec := &memoryVectorProcessorVector{
		VectorStore: vector.NewFakeVectorStore(),
		deleteErr:   errors.New("vector store rejected the delete"),
	}
	item := &store.MemoryVectorOutboxItem{
		ID: 7, Operation: "delete", ChatSessionID: "sess-v", DocumentID: "memory:sess-v:7",
		Status: "ready",
	}
	server := newEventualVectorServer(t, vec, item)

	result, err := server.processMemoryVectorOutboxOnce(context.Background(), "worker", time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("processMemoryVectorOutboxOnce: %v", err)
	}
	if result.CanonicalState == "completed" {
		t.Fatal("a rejected delete must not be reported as completed")
	}
	if result.Failure == "" {
		t.Error("a rejected delete must say why")
	}
}
