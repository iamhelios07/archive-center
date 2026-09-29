package vector

import (
	"context"
	"errors"
	"testing"
	"time"
)

type visibilityBlockingTestStore struct {
	VectorStore
	upserts     int
	awaitIDs    []string
	awaitBudget time.Duration
	awaitErr    error
	readback    []VectorDocument
}

func (s *visibilityBlockingTestStore) Upsert(context.Context, string, []VectorDocument) error {
	s.upserts++
	return nil
}

func (s *visibilityBlockingTestStore) GetDocuments(context.Context, []string) ([]VectorDocument, error) {
	return append([]VectorDocument(nil), s.readback...), nil
}

func (s *visibilityBlockingTestStore) AwaitVisible(_ context.Context, ids []string, budget time.Duration) error {
	s.awaitIDs = append([]string(nil), ids...)
	s.awaitBudget = budget
	return s.awaitErr
}

func TestVisibilityBlockingVectorStoreWaitsBeforeReportingUpsertSuccess(t *testing.T) {
	provider := &visibilityBlockingTestStore{VectorStore: NewFakeVectorStore()}
	store := NewVisibilityBlockingVectorStore(provider)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := store.Upsert(ctx, "session", []VectorDocument{{ID: "one"}, {ID: "two"}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if provider.upserts != 1 {
		t.Fatalf("upserts = %d, want 1", provider.upserts)
	}
	if got := provider.awaitIDs; len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("AwaitVisible ids = %v, want [one two]", got)
	}
	if provider.awaitBudget <= 0 || provider.awaitBudget > time.Second {
		t.Fatalf("AwaitVisible budget = %s, want remaining caller deadline", provider.awaitBudget)
	}
}

func TestVisibilityBlockingVectorStoreServesAcceptedDocumentDuringReplicaRegression(t *testing.T) {
	provider := &visibilityBlockingTestStore{VectorStore: NewFakeVectorStore()}
	store := NewVisibilityBlockingVectorStore(provider)
	doc := VectorDocument{ID: "one", ChatSessionID: "session", DocumentText: "new value", Embedding: []float32{1}}
	if err := store.Upsert(context.Background(), "session", []VectorDocument{doc}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	// The remote replica returns an empty (or stale) exact read immediately after
	// it acknowledged visibility. The facade must not make its completed write
	// disappear from the caller-visible contract.
	readback, err := store.(ExactDocumentReader).GetDocuments(context.Background(), []string{"one"})
	if err != nil {
		t.Fatalf("GetDocuments: %v", err)
	}
	if len(readback) != 1 || readback[0].DocumentText != doc.DocumentText {
		t.Fatalf("readback = %#v, want accepted document %#v", readback, doc)
	}
	accelerator, err := store.(AcceleratorExactDocumentReader).GetAcceleratorDocuments(context.Background(), []string{"one"})
	if err != nil {
		t.Fatalf("GetAcceleratorDocuments: %v", err)
	}
	if len(accelerator) != 0 {
		t.Fatalf("accelerator readback = %#v, want unmasked empty replica result", accelerator)
	}
}

func TestVisibilityBlockingVectorStoreReturnsPendingOnVisibilityTimeout(t *testing.T) {
	provider := &visibilityBlockingTestStore{
		VectorStore: NewFakeVectorStore(),
		awaitErr:    context.DeadlineExceeded,
	}
	store := NewVisibilityBlockingVectorStore(provider)

	err := store.Upsert(context.Background(), "session", []VectorDocument{{ID: "one"}})
	if !errors.Is(err, ErrVisibilityPending) {
		t.Fatalf("Upsert error = %v, want ErrVisibilityPending", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Upsert error = %v, want original context deadline cause", err)
	}
	if provider.upserts != 1 || len(provider.awaitIDs) != 1 {
		t.Fatalf("upserts=%d awaits=%v, want one accepted mutation and one visibility wait", provider.upserts, provider.awaitIDs)
	}
}

func TestVisibilityBlockingVectorStoreLeavesSynchronousProviderUnchanged(t *testing.T) {
	store := NewVisibilityBlockingVectorStore(NewFakeVectorStore())
	if err := store.Upsert(context.Background(), "session", []VectorDocument{{ID: "one"}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
}
