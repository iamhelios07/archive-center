package vector

import (
	"context"
	"testing"
	"time"
)

type mutationFenceProbeStore struct {
	VectorStore
	listEntered  chan struct{}
	releaseList  chan struct{}
	getEntered   chan []string
	releaseGet   chan struct{}
	upsertCalled chan struct{}
}

func (s *mutationFenceProbeStore) ListDocuments(context.Context, string) ([]VectorDocument, error) {
	close(s.listEntered)
	<-s.releaseList
	return nil, nil
}

func (s *mutationFenceProbeStore) Upsert(context.Context, string, []VectorDocument) error {
	close(s.upsertCalled)
	return nil
}

func (s *mutationFenceProbeStore) GetDocuments(_ context.Context, ids []string) ([]VectorDocument, error) {
	s.getEntered <- append([]string(nil), ids...)
	<-s.releaseGet
	return []VectorDocument{{ID: ids[0]}}, nil
}

func TestMutationFenceBlocksConcurrentVectorWriterAcrossMigrationProofWindow(t *testing.T) {
	probe := &mutationFenceProbeStore{
		VectorStore:  NewFakeVectorStore(),
		listEntered:  make(chan struct{}),
		releaseList:  make(chan struct{}),
		upsertCalled: make(chan struct{}),
	}
	wrapped := NewMutationFencedStore(probe)
	fencer, ok := wrapped.(MutationFencer)
	if !ok {
		t.Fatal("mutation-fenced store does not expose fence callback")
	}
	fenceDone := make(chan error, 1)
	go func() {
		fenceDone <- fencer.WithExclusiveMutationFence(context.Background(), func(raw VectorStore) error {
			_, err := raw.(DocumentLister).ListDocuments(context.Background(), "target")
			return err
		})
	}()
	<-probe.listEntered
	upsertDone := make(chan error, 1)
	go func() {
		upsertDone <- wrapped.Upsert(context.Background(), "target", []VectorDocument{{ID: "late"}})
	}()
	select {
	case <-probe.upsertCalled:
		t.Fatal("concurrent vector writer crossed the exclusive proof fence")
	case <-time.After(25 * time.Millisecond):
	}
	close(probe.releaseList)
	if err := <-fenceDone; err != nil {
		t.Fatal(err)
	}
	if err := <-upsertDone; err != nil {
		t.Fatal(err)
	}
}

func TestMutationFenceForwardsExactDocumentReadsAndBlocksWriter(t *testing.T) {
	probe := &mutationFenceProbeStore{
		VectorStore:  NewFakeVectorStore(),
		getEntered:   make(chan []string, 1),
		releaseGet:   make(chan struct{}),
		upsertCalled: make(chan struct{}),
	}
	wrapped := NewMutationFencedStore(probe)
	reader, ok := wrapped.(ExactDocumentReader)
	if !ok {
		t.Fatal("mutation-fenced store does not forward exact document reads")
	}
	readDone := make(chan []VectorDocument, 1)
	go func() {
		docs, err := reader.GetDocuments(context.Background(), []string{"doc-a", "doc-b"})
		if err != nil {
			t.Errorf("GetDocuments: %v", err)
			readDone <- nil
			return
		}
		readDone <- docs
	}()
	ids := <-probe.getEntered
	if len(ids) != 2 || ids[0] != "doc-a" || ids[1] != "doc-b" {
		t.Fatalf("forwarded ids = %#v", ids)
	}
	upsertDone := make(chan error, 1)
	go func() {
		upsertDone <- wrapped.Upsert(context.Background(), "target", []VectorDocument{{ID: "late"}})
	}()
	select {
	case <-probe.upsertCalled:
		t.Fatal("concurrent vector writer crossed the exact-read fence")
	case <-time.After(25 * time.Millisecond):
	}
	close(probe.releaseGet)
	docs := <-readDone
	if len(docs) != 1 || docs[0].ID != "doc-a" {
		t.Fatalf("forwarded documents = %#v", docs)
	}
	if err := <-upsertDone; err != nil {
		t.Fatal(err)
	}
}

type mutationFenceAsyncProbe struct {
	VectorStore
	awaitIDs        []string
	acceleratorDocs []VectorDocument
}

func (s *mutationFenceAsyncProbe) AwaitVisible(_ context.Context, ids []string, _ time.Duration) error {
	s.awaitIDs = append([]string(nil), ids...)
	return nil
}

func (s *mutationFenceAsyncProbe) GetAcceleratorDocuments(_ context.Context, ids []string) ([]VectorDocument, error) {
	return append([]VectorDocument(nil), s.acceleratorDocs...), nil
}

func TestMutationFenceForwardsAsyncVisibilityAndAcceleratorReads(t *testing.T) {
	probe := &mutationFenceAsyncProbe{
		VectorStore:     NewFakeVectorStore(),
		acceleratorDocs: []VectorDocument{{ID: "accelerator-only"}},
	}
	wrapped := NewMutationFencedStore(probe)
	if !HasVisibilityWaiter(wrapped) {
		t.Fatal("mutation fence hid the delegate visibility waiter")
	}
	waiter, ok := wrapped.(VectorVisibilityWaiter)
	if !ok {
		t.Fatal("mutation fence does not forward VectorVisibilityWaiter")
	}
	if err := waiter.AwaitVisible(context.Background(), []string{"doc-a"}, time.Second); err != nil {
		t.Fatalf("AwaitVisible: %v", err)
	}
	if len(probe.awaitIDs) != 1 || probe.awaitIDs[0] != "doc-a" {
		t.Fatalf("forwarded visibility ids = %#v", probe.awaitIDs)
	}
	reader, ok := wrapped.(AcceleratorExactDocumentReader)
	if !ok {
		t.Fatal("mutation fence does not forward AcceleratorExactDocumentReader")
	}
	docs, err := reader.GetAcceleratorDocuments(context.Background(), []string{"doc-a"})
	if err != nil {
		t.Fatalf("GetAcceleratorDocuments: %v", err)
	}
	if len(docs) != 1 || docs[0].ID != "accelerator-only" {
		t.Fatalf("accelerator documents = %#v", docs)
	}
}

func TestMutationFenceDoesNotMarkSynchronousDelegateAsAsync(t *testing.T) {
	wrapped := NewMutationFencedStore(NewFakeVectorStore())
	if !HasVisibilityWaiter(wrapped) {
		return
	}
	t.Fatal("mutation fence must not classify a synchronous delegate as asynchronous")
}

type mutationFenceExactOnlyProbe struct {
	VectorStore
}

func (s *mutationFenceExactOnlyProbe) GetDocuments(_ context.Context, ids []string) ([]VectorDocument, error) {
	return []VectorDocument{{ID: ids[0], DocumentText: "synchronous exact read"}}, nil
}

func TestMutationFenceAcceleratorReaderFallsBackForSynchronousExactStore(t *testing.T) {
	wrapped := NewMutationFencedStore(&mutationFenceExactOnlyProbe{VectorStore: NewFakeVectorStore()})
	reader, ok := wrapped.(AcceleratorExactDocumentReader)
	if !ok {
		t.Fatal("mutation fence does not expose the outbox accelerator-read surface")
	}
	docs, err := reader.GetAcceleratorDocuments(context.Background(), []string{"doc-a"})
	if err != nil {
		t.Fatalf("GetAcceleratorDocuments: %v", err)
	}
	if len(docs) != 1 || docs[0].ID != "doc-a" || docs[0].DocumentText != "synchronous exact read" {
		t.Fatalf("synchronous fallback documents = %#v", docs)
	}
}
