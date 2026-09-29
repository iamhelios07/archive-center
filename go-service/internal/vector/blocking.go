package vector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// visibilityBlockingFallback is used only when the caller did not supply a
// deadline. A VectorStore call must never become an unbounded wait merely
// because its caller used context.Background().
const visibilityBlockingFallback = 90 * time.Second

// visibilityBlockingStore adapts an eventually-consistent accelerator to the
// synchronous VectorStore write contract. Upsert returns successfully only once
// the accelerator confirms that every requested ID is exactly readable.
//
// Its context is the caller's cancellation boundary, analogous to a scoped
// runBlocking: the bridge remains asynchronous internally, but its accepted
// mutation is not exposed as a completed VectorStore write until visibility is
// observed. A timeout deliberately returns ErrVisibilityPending rather than a
// false success. The durable outbox can then resume observation without sending
// another upsert.
type visibilityBlockingStore struct {
	delegate VectorStore

	// accepted retains the caller's latest confirmed mutation. Vectorize can
	// briefly answer an exact get with an older replica even after AwaitVisible
	// observed the new one. Serving this overlay to ordinary exact reads keeps a
	// completed Upsert observationally synchronous. AcceleratorExactDocumentReader
	// deliberately bypasses it so the outbox never mistakes cached state for a
	// Vectorize readback.
	mu       sync.RWMutex
	accepted map[string]VectorDocument
}

// NewVisibilityBlockingVectorStore preserves VectorStore's public signatures
// while making providers that implement VectorVisibilityWaiter synchronous to
// their callers. Providers without that optional capability retain their
// established immediate Upsert behaviour.
func NewVisibilityBlockingVectorStore(delegate VectorStore) VectorStore {
	if delegate == nil {
		return nil
	}
	return &visibilityBlockingStore{delegate: delegate, accepted: make(map[string]VectorDocument)}
}

func (s *visibilityBlockingStore) Search(ctx context.Context, sessionID string, embedding []float32, limit int, filter string) ([]VectorDocument, error) {
	return s.delegate.Search(ctx, sessionID, embedding, limit, filter)
}

func (s *visibilityBlockingStore) Upsert(ctx context.Context, sessionID string, docs []VectorDocument) error {
	if err := s.delegate.Upsert(ctx, sessionID, docs); err != nil {
		return err
	}
	waiter, ok := s.delegate.(VectorVisibilityWaiter)
	if !ok || len(docs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		if doc.ID != "" {
			ids = append(ids, doc.ID)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	if err := waiter.AwaitVisible(ctx, ids, visibilityBlockingBudget(ctx)); err != nil {
		return fmt.Errorf("%w: %v", ErrVisibilityPending, err)
	}
	s.rememberAccepted(docs)
	return nil
}

func (s *visibilityBlockingStore) DeleteSession(ctx context.Context, sessionID string) error {
	if err := s.delegate.DeleteSession(ctx, sessionID); err != nil {
		return err
	}
	s.forgetSession(sessionID)
	return nil
}
func (s *visibilityBlockingStore) Rebuild(ctx context.Context, sessionID string) error {
	return s.delegate.Rebuild(ctx, sessionID)
}
func (s *visibilityBlockingStore) Health(ctx context.Context) (HealthSnapshot, error) {
	return s.delegate.Health(ctx)
}
func (s *visibilityBlockingStore) Count(ctx context.Context, sessionID string) (int, error) {
	return s.delegate.Count(ctx, sessionID)
}
func (s *visibilityBlockingStore) Close(ctx context.Context) error { return s.delegate.Close(ctx) }

func (s *visibilityBlockingStore) DeleteDocuments(ctx context.Context, ids []string) error {
	if deleter, ok := s.delegate.(DocumentDeleter); ok {
		if err := deleter.DeleteDocuments(ctx, ids); err != nil {
			return err
		}
		s.forgetAccepted(ids)
		return nil
	}
	return ErrNotEnabled
}
func (s *visibilityBlockingStore) GetDocuments(ctx context.Context, ids []string) ([]VectorDocument, error) {
	if reader, ok := s.delegate.(ExactDocumentReader); ok {
		remote, err := reader.GetDocuments(ctx, ids)
		if err != nil {
			return nil, err
		}
		return s.mergeAccepted(ids, remote), nil
	}
	return nil, ErrNotEnabled
}
func (s *visibilityBlockingStore) GetAcceleratorDocuments(ctx context.Context, ids []string) ([]VectorDocument, error) {
	if reader, ok := s.delegate.(AcceleratorExactDocumentReader); ok {
		return reader.GetAcceleratorDocuments(ctx, ids)
	}
	if reader, ok := s.delegate.(ExactDocumentReader); ok {
		return reader.GetDocuments(ctx, ids)
	}
	return nil, ErrNotEnabled
}
func (s *visibilityBlockingStore) ListDocuments(ctx context.Context, sessionID string) ([]VectorDocument, error) {
	if lister, ok := s.delegate.(DocumentLister); ok {
		return lister.ListDocuments(ctx, sessionID)
	}
	return nil, ErrNotEnabled
}
func (s *visibilityBlockingStore) AwaitVisible(ctx context.Context, ids []string, budget time.Duration) error {
	if waiter, ok := s.delegate.(VectorVisibilityWaiter); ok {
		return waiter.AwaitVisible(ctx, ids, budget)
	}
	return nil
}
func (s *visibilityBlockingStore) QueryExact(ctx context.Context, query ExactQuery) ([]ExactQueryResult, error) {
	if querier, ok := s.delegate.(ExactMetadataQuerier); ok {
		return querier.QueryExact(ctx, query)
	}
	return nil, ErrNotEnabled
}
func (s *visibilityBlockingStore) ResetAll(ctx context.Context) error {
	if resetter, ok := s.delegate.(CollectionResetter); ok {
		return resetter.ResetAll(ctx)
	}
	return ErrNotEnabled
}

func (s *visibilityBlockingStore) rememberAccepted(docs []VectorDocument) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, doc := range docs {
		if id := strings.TrimSpace(doc.ID); id != "" {
			s.accepted[id] = cloneVectorDocument(doc)
		}
	}
}

func (s *visibilityBlockingStore) forgetAccepted(ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.accepted, strings.TrimSpace(id))
	}
}

func (s *visibilityBlockingStore) forgetSession(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, doc := range s.accepted {
		if doc.ChatSessionID == sessionID {
			delete(s.accepted, id)
		}
	}
}

func (s *visibilityBlockingStore) mergeAccepted(ids []string, remote []VectorDocument) []VectorDocument {
	found := make(map[string]VectorDocument, len(remote))
	for _, doc := range remote {
		found[strings.TrimSpace(doc.ID)] = doc
	}
	s.mu.RLock()
	for _, id := range ids {
		if accepted, ok := s.accepted[strings.TrimSpace(id)]; ok {
			found[strings.TrimSpace(id)] = cloneVectorDocument(accepted)
		}
	}
	s.mu.RUnlock()
	out := make([]VectorDocument, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		if doc, ok := found[id]; ok {
			out = append(out, doc)
		}
	}
	return out
}

func cloneVectorDocument(doc VectorDocument) VectorDocument {
	clone := doc
	clone.Embedding = append([]float32(nil), doc.Embedding...)
	if doc.Metadata != nil {
		clone.Metadata = make(map[string]any, len(doc.Metadata))
		for key, value := range doc.Metadata {
			clone.Metadata[key] = value
		}
	}
	return clone
}

func visibilityBlockingBudget(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline)
	}
	return visibilityBlockingFallback
}
