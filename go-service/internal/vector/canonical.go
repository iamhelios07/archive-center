package vector

import (
	"context"
	"sort"
	"strings"
	"time"
)

// CanonicalDocumentProvider supplies the durable document manifest used by an
// eventually-consistent vector accelerator. It is deliberately expressed in
// vector terms so the composition root can pair a D1 provider with Vectorize
// without making this package depend on the storage layer.
type CanonicalDocumentProvider interface {
	ListCanonicalVectorDocuments(context.Context, string) ([]VectorDocument, error)
	GetCanonicalVectorDocuments(context.Context, []string) ([]VectorDocument, error)
}

// DurableSearchOverlayProvider exposes the small, durable delta that may not
// yet be represented by an eventually consistent search accelerator. It is an
// internal composition capability: VectorStore.Search keeps its public
// signature and callers never need to know whether an overlay is present.
//
// A returned snapshot is usable only when Truncated is false. Providers must
// return current visibility-pending upserts and canonical delete masks from one
// logical snapshot, so an old accelerator hit cannot outlive a newer tombstone.
// An empty sessionID requests an aggregate operator snapshot; it is for
// observability only and must never be merged into a session Search.
type DurableSearchOverlayProvider interface {
	DurableSearchOverlay(context.Context, string, int) (DurableSearchOverlaySnapshot, error)
}

// DurableSearchOverlaySnapshot is the D1-owned delta to merge with accelerator
// search results. PendingCount is the total number of pending upserts, not just
// the portion returned under the requested bound. OldestPendingAt is zero when
// no pending upsert exists.
type DurableSearchOverlaySnapshot struct {
	Upserts         []VectorDocument
	TombstoneIDs    []string
	PendingCount    int
	OldestPendingAt time.Time
	Truncated       bool
}

// canonicalStore keeps the public VectorStore contract unchanged while using
// canonical storage for exact lifecycle reads. Similarity search remains the
// accelerator's responsibility.
type canonicalStore struct {
	accelerator VectorStore
	canonical   CanonicalDocumentProvider
}

func NewCanonicalVectorStore(accelerator VectorStore, canonical CanonicalDocumentProvider) VectorStore {
	if accelerator == nil || canonical == nil {
		return accelerator
	}
	return &canonicalStore{accelerator: accelerator, canonical: canonical}
}

func (s *canonicalStore) Search(ctx context.Context, sessionID string, embedding []float32, limit int, filter string) ([]VectorDocument, error) {
	return s.accelerator.Search(ctx, sessionID, embedding, limit, filter)
}
func (s *canonicalStore) Upsert(ctx context.Context, sessionID string, docs []VectorDocument) error {
	return s.accelerator.Upsert(ctx, sessionID, docs)
}
func (s *canonicalStore) DeleteSession(ctx context.Context, sessionID string) error {
	docs, err := s.ListDocuments(ctx, sessionID)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		if id := strings.TrimSpace(doc.ID); id != "" {
			ids = append(ids, id)
		}
	}
	return s.DeleteDocuments(ctx, ids)
}
func (s *canonicalStore) Rebuild(ctx context.Context, sessionID string) error {
	return s.accelerator.Rebuild(ctx, sessionID)
}
func (s *canonicalStore) Health(ctx context.Context) (HealthSnapshot, error) {
	return s.accelerator.Health(ctx)
}
func (s *canonicalStore) Count(ctx context.Context, sessionID string) (int, error) {
	if strings.TrimSpace(sessionID) == "" {
		return s.accelerator.Count(ctx, sessionID)
	}
	docs, err := s.ListDocuments(ctx, sessionID)
	return len(docs), err
}
func (s *canonicalStore) Close(ctx context.Context) error { return s.accelerator.Close(ctx) }
func (s *canonicalStore) DeleteDocuments(ctx context.Context, ids []string) error {
	if deleter, ok := s.accelerator.(DocumentDeleter); ok {
		return deleter.DeleteDocuments(ctx, ids)
	}
	return ErrNotEnabled
}
func (s *canonicalStore) ListDocuments(ctx context.Context, sessionID string) ([]VectorDocument, error) {
	docs, err := s.canonical.ListCanonicalVectorDocuments(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].ID < docs[j].ID })
	return docs, nil
}
func (s *canonicalStore) GetAcceleratorDocuments(ctx context.Context, ids []string) ([]VectorDocument, error) {
	if reader, ok := s.accelerator.(AcceleratorExactDocumentReader); ok {
		return reader.GetAcceleratorDocuments(ctx, ids)
	}
	if reader, ok := s.accelerator.(ExactDocumentReader); ok {
		return reader.GetDocuments(ctx, ids)
	}
	return nil, ErrNotEnabled
}

func (s *canonicalStore) GetDocuments(ctx context.Context, ids []string) ([]VectorDocument, error) {
	canonical, err := s.canonical.GetCanonicalVectorDocuments(ctx, ids)
	if err != nil {
		return nil, err
	}
	found := make(map[string]VectorDocument, len(canonical))
	for _, doc := range canonical {
		found[strings.TrimSpace(doc.ID)] = doc
	}
	if reader, ok := s.accelerator.(ExactDocumentReader); ok {
		remote, err := reader.GetDocuments(ctx, ids)
		if err != nil {
			return nil, err
		}
		for _, doc := range remote {
			if _, ok := found[strings.TrimSpace(doc.ID)]; !ok {
				found[strings.TrimSpace(doc.ID)] = doc
			}
		}
	}
	out := make([]VectorDocument, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if doc, ok := found[id]; ok {
			out = append(out, doc)
		}
	}
	return out, nil
}
func (s *canonicalStore) AwaitVisible(ctx context.Context, ids []string, budget time.Duration) error {
	if waiter, ok := s.accelerator.(VectorVisibilityWaiter); ok {
		return waiter.AwaitVisible(ctx, ids, budget)
	}
	return nil
}
func (s *canonicalStore) VisibilityWaiterEnabled() bool {
	return HasVisibilityWaiter(s.accelerator)
}
