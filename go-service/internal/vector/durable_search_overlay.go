package vector

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

const durableSearchOverlayDefaultLimit = 5

// durableSearchOverlayStore merges D1's small, durable visibility delta into
// an accelerator search. It never writes the accelerator or outbox: Search is
// a read-only correction path, so retrying a query cannot replay a mutation.
type durableSearchOverlayStore struct {
	accelerator VectorStore
	overlay     DurableSearchOverlayProvider
}

// NewDurableSearchOverlayVectorStore leaves stores without a D1 overlay
// provider untouched. This keeps the local Chroma profile on its existing
// search path while allowing the Cloudflare composition root to opt in.
func NewDurableSearchOverlayVectorStore(accelerator VectorStore, overlay DurableSearchOverlayProvider) VectorStore {
	if accelerator == nil || overlay == nil {
		return accelerator
	}
	return &durableSearchOverlayStore{accelerator: accelerator, overlay: overlay}
}

func (s *durableSearchOverlayStore) DurableSearchOverlay(ctx context.Context, sessionID string, maximum int) (DurableSearchOverlaySnapshot, error) {
	return s.overlay.DurableSearchOverlay(ctx, sessionID, maximum)
}

func (s *durableSearchOverlayStore) Search(ctx context.Context, sessionID string, embedding []float32, limit int, filter string) ([]VectorDocument, error) {
	acceleratorDocs, err := s.accelerator.Search(ctx, sessionID, embedding, limit, filter)
	if err != nil && err != ErrNotFound {
		return nil, err
	}
	if err == ErrNotFound {
		acceleratorDocs = nil
	}

	effectiveLimit := limit
	if effectiveLimit <= 0 {
		effectiveLimit = durableSearchOverlayDefaultLimit
	}
	snapshot, err := s.overlay.DurableSearchOverlay(ctx, sessionID, DurableSearchOverlayMaxDocuments)
	if err != nil {
		return nil, err
	}
	if snapshot.Truncated {
		return nil, fmt.Errorf("%w: pending=%d, maximum=%d", ErrDurableSearchOverloaded, snapshot.PendingCount, effectiveLimit)
	}

	tombstones := make(map[string]struct{}, len(snapshot.TombstoneIDs))
	for _, id := range snapshot.TombstoneIDs {
		if id = strings.TrimSpace(id); id != "" {
			tombstones[id] = struct{}{}
		}
	}

	// Preserve the accelerator's existing rank order for ties. An overlay upsert
	// with the same ID replaces that candidate in place; overlay-only IDs are
	// appended in sorted ID order before the stable similarity comparator runs.
	byID := make(map[string]VectorDocument, len(acceleratorDocs)+len(snapshot.Upserts))
	orderedIDs := make([]string, 0, len(acceleratorDocs)+len(snapshot.Upserts))
	for _, doc := range acceleratorDocs {
		id := strings.TrimSpace(doc.ID)
		if id == "" {
			continue
		}
		if _, deleted := tombstones[id]; deleted {
			continue
		}
		if _, seen := byID[id]; !seen {
			orderedIDs = append(orderedIDs, id)
		}
		byID[id] = doc
	}

	overlayOnlyIDs := make([]string, 0, len(snapshot.Upserts))
	for _, doc := range snapshot.Upserts {
		id := strings.TrimSpace(doc.ID)
		if id == "" {
			return nil, fmt.Errorf("durable search overlay contains an upsert without an id")
		}
		if _, deleted := tombstones[id]; deleted {
			continue
		}
		if !searchFilterMatches(doc, sessionID, filter) {
			continue
		}
		similarity, valid := cosineSimilarity(embedding, doc.Embedding)
		if !valid {
			return nil, fmt.Errorf("durable search overlay document %q has an incompatible embedding", id)
		}
		doc.ID = id
		doc.Similarity = similarity
		doc.SimilarityAvailable = true
		doc.SimilaritySource = "cosine_from_query_and_durable_overlay"
		doc.Distance = 1 - similarity
		if _, exists := byID[id]; !exists {
			overlayOnlyIDs = append(overlayOnlyIDs, id)
		}
		byID[id] = doc
	}
	sort.Strings(overlayOnlyIDs)
	orderedIDs = append(orderedIDs, overlayOnlyIDs...)

	docs := make([]VectorDocument, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		if _, deleted := tombstones[id]; deleted {
			continue
		}
		docs = append(docs, byID[id])
	}
	if len(docs) == 0 {
		return nil, ErrNotFound
	}
	sort.SliceStable(docs, func(i, j int) bool {
		left := docs[i]
		right := docs[j]
		if left.SimilarityAvailable != right.SimilarityAvailable {
			return left.SimilarityAvailable
		}
		if left.Similarity != right.Similarity {
			return left.Similarity > right.Similarity
		}
		return left.Distance < right.Distance
	})
	if len(docs) > effectiveLimit {
		docs = docs[:effectiveLimit]
	}
	return docs, nil
}

func (s *durableSearchOverlayStore) Upsert(ctx context.Context, sessionID string, docs []VectorDocument) error {
	return s.accelerator.Upsert(ctx, sessionID, docs)
}
func (s *durableSearchOverlayStore) DeleteSession(ctx context.Context, sessionID string) error {
	return s.accelerator.DeleteSession(ctx, sessionID)
}
func (s *durableSearchOverlayStore) Rebuild(ctx context.Context, sessionID string) error {
	return s.accelerator.Rebuild(ctx, sessionID)
}
func (s *durableSearchOverlayStore) Health(ctx context.Context) (HealthSnapshot, error) {
	return s.accelerator.Health(ctx)
}
func (s *durableSearchOverlayStore) Count(ctx context.Context, sessionID string) (int, error) {
	return s.accelerator.Count(ctx, sessionID)
}
func (s *durableSearchOverlayStore) Close(ctx context.Context) error { return s.accelerator.Close(ctx) }

func (s *durableSearchOverlayStore) DeleteDocuments(ctx context.Context, ids []string) error {
	if deleter, ok := s.accelerator.(DocumentDeleter); ok {
		return deleter.DeleteDocuments(ctx, ids)
	}
	return ErrNotEnabled
}
func (s *durableSearchOverlayStore) ListDocuments(ctx context.Context, sessionID string) ([]VectorDocument, error) {
	if lister, ok := s.accelerator.(DocumentLister); ok {
		return lister.ListDocuments(ctx, sessionID)
	}
	return nil, ErrNotEnabled
}
func (s *durableSearchOverlayStore) GetDocuments(ctx context.Context, ids []string) ([]VectorDocument, error) {
	if reader, ok := s.accelerator.(ExactDocumentReader); ok {
		return reader.GetDocuments(ctx, ids)
	}
	return nil, ErrNotEnabled
}
func (s *durableSearchOverlayStore) GetAcceleratorDocuments(ctx context.Context, ids []string) ([]VectorDocument, error) {
	if reader, ok := s.accelerator.(AcceleratorExactDocumentReader); ok {
		return reader.GetAcceleratorDocuments(ctx, ids)
	}
	if reader, ok := s.accelerator.(ExactDocumentReader); ok {
		return reader.GetDocuments(ctx, ids)
	}
	return nil, ErrNotEnabled
}
func (s *durableSearchOverlayStore) AwaitVisible(ctx context.Context, ids []string, budget time.Duration) error {
	if waiter, ok := s.accelerator.(VectorVisibilityWaiter); ok {
		return waiter.AwaitVisible(ctx, ids, budget)
	}
	return nil
}
func (s *durableSearchOverlayStore) VisibilityWaiterEnabled() bool {
	if capability, ok := s.accelerator.(VisibilityWaiterCapability); ok {
		return capability.VisibilityWaiterEnabled()
	}
	_, ok := s.accelerator.(VectorVisibilityWaiter)
	return ok
}
func (s *durableSearchOverlayStore) RecoverIndex(ctx context.Context, path string, rebuild func(VectorStore) error) (string, error) {
	if recovery, ok := s.accelerator.(IndexRecovery); ok {
		return recovery.RecoverIndex(ctx, path, rebuild)
	}
	return "", ErrNotEnabled
}
func (s *durableSearchOverlayStore) ResumeIndexRecovery(ctx context.Context, path string) error {
	if recovery, ok := s.accelerator.(IndexRecovery); ok {
		return recovery.ResumeIndexRecovery(ctx, path)
	}
	return nil
}
func (s *durableSearchOverlayStore) RecoverySnapshot(ctx context.Context, path string) ([]VectorDocument, int, error) {
	if recovery, ok := s.accelerator.(IndexRecovery); ok {
		return recovery.RecoverySnapshot(ctx, path)
	}
	return nil, 0, ErrNotEnabled
}
