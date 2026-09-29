package vector

import (
	"context"
	"errors"
	"testing"
)

type durableOverlayTestAccelerator struct {
	VectorStore
	docs []VectorDocument
	err  error
}

func (s *durableOverlayTestAccelerator) Search(context.Context, string, []float32, int, string) ([]VectorDocument, error) {
	return append([]VectorDocument(nil), s.docs...), s.err
}

type durableOverlayTestProvider struct {
	snapshot DurableSearchOverlaySnapshot
	err      error
	session  string
	maximum  int
}

func (p *durableOverlayTestProvider) DurableSearchOverlay(_ context.Context, sessionID string, maximum int) (DurableSearchOverlaySnapshot, error) {
	p.session = sessionID
	p.maximum = maximum
	return p.snapshot, p.err
}

func TestDurableSearchOverlayAddsFilteredPendingUpsertAndRanksByCosine(t *testing.T) {
	accelerator := &durableOverlayTestAccelerator{
		VectorStore: NewFakeVectorStore(),
		docs: []VectorDocument{{
			ID: "accelerator", ChatSessionID: "session", Tier: "memory", SourceTable: "memory_units",
			Similarity: 0.5, SimilarityAvailable: true, Distance: 0.5,
		}},
	}
	overlay := &durableOverlayTestProvider{snapshot: DurableSearchOverlaySnapshot{Upserts: []VectorDocument{
		{ID: "pending", ChatSessionID: "session", Tier: "memory", SourceTable: "memory_units", Embedding: []float32{1, 0}},
		{ID: "other-tier", ChatSessionID: "session", Tier: "episode", SourceTable: "memory_units", Embedding: []float32{1, 0}},
	}}}
	store := NewDurableSearchOverlayVectorStore(accelerator, overlay)

	docs, err := store.Search(context.Background(), "session", []float32{1, 0}, 3, `tier == "memory"`)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if overlay.session != "session" || overlay.maximum != 3 {
		t.Fatalf("snapshot call = session=%q maximum=%d, want session/3", overlay.session, overlay.maximum)
	}
	if len(docs) != 2 || docs[0].ID != "pending" || docs[1].ID != "accelerator" {
		t.Fatalf("docs = %#v, want pending cosine result then accelerator result", docs)
	}
	if docs[0].SimilaritySource != "cosine_from_query_and_durable_overlay" {
		t.Fatalf("pending similarity source = %q", docs[0].SimilaritySource)
	}
}

func TestDurableSearchOverlayMasksTombstonesAndCanonicalUpsertReplacesAccelerator(t *testing.T) {
	accelerator := &durableOverlayTestAccelerator{
		VectorStore: NewFakeVectorStore(),
		docs: []VectorDocument{
			{ID: "deleted", ChatSessionID: "session", Similarity: 0.99, SimilarityAvailable: true, Distance: 0.01},
			{ID: "replaced", ChatSessionID: "session", Similarity: 0.95, SimilarityAvailable: true, Distance: 0.05, DocumentText: "stale"},
		},
	}
	overlay := &durableOverlayTestProvider{snapshot: DurableSearchOverlaySnapshot{
		TombstoneIDs: []string{"deleted"},
		Upserts:      []VectorDocument{{ID: "replaced", ChatSessionID: "session", Embedding: []float32{1, 0}, DocumentText: "canonical"}},
	}}

	docs, err := NewDurableSearchOverlayVectorStore(accelerator, overlay).Search(context.Background(), "session", []float32{1, 0}, 5, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(docs) != 1 || docs[0].ID != "replaced" || docs[0].DocumentText != "canonical" {
		t.Fatalf("docs = %#v, want canonical replacement only", docs)
	}
}

func TestDurableSearchOverlayTreatsAcceleratorNotFoundAsAnEmptyCandidateSet(t *testing.T) {
	accelerator := &durableOverlayTestAccelerator{VectorStore: NewFakeVectorStore(), err: ErrNotFound}
	overlay := &durableOverlayTestProvider{snapshot: DurableSearchOverlaySnapshot{Upserts: []VectorDocument{{
		ID: "pending", ChatSessionID: "session", Embedding: []float32{1, 0},
	}}}}

	docs, err := NewDurableSearchOverlayVectorStore(accelerator, overlay).Search(context.Background(), "session", []float32{1, 0}, 0, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(docs) != 1 || docs[0].ID != "pending" || overlay.maximum != durableSearchOverlayDefaultLimit {
		t.Fatalf("docs=%#v maximum=%d, want pending result and default maximum", docs, overlay.maximum)
	}
}

func TestDurableSearchOverlayFailsClosedWhenSnapshotIsTruncated(t *testing.T) {
	accelerator := &durableOverlayTestAccelerator{VectorStore: NewFakeVectorStore(), docs: []VectorDocument{{ID: "partial"}}}
	overlay := &durableOverlayTestProvider{snapshot: DurableSearchOverlaySnapshot{PendingCount: 6, Truncated: true}}

	_, err := NewDurableSearchOverlayVectorStore(accelerator, overlay).Search(context.Background(), "session", []float32{1}, 5, "")
	if !errors.Is(err, ErrDurableSearchOverloaded) {
		t.Fatalf("Search error = %v, want ErrDurableSearchOverloaded", err)
	}
}

func TestDurableSearchOverlayRejectsMalformedPendingEmbedding(t *testing.T) {
	accelerator := &durableOverlayTestAccelerator{VectorStore: NewFakeVectorStore(), err: ErrNotFound}
	overlay := &durableOverlayTestProvider{snapshot: DurableSearchOverlaySnapshot{Upserts: []VectorDocument{{
		ID: "bad", ChatSessionID: "session", Embedding: []float32{1, 0},
	}}}}

	_, err := NewDurableSearchOverlayVectorStore(accelerator, overlay).Search(context.Background(), "session", []float32{1}, 5, "")
	if err == nil {
		t.Fatal("Search error = nil, want malformed overlay error")
	}
}

func TestDurableSearchOverlayLeavesStoresWithoutProviderUnchanged(t *testing.T) {
	accelerator := NewFakeVectorStore()
	if got := NewDurableSearchOverlayVectorStore(accelerator, nil); got != accelerator {
		t.Fatalf("store = %T, want original accelerator", got)
	}
}
