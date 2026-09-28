package vector

import (
	"context"
	"strings"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/cloudflarebridge"
)

// The implementation plan requires a mock contract that verifies session
// isolation and duplicate document ids against this provider. Both are safety
// properties rather than recall quality: a leak means a user is shown another
// session's memories, and a duplicate means a write is applied twice or a
// readback disagrees with the write that produced it.

// TestVectorizeRecallNeverCrossesASessionBoundary is the isolation guarantee.
//
// The other filter test asserts the SHAPE of the filter that is sent. This one
// asserts what the filter DOES, which is the property that matters: a recall
// scoped to one session must not return another session's documents even though
// the index holds both and the query vector matches all of them.
//
// The seed deliberately makes the foreign document the CLOSEST match. A filter
// that were ignored, or applied with the wrong field name, would return the
// foreign document first, so a leak cannot hide behind a plausible ordering.
func TestVectorizeRecallNeverCrossesASessionBoundary(t *testing.T) {
	store, index, _ := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-mine")
	vectorizeSeedSession(t, index, "sess-theirs")

	// The closest possible match belongs to the OTHER session.
	index.put("memory:sess-theirs:closest", []float32{1, 0}, map[string]any{
		vectorizeDocumentIDKey:   "memory:sess-theirs:closest",
		vectorizeDocumentTextKey: "another session's private memory",
		"tier":                   "memory",
		"chat_session_id":        "sess-theirs",
		"source_table":           "memories",
	})

	docs, err := store.Search(context.Background(), "sess-mine", []float32{1, 0}, 10, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("the scoped session's own documents must be returned")
	}
	for _, doc := range docs {
		if strings.Contains(doc.ID, "sess-theirs") {
			t.Fatalf("recall for sess-mine returned %q from sess-theirs; a session boundary was crossed", doc.ID)
		}
		if doc.ChatSessionID != "" && doc.ChatSessionID != "sess-mine" {
			t.Errorf("document %q carries session %q in a recall scoped to sess-mine", doc.ID, doc.ChatSessionID)
		}
	}
	// The foreign document is genuinely in the index and genuinely closer, so a
	// passing assertion here cannot be explained by it being absent.
	if !index.has("memory:sess-theirs:closest") {
		t.Fatal("fixture is wrong: the foreign document must exist in the index")
	}

	// The other direction, and a reference document alongside both.
	index.put("reference_work:work-1", []float32{1, 0}, map[string]any{
		vectorizeDocumentIDKey:   "reference_work:work-1",
		vectorizeDocumentTextKey: "a reference work passage",
		"tier":                   "reference_work",
		"source_table":           "reference_library",
	})
	docs, err = store.Search(context.Background(), "sess-mine", []float32{1, 0}, 10, "")
	if err != nil {
		t.Fatalf("Search after adding a reference document: %v", err)
	}
	for _, doc := range docs {
		if strings.HasPrefix(doc.ID, "reference_") {
			t.Fatalf("a session recall returned the reference document %q; the one index holds two document families", doc.ID)
		}
	}
}

// TestVectorizeReferenceRecallDoesNotReturnSessionMemories is the mirror of the
// isolation guarantee.
//
// One Vectorize index holds both the session-memory family and the reference
// library, which ChromaDB kept in two separate collections. The tier prefix is
// what separates them, so a reference query that failed to carry its tier clause
// would sweep in every session's memories — and a reference audit reads that
// result as "the index is correct".
func TestVectorizeReferenceRecallDoesNotReturnSessionMemories(t *testing.T) {
	store, index, _ := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-mine")
	index.put("reference_work:work-1", []float32{1, 0}, map[string]any{
		vectorizeDocumentIDKey:   "reference_work:work-1",
		vectorizeDocumentTextKey: "a reference work passage",
		"tier":                   "reference_work",
		"source_table":           "reference_library",
		"work_id":                "work-1",
	})

	results, err := any(store).(ExactMetadataQuerier).QueryExact(context.Background(), ExactQuery{
		Embedding: []float32{1, 0},
		Limit:     10,
		Where:     map[string]any{"tier": "reference_work", "work_id": "work-1"},
	})
	if err != nil {
		t.Fatalf("QueryExact: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("the reference document must be returned")
	}
	for _, result := range results {
		if strings.HasPrefix(result.Document.ID, "memory:") {
			t.Fatalf("a reference exact query returned the session memory %q", result.Document.ID)
		}
	}
}

// TestVectorizeUpsertIsIdempotentByDocumentID pins the duplicate-id contract.
//
// Vectorize replaces a vector that already carries the id, and the outbox
// depends on that: a replayed upsert after a partial failure must converge
// rather than accumulate. A duplicate inside ONE batch is the harder case, since
// it must not depend on the order the provider happens to apply entries in.
func TestVectorizeUpsertIsIdempotentByDocumentID(t *testing.T) {
	store, index, stub := newVectorizeIndexedStore(t)
	ctx := context.Background()

	documents := []VectorDocument{
		{ID: "memory:sess-dup:1", Embedding: []float32{0.1, 0.2}, ChatSessionID: "sess-dup", Tier: "memory", DocumentText: "first"},
		{ID: "memory:sess-dup:2", Embedding: []float32{0.3, 0.4}, ChatSessionID: "sess-dup", Tier: "memory", DocumentText: "second"},
	}
	if err := store.Upsert(ctx, "sess-dup", documents); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if index.size() != 2 {
		t.Fatalf("index size = %d, want 2", index.size())
	}

	// A replay of the same batch must leave exactly the same two documents.
	upsertsBefore := len(stub.callsFor(cloudflarebridge.OpVectorUpsert))
	if err := store.Upsert(ctx, "sess-dup", documents); err != nil {
		t.Fatalf("replaying the upsert: %v", err)
	}
	if index.size() != 2 {
		t.Errorf("index size after a replay = %d, want 2; a replayed upsert must not duplicate a document", index.size())
	}
	if got := len(stub.callsFor(cloudflarebridge.OpVectorUpsert)) - upsertsBefore; got != 1 {
		t.Errorf("replay issued %d upsert calls, want 1", got)
	}

	// The same id twice in one batch is a single document, and the later entry
	// wins, exactly as a replace-by-id does.
	duplicated := []VectorDocument{
		{ID: "memory:sess-dup:3", Embedding: []float32{1, 0}, ChatSessionID: "sess-dup", Tier: "memory", DocumentText: "before"},
		{ID: "memory:sess-dup:3", Embedding: []float32{0.5, 0.5}, ChatSessionID: "sess-dup", Tier: "memory", DocumentText: "after"},
	}
	if err := store.Upsert(ctx, "sess-dup", duplicated); err != nil {
		t.Fatalf("Upsert with a duplicated id: %v", err)
	}
	if index.size() != 3 {
		t.Errorf("index size = %d, want 3: a duplicated id is one document", index.size())
	}
	found, err := any(store).(ExactDocumentReader).GetDocuments(ctx, []string{"memory:sess-dup:3"})
	if err != nil {
		t.Fatalf("GetDocuments: %v", err)
	}
	if len(found) != 1 {
		t.Fatalf("a duplicated id resolved to %d documents, want 1", len(found))
	}
	if found[0].DocumentText != "after" {
		t.Errorf("stored text = %q, want the later entry of the batch to win", found[0].DocumentText)
	}
}

// TestVectorizeDeleteIsIdempotentForAnAbsentDocument pins the other half of the
// outbox's replay contract. A delete readback verifies the document is GONE, and
// a delete that reported "not found" as an error would poison the retry forever.
func TestVectorizeDeleteIsIdempotentForAnAbsentDocument(t *testing.T) {
	store, index, _ := newVectorizeIndexedStore(t)
	ctx := context.Background()

	if err := store.Upsert(ctx, "sess-del", []VectorDocument{{
		ID: "memory:sess-del:1", Embedding: []float32{1, 0}, ChatSessionID: "sess-del", Tier: "memory",
	}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := any(store).(DocumentDeleter).DeleteDocuments(ctx, []string{"memory:sess-del:1"}); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	if index.has("memory:sess-del:1") {
		t.Fatal("the document was not deleted")
	}
	// Replaying the delete must succeed: the outbox replays it after a partial
	// failure, and a second failure would burn another attempt for nothing.
	if err := any(store).(DocumentDeleter).DeleteDocuments(ctx, []string{"memory:sess-del:1"}); err != nil {
		t.Fatalf("replaying the delete of an absent document: %v", err)
	}
	// And a delete of an empty set is not a call at all.
	before := index.size()
	if err := any(store).(DocumentDeleter).DeleteDocuments(ctx, nil); err != nil {
		t.Fatalf("deleting nothing: %v", err)
	}
	if index.size() != before {
		t.Errorf("an empty delete changed the index size from %d", before)
	}
}
