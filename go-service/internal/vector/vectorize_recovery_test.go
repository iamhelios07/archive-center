package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/cloudflarebridge"
)

// These tests cover the paths where a Vectorize provider either matches the
// ChromaDB behaviour it replaces or visibly does not: startup index recovery,
// turn rollback, the memory-vector outbox, and the float64 handling of the
// similarity itself.

func TestVectorizeStartupRecoveryReadsBackEveryRecoveredBatch(t *testing.T) {
	// startup_vector_recovery.go restores a snapshot in batches, reads every id
	// of the batch back, and fails the recovery when the readback is short. The
	// read must therefore return the batch in full and with its stored vectors.
	store, index, _ := newVectorizeIndexedStore(t)
	const total = 250
	docs := make([]VectorDocument, 0, total)
	for i := 0; i < total; i++ {
		docs = append(docs, VectorDocument{
			ID:            fmt.Sprintf("memory:sess-1:%d", i),
			Embedding:     []float32{float32(i%7) + 0.25, float32(i%3) + 0.5, 0.75},
			Tier:          "memory",
			ChatSessionID: "sess-1",
			SourceTable:   "memories",
			SourceRowID:   fmt.Sprint(i),
			DocumentText:  fmt.Sprintf("recovered memory %d", i),
			Metadata:      map[string]any{"content_fingerprint": fmt.Sprintf("fp-%d", i)},
		})
	}
	reader := any(store).(ExactDocumentReader)
	ctx := context.Background()
	for start := 0; start < total; start += 100 {
		batch := docs[start:min(start+100, total)]
		if err := store.Upsert(ctx, "sess-1", batch); err != nil {
			t.Fatalf("Upsert batch at %d: %v", start, err)
		}
		ids := make([]string, len(batch))
		for i, doc := range batch {
			ids[i] = doc.ID
		}
		read, err := reader.GetDocuments(ctx, ids)
		if err != nil {
			t.Fatalf("GetDocuments at %d: %v", start, err)
		}
		if len(read) != len(ids) {
			t.Fatalf("recovered batch readback is incomplete: %d of %d", len(read), len(ids))
		}
		for i, doc := range read {
			if doc.ID != ids[i] {
				t.Fatalf("readback %d = %q, want %q", i, doc.ID, ids[i])
			}
			if len(doc.Embedding) != 3 {
				t.Fatalf("%s: stored vector is missing, so the recovery probe cannot run", doc.ID)
			}
			if doc.DocumentText != batch[i].DocumentText {
				t.Fatalf("%s: DocumentText = %q, want the snapshot text", doc.ID, doc.DocumentText)
			}
		}
	}
	// The recovery ends with a probe query built from a recovered embedding.
	if _, err := store.Search(ctx, docs[0].ChatSessionID, docs[0].Embedding, 1, ""); err != nil {
		t.Fatalf("recovery probe query: %v", err)
	}
	// A snapshot that only partly landed must read back short, never as a
	// complete batch.
	listed, err := any(store).(DocumentLister).ListDocuments(ctx, "sess-1")
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	if len(listed) != total {
		t.Fatalf("enumeration = %d documents, want %d", len(listed), total)
	}
	index.put("memory:sess-1:short", []float32{0, 0, 1}, map[string]any{
		vectorizeDocumentIDKey: "memory:sess-1:short",
		"chat_session_id":      "sess-1",
	})
	read, err := reader.GetDocuments(ctx, []string{"memory:sess-1:0", "memory:sess-1:short", "memory:sess-1:9999"})
	if err != nil {
		t.Fatalf("GetDocuments: %v", err)
	}
	if len(read) != 2 {
		t.Fatalf("GetDocuments = %d documents, want the 2 that exist", len(read))
	}
}

func TestVectorizeRecoveryProbeSearchDoesNotRequestStoredValues(t *testing.T) {
	// A recall query, including the recovery probe, must not pay for the stored
	// vector. Vectorize only returns values when the request asks for them, so a
	// recall that asked for them would be paying for a payload it never reads.
	store, index, stub := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	if _, err := store.Search(context.Background(), "sess-1", []float32{1, 0}, 3, ""); err != nil {
		t.Fatalf("Search: %v", err)
	}
	queries := stub.callsFor(cloudflarebridge.OpVectorQuery)
	if len(queries) != 1 {
		t.Fatalf("vector.query count = %d, want 1", len(queries))
	}
	if vectorizeStubBool(queries[0].Payload["includeValues"]) {
		t.Error("a recall query must not set includeValues")
	}
}

func TestVectorizeStoredVectorRoundTripsExactlyAndAgreesWithTheProviderScore(t *testing.T) {
	// Vectorize scores arrive as JSON float64 while the stored vectors are
	// float32. The provider trusts the score, so the only float64 exposure is
	// the round trip of a stored value, and it must not drift: widening a
	// float32 to float64 and parsing it back is exact.
	store, index, _ := newVectorizeIndexedStore(t)
	primeVectorizeProbe(t, store, []float32{1, 0})
	written := []float32{0.1, 0.2, 0.3, 0.7, 1.0 / 3.0}
	query := []float32{0.5, 0.25, 0.875, 0.125, 0.0625}
	id := "memory:sess-1:1"
	index.put(id, written, map[string]any{
		vectorizeDocumentIDKey:   id,
		vectorizeDocumentTextKey: "float round trip",
		"tier":                   "memory",
		"chat_session_id":        "sess-1",
	})
	read, err := any(store).(ExactDocumentReader).GetDocuments(context.Background(), []string{id})
	if err != nil {
		t.Fatalf("GetDocuments: %v", err)
	}
	if len(read) != 1 {
		t.Fatalf("GetDocuments returned %d documents, want 1", len(read))
	}
	for i, value := range written {
		if read[0].Embedding[i] != value {
			t.Errorf("stored value %d = %v, want %v: the float64 round trip drifted", i, read[0].Embedding[i], value)
		}
	}
	recomputed, ok := cosineSimilarity(query, read[0].Embedding)
	if !ok {
		t.Fatal("cosine of the query and the stored vector is not computable")
	}
	matches, err := store.searchMatchesForTest(t, query)
	if err != nil {
		t.Fatalf("provider query: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("the provider answered no match")
	}
	// The provider computes cosine in float32 and the readback path computes it
	// in float64, so the two agree to float32 precision and no further. That
	// difference is the reason the provider trusts the score instead of
	// recomputing it: a recall does not have the stored vector at all.
	if math.Abs(matches[0]-recomputed) > 1e-6 {
		t.Errorf("provider score %v and recomputed cosine %v differ by more than float32 precision", matches[0], recomputed)
	}
}

func TestVectorizeRecallEligibilityMatchesTheChromaCosineBand(t *testing.T) {
	// The band that matters is 0.30 to 0.55. A cosine score inside it is
	// eligible for a Chroma recall and must stay eligible here; a source ending
	// in distance_inverse would raise the bar to 0.55 and drop it silently.
	store, index, _ := newVectorizeIndexedStore(t)
	query := []float32{1, 0}
	// A stored vector at cosine 0.4472 with the query: inside the band.
	index.put("memory:sess-1:band", []float32{0.4, 0.8}, map[string]any{
		vectorizeDocumentIDKey:   "memory:sess-1:band",
		vectorizeDocumentTextKey: "band",
		"tier":                   "memory",
		"chat_session_id":        "sess-1",
	})
	docs, err := store.Search(context.Background(), "sess-1", query, 3, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(docs) != 1 {
		t.Fatalf("Search returned %d documents, want 1", len(docs))
	}
	similarity := docs[0].Similarity
	if similarity < 0.30 || similarity > 0.55 {
		t.Fatalf("similarity %f is outside the band the test is about", similarity)
	}
	if !vectorizeRecallEligible(similarity, docs[0].SimilaritySource) {
		t.Errorf("similarity %f with source %q is not eligible, but a Chroma cosine recall accepts it",
			similarity, docs[0].SimilaritySource)
	}
	// The Chroma source for the same number is eligible too, so the two
	// providers keep the same threshold for the same score.
	if !vectorizeRecallEligible(similarity, "cosine_from_query_and_stored_embedding") {
		t.Error("the Chroma cosine source must accept the same score")
	}
	// And the trap this stage exists to avoid.
	if vectorizeRecallEligible(similarity, "vectorize_distance_inverse") {
		t.Error("an inverse-distance source would silently discard this hit")
	}
	if similarity < 0.30 && vectorizeRecallEligible(similarity, docs[0].SimilaritySource) {
		t.Error("a score below the cosine threshold must not be eligible")
	}
}

func TestVectorizeTurnRollbackDeletesExactlyTheIdsItKnows(t *testing.T) {
	// A turn rollback knows the canonical row ids it must remove. Without that
	// it would have to sweep the whole session or give up and leave vectors that
	// still match.
	store, index, stub := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	vectorizeSeedSession(t, index, "sess-2")
	primeVectorizeProbe(t, store, []float32{1, 0})
	ids := []string{"memory:sess-1:2", "memory:sess-1:4"}
	if err := any(store).(DocumentDeleter).DeleteDocuments(context.Background(), ids); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	readback, err := any(store).(ExactDocumentReader).GetDocuments(context.Background(), ids)
	if err != nil {
		t.Fatalf("rollback readback: %v", err)
	}
	if len(readback) != 0 {
		t.Fatalf("readback = %v, want the rolled back documents to be gone", vectorizeDocumentIDs(readback))
	}
	for _, survivor := range []string{"memory:sess-1:1", "memory:sess-1:3", "memory:sess-2:1"} {
		if !index.has(survivor) {
			t.Errorf("%s was deleted by a rollback that did not own it", survivor)
		}
	}
	// A rollback of the same turn again is a no-op, not a failure.
	if err := any(store).(DocumentDeleter).DeleteDocuments(context.Background(), ids); err != nil {
		t.Fatalf("replayed rollback: %v", err)
	}
	if deletes := stub.callsFor(cloudflarebridge.OpVectorDelete); len(deletes) != 2 {
		t.Errorf("vector.delete count = %d, want one batch per attempt", len(deletes))
	}
}

func TestVectorizeOutboxReplaysADeleteAfterAPartialFailure(t *testing.T) {
	attempts := 0
	index := newVectorizeFakeIndex(true)
	inner := index.responder()
	stub := newVectorizeWorkerStub(t, func(call vectorizeStubCall) (string, *vectorizeStubFailure) {
		if call.Operation == cloudflarebridge.OpVectorDelete {
			attempts++
			// The first attempt fails the way a real Vectorize execution
			// failure does, after the Worker has already applied part of the
			// batch.
			if attempts == 1 {
				ids, _ := call.Payload["ids"].([]any)
				if len(ids) > 0 {
					_, _ = inner(vectorizeStubCall{Operation: call.Operation, Payload: map[string]any{"ids": ids[:1]}})
				}
				return "", &vectorizeStubFailure{
					Code:      "vector_execution_failed",
					Message:   "Vectorize delete failed after applying part of the batch",
					Retryable: true,
				}
			}
		}
		return inner(call)
	})
	store := stub.store(t)
	vectorizeSeedSession(t, index, "sess-1")
	primeVectorizeProbe(t, store, []float32{1, 0})
	ids := []string{"memory:sess-1:1", "memory:sess-1:2", "memory:sess-1:3"}
	err := any(store).(DocumentDeleter).DeleteDocuments(context.Background(), ids)
	if !cloudflarebridge.Retryable(err) {
		t.Fatalf("err = %v, want a retryable execution failure so the outbox replays it", err)
	}
	if !index.has("memory:sess-1:3") {
		t.Error("the surviving document should still exist after the partial failure")
	}
	if err := any(store).(DocumentDeleter).DeleteDocuments(context.Background(), ids); err != nil {
		t.Fatalf("replaying the delete: %v", err)
	}
	for _, id := range ids {
		if index.has(id) {
			t.Errorf("%s survived the replayed delete", id)
		}
	}
	// The replay deleted an id the partial failure had already removed, and that
	// must not be reported as a failure: a not-found delete would poison the
	// retry forever.
	readback, err := any(store).(ExactDocumentReader).GetDocuments(context.Background(), ids)
	if err != nil {
		t.Fatalf("delete readback: %v", err)
	}
	if len(readback) != 0 {
		t.Fatalf("readback = %v, want no document left", vectorizeDocumentIDs(readback))
	}
}

func TestVectorizeParkedRequestFailureIsNotReplayedForever(t *testing.T) {
	// A malformed payload is not retryable. Reporting it as retryable would spin
	// the outbox on a payload that can never succeed.
	attempts := 0
	stub := newVectorizeWorkerStub(t, func(call vectorizeStubCall) (string, *vectorizeStubFailure) {
		attempts++
		return "", &vectorizeStubFailure{
			Code:      "vector_request_invalid",
			Message:   "values must be an array of numbers",
			Retryable: false,
		}
	})
	store := stub.store(t)
	for i := 0; i < 3; i++ {
		err := store.Upsert(context.Background(), "sess-1", []VectorDocument{{
			ID: "memory:sess-1:1", Embedding: []float32{1, 0}, ChatSessionID: "sess-1",
		}})
		if err == nil {
			t.Fatal("a rejected payload must be reported")
		}
		if cloudflarebridge.Retryable(err) {
			t.Fatalf("attempt %d: err = %v, want a parked, non-retryable failure", i+1, err)
		}
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3: the provider must not retry on its own", attempts)
	}
}

func TestVectorizeEnumerationNeverRanksWithTheProbeVector(t *testing.T) {
	// An enumeration query ranks by the dimension probe vector, which carries no
	// meaning. Reporting that ranking as a similarity would let an integrity
	// audit read a probe artefact as a measurement.
	store, index, _ := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	primeVectorizeProbe(t, store, []float32{1, 0})
	docs, err := any(store).(DocumentLister).ListDocuments(context.Background(), "sess-1")
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("ListDocuments returned nothing")
	}
	for _, doc := range docs {
		if doc.SimilarityAvailable || doc.Similarity != 0 || doc.Distance != 0 || doc.SimilaritySource != "" {
			t.Errorf("%s: enumeration reported a measurement it cannot make: %+v", doc.ID, doc)
		}
		if len(doc.Embedding) == 0 {
			t.Errorf("%s: enumeration must still return the stored vector", doc.ID)
		}
	}
}

func TestVectorizeAccountNeutralityHoldsInEveryRequest(t *testing.T) {
	// No account, database, index, namespace, route or domain may appear in what
	// this provider sends, and it is reached only through the bridge operation
	// names.
	store, index, stub := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	primeVectorizeProbe(t, store, []float32{1, 0})
	ctx := context.Background()
	if err := store.Upsert(ctx, "sess-1", []VectorDocument{{
		ID: "memory:sess-1:9", Embedding: []float32{0.5, 0.5}, ChatSessionID: "sess-1",
		DocumentText: "neutral", Metadata: map[string]any{"work_id": "work-1"},
	}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if _, err := store.Search(ctx, "sess-1", []float32{1, 0}, 2, ""); err != nil {
		t.Fatalf("Search: %v", err)
	}
	if _, err := any(store).(DocumentLister).ListDocuments(ctx, "sess-1"); err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	if err := store.DeleteSession(ctx, "sess-1"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if _, err := store.Health(ctx); err != nil {
		t.Fatalf("Health: %v", err)
	}
	allowed := map[string]bool{
		cloudflarebridge.OpVectorUpsert: true,
		cloudflarebridge.OpVectorDelete: true,
		cloudflarebridge.OpVectorQuery:  true,
		cloudflarebridge.OpVectorHealth: true,
	}
	for _, call := range stub.allCalls() {
		if !allowed[call.Operation] {
			t.Errorf("operation %q is not one of the four vector operations", call.Operation)
		}
		encoded, err := json.Marshal(call.Payload)
		if err != nil {
			t.Fatalf("encode %s payload: %v", call.Operation, err)
		}
		lowered := strings.ToLower(string(encoded))
		for _, forbidden := range []string{
			"api.cloudflare.com", "https://", "http://", "workers.dev",
			"account_id", "database_id", "index_name", "namespace", "bearer ", "api_token",
		} {
			if strings.Contains(lowered, forbidden) {
				t.Errorf("%s payload of %s contains %q", call.Operation, call.Operation, forbidden)
			}
		}
	}
}

// searchMatchesForTest runs a bare provider query so a test can read the score
// the index itself returned, without the document mapping applied on top.
func (s *vectorizeStore) searchMatchesForTest(t *testing.T, query []float32) ([]float64, error) {
	t.Helper()
	matches, err := s.queryMatches(context.Background(), vectorizeQueryPayload{
		Vector: query, TopK: 1, IncludeValues: true,
	})
	if err != nil {
		return nil, err
	}
	out := make([]float64, 0, len(matches))
	for _, match := range matches {
		out = append(out, match.Score)
	}
	return out, nil
}
