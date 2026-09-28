package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/cloudflarebridge"
)

const vectorizeStubToken = "vectorize-bridge-token"

type vectorizeStubCall struct {
	Operation string
	Payload   map[string]any
}

type vectorizeStubFailure struct {
	Code      string
	Message   string
	Retryable bool
}

// vectorizeWorkerStub is a Worker that speaks the frozen bridge envelope. It
// validates the envelope, echoes the request id, and hands the operation to a
// responder that stands in for the VECTORIZE binding.
type vectorizeWorkerStub struct {
	mu       sync.Mutex
	calls    []vectorizeStubCall
	respond  func(vectorizeStubCall) (string, *vectorizeStubFailure)
	endpoint string
}

func newVectorizeWorkerStub(t *testing.T, respond func(vectorizeStubCall) (string, *vectorizeStubFailure)) *vectorizeWorkerStub {
	t.Helper()
	stub := &vectorizeWorkerStub{respond: respond}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("bridge method = %q, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+vectorizeStubToken {
			t.Errorf("bridge authorization = %q, want the injected bridge token", got)
		}
		var request cloudflarebridge.Request
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("bridge request is not a JSON envelope: %v", err)
			return
		}
		if request.Version != cloudflarebridge.EnvelopeVersion {
			t.Errorf("bridge request version = %d, want %d", request.Version, cloudflarebridge.EnvelopeVersion)
		}
		if strings.TrimSpace(request.ID) == "" {
			t.Error("bridge request id must not be empty")
		}
		payload := map[string]any{}
		if len(request.Payload) > 0 {
			if err := json.Unmarshal(request.Payload, &payload); err != nil {
				t.Errorf("bridge payload is not a JSON object: %v", err)
			}
		}
		result, failure := stub.dispatch(vectorizeStubCall{Operation: request.Operation, Payload: payload})
		response := cloudflarebridge.Response{Version: cloudflarebridge.EnvelopeVersion, ID: request.ID}
		if failure != nil {
			response.OK = false
			response.ErrorCode = failure.Code
			response.ErrorMessage = failure.Message
			response.Retryable = failure.Retryable
		} else {
			response.OK = true
			response.Result = json.RawMessage(result)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("encode bridge response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	stub.endpoint = server.URL
	return stub
}

func (s *vectorizeWorkerStub) dispatch(call vectorizeStubCall) (string, *vectorizeStubFailure) {
	s.mu.Lock()
	s.calls = append(s.calls, call)
	respond := s.respond
	s.mu.Unlock()
	if respond == nil {
		return "", &vectorizeStubFailure{Code: "not_implemented", Message: call.Operation}
	}
	return respond(call)
}

func (s *vectorizeWorkerStub) callsFor(operation string) []vectorizeStubCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []vectorizeStubCall{}
	for _, call := range s.calls {
		if call.Operation == operation {
			out = append(out, call)
		}
	}
	return out
}

func (s *vectorizeWorkerStub) allCalls() []vectorizeStubCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]vectorizeStubCall(nil), s.calls...)
}

// store builds the provider over this stub, through the real bridge client.
func (s *vectorizeWorkerStub) store(t *testing.T) *vectorizeStore {
	t.Helper()
	client, err := cloudflarebridge.NewClient(s.endpoint, vectorizeStubToken, 10*time.Second)
	if err != nil {
		t.Fatalf("cloudflarebridge.NewClient: %v", err)
	}
	raw, err := NewVectorizeStore(client)
	if err != nil {
		t.Fatalf("NewVectorizeStore: %v", err)
	}
	store, ok := raw.(*vectorizeStore)
	if !ok {
		t.Fatalf("NewVectorizeStore returned %T, want *vectorizeStore", raw)
	}
	return store
}

type vectorizeFakeVector struct {
	values []float32
	meta   map[string]any
}

// vectorizeFakeIndex is an in-memory stand-in for the Vectorize binding. It
// answers only what the frozen contract exposes: upsert by id, delete by id, a
// filtered topK query with cosine scores, and binding presence.
type vectorizeFakeIndex struct {
	mu      sync.Mutex
	order   []string
	vectors map[string]vectorizeFakeVector
	bound   bool
}

func newVectorizeFakeIndex(bound bool) *vectorizeFakeIndex {
	return &vectorizeFakeIndex{vectors: map[string]vectorizeFakeVector{}, bound: bound}
}

func (i *vectorizeFakeIndex) put(id string, values []float32, meta map[string]any) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if _, exists := i.vectors[id]; !exists {
		i.order = append(i.order, id)
	}
	i.vectors[id] = vectorizeFakeVector{values: append([]float32(nil), values...), meta: meta}
}

func (i *vectorizeFakeIndex) size() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.vectors)
}

func (i *vectorizeFakeIndex) has(id string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	_, exists := i.vectors[id]
	return exists
}

func (i *vectorizeFakeIndex) responder() func(vectorizeStubCall) (string, *vectorizeStubFailure) {
	return func(call vectorizeStubCall) (string, *vectorizeStubFailure) {
		var result any
		switch call.Operation {
		case cloudflarebridge.OpVectorUpsert:
			result = i.upsert(call.Payload)
		case cloudflarebridge.OpVectorDelete:
			result = i.remove(call.Payload)
		case cloudflarebridge.OpVectorQuery:
			result = i.query(call.Payload)
		case cloudflarebridge.OpVectorHealth:
			result = i.health()
		default:
			return "", &vectorizeStubFailure{Code: "unknown_operation", Message: call.Operation}
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return "", &vectorizeStubFailure{Code: "vector_execution_failed", Message: err.Error(), Retryable: true}
		}
		return string(encoded), nil
	}
}

func (i *vectorizeFakeIndex) upsert(payload map[string]any) any {
	entries, _ := payload["vectors"].([]any)
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, entry := range entries {
		item, _ := entry.(map[string]any)
		id := vectorizeStubString(item["id"])
		if id == "" {
			continue
		}
		if _, exists := i.vectors[id]; !exists {
			i.order = append(i.order, id)
		}
		meta, _ := item["metadata"].(map[string]any)
		i.vectors[id] = vectorizeFakeVector{values: vectorizeStubFloat32List(item["values"]), meta: meta}
	}
	return map[string]any{"upserted": len(entries)}
}

// remove deletes by id. An absent id is not an error and is not counted, which
// is what keeps a replayed outbox delete from failing forever.
func (i *vectorizeFakeIndex) remove(payload map[string]any) any {
	ids, _ := payload["ids"].([]any)
	i.mu.Lock()
	defer i.mu.Unlock()
	deleted := 0
	for _, raw := range ids {
		id := vectorizeStubString(raw)
		if _, exists := i.vectors[id]; !exists {
			continue
		}
		delete(i.vectors, id)
		for position, stored := range i.order {
			if stored == id {
				i.order = append(i.order[:position], i.order[position+1:]...)
				break
			}
		}
		deleted++
	}
	return map[string]any{"deleted": deleted}
}

func (i *vectorizeFakeIndex) query(payload map[string]any) any {
	query := vectorizeStubFloat32List(payload["vector"])
	topK := int(vectorizeStubFloat(payload["topK"]))
	filter, _ := payload["filter"].(map[string]any)
	includeValues, _ := payload["includeValues"].(bool)
	i.mu.Lock()
	defer i.mu.Unlock()
	matches := []map[string]any{}
	for _, id := range i.order {
		stored, exists := i.vectors[id]
		if !exists || !vectorizeFakeMetadataMatchesAll(stored.meta, filter) {
			continue
		}
		score, ok := cosineSimilarity(query, stored.values)
		if !ok {
			score = 0
		}
		match := map[string]any{"id": id, "score": score}
		if includeValues {
			values := make([]float64, len(stored.values))
			for position, value := range stored.values {
				values[position] = float64(value)
			}
			match["values"] = values
		}
		if len(stored.meta) > 0 {
			match["metadata"] = stored.meta
		}
		matches = append(matches, match)
	}
	sort.SliceStable(matches, func(a, b int) bool {
		return matches[a]["score"].(float64) > matches[b]["score"].(float64)
	})
	if topK > 0 && len(matches) > topK {
		matches = matches[:topK]
	}
	return map[string]any{"matches": matches}
}

func (i *vectorizeFakeIndex) health() any {
	i.mu.Lock()
	defer i.mu.Unlock()
	return map[string]any{"bound": i.bound, "version": "stub-index-1"}
}

func vectorizeFakeMetadataMatchesAll(stored map[string]any, filter map[string]any) bool {
	for field, want := range filter {
		if !vectorizeFakeMetadataEqual(want, stored[field]) {
			return false
		}
	}
	return true
}

func vectorizeFakeMetadataEqual(want any, got any) bool {
	switch typed := want.(type) {
	case string:
		text, ok := got.(string)
		return ok && text == typed
	case bool:
		flag, ok := got.(bool)
		return ok && flag == typed
	case float64:
		number, ok := got.(float64)
		return ok && number == typed
	default:
		return false
	}
}

func vectorizeStubString(value any) string {
	text, _ := value.(string)
	return text
}

func vectorizeStubFloat(value any) float64 {
	number, _ := value.(float64)
	return number
}

func vectorizeStubBool(value any) bool {
	flag, _ := value.(bool)
	return flag
}

func vectorizeStubFloat32List(value any) []float32 {
	raw, _ := value.([]any)
	out := make([]float32, 0, len(raw))
	for _, item := range raw {
		number, _ := item.(float64)
		out = append(out, float32(number))
	}
	return out
}

func vectorizeStubFilter(payload map[string]any) (map[string]any, bool) {
	filter, present := payload["filter"]
	if !present {
		return nil, false
	}
	out, ok := filter.(map[string]any)
	return out, ok
}

// vectorizeRecallEligible mirrors prepareTurnVectorSimilarityEligible, which
// lives in the httpapi package and cannot be imported from here. It exists so
// the similarity source is asserted against the threshold rule that consumes
// it rather than against a restatement of the intended value.
func vectorizeRecallEligible(score float64, source string) bool {
	if strings.HasSuffix(strings.TrimSpace(source), "distance_inverse") {
		return score >= 0.55
	}
	return score >= 0.30
}

func vectorizeSeedSession(t *testing.T, index *vectorizeFakeIndex, sessionID string) {
	t.Helper()
	seed := []struct {
		id     string
		values []float32
	}{
		{"memory:" + sessionID + ":1", []float32{1, 0}},
		{"memory:" + sessionID + ":2", []float32{0.8, 0.6}},
		{"memory:" + sessionID + ":3", []float32{0.6, 0.8}},
		{"memory:" + sessionID + ":4", []float32{0, 1}},
	}
	for _, doc := range seed {
		index.put(doc.id, doc.values, map[string]any{
			vectorizeDocumentIDKey:   doc.id,
			vectorizeDocumentTextKey: "seeded " + doc.id,
			"tier":                   "memory",
			"chat_session_id":        sessionID,
			"source_table":           "memories",
		})
	}
}

func newVectorizeIndexedStore(t *testing.T) (*vectorizeStore, *vectorizeFakeIndex, *vectorizeWorkerStub) {
	t.Helper()
	index := newVectorizeFakeIndex(true)
	stub := newVectorizeWorkerStub(t, index.responder())
	return stub.store(t), index, stub
}

// primeVectorizeProbe performs the operation a live process performs before it
// ever enumerates: one recall, which is also how the index dimension is learned.
// A process that has written or recalled nothing cannot enumerate at all, and
// TestVectorizeEnumerationNeedsAnObservedIndexDimension covers that.
func primeVectorizeProbe(t *testing.T, store *vectorizeStore, query []float32) {
	t.Helper()
	if _, err := store.Search(context.Background(), "", query, 1, ""); err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("prime the index dimension with a recall: %v", err)
	}
	if store.probeVector() == nil {
		t.Fatal("a recall must learn the index dimension")
	}
}

func TestVectorizeStoreAdvertisesEveryOptionalCapability(t *testing.T) {
	stub := newVectorizeWorkerStub(t, newVectorizeFakeIndex(true).responder())
	store := stub.store(t)
	// The runtime discovers these with a type switch. A missing one is a silent
	// recall-quality regression, not a loud failure.
	if _, ok := any(store).(DocumentDeleter); !ok {
		t.Error("vectorize store does not implement DocumentDeleter; a rollback cannot delete the exact ids it knows")
	}
	if _, ok := any(store).(ExactDocumentReader); !ok {
		t.Error("vectorize store does not implement ExactDocumentReader; the outbox and startup recovery cannot read a write back")
	}
	if _, ok := any(store).(DocumentLister); !ok {
		t.Error("vectorize store does not implement DocumentLister; integrity audits cannot enumerate a session")
	}
	if _, ok := any(store).(ExactMetadataQuerier); !ok {
		t.Error("vectorize store does not implement ExactMetadataQuerier; reference diagnostics lose the provider measurement")
	}
	raw, err := NewVectorizeStore(nil)
	if err == nil {
		t.Fatal("NewVectorizeStore(nil) must fail")
	}
	if raw != nil {
		t.Errorf("NewVectorizeStore(nil) store = %#v, want nil", raw)
	}
}

func TestVectorizeSearchUsesTheCosineScaleAndTheCallerLimit(t *testing.T) {
	store, index, stub := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")

	docs, err := store.Search(context.Background(), "sess-1", []float32{1, 0}, 2, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("Search returned %d documents, want the requested 2", len(docs))
	}
	if docs[0].ID != "memory:sess-1:1" || docs[1].ID != "memory:sess-1:2" {
		t.Fatalf("Search order = %q, %q; want the two closest documents", docs[0].ID, docs[1].ID)
	}
	for _, doc := range docs {
		if !doc.SimilarityAvailable {
			t.Errorf("%s: SimilarityAvailable = false, want true", doc.ID)
		}
		if doc.SimilaritySource != vectorizeSimilaritySource {
			t.Errorf("%s: SimilaritySource = %q, want %q", doc.ID, doc.SimilaritySource, vectorizeSimilaritySource)
		}
		// The single worst bug available to this provider: a source that ends
		// in distance_inverse selects the 0.55 threshold and drops cosine hits
		// in the 0.30 to 0.55 band without a word anywhere.
		if strings.HasSuffix(doc.SimilaritySource, "distance_inverse") {
			t.Errorf("%s: SimilaritySource %q selects the inverse-distance threshold", doc.ID, doc.SimilaritySource)
		}
		if !vectorizeRecallEligible(doc.Similarity, doc.SimilaritySource) {
			t.Errorf("%s: similarity %f is not eligible under the recall threshold", doc.ID, doc.Similarity)
		}
		if math.Abs(doc.Distance-(1-doc.Similarity)) > 1e-9 {
			t.Errorf("%s: Distance = %f, want 1 - similarity = %f", doc.ID, doc.Distance, 1-doc.Similarity)
		}
	}
	if math.Abs(docs[0].Similarity-1) > 1e-9 {
		t.Errorf("closest similarity = %f, want 1", docs[0].Similarity)
	}

	queries := stub.callsFor(cloudflarebridge.OpVectorQuery)
	if len(queries) != 1 {
		t.Fatalf("vector.query count = %d, want 1", len(queries))
	}
	payload := queries[0].Payload
	if got := int(vectorizeStubFloat(payload["topK"])); got != 2 {
		t.Errorf("topK = %d, want the caller-requested limit 2", got)
	}
	if vectorizeStubBool(payload["includeValues"]) {
		t.Error("a recall query must not request stored values")
	}
	filter, present := vectorizeStubFilter(payload)
	if !present {
		t.Fatal("a session-scoped recall must send a filter")
	}
	if filter["chat_session_id"] != "sess-1" {
		t.Errorf("filter = %#v, want the session clause", filter)
	}
}

func TestVectorizeSearchSortsBeforeItTruncates(t *testing.T) {
	// The provider returns its own order. The stable sort is what makes the
	// requested limit deterministic, and truncation happens after it.
	stub := newVectorizeWorkerStub(t, func(call vectorizeStubCall) (string, *vectorizeStubFailure) {
		if call.Operation != cloudflarebridge.OpVectorQuery {
			return "", &vectorizeStubFailure{Code: "vector_request_invalid", Message: call.Operation}
		}
		return `{"matches":[
			{"id":"memory:s:1","score":0.42,"metadata":{"tier":"memory"}},
			{"id":"memory:s:2","score":0.81,"metadata":{"tier":"memory"}},
			{"id":"memory:s:3","score":0.10,"metadata":{"tier":"memory"}},
			{"id":"memory:s:4","score":0.65,"metadata":{"tier":"memory"}}
		]}`, nil
	})
	store := stub.store(t)
	docs, err := store.Search(context.Background(), "", []float32{1, 0}, 2, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(docs) != 2 || docs[0].ID != "memory:s:2" || docs[1].ID != "memory:s:4" {
		t.Fatalf("Search = %v, want the two highest scores in descending order", vectorizeDocumentIDs(docs))
	}
	queries := stub.callsFor(cloudflarebridge.OpVectorQuery)
	if len(queries) != 1 {
		t.Fatalf("vector.query count = %d, want 1", len(queries))
	}
	if _, present := vectorizeStubFilter(queries[0].Payload); present {
		t.Error("an empty session and filter must send no filter at all, never an empty object")
	}
}

func TestVectorizeSearchSurfacesAMissingIndexAsNotFound(t *testing.T) {
	store, index, _ := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	if _, err := store.Search(context.Background(), "empty-session", []float32{1, 0}, 3, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Search on an empty session: err = %v, want ErrNotFound", err)
	}
	if _, err := store.Search(context.Background(), "sess-1", nil, 3, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Search without a query vector: err = %v, want ErrNotFound", err)
	}
}

func TestVectorizeSearchBuildsAFlatFilterAndNeverSendsAnEmptyOne(t *testing.T) {
	store, index, stub := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	if _, err := store.Search(context.Background(), "sess-1", []float32{1, 0}, 5, `tier == "memory"`); err != nil {
		t.Fatalf("Search: %v", err)
	}
	queries := stub.callsFor(cloudflarebridge.OpVectorQuery)
	filter, present := vectorizeStubFilter(queries[len(queries)-1].Payload)
	if !present {
		t.Fatal("a tier filter must be sent")
	}
	if filter["chat_session_id"] != "sess-1" || filter["tier"] != "memory" {
		t.Errorf("filter = %#v, want the session and tier clauses flattened into one object", filter)
	}
	docs, err := store.Search(context.Background(), "sess-1", []float32{1, 0}, 5, `source_table == "episodes"`)
	if err == nil {
		t.Fatalf("Search with an unsatisfiable filter returned %v, want ErrNotFound", vectorizeDocumentIDs(docs))
	}
	queries = stub.callsFor(cloudflarebridge.OpVectorQuery)
	filter, _ = vectorizeStubFilter(queries[len(queries)-1].Payload)
	if filter["source_table"] != "episodes" || len(filter) != 2 {
		t.Errorf("filter = %#v, want the source table clause beside the session clause", filter)
	}
}

func TestVectorizeUpsertSendsTheSameIdentityMetadataChromaWrites(t *testing.T) {
	store, index, _ := newVectorizeIndexedStore(t)
	docs := []VectorDocument{{
		ID:                    "memory:sess-1:7",
		Embedding:             []float32{0.25, 0.5},
		Tier:                  "memory",
		ChatSessionID:         "sess-1",
		SourceTable:           "memories",
		SourceRowID:           "7",
		SchemaVersion:         "memory.v1",
		DocumentText:          "the traveller remembers the red gate",
		SearchTextPolicy:      "contextualized",
		RawLanguage:           "ko",
		SummaryLanguage:       "en",
		SessionOutputLanguage: "en",
		AliasCount:            2,
		MigrationID:           41,
		MigratedFromSessionID: "sess-0",
		Metadata:              map[string]any{"work_id": "work-1", "nested": map[string]any{"ignored": true}},
	}}
	if err := store.Upsert(context.Background(), "sess-1", docs); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if !index.has("memory:sess-1:7") {
		t.Fatal("the document was not written")
	}
	stored := index.vectors["memory:sess-1:7"]
	meta := stored.meta
	if meta["tier"] != "memory" || meta["chat_session_id"] != "sess-1" {
		t.Errorf("identity metadata = %#v, want the reserved session and tier fields", meta)
	}
	if meta["source_table"] != "memories" || meta["source_row_id"] != "7" || meta["schema_version"] != "memory.v1" {
		t.Errorf("source metadata = %#v, want the ChromaDB identity fields", meta)
	}
	if meta["search_text_policy"] != "contextualized" || meta["raw_language"] != "ko" ||
		meta["summary_language"] != "en" || meta["session_output_language"] != "en" {
		t.Errorf("language metadata = %#v, want the optional ChromaDB fields", meta)
	}
	if vectorizeStubFloat(meta["alias_count"]) != 2 || vectorizeStubString(meta["migration_id"]) != "41" ||
		meta["migrated_from_session_id"] != "sess-0" {
		t.Errorf("migration metadata = %#v, want the ChromaDB migration fields", meta)
	}
	if meta[vectorizeDocumentIDKey] != "memory:sess-1:7" {
		t.Errorf("reserved document id metadata = %#v, want the id a get-by-id filter needs", meta[vectorizeDocumentIDKey])
	}
	if meta[vectorizeDocumentTextKey] != "the traveller remembers the red gate" {
		t.Errorf("document text metadata = %#v, want the text the readback fingerprint is computed over", meta[vectorizeDocumentTextKey])
	}
	if meta["work_id"] != "work-1" {
		t.Errorf("custom scalar metadata missing: %#v", meta)
	}
	if _, exists := meta["nested"]; exists {
		t.Errorf("non-scalar metadata must not be sent: %#v", meta)
	}
	if vectorizeStubFloat(meta["embedding_dim"]) != 2 {
		t.Errorf("embedding_dim = %#v, want 2", meta["embedding_dim"])
	}
}

func TestVectorizeUpsertRejectsAnIncompleteWrite(t *testing.T) {
	stub := newVectorizeWorkerStub(t, func(call vectorizeStubCall) (string, *vectorizeStubFailure) {
		return `{"upserted":1}`, nil
	})
	store := stub.store(t)
	err := store.Upsert(context.Background(), "sess-1", []VectorDocument{
		{ID: "a", Embedding: []float32{1, 0}, ChatSessionID: "sess-1"},
		{ID: "b", Embedding: []float32{0, 1}, ChatSessionID: "sess-1"},
	})
	if err == nil {
		t.Fatal("a short upsert count must be reported, not ignored")
	}
	if !strings.Contains(err.Error(), "1 of 2") {
		t.Errorf("err = %v, want the accepted and requested counts", err)
	}
}

func TestVectorizeUpsertRejectsMixedDimensionsBeforeTheBridge(t *testing.T) {
	store, _, stub := newVectorizeIndexedStore(t)
	err := store.Upsert(context.Background(), "sess-1", []VectorDocument{
		{ID: "a", Embedding: []float32{1, 0, 0}, ChatSessionID: "sess-1"},
		{ID: "b", Embedding: []float32{0, 1}, ChatSessionID: "sess-1"},
	})
	if err == nil {
		t.Fatal("a mixed-dimension batch must be reported locally")
	}
	if !strings.Contains(err.Error(), "dimension") {
		t.Errorf("err = %v, want it to name the dimension mismatch", err)
	}
	if calls := stub.callsFor(cloudflarebridge.OpVectorUpsert); len(calls) != 0 {
		t.Errorf("vector.upsert count = %d, want 0: a permanently bad batch must not be replayed as a retryable execution failure", len(calls))
	}
	if err := store.Upsert(context.Background(), "sess-1", []VectorDocument{{ID: "a", ChatSessionID: "sess-1"}}); err == nil {
		t.Error("a document without an embedding must be reported, not sent")
	}
}

func TestVectorizeDeleteDocumentsIsIdempotentAndSilentWhenEmpty(t *testing.T) {
	store, index, stub := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	ctx := context.Background()
	if err := store.DeleteDocuments(ctx, nil); err != nil {
		t.Fatalf("DeleteDocuments(nil): %v", err)
	}
	if err := store.DeleteDocuments(ctx, []string{"  ", ""}); err != nil {
		t.Fatalf("DeleteDocuments of the empty string only: %v", err)
	}
	if calls := stub.callsFor(cloudflarebridge.OpVectorDelete); len(calls) != 0 {
		t.Errorf("vector.delete count = %d, want 0 for an empty id list", len(calls))
	}
	if err := store.DeleteDocuments(ctx, []string{"memory:sess-1:1", "memory:sess-1:1", " "}); err != nil {
		t.Fatalf("DeleteDocuments: %v", err)
	}
	if index.has("memory:sess-1:1") {
		t.Error("the requested document still exists")
	}
	calls := stub.callsFor(cloudflarebridge.OpVectorDelete)
	if len(calls) != 1 {
		t.Fatalf("vector.delete count = %d, want 1", len(calls))
	}
	ids, _ := calls[0].Payload["ids"].([]any)
	if len(ids) != 1 || vectorizeStubString(ids[0]) != "memory:sess-1:1" {
		t.Errorf("delete ids = %#v, want the deduplicated requested ids", ids)
	}
	// The outbox replays a delete after a partial failure. An absent id is the
	// state the caller asked for, so a replay must not fail.
	if err := store.DeleteDocuments(ctx, []string{"memory:sess-1:1"}); err != nil {
		t.Fatalf("replaying a delete of an absent id: %v", err)
	}
}

func TestVectorizeDeleteSessionEnumeratesTheSessionOnly(t *testing.T) {
	store, index, stub := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	vectorizeSeedSession(t, index, "sess-2")
	primeVectorizeProbe(t, store, []float32{1, 0})
	queriesBefore := len(stub.callsFor(cloudflarebridge.OpVectorQuery))
	if err := store.DeleteSession(context.Background(), "sess-1"); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if index.has("memory:sess-1:1") || index.has("memory:sess-1:4") {
		t.Error("the session documents survived the sweep")
	}
	if !index.has("memory:sess-2:1") {
		t.Error("another session was deleted")
	}
	if index.size() != 4 {
		t.Errorf("index size = %d, want the other session only", index.size())
	}
	// Enumeration is an increasing-topK query, so a session smaller than one
	// page costs exactly one round trip.
	if queries := stub.callsFor(cloudflarebridge.OpVectorQuery); len(queries)-queriesBefore != 1 {
		t.Errorf("enumeration round trips = %d, want 1 for a four document session", len(queries)-queriesBefore)
	}
	deletes := stub.callsFor(cloudflarebridge.OpVectorDelete)
	if len(deletes) != 1 {
		t.Fatalf("vector.delete count = %d, want 1 batch", len(deletes))
	}
	ids, _ := deletes[0].Payload["ids"].([]any)
	if len(ids) != 4 {
		t.Errorf("delete ids = %d, want the 4 enumerated documents", len(ids))
	}
	if err := store.DeleteSession(context.Background(), "  "); err != nil {
		t.Fatalf("DeleteSession of an empty session id: %v", err)
	}
}

// vectorizePagedResponder answers every query with min(topK, total) synthetic
// matches, so an enumeration sweep can be driven to its cap without a ten
// thousand document index. The sweep completeness rule is the only thing under
// test here: a short page proves the filtered set is smaller than the requested
// topK, and a full page at every step does not.
func vectorizePagedResponder(total int) func(vectorizeStubCall) (string, *vectorizeStubFailure) {
	return func(call vectorizeStubCall) (string, *vectorizeStubFailure) {
		switch call.Operation {
		case cloudflarebridge.OpVectorUpsert:
			entries, _ := call.Payload["vectors"].([]any)
			return fmt.Sprintf(`{"upserted":%d}`, len(entries)), nil
		case cloudflarebridge.OpVectorDelete:
			ids, _ := call.Payload["ids"].([]any)
			return fmt.Sprintf(`{"deleted":%d}`, len(ids)), nil
		case cloudflarebridge.OpVectorHealth:
			return `{"bound":true}`, nil
		case cloudflarebridge.OpVectorQuery:
		default:
			return "", &vectorizeStubFailure{Code: "not_implemented", Message: call.Operation}
		}
		topK := int(vectorizeStubFloat(call.Payload["topK"]))
		if topK > total {
			topK = total
		}
		matches := make([]string, 0, topK)
		for i := 0; i < topK; i++ {
			matches = append(matches, fmt.Sprintf(`{"id":"memory:sess-1:%d","score":0.5,"values":[1,0],"metadata":{"chat_session_id":"sess-1"}}`, i))
		}
		return `{"matches":[` + strings.Join(matches, ",") + `]}`, nil
	}
}

func TestVectorizeDeleteSessionFailsRatherThanDeletingPartOfASession(t *testing.T) {
	// A session at the enumeration cap still answers a full page on the last
	// step, so the sweep cannot prove it is complete. A half-deleted session is
	// worse than an untouched one, because the vectors that survive still
	// answer recalls.
	store := newVectorizeWorkerStub(t, vectorizePagedResponder(vectorizeListMaxDocuments)).store(t)
	if err := store.Upsert(context.Background(), "sess-1", []VectorDocument{{
		ID: "memory:sess-1:0", Embedding: []float32{1, 0}, ChatSessionID: "sess-1",
	}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	err := store.DeleteSession(context.Background(), "sess-1")
	if err == nil {
		t.Fatal("an incomplete enumeration must fail instead of reporting a finished delete")
	}
	if !strings.Contains(err.Error(), "cap") {
		t.Errorf("err = %v, want it to name the enumeration cap", err)
	}
}

func TestVectorizeQueryExactKeepsProviderOrderAndOmitsTheDistance(t *testing.T) {
	store, index, stub := newVectorizeIndexedStore(t)
	index.put("reference:a", []float32{1, 0}, map[string]any{
		vectorizeDocumentIDKey:   "reference:a",
		vectorizeDocumentTextKey: "alpha claim",
		"work_id":                "work-1",
		"continuity_id":          "main",
		"review_status":          "approved",
	})
	index.put("reference:b", []float32{0.9, 0.44}, map[string]any{
		vectorizeDocumentIDKey:   "reference:b",
		vectorizeDocumentTextKey: "beta claim",
		"work_id":                "work-1",
		"continuity_id":          "main",
		"review_status":          "approved",
	})
	index.put("other:c", []float32{1, 0}, map[string]any{
		vectorizeDocumentIDKey:   "other:c",
		vectorizeDocumentTextKey: "not approved",
		"work_id":                "work-1",
		"continuity_id":          "main",
		"review_status":          "draft",
	})
	querier := any(store).(ExactMetadataQuerier)
	results, err := querier.QueryExact(context.Background(), ExactQuery{
		Embedding: []float32{1, 0},
		Limit:     10,
		Where: map[string]any{"$and": []map[string]any{
			{"work_id": "work-1"},
			{"continuity_id": "main"},
			{"review_status": "approved"},
		}},
	})
	if err != nil {
		t.Fatalf("QueryExact: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("QueryExact returned %d results, want the 2 approved claims", len(results))
	}
	for i, result := range results {
		if result.ChromaRank != i+1 {
			t.Errorf("result %d: ChromaRank = %d, want the provider position %d", i, result.ChromaRank, i+1)
		}
		if result.DistanceAvailable {
			t.Errorf("%s: DistanceAvailable = true, but Vectorize reports a score, not a distance", result.Document.ID)
		}
		if result.Distance != 0 {
			t.Errorf("%s: Distance = %f, want it left unmeasured", result.Document.ID, result.Distance)
		}
		if !result.CosineAvailable {
			t.Errorf("%s: CosineAvailable = false, want the cosine of the two real vectors", result.Document.ID)
		}
		if !result.Document.SimilarityAvailable || result.Document.SimilaritySource != vectorizeSimilaritySource {
			t.Errorf("%s: provider measurement not preserved: %+v", result.Document.ID, result.Document)
		}
		if len(result.Document.Embedding) == 0 {
			t.Errorf("%s: stored values were not requested, so the probe cannot be reproduced", result.Document.ID)
		}
		if result.Document.DocumentText == "" {
			t.Errorf("%s: document text did not survive the metadata round trip", result.Document.ID)
		}
	}
	if results[0].Document.ID != "reference:a" {
		t.Errorf("first result = %q, want the provider order to be preserved", results[0].Document.ID)
	}
	// An exact query is a diagnostic: it must not be narrowed by a reranking
	// limit of its own.
	payload := stub.callsFor(cloudflarebridge.OpVectorQuery)[0].Payload
	if got := int(vectorizeStubFloat(payload["topK"])); got != 10 {
		t.Errorf("topK = %d, want the requested 10", got)
	}
	if !vectorizeStubBool(payload["includeValues"]) {
		t.Error("an exact query must request the stored values")
	}
	filter, _ := vectorizeStubFilter(payload)
	if len(filter) != 3 || filter["review_status"] != "approved" {
		t.Errorf("filter = %#v, want the $and clauses flattened into one equality object", filter)
	}
}

func TestVectorizeQueryExactRefusesAFilterItCannotExpress(t *testing.T) {
	store, _, _ := newVectorizeIndexedStore(t)
	_, err := any(store).(ExactMetadataQuerier).QueryExact(context.Background(), ExactQuery{
		Embedding: []float32{1, 0},
		Limit:     3,
		Where:     map[string]any{"$or": []map[string]any{{"tier": "memory"}, {"tier": "evidence"}}},
	})
	if err == nil {
		t.Fatal("a filter the transport cannot express must fail rather than widen the candidate set silently")
	}
	if !strings.Contains(err.Error(), "exact-match AND") {
		t.Errorf("err = %v, want it to name the filter limitation", err)
	}
	if _, err := any(store).(ExactMetadataQuerier).QueryExact(context.Background(), ExactQuery{Limit: 3}); !errors.Is(err, ErrNotFound) {
		t.Errorf("QueryExact without an embedding: err = %v, want ErrNotFound", err)
	}
}

func TestVectorizeRebuildRefusesInsteadOfPretending(t *testing.T) {
	store, index, stub := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	err := store.Rebuild(context.Background(), "sess-1")
	if err == nil {
		t.Fatal("Rebuild must not report success it cannot deliver")
	}
	if !strings.Contains(err.Error(), "no collection swap") {
		t.Errorf("err = %v, want it to name the missing atomic swap", err)
	}
	if index.size() != 4 {
		t.Errorf("index size = %d, want the index untouched", index.size())
	}
	if len(stub.allCalls()) != 0 {
		t.Errorf("Rebuild issued %d bridge calls, want none", len(stub.allCalls()))
	}
}

func TestVectorizeHealthReportsOnlyWhatTheTransportCanObserve(t *testing.T) {
	store, _, _ := newVectorizeIndexedStore(t)
	health, err := store.Health(context.Background())
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if health.Status != "ok" || !health.ModelReady {
		t.Fatalf("health = %+v, want a ready binding", health)
	}
	if health.TotalCount != 0 {
		t.Errorf("TotalCount = %d, want 0: this contract has no collection count", health.TotalCount)
	}
	if health.PersistDir != "" || health.ProjectModel != "" {
		t.Errorf("health = %+v, want no local directory and no provider model", health)
	}
	joined := strings.Join(health.PreflightIssues, " | ")
	if !strings.Contains(joined, "count") {
		t.Errorf("preflight issues = %v, want the unobservable count reported", health.PreflightIssues)
	}
	if !strings.Contains(joined, "dimension") {
		t.Errorf("preflight issues = %v, want the unobserved index dimension reported", health.PreflightIssues)
	}

	unbound := newVectorizeWorkerStub(t, newVectorizeFakeIndex(false).responder())
	health, err = unbound.store(t).Health(context.Background())
	if err != nil {
		t.Fatalf("an unbound binding must render, not fail: %v", err)
	}
	if health.Status != "unhealthy" || health.ModelReady {
		t.Fatalf("health = %+v, want an unhealthy report for an absent binding", health)
	}
	if len(health.PreflightIssues) == 0 {
		t.Error("an unhealthy report must say why")
	}

	failing := newVectorizeWorkerStub(t, func(vectorizeStubCall) (string, *vectorizeStubFailure) {
		return "", &vectorizeStubFailure{Code: "vector_execution_failed", Message: "binding query failed", Retryable: true}
	})
	health, err = failing.store(t).Health(context.Background())
	if err == nil {
		t.Fatal("a failing health call must report its error")
	}
	if health.Status != "error" || health.ModelReady {
		t.Errorf("health = %+v, want an error status with the model marked not ready", health)
	}
}

func TestVectorizeCountEnumeratesTheSessionAndRefusesAWholeIndexCount(t *testing.T) {
	store, index, _ := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	vectorizeSeedSession(t, index, "sess-2")
	primeVectorizeProbe(t, store, []float32{1, 0})
	ctx := context.Background()
	count, err := store.Count(ctx, "sess-1")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 4 {
		t.Errorf("Count = %d, want 4", count)
	}
	if _, err := store.Count(ctx, "  "); err == nil {
		t.Error("a whole-index count must fail: this contract has no collection count")
	} else if !strings.Contains(err.Error(), "collection count") {
		t.Errorf("err = %v, want it to name the missing collection count", err)
	}
}

func TestVectorizeCountIsExactAtTheEnumerationCap(t *testing.T) {
	// A session of one document short of the cap terminates on the last page and
	// is counted exactly. A session at the cap still answers a full page on the
	// last step, so the sweep cannot prove it is complete and must fail rather
	// than report a number that may be short.
	for _, tc := range []struct {
		documents int
		want      int
		wantErr   bool
	}{
		{vectorizeListMaxDocuments - 1, vectorizeListMaxDocuments - 1, false},
		{vectorizeListMaxDocuments, 0, true},
	} {
		store := newVectorizeWorkerStub(t, vectorizePagedResponder(tc.documents)).store(t)
		if err := store.Upsert(context.Background(), "sess-1", []VectorDocument{{
			ID: "memory:sess-1:0", Embedding: []float32{1, 0}, ChatSessionID: "sess-1",
		}}); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
		count, err := store.Count(context.Background(), "sess-1")
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%d documents: Count = %d, want a failure rather than a partial count", tc.documents, count)
			}
			if !strings.Contains(err.Error(), "cap") {
				t.Errorf("%d documents: err = %v, want it to name the enumeration cap", tc.documents, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%d documents: Count: %v", tc.documents, err)
		}
		if count != tc.want {
			t.Errorf("%d documents: Count = %d, want %d", tc.documents, count, tc.want)
		}
	}
}

func TestVectorizeGetDocumentsReadsExactlyTheRequestedIDsWithTheirVectors(t *testing.T) {
	store, index, stub := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	primeVectorizeProbe(t, store, []float32{1, 0})
	readsBefore := len(stub.callsFor(cloudflarebridge.OpVectorQuery))
	docs, err := any(store).(ExactDocumentReader).GetDocuments(context.Background(), []string{
		"memory:sess-1:3", "memory:sess-1:1", "memory:sess-1:3", "  ", "memory:sess-1:absent",
	})
	if err != nil {
		t.Fatalf("GetDocuments: %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("GetDocuments returned %d documents, want the 2 that exist in request order", len(docs))
	}
	if docs[0].ID != "memory:sess-1:3" || docs[1].ID != "memory:sess-1:1" {
		t.Errorf("GetDocuments = %v, want the requested order", vectorizeDocumentIDs(docs))
	}
	// startup_vector_recovery.go reuses this embedding to probe the index and
	// to reconcile the cached snapshot, so it must not be empty.
	if len(docs[0].Embedding) != 2 || docs[0].Embedding[0] != 0.6 || docs[0].Embedding[1] != 0.8 {
		t.Errorf("stored values = %v, want the values the document was written with", docs[0].Embedding)
	}
	if docs[0].DocumentText != "seeded memory:sess-1:3" {
		t.Errorf("DocumentText = %q, want the text carried through metadata", docs[0].DocumentText)
	}
	if docs[0].ChatSessionID != "sess-1" || docs[0].Tier != "memory" {
		t.Errorf("document identity = %+v, want the ChromaDB identity fields", docs[0])
	}
	if _, leaked := docs[0].Metadata[vectorizeDocumentTextKey]; leaked {
		t.Error("the reserved text field must not be exposed as document metadata")
	}
	if docs[0].SimilarityAvailable || docs[0].Distance != 0 {
		t.Error("a read must not report a similarity: the ranking that produced it is a dimension probe")
	}
	reads := stub.callsFor(cloudflarebridge.OpVectorQuery)
	if len(reads)-readsBefore != 3 {
		t.Errorf("read round trips = %d, want one per requested id", len(reads)-readsBefore)
	}
	for _, call := range reads[readsBefore:] {
		if !vectorizeStubBool(call.Payload["includeValues"]) {
			t.Error("a read must request the stored values")
		}
		filter, _ := vectorizeStubFilter(call.Payload)
		if len(filter) != 1 || filter[vectorizeDocumentIDKey] == nil {
			t.Errorf("read filter = %#v, want the reserved document id equality", filter)
		}
	}
	empty, err := any(store).(ExactDocumentReader).GetDocuments(context.Background(), []string{" ", ""})
	if err != nil {
		t.Fatalf("GetDocuments of the empty string only: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("GetDocuments returned %v, want no documents and no bridge call", empty)
	}
}

func TestVectorizeListDocumentsEnumeratesTheSessionAndSortsByID(t *testing.T) {
	store, index, _ := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	vectorizeSeedSession(t, index, "sess-2")
	primeVectorizeProbe(t, store, []float32{1, 0})
	docs, err := any(store).(DocumentLister).ListDocuments(context.Background(), "sess-1")
	if err != nil {
		t.Fatalf("ListDocuments: %v", err)
	}
	got := vectorizeDocumentIDs(docs)
	want := []string{"memory:sess-1:1", "memory:sess-1:2", "memory:sess-1:3", "memory:sess-1:4"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ListDocuments = %v, want %v", got, want)
	}
	for _, doc := range docs {
		if len(doc.Embedding) == 0 {
			t.Fatalf("%s: enumeration must return the stored values", doc.ID)
		}
	}
	all, err := any(store).(DocumentLister).ListDocuments(context.Background(), "")
	if err != nil {
		t.Fatalf("ListDocuments of the whole index: %v", err)
	}
	if len(all) != 8 {
		t.Errorf("whole index listing = %d documents, want 8", len(all))
	}
}

func TestVectorizeEnumerationNeedsAnObservedIndexDimension(t *testing.T) {
	store, index, _ := newVectorizeIndexedStore(t)
	vectorizeSeedSession(t, index, "sess-1")
	ctx := context.Background()
	// Vectorize rejects a query whose vector dimension differs from the index
	// and this contract reports no dimension, so a read-only process that has
	// neither written nor recalled anything cannot enumerate. It is told so
	// rather than handed a fabricated result.
	if _, err := any(store).(ExactDocumentReader).GetDocuments(ctx, []string{"memory:sess-1:1"}); err == nil {
		t.Error("GetDocuments must report the unknown dimension instead of guessing one")
	}
	if _, err := any(store).(DocumentLister).ListDocuments(ctx, "sess-1"); err == nil {
		t.Error("ListDocuments must report the unknown dimension instead of guessing one")
	}
	if err := store.DeleteSession(ctx, "sess-1"); err == nil {
		t.Error("DeleteSession must report the unknown dimension instead of guessing one")
	}
	if _, err := store.Count(ctx, "sess-1"); err == nil {
		t.Error("Count must report the unknown dimension instead of guessing one")
	}
	if index.size() != 4 {
		t.Fatalf("index size = %d, want no enumeration to have deleted anything", index.size())
	}
	if err := store.Upsert(ctx, "sess-1", []VectorDocument{{
		ID: "memory:sess-1:9", Embedding: []float32{0.5, 0.5}, ChatSessionID: "sess-1",
	}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	docs, err := any(store).(DocumentLister).ListDocuments(ctx, "sess-1")
	if err != nil {
		t.Fatalf("ListDocuments after the dimension was observed: %v", err)
	}
	if len(docs) != 5 {
		t.Errorf("ListDocuments = %d documents, want 5", len(docs))
	}
}

func TestVectorizeDimensionFailureIsReportedAndReobserved(t *testing.T) {
	failing := true
	stub := newVectorizeWorkerStub(t, func(call vectorizeStubCall) (string, *vectorizeStubFailure) {
		switch call.Operation {
		case cloudflarebridge.OpVectorUpsert:
			return `{"upserted":1}`, nil
		case cloudflarebridge.OpVectorQuery:
			if failing {
				failing = false
				return "", &vectorizeStubFailure{
					Code:      "vector_execution_failed",
					Message:   "query vector dimension 768 does not match the index dimension 3",
					Retryable: true,
				}
			}
			return `{"matches":[]}`, nil
		default:
			return "", &vectorizeStubFailure{Code: "not_implemented", Message: call.Operation}
		}
	})
	store := stub.store(t)
	if err := store.Upsert(context.Background(), "s", []VectorDocument{{
		ID: "memory:s:0", Embedding: []float32{1, 0, 0}, ChatSessionID: "s",
	}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	_, err := any(store).(DocumentLister).ListDocuments(context.Background(), "s")
	if err == nil {
		t.Fatal("a dimension failure must be reported")
	}
	if !strings.Contains(err.Error(), "dimension mismatch") {
		t.Errorf("err = %v, want it to name the dimension mismatch", err)
	}
	if store.probeVector() != nil {
		t.Error("a dimension failure must discard the probe so the next observation re-learns it")
	}
}

func TestVectorizeBridgeErrorRetryabilityReachesTheOutbox(t *testing.T) {
	calls := 0
	stub := newVectorizeWorkerStub(t, func(call vectorizeStubCall) (string, *vectorizeStubFailure) {
		switch call.Operation {
		case cloudflarebridge.OpVectorUpsert:
			return `{"upserted":1}`, nil
		case cloudflarebridge.OpVectorDelete:
			calls++
			if calls == 1 {
				return "", &vectorizeStubFailure{
					Code:      "vector_execution_failed",
					Message:   "Vectorize delete failed",
					Retryable: true,
				}
			}
			return `{"deleted":1}`, nil
		default:
			return "", &vectorizeStubFailure{Code: "not_implemented", Message: call.Operation}
		}
	})
	store := stub.store(t)
	if err := store.Upsert(context.Background(), "s", []VectorDocument{{
		ID: "memory:s:0", Embedding: []float32{1, 0}, ChatSessionID: "s",
	}}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	// A retryable execution failure must read as retryable, so the outbox
	// replays an idempotent delete instead of parking it.
	err := store.DeleteDocuments(context.Background(), []string{"memory:s:0"})
	var bridgeErr *cloudflarebridge.BridgeError
	if !errors.As(err, &bridgeErr) {
		t.Fatalf("err = %v, want a *BridgeError", err)
	}
	if !bridgeErr.Retryable || !cloudflarebridge.Retryable(err) {
		t.Error("vector_execution_failed must stay retryable: upsert and delete are both idempotent by document id")
	}
	if err := store.DeleteDocuments(context.Background(), []string{"memory:s:0"}); err != nil {
		t.Fatalf("replaying the delete: %v", err)
	}

	invalid := newVectorizeWorkerStub(t, func(vectorizeStubCall) (string, *vectorizeStubFailure) {
		return "", &vectorizeStubFailure{Code: "vector_request_invalid", Message: "values must be numbers", Retryable: false}
	})
	err = invalid.store(t).Upsert(context.Background(), "s", []VectorDocument{{
		ID: "memory:s:0", Embedding: []float32{1, 0}, ChatSessionID: "s",
	}})
	if !errors.As(err, &bridgeErr) {
		t.Fatalf("err = %v, want a *BridgeError", err)
	}
	if bridgeErr.Retryable || cloudflarebridge.Retryable(err) {
		t.Error("vector_request_invalid must not be retryable: replaying a malformed payload only spins")
	}
}

func vectorizeDocumentIDs(docs []VectorDocument) []string {
	out := make([]string, 0, len(docs))
	for _, doc := range docs {
		out = append(out, doc.ID)
	}
	return out
}
