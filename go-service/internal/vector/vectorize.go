package vector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/cloudflarebridge"
)

// vectorizeStore is the Cloudflare Vectorize provider for Archive Center.
//
// Transport. Vectorize is reached only through the Container-to-Worker bridge
// client. The Worker holds the VECTORIZE binding and this process dispatches
// the four frozen vector.* operations through it. Nothing in this file names an
// account, a database, an index, a namespace, a route, or a domain, and nothing
// authenticates to a Cloudflare API: there is no REST URL and no API token
// anywhere on this side of the bridge. The index is reached, and known, only as
// "the binding the Worker is holding".
//
// Purpose. The store exists to produce the same observable recall as the local
// ChromaDB provider through a different transport, so the similarity policy is
// the load-bearing part of it. Vectorize answers a query with a cosine score
// over the same stored vectors Chroma recomputes in Go after reading the stored
// embedding back, so both providers carry a cosine-scaled Similarity and the
// same 0.30 eligibility threshold applies to both. Converting the score into a
// distance and reporting an inverse-distance similarity would select the 0.55
// threshold in prepareTurnVectorSimilarityEligible instead, and every cosine hit
// between 0.30 and 0.55 would be discarded with nothing anywhere reporting why.
// See vectorizeSimilaritySource.
//
// The four optional capability interfaces below are not decoration. The runtime
// discovers them with a type switch and degrades silently when one is missing,
// so a store without them is a quiet recall-quality regression rather than a
// startup failure:
//
//   - DocumentDeleter: a turn rollback knows the canonical row ids it must
//     remove. Without DeleteDocuments the rollback can only fall back to a
//     session-wide sweep, or give up and leave vectors that still match.
//
//   - ExactDocumentReader: the memory-vector outbox reads a document straight
//     back after writing or deleting it, and startup_vector_recovery.go reads
//     back every recovered document to prove the batch landed. Without
//     GetDocuments the outbox parks on "vector exact readback is not supported"
//     and recovery cannot probe the collection at all.
//
//   - DocumentLister: integrity audits, session migration previews and the
//     managed-vector admin surface need a full per-session listing. Without
//     ListDocuments those routes silently report a store that cannot be
//     inspected, and startup_vector_recovery.go cannot reconcile its cached
//     snapshot against what the provider actually holds.
//
//   - ExactMetadataQuerier: the reference-library diagnostics exist to prove
//     that the vector database is really query-sensitive. Without QueryExact
//     they cannot observe the provider response order or any raw measurement,
//     and the dashboard reports the capability as missing.
//
// GetDocuments and ListDocuments MUST populate VectorDocument.Embedding, and
// they must request includeValues to get it: Vectorize never returns a stored
// value unless the request asks for one. Search must NOT require values,
// because a recall query does not need the stored vector and it is the
// expensive part of the response payload.

const (
	// vectorizeSimilaritySource labels the origin of VectorDocument.Similarity
	// for every ranked result this provider returns.
	//
	// It must never end in distance_inverse. That suffix is the switch in
	// prepareTurnVectorSimilarityEligible, which raises the bar from the 0.30
	// cosine threshold to the 0.55 inverse-distance threshold. A cosine score
	// of 0.4 would then be dropped from a recall the user can see locally, and
	// the only symptom is a missing memory.
	vectorizeSimilaritySource = "vectorize_cosine_score"

	// vectorizeDocumentIDKey is the reserved metadata field that carries the
	// document id. The frozen contract has no get-by-id operation, so a
	// metadata equality filter is the only way to pin a query to exactly one
	// document.
	vectorizeDocumentIDKey = "archive_center_document_id"

	// vectorizeDocumentTextKey is the reserved metadata field that carries the
	// document text. ChromaDB keeps the text in its own documents column;
	// Vectorize has no document field at all, so metadata is the only place the
	// text can survive the round trip that the outbox readback (a content
	// fingerprint over DocumentText) and the recovery probe depend on.
	vectorizeDocumentTextKey = "archive_center_document"
)

const (
	// vectorizeQueryPageSize is the topK step of an enumeration sweep. It is
	// also the delete batch size, which keeps a single bridge request well
	// inside the payload and argument limits Vectorize imposes.
	vectorizeQueryPageSize = 100

	// Vectorize rejects a full metadata/value query above 50. It also has no
	// cursor, so a full page at this ceiling is explicitly reported as an
	// incomplete diagnostic rather than fabricated as a canonical listing.
	vectorizeFullQueryMaxTopK = 50
	// Retained for package-level compatibility with historical diagnostic tests;
	// it is no longer an enumeration cap.
	vectorizeListMaxDocuments = 10000

	// vectorizeReadConcurrency bounds the parallel per-document reads an
	// exact GetDocuments call issues.
	vectorizeReadConcurrency = 8
)

// vectorizeStore implements VectorStore over the Worker bridge.
type vectorizeStore struct {
	client *cloudflarebridge.Client

	// probeMu guards probe, the most recent embedding this process has seen
	// through any operation. It exists only to carry the index dimension into
	// enumeration queries; see queryProbe.
	probeMu sync.RWMutex
	probe   []float32

	// geometryMu guards dimension, the index width reported by the Worker.
	//
	// It is kept apart from probe because the two are not equally trustworthy.
	// probe is an embedding this process actually wrote or recalled, so it is
	// evidence about the index; dimension is the index's own answer, and it is
	// the only one available on a cold process.
	geometryMu sync.RWMutex
	dimension  int
}

// The runtime type-switches on the optional capabilities, so a missing method
// is a silent regression rather than a compile error. Assert them here.
var (
	_ VectorStore          = (*vectorizeStore)(nil)
	_ DocumentDeleter      = (*vectorizeStore)(nil)
	_ ExactDocumentReader  = (*vectorizeStore)(nil)
	_ DocumentLister       = (*vectorizeStore)(nil)
	_ ExactMetadataQuerier = (*vectorizeStore)(nil)
)

// NewVectorizeStore returns a VectorStore backed by the Worker bridge. The
// client is shared with the D1 provider, so this store never closes it.
func NewVectorizeStore(client *cloudflarebridge.Client) (VectorStore, error) {
	if client == nil {
		return nil, errors.New("vectorize store: bridge client is required")
	}
	return &vectorizeStore{client: client}, nil
}

// vectorizeUpsertVector is one entry of the vector.upsert request.
type vectorizeUpsertVector struct {
	ID       string         `json:"id"`
	Values   []float32      `json:"values"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type vectorizeUpsertPayload struct {
	Vectors []vectorizeUpsertVector `json:"vectors"`
}

type vectorizeUpsertResult struct {
	Upserted int `json:"upserted"`
}

type vectorizeDeletePayload struct {
	IDs []string `json:"ids"`
}

type vectorizeDeleteResult struct {
	Deleted int `json:"deleted"`
}

type vectorizeQueryPayload struct {
	Vector        []float32      `json:"vector"`
	TopK          int            `json:"topK"`
	Filter        map[string]any `json:"filter,omitempty"`
	IncludeValues bool           `json:"includeValues"`
}

type vectorizeQueryMatch struct {
	ID       string         `json:"id"`
	Score    float64        `json:"score"`
	Values   []float64      `json:"values,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

type vectorizeQueryResult struct {
	Matches []vectorizeQueryMatch `json:"matches"`
}

type vectorizeHealthResult struct {
	Bound bool `json:"bound"`
	// Dimensions is the fixed embedding width of the index. It is what makes a
	// read-only enumeration possible on a cold process, and it is a property of
	// the index rather than of this deployment, so reporting it discloses
	// nothing account-specific.
	Dimensions int `json:"dimensions,omitempty"`
	// VectorCount is the index-wide document count. MariaDB remains canonical for
	// per-session counts, but an index-wide figure is useful for spotting an index
	// that has drifted from canonical truth.
	VectorCount int `json:"vectorCount,omitempty"`
	// ProcessedUpToMutation is the index's opaque mutation marker, NOT a count.
	//
	// Vectorize answers with a mutation changeset identifier — a UUID string such
	// as "98b98188-19de-42b2-94d4-1969fa0b7cd6" — and this field was typed as
	// uint64 on the assumption that it was a monotonic watermark. Nothing read the
	// value, so the type looked harmless; what it actually did was make the decode
	// fail, which failed Health, which fails the startup preflight, which stops a
	// real deployment from starting at all.
	//
	// The assumption survived until now because Vectorize is a remote-only binding
	// with no local simulator: every local test decoded a stub that happened to use
	// a number. It is a string because the wire says it is, and nothing here may
	// depend on its shape — it exists to be reported, not interpreted.
	ProcessedUpToMutation string `json:"processedUpToMutation,omitempty"`
	// GeometryError is set when the binding is present but describe() failed. The
	// health report stays useful: a bound-but-unreadable index is still worth
	// reporting, just without geometry.
	GeometryError bool `json:"geometryError,omitempty"`
}

type vectorizeGetPayload struct {
	IDs []string `json:"ids"`
}

// vectorizeGetResult carries exact, unscored documents keyed by id.
type vectorizeGetResult struct {
	Vectors []vectorizeStoredVector `json:"vectors"`
}

// vectorizeStoredVector is one document read back by id.
//
// Values are decoded as []float64 because that is what JSON carries, and
// narrowed to the float32 the index stores on the way into the VectorDocument.
// A get response and a query response arrive over the same JSON encoding, so
// they share the element type deliberately rather than by accident.
type vectorizeStoredVector struct {
	ID       string         `json:"id"`
	Values   []float64      `json:"values"`
	Metadata map[string]any `json:"metadata"`
}

// indexGeometry asks the Worker for the index dimension, caching the answer.
//
// The cache is not an optimisation; it is what keeps a listing from costing a
// health round trip per page. A failure is not cached, so a Worker that can
// describe its index once can do so again after a transient fault.
func (s *vectorizeStore) indexGeometry(ctx context.Context) (int, error) {
	s.geometryMu.Lock()
	cached := s.dimension
	s.geometryMu.Unlock()
	if cached > 0 {
		return cached, nil
	}
	var result vectorizeHealthResult
	if err := s.client.Do(ctx, cloudflarebridge.OpVectorHealth, nil, &result); err != nil {
		return 0, err
	}
	if result.Dimensions <= 0 {
		return 0, errors.New("vectorize store: the Worker did not report an index dimension")
	}
	s.rememberDimension(result.Dimensions)
	return result.Dimensions, nil
}

// rememberDimension caches the index width. A width already learned is never
// overwritten, because a second, different answer means the index was rebuilt
// under this process and the cached probe vectors are now the wrong length. The
// existing embedding probe is the thing that detects that case, on the next
// query, where the error names the cause.
func (s *vectorizeStore) rememberDimension(dimension int) {
	if dimension <= 0 {
		return
	}
	s.geometryMu.Lock()
	if s.dimension == 0 {
		s.dimension = dimension
	}
	s.geometryMu.Unlock()
}

// Search runs one recall query and returns the provider's matches, sorted with
// the same stable comparator and truncated in the same order Chroma uses.
func (s *vectorizeStore) Search(ctx context.Context, sessionID string, vector []float32, limit int, filter string) ([]VectorDocument, error) {
	if len(vector) == 0 {
		return nil, ErrNotFound
	}
	if limit <= 0 {
		limit = 5
	}
	if limit > vectorizeFullQueryMaxTopK {
		return nil, fmt.Errorf("vectorize store: recall limit %d exceeds the %d-result full-metadata limit", limit, vectorizeFullQueryMaxTopK)
	}
	matches, err := s.queryMatches(ctx, vectorizeQueryPayload{
		Vector: vector,
		TopK:   limit,
		Filter: vectorizeSearchFilter(sessionID, filter),
		// A recall query does not need the stored vector, and the stored
		// vector is the expensive part of the response.
		IncludeValues: false,
	})
	if err != nil {
		return nil, err
	}
	// The query succeeded, so the query vector has the index dimension. Learn it
	// even when the answer is empty: a recall that matches nothing still proves
	// the dimension.
	s.rememberProbe(vector)
	if len(matches) == 0 {
		return nil, ErrNotFound
	}
	docs := make([]VectorDocument, 0, len(matches))
	for _, match := range matches {
		doc := s.documentFromMatch(match)
		// Vectorize reports cosine similarity over the stored vector, which is
		// the same quantity Chroma ends up carrying after it overrides its own
		// inverse-distance similarity with a cosine recomputation. Cosine
		// distance is therefore 1 - score on both providers, on the same scale.
		doc.Similarity = match.Score
		doc.SimilarityAvailable = true
		doc.SimilaritySource = vectorizeSimilaritySource
		doc.Distance = 1 - match.Score
		docs = append(docs, doc)
	}
	// The provider returns its own order and the stable sort is what makes a
	// rerun deterministic. This comparator is Chroma's, unchanged.
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
	if len(docs) > limit {
		docs = docs[:limit]
	}
	return docs, nil
}

// QueryExact exposes the provider response order and the provider's own
// measurement, without Archive Center's reranking, overfetching or
// normalisation. It does not re-sort and does not truncate beyond the
// requested limit.
func (s *vectorizeStore) QueryExact(ctx context.Context, query ExactQuery) ([]ExactQueryResult, error) {
	if len(query.Embedding) == 0 {
		return nil, ErrNotFound
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 5
	}
	if limit > vectorizeFullQueryMaxTopK {
		return nil, fmt.Errorf("vectorize store: exact query limit %d exceeds the %d-result full-metadata limit", limit, vectorizeFullQueryMaxTopK)
	}
	filter, err := vectorizeExactFilter(query.Where)
	if err != nil {
		return nil, err
	}
	matches, err := s.queryMatches(ctx, vectorizeQueryPayload{
		Vector:        query.Embedding,
		TopK:          limit,
		Filter:        filter,
		IncludeValues: true,
	})
	if err != nil {
		return nil, err
	}
	s.rememberProbe(query.Embedding)
	if len(matches) == 0 {
		return nil, ErrNotFound
	}
	results := make([]ExactQueryResult, 0, len(matches))
	for i, match := range matches {
		doc := s.documentFromMatch(match)
		// The provider measurement is kept on the document so a diagnostic can
		// still read what Vectorize actually answered.
		doc.Similarity = match.Score
		doc.SimilarityAvailable = true
		doc.SimilaritySource = vectorizeSimilaritySource
		doc.Distance = 1 - match.Score
		result := ExactQueryResult{Document: doc, ChromaRank: i + 1}
		// What Vectorize does not expose is a raw distance. It answers with a
		// single cosine score and nothing else, so ExactQueryResult.Distance
		// stays unavailable instead of carrying 1 - score, which is a
		// re-expression of the score rather than a second, independent
		// measurement. Reporting it as available would let a
		// query-sensitivity diagnostic read the same number twice and conclude
		// that the index is more sensitive than it is. Cosine is still reported
		// below whenever the two real vectors allow it, exactly as Chroma does.
		if similarity, ok := cosineSimilarity(query.Embedding, doc.Embedding); ok {
			result.CosineSimilarity = similarity
			result.CosineAvailable = true
		}
		results = append(results, result)
	}
	return results, nil
}

// Upsert writes documents into the index. Vectorize replaces any vector that
// already has the same id, so an upsert is idempotent and the outbox can replay
// one without duplicating a document.
func (s *vectorizeStore) Upsert(ctx context.Context, sessionID string, docs []VectorDocument) error {
	if len(docs) == 0 {
		return nil
	}
	vectors := make([]vectorizeUpsertVector, 0, len(docs))
	for i, doc := range docs {
		if len(doc.Embedding) == 0 {
			return fmt.Errorf("vectorize store: document %q has no embedding", strings.TrimSpace(doc.ID))
		}
		id := strings.TrimSpace(doc.ID)
		if id == "" {
			id = fmt.Sprintf("%s:%s:%d", doc.SourceTable, sessionID, i+1)
		}
		vectors = append(vectors, vectorizeUpsertVector{
			ID:       id,
			Values:   doc.Embedding,
			Metadata: vectorizeMetadataForDocument(doc, sessionID),
		})
	}
	// One index holds exactly one dimension. A batch that mixes dimensions can
	// only fail, and it can only fail forever, so it is reported here with the
	// reason instead of being sent to become a retryable execution failure.
	if err := vectorizeDimensionConsistency(vectors); err != nil {
		return err
	}
	var result vectorizeUpsertResult
	if err := s.client.Do(ctx, cloudflarebridge.OpVectorUpsert, vectorizeUpsertPayload{Vectors: vectors}, &result); err != nil {
		return fmt.Errorf("vectorize store: upsert %d documents: %w", len(vectors), err)
	}
	if result.Upserted != len(vectors) {
		return fmt.Errorf("vectorize store: upsert accepted %d of %d documents; the batch is replayable because upsert is idempotent", result.Upserted, len(vectors))
	}
	s.rememberProbe(vectors[0].Values)
	return nil
}

// DeleteSession removes every vector of one session.
//
// Vectorize has no session primitive: it deletes by document id. This therefore
// enumerates the session, then deletes the enumerated ids. The enumeration is
// an increasing-topK query, so the sweep costs one round trip per 100 documents
// and its completeness is bounded by Vectorize approximate recall, not by a
// server-side filter delete like Chroma's. The delete itself is a sequence of
// batched vector.delete calls and is NOT atomic: a failure in the middle leaves
// the session partially deleted. Because a delete of an absent id is not an
// error, a replay of the whole DeleteSession converges, which is why a partial
// failure is returned as an error rather than hidden.
func (s *vectorizeStore) DeleteSession(ctx context.Context, sessionID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return nil
	}
	docs, err := s.ListDocuments(ctx, sessionID)
	if err != nil {
		// Nothing is deleted on an incomplete enumeration. A half-deleted
		// session is a worse state than an untouched one, because the surviving
		// vectors still answer recalls.
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

// DeleteDocuments removes specific vectors by id. A delete of an id that does
// not exist, and a delete of an empty id list, both succeed: the outbox replays
// a delete after a partial failure, and a delete that reported "not found"
// would poison that retry forever.
func (s *vectorizeStore) DeleteDocuments(ctx context.Context, ids []string) error {
	clean := make([]string, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		clean = append(clean, id)
	}
	if len(clean) == 0 {
		return nil
	}
	for start := 0; start < len(clean); start += vectorizeQueryPageSize {
		end := min(start+vectorizeQueryPageSize, len(clean))
		var result vectorizeDeleteResult
		if err := s.client.Do(ctx, cloudflarebridge.OpVectorDelete, vectorizeDeletePayload{IDs: clean[start:end]}, &result); err != nil {
			return fmt.Errorf("vectorize store: delete %d of %d documents: %w", end-start, len(clean), err)
		}
		// result.Deleted is informational. A short count means some ids were
		// already absent, which is the state the caller asked for.
	}
	return nil
}

// GetDocuments reads the requested document ids and populates Embedding, which
// is what startup_vector_recovery.go reuses to run its probe query and to
// reconcile the cached snapshot.
//
// This is the EXACT read ChromaDB performs, and it is exact in the sense that
// matters: vector.get maps to the binding's getByIds, which is a keyed lookup,
// not a similarity walk. The previous implementation pinned each id with a
// metadata equality filter over a filtered query, which is approximate — an ANN
// index may drop a matching candidate from the walk — and cost one bridge round
// trip per id. A false "absent" there is not cosmetic: the outbox verifies a
// mutation by reading the document back, so a false absent becomes a duplicate
// write, and startup recovery would re-upsert documents the index already holds.
//
// Ids that are not present are simply absent from the result, exactly as they
// are for Chroma, so a caller can retry the whole batch. Requested order is
// preserved among the ids that were found, and a request naming an id twice
// returns that document once.
func (s *vectorizeStore) GetDocuments(ctx context.Context, ids []string) ([]VectorDocument, error) {
	clean := make([]string, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" && !seen[id] {
			seen[id] = true
			clean = append(clean, id)
		}
	}
	if len(clean) == 0 {
		return []VectorDocument{}, nil
	}
	var result vectorizeGetResult
	if err := s.client.Do(ctx, cloudflarebridge.OpVectorGet, vectorizeGetPayload{IDs: clean}, &result); err != nil {
		return nil, err
	}
	byID := make(map[string]VectorDocument, len(result.Vectors))
	for _, stored := range result.Vectors {
		id := strings.TrimSpace(stored.ID)
		if id == "" {
			continue
		}
		byID[id] = s.documentFromStored(stored)
	}
	out := make([]VectorDocument, 0, len(clean))
	for _, id := range clean {
		if doc, ok := byID[id]; ok {
			out = append(out, doc)
		}
	}
	return out, nil
}

// ListDocuments is a bounded Vectorize diagnostic. Vectorize ANN queries do
// not provide cursor or full-scan semantics, so Cloudflare lifecycle callers
// use the D1-backed adapter; this raw provider refuses to mistake a full page
// for a complete canonical manifest.
func (s *vectorizeStore) ListDocuments(ctx context.Context, sessionID string) ([]VectorDocument, error) {
	probe, err := s.queryProbe(ctx)
	if err != nil {
		return nil, err
	}
	var filter map[string]any
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
		filter = map[string]any{"chat_session_id": sessionID}
	}
	matches, err := s.queryMatches(ctx, vectorizeQueryPayload{Vector: probe, TopK: vectorizeFullQueryMaxTopK, Filter: filter, IncludeValues: true})
	if err != nil {
		return nil, err
	}
	if len(matches) >= vectorizeFullQueryMaxTopK {
		return nil, fmt.Errorf("vectorize store: enumeration cap reached at the %d-result full-metadata Vectorize limit; use the canonical document manifest", vectorizeFullQueryMaxTopK)
	}
	out := make([]VectorDocument, 0, len(matches))
	for _, match := range matches {
		if strings.TrimSpace(match.ID) != "" {
			out = append(out, s.documentFromMatch(match))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Rebuild cannot be offered on this transport.
//
// ChromaDB validates a freshly built collection with a sample query and then
// swaps two names, so a failed rebuild leaves the original index serving.
// Vectorize has one index, mutated in place: there is no second index to build
// into, no name to swap, and therefore no atomic promotion to be had. The
// closest honest behaviour is to refuse and leave the index untouched; an
// operator who needs a rebuild must run the canonical backfill in place, which
// means recall is served from a partially rebuilt index for the duration.
func (s *vectorizeStore) Rebuild(ctx context.Context, sessionID string) error {
	return errors.New("vectorize store: Vectorize is a single in-place index with no collection swap; rebuild is orchestrated by the canonical backfill pipeline and cannot be validated-then-promoted here")
}

// Health reports what this transport can actually observe.
//
// Observable: whether the Worker holds a Vectorize binding, and that is what
// ModelReady reports, because the embedding model runs in this process rather
// than in the provider and the runtime preflight gates on binding presence.
// Not observable: the document count, the persist directory, the embedding
// model, the collection name (nothing on this side of the bridge may name the
// index), and the optional index version the Worker returns, for which
// HealthSnapshot has no field. TotalCount is therefore reported as 0 with a
// preflight issue rather than filled with a plausible number, and an unbound
// binding renders as an unhealthy report instead of a failed call.
func (s *vectorizeStore) Health(ctx context.Context) (HealthSnapshot, error) {
	var result vectorizeHealthResult
	if err := s.client.Do(ctx, cloudflarebridge.OpVectorHealth, nil, &result); err != nil {
		return HealthSnapshot{
			Status:          "error",
			Collection:      vectorizeCollectionLabel,
			ModelReady:      false,
			PreflightIssues: []string{err.Error()},
		}, err
	}
	if !result.Bound {
		return HealthSnapshot{
			Status:          "unhealthy",
			Collection:      vectorizeCollectionLabel,
			ModelReady:      false,
			PreflightIssues: []string{"the Worker reports no Vectorize binding; vector recall is unavailable"},
		}, nil
	}
	issues := []string{}
	if result.GeometryError {
		// An operator needs to know WHICH capability is missing, because the
		// consequence is specific: without the dimension, document enumeration on
		// a cold process fails until this process writes or recalls something.
		issues = append(issues, "the Worker could not describe the index; document enumeration on a cold process is unavailable")
	}
	if result.Dimensions <= 0 && !result.GeometryError {
		issues = append(issues, "the index embedding dimension is unreported; document enumeration on a cold process is unavailable")
	}
	if result.Dimensions > 0 {
		s.rememberDimension(result.Dimensions)
	}
	// result.Version is the bridge envelope version, not an index version. The
	// binding exposes no account-neutral index name or version, and HealthSnapshot
	// has no field for one. Inventing an index identity an operator could not
	// resolve would be worse than leaving it out.
	return HealthSnapshot{
		Status:          "ok",
		Collection:      vectorizeCollectionLabel,
		TotalCount:      result.VectorCount,
		ModelReady:      true,
		PreflightIssues: issues,
	}, nil
}

// Count returns the number of vectors for one session.
//
// A WHOLE-INDEX count is index-wide rather than per session, and the binding
// reports it, so an empty session id now answers from the index instead of
// refusing. That is still not a session count and the distinction matters: the
// index-wide figure counts reference-library documents and every session's
// memories, so it is only meaningful as a drift signal against canonical truth,
// never as "how many documents does this session have".
//
// A per-session count remains an enumeration: it is exact whenever the sweep
// completes (a short page proves the filtered set is smaller than the requested
// topK) and it fails rather than returning a partial number when the cap is
// reached.
func (s *vectorizeStore) Count(ctx context.Context, sessionID string) (int, error) {
	if strings.TrimSpace(sessionID) == "" {
		// One health call carries both the dimension and the count, so there is no
		// reason to ask twice.
		var result vectorizeHealthResult
		if err := s.client.Do(ctx, cloudflarebridge.OpVectorHealth, nil, &result); err != nil {
			return 0, err
		}
		if result.GeometryError || result.Dimensions <= 0 {
			return 0, errors.New("vectorize store: the Worker could not describe the index, so its document count is unavailable; MariaDB remains the canonical authority for totals")
		}
		s.rememberDimension(result.Dimensions)
		return result.VectorCount, nil
	}
	docs, err := s.ListDocuments(ctx, sessionID)
	if err != nil {
		return 0, err
	}
	return len(docs), nil
}

// Close releases nothing. The bridge client is shared with the D1 provider and
// owned by the runtime that constructed it.
func (s *vectorizeStore) Close(ctx context.Context) error { return nil }

// vectorizeCollectionLabel names the index in health output. It is a provider
// label, not an index name: the contract deliberately does not carry one, and
// an account-scoped identifier must not appear on this side of the bridge.
const vectorizeCollectionLabel = "vectorize"

func (s *vectorizeStore) queryMatches(ctx context.Context, payload vectorizeQueryPayload) ([]vectorizeQueryMatch, error) {
	var result vectorizeQueryResult
	if err := s.client.Do(ctx, cloudflarebridge.OpVectorQuery, payload, &result); err != nil {
		if isVectorizeDimensionError(err) {
			// The index was rebuilt with a different embedding model. Forget
			// the probe so the next observation re-learns the dimension.
			s.forgetProbe()
			return nil, fmt.Errorf("vectorize index dimension mismatch: the index holds one embedding dimension; re-embed every document with one model or recreate the index. Original error: %w", err)
		}
		return nil, fmt.Errorf("vectorize store: query failed: %w", err)
	}
	return result.Matches, nil
}

// AwaitVisible blocks until every requested id is readable.
//
// Vectorize acknowledges a mutation with a mutation id and applies it
// afterwards, so a read immediately after an upsert can legitimately return
// nothing. The outbox verifies an upsert by reading the document back
// immediately, and every failed readback costs it an attempt out of a bounded
// budget; at the limit the operation is parked permanently. Left unhandled, a
// correctly applied write would be reported as a retry limit reached and its
// document would never reach the index.
//
// Polling is the honest mechanism. The index's processedUpToMutation marker says
// which mutations have been applied but not when they will be, so it cannot
// replace a bounded wait.
//
// The budget belongs to the caller, and the common case costs exactly one read
// because the loop checks before it sleeps.
func (s *vectorizeStore) AwaitVisible(ctx context.Context, ids []string, budget time.Duration) error {
	clean := make([]string, 0, len(ids))
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" {
			clean = append(clean, id)
		}
	}
	if len(clean) == 0 {
		return nil
	}
	if budget <= 0 {
		budget = vectorizeVisibilityBudget
	}
	deadline := time.Now().Add(budget)
	for {
		found, err := s.GetDocuments(ctx, clean)
		if err != nil {
			return err
		}
		if len(found) >= len(clean) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"vectorize: %d of %d documents are not readable yet after %s; the mutation is asynchronous and may still be applied, so this is not a failed write",
				len(clean)-len(found), len(clean), budget)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(vectorizeVisibilityPoll):
		}
	}
}

const (
	// vectorizeVisibilityPoll is the gap between visibility checks. It is short
	// because the wait happens inside an outbox worker's lease, and a long poll
	// would hold the claim against other work.
	vectorizeVisibilityPoll = 25 * time.Millisecond
	// vectorizeVisibilityBudget bounds the in-line wait. It is short enough that
	// a genuinely stuck mutation falls through to the outbox's own retry schedule
	// rather than stalling a worker, and long enough to cover the common case.
	vectorizeVisibilityBudget = 2 * time.Second
)

// documentFromMatch maps one match to a VectorDocument. The reserved transport
// fields are removed from the exposed metadata, because they are this
// transport's representation of the document id and the document text rather
// than document metadata.
func (s *vectorizeStore) documentFromMatch(match vectorizeQueryMatch) VectorDocument {
	return s.documentFromStored(vectorizeStoredVector{ID: match.ID, Values: match.Values, Metadata: match.Metadata})
}

// documentFromStored maps one document read back by id to a VectorDocument.
//
// It shares documentFromMatch's mapping on purpose. A document read through
// vector.get and the same document read through a query must produce the same
// VectorDocument, because the outbox compares a readback against the write it
// just made, and a mapping that differed between the two paths would make every
// readback look like a change. The reserved transport fields are removed from the
// exposed metadata, because they are this transport's representation of the
// document id and the document text rather than document metadata.
func (s *vectorizeStore) documentFromStored(stored vectorizeStoredVector) VectorDocument {
	meta := chromaScalarMetadata(stored.Metadata)
	text := stringFromAny(meta[vectorizeDocumentTextKey])
	delete(meta, vectorizeDocumentTextKey)
	delete(meta, vectorizeDocumentIDKey)
	doc := vectorDocumentFromChroma(stored.ID, text, meta)
	doc.Embedding = vectorizeValues(stored.Values)
	return doc
}

// vectorizeMetadataForDocument builds the metadata of one upserted vector. It
// writes the same reserved identity fields as the ChromaDB upsert, adds the two
// Vectorize-only transport fields, and keeps the caller metadata untouched so a
// Vectorize index filters on exactly what a Chroma collection filters on.
//
// It deliberately does not trim metadata to fit a per-vector field budget.
// Silently dropping a field would break the exact-match metadata filters that
// reference recall runs on, which is a worse failure than a rejected upsert:
// the rejection is loud, names the reason, and is an operator decision.
func vectorizeMetadataForDocument(doc VectorDocument, sessionID string) map[string]any {
	meta := chromaScalarMetadata(doc.Metadata)
	meta["tier"] = doc.Tier
	meta["chat_session_id"] = firstNonEmpty(doc.ChatSessionID, sessionID)
	meta["source_table"] = doc.SourceTable
	meta["source_row_id"] = doc.SourceRowID
	meta["schema_version"] = doc.SchemaVersion
	meta["embedding_dim"] = len(doc.Embedding)
	if strings.TrimSpace(doc.SearchTextPolicy) != "" {
		meta["search_text_policy"] = strings.TrimSpace(doc.SearchTextPolicy)
	}
	if strings.TrimSpace(doc.RawLanguage) != "" {
		meta["raw_language"] = strings.TrimSpace(doc.RawLanguage)
	}
	if strings.TrimSpace(doc.SummaryLanguage) != "" {
		meta["summary_language"] = strings.TrimSpace(doc.SummaryLanguage)
	}
	if strings.TrimSpace(doc.SessionOutputLanguage) != "" {
		meta["session_output_language"] = strings.TrimSpace(doc.SessionOutputLanguage)
	}
	if doc.AliasCount > 0 {
		meta["alias_count"] = doc.AliasCount
	}
	if doc.MigrationID > 0 {
		meta["migration_id"] = strconv.FormatInt(doc.MigrationID, 10)
	}
	if strings.TrimSpace(doc.MigratedFromSessionID) != "" {
		meta["migrated_from_session_id"] = strings.TrimSpace(doc.MigratedFromSessionID)
	}
	meta[vectorizeDocumentIDKey] = strings.TrimSpace(doc.ID)
	meta[vectorizeDocumentTextKey] = doc.DocumentText
	return meta
}

// vectorizeDimensionConsistency rejects a batch that mixes embedding
// dimensions. Vectorize would reject it, and because that rejection is an
// execution failure it is retryable, so a mixed batch would be replayed
// forever instead of reported once.
func vectorizeDimensionConsistency(vectors []vectorizeUpsertVector) error {
	dimension := 0
	for _, vector := range vectors {
		if dimension == 0 {
			dimension = len(vector.Values)
			continue
		}
		if len(vector.Values) != dimension {
			return fmt.Errorf("vectorize index dimension mismatch: batch mixes embedding dimensions %d and %d; one index holds exactly one dimension, so re-embed the batch with a single model or recreate the index", dimension, len(vector.Values))
		}
	}
	return nil
}

// vectorizeSearchFilter builds the flat exact-match filter of a recall query.
// Vectorize filters are an AND over metadata equality and have no $and/$or
// language, so the session, tier and source table clauses flatten into one
// object. An empty filter is never sent.
func vectorizeSearchFilter(sessionID string, filter string) map[string]any {
	sessionID, tier, sourceTable := searchFilterValues(sessionID, filter)
	out := map[string]any{}
	if sessionID != "" {
		out["chat_session_id"] = sessionID
	}
	if tier != "" {
		out["tier"] = tier
	}
	if sourceTable != "" {
		out["source_table"] = sourceTable
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// vectorizeExactFilter translates a provider-neutral ExactQuery.Where into the
// equality AND that Vectorize accepts. Archive Center builds these filters as
// $and over equality clauses. Anything else is refused rather than dropped:
// silently ignoring a clause would return a wider candidate set than the caller
// approved, which is exactly the fabrication the exact query exists to prevent.
func vectorizeExactFilter(where map[string]any) (map[string]any, error) {
	if len(where) == 0 {
		return nil, nil
	}
	out := map[string]any{}
	if err := collectVectorizeEquality(out, where); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func collectVectorizeEquality(out map[string]any, where map[string]any) error {
	for key, value := range where {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		switch typed := value.(type) {
		case map[string]any:
			if key != "$and" {
				return fmt.Errorf("vectorize store: metadata filter operator %q cannot be translated; a Vectorize filter is an exact-match AND only", key)
			}
			if err := collectVectorizeEquality(out, typed); err != nil {
				return err
			}
		case []map[string]any:
			if key != "$and" {
				return fmt.Errorf("vectorize store: metadata filter operator %q cannot be translated; a Vectorize filter is an exact-match AND only", key)
			}
			for _, clause := range typed {
				if err := collectVectorizeEquality(out, clause); err != nil {
					return err
				}
			}
		case []any:
			if key != "$and" {
				return fmt.Errorf("vectorize store: metadata filter operator %q cannot be translated; a Vectorize filter is an exact-match AND only", key)
			}
			for _, clause := range typed {
				nested, ok := clause.(map[string]any)
				if !ok {
					return fmt.Errorf("vectorize store: metadata filter clause under %q is not an equality map", key)
				}
				if err := collectVectorizeEquality(out, nested); err != nil {
					return err
				}
			}
		case string, bool, float64, float32, int, int32, int64, json.Number:
			out[key] = typed
		default:
			return fmt.Errorf("vectorize store: metadata filter field %q has a value a Vectorize filter cannot express", key)
		}
	}
	return nil
}

// vectorizeValues narrows JSON-decoded float64 values to the float32 the index
// stores. It is lossless in the sense that matters: a float32 widened to
// float64 in JSON and narrowed back is bit-identical, so a document read back
// is the same float32 the process wrote.
func vectorizeValues(values []float64) []float32 {
	if len(values) == 0 {
		return nil
	}
	out := make([]float32, len(values))
	for i, value := range values {
		out[i] = float32(value)
	}
	return out
}

// isVectorizeDimensionError recognises the one provider failure this store can
// act on. The text is the Worker error text, so this is a heuristic in the
// same spirit as the ChromaDB dimension guard it mirrors. A false positive
// only costs a discarded probe and a clearer error message, which is why the
// test is the single word rather than a parsed sentence.
func isVectorizeDimensionError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "dimension")
}

func (s *vectorizeStore) rememberProbe(embedding []float32) {
	if len(embedding) == 0 {
		return
	}
	copied := append([]float32(nil), embedding...)
	s.probeMu.Lock()
	s.probe = copied
	s.probeMu.Unlock()
}

func (s *vectorizeStore) forgetProbe() {
	s.probeMu.Lock()
	s.probe = nil
	s.probeMu.Unlock()
}

func (s *vectorizeStore) probeVector() []float32 {
	s.probeMu.RLock()
	defer s.probeMu.RUnlock()
	if len(s.probe) == 0 {
		return nil
	}
	return append([]float32(nil), s.probe...)
}

// queryProbe returns the vector that carries the index dimension into an
// enumeration query.
//
// Vectorize rejects a query whose vector dimension differs from the index, so an
// enumeration has to send a correctly sized vector. WHICH vector is irrelevant:
// every enumeration query also sends a filter, and a filter that pins a single
// document (or a single session) fixes the answer without reference to the query
// vector.
//
// The dimension comes from the index itself. vector.health reports it from the
// binding's describe(), so a read-only enumeration works on a cold process with
// no prior write and no prior recall. That case is the COMMON one rather than an
// edge one: Cloudflare Containers scale to zero, so a freshly started Container
// has observed nothing, and DeleteSession in particular cannot prime a probe
// through itself.
//
// The observed embedding remains the fallback for a Worker that cannot describe
// its index, because it is still the correct dimension whenever this process has
// written anything. Only a cold process on a Worker without describe() fails, and
// it fails with a named error rather than a fabricated result.
func (s *vectorizeStore) queryProbe(ctx context.Context) ([]float32, error) {
	if probe := s.probeVector(); len(probe) > 0 {
		return probe, nil
	}
	if dimension, err := s.indexGeometry(ctx); err == nil && dimension > 0 {
		// A unit vector is the least arbitrary filler available: it is a real
		// direction, so the index answers a well-defined query instead of
		// dividing by a zero norm, and the filter is what decides the result.
		probe := make([]float32, dimension)
		probe[0] = 1
		return probe, nil
	}
	return nil, errors.New("vectorize store: the index embedding dimension is unknown; the Worker could not describe the index and this process has not written or recalled a document, so enumeration has no query vector to send")
}
