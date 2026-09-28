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

	// vectorizeListMaxDocuments caps one enumeration. Vectorize has no
	// collection count and no scroll cursor in this contract, so an enumeration
	// is an increasing-topK query. Rather than report a partial count as a real
	// one, the sweep fails loudly when the cap is reached with a full page.
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
	Bound   bool   `json:"bound"`
	Version string `json:"version,omitempty"`
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
// This is not ChromaDB get-by-id. The frozen contract has no such operation, so
// each id is pinned with a metadata equality filter on the reserved document id
// field, which reduces the candidate set to that one document and makes the
// answer independent of the approximate index. The cost is one bridge round trip
// per requested id, issued with bounded concurrency. Ids that are not present
// are simply absent from the result, as they are for Chroma.
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
	probe, err := s.queryProbe()
	if err != nil {
		return nil, err
	}
	docs := make([]VectorDocument, len(clean))
	found := make([]bool, len(clean))
	failures := make([]error, len(clean))
	var wg sync.WaitGroup
	slots := make(chan struct{}, vectorizeReadConcurrency)
	for i := range clean {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			matches, err := s.queryMatches(ctx, vectorizeQueryPayload{
				Vector:        probe,
				TopK:          1,
				Filter:        map[string]any{vectorizeDocumentIDKey: clean[i]},
				IncludeValues: true,
			})
			if err != nil {
				failures[i] = err
				return
			}
			for _, match := range matches {
				if strings.TrimSpace(match.ID) == clean[i] {
					docs[i] = s.documentFromMatch(match)
					found[i] = true
					return
				}
			}
		}(i)
	}
	wg.Wait()
	for _, err := range failures {
		if err != nil {
			return nil, err
		}
	}
	out := make([]VectorDocument, 0, len(clean))
	for i := range clean {
		if found[i] {
			out = append(out, docs[i])
		}
	}
	return out, nil
}

// ListDocuments enumerates one session, or the whole index when sessionID is
// empty, with Embedding populated for every document.
//
// The sweep raises topK until the index answers with a short page, which is
// also how the enumeration knows it is complete: a short page means fewer
// documents match the filter than were requested. Results are deduplicated and
// ordered by id so two audits of the same session render identically, and no
// similarity is reported: the ranking that produced this order came from the
// dimension probe vector and carries no meaning.
func (s *vectorizeStore) ListDocuments(ctx context.Context, sessionID string) ([]VectorDocument, error) {
	probe, err := s.queryProbe()
	if err != nil {
		return nil, err
	}
	var filter map[string]any
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
		filter = map[string]any{"chat_session_id": sessionID}
	}
	out := []VectorDocument{}
	seen := map[string]bool{}
	for topK := vectorizeQueryPageSize; topK <= vectorizeListMaxDocuments; topK += vectorizeQueryPageSize {
		matches, err := s.queryMatches(ctx, vectorizeQueryPayload{
			Vector:        probe,
			TopK:          topK,
			Filter:        filter,
			IncludeValues: true,
		})
		if err != nil {
			return nil, err
		}
		for _, match := range matches {
			id := strings.TrimSpace(match.ID)
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, s.documentFromMatch(match))
		}
		if len(matches) < topK {
			sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
			return out, nil
		}
	}
	return nil, fmt.Errorf("vectorize store: enumeration stopped at the %d document cap with a full page still returned; this session may hold more documents than one bridge contract can list", vectorizeListMaxDocuments)
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
	issues := []string{"the total document count is not observable: this bridge contract has no collection count operation"}
	if s.probeVector() == nil {
		issues = append(issues, "the index embedding dimension has not been observed in this process yet; document enumeration is unavailable until the first upsert or recall")
	}
	// result.Version is the index version. It is decoded but not surfaced:
	// HealthSnapshot has no field for it and nothing in the runtime consumes
	// one, so an operator cannot see an index version change from /ready.
	// Adding that field is a shared-contract change, not a provider-local one.
	return HealthSnapshot{
		Status:          "ok",
		Collection:      vectorizeCollectionLabel,
		ModelReady:      true,
		PreflightIssues: issues,
	}, nil
}

// Count returns the number of vectors for one session.
//
// Vectorize has no cheap collection count, so a session count is an
// enumeration: it is exact whenever the sweep completes (a short page proves
// the filtered set is smaller than the requested topK) and it fails rather than
// returning a partial number when the cap is reached. A whole-index count is
// not offered at all, because an unbounded sweep of every session is not a
// count; MariaDB remains the canonical authority for that.
func (s *vectorizeStore) Count(ctx context.Context, sessionID string) (int, error) {
	if strings.TrimSpace(sessionID) == "" {
		return 0, errors.New("vectorize store: this bridge contract has no collection count; Count needs a session id and enumerates that session")
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

// documentFromMatch maps one match to a VectorDocument. The reserved transport
// fields are removed from the exposed metadata, because they are this
// transport's representation of the document id and the document text rather
// than document metadata.
func (s *vectorizeStore) documentFromMatch(match vectorizeQueryMatch) VectorDocument {
	meta := chromaScalarMetadata(match.Metadata)
	text := stringFromAny(meta[vectorizeDocumentTextKey])
	delete(meta, vectorizeDocumentTextKey)
	delete(meta, vectorizeDocumentIDKey)
	doc := vectorDocumentFromChroma(match.ID, text, meta)
	doc.Embedding = vectorizeValues(match.Values)
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
	out := map[string]any{}
	if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
		out["chat_session_id"] = sessionID
	}
	if tier := tierFromFilter(filter); tier != "" {
		out["tier"] = tier
	}
	if sourceTable := metadataStringEqualityFromFilter(filter, "source_table"); sourceTable != "" {
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
// Vectorize rejects a query whose vector dimension differs from the index, and
// this contract has no operation that reports the dimension, so an enumeration
// has to send a real vector. Which vector is irrelevant: every enumeration
// query also sends a filter, and a filter that pins a single document (or a
// single session) fixes the answer without reference to the query vector. The
// provider therefore reuses the most recent embedding it has seen, which is
// always the correct dimension because it is the dimension this process writes.
//
// The cost is a real limitation: a process that performs a read-only
// enumeration before any upsert or recall in that process has no dimension to
// send. It gets a named error rather than a fabricated result, and the next
// upsert or recall primes the probe.
func (s *vectorizeStore) queryProbe() ([]float32, error) {
	probe := s.probeVector()
	if len(probe) == 0 {
		return nil, errors.New("vectorize store: the index embedding dimension has not been observed in this process yet; enumeration needs a query vector, so upsert or recall one document first")
	}
	return probe, nil
}
