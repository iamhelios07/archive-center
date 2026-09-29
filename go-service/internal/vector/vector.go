// Package vector defines the vector search contract for Archive Center 2.0.
package vector

import (
	"context"
	"errors"
	"time"
)

// Common errors.
var (
	ErrNotFound                = errors.New("no vector results found")
	ErrNotEnabled              = errors.New("vector store is not enabled")
	ErrVisibilityPending       = errors.New("vector mutation accepted but is not visible before the context deadline")
	ErrDurableSearchOverloaded = errors.New("durable search overlay exceeds its bounded correction capacity")
)

// Durable Search overlay limits make the bounded correction contract observable
// without widening VectorStore's Chroma-compatible method signatures.
const (
	DurableSearchOverlayMaxDocuments = 200
	DurableSearchOverlaySoftTTL      = 5 * time.Minute
	DurableSearchOverlayHardAlertTTL = 15 * time.Minute
)

// VectorStore defines the core vector search and management contract.
// This mirrors the Chroma shadow operations analyzed in 0.8.
type VectorStore interface {
	// Search returns the top-k most similar vectors for a session.
	// Filter is a metadata expression (e.g. `tier == "memory"`).
	Search(ctx context.Context, sessionID string, vector []float32, limit int, filter string) ([]VectorDocument, error)

	// Upsert inserts or updates documents into the vector store.
	Upsert(ctx context.Context, sessionID string, docs []VectorDocument) error

	// DeleteSession removes all vectors for a session.
	DeleteSession(ctx context.Context, sessionID string) error

	// Rebuild creates a new collection from MariaDB canonical truth,
	// validates it with a sample query, then atomically swaps.
	Rebuild(ctx context.Context, sessionID string) error

	// Health returns a diagnostic snapshot of the vector store.
	Health(ctx context.Context) (HealthSnapshot, error)

	// Count returns the number of vectors for a session.
	Count(ctx context.Context, sessionID string) (int, error)

	// Close releases underlying connections and resources.
	Close(ctx context.Context) error
}

// DocumentDeleter is an optional extension for removing specific vector docs.
// It is used by turn rollback when canonical row IDs are known before deletion.
type DocumentDeleter interface {
	DeleteDocuments(ctx context.Context, ids []string) error
}

// ExactDocumentReader reads only the requested document IDs. Mutation callers
// use it to verify provider-applied writes without listing a full collection.
type ExactDocumentReader interface {
	GetDocuments(ctx context.Context, ids []string) ([]VectorDocument, error)
}

// AcceleratorExactDocumentReader bypasses a canonical fallback when a worker
// must prove the retrieval accelerator itself has become visible.
type AcceleratorExactDocumentReader interface {
	GetAcceleratorDocuments(ctx context.Context, ids []string) ([]VectorDocument, error)
}

// VectorVisibilityWaiter is an optional extension for stores whose writes
// become visible to a read some time after the write is acknowledged.
//
// It exists because the outbox verifies an upsert by reading the document back
// IMMEDIATELY, and a synchronously applied write is what that verification
// assumes. On an eventually consistent index the readback legitimately returns
// nothing for a short window, and the verification then reports a count of zero.
//
// That is not a cosmetic difference. The outbox counts every failed readback as
// an attempt, and once the attempt limit is reached the operation is parked as a
// permanent failure. So a provider that applies writes asynchronously would
// lose documents from its index permanently and report a retry limit reached
// for a write that actually succeeded.
//
// A store that does not implement this is synchronous, and the caller reads
// back immediately exactly as before. The knowledge stays in the provider that
// owns the consistency model, and callers keep working in terms of documents
// rather than in terms of a specific store's timing.
type VectorVisibilityWaiter interface {
	// AwaitVisible blocks until every requested id is readable, the budget is
	// exhausted, or ctx is done. It returns nil only when all ids were seen.
	//
	// A budget expiry is not an error the caller should treat as a failed write:
	// the write may still land. It is reported so the caller can retry on its own
	// schedule rather than assuming the mutation never happened.
	AwaitVisible(ctx context.Context, ids []string, budget time.Duration) error
}

// VisibilityWaiterCapability lets a wrapper forward VectorVisibilityWaiter
// without falsely advertising asynchronous visibility to callers when its
// wrapped store is synchronous. It is needed because Go optional interfaces are
// satisfied by a wrapper's method set, not by its delegate at runtime.
type VisibilityWaiterCapability interface {
	VisibilityWaiterEnabled() bool
}

// HasVisibilityWaiter reports whether a store has an active asynchronous
// visibility waiter. A direct waiter predating VisibilityWaiterCapability is
// treated as active for compatibility.
func HasVisibilityWaiter(store VectorStore) bool {
	if _, ok := store.(VectorVisibilityWaiter); !ok {
		return false
	}
	if capability, ok := store.(VisibilityWaiterCapability); ok {
		return capability.VisibilityWaiterEnabled()
	}
	return true
}

// DocumentLister is an optional diagnostic extension for full vector integrity
// audits. It returns stored vector metadata without changing runtime recall.
type DocumentLister interface {
	ListDocuments(ctx context.Context, sessionID string) ([]VectorDocument, error)
}

// ExactMetadataQuerier exposes ChromaDB's response order and raw measurements
// without applying Archive Center's session-memory reranking policy. It is used
// by reference-library diagnostics where a fabricated or normalized score would
// hide whether the vector database is actually query-sensitive.
type ExactMetadataQuerier interface {
	QueryExact(ctx context.Context, query ExactQuery) ([]ExactQueryResult, error)
}

type ExactQuery struct {
	Embedding []float32
	Limit     int
	Where     map[string]any
}

type ExactQueryResult struct {
	Document          VectorDocument
	ChromaRank        int
	Distance          float64
	DistanceAvailable bool
	CosineSimilarity  float64
	CosineAvailable   bool
}

// CollectionResetter is an explicit operator/debug-only extension for clearing
// all vector documents while preserving service configuration.
type CollectionResetter interface {
	ResetAll(ctx context.Context) error
}

// VectorDocument maps to a single upserted row in the vector store.
type VectorDocument struct {
	ID                    string
	Embedding             []float32
	Distance              float64
	Similarity            float64
	SimilarityAvailable   bool
	SimilaritySource      string
	Tier                  string
	ChatSessionID         string
	SourceTable           string
	SourceRowID           string
	SchemaVersion         string
	DocumentText          string
	SearchTextPolicy      string
	RawLanguage           string
	SummaryLanguage       string
	SessionOutputLanguage string
	AliasCount            int
	MigrationID           int64
	MigratedFromSessionID string
	Metadata              map[string]any
}

// HealthSnapshot is the diagnostic shape returned by Health.
type HealthSnapshot struct {
	Status          string
	Collection      string
	PersistDir      string
	TotalCount      int
	ProjectModel    string
	ModelReady      bool
	PreflightIssues []string
}
