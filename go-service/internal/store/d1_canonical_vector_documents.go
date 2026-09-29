package store

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/risulongmemory/archive-center-go/internal/vector"
)

// CanonicalVectorDocumentStore is consumed only at the composition root. It
// exposes D1's durable manifest without leaking a D1-specific operation to
// vector callers.
type CanonicalVectorDocumentStore interface {
	vector.CanonicalDocumentProvider
}

var _ CanonicalVectorDocumentStore = (*d1Store)(nil)

func (s *d1Store) ListCanonicalVectorDocuments(ctx context.Context, sessionID string) ([]vector.VectorDocument, error) {
	sessionID = strings.TrimSpace(sessionID)
	query := `
		SELECT o.document_json
		FROM memory_vector_outbox o
		JOIN memory_source_revisions r
		  ON r.source_revision = o.source_revision
		 AND r.chat_session_id = o.chat_session_id
		WHERE o.operation = 'upsert'
		  AND (o.status = 'completed' OR (o.status = 'retryable' AND o.last_error = ?))
		  AND o.required_source_state = 'active' AND r.lifecycle_state = 'active'`
	args := []any{MemoryVectorVisibilityPendingMarker}
	if sessionID != "" {
		query += " AND o.chat_session_id = ?"
		args = append(args, sessionID)
	}
	query += ` AND o.id = (
		SELECT MAX(newer.id) FROM memory_vector_outbox newer
		WHERE newer.document_id = o.document_id AND newer.operation = 'upsert'
		  AND (newer.status = 'completed' OR (newer.status = 'retryable' AND newer.last_error = ?))
	) ORDER BY o.document_id`
	args = append(args, MemoryVectorVisibilityPendingMarker)
	return d1CanonicalVectorDocuments(ctx, s, query, args...)
}

func (s *d1Store) GetCanonicalVectorDocuments(ctx context.Context, ids []string) ([]vector.VectorDocument, error) {
	clean := make([]string, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" && !seen[id] {
			seen[id] = true
			clean = append(clean, id)
		}
	}
	if len(clean) == 0 {
		return []vector.VectorDocument{}, nil
	}
	placeholders := make([]string, len(clean))
	args := make([]any, len(clean))
	for i, id := range clean {
		placeholders[i] = "?"
		args[i] = id
	}
	return d1CanonicalVectorDocuments(ctx, s, `
		SELECT o.document_json
		FROM memory_vector_outbox o
		JOIN memory_source_revisions r
		  ON r.source_revision = o.source_revision
		 AND r.chat_session_id = o.chat_session_id
		WHERE o.document_id IN (`+strings.Join(placeholders, ",")+`)
		  AND o.operation = 'upsert'
		  AND (o.status = 'completed' OR (o.status = 'retryable' AND o.last_error = ?))
		  AND o.required_source_state = 'active' AND r.lifecycle_state = 'active'
		  AND o.id = (SELECT MAX(newer.id) FROM memory_vector_outbox newer
		              WHERE newer.document_id = o.document_id AND newer.operation = 'upsert'
		                AND (newer.status = 'completed' OR (newer.status = 'retryable' AND newer.last_error = ?)))`, append(args, MemoryVectorVisibilityPendingMarker, MemoryVectorVisibilityPendingMarker)...)

}

func d1CanonicalVectorDocuments(ctx context.Context, s *d1Store, query string, args ...any) ([]vector.VectorDocument, error) {
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	docs := []vector.VectorDocument{}
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var doc vector.VectorDocument
		if err := json.Unmarshal([]byte(payload), &doc); err != nil {
			return nil, err
		}
		doc.ID = strings.TrimSpace(doc.ID)
		if doc.ID != "" {
			docs = append(docs, doc)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return docs, nil
}
