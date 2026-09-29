package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/risulongmemory/archive-center-go/internal/vector"
)

// CanonicalVectorDocumentStore is consumed only at the composition root. It
// exposes D1's durable manifest without leaking a D1-specific operation to
// vector callers.
type CanonicalVectorDocumentStore interface {
	vector.CanonicalDocumentProvider
	vector.DurableSearchOverlayProvider
}

var _ CanonicalVectorDocumentStore = (*d1Store)(nil)

// DurableSearchOverlay returns only the D1 state that an eventually-consistent
// accelerator may not yet reflect. The ranked CTE makes one latest-operation
// decision per session/document: an acknowledged-but-invisible upsert becomes a
// search candidate, while a currently effective delete or inactive source
// becomes a tombstone. A newer delete therefore cannot be bypassed by an older
// completed upsert.
//
// The bounded result is deliberately all-or-nothing. A caller must reject a
// truncated snapshot rather than silently merge a partial set of corrections
// into accelerator results.
func (s *d1Store) DurableSearchOverlay(ctx context.Context, sessionID string, maxDocuments int) (vector.DurableSearchOverlaySnapshot, error) {
	if maxDocuments <= 0 {
		return vector.DurableSearchOverlaySnapshot{}, fmt.Errorf("durable search overlay maximum must be positive")
	}
	sessionID = strings.TrimSpace(sessionID)
	query := `
		WITH ranked AS (
			SELECT o.document_id, o.document_json, o.operation,
			       o.required_source_state, o.status, o.last_error, o.updated_at,
			       r.lifecycle_state,
			       ROW_NUMBER() OVER (
			         PARTITION BY o.chat_session_id, o.document_id
			         ORDER BY o.id DESC
			       ) AS revision_rank
			FROM memory_vector_outbox o
			JOIN memory_source_revisions r
			  ON r.source_revision = o.source_revision
			 AND r.chat_session_id = o.chat_session_id
		`
	args := make([]any, 0, 4)
	if sessionID != "" {
		query += " WHERE o.chat_session_id = ?"
		args = append(args, sessionID)
	}
	query += `
		), effective AS (
			SELECT document_id, document_json, updated_at,
			       CASE
			         WHEN operation = 'upsert'
			          AND status = 'retryable'
			          AND last_error = ?
			          AND required_source_state = 'active'
			          AND lifecycle_state = 'active'
			           THEN 'upsert'
			         WHEN lifecycle_state <> 'active'
			           THEN 'tombstone'
			         WHEN operation = 'delete'
			          AND status <> 'stale_rejected'
			          AND ((required_source_state = 'active' AND lifecycle_state = 'active')
			            OR (required_source_state = 'inactive' AND lifecycle_state <> 'active'))
			           THEN 'tombstone'
			         ELSE ''
			       END AS overlay_kind
			FROM ranked
			WHERE revision_rank = 1
		), selected AS (
			SELECT overlay_kind, document_id, document_json, updated_at,
			       SUM(CASE WHEN overlay_kind = 'upsert' THEN 1 ELSE 0 END) OVER () AS pending_count,
			       MIN(CASE WHEN overlay_kind = 'upsert' THEN updated_at END) OVER () AS oldest_pending_at,
			       COUNT(*) OVER () AS overlay_count
			FROM effective
			WHERE overlay_kind <> ''
		)
		SELECT overlay_kind, document_id, COALESCE(document_json, ''), updated_at,
		       pending_count, COALESCE(oldest_pending_at, ''), overlay_count
		FROM selected
		ORDER BY document_id
		LIMIT ?`
	args = append(args, MemoryVectorVisibilityPendingMarker, maxDocuments+1)

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return vector.DurableSearchOverlaySnapshot{}, err
	}
	defer rows.Close()

	snapshot := vector.DurableSearchOverlaySnapshot{
		Upserts:      []vector.VectorDocument{},
		TombstoneIDs: []string{},
	}
	first := true
	for rows.Next() {
		var kind, documentID, payload, updatedAtText, oldestPendingAtText string
		var pendingCount, overlayCount int
		if err := rows.Scan(&kind, &documentID, &payload, &updatedAtText, &pendingCount, &oldestPendingAtText, &overlayCount); err != nil {
			return vector.DurableSearchOverlaySnapshot{}, err
		}
		if first {
			first = false
			snapshot.PendingCount = pendingCount
			if oldestPendingAtText != "" {
				oldestPendingAt, err := parseD1Time(oldestPendingAtText)
				if err != nil {
					return vector.DurableSearchOverlaySnapshot{}, fmt.Errorf("durable search overlay oldest pending timestamp: %w", err)
				}
				snapshot.OldestPendingAt = oldestPendingAt
			}
			snapshot.Truncated = overlayCount > maxDocuments
			if snapshot.Truncated {
				return snapshot, nil
			}
		}
		documentID = strings.TrimSpace(documentID)
		if documentID == "" {
			continue
		}
		switch kind {
		case "tombstone":
			snapshot.TombstoneIDs = append(snapshot.TombstoneIDs, documentID)
		case "upsert":
			var document vector.VectorDocument
			if err := json.Unmarshal([]byte(payload), &document); err != nil {
				return vector.DurableSearchOverlaySnapshot{}, fmt.Errorf("durable search overlay document %q: %w", documentID, err)
			}
			document.ID = strings.TrimSpace(document.ID)
			if document.ID == "" {
				return vector.DurableSearchOverlaySnapshot{}, fmt.Errorf("durable search overlay document %q has no id", documentID)
			}
			if document.ID != documentID {
				return vector.DurableSearchOverlaySnapshot{}, fmt.Errorf("durable search overlay document id %q does not match outbox id %q", document.ID, documentID)
			}
			updatedAt, err := parseD1Time(updatedAtText)
			if err != nil {
				return vector.DurableSearchOverlaySnapshot{}, fmt.Errorf("durable search overlay document %q updated_at: %w", documentID, err)
			}
			if snapshot.OldestPendingAt.IsZero() || updatedAt.Before(snapshot.OldestPendingAt) {
				snapshot.OldestPendingAt = updatedAt
			}
			snapshot.Upserts = append(snapshot.Upserts, document)
		default:
			return vector.DurableSearchOverlaySnapshot{}, fmt.Errorf("durable search overlay has unsupported kind %q", kind)
		}
	}
	if err := rows.Err(); err != nil {
		return vector.DurableSearchOverlaySnapshot{}, err
	}
	return snapshot, nil
}

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
