package store

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/vector"
)

func d1OverlayStringPtr(value string) *string { return &value }

func d1OverlayDocumentJSON(t *testing.T, id, sessionID string) string {
	t.Helper()
	payload, err := json.Marshal(vector.VectorDocument{
		ID:            id,
		ChatSessionID: sessionID,
		Tier:          "memory",
		SourceTable:   "precise_memory_units",
		DocumentText:  "durable overlay fixture",
		Embedding:     []float32{1, 0},
	})
	if err != nil {
		t.Fatalf("marshal overlay document: %v", err)
	}
	return string(payload)
}

func TestD1DurableSearchOverlayUsesLatestOperationAndCanonicalLifecycle(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	marker := MemoryVectorVisibilityPendingMarker

	d1SeedAdmissionSource(t, conn, "s1", "s1-live", 1, "active")
	d1SeedAdmissionSource(t, conn, "s1", "s1-dead", 2, "invalidated")
	d1SeedAdmissionSource(t, conn, "s2", "s2-live", 1, "active")

	pendingID := "memory:s1:pending"
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s1", sourceRevision: "s1-live", documentID: pendingID,
		documentJSON: d1OverlayStringPtr(d1OverlayDocumentJSON(t, pendingID, "s1")),
		status:       "retryable", lastError: &marker, createdAt: created, updatedAt: created,
	})
	// The completed manifest belongs to the accelerator already and must not add a
	// duplicate candidate to the durable delta.
	completedID := "memory:s1:completed"
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s1", sourceRevision: "s1-live", documentID: completedID,
		documentJSON: d1OverlayStringPtr(d1OverlayDocumentJSON(t, completedID, "s1")),
		status:       "completed", createdAt: created, updatedAt: created,
	})
	// A later effective delete wins over the older visibility-pending upsert.
	deletedID := "memory:s1:deleted"
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s1", sourceRevision: "s1-live", documentID: deletedID,
		documentJSON: d1OverlayStringPtr(d1OverlayDocumentJSON(t, deletedID, "s1")),
		status:       "retryable", lastError: &marker, createdAt: created, updatedAt: created,
	})
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		operation: "delete", sessionID: "s1", sourceRevision: "s1-live", documentID: deletedID,
		requiredSourceState: "active", status: "pending", createdAt: created.Add(time.Second), updatedAt: created.Add(time.Second),
	})
	// An inactive source is a tombstone even when its last vector operation was an
	// upsert, so an accelerator replica cannot resurrect rolled-back memory.
	inactiveID := "memory:s1:inactive"
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s1", sourceRevision: "s1-dead", documentID: inactiveID,
		documentJSON: d1OverlayStringPtr(d1OverlayDocumentJSON(t, inactiveID, "s1")),
		status:       "completed", createdAt: created, updatedAt: created,
	})
	// A delete invalidated by source fencing must not mask an active current doc.
	staleDeleteID := "memory:s1:stale-delete"
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		operation: "delete", sessionID: "s1", sourceRevision: "s1-live", documentID: staleDeleteID,
		requiredSourceState: "inactive", status: "stale_rejected", createdAt: created, updatedAt: created,
	})
	otherSessionID := "memory:s2:pending"
	d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
		sessionID: "s2", sourceRevision: "s2-live", documentID: otherSessionID,
		documentJSON: d1OverlayStringPtr(d1OverlayDocumentJSON(t, otherSessionID, "s2")),
		status:       "retryable", lastError: &marker, createdAt: created, updatedAt: created,
	})

	snapshot, err := st.DurableSearchOverlay(ctx, "s1", 20)
	if err != nil {
		t.Fatalf("DurableSearchOverlay: %v", err)
	}
	if snapshot.Truncated || snapshot.PendingCount != 1 {
		t.Fatalf("snapshot bounds = %#v, want one complete pending candidate", snapshot)
	}
	if !snapshot.OldestPendingAt.Equal(created) {
		t.Fatalf("oldest pending = %s, want %s", snapshot.OldestPendingAt, created)
	}
	if len(snapshot.Upserts) != 1 || snapshot.Upserts[0].ID != pendingID {
		t.Fatalf("upserts = %#v, want %q only", snapshot.Upserts, pendingID)
	}
	wantTombstones := []string{deletedID, inactiveID}
	if !reflect.DeepEqual(snapshot.TombstoneIDs, wantTombstones) {
		t.Fatalf("tombstones = %#v, want %#v", snapshot.TombstoneIDs, wantTombstones)
	}
}

func TestD1DurableSearchOverlayRejectsPartialSnapshot(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	marker := MemoryVectorVisibilityPendingMarker
	d1SeedAdmissionSource(t, conn, "s1", "rev-one", 1, "active")
	d1SeedAdmissionSource(t, conn, "s1", "rev-two", 2, "active")
	for _, seed := range []struct {
		revision string
		id       string
	}{
		{revision: "rev-one", id: "memory:s1:one"},
		{revision: "rev-two", id: "memory:s1:two"},
	} {
		d1OutboxSeedItem(t, conn, d1OutboxItemSeed{
			sessionID: "s1", sourceRevision: seed.revision, documentID: seed.id,
			documentJSON: d1OverlayStringPtr(d1OverlayDocumentJSON(t, seed.id, "s1")),
			status:       "retryable", lastError: &marker,
		})
	}

	snapshot, err := st.DurableSearchOverlay(ctx, "s1", 1)
	if err != nil {
		t.Fatalf("DurableSearchOverlay: %v", err)
	}
	if !snapshot.Truncated || snapshot.PendingCount != 2 {
		t.Fatalf("snapshot = %#v, want truncated snapshot with two pending upserts", snapshot)
	}
	if len(snapshot.Upserts) != 0 || len(snapshot.TombstoneIDs) != 0 {
		t.Fatalf("truncated snapshot leaked a partial correction set: %#v", snapshot)
	}
	if snapshot.OldestPendingAt.IsZero() {
		t.Fatalf("truncated snapshot omitted the oldest pending timestamp: %#v", snapshot)
	}
}

func TestD1DurableSearchOverlayRejectsInvalidBound(t *testing.T) {
	st, _ := newD1TestStore(t)
	if _, err := st.DurableSearchOverlay(context.Background(), "s1", 0); err == nil {
		t.Fatal("DurableSearchOverlay accepted a zero bound")
	}
}
