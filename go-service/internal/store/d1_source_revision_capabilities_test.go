package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestD1SourceRevisionLifecycle(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	if _, err := conn.Exec(ctx, `INSERT INTO chat_logs (chat_session_id,turn_index,role,content) VALUES ('source-s',7,'user','hello'),('source-s',7,'assistant','world')`); err != nil {
		t.Fatal(err)
	}
	source := &MemorySourceRevision{SourceRevision: "rev-7", ChatSessionID: "source-s", LogicalTurnID: "logical-7", TurnIndex: 7, UserContent: "hello", AssistantContent: "world", CombinedContentHash: "combined", HashAlgorithm: "sha256", HostObservedAtMS: 7}
	registered, err := st.RegisterAcceptedSourceRevision(ctx, source)
	if err != nil || !registered.Inserted {
		t.Fatalf("register = %#v, %v", registered, err)
	}
	registered, err = st.RegisterAcceptedSourceRevision(ctx, source)
	if err != nil || !registered.Idempotent {
		t.Fatalf("replay = %#v, %v", registered, err)
	}
	hash := sha256.Sum256([]byte(`{"turn":7}`))
	if err := st.SaveCriticInputSnapshot(ctx, "source-s", "rev-7", `{"turn":7}`, hex.EncodeToString(hash[:]), time.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetSourceRevision(ctx, "source-s", "rev-7")
	if err != nil || got.CriticInputSnapshotJSON != `{"turn":7}` {
		t.Fatalf("get = %#v, %v", got, err)
	}
	active, err := st.ListActiveSourceRevisions(ctx, "source-s", 7, 7)
	if err != nil || len(active) != 1 {
		t.Fatalf("active = %#v, %v", active, err)
	}
	if err := st.InvalidateSourceRevisions(ctx, "source-s", 7, "invalidated", "rollback", time.Now()); err != nil {
		t.Fatal(err)
	}
	isActive, err := st.IsSourceRevisionActive(ctx, "source-s", "rev-7")
	if err != nil || isActive {
		t.Fatalf("active after invalidation = %v, %v", isActive, err)
	}
	history, err := st.ListSourceRevisions(ctx, "source-s", 7, 7)
	if err != nil || len(history) != 1 || history[0].LifecycleState != "invalidated" {
		t.Fatalf("history = %#v, %v", history, err)
	}
}
