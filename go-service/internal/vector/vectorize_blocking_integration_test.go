package vector

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/cloudflarebridge"
)

// TestCloudflareVectorizeRemoteBlockingFacade verifies the caller-visible
// runBlocking boundary against the real remote binding. It is opt-in like the
// broader remote integration test and never runs without its bridge variables.
func TestCloudflareVectorizeRemoteBlockingFacade(t *testing.T) {
	baseURL := strings.TrimSpace(os.Getenv("AC_CLOUDFLARE_INTEGRATION_BRIDGE_URL"))
	token := os.Getenv("AC_CLOUDFLARE_INTEGRATION_BRIDGE_TOKEN")
	if baseURL == "" || token == "" {
		t.Skip("AC_CLOUDFLARE_INTEGRATION_BRIDGE_URL and AC_CLOUDFLARE_INTEGRATION_BRIDGE_TOKEN are not set")
	}
	client, err := cloudflarebridge.NewClient(baseURL, token, 60*time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	raw, err := NewVectorizeStore(client)
	if err != nil {
		t.Fatalf("NewVectorizeStore: %v", err)
	}
	store := NewVisibilityBlockingVectorStore(raw)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	sessionID := "ci-vectorize-blocking-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	id := sessionID + ":1"
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := raw.DeleteSession(cleanupCtx, sessionID); err != nil {
			t.Logf("cleanup: DeleteSession: %v", err)
		}
	}()

	doc := VectorDocument{
		ID: id, Embedding: remoteBlockingTestVector(3, remoteBlockingIntegrationDimensions(t)), ChatSessionID: sessionID,
		Tier: "memory", SourceTable: "memories", SourceRowID: "1", DocumentText: "remote blocking facade probe",
	}
	if err := store.Upsert(ctx, sessionID, []VectorDocument{doc}); err != nil {
		t.Fatalf("blocking Upsert: %v", err)
	}
	readback, err := store.(ExactDocumentReader).GetDocuments(ctx, []string{id})
	if err != nil {
		t.Fatalf("GetDocuments after blocking Upsert: %v", err)
	}
	if len(readback) != 1 || readback[0].DocumentText != doc.DocumentText {
		t.Fatalf("GetDocuments after successful blocking Upsert = %#v, want %q", readback, doc.DocumentText)
	}
}

func remoteBlockingTestVector(seed, dimensions int) []float32 {
	if dimensions <= 0 {
		dimensions = 1
	}
	out := make([]float32, dimensions)
	for i := range out {
		out[i] = float32((i%17)+1+seed) / 32
	}
	return out
}

func remoteBlockingIntegrationDimensions(t *testing.T) int {
	t.Helper()
	dimensions := 1024
	if raw := strings.TrimSpace(os.Getenv("AC_CLOUDFLARE_INTEGRATION_DIMENSIONS")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("AC_CLOUDFLARE_INTEGRATION_DIMENSIONS = %q is not a positive integer", raw)
		}
		dimensions = parsed
	}
	return dimensions
}
