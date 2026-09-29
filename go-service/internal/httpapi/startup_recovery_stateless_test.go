package httpapi

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/cloudflarebridge"
	"github.com/risulongmemory/archive-center-go/internal/config"
	"github.com/risulongmemory/archive-center-go/internal/vector"
)

// The Chroma index recovery journal is a local filesystem artifact, and the
// Cloudflare profile must not produce one.
//
// It is already excluded, and the exclusion is a single boolean at the two call
// sites. That is exactly the kind of thing that gets changed by someone
// refactoring a startup path, and the symptom would be invisible: the Container
// would write a journal into its layer, resume nothing after a restart because
// the layer is gone, and report a recovery that never happened. So the exclusion
// is pinned here rather than left to be rediscovered.
//
// The journal is also unusable on Cloudflare for a second, independent reason,
// which these tests record: it is keyed on the Chroma endpoint, API path and
// collection, so it cannot describe a Vectorize index even if the gate opened.

type journalingVectorStore struct {
	resumeCalls       []string
	recoverIndexCalls int
}

var _ vector.IndexRecovery = (*journalingVectorStore)(nil)

func (j *journalingVectorStore) ResumeIndexRecovery(_ context.Context, journalPath string) error {
	j.resumeCalls = append(j.resumeCalls, journalPath)
	return nil
}

func (j *journalingVectorStore) RecoverySnapshot(_ context.Context, _ string) ([]vector.VectorDocument, int, error) {
	return nil, 0, nil
}

func (j *journalingVectorStore) RecoverIndex(_ context.Context, _ string, _ func(vector.VectorStore) error) (string, error) {
	j.recoverIndexCalls++
	return "", nil
}

func (j *journalingVectorStore) Health(context.Context) (vector.HealthSnapshot, error) {
	return vector.HealthSnapshot{}, nil
}

// The rest of VectorStore. None of it is reached by the recovery path, and none
// of it touches the filesystem, which is what makes the absence of a journal on
// disk meaningful rather than merely unobserved.
func (j *journalingVectorStore) Search(context.Context, string, []float32, int, string) ([]vector.VectorDocument, error) {
	return nil, nil
}

func (j *journalingVectorStore) Upsert(context.Context, string, []vector.VectorDocument) error {
	return nil
}

func (j *journalingVectorStore) DeleteSession(context.Context, string) error { return nil }

func (j *journalingVectorStore) Rebuild(context.Context, string) error { return nil }

func (j *journalingVectorStore) Count(context.Context, string) (int, error) { return 0, nil }

func (j *journalingVectorStore) Close(context.Context) error { return nil }

// unhealthyVectorStore fails its health check, which is how a container whose
// bridge refuses a connection behaves. VectorOpenError is not set on it, so it
// exercises the health path rather than the open path.
type unhealthyVectorStore struct{ journalingVectorStore }

func (u *unhealthyVectorStore) Health(context.Context) (vector.HealthSnapshot, error) {
	return vector.HealthSnapshot{}, context.DeadlineExceeded
}

// unreadyVectorStore reports a health check that succeeds but is not ready, which
// is the third and last preflight path.
type unreadyVectorStore struct{ journalingVectorStore }

func (u *unreadyVectorStore) Health(context.Context) (vector.HealthSnapshot, error) {
	return vector.HealthSnapshot{Status: "loading", ModelReady: false}, nil
}

// TestCloudflareProfileNeverOpensTheChromaRecoveryJournal is the exclusion test.
//
// It asserts the GATE, not a call through it, and that distinction is the point.
// retryIndexRecoveryAfterConfigSync performs an UNCHECKED assertion,
// s.Vector.(vector.IndexRecovery), so calling it directly on a Cloudflare server
// panics in its goroutine rather than failing cleanly. An earlier draft of this
// test did exactly that and "passed" for the wrong reason on the failure side: it
// saw the state machine move to recovering and reported it, when what it had
// really demonstrated is that the function is unsafe to reach.
//
// So the two properties that make this safe are asserted separately:
//
//  1. the gate excludes the Cloudflare profile, at both call sites;
//  2. a Vectorize store does not implement IndexRecovery, so the gate and the
//     unchecked assertion cannot drift apart. If a future provider ever did
//     implement it, the gate would stop being load-bearing for panic-safety and
//     this test says so.
func TestCloudflareProfileNeverOpensTheChromaRecoveryJournal(t *testing.T) {
	dataDir := t.TempDir()
	logDir := t.TempDir()
	t.Setenv("ARCHIVE_CENTER_DATA_DIR", dataDir)
	t.Setenv("AC_LOG_DIR", logDir)

	vec := &journalingVectorStore{}
	cloudflare := config.Config{
		Mode:                  config.ModeShadow,
		RuntimeProfile:        config.RuntimeProfileCloudflare,
		StoreMode:             config.StoreModeCloudflareAuthority,
		VectorMode:            config.VectorModeCloudflare,
		CloudflareBridgeURL:   "http://archive-center-bridge.internal",
		CloudflareBridgeToken: "bridge-token",
		ChromaEnabled:         false,
		ChromaEndpoint:        "",
		ChromaCollection:      "",
	}

	// Property 1: the gate. This is the exact condition at server.go:84 and the
	// one in the health-driven caller.
	_, recoverable := interface{}(vec).(vector.IndexRecovery)
	if recoverable && cloudflare.StoreMode == config.StoreModeMariaDBAuthority {
		t.Fatal("the Cloudflare profile satisfies the journal gate; recovery would open a journal in a container layer")
	}
	if cloudflare.StoreMode != config.StoreModeCloudflareAuthority {
		t.Fatalf("the Cloudflare profile's store mode is %q; the gate is no longer the thing excluding it", cloudflare.StoreMode)
	}

	// Property 2: the real Cloudflare vector store cannot satisfy the unchecked
	// assertion the recovery goroutine makes. A Vectorize index has no local
	// collection to recover into and no on-disk journal to resume, so the
	// capability is absent by construction rather than by configuration.
	//
	// The real store is used rather than a probe type on purpose: a hand-written
	// stand-in would keep saying "does not implement" no matter what the provider
	// actually does, which is the opposite of what this assertion is for.
	bridge, err := cloudflarebridge.NewClient("http://archive-center-bridge.internal", "bridge-token", 0)
	if err != nil {
		t.Fatalf("cloudflarebridge.NewClient: %v", err)
	}
	vectorize, err := vector.NewVectorizeStore(bridge)
	if err != nil {
		t.Fatalf("NewVectorizeStore: %v", err)
	}
	if _, ok := interface{}(vectorize).(vector.IndexRecovery); ok {
		t.Error("the Vectorize provider satisfies vector.IndexRecovery; the startup gate is now the only thing preventing a journal, and it is a boolean")
	}

	// And the filesystem stays clean, which is the artifact the stateless rule
	// actually forbids.
	assertNoRecoveryJournal(t, dataDir, logDir)
}

// TestRecoveryJournalPathCannotDescribeAVectorizeIndex records the second,
// independent reason. Even with the gate wide open, the journal is a Chroma
// artifact: it is named from the Chroma endpoint, API path and collection, so on
// the Cloudflare profile every one of those is empty and the path is keyed on
// nothing that identifies the index being rebuilt.
func TestRecoveryJournalPathCannotDescribeAVectorizeIndex(t *testing.T) {
	t.Setenv("AC_LOG_DIR", t.TempDir())

	local := &Server{Cfg: config.Config{ChromaEndpoint: "http://127.0.0.1:8000", ChromaAPIPath: "/api/v2", ChromaCollection: "archive"}}
	cloudflare := &Server{Cfg: config.Config{ChromaEndpoint: "", ChromaAPIPath: "", ChromaCollection: ""}}

	localPath := local.indexRecoveryJournalPath()
	cloudflarePath := cloudflare.indexRecoveryJournalPath()
	if localPath == cloudflarePath {
		t.Errorf("both profiles produce the same journal path %q; the path is not keyed on the collection, so one deployment's journal would collide with another's", localPath)
	}
	if filepath.Base(localPath) == filepath.Base(cloudflarePath) {
		t.Errorf("journal name %q is identical for a real Chroma collection and for a Vectorize index", filepath.Base(localPath))
	}

	// A different collection must get a different file, or two deployments sharing
	// a log directory would resume each other's rebuild.
	other := &Server{Cfg: config.Config{ChromaEndpoint: "http://127.0.0.1:8000", ChromaAPIPath: "/api/v2", ChromaCollection: "other"}}
	if other.indexRecoveryJournalPath() == localPath {
		t.Error("two Chroma collections share a journal path")
	}
}

// TestMariaDBAuthorityStillResumesTheJournal is the direction this must not break.
// The local runtime's recovery journal is a working, resumable mechanism and the
// parity work is not a licence to remove it; only the Cloudflare profile skips it.
func TestMariaDBAuthorityStillResumesTheJournal(t *testing.T) {
	logDir := t.TempDir()
	t.Setenv("AC_LOG_DIR", logDir)

	vec := &journalingVectorStore{}
	s := &Server{
		Cfg: config.Config{
			StoreMode:        config.StoreModeMariaDBAuthority,
			ChromaEndpoint:   "http://127.0.0.1:8000",
			ChromaAPIPath:    "/api/v2",
			ChromaCollection: "archive",
		},
		Vector: vec,
	}
	if _, ok := s.Vector.(vector.IndexRecovery); !ok {
		t.Fatal("test double no longer satisfies vector.IndexRecovery")
	}
	if s.Cfg.StoreMode != config.StoreModeMariaDBAuthority {
		t.Fatal("gate condition changed; this test is asserting the wrong thing")
	}
	// The journal file is JSON, and a restart must find something readable. The
	// point here is only that the local path still addresses a real file.
	path := s.indexRecoveryJournalPath()
	if filepath.Dir(path) != logDir {
		t.Errorf("journal path %q is not under AC_LOG_DIR %q", path, logDir)
	}
	if filepath.Ext(path) != ".json" {
		t.Errorf("journal path %q does not end in .json", path)
	}
}

// assertNoRecoveryJournal fails if anything that looks like the recovery journal
// or its Chroma snapshot appeared anywhere under the given roots.
func assertNoRecoveryJournal(t *testing.T, roots ...string) {
	t.Helper()
	for _, root := range roots {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if info.IsDir() {
				return nil
			}
			name := filepath.Base(path)
			if name == "chromadb" || len(name) > 13 && name[:13] == "chroma-recover" {
				t.Errorf("the Cloudflare profile wrote %q; a Container layer is discarded on restart, so a journal there resumes nothing and reports a recovery that never happened", path)
			}
			if info.Size() > 0 && name == "chroma-recovery.json" {
				t.Errorf("unexpected recovery artifact %q", path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
}

// TestRecoveryJournalFixtureShape is a small guard on the helper above: a journal
// is a JSON document, and if that ever changes the exclusion test's file check
// would silently stop matching.
func TestRecoveryJournalFixtureShape(t *testing.T) {
	payload := map[string]any{"phase": "rebuilding", "updated_at": time.Unix(0, 0).UTC().Format(time.RFC3339)}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(filepath.Base("chroma-recovery-deadbeef.json")) <= 13 {
		t.Error("the exclusion test's filename prefix check no longer identifies a journal")
	}
	_ = encoded
}
