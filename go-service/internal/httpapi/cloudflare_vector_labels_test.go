package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/config"
	"github.com/risulongmemory/archive-center-go/internal/store"
	"github.com/risulongmemory/archive-center-go/internal/vector"
)

// The readiness report has to be able to send an operator to the right place.
//
// It used to branch on ChromaEnabled alone. That is false on the Cloudflare
// profile, so a Cloudflare deployment reported vector_accelerator as
// "mariadb_fallback" and vector_engine_policy as "fallback" — which is wrong
// twice over: the accelerator there is Vectorize, and the canonical store is D1.
// Naming MariaDB points an operator at a store that is not in the path at all.
//
// These tests drive the real handler over the real configuration shapes rather
// than a helper, so a future refactor that reintroduces the Chroma-only branch
// fails here.

func serveReadyWithVectorConfig(t *testing.T, cfg config.Config, vec vector.VectorStore) map[string]any {
	t.Helper()
	srv := &Server{
		Cfg:    cfg,
		Store:  store.NewNoopStore(),
		Vector: vec,
		RuntimeConfig: RuntimeConfig{
			Synced: true, FailedQueueMaxAttempts: 4,
		},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		srv.handleReady(w, r)
	})
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))

	var payload struct {
		Checks map[string]any `json:"checks"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode ready payload %q: %v", recorder.Body.String(), err)
	}
	return payload.Checks
}

func cloudflareVectorConfig() config.Config {
	return config.Config{
		// shadow, not live: a live or cutover mode demands the MariaDB authority
		// DSN (config.go:454), and the point of this profile is that it does not
		// use it. Validate() still enforces the Cloudflare rules under shadow.
		Mode:                  config.ModeShadow,
		RuntimeProfile:        config.RuntimeProfileCloudflare,
		StoreMode:             config.StoreModeCloudflareAuthority,
		VectorMode:            config.VectorModeCloudflare,
		CloudflareBridgeURL:   "http://archive-center-bridge.internal",
		CloudflareBridgeToken: "bridge-token",
		ChromaEnabled:         false,
		ChromaEndpoint:        "",
	}
}

// TestReadyNamesVectorizeOnTheCloudflareProfile is the regression. The two wrong
// values are asserted by name so the failure message says which lie was told.
func TestReadyNamesVectorizeOnTheCloudflareProfile(t *testing.T) {
	checks := serveReadyWithVectorConfig(t, cloudflareVectorConfig(), vector.NewFakeVectorStore())

	if got := checks["vector_accelerator"]; got != "vectorize" {
		t.Errorf("vector_accelerator = %v, want %q; a Cloudflare deployment reporting MariaDB sends an operator to a store that is not in the path", got, "vectorize")
	}
	if got := checks["vector_engine_policy"]; got != "vectorize_required" {
		t.Errorf("vector_engine_policy = %v, want %q; the Cloudflare profile refuses to start without the accelerator, so it is required rather than optional", got, "vectorize_required")
	}
	if got := checks["vector_accelerator_reachable"]; got != "vectorize" {
		t.Errorf("vector_accelerator_reachable = %v, want %q; the bridge credentials are present", got, "vectorize")
	}
}

// TestReadyNamesChromaOnALocalDeployment pins that the local path is unchanged.
// A generic "vector" label that lost the engine name would be a regression in the
// other direction.
func TestReadyNamesChromaOnALocalDeployment(t *testing.T) {
	cfg := config.Default()
	cfg.RuntimeProfile = config.RuntimeProfileFullLocal
	cfg.VectorMode = config.VectorModeExternal
	cfg.StoreMode = config.StoreModeMariaDBAuthority
	cfg.ChromaEnabled = true
	cfg.ChromaEndpoint = "http://127.0.0.1:8000"

	checks := serveReadyWithVectorConfig(t, cfg, vector.NewFakeVectorStore())
	if got := checks["vector_accelerator"]; got != "chromadb" {
		t.Errorf("vector_accelerator = %v, want %q", got, "chromadb")
	}
	if got := checks["vector_engine_policy"]; got != "chromadb_required" {
		t.Errorf("vector_engine_policy = %v, want %q", got, "chromadb_required")
	}
	if got := checks["vector_accelerator_reachable"]; got != "chromadb" {
		t.Errorf("vector_accelerator_reachable = %v, want %q", got, "chromadb")
	}
}

// TestReadySeparatesTheEngineFromItsReachability is the distinction the two keys
// exist for. A deployment is BUILT AROUND an engine whether or not one is
// reachable right now, and collapsing the two would either blank a useful label
// in fallback mode or report a live index that nothing is connected to.
func TestReadySeparatesTheEngineFromItsReachability(t *testing.T) {
	cfg := config.Default()
	cfg.RuntimeProfile = config.RuntimeProfileCoreLite
	cfg.StoreMode = config.StoreModeMariaDBAuthority
	cfg.VectorMode = config.VectorModeFallback
	cfg.ChromaEnabled = false
	cfg.ChromaEndpoint = ""

	checks := serveReadyWithVectorConfig(t, cfg, vector.NewFakeVectorStore())
	if got := checks["vector_accelerator"]; got != "mariadb_fallback" {
		t.Errorf("vector_accelerator = %v, want %q; a fallback profile still runs a lexical vector substitute", got, "mariadb_fallback")
	}
	if got := checks["vector_engine_policy"]; got != "fallback" {
		t.Errorf("vector_engine_policy = %v, want %q", got, "fallback")
	}
	// The product-level label still names the engine this system is for, while
	// the reachability label is empty because nothing is connected. Both answers
	// are true and they are different questions.
	if got := checks["vector_accelerator_reachable"]; got != "" {
		t.Errorf("vector_accelerator_reachable = %v, want the empty string when no accelerator is reachable", got)
	}
}

// TestReadyReportsVectorOffWhenTheProfileTurnsItOff pins the third branch, which
// previously fell through to "mariadb_fallback" for any mode that was neither
// Chroma-enabled nor off.
func TestReadyReportsVectorOffWhenTheProfileTurnsItOff(t *testing.T) {
	cfg := config.Default()
	cfg.RuntimeProfile = config.RuntimeProfileCoreLite
	cfg.StoreMode = config.StoreModeMariaDBAuthority
	cfg.VectorMode = config.VectorModeOff
	cfg.ChromaEnabled = false

	checks := serveReadyWithVectorConfig(t, cfg, vector.NewFakeVectorStore())
	if got := checks["vector_accelerator"]; got != "none" {
		t.Errorf("vector_accelerator = %v, want %q", got, "none")
	}
	if got := checks["vector_engine_policy"]; got != "off" {
		t.Errorf("vector_engine_policy = %v, want %q", got, "off")
	}
}

// TestCloudflareProfileRefusesToStartWithoutAnUnreachableAccelerator records why
// the Cloudflare branch needs no "optional" case.
//
// It is tempting to test a running Cloudflare /ready with no bridge token, to
// check the report says "not reachable". That state cannot exist: Validate()
// refuses that configuration, so NewServer is never constructed and no request
// is ever served. Asserting on it would be testing a fiction, and it would
// quietly make the code look more flexible than it is.
//
// So the invariant is pinned where it is actually enforced. A process that
// answers /ready under this profile is by construction an accelerated one, which
// is why the engine label and the reachability label are allowed to coincide
// here while they deliberately differ in fallback mode.
func TestCloudflareProfileRefusesToStartWithoutAnUnreachableAccelerator(t *testing.T) {
	missingToken := cloudflareVectorConfig()
	missingToken.CloudflareBridgeToken = ""
	if err := missingToken.Validate(); err == nil {
		t.Error("Validate() accepted a Cloudflare profile with no bridge token; a running process could then claim Vectorize while being unable to reach it")
	}

	missingURL := cloudflareVectorConfig()
	missingURL.CloudflareBridgeURL = ""
	if err := missingURL.Validate(); err == nil {
		t.Error("Validate() accepted a Cloudflare profile with no bridge URL")
	}

	// And the complete configuration, the only one that can serve, is accepted.
	if err := cloudflareVectorConfig().Validate(); err != nil {
		t.Errorf("Validate() rejected a complete Cloudflare profile: %v", err)
	}

	// Under the one configuration that serves, both labels name the same engine,
	// and that agreement is a consequence of the profile being required.
	checks := serveReadyWithVectorConfig(t, cloudflareVectorConfig(), vector.NewFakeVectorStore())
	if checks["vector_accelerator"] != checks["vector_accelerator_reachable"] {
		t.Errorf("engine = %v and reachable = %v disagree, but a serving Cloudflare process is accelerated by construction",
			checks["vector_accelerator"], checks["vector_accelerator_reachable"])
	}
}

// TestReadyVectorLabelsSurviveTheStoreLayer keeps the assertion honest about what
// it exercises: the labels come from configuration, not from the store, so a
// Noop store is enough and the test does not need a provider.
func TestReadyVectorLabelsSurviveTheStoreLayer(t *testing.T) {
	checks := serveReadyWithVectorConfig(t, cloudflareVectorConfig(), vector.NewFakeVectorStore())
	for _, key := range []string{"vector_accelerator", "vector_engine_policy", "vector_accelerator_reachable"} {
		if _, present := checks[key]; !present {
			t.Errorf("readiness report is missing %q", key)
		}
	}
	_ = context.Background()
}
