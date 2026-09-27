package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/config"
)

func newCloudflareTestConfig() config.Config {
	cfg := config.Default()
	cfg.RuntimeProfile = config.RuntimeProfileCloudflare
	cfg.StoreMode = config.StoreModeCloudflareAuthority
	cfg.VectorMode = config.VectorModeCloudflare
	cfg.CloudflareBridgeURL = "http://archive-center-bridge.internal"
	cfg.CloudflareBridgeToken = "bridge-token"
	return cfg
}

func TestHandleReadyCloudflareProfileBlocksUntilParityComplete(t *testing.T) {
	mux := http.NewServeMux()
	srv := NewServer(newCloudflareTestConfig())
	srv.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	var resp readyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Ready || resp.StoreReady || !resp.Degraded {
		t.Fatalf("bootstrap-only Cloudflare profile must not report ready: %+v", resp)
	}
	if resp.RuntimeProfile != string(config.RuntimeProfileCloudflare) {
		t.Errorf("runtime_profile = %q, want %q", resp.RuntimeProfile, config.RuntimeProfileCloudflare)
	}
	if resp.VectorMode != string(config.VectorModeCloudflare) {
		t.Errorf("vector_mode = %q, want %q", resp.VectorMode, config.VectorModeCloudflare)
	}
	if resp.Checks["ready_blocker"] != "cloudflare_parity_incomplete" {
		t.Errorf("ready_blocker = %q, want cloudflare_parity_incomplete", resp.Checks["ready_blocker"])
	}
	if resp.Checks["cloudflare_profile"] != "bootstrap_only" {
		t.Errorf("cloudflare_profile = %q, want bootstrap_only", resp.Checks["cloudflare_profile"])
	}
	if resp.Checks["cloudflare_parity"] != "incomplete" {
		t.Errorf("cloudflare_parity = %q, want incomplete", resp.Checks["cloudflare_parity"])
	}
	if resp.Checks["cloudflare_bridge"] != "configured" {
		t.Errorf("cloudflare_bridge = %q, want configured", resp.Checks["cloudflare_bridge"])
	}
	if resp.Checks["store_mode"] != string(config.StoreModeCloudflareAuthority) {
		t.Errorf("store_mode = %q, want %q", resp.Checks["store_mode"], config.StoreModeCloudflareAuthority)
	}
}

func TestHandleReadyCloudflareProfileReportsMissingBridgeConfig(t *testing.T) {
	cfg := newCloudflareTestConfig()
	cfg.CloudflareBridgeURL = ""
	cfg.CloudflareBridgeToken = ""

	mux := http.NewServeMux()
	srv := NewServer(cfg)
	srv.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	var resp readyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Checks["cloudflare_bridge"] != "not_configured" {
		t.Errorf("cloudflare_bridge = %q, want not_configured", resp.Checks["cloudflare_bridge"])
	}
	if resp.Checks["ready_blocker"] != "cloudflare_parity_incomplete" {
		t.Errorf("ready_blocker = %q, want cloudflare_parity_incomplete", resp.Checks["ready_blocker"])
	}
}

func TestNewServerCloudflareAuthorityKeepsWriteGuardClosed(t *testing.T) {
	cfg := newCloudflareTestConfig()
	srv := NewServer(cfg)

	if srv.Store == nil {
		t.Fatal("Store should not be nil")
	}
	if srv.StoreOpenError != nil {
		t.Fatalf("StoreOpenError = %v, want nil (noop bootstrap placeholder)", srv.StoreOpenError)
	}
	if srv.usesShadowWriteStore() {
		t.Error("usesShadowWriteStore() must stay false for the Stage 2 cloudflare bootstrap; the D1 store lands in Stage 3")
	}
	if _, ok := srv.Store.(storeShadowReporterForTest); ok {
		t.Error("cloudflare bootstrap store must not pose as a shadow status reporter")
	}
}

func TestHandleReadyLocalProfilesKeepLegacyChecks(t *testing.T) {
	// The Cloudflare profile must not alter local readiness representation.
	mux := http.NewServeMux()
	srv := setupTestServer()
	srv.RegisterRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var resp readyResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	for _, key := range []string{"cloudflare_profile", "cloudflare_parity", "cloudflare_bridge"} {
		if _, ok := resp.Checks[key]; ok {
			t.Errorf("local profile must not report %q check, got %q", key, resp.Checks[key])
		}
	}
}

// storeShadowReporterForTest mirrors store.ShadowStatusReporter for the
// bootstrap guard assertion without importing the store package here.
type storeShadowReporterForTest interface {
	ShadowStatus() (int64, error)
}
