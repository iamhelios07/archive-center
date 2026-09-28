package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/config"
	"github.com/risulongmemory/archive-center-go/internal/store"
)

func newCloudflareTestConfig() config.Config {
	cfg := config.Default()
	cfg.RuntimeProfile = config.RuntimeProfileCloudflare
	cfg.StoreMode = config.StoreModeCloudflareAuthority
	cfg.VectorMode = config.VectorModeCloudflare
	// A loopback port with nothing listening: the bridge client validates the URL
	// at construction, and a write fails immediately instead of waiting on DNS.
	cfg.CloudflareBridgeURL = "http://127.0.0.1:9"
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
	// The parity gap must be observable: routes gate features on optional
	// capabilities, so readiness publishes how many the provider satisfies.
	coverage := resp.Checks["store_capabilities"]
	if coverage == "" || !strings.Contains(coverage, "/") {
		t.Errorf("store_capabilities = %q, want an implemented/total report", coverage)
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

// TestNewServerCloudflareAuthorityWiresD1Provider verifies that the Cloudflare
// profile now opens the real D1 provider and that the canonical write capability
// is provider-neutral: the write and reset routes must not be gated on MariaDB
// mode names alone.
func TestNewServerCloudflareAuthorityWiresD1Provider(t *testing.T) {
	cfg := newCloudflareTestConfig()
	srv := NewServer(cfg)

	if srv.Store == nil {
		t.Fatal("Store should not be nil")
	}
	if srv.StoreOpenError != nil {
		t.Fatalf("StoreOpenError = %v, want nil for a fully configured cloudflare profile", srv.StoreOpenError)
	}
	if !srv.usesShadowWriteStore() {
		t.Error("the cloudflare D1 provider must expose the canonical write capability")
	}
	if !srv.hasCanonicalWriteCapability() {
		t.Error("hasCanonicalWriteCapability() must be true for cloudflare_authority")
	}
	if source := srv.storeWriteSource(); source != "cloudflare_d1" {
		t.Errorf("storeWriteSource() = %q, want cloudflare_d1", source)
	}
	if _, ok := srv.Store.(storeShadowReporterForTest); ok {
		t.Error("the D1 provider must not pose as a shadow status reporter")
	}

	// The provided store must be the D1 provider, not a silent no-op: a write
	// against an unreachable bridge fails loudly, whereas the no-op store would
	// return nil and hide the missing transport.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Store.SaveChatLog(ctx, &store.ChatLog{
		ChatSessionID: "s1", TurnIndex: 1, Role: "user", Content: "x",
	}); err == nil {
		t.Error("a write through an unreachable D1 bridge must fail, not silently succeed")
	}
}

// TestNewServerCloudflareAuthorityWithoutBridgeFailsLoudly verifies that an
// unwired Cloudflare profile reports an open error instead of falling back to a
// no-op store that would appear to persist writes.
func TestNewServerCloudflareAuthorityWithoutBridgeFailsLoudly(t *testing.T) {
	cfg := newCloudflareTestConfig()
	cfg.CloudflareBridgeURL = ""
	cfg.CloudflareBridgeToken = ""

	srv := NewServer(cfg)
	if srv.StoreOpenError == nil {
		t.Fatal("a cloudflare profile without bridge configuration must report an open error")
	}
	if err := srv.ValidateRuntimeDependencies(context.Background()); err == nil {
		t.Error("startup preflight must reject an unusable cloudflare authority store")
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
