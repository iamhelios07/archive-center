package config

import (
	"testing"
)

// Vector accelerator reachability predicates.
//
// These exist because a dozen call sites had each grown a private answer to
// "is a vector accelerator configured?", and they read ChromaEndpoint. That is
// the right question for a local ChromaDB deployment and the wrong one for the
// Cloudflare profile, where ChromaEndpoint is deliberately empty because the
// accelerator is Vectorize behind the Worker bridge.
//
// The failure these predicates prevent is silent. A Cloudflare deployment with
// a fully working vector index reported "vector not configured" at every call
// site, so recall quietly fell back to lexical fill, clean upserts skipped, and
// admin reindex reported nothing to do. Nothing in that chain announces itself
// as a missing provider, and the deployment still starts.

// TestVectorAcceleratorConfiguredOnTheCloudflareProfile is the regression this
// whole predicate exists for: a Cloudflare deployment has an accelerator, and it
// must be reported as configured even though it has no Chroma endpoint.
func TestVectorAcceleratorConfiguredOnTheCloudflareProfile(t *testing.T) {
	cfg := Config{
		RuntimeProfile:        RuntimeProfileCloudflare,
		StoreMode:             StoreModeCloudflareAuthority,
		VectorMode:            VectorModeCloudflare,
		CloudflareBridgeURL:   "http://archive-center-bridge.internal",
		CloudflareBridgeToken: "bridge-token",
	}

	if cfg.ChromaEndpoint != "" {
		t.Fatalf("fixture is wrong: the Cloudflare profile must have no Chroma endpoint, got %q", cfg.ChromaEndpoint)
	}
	if cfg.ChromaEnabled {
		t.Fatal("fixture is wrong: the Cloudflare profile must keep ChromaEnabled false")
	}
	if !cfg.VectorAcceleratorConfigured() {
		t.Error("a wired Cloudflare profile must report a configured vector accelerator")
	}
	if !cfg.VectorAcceleratorEnabled() {
		t.Error("a Cloudflare profile selects Vectorize as required, so the accelerator is enabled")
	}
	if got := cfg.VectorAcceleratorName(); got != "vectorize" {
		t.Errorf("VectorAcceleratorName() = %q, want %q", got, "vectorize")
	}
}

// TestVectorAcceleratorNotConfiguredWithoutBridgeCredentials pins the negative
// case. Validate() already refuses to start this profile, so reaching it means a
// hand-built Config: reporting a working accelerator there would make health
// claim an index that nothing can reach.
func TestVectorAcceleratorNotConfiguredWithoutBridgeCredentials(t *testing.T) {
	base := Config{
		RuntimeProfile: RuntimeProfileCloudflare,
		StoreMode:      StoreModeCloudflareAuthority,
		VectorMode:     VectorModeCloudflare,
	}
	for _, tc := range []struct {
		name    string
		url     string
		token   string
		wantCfg bool
	}{
		{name: "neither", wantCfg: false},
		{name: "url only", url: "http://archive-center-bridge.internal", wantCfg: false},
		{name: "token only", token: "bridge-token", wantCfg: false},
		{name: "both", url: "http://archive-center-bridge.internal", token: "bridge-token", wantCfg: true},
		{name: "blank url", url: "   ", token: "bridge-token", wantCfg: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.CloudflareBridgeURL = tc.url
			cfg.CloudflareBridgeToken = tc.token
			if got := cfg.VectorAcceleratorConfigured(); got != tc.wantCfg {
				t.Errorf("VectorAcceleratorConfigured() = %t, want %t", got, tc.wantCfg)
			}
		})
	}
}

// TestVectorAcceleratorConfiguredOnChromaKeepsLocalBehaviour pins that the local
// path is unchanged. A Chroma endpoint still means configured, and its absence
// still means not configured, whatever the Cloudflare profile does.
func TestVectorAcceleratorConfiguredOnChromaKeepsLocalBehaviour(t *testing.T) {
	cfg := Config{
		RuntimeProfile: RuntimeProfileFullLocal,
		StoreMode:      StoreModeMariaDBAuthority,
		VectorMode:     VectorModeExternal,
		ChromaEndpoint: "http://127.0.0.1:8000",
		ChromaEnabled:  true,
	}
	if !cfg.VectorAcceleratorConfigured() {
		t.Error("a Chroma endpoint must still report a configured accelerator")
	}
	if !cfg.VectorAcceleratorEnabled() {
		t.Error("ChromaEnabled must still report the accelerator enabled")
	}
	if got := cfg.VectorAcceleratorName(); got != "chromadb" {
		t.Errorf("VectorAcceleratorName() = %q, want %q", got, "chromadb")
	}

	cfg.ChromaEndpoint = ""
	if cfg.VectorAcceleratorConfigured() {
		t.Error("no endpoint must report no accelerator")
	}
	if got := cfg.VectorAcceleratorName(); got != "" {
		t.Errorf("VectorAcceleratorName() = %q, want the empty string when nothing is configured", got)
	}
}

// TestVectorAcceleratorSelectedNamesTheProductEngine pins the product-level
// label, which is a different question from reachability.
//
// The recall trace names the engine the deployment is BUILT AROUND even while it
// runs a bounded read shadow with nothing reachable. Collapsing that with
// reachability would blank a useful label in read-shadow mode and, worse, would
// let a read shadow be reported as a live read. The Cloudflare profile is the
// case that must not say ChromaDB.
func TestVectorAcceleratorSelectedNamesTheProductEngine(t *testing.T) {
	cloudflare := Config{
		RuntimeProfile:        RuntimeProfileCloudflare,
		StoreMode:             StoreModeCloudflareAuthority,
		VectorMode:            VectorModeCloudflare,
		CloudflareBridgeURL:   "http://archive-center-bridge.internal",
		CloudflareBridgeToken: "bridge-token",
	}
	if got := cloudflare.VectorAcceleratorSelected(); got != "vectorize" {
		t.Errorf("VectorAcceleratorSelected() = %q, want %q; a Cloudflare trace must not name ChromaDB", got, "vectorize")
	}
	// A half-selected Cloudflare profile must not be mislabelled either.
	half := cloudflare
	half.VectorMode = VectorModeExternal
	if got := half.VectorAcceleratorSelected(); got != "chromadb" {
		t.Errorf("VectorAcceleratorSelected() = %q, want %q; a profile still on a local vector mode is a MariaDB deployment", got, "chromadb")
	}

	// The local runtime names ChromaDB even when nothing is reachable: that is a
	// statement about the product, not about a live connection.
	for _, cfg := range []Config{
		{RuntimeProfile: RuntimeProfileCoreLite},
		{RuntimeProfile: RuntimeProfileCoreLite, VectorMode: VectorModeFallback},
		{RuntimeProfile: RuntimeProfileFullLocal, VectorMode: VectorModeExternal, ChromaEndpoint: "http://127.0.0.1:8000", ChromaEnabled: true},
	} {
		if got := cfg.VectorAcceleratorSelected(); got != "chromadb" {
			t.Errorf("VectorAcceleratorSelected() = %q, want %q", got, "chromadb")
		}
	}
}

// TestVectorAcceleratorConfiguredIgnoresVectorModeForChroma pins that the
// Cloudflare branch is selected by the vector MODE, not by the profile name. A
// half-selected profile must not take the bridge branch and then report
// itself unconfigured, which is the confusing failure that made this worth
// splitting out from IsCloudflareProfile.
func TestVectorAcceleratorConfiguredIgnoresVectorModeForChroma(t *testing.T) {
	cfg := Config{
		RuntimeProfile: RuntimeProfileCloudflare,
		StoreMode:      StoreModeCloudflareAuthority,
		// Not the Cloudflare vector mode: this profile is mid-migration, still
		// pointing at a Chroma endpoint.
		VectorMode:    VectorModeExternal,
		ChromaEnabled: true,
	}
	if cfg.IsCloudflareProfile() {
		t.Fatal("fixture is wrong: IsCloudflareProfile must be false without the cloudflare vector mode")
	}
	cfg.ChromaEndpoint = "http://127.0.0.1:8000"
	if !cfg.VectorAcceleratorConfigured() {
		t.Error("a non-cloudflare vector mode must keep reading the Chroma endpoint")
	}
	if got := cfg.VectorAcceleratorName(); got != "chromadb" {
		t.Errorf("VectorAcceleratorName() = %q, want %q", got, "chromadb")
	}
}
