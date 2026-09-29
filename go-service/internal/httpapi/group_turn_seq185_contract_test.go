package httpapi

import (
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/config"
)

// There are two kinds of surface in this response and they answer different
// questions, and the difference is the whole point.
//
// A LIVE surface reports the deployment that is running. It must follow the
// profile: a report that says "mariadb" when the canonical store is D1 sends an
// operator to a database that is not in the path, and that is the exact failure
// Stage 5 fixed in /ready.
//
// A CONTRACT surface states a rule or records a decision. It says what it means
// whether or not that is what is deployed. buildChromaIdentitySQLiteHydration
// describes how a hit hydrates to a canonical row; the policy is the same on
// Cloudflare, but the contract is named for the engine it was specified against,
// and buildFirstLiveScopeDecision209 records a decision that was taken while the
// deployment was MariaDB plus ChromaDB. Rewriting either would falsify it.
//
// These tests pin both halves, because the mistake goes in both directions: making
// a contract generic to look consistent, and making a report generic to avoid
// naming anything.

// TestSeq185ContractSurfacesStayChromaShaped keeps the contract surfaces as they
// are. If someone "fixes" these to follow the profile, this fails and the comment
// above the builders says why not.
func TestSeq185ContractSurfacesStayChromaShaped(t *testing.T) {
	cases := map[string]map[string]any{
		"bounded_live_scope":               buildBoundedLiveScope(),
		"sqlite_truth_preserve":            buildSQLiteTruthPreserve(),
		"chroma_identity_sqlite_hydration": buildChromaIdentitySQLiteHydration(),
		"first_live_scope_decision":        buildFirstLiveScopeDecision209(),
		"chroma_miss_fallback_preserve":    buildChromaMissFallbackPreserve(),
		"chroma_sqlite_dedupe_merge":       buildChromaSQLiteDedupeMerge(),
		"chroma_enabled_smoke_check":       buildChromaEnabledSmokeCheck(),
		"chroma_candidate_merge_replace":   buildChromaCandidateMergeReplaceDecision210(),
		"limited_live_chroma_smoke_check":  buildLimitedLiveChromaSmokeCheck202(),
		"bundle_release_gate":              buildBundleReleaseGate201(),
	}

	for name, surface := range cases {
		// A contract surface must say so. If a builder ever stops asserting
		// truth_authority=false it has become a live report, and this loop is what
		// makes that change visible instead of silent.
		if authority, present := surface["truth_authority"]; !present || authority != false {
			t.Errorf("%s: truth_authority = %v, want false; a surface that claims authority is a live report and must follow the profile instead", name, authority)
		}
		if accelerator, present := surface["vector_accelerator"]; present && accelerator != "chromadb" {
			t.Errorf("%s: vector_accelerator = %v, want %q; this is a Chroma rule contract, not a report of the running deployment", name, accelerator, "chromadb")
		}
		if authority, present := surface["canonical_truth_authority"]; present && authority != "mariadb" {
			t.Errorf("%s: canonical_truth_authority = %v, want %q; the decision these record was taken against MariaDB", name, authority, "mariadb")
		}
	}
}

// TestLiveSurfacesFollowTheProfile is the other half, and the direction that
// actually mattered. A live report that names a store or an engine is sending
// an operator somewhere, so it has to be right.
func TestLiveSurfacesFollowTheProfile(t *testing.T) {
	cloudflare := config.Config{
		Mode:                  config.ModeShadow,
		RuntimeProfile:        config.RuntimeProfileCloudflare,
		StoreMode:             config.StoreModeCloudflareAuthority,
		VectorMode:            config.VectorModeCloudflare,
		CloudflareBridgeURL:   "http://archive-center-bridge.internal",
		CloudflareBridgeToken: "bridge-token",
		Auth:                  config.AuthConfig{Enforce: true, BearerToken: "operator-token"},
	}
	checks := serveReadyWithVectorConfig(t, cloudflare, nil)

	if got := checks["vector_accelerator"]; got != "vectorize" {
		t.Errorf("readiness vector_accelerator = %v, want vectorize", got)
	}
	if got := checks["store_mode"]; got != string(config.StoreModeCloudflareAuthority) {
		t.Errorf("readiness store_mode = %v, want %q", got, config.StoreModeCloudflareAuthority)
	}
	for _, key := range []string{"vector_accelerator", "vector_engine_policy", "vector_accelerator_reachable", "turn_preparation_settings"} {
		if _, present := checks[key]; !present {
			t.Errorf("readiness is missing %q, which is how a live surface becomes indistinguishable from a contract one", key)
		}
	}
}
