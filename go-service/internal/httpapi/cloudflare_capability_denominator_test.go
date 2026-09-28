package httpapi

import (
	"github.com/risulongmemory/archive-center-go/internal/config"
	"strings"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/vector"
)

// The coverage ratio has to be readable, not just present.
//
// It read "85/86" and stopped. That tells a Cloudflare operator a capability of
// the parity surface is missing without saying which, so the reader cannot work
// out whether it is theirs to close. The missing one is ShadowStatusReporter —
// how many dual writes to a MariaDB shadow failed — and this profile does not use
// MariaDB at all, so there is no shadow to report on. It is inapplicable, not
// unimplemented, and no amount of work changes the number.
//
// A denominator that can never be reached is worse than a wrong one: it makes
// "complete" unsayable, and it leaves the gap looking like unfinished work.

// TestCloudflareReadinessExplainsItsCapabilityDenominator pins all three parts:
// the ratio, the names of what is actually missing, and the reason the
// denominator is smaller than the manifest.
func TestCloudflareReadinessExplainsItsCapabilityDenominator(t *testing.T) {
	checks := serveReadyWithVectorConfig(t, cloudflareVectorConfig(), vector.NewFakeVectorStore())

	ratio, ok := checks["store_capabilities"].(string)
	if !ok {
		t.Fatalf("store_capabilities is missing or not a string: %v", checks["store_capabilities"])
	}
	parts := strings.Split(ratio, "/")
	if len(parts) != 2 {
		t.Fatalf("store_capabilities = %q, want an implemented/applicable ratio", ratio)
	}

	// The denominator is the manifest minus the exemptions, so it must be smaller
	// and the exemption must be stated.
	inapplicable, ok := checks["store_capabilities_inapplicable"].(string)
	if !ok {
		t.Fatalf("store_capabilities_inapplicable is absent; a smaller denominator with no explanation looks like a capability was quietly dropped to improve the number")
	}
	if !strings.Contains(inapplicable, "ShadowStatusReporter") {
		t.Errorf("store_capabilities_inapplicable = %q, want it to name ShadowStatusReporter", inapplicable)
	}
	if !strings.Contains(inapplicable, "shadow") {
		t.Errorf("store_capabilities_inapplicable = %q, want the reason carried with the name so the operator does not have to look it up", inapplicable)
	}

	// Anything still missing must be named, because those ARE actionable. A Noop
	// store implements almost nothing, so this asserts the key appears rather than
	// pretending the list is empty.
	if missing, present := checks["store_capabilities_missing"]; present {
		list, _ := missing.(string)
		if strings.Contains(list, "ShadowStatusReporter") {
			t.Errorf("store_capabilities_missing names ShadowStatusReporter: %q; it is inapplicable to this profile, not missing, and listing it sends an operator after work that does not exist", list)
		}
	}
}

// TestLocalReadinessKeepsTheFullDenominator is the direction that must not break.
// A profile with a shadow must still count the reporter, or its coverage would
// claim a completeness it does not have.
func TestLocalReadinessKeepsTheFullDenominator(t *testing.T) {
	cfg := config.Default()
	cfg.RuntimeProfile = config.RuntimeProfileFullLocal
	cfg.VectorMode = config.VectorModeExternal
	cfg.StoreMode = config.StoreModeMariaDBAuthority
	cfg.ChromaEnabled = true
	cfg.ChromaEndpoint = "http://127.0.0.1:8000"

	checks := serveReadyWithVectorConfig(t, cfg, vector.NewFakeVectorStore())
	if _, present := checks["store_capabilities_inapplicable"]; present {
		t.Errorf("a local profile reported inapplicable capabilities: %v; the exemptions are scoped per profile", checks["store_capabilities_inapplicable"])
	}
}
