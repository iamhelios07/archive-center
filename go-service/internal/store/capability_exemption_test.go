package store

import (
	"strings"
	"testing"
)

// The coverage number is a parity signal, so it has to be readable.
//
// "85/86" told a Cloudflare operator that one capability of the parity surface was
// missing, without saying which. The missing one is ShadowStatusReporter — how
// many dual writes to a MariaDB shadow failed — and the Cloudflare profile does
// not use MariaDB at all, so there is no shadow to report on. It is inapplicable,
// not unimplemented, and no amount of work would change the number.
//
// A permanently unreachable denominator is worse than a wrong one: it makes
// "complete" unsayable, and it leaves the reader unable to work out that the gap
// is not theirs to close.

// TestCloudflareCoverageExcludesTheShadowReporter pins the exemption.
func TestCloudflareCoverageExcludesTheShadowReporter(t *testing.T) {
	d1, _ := newD1TestStore(t)

	_, rawTotal := CapabilityCoverage(d1)
	implemented, applicable, total := CapabilityCoverageForProfile(d1, "cloudflare")

	if total != rawTotal {
		t.Errorf("the total should still report the whole manifest: got %d, want %d", total, rawTotal)
	}
	if applicable != total-1 {
		t.Errorf("applicable = %d, want %d; exactly one capability is inapplicable to this profile", applicable, total-1)
	}
	if implemented != applicable {
		t.Errorf("implemented = %d of %d applicable, want all of them; the remaining capability is absent because nothing in this profile uses MariaDB, not because D1 lacks it", implemented, applicable)
	}
	if missing := MissingApplicableCapabilities(d1, "cloudflare"); len(missing) != 0 {
		t.Errorf("applicable missing = %v, want none", missing)
	}
	// The exemption must be doing the work, not the absence of the capability.
	if len(MissingCapabilities(d1)) == 0 {
		t.Fatal("this test is no longer exercising the exemption; no capability is missing at all")
	}
}

// TestTheExemptionIsNamedAndExplained is the guard against an exemption becoming a
// place to hide a gap.
//
// Every exemption must carry a reason, because an exemption without one is
// indistinguishable from a claim quietly removed to make a number look better.
func TestTheExemptionIsNamedAndExplained(t *testing.T) {
	exemptions := CapabilityExemptionsFor("cloudflare")
	if len(exemptions) == 0 {
		t.Fatal("no exemptions declared; the coverage number cannot reach its denominator")
	}
	names := CapabilityNames()
	for _, exemption := range exemptions {
		if strings.TrimSpace(exemption.Reason) == "" {
			t.Errorf("exemption %q has no reason; an unexplained exemption is how a real gap gets hidden", exemption.Capability)
		}
		// A typo here would exempt nothing while looking like it exempted
		// something, and the coverage number would silently stay one short.
		found := false
		for _, name := range names {
			if name == exemption.Capability {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("exemption names %q, which is not in the manifest; the exemption is doing nothing", exemption.Capability)
		}
	}
}

// TestOtherProfilesAreUnaffected keeps the exemption from leaking. A profile that
// does have a shadow must still count the reporter, or its coverage would claim
// completeness it does not have.
func TestOtherProfilesAreUnaffected(t *testing.T) {
	d1, _ := newD1TestStore(t)
	implemented, applicable, total := CapabilityCoverageForProfile(d1, "full_local")
	if applicable != total {
		t.Errorf("applicable = %d of %d for a profile with no exemptions declared", applicable, total)
	}
	if implemented > applicable {
		t.Errorf("implemented %d exceeds applicable %d", implemented, applicable)
	}
}

// TestNoExemptionForACapabilityTheStoreActuallyHas keeps the mechanism honest in
// the other direction: exempting something that is implemented would hide a
// genuine regression from the number.
func TestNoExemptionForACapabilityTheStoreActuallyHas(t *testing.T) {
	d1, _ := newD1TestStore(t)
	implemented := map[string]bool{}
	for _, status := range CapabilityReport(d1) {
		implemented[status.Name] = status.Implemented
	}
	for _, exemption := range CapabilityExemptionsFor("cloudflare") {
		if implemented[exemption.Capability] {
			t.Errorf("capability %q is exempted on the cloudflare profile but the D1 store implements it; the exemption is unnecessary and hides that it works", exemption.Capability)
		}
	}
}
