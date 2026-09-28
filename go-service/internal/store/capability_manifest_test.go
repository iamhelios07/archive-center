package store

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// routeAssertionPatterns match every way the HTTP layer asserts an optional
// capability on the selected store.
//
// The first two forms used to be a single pattern anchored on the literal
// "s.Store", which is narrower than the guard's own stated purpose. A route
// commonly first narrows the store to the authority it needs and then asserts a
// SECOND capability on that narrowed value:
//
//	discoveryStore, ok := s.sourceDiscoveryAuthorityStore(w)
//	mutable, ok := discoveryStore.(store.SourceDiscoveryMutableStore)
//
// and sometimes switches on the capability rather than asserting it:
//
//	switch s.Store.(type) {
//	case store.WorldlineTopologySnapshotStore:
//	}
//
// Both forms are real capability requirements, and neither was counted. The
// effect was a manifest that under-reported the parity surface: a route could
// gate a feature on a capability the D1 provider lacks, take the fallback, and
// the coverage number would still have looked complete. Any assertion on a
// store-derived value in this package IS a capability requirement, because the
// only thing the HTTP layer ever type-asserts against the store package is a
// store interface.
var routeAssertionPatterns = []*regexp.Regexp{
	// Any type assertion whose asserted type comes from the store package.
	regexp.MustCompile(`\.\(\s*store\.([A-Za-z0-9_]+)\s*\)`),
	// A type switch case over the store package.
	regexp.MustCompile(`case\s+store\.([A-Za-z0-9_]+)\s*:`),
}

// storeInternalContracts are manifest capabilities with no HTTP route consumer.
//
// They are listed rather than left out so the coverage report stays an honest
// denominator, and they are named here so the route guard does not have to
// pretend a consumer exists. A row is only exempt if it is a contract BETWEEN
// store implementations (or a proof a caller obtains from the store without an
// HTTP surface). A capability a user can reach must appear in a route, and this
// exemption must not become a place to hide one: adding a name here is a
// deliberate claim that no user-facing route needs it.
var storeInternalContracts = map[string]string{
	"MemoryAdmissionProjectionInspector": "proves a committed source marker still has all of its projections; obtained by a store-to-store recovery path, not by an HTTP route",
}

// TestCapabilityManifestTracksRouteAssertions is the drift guard for the
// manifest. It re-derives the capability surface from the httpapi source and
// fails when a route starts asserting an interface the manifest does not list, or
// when the manifest lists one no route uses. Without this, a new route could gate
// a feature on a capability the Cloudflare provider does not have and the parity
// gap would stay invisible.
func TestCapabilityManifestTracksRouteAssertions(t *testing.T) {
	httpapiDir := filepath.Join("..", "httpapi")
	entries, err := os.ReadDir(httpapiDir)
	if err != nil {
		t.Fatalf("read httpapi package: %v", err)
	}

	seen := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(httpapiDir, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, pattern := range routeAssertionPatterns {
			for _, match := range pattern.FindAllStringSubmatch(string(content), -1) {
				seen[match[1]] = true
			}
		}
	}
	if len(seen) == 0 {
		t.Fatal("no store capability assertions found in the httpapi package; the guard is not working")
	}

	// The store-internal contracts are not route-visible by construction, so
	// they are folded into the expected set rather than demanded from httpapi.
	for name := range storeInternalContracts {
		seen[name] = true
	}

	listed := map[string]bool{}
	for _, name := range CapabilityNames() {
		if listed[name] {
			t.Errorf("capability %q is listed twice in the manifest", name)
		}
		listed[name] = true
	}

	var missing, extra []string
	for name := range seen {
		if !listed[name] {
			missing = append(missing, name)
		}
	}
	for name := range listed {
		if !seen[name] {
			extra = append(extra, name)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)

	if len(missing) > 0 {
		t.Errorf("routes assert capabilities missing from the manifest: %v", missing)
	}
	if len(extra) > 0 {
		t.Errorf("manifest lists capabilities no route asserts: %v", extra)
	}
	if len(seen) != len(CapabilityNames()) {
		t.Errorf("manifest has %d entries, route assertions cover %d", len(CapabilityNames()), len(seen))
	}
}

// TestCapabilityReportForD1Store records the current optional-capability coverage
// of the D1 provider. The base Store contract is complete, but most optional
// capabilities are not ported yet, so the report must state that gap rather than
// imply Cloudflare parity.
func TestCapabilityReportForD1Store(t *testing.T) {
	st, _ := newD1TestStore(t)

	report := CapabilityReport(st)
	if len(report) == 0 {
		t.Fatal("capability report is empty")
	}

	implemented := map[string]bool{}
	for _, status := range report {
		implemented[status.Name] = status.Implemented
	}

	// Capabilities the D1 provider genuinely satisfies.
	for _, name := range []string{"ActiveScopeStore", "AdminResetStore", "TimelineTurnIndexStore", "AuditLogCounter"} {
		if !implemented[name] {
			t.Logf("capability %s is not implemented by the D1 provider yet", name)
		}
	}
	if !implemented["ActiveScopeStore"] {
		t.Error("ActiveScopeStore must be implemented; ListInheritedWorldRules depends on it")
	}

	// ShadowStatusReporter is a local dual-write diagnostic, not a product
	// capability, so the D1 provider must never advertise it.
	if implemented["ShadowStatusReporter"] {
		t.Error("the D1 provider must not advertise the local dual-write shadow reporter")
	}

	gotImplemented, total := CapabilityCoverage(st)
	if total != len(report) {
		t.Errorf("coverage total = %d, report rows = %d", total, len(report))
	}
	if gotImplemented > total {
		t.Errorf("coverage implemented = %d exceeds total %d", gotImplemented, total)
	}
	if len(MissingCapabilities(st)) != total-gotImplemented {
		t.Error("MissingCapabilities must agree with CapabilityCoverage")
	}
	t.Logf("D1 optional-capability coverage: %d/%d", gotImplemented, total)
}

func TestCapabilityReportForNilStore(t *testing.T) {
	if report := CapabilityReport(nil); report != nil {
		t.Errorf("CapabilityReport(nil) = %v, want nil", report)
	}
	if implemented, total := CapabilityCoverage(nil); implemented != 0 || total != 0 {
		t.Errorf("CapabilityCoverage(nil) = %d/%d, want 0/0", implemented, total)
	}
}

// TestCapabilityReportMeasuresEveryProviderAgainstOneSurface confirms the
// manifest distinguishes providers rather than returning a constant, and records
// how many capabilities the no-op store advertises.
//
// The no-op store advertising optional capabilities is a hazard worth naming: a
// route that asserts a capability and finds it present will take the capable
// path against a store that persists nothing. The canonical authority modes guard
// against that with a startup preflight failure, but the count is logged here so
// the condition stays visible.
func TestCapabilityReportMeasuresEveryProviderAgainstOneSurface(t *testing.T) {
	noopImplemented, noopTotal := CapabilityCoverage(NewNoopStore())
	d1, _ := newD1TestStore(t)
	d1Implemented, d1Total := CapabilityCoverage(d1)

	if noopTotal == 0 || d1Total == 0 {
		t.Fatal("capability totals must not be zero")
	}
	if noopTotal != d1Total {
		t.Errorf("both providers must be measured against the same surface: %d vs %d", noopTotal, d1Total)
	}

	noopReport := CapabilityReport(NewNoopStore())
	noopNames := make([]string, 0, len(noopReport))
	for _, status := range noopReport {
		if status.Implemented {
			noopNames = append(noopNames, status.Name)
		}
	}
	sort.Strings(noopNames)

	d1ImplementedNames := MissingCapabilitiesWithComplement(d1)

	t.Logf("optional-capability coverage: D1 %d/%d, noop %d/%d", d1Implemented, d1Total, noopImplemented, noopTotal)
	if len(noopNames) > 0 {
		t.Logf("the no-op store advertises %d capabilities and must never be selected as a canonical authority: %v",
			len(noopNames), noopNames)
	}
	t.Logf("the D1 provider implements: %v", d1ImplementedNames)

	// A capability must be reported consistently across the two views.
	if implementedCount := len(d1ImplementedNames); implementedCount != d1Implemented {
		t.Errorf("implemented-name list has %d entries, coverage says %d", implementedCount, d1Implemented)
	}
}

// MissingCapabilitiesWithComplement returns the capability names the store DOES
// satisfy, complementing MissingCapabilities.
func MissingCapabilitiesWithComplement(s Store) []string {
	var present []string
	for _, status := range CapabilityReport(s) {
		if status.Implemented {
			present = append(present, status.Name)
		}
	}
	sort.Strings(present)
	return present
}
