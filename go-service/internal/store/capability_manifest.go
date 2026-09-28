package store

// D1 capability manifest.
//
// Cloudflare parity is not only the base Store contract: HTTP routes gate individual
// features on optional capability interfaces through type assertions. A capability the
// D1 provider does not implement therefore silently disables the route that needs it.
//
// d1CapabilityProbes lists every optional interface the HTTP layer asserts on the
// selected store. Each probe performs the same assertion the route performs, so the
// manifest is computed from the implementation itself and cannot drift from it. A probe
// that names a removed interface fails to compile.

// CapabilityStatus reports whether a store satisfies one optional capability.
type CapabilityStatus struct {
	// Name is the optional interface name.
	Name string
	// Implemented reports whether the examined store satisfies the capability.
	Implemented bool
}

type capabilityProbe struct {
	name  string
	probe func(Store) bool
}

// d1CapabilityProbes enumerates the optional capability surface the HTTP layer asserts.
var d1CapabilityProbes = []capabilityProbe{
	{name: "ActiveInteractionMemoryReader", probe: func(s Store) bool { _, ok := s.(ActiveInteractionMemoryReader); return ok }},
	{name: "ActiveScopeStore", probe: func(s Store) bool { _, ok := s.(ActiveScopeStore); return ok }},
	{name: "ActiveSourceRevisionLister", probe: func(s Store) bool { _, ok := s.(ActiveSourceRevisionLister); return ok }},
	{name: "AdminResetStore", probe: func(s Store) bool { _, ok := s.(AdminResetStore); return ok }},
	{name: "AdminJobSnapshotStore", probe: func(s Store) bool { _, ok := s.(AdminJobSnapshotStore); return ok }},
	{name: "AdminResetRunReader", probe: func(s Store) bool { _, ok := s.(AdminResetRunReader); return ok }},
	{name: "ArcSummaryStore", probe: func(s Store) bool { _, ok := s.(ArcSummaryStore); return ok }},
	{name: "AuditLogCounter", probe: func(s Store) bool { _, ok := s.(AuditLogCounter); return ok }},
	{name: "CanonPackStore", probe: func(s Store) bool { _, ok := s.(CanonPackStore); return ok }},
	{name: "CanonRegistryStore", probe: func(s Store) bool { _, ok := s.(CanonRegistryStore); return ok }},
	{name: "CaptureVerificationStore", probe: func(s Store) bool { _, ok := s.(CaptureVerificationStore); return ok }},
	{name: "ChapterSummaryStore", probe: func(s Store) bool { _, ok := s.(ChapterSummaryStore); return ok }},
	{name: "CharacterPerspectiveMemoryReader", probe: func(s Store) bool { _, ok := s.(CharacterPerspectiveMemoryReader); return ok }},
	{name: "CharacterProvenanceRepairStore", probe: func(s Store) bool { _, ok := s.(CharacterProvenanceRepairStore); return ok }},
	{name: "CharacterStateHistoryStore", probe: func(s Store) bool { _, ok := s.(CharacterStateHistoryStore); return ok }},
	{name: "ConsequenceRecordStore", probe: func(s Store) bool { _, ok := s.(ConsequenceRecordStore); return ok }},
	{name: "CriticInputSnapshotStore", probe: func(s Store) bool { _, ok := s.(CriticInputSnapshotStore); return ok }},
	{name: "EffectiveInputListStore", probe: func(s Store) bool { _, ok := s.(EffectiveInputListStore); return ok }},
	{name: "EntityIdentityCatalogReader", probe: func(s Store) bool { _, ok := s.(EntityIdentityCatalogReader); return ok }},
	{name: "EntityIdentityLinkWriter", probe: func(s Store) bool { _, ok := s.(EntityIdentityLinkWriter); return ok }},
	{name: "EntityIdentityWriteAvailability", probe: func(s Store) bool { _, ok := s.(EntityIdentityWriteAvailability); return ok }},
	{name: "EntityIdentityWriter", probe: func(s Store) bool { _, ok := s.(EntityIdentityWriter); return ok }},
	{name: "EpisodeSummaryStore", probe: func(s Store) bool { _, ok := s.(EpisodeSummaryStore); return ok }},
	{name: "ExplorerMutationStore", probe: func(s Store) bool { _, ok := s.(ExplorerMutationStore); return ok }},
	{name: "ForkLineageStore", probe: func(s Store) bool { _, ok := s.(ForkLineageStore); return ok }},
	{name: "GeneralVectorPreciseMemoryReader", probe: func(s Store) bool { _, ok := s.(GeneralVectorPreciseMemoryReader); return ok }},
	{name: "GuidancePlanStateStore", probe: func(s Store) bool { _, ok := s.(GuidancePlanStateStore); return ok }},
	{name: "LogicalTurnReplacementStore", probe: func(s Store) bool { _, ok := s.(LogicalTurnReplacementStore); return ok }},
	{name: "LorebookReferenceExplorerStore", probe: func(s Store) bool { _, ok := s.(LorebookReferenceExplorerStore); return ok }},
	{name: "LorebookReferenceStore", probe: func(s Store) bool { _, ok := s.(LorebookReferenceStore); return ok }},
	{name: "MemoryAdmissionWriteAvailability", probe: func(s Store) bool { _, ok := s.(MemoryAdmissionWriteAvailability); return ok }},
	{name: "MemoryAdmissionWriter", probe: func(s Store) bool { _, ok := s.(MemoryAdmissionWriter); return ok }},
	{name: "MemoryDerivationLifecycleAvailability", probe: func(s Store) bool { _, ok := s.(MemoryDerivationLifecycleAvailability); return ok }},
	{name: "MemoryReprocessingJobReopener", probe: func(s Store) bool { _, ok := s.(MemoryReprocessingJobReopener); return ok }},
	{name: "MemoryReprocessingJobStore", probe: func(s Store) bool { _, ok := s.(MemoryReprocessingJobStore); return ok }},
	{name: "MemoryReprocessingWakeScheduleStore", probe: func(s Store) bool { _, ok := s.(MemoryReprocessingWakeScheduleStore); return ok }},
	{name: "MemoryVectorOutboxLaneStore", probe: func(s Store) bool { _, ok := s.(MemoryVectorOutboxLaneStore); return ok }},
	{name: "MemoryVectorOutboxMaintenanceStore", probe: func(s Store) bool { _, ok := s.(MemoryVectorOutboxMaintenanceStore); return ok }},
	{name: "MemoryAdmissionProjectionInspector", probe: func(s Store) bool { _, ok := s.(MemoryAdmissionProjectionInspector); return ok }},
	{name: "MemoryVectorMaterializedCompletionStore", probe: func(s Store) bool { _, ok := s.(MemoryVectorMaterializedCompletionStore); return ok }},
	{name: "MemoryVectorOutboxStore", probe: func(s Store) bool { _, ok := s.(MemoryVectorOutboxStore); return ok }},
	{name: "PersonaCapsuleStore", probe: func(s Store) bool { _, ok := s.(PersonaCapsuleStore); return ok }},
	{name: "PreciseMemoryWriteAvailability", probe: func(s Store) bool { _, ok := s.(PreciseMemoryWriteAvailability); return ok }},
	{name: "PreciseMemoryWriter", probe: func(s Store) bool { _, ok := s.(PreciseMemoryWriter); return ok }},
	{name: "PrepareTurnRangeStore", probe: func(s Store) bool { _, ok := s.(PrepareTurnRangeStore); return ok }},
	{name: "ProtagonistEntityMemoryManagementStore", probe: func(s Store) bool { _, ok := s.(ProtagonistEntityMemoryManagementStore); return ok }},
	{name: "ProtagonistEntityMemoryOwnerIndexStore", probe: func(s Store) bool { _, ok := s.(ProtagonistEntityMemoryOwnerIndexStore); return ok }},
	{name: "ProtagonistEntityMemoryRepairStore", probe: func(s Store) bool { _, ok := s.(ProtagonistEntityMemoryRepairStore); return ok }},
	{name: "ProtagonistEntityMemoryStore", probe: func(s Store) bool { _, ok := s.(ProtagonistEntityMemoryStore); return ok }},
	{name: "PsychologyBranchStore", probe: func(s Store) bool { _, ok := s.(PsychologyBranchStore); return ok }},
	{name: "ReferenceCoverageStore", probe: func(s Store) bool { _, ok := s.(ReferenceCoverageStore); return ok }},
	{name: "ReferenceLibraryStore", probe: func(s Store) bool { _, ok := s.(ReferenceLibraryStore); return ok }},
	{name: "ReversibleStatusTransitionStore", probe: func(s Store) bool { _, ok := s.(ReversibleStatusTransitionStore); return ok }},
	{name: "ReviewedEntityIdentityResolver", probe: func(s Store) bool { _, ok := s.(ReviewedEntityIdentityResolver); return ok }},
	{name: "RollbackStore", probe: func(s Store) bool { _, ok := s.(RollbackStore); return ok }},
	{name: "SagaDigestStore", probe: func(s Store) bool { _, ok := s.(SagaDigestStore); return ok }},
	{name: "SessionMigrationRecoveryStore", probe: func(s Store) bool { _, ok := s.(SessionMigrationRecoveryStore); return ok }},
	{name: "SessionMigrationSourceLockFenceStore", probe: func(s Store) bool { _, ok := s.(SessionMigrationSourceLockFenceStore); return ok }},
	{name: "SessionMigrationSourceLockStore", probe: func(s Store) bool { _, ok := s.(SessionMigrationSourceLockStore); return ok }},
	{name: "SessionMigrationStore", probe: func(s Store) bool { _, ok := s.(SessionMigrationStore); return ok }},
	{name: "SessionMigrationVectorParityStore", probe: func(s Store) bool { _, ok := s.(SessionMigrationVectorParityStore); return ok }},
	{name: "SessionMigrationVectorStore", probe: func(s Store) bool { _, ok := s.(SessionMigrationVectorStore); return ok }},
	{name: "SessionRouteBindingStore", probe: func(s Store) bool { _, ok := s.(SessionRouteBindingStore); return ok }},
	{name: "SessionRoutingBaselineStore", probe: func(s Store) bool { _, ok := s.(SessionRoutingBaselineStore); return ok }},
	{name: "SessionStateSnapshotReader", probe: func(s Store) bool { _, ok := s.(SessionStateSnapshotReader); return ok }},
	{name: "SessionStitchStore", probe: func(s Store) bool { _, ok := s.(SessionStitchStore); return ok }},
	{name: "ShadowStatusReporter", probe: func(s Store) bool { _, ok := s.(ShadowStatusReporter); return ok }},
	{name: "SourceDiscoveryMutableStore", probe: func(s Store) bool { _, ok := s.(SourceDiscoveryMutableStore); return ok }},
	{name: "SourceDiscoveryQueryStore", probe: func(s Store) bool { _, ok := s.(SourceDiscoveryQueryStore); return ok }},
	{name: "SourceDiscoveryStore", probe: func(s Store) bool { _, ok := s.(SourceDiscoveryStore); return ok }},
	{name: "SourceRevisionHistoryLister", probe: func(s Store) bool { _, ok := s.(SourceRevisionHistoryLister); return ok }},
	{name: "SourceRevisionStore", probe: func(s Store) bool { _, ok := s.(SourceRevisionStore); return ok }},
	{name: "StateRepairArtifactReader", probe: func(s Store) bool { _, ok := s.(StateRepairArtifactReader); return ok }},
	{name: "StatusChangeEventSourceLookupStore", probe: func(s Store) bool { _, ok := s.(StatusChangeEventSourceLookupStore); return ok }},
	{name: "StatusCurrentValueStore", probe: func(s Store) bool { _, ok := s.(StatusCurrentValueStore); return ok }},
	{name: "StatusLifecycleStore", probe: func(s Store) bool { _, ok := s.(StatusLifecycleStore); return ok }},
	{name: "StatusSchemaProposalStore", probe: func(s Store) bool { _, ok := s.(StatusSchemaProposalStore); return ok }},
	{name: "StatusSchemaRegistryStore", probe: func(s Store) bool { _, ok := s.(StatusSchemaRegistryStore); return ok }},
	{name: "SupersessionResolutionStore", probe: func(s Store) bool { _, ok := s.(SupersessionResolutionStore); return ok }},
	{name: "ThemeOffscreenCarryStore", probe: func(s Store) bool { _, ok := s.(ThemeOffscreenCarryStore); return ok }},
	{name: "TimelineTurnIndexStore", probe: func(s Store) bool { _, ok := s.(TimelineTurnIndexStore); return ok }},
	{name: "TurnPreparationSettingsStore", probe: func(s Store) bool { _, ok := s.(TurnPreparationSettingsStore); return ok }},
	{name: "UniqueActiveEntitySurfaceIdentityResolver", probe: func(s Store) bool { _, ok := s.(UniqueActiveEntitySurfaceIdentityResolver); return ok }},
	{name: "UniqueActiveEntitySurfaceResolver", probe: func(s Store) bool { _, ok := s.(UniqueActiveEntitySurfaceResolver); return ok }},
	{name: "VectorRecoveryCacheReader", probe: func(s Store) bool { _, ok := s.(VectorRecoveryCacheReader); return ok }},
	{name: "WorldlineTopologySnapshotStore", probe: func(s Store) bool { _, ok := s.(WorldlineTopologySnapshotStore); return ok }},
}

// CapabilityReport returns the optional-capability coverage of a store, in a stable
// order. It is the machine-readable statement of which Cloudflare-parity capabilities
// are present, so a parity gap is observable instead of inferred from a disabled route.
func CapabilityReport(s Store) []CapabilityStatus {
	if s == nil {
		return nil
	}
	out := make([]CapabilityStatus, 0, len(d1CapabilityProbes))
	for _, p := range d1CapabilityProbes {
		out = append(out, CapabilityStatus{Name: p.name, Implemented: p.probe(s)})
	}
	return out
}

// MissingCapabilities returns the capability names a store does not satisfy.
func MissingCapabilities(s Store) []string {
	var missing []string
	for _, status := range CapabilityReport(s) {
		if !status.Implemented {
			missing = append(missing, status.Name)
		}
	}
	return missing
}

// CapabilityExemption declares a capability that a deployment profile cannot have,
// as opposed to one it has not implemented yet.
//
// The distinction matters because the coverage number is a parity signal, and a
// permanently inapplicable capability makes that signal permanently wrong: the
// count can never reach N/N, so an operator cannot tell "this profile is
// complete" from "this profile is one short and always will be". Worse, the
// number alone does not say WHICH capability, so the reader cannot work out that
// the missing one does not apply.
//
// An exemption is therefore a deliberate, named claim with a reason attached, not
// a place to hide a gap. Without a reason it would be indistinguishable from
// quietly dropping an inconvenient entry.
type CapabilityExemption struct {
	Profile    string
	Capability string
	Reason     string
}

// capabilityExemptions lists the capabilities a profile cannot satisfy.
//
// There is exactly one, and it is a statement about the topology rather than a
// deferral: ShadowStatusReporter reports how many dual writes to a shadow failed,
// and only the MariaDB shadow store implements it. The Cloudflare profile does not
// use MariaDB at all, so there is no shadow to report on. Counting it as missing
// would tell a Cloudflare operator that one capability of the parity surface is
// unimplemented, which is not true and cannot be fixed.
var capabilityExemptions = []CapabilityExemption{
	{
		Profile:    "cloudflare",
		Capability: "ShadowStatusReporter",
		Reason:     "there is no MariaDB shadow to report dual-write failures for on this profile",
	},
}

// CapabilityExemptionsFor returns the exemptions that apply to a profile.
func CapabilityExemptionsFor(profile string) []CapabilityExemption {
	out := make([]CapabilityExemption, 0, 1)
	for _, exemption := range capabilityExemptions {
		if exemption.Profile == profile {
			out = append(out, exemption)
		}
	}
	return out
}

// CapabilityCoverageForProfile reports coverage with the profile's inapplicable
// capabilities removed from both the numerator and the denominator.
//
// It does NOT relax what "implemented" means: a capability that is applicable and
// absent is still counted as absent. Only a declared exemption moves, and every
// exemption carries a reason that the caller can put in front of an operator.
func CapabilityCoverageForProfile(s Store, profile string) (implemented, applicable, total int) {
	exempt := map[string]bool{}
	for _, exemption := range CapabilityExemptionsFor(profile) {
		exempt[exemption.Capability] = true
	}
	for _, status := range CapabilityReport(s) {
		if exempt[status.Name] {
			continue
		}
		applicable++
		if status.Implemented {
			implemented++
		}
	}
	return implemented, applicable, len(CapabilityReport(s))
}

// MissingApplicableCapabilities returns the capabilities that are absent AND
// applicable, which is the list an operator can act on.
func MissingApplicableCapabilities(s Store, profile string) []string {
	exempt := map[string]bool{}
	for _, exemption := range CapabilityExemptionsFor(profile) {
		exempt[exemption.Capability] = true
	}
	var missing []string
	for _, name := range MissingCapabilities(s) {
		if !exempt[name] {
			missing = append(missing, name)
		}
	}
	return missing
}

// CapabilityCoverage returns the implemented and total optional-capability counts
// for a store. It exists so parity progress is a number that can be reported and
// asserted instead of an impression, and so an incomplete provider cannot be
// presented as feature-complete.
func CapabilityCoverage(s Store) (implemented, total int) {
	report := CapabilityReport(s)
	for _, status := range report {
		if status.Implemented {
			implemented++
		}
	}
	return implemented, len(report)
}

// CapabilityNames returns every optional capability name in manifest order.
func CapabilityNames() []string {
	out := make([]string, 0, len(d1CapabilityProbes))
	for _, p := range d1CapabilityProbes {
		out = append(out, p.name)
	}
	return out
}
