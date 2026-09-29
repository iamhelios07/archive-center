package store

import (
	"context"
	"strconv"
	"testing"
)

// D1 memory admission projection inspector tests.
//
// The inspector is the proof obligation for the two-batch admission, so the test
// that matters most is the crash window: canonical rows committed, outbox
// operations lost, marker not yet written. That is the only state the marker
// placement exists to make recoverable, and this is the only thing that can say
// it happened.

// d1ExpectationFor derives the inspection expectation from a committed admission
// the way a caller would: from the committed result and the accepted raw source.
func d1ExpectationFor(
	fixture d1AdmissionFixture,
	admission *MemoryAdmission,
	artifacts ...string,
) MemoryAdmissionProjectionExpectation {
	evidenceTexts := make([]string, 0, len(admission.Evidence))
	for _, evidence := range admission.Evidence {
		if evidence != nil {
			evidenceTexts = append(evidenceTexts, evidence.EvidenceText)
		}
	}
	memoryExpected := admission.Memory != nil
	summary := ""
	if memoryExpected {
		summary = admission.Memory.SummaryJSON
	}
	return MemoryAdmissionProjectionExpectation{
		ChatSessionID:            fixture.SessionID,
		SourceRevision:           fixture.Revision,
		TurnIndex:                fixture.Turn,
		DerivationVersion:        admission.DerivationVersion,
		ExtractorVersion:         admission.ExtractorVersion,
		IndexVersion:             admission.IndexVersion,
		ResultHash:               admission.ResultHash,
		ResultJSON:               admission.ResultJSON,
		MemoryExpected:           memoryExpected,
		MemorySummaryJSON:        summary,
		EvidenceTexts:            evidenceTexts,
		PreciseUnitCount:         len(admission.PreciseUnits),
		AdmissionVectorArtifacts: artifacts,
	}
}

// TestD1MemoryAdmissionInspectorIsAdvertised pins the delivery state.
func TestD1MemoryAdmissionInspectorIsAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var store Store = st
	if _, ok := store.(MemoryAdmissionProjectionInspector); !ok {
		t.Fatal("the D1 provider must expose MemoryAdmissionProjectionInspector")
	}
	reported := false
	for _, status := range CapabilityReport(st) {
		if status.Name != "MemoryAdmissionProjectionInspector" {
			continue
		}
		reported = true
		if !status.Implemented {
			t.Error("the capability manifest reports the admission inspector as missing")
		}
	}
	if !reported {
		t.Error("MemoryAdmissionProjectionInspector is not probed by the capability manifest")
	}
}

// TestD1MemoryAdmissionInspectorConfirmsACommittedAdmission is the baseline: a
// normal admission satisfies its own expectation.
func TestD1MemoryAdmissionInspectorConfirmsACommittedAdmission(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"complete"}`)
	admission.Memory = &Memory{
		ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn,
		SummaryJSON: `{"headline":"the lantern"}`, Importance: 6,
	}
	evidence := d1AdmissionEvidence(fixture, "the lantern was lit")
	admission.Evidence = []*DirectEvidence{evidence}
	unit := d1AdmissionUnit(fixture, "u-proof", "the lantern was lit")
	admission.PreciseUnits = []*PreciseMemoryUnit{unit}
	admission.Vectors = []MemoryAdmissionVector{
		{ArtifactType: "memory", DocumentText: "aggregate"},
		{ArtifactType: "evidence", EvidenceText: "the lantern was lit", DocumentText: "the lantern was lit"},
	}

	if _, err := st.CommitMemoryAdmission(ctx, admission); err != nil {
		t.Fatalf("CommitMemoryAdmission: %v", err)
	}

	artifacts := []string{
		"memory:" + fixture.SessionID + ":" + d1TestItoa(d1TestID(t, conn, "SELECT id FROM memories LIMIT 1")),
		d1AdmissionEvidenceDocumentID(fixture.SessionID, evidence.ID),
		d1AdmissionPreciseDocumentID(fixture.SessionID, unit.UnitID),
	}
	// Each unit contributes one dependency edge for the source revision and one
	// for its evidence.
	expectation := d1ExpectationFor(fixture, admission, artifacts...)
	expectation.PreciseDependencyCount = 2

	inspection, err := st.InspectMemoryAdmissionProjection(ctx, expectation)
	if err != nil {
		t.Fatalf("InspectMemoryAdmissionProjection: %v", err)
	}
	if !inspection.Complete {
		t.Errorf("a committed admission must satisfy its own expectation: missing=%v stale=%v actual=%v",
			inspection.MissingLanes, inspection.StaleLanes, inspection.ActualCounts)
	}
	if got := inspection.ActualCounts[d1ProjectionLaneMemory]; got != 1 {
		t.Errorf("%s actual = %d, want 1", d1ProjectionLaneMemory, got)
	}
	if got := inspection.ActualCounts[d1ProjectionLaneEvidence]; got != 1 {
		t.Errorf("%s actual = %d, want 1", d1ProjectionLaneEvidence, got)
	}
	if got := inspection.ActualCounts[d1ProjectionLanePrecise]; got != 1 {
		t.Errorf("%s actual = %d, want 1", d1ProjectionLanePrecise, got)
	}
	if got := inspection.ActualCounts[d1ProjectionLaneDependency]; got != 2 {
		t.Errorf("%s actual = %d, want 2", d1ProjectionLaneDependency, got)
	}
	if got := inspection.ActualCounts[d1ProjectionLaneOutbox]; got != 3 {
		t.Errorf("%s actual = %d, want 3", d1ProjectionLaneOutbox, got)
	}
}

// TestD1MemoryAdmissionInspectorDetectsTheLostSecondBatch is the test the whole
// two-batch design rests on.
//
// It reproduces the crash window exactly: the canonical rows are committed, the
// outbox operations are gone, and the marker is back to pending. The inspector
// must report the outbox lane as missing and the inspection as incomplete, so an
// operator replay knows the projection is unfinished instead of trusting the
// canonical rows alone.
func TestD1MemoryAdmissionInspectorDetectsTheLostSecondBatch(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"partial"}`)
	admission.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn}
	evidence := d1AdmissionEvidence(fixture, "a remembered fact")
	admission.Evidence = []*DirectEvidence{evidence}
	unit := d1AdmissionUnit(fixture, "u-partial", "a remembered fact")
	admission.PreciseUnits = []*PreciseMemoryUnit{unit}
	admission.Vectors = []MemoryAdmissionVector{{ArtifactType: "memory", DocumentText: "aggregate"}}

	if _, err := st.CommitMemoryAdmission(ctx, admission); err != nil {
		t.Fatalf("CommitMemoryAdmission: %v", err)
	}

	artifacts := []string{
		"memory:" + fixture.SessionID + ":" + d1TestItoa(d1TestID(t, conn, "SELECT id FROM memories LIMIT 1")),
	}
	expectation := d1ExpectationFor(fixture, admission, artifacts...)
	expectation.PreciseDependencyCount = 2

	// Before the simulated crash the projection is complete.
	before, err := st.InspectMemoryAdmissionProjection(ctx, expectation)
	if err != nil {
		t.Fatalf("inspect before the crash: %v", err)
	}
	if !before.Complete {
		t.Fatalf("the projection must be complete before the crash: missing=%v stale=%v",
			before.MissingLanes, before.StaleLanes)
	}

	// Reproduce the crash: batch 1 survived, batch 2 did not.
	if _, err := conn.Exec(ctx, `DELETE FROM memory_vector_outbox`); err != nil {
		t.Fatalf("clear the outbox: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		UPDATE memory_source_revisions
		SET derived_admission_state = 'pending', derived_admitted_at = NULL
		WHERE source_revision = ?`, fixture.Revision); err != nil {
		t.Fatalf("reset the commit marker: %v", err)
	}

	after, err := st.InspectMemoryAdmissionProjection(ctx, expectation)
	if err != nil {
		t.Fatalf("inspect after the crash: %v", err)
	}
	if after.Complete {
		t.Error("an inspection must be incomplete once the outbox operations are lost")
	}
	if !d1LaneReported(after.MissingLanes, d1ProjectionLaneOutbox) {
		t.Errorf("missing lanes = %v, want it to name %q", after.MissingLanes, d1ProjectionLaneOutbox)
	}
	// The canonical lanes are still whole, which is exactly the point: the
	// inspector localises the loss instead of reporting a blanket failure.
	if got := after.ActualCounts[d1ProjectionLaneMemory]; got != 1 {
		t.Errorf("%s actual = %d, want 1; the canonical row survived the crash", d1ProjectionLaneMemory, got)
	}
	if got := after.ActualCounts[d1ProjectionLanePrecise]; got != 1 {
		t.Errorf("%s actual = %d, want 1", d1ProjectionLanePrecise, got)
	}
	if got := after.ActualCounts[d1ProjectionLaneOutbox]; got != 0 {
		t.Errorf("%s actual = %d, want 0", d1ProjectionLaneOutbox, got)
	}
}

// TestD1MemoryAdmissionInspectorReportsAStaleRow pins the distinction between a
// missing row and a row that is present but no longer current, because the two
// need different repairs.
func TestD1MemoryAdmissionInspectorReportsAStaleRow(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"stale"}`)
	admission.Memory = &Memory{
		ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn,
		SummaryJSON: `{"headline":"original"}`,
	}
	admission.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "a fact")}
	if _, err := st.CommitMemoryAdmission(ctx, admission); err != nil {
		t.Fatalf("CommitMemoryAdmission: %v", err)
	}
	expectation := d1ExpectationFor(fixture, admission)

	// A different summary than the committed extraction recorded: present, wrong.
	if _, err := conn.Exec(ctx, `UPDATE memories SET summary_json = ?`, `{"headline":"rewritten"}`); err != nil {
		t.Fatalf("rewrite the memory summary: %v", err)
	}
	inspection, err := st.InspectMemoryAdmissionProjection(ctx, expectation)
	if err != nil {
		t.Fatalf("InspectMemoryAdmissionProjection: %v", err)
	}
	if inspection.Complete {
		t.Fatal("a memory carrying a different summary must not read as complete")
	}
	if !d1LaneReported(inspection.StaleLanes, d1ProjectionLaneMemory) {
		t.Errorf("stale lanes = %v, want it to name %q", inspection.StaleLanes, d1ProjectionLaneMemory)
	}
	if d1LaneReported(inspection.MissingLanes, d1ProjectionLaneMemory) {
		t.Error("a stale row is present; it must not be reported as missing")
	}
}

// TestD1MemoryAdmissionInspectorFlagsALostDerivationLink pins the provenance
// rule. A precise unit that is active but whose evidence is gone survives every
// count-based check, and would be rendered to the user as a memory with no
// provenance.
func TestD1MemoryAdmissionInspectorFlagsALostDerivationLink(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"lineage"}`)
	admission.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "a fact")}
	admission.PreciseUnits = []*PreciseMemoryUnit{d1AdmissionUnit(fixture, "u-lineage", "a fact")}
	if _, err := st.CommitMemoryAdmission(ctx, admission); err != nil {
		t.Fatalf("CommitMemoryAdmission: %v", err)
	}
	expectation := d1ExpectationFor(fixture, admission)
	expectation.PreciseDependencyCount = 2

	if inspection, err := st.InspectMemoryAdmissionProjection(ctx, expectation); err != nil {
		t.Fatalf("inspect the intact projection: %v", err)
	} else if !inspection.Complete {
		t.Fatalf("the intact projection must be complete: missing=%v stale=%v",
			inspection.MissingLanes, inspection.StaleLanes)
	}

	// Break only the link: tombstone the evidence the unit was derived from,
	// leaving the unit active and counted.
	if _, err := conn.Exec(ctx, `UPDATE direct_evidence_records SET tombstoned = 1`); err != nil {
		t.Fatalf("tombstone the evidence: %v", err)
	}
	inspection, err := st.InspectMemoryAdmissionProjection(ctx, expectation)
	if err != nil {
		t.Fatalf("inspect the broken projection: %v", err)
	}
	if inspection.Complete {
		t.Error("an active unit whose evidence is tombstoned must not read as complete")
	}
	// The unit is still counted, so the loss is reported as staleness rather than
	// as a vanished row.
	if got := inspection.ActualCounts[d1ProjectionLanePrecise]; got != 1 {
		t.Errorf("%s actual = %d, want 1; the unit itself is still active", d1ProjectionLanePrecise, got)
	}
	if !d1LaneReported(inspection.StaleLanes, d1ProjectionLanePrecise) {
		t.Errorf("stale lanes = %v, want it to name %q", inspection.StaleLanes, d1ProjectionLanePrecise)
	}
}

// TestD1MemoryAdmissionInspectorTreatsZeroExpectedAsALegitimateEmptyLane pins
// the rule that makes a legitimately empty lane distinguishable from a lost one.
func TestD1MemoryAdmissionInspectorTreatsZeroExpectedAsALegitimateEmptyLane(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"no evidence"}`)
	if _, err := st.CommitMemoryAdmission(ctx, admission); err != nil {
		t.Fatalf("CommitMemoryAdmission: %v", err)
	}

	// An admission that promised nothing must inspect as complete: every lane has
	// a zero expectation and a zero count.
	inspection, err := st.InspectMemoryAdmissionProjection(ctx, d1ExpectationFor(fixture, admission))
	if err != nil {
		t.Fatalf("InspectMemoryAdmissionProjection: %v", err)
	}
	if !inspection.Complete {
		t.Errorf("an admission that produced nothing must inspect as complete: missing=%v stale=%v",
			inspection.MissingLanes, inspection.StaleLanes)
	}
	for _, lane := range d1ProjectionLaneNames() {
		if inspection.ExpectedCounts[lane] != 0 {
			t.Errorf("%s expected = %d, want 0", lane, inspection.ExpectedCounts[lane])
		}
		if inspection.ActualCounts[lane] != 0 {
			t.Errorf("%s actual = %d, want 0", lane, inspection.ActualCounts[lane])
		}
	}
}

// TestD1MemoryAdmissionInspectorRefusesAnIncompleteExpectation pins that a
// caller which forgot the turn identity gets an explicit failure rather than a
// silent all-clear. Every lane reading zero expectations would otherwise look
// complete.
func TestD1MemoryAdmissionInspectorRefusesAnIncompleteExpectation(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	for name, expectation := range map[string]MemoryAdmissionProjectionExpectation{
		"no session":     {SourceRevision: "r", TurnIndex: 1},
		"no revision":    {ChatSessionID: "s", TurnIndex: 1},
		"no turn":        {ChatSessionID: "s", SourceRevision: "r"},
		"everything":     {},
		"blank revision": {ChatSessionID: "s", SourceRevision: "   ", TurnIndex: 1},
	} {
		inspection, err := st.InspectMemoryAdmissionProjection(ctx, expectation)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if inspection.Complete {
			t.Errorf("%s: an expectation with no turn identity must not inspect as complete", name)
		}
		if !d1LaneReported(inspection.MissingLanes, "expectation_identity") {
			t.Errorf("%s: missing lanes = %v, want it to name the missing identity", name, inspection.MissingLanes)
		}
	}
}

// TestD1MemoryAdmissionInspectorDoesNotMutate pins the read-only contract. An
// inspector that "helpfully" repaired what it found would race a live admission
// and could retire a row the in-flight turn is about to write.
func TestD1MemoryAdmissionInspectorDoesNotMutate(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"read only"}`)
	admission.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "a fact")}
	admission.PreciseUnits = []*PreciseMemoryUnit{d1AdmissionUnit(fixture, "u-ro", "a fact")}
	if _, err := st.CommitMemoryAdmission(ctx, admission); err != nil {
		t.Fatalf("CommitMemoryAdmission: %v", err)
	}
	expectation := d1ExpectationFor(fixture, admission)
	expectation.PreciseDependencyCount = 2

	before := d1AdmissionCounts(t, conn)
	// Ask for something that is definitely not there, so the inspection has every
	// reason to want to act on it.
	expectation.EvidenceTexts = append(expectation.EvidenceTexts, "a fact nobody stored")
	expectation.AdmissionVectorArtifacts = []string{"memory:s1:999999"}
	expectation.PreciseUnitCount = 9

	if _, err := st.InspectMemoryAdmissionProjection(ctx, expectation); err != nil {
		t.Fatalf("InspectMemoryAdmissionProjection: %v", err)
	}
	after := d1AdmissionCounts(t, conn)
	for table, want := range before {
		if after[table] != want {
			t.Errorf("%s rows = %d after the inspection, want %d; the inspector must not write",
				table, after[table], want)
		}
	}
}

// d1LaneReported reports whether a named lane appears in a list.
func d1LaneReported(lanes []string, name string) bool {
	for _, lane := range lanes {
		if lane == name {
			return true
		}
	}
	return false
}

func d1TestID(t *testing.T, conn *sqliteD1Conn, query string) int64 {
	t.Helper()
	var id int64
	if err := conn.QueryRow(context.Background(), query).Scan(&id); err != nil {
		t.Fatalf("read the row id: %v", err)
	}
	return id
}

func d1TestItoa(v int64) string { return strconv.FormatInt(v, 10) }

func d1AdmissionCounts(t *testing.T, conn *sqliteD1Conn) map[string]int {
	t.Helper()
	out := map[string]int{}
	for table, query := range map[string]string{
		"memories":     `SELECT COUNT(*) FROM memories`,
		"evidence":     `SELECT COUNT(*) FROM direct_evidence_records`,
		"units":        `SELECT COUNT(*) FROM precise_memory_units`,
		"dependencies": `SELECT COUNT(*) FROM memory_derivation_dependencies`,
		"outbox":       `SELECT COUNT(*) FROM memory_vector_outbox`,
	} {
		out[table] = d1Count(t, conn, query)
	}
	return out
}
