package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

// D1 canonical memory admission tests.
//
// These run against the real SQLite engine through the D1 transport with foreign
// keys enforced, because the admission's correctness is a property of the SQL it
// sends, not of the Go that assembles it.
//
// Two things are worth more attention here than elsewhere in this provider:
//
//   - The precise unit's root_evidence_id is written by a subquery that runs
//     inside the batch, after the evidence insert it depends on. If that
//     ordering or that subquery is wrong, the unit is silently linked to nothing
//     and the admission still reports success. So the tests assert the RESOLVED
//     id, not merely that a row exists.
//   - The commit is split across two batches with the commit marker in the
//     second. So the tests assert what a crash between the batches leaves
//     behind, and that re-running the same admission heals it.

// d1AdmissionFixture is a session with one accepted source revision, ready to be
// admitted.
type d1AdmissionFixture struct {
	SessionID string
	Revision  string
	Turn      int
}

func d1SeedAdmissionSource(t *testing.T, conn *sqliteD1Conn, sessionID, revision string, turn int, lifecycle string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO memory_source_revisions (
		source_revision, chat_session_id, logical_turn_id, turn_index,
		raw_user_content, raw_assistant_content, combined_content_hash,
		hash_algorithm, host_observed_at_ms, lifecycle_state
	) VALUES (?, ?, ?, ?, 'u', 'a', ?, 'sha256', 1, ?)`,
		revision, sessionID, "logical-"+revision, turn, "hash-"+revision, lifecycle); err != nil {
		t.Fatalf("seed source revision %s: %v", revision, err)
	}
}

func d1NewAdmissionFixture(t *testing.T, conn *sqliteD1Conn) d1AdmissionFixture {
	t.Helper()
	fixture := d1AdmissionFixture{SessionID: "s1", Revision: "s1-rev-1", Turn: 1}
	d1SeedAdmissionSource(t, conn, fixture.SessionID, fixture.Revision, fixture.Turn, "active")
	return fixture
}

// d1SupersedeAdmissionSource replaces the fixture's revision with a new one on
// the same turn, and returns the new fixture.
//
// This is how the evidence reconcile is reached in reality. The marker check
// keys idempotency on the VERSION TRIPLE, not the extraction, so a second
// admission for the same source revision is a replay by definition and never
// reconciles. A turn that is edited produces a new source revision on the same
// turn, and that is what makes the previous extraction's evidence retirable.
//
// The schema allows only one active revision per logical turn, so the old one is
// superseded as the new one is accepted.
func d1SupersedeAdmissionSource(t *testing.T, conn *sqliteD1Conn, fixture d1AdmissionFixture, newRevision string) d1AdmissionFixture {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `
		UPDATE memory_source_revisions
		SET lifecycle_state = 'superseded', superseded_by_revision = ?
		WHERE source_revision = ?`, newRevision, fixture.Revision); err != nil {
		t.Fatalf("supersede source revision: %v", err)
	}
	d1SeedAdmissionSource(t, conn, fixture.SessionID, newRevision, fixture.Turn, "active")
	return d1AdmissionFixture{SessionID: fixture.SessionID, Revision: newRevision, Turn: fixture.Turn}
}

// d1Admission builds a valid admission. The result hash is computed with the
// provider's own function, because the admission refuses any hash that does not
// match — a test that hand-wrote one would be testing the wrong thing.
func d1Admission(fixture d1AdmissionFixture, extraction string) *MemoryAdmission {
	admission := &MemoryAdmission{
		ContractVersion:   MemoryAdmissionContract,
		ChatSessionID:     fixture.SessionID,
		SourceRevision:    fixture.Revision,
		TurnIndex:         fixture.Turn,
		DerivationVersion: "derivation.v1",
		ExtractorVersion:  "extractor.v1",
		IndexVersion:      "index.v1",
		ResultJSON:        extraction,
		CreatedAt:         time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
	}
	admission.ResultHash = memoryAdmissionExpectedResultHash(admission)
	return admission
}

// d1AdmissionEvidence builds one evidence row for the fixture's turn.
func d1AdmissionEvidence(fixture d1AdmissionFixture, text string) *DirectEvidence {
	return &DirectEvidence{
		ChatSessionID:       fixture.SessionID,
		EvidenceKind:        "fact_event",
		EvidenceText:        text,
		SourceTurnStart:     fixture.Turn,
		SourceTurnEnd:       fixture.Turn,
		ArchiveState:        "captured",
		CaptureStage:        "critic_extract",
		CaptureVerification: "verified",
		CreatedAt:           time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
	}
}

// d1AdmissionUnit builds one precise unit derived from an evidence excerpt.
func d1AdmissionUnit(fixture d1AdmissionFixture, unitID, excerpt string) *PreciseMemoryUnit {
	return &PreciseMemoryUnit{
		UnitID:                unitID,
		ContractVersion:       PreciseMemoryUnitContract,
		ChatSessionID:         fixture.SessionID,
		SourceTurnStart:       fixture.Turn,
		SourceTurnEnd:         fixture.Turn,
		SourceContract:        "source_acceptance_observation.v1",
		SourceRevision:        fixture.Revision,
		SourceContentHash:     "content-" + unitID,
		SourceRole:            "combined_turn_pair",
		SourceSpanStart:       0,
		SourceSpanEnd:         1,
		EvidenceExcerpt:       excerpt,
		EvidenceHash:          "eh-" + unitID,
		DirectEvidenceIDsJSON: "[]",
		Kind:                  "event",
		PayloadJSON:           `{"summary":"` + unitID + ` happened"}`,
		TruthScope:            "objective",
		EpistemicMode:         "direct",
		AuthorityClass:        "objective_world_state",
		AdmissionState:        "committed",
		ReviewState:           "source_observed",
		Visibility:            "public",
		Confidence:            0.9,
		IdempotencyKey:        "ik-" + unitID,
		LifecycleState:        "active",
		DerivationVersion:     "derivation.v1",
		ExtractorVersion:      "extractor.v1",
		IndexVersion:          "index.v1",
		CreatedAt:             time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		UpdatedAt:             time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
	}
}

func d1AdmissionCount(t *testing.T, conn *sqliteD1Conn, query string, args ...any) int {
	t.Helper()
	return d1Count(t, conn, query, args...)
}

// ---------------------------------------------------------------------------
// delivery state
// ---------------------------------------------------------------------------

// TestD1MemoryAdmissionCapabilitiesAreAdvertised pins the delivery state.
func TestD1MemoryAdmissionCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var store Store = st
	if _, ok := store.(MemoryAdmissionWriter); !ok {
		t.Fatal("the D1 provider must expose MemoryAdmissionWriter")
	}
	for _, capability := range []string{"MemoryAdmissionWriter", "MemoryAdmissionWriteAvailability"} {
		reported := false
		for _, status := range CapabilityReport(st) {
			if status.Name != capability {
				continue
			}
			reported = true
			if !status.Implemented {
				t.Errorf("the capability manifest reports %s as missing", capability)
			}
		}
		if !reported {
			t.Errorf("%s is not probed by the capability manifest", capability)
		}
	}
	if !st.MemoryAdmissionWritesEnabled() {
		t.Error("a D1 store with a live connection must report admission writes as enabled")
	}
	var nilStore *d1Store
	if nilStore.MemoryAdmissionWritesEnabled() {
		t.Error("a D1 store with no connection must not claim it can admit memory")
	}
}

// ---------------------------------------------------------------------------
// fence and idempotency
// ---------------------------------------------------------------------------

// TestD1CommitMemoryAdmissionFencesOnInactiveSource pins the fence, and pins
// that a refused admission leaves the source exactly as it was.
//
// The staging path is the trap here: a failure normally records the Critic
// result so a later worker can retry it. A stale source must NOT be staged,
// because there is nothing left to project onto, and staging would flip a rolled
// back revision back to pending.
func TestD1CommitMemoryAdmissionFencesOnInactiveSource(t *testing.T) {
	for _, lifecycle := range []string{"superseded", "invalidated", "deleted"} {
		t.Run(lifecycle, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			d1SeedAdmissionSource(t, conn, "s1", "s1-rev-1", 1, lifecycle)
			fixture := d1AdmissionFixture{SessionID: "s1", Revision: "s1-rev-1", Turn: 1}

			admission := d1Admission(fixture, `{"summary":"stale"}`)
			admission.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "a fact")}

			result, err := st.CommitMemoryAdmission(context.Background(), admission)
			if !errors.Is(err, ErrSourceRevisionStale) {
				t.Errorf("error = %v, want ErrSourceRevisionStale", err)
			}
			if result.CommittedResultHash != "" {
				t.Errorf("a refused admission reported a committed hash %q", result.CommittedResultHash)
			}
			if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memories`); got != 0 {
				t.Errorf("memory rows = %d, want 0", got)
			}
			if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM direct_evidence_records`); got != 0 {
				t.Errorf("evidence rows = %d, want 0", got)
			}
			if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != 0 {
				t.Errorf("outbox rows = %d, want 0", got)
			}
			// The marker must be untouched: still not committed, and NOT restaged.
			var admissionState string
			if err := conn.QueryRow(context.Background(),
				`SELECT derived_admission_state FROM memory_source_revisions WHERE source_revision = ?`,
				fixture.Revision).Scan(&admissionState); err != nil {
				t.Fatalf("read the source marker: %v", err)
			}
			if admissionState != "pending" {
				t.Errorf("derived_admission_state = %q, want it left at %q", admissionState, "pending")
			}
		})
	}
}

// TestD1CommitMemoryAdmissionRejectsUnknownSource pins the other fence: a source
// revision that was never accepted.
func TestD1CommitMemoryAdmissionRejectsUnknownSource(t *testing.T) {
	st, _ := newD1TestStore(t)
	fixture := d1AdmissionFixture{SessionID: "s1", Revision: "s1-rev-never", Turn: 1}
	admission := d1Admission(fixture, `{"summary":"orphan"}`)

	if _, err := st.CommitMemoryAdmission(context.Background(), admission); !errors.Is(err, ErrSourceRevisionStale) {
		t.Errorf("error = %v, want ErrSourceRevisionStale", err)
	}
}

// TestD1CommitMemoryAdmissionRejectsInvalidInput pins the argument contract. The
// result hash in particular is checked against the extraction, so an admission
// whose stored result does not match its projections is refused before anything
// is written.
func TestD1CommitMemoryAdmissionRejectsInvalidInput(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	if _, err := st.CommitMemoryAdmission(ctx, nil); err == nil {
		t.Error("a nil admission must be rejected")
	}

	// A result hash that does not match the extraction would let a caller store
	// projections that belong to a different extraction.
	mismatched := d1Admission(fixture, `{"summary":"one"}`)
	mismatched.ResultHash = strings.Repeat("a", 64)
	if _, err := st.CommitMemoryAdmission(ctx, mismatched); err == nil ||
		!strings.Contains(err.Error(), "result hash mismatch") {
		t.Errorf("error = %v, want a result hash mismatch", err)
	}

	// A memory that claims a different turn than the source would put the
	// aggregate somewhere the source cannot find it.
	wrongTurn := d1Admission(fixture, `{"summary":"two"}`)
	wrongTurn.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn + 5}
	if _, err := st.CommitMemoryAdmission(ctx, wrongTurn); err == nil ||
		!strings.Contains(err.Error(), "projection identity mismatch") {
		t.Errorf("error = %v, want a projection identity mismatch", err)
	}

	if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memories`); got != 0 {
		t.Errorf("memory rows = %d, want 0 after three rejected admissions", got)
	}
}

// TestD1CommitMemoryAdmissionReplaysIdempotently pins the replay contract.
//
// A retried turn must report the extraction that actually won and write nothing.
// The stored evidence ids are checked too, because the caller returns them to
// the client as the result of the admitted turn.
func TestD1CommitMemoryAdmissionReplaysIdempotently(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"the lantern"}`)
	admission.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "the lantern was lit")}
	admission.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn, Importance: 7}

	first, err := st.CommitMemoryAdmission(ctx, admission)
	if err != nil {
		t.Fatalf("first CommitMemoryAdmission: %v", err)
	}
	if first.Idempotent {
		t.Error("a first admission must not report an idempotent replay")
	}
	if first.CommittedResultHash != admission.ResultHash {
		t.Errorf("committed hash = %q, want %q", first.CommittedResultHash, admission.ResultHash)
	}
	if !first.MemoryInserted || first.EvidenceInserted != 1 {
		t.Errorf("first admission reported memory inserted=%v, evidence inserted=%d; want true, 1",
			first.MemoryInserted, first.EvidenceInserted)
	}
	if admission.Evidence[0].ID == 0 {
		t.Error("the assigned evidence id must be written back for the caller")
	}
	if admission.Memory.ID == 0 {
		t.Error("the assigned memory id must be written back for the caller")
	}

	counts := map[string]int{
		"memories":      d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memories`),
		"evidence":      d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM direct_evidence_records`),
		"outbox":        d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`),
		"dependencies":  d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memory_derivation_dependencies`),
		"precise_units": d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM precise_memory_units`),
	}

	replay := d1Admission(fixture, `{"summary":"the lantern"}`)
	replay.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "the lantern was lit")}
	replay.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn, Importance: 7}

	second, err := st.CommitMemoryAdmission(ctx, replay)
	if err != nil {
		t.Fatalf("replayed CommitMemoryAdmission: %v", err)
	}
	if !second.Idempotent {
		t.Fatal("a replay of a committed admission must report Idempotent")
	}
	if second.ExistingResultHash != admission.ResultHash {
		t.Errorf("existing hash = %q, want the committed %q", second.ExistingResultHash, admission.ResultHash)
	}
	if second.CommittedResultHash != admission.ResultHash {
		t.Errorf("committed hash = %q, want %q", second.CommittedResultHash, admission.ResultHash)
	}
	// Nothing at all was rewritten, including the vectors.
	for table, want := range counts {
		query := map[string]string{
			"memories":      `SELECT COUNT(*) FROM memories`,
			"evidence":      `SELECT COUNT(*) FROM direct_evidence_records`,
			"outbox":        `SELECT COUNT(*) FROM memory_vector_outbox`,
			"dependencies":  `SELECT COUNT(*) FROM memory_derivation_dependencies`,
			"precise_units": `SELECT COUNT(*) FROM precise_memory_units`,
		}[table]
		if got := d1AdmissionCount(t, conn, query); got != want {
			t.Errorf("%s rows after the replay = %d, want %d", table, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// the first commit
// ---------------------------------------------------------------------------

// TestD1CommitMemoryAdmissionLinksUnitsToCommittedEvidence is the test that the
// whole subquery design exists for.
//
// A precise unit's root_evidence_id is written by a scalar subquery evaluated
// inside batch 1, after the evidence insert. If the subquery is wrong, the
// binding is wrong, or the ordering is wrong, the unit is committed with no
// evidence link — and the admission still reports success, because nothing about
// the return value would reveal it. So the test reads the resolved id back and
// requires it to be the evidence row the admission actually inserted.
func TestD1CommitMemoryAdmissionLinksUnitsToCommittedEvidence(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"two facts"}`)
	first := d1AdmissionEvidence(fixture, "the lantern was lit")
	second := d1AdmissionEvidence(fixture, "the bridge was still wet")
	admission.Evidence = []*DirectEvidence{first, second}
	admission.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn}
	unitA := d1AdmissionUnit(fixture, "u-a", "the lantern was lit")
	unitB := d1AdmissionUnit(fixture, "u-b", "the bridge was still wet")
	admission.PreciseUnits = []*PreciseMemoryUnit{unitA, unitB}

	result, err := st.CommitMemoryAdmission(ctx, admission)
	if err != nil {
		t.Fatalf("CommitMemoryAdmission: %v", err)
	}
	if result.PreciseInserted != 2 {
		t.Errorf("precise inserted = %d, want 2", result.PreciseInserted)
	}

	// Each unit must point at the evidence matching its own excerpt, not at the
	// first row inserted.
	for _, tc := range []struct {
		unitID  string
		wantRef int64
	}{{unitA.UnitID, first.ID}, {unitB.UnitID, second.ID}} {
		var rootID int64
		var idsJSON string
		if err := conn.QueryRow(ctx,
			`SELECT root_evidence_id, direct_evidence_ids_json FROM precise_memory_units WHERE unit_id = ?`,
			tc.unitID).Scan(&rootID, &idsJSON); err != nil {
			t.Fatalf("read unit %s: %v", tc.unitID, err)
		}
		if rootID != tc.wantRef {
			t.Errorf("unit %s root_evidence_id = %d, want the evidence row %d", tc.unitID, rootID, tc.wantRef)
		}
		var decoded []int64
		if err := json.Unmarshal([]byte(idsJSON), &decoded); err != nil {
			t.Errorf("unit %s direct_evidence_ids_json = %q, which is not a JSON array: %v", tc.unitID, idsJSON, err)
		}
		if len(decoded) != 1 || decoded[0] != tc.wantRef {
			t.Errorf("unit %s direct_evidence_ids_json = %v, want [%d]", tc.unitID, decoded, tc.wantRef)
		}
	}

	// The dependency edges must agree: one from the source revision, one from the
	// evidence the unit was derived from.
	for _, unit := range []*PreciseMemoryUnit{unitA, unitB} {
		if got := d1AdmissionCount(t, conn, `
			SELECT COUNT(*) FROM memory_derivation_dependencies
			WHERE child_artifact_type = 'precise_memory_unit' AND child_artifact_id = ?`,
			unit.UnitID); got != 2 {
			t.Errorf("unit %s dependency rows = %d, want 2 (source revision and evidence)", unit.UnitID, got)
		}
	}
}

// TestD1CommitMemoryAdmissionQueuesVectorsForEveryArtifact pins the outbox half
// of batch 2: the aggregate memory, each evidence row, and each eligible precise
// unit all get a document, and the marker commits last.
func TestD1CommitMemoryAdmissionQueuesVectorsForEveryArtifact(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"vector work"}`)
	admission.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn}
	evidence := d1AdmissionEvidence(fixture, "the lantern was lit")
	admission.Evidence = []*DirectEvidence{evidence}
	unit := d1AdmissionUnit(fixture, "u-v", "the lantern was lit")
	unit.VectorEmbedding = []float32{0.1, 0.2}
	admission.PreciseUnits = []*PreciseMemoryUnit{unit}
	// The aggregate artifacts are enqueued only for the vectors the caller asked
	// for. Without this list the memory and the evidence would correctly get no
	// document, because "not requested" is not "deliberately excluded".
	admission.Vectors = []MemoryAdmissionVector{
		{ArtifactType: "memory", DocumentText: "the turn's aggregate", Embedding: []float32{0.3}},
		{ArtifactType: "evidence", EvidenceText: "the lantern was lit", DocumentText: "the lantern was lit"},
	}

	result, err := st.CommitMemoryAdmission(ctx, admission)
	if err != nil {
		t.Fatalf("CommitMemoryAdmission: %v", err)
	}

	// One document for the memory, one for the evidence, one for the precise
	// unit. All three are upserts.
	if got := d1AdmissionCount(t, conn,
		`SELECT COUNT(*) FROM memory_vector_outbox WHERE operation = 'upsert'`); got != 3 {
		t.Errorf("upsert rows = %d, want 3", got)
	}
	wantMemoryID := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memories`)
	if wantMemoryID != 1 {
		t.Fatalf("memory rows = %d, want 1", wantMemoryID)
	}
	var memoryDocID int
	if err := conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM memory_vector_outbox
		WHERE document_id = 'memory:' || ? || ':' || (SELECT id FROM memories LIMIT 1)`,
		fixture.SessionID).Scan(&memoryDocID); err != nil {
		t.Fatalf("read the memory document: %v", err)
	}
	if memoryDocID != 1 {
		t.Error("the aggregate memory's document was not enqueued under its own id")
	}
	if got := d1AdmissionCount(t, conn,
		`SELECT COUNT(*) FROM memory_vector_outbox WHERE document_id = ?`,
		d1AdmissionEvidenceDocumentID(fixture.SessionID, evidence.ID)); got != 1 {
		t.Error("the evidence document was not enqueued under the assigned evidence id")
	}
	if got := d1AdmissionCount(t, conn,
		`SELECT COUNT(*) FROM memory_vector_outbox WHERE document_id = ?`,
		d1AdmissionPreciseDocumentID(fixture.SessionID, unit.UnitID)); got != 1 {
		t.Error("the precise unit's document was not enqueued")
	}
	if result.VectorOperations != 3 {
		t.Errorf("vector operations = %d, want 3", result.VectorOperations)
	}

	// The marker is the commit point and must be committed with the admission's
	// own result hash.
	var admissionState, resultHash, admittedAt string
	if err := conn.QueryRow(ctx, `
		SELECT derived_admission_state, derived_result_hash, derived_admitted_at
		FROM memory_source_revisions WHERE source_revision = ?`, fixture.Revision).
		Scan(&admissionState, &resultHash, &admittedAt); err != nil {
		t.Fatalf("read the committed marker: %v", err)
	}
	if admissionState != "committed" {
		t.Errorf("derived_admission_state = %q, want %q", admissionState, "committed")
	}
	if resultHash != admission.ResultHash {
		t.Errorf("derived_result_hash = %q, want %q", resultHash, admission.ResultHash)
	}
	if admittedAt == "" {
		t.Error("derived_admitted_at must be stamped when the admission commits")
	}
}

// TestD1CommitMemoryAdmissionExcludesPublicMemoryProjection pins the
// no_public_memory_projection delete, which is what keeps a deliberately private
// aggregate out of the vector index.
func TestD1CommitMemoryAdmissionExcludesPublicMemoryProjection(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"private aggregate"}`)
	admission.MemoryPublicProjectionExcluded = true
	admission.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn}
	if _, err := st.CommitMemoryAdmission(ctx, admission); err != nil {
		t.Fatalf("CommitMemoryAdmission: %v", err)
	}

	var memoryID int64
	if err := conn.QueryRow(ctx, `SELECT id FROM memories LIMIT 1`).Scan(&memoryID); err != nil {
		t.Fatalf("read the memory id: %v", err)
	}
	documentID := "memory:" + fixture.SessionID + ":" + strconv.FormatInt(memoryID, 10)
	var operation, documentJSON string
	if err := conn.QueryRow(ctx,
		`SELECT operation, document_json FROM memory_vector_outbox WHERE document_id = ?`,
		documentID).Scan(&operation, &documentJSON); err != nil {
		t.Fatalf("the excluded memory's document was not enqueued for deletion: %v", err)
	}
	if operation != "delete" {
		t.Errorf("operation = %q, want a delete", operation)
	}
	var audit struct {
		DeleteReason string `json:"delete_reason"`
	}
	if err := json.Unmarshal([]byte(documentJSON), &audit); err != nil {
		t.Fatalf("the delete audit is not JSON: %v", err)
	}
	if audit.DeleteReason != "no_public_memory_projection" {
		t.Errorf("delete reason = %q, want %q", audit.DeleteReason, "no_public_memory_projection")
	}
}

// ---------------------------------------------------------------------------
// reconciliation
// ---------------------------------------------------------------------------

// TestD1CommitMemoryAdmissionRetiresUncitedEvidence pins the evidence
// tombstone path, together with the vector delete that retires its document.
//
// Tombstoning rather than deleting is what makes the reconcile converge: an
// extraction that cites the fact again finds the row and reactivates it instead
// of inserting a second copy.
func TestD1CommitMemoryAdmissionRetiresUncitedEvidence(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	first := d1Admission(fixture, `{"summary":"first extraction"}`)
	first.Evidence = []*DirectEvidence{
		d1AdmissionEvidence(fixture, "kept fact"),
		d1AdmissionEvidence(fixture, "dropped fact"),
	}
	if result, err := st.CommitMemoryAdmission(ctx, first); err != nil {
		t.Fatalf("first CommitMemoryAdmission: %v", err)
	} else if result.EvidenceInserted != 2 {
		t.Fatalf("first admission inserted %d evidence rows, want 2", result.EvidenceInserted)
	}

	// The turn is edited, so a new source revision on the same turn supersedes
	// the extraction that cited the dropped fact.
	next := d1SupersedeAdmissionSource(t, conn, fixture, "s1-rev-2")
	second := d1Admission(next, `{"summary":"second extraction"}`)
	second.Evidence = []*DirectEvidence{d1AdmissionEvidence(next, "kept fact")}
	result, err := st.CommitMemoryAdmission(ctx, second)
	if err != nil {
		t.Fatalf("second CommitMemoryAdmission: %v", err)
	}
	if result.EvidenceRetired != 1 {
		t.Errorf("evidence retired = %d, want 1", result.EvidenceRetired)
	}
	if result.EvidenceInserted != 0 {
		t.Errorf("evidence inserted = %d, want 0; the kept fact already existed", result.EvidenceInserted)
	}

	if got := d1AdmissionCount(t, conn,
		`SELECT COUNT(*) FROM direct_evidence_records WHERE tombstoned = 0 AND evidence_text = 'kept fact'`); got != 1 {
		t.Errorf("the kept fact has %d live rows, want 1", got)
	}
	if got := d1AdmissionCount(t, conn,
		`SELECT COUNT(*) FROM direct_evidence_records WHERE tombstoned = 1 AND evidence_text = 'dropped fact'`); got != 1 {
		t.Error("the dropped fact must be tombstoned, not deleted")
	}
	if got := d1AdmissionCount(t, conn,
		`SELECT COUNT(*) FROM memory_vector_outbox
		 WHERE operation = 'delete' AND document_json LIKE '%retired_evidence%'`); got != 1 {
		t.Error("the retired evidence's document must get a delete carrying the retired_evidence reason")
	}
}

// TestD1CommitMemoryAdmissionReactivatesTombstonedEvidence pins the other
// direction. A fact that came back must be reactivated, counted, and NOT counted
// as inserted — a reinsertion would leave two rows for one fact.
func TestD1CommitMemoryAdmissionReactivatesTombstonedEvidence(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	first := d1Admission(fixture, `{"summary":"one"}`)
	first.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "transient fact")}
	if _, err := st.CommitMemoryAdmission(ctx, first); err != nil {
		t.Fatalf("first CommitMemoryAdmission: %v", err)
	}
	// An extraction that cites nothing retires the fact.
	next := d1SupersedeAdmissionSource(t, conn, fixture, "s1-rev-2")
	second := d1Admission(next, `{"summary":"two"}`)
	if _, err := st.CommitMemoryAdmission(ctx, second); err != nil {
		t.Fatalf("second CommitMemoryAdmission: %v", err)
	}
	if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM direct_evidence_records WHERE tombstoned = 1`); got != 1 {
		t.Fatalf("tombstoned rows = %d, want 1 before the reactivation", got)
	}
	// A later revision cites it again.
	last := d1SupersedeAdmissionSource(t, conn, next, "s1-rev-3")
	third := d1Admission(last, `{"summary":"three"}`)
	third.Evidence = []*DirectEvidence{d1AdmissionEvidence(last, "transient fact")}
	result, err := st.CommitMemoryAdmission(ctx, third)
	if err != nil {
		t.Fatalf("third CommitMemoryAdmission: %v", err)
	}
	if result.EvidenceReactivated != 1 {
		t.Errorf("evidence reactivated = %d, want 1", result.EvidenceReactivated)
	}
	if result.EvidenceInserted != 0 {
		t.Errorf("evidence inserted = %d, want 0; a reactivated fact must not be reinserted", result.EvidenceInserted)
	}
	if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM direct_evidence_records`); got != 1 {
		t.Errorf("evidence rows = %d, want 1; reactivation must not duplicate the row", got)
	}
	if got := d1AdmissionCount(t, conn,
		`SELECT COUNT(*) FROM direct_evidence_records WHERE tombstoned = 0`); got != 1 {
		t.Error("the reactivated fact must be live again")
	}
}

// TestD1CommitMemoryAdmissionRetiresUndesiredPreciseUnits pins the precise
// reconcile's removal path: the unit is invalidated, its dependency edges are
// invalidated with it, and its document gets a delete.
//
// The reconcile is reached through the replay-refresh path, which is the only
// way an ALREADY COMMITTED source revision is re-projected. A new revision would
// carry a different source_revision and precise units are scoped by revision, so
// it would never see the previous revision's units. Rebuilding the projections of
// a committed extraction is exactly what refresh is for.
//
// The dependency invalidation is the part that is easy to miss. A unit that is
// invalidated but still has active edges would be resurrected by any later
// recomputation that trusts the lineage.
func TestD1CommitMemoryAdmissionRetiresUndesiredPreciseUnits(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	// The same extraction both times, so the committed result hash matches and
	// the refresh branch is allowed to rebuild its projections.
	const extraction = `{"summary":"unit churn"}`
	build := func(unitIDs ...string) *MemoryAdmission {
		admission := d1Admission(fixture, extraction)
		admission.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "a fact")}
		for _, unitID := range unitIDs {
			admission.PreciseUnits = append(admission.PreciseUnits,
				d1AdmissionUnit(fixture, unitID, "a fact"))
		}
		return admission
	}

	if _, err := st.CommitMemoryAdmission(ctx, build("u-keep", "u-drop")); err != nil {
		t.Fatalf("first CommitMemoryAdmission: %v", err)
	}
	if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM precise_memory_units`); got != 2 {
		t.Fatalf("precise units after the first admission = %d, want 2", got)
	}

	refreshCtx := WithMemoryAdmissionVectorReplay(ctx, true, false)
	result, err := st.CommitMemoryAdmission(refreshCtx, build("u-keep"))
	if err != nil {
		t.Fatalf("refreshed CommitMemoryAdmission: %v", err)
	}
	if result.Idempotent {
		t.Fatal("a refresh must rebuild the projections, not short-circuit as a replay")
	}
	if result.PreciseRetired != 1 {
		t.Errorf("precise retired = %d, want 1", result.PreciseRetired)
	}
	if result.PreciseInserted != 0 {
		t.Errorf("precise inserted = %d, want 0; u-keep already exists", result.PreciseInserted)
	}

	if got := d1AdmissionCount(t, conn,
		`SELECT COUNT(*) FROM precise_memory_units WHERE unit_id = 'u-drop' AND lifecycle_state = 'invalidated'`); got != 1 {
		t.Error("the dropped unit must be invalidated, not deleted")
	}
	if got := d1AdmissionCount(t, conn,
		`SELECT COUNT(*) FROM memory_derivation_dependencies
		 WHERE child_artifact_id = 'u-drop' AND lifecycle_state = 'active'`); got != 0 {
		t.Errorf("the dropped unit still has %d active dependency edges; a later recomputation would resurrect it", got)
	}
	if got := d1AdmissionCount(t, conn, `
		SELECT COUNT(*) FROM memory_derivation_dependencies
		WHERE child_artifact_id = 'u-drop' AND lifecycle_state = 'invalidated' AND invalidated_at IS NOT NULL`); got == 0 {
		t.Error("the dropped unit's edges must record when they were invalidated")
	}
	if got := d1AdmissionCount(t, conn, `
		SELECT COUNT(*) FROM memory_vector_outbox WHERE document_id = ? AND operation = 'delete'`,
		d1AdmissionPreciseDocumentID(fixture.SessionID, "u-drop")); got != 1 {
		t.Error("the dropped unit's document must get a delete")
	}
	// The surviving unit is untouched, and still linked to its evidence.
	if got := d1AdmissionCount(t, conn, `
		SELECT COUNT(*) FROM precise_memory_units
		WHERE unit_id = 'u-keep' AND lifecycle_state = 'active' AND root_evidence_id IS NOT NULL`); got != 1 {
		t.Error("the kept unit must stay active and linked to its evidence")
	}
}

// TestD1CommitMemoryAdmissionRejectsUnitWithoutCommittedEvidence pins the guard
// that stops a precise unit from being projected onto evidence that does not
// exist. It is the reference's guard and it must fire before any statement runs.
func TestD1CommitMemoryAdmissionRejectsUnitWithoutCommittedEvidence(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"dangling"}`)
	admission.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "the only fact")}
	// The unit cites an excerpt no evidence row carries.
	admission.PreciseUnits = []*PreciseMemoryUnit{d1AdmissionUnit(fixture, "u-dangling", "an absent fact")}

	_, err := st.CommitMemoryAdmission(ctx, admission)
	if err == nil || !strings.Contains(err.Error(), "precise memory evidence was not committed") {
		t.Errorf("error = %v, want the evidence guard to fire", err)
	}
	// Nothing may survive: batch 1 is atomic and must have rolled back the
	// evidence it had already inserted.
	if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM direct_evidence_records`); got != 0 {
		t.Errorf("evidence rows = %d, want 0; a refused admission must write nothing", got)
	}
	if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memories`); got != 0 {
		t.Errorf("memory rows = %d, want 0", got)
	}
	var admissionState string
	if err := conn.QueryRow(ctx,
		`SELECT derived_admission_state FROM memory_source_revisions WHERE source_revision = ?`,
		fixture.Revision).Scan(&admissionState); err != nil {
		t.Fatalf("read the source marker: %v", err)
	}
	if admissionState != "pending" {
		t.Errorf("derived_admission_state = %q, want it left at %q", admissionState, "pending")
	}
}

// TestD1CommitMemoryAdmissionBatchOneIsAtomic pins the transaction boundary.
//
// The unit carries a memory_kind the canonical schema's CHECK rejects, so the
// batch fails partway through — after the memory and the evidence statements
// have already run. Nothing may survive, and the marker must stay pending so a
// retry re-runs the whole thing.
func TestD1CommitMemoryAdmissionBatchOneIsAtomic(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"constraint violation"}`)
	admission.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn}
	admission.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "a fact")}
	bad := d1AdmissionUnit(fixture, "u-bad", "a fact")
	bad.Kind = "not_a_memory_kind"
	admission.PreciseUnits = []*PreciseMemoryUnit{bad}

	if _, err := st.CommitMemoryAdmission(ctx, admission); err == nil {
		t.Fatal("a CHECK violation must surface as an error")
	}
	for table, query := range map[string]string{
		"memories":     `SELECT COUNT(*) FROM memories`,
		"evidence":     `SELECT COUNT(*) FROM direct_evidence_records`,
		"units":        `SELECT COUNT(*) FROM precise_memory_units`,
		"dependencies": `SELECT COUNT(*) FROM memory_derivation_dependencies`,
		"outbox":       `SELECT COUNT(*) FROM memory_vector_outbox`,
	} {
		if got := d1AdmissionCount(t, conn, query); got != 0 {
			t.Errorf("%s rows = %d, want 0; batch 1 must roll back the whole admission", table, got)
		}
	}
	var admissionState string
	if err := conn.QueryRow(ctx,
		`SELECT derived_admission_state FROM memory_source_revisions WHERE source_revision = ?`,
		fixture.Revision).Scan(&admissionState); err != nil {
		t.Fatalf("read the source marker: %v", err)
	}
	if admissionState != "pending" {
		t.Errorf("derived_admission_state = %q, want %q", admissionState, "pending")
	}
}

// TestD1CommitMemoryAdmissionHealsAPartialCommit is the test the approved
// two-batch design exists for.
//
// It reproduces the crash window exactly: batch 1 has committed the canonical
// rows, but the outbox operations and the commit marker never ran. The source is
// therefore still pending while the rows exist. Re-running the same admission
// must converge — find the rows already present, enqueue the vectors, and
// commit the marker — without duplicating a single canonical row.
func TestD1CommitMemoryAdmissionHealsAPartialCommit(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	build := func() *MemoryAdmission {
		admission := d1Admission(fixture, `{"summary":"healed"}`)
		admission.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn}
		admission.Evidence = []*DirectEvidence{d1AdmissionEvidence(fixture, "a remembered fact")}
		admission.PreciseUnits = []*PreciseMemoryUnit{
			d1AdmissionUnit(fixture, "u-heal", "a remembered fact"),
		}
		return admission
	}

	if _, err := st.CommitMemoryAdmission(ctx, build()); err != nil {
		t.Fatalf("initial CommitMemoryAdmission: %v", err)
	}
	before := map[string]int{
		"memories":     d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memories`),
		"evidence":     d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM direct_evidence_records`),
		"units":        d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM precise_memory_units`),
		"dependencies": d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memory_derivation_dependencies`),
	}

	// Reproduce the crash: undo batch 2 only.
	if _, err := conn.Exec(ctx, `DELETE FROM memory_vector_outbox`); err != nil {
		t.Fatalf("clear the outbox: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		UPDATE memory_source_revisions
		SET derived_admission_state = 'pending', derived_admitted_at = NULL
		WHERE source_revision = ?`, fixture.Revision); err != nil {
		t.Fatalf("reset the commit marker: %v", err)
	}

	healed, err := st.CommitMemoryAdmission(ctx, build())
	if err != nil {
		t.Fatalf("CommitMemoryAdmission after the simulated crash: %v", err)
	}
	if healed.Idempotent {
		t.Error("a source that is still pending must be re-admitted, not treated as a replay")
	}

	// No canonical row was duplicated.
	for table, want := range before {
		query := map[string]string{
			"memories":     `SELECT COUNT(*) FROM memories`,
			"evidence":     `SELECT COUNT(*) FROM direct_evidence_records`,
			"units":        `SELECT COUNT(*) FROM precise_memory_units`,
			"dependencies": `SELECT COUNT(*) FROM memory_derivation_dependencies`,
		}[table]
		if got := d1AdmissionCount(t, conn, query); got != want {
			t.Errorf("%s rows after the heal = %d, want %d; the reconcile must converge, not duplicate", table, got, want)
		}
	}
	// The vectors are back.
	if got := d1AdmissionCount(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got == 0 {
		t.Error("the healed admission must enqueue the vector operations that were lost")
	}
	// And the marker is committed again.
	var admissionState string
	if err := conn.QueryRow(ctx,
		`SELECT derived_admission_state FROM memory_source_revisions WHERE source_revision = ?`,
		fixture.Revision).Scan(&admissionState); err != nil {
		t.Fatalf("read the healed marker: %v", err)
	}
	if admissionState != "committed" {
		t.Errorf("derived_admission_state = %q, want %q", admissionState, "committed")
	}
}

// TestD1CommitMemoryAdmissionStagesACriticResultOnFailure pins the staging path
// that makes a retry possible without re-running the extraction.
func TestD1CommitMemoryAdmissionStagesACriticResultOnFailure(t *testing.T) {
	st, conn := newD1TestStore(t)
	fixture := d1NewAdmissionFixture(t, conn)
	ctx := context.Background()

	admission := d1Admission(fixture, `{"summary":"expensive extraction"}`)
	admission.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn}
	bad := d1AdmissionUnit(fixture, "u-bad", "no evidence row exists for this")
	admission.PreciseUnits = []*PreciseMemoryUnit{bad}

	if _, err := st.CommitMemoryAdmission(ctx, admission); err == nil {
		t.Fatal("the admission must fail")
	}

	// The extraction is preserved so a later worker can re-project it.
	var state, derivation, extractor, indexVersion, resultHash, resultJSON string
	if err := conn.QueryRow(ctx, `
		SELECT derived_admission_state, derived_admission_version, derived_extractor_version,
		       derived_index_version, derived_result_hash, derived_result_json
		FROM memory_source_revisions WHERE source_revision = ?`, fixture.Revision).
		Scan(&state, &derivation, &extractor, &indexVersion, &resultHash, &resultJSON); err != nil {
		t.Fatalf("read the staged marker: %v", err)
	}
	if state != "pending" {
		t.Errorf("derived_admission_state = %q, want %q", state, "pending")
	}
	if resultHash != admission.ResultHash {
		t.Errorf("staged result hash = %q, want the extraction's %q", resultHash, admission.ResultHash)
	}
	if resultJSON != admission.ResultJSON {
		t.Error("the staged result JSON must be the extraction that succeeded")
	}
	if derivation != admission.DerivationVersion || extractor != admission.ExtractorVersion ||
		indexVersion != admission.IndexVersion {
		t.Error("the staged version triple must match the extraction's")
	}

	// Staging is idempotent: a second failure over the same result must not
	// conflict with the row the first one left.
	second := d1Admission(fixture, `{"summary":"expensive extraction"}`)
	second.Memory = &Memory{ChatSessionID: fixture.SessionID, TurnIndex: fixture.Turn}
	second.PreciseUnits = []*PreciseMemoryUnit{d1AdmissionUnit(fixture, "u-bad", "still no evidence")}
	if _, err := st.CommitMemoryAdmission(ctx, second); err == nil {
		t.Fatal("the second admission must fail too")
	}
	if !strings.Contains(d1ReadAdmissionState(t, conn, fixture.Revision), "pending") {
		t.Error("the second failure must leave the staged row alone")
	}
}

func d1ReadAdmissionState(t *testing.T, conn *sqliteD1Conn, revision string) string {
	t.Helper()
	var state string
	if err := conn.QueryRow(context.Background(),
		`SELECT derived_admission_state FROM memory_source_revisions WHERE source_revision = ?`,
		revision).Scan(&state); err != nil {
		t.Fatalf("read the admission state: %v", err)
	}
	return state
}
