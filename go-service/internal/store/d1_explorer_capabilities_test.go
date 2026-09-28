package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestD1ExplorerMemoryPatchWritesOnlyPatchedFields pins the bounded-edit
// contract: an Explorer correction must land on the columns the operator named
// and leave every other column of the row exactly as it was.
func TestD1ExplorerMemoryPatchWritesOnlyPatchedFields(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	mem := &Memory{
		ChatSessionID: "s1", TurnIndex: 4,
		SummaryJSON: `{"k":"old"}`, Importance: 0.25, PlaceWing: "east", PlaceRoom: "attic",
		CreatedAt: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC),
	}
	if err := st.SaveMemory(ctx, mem); err != nil {
		t.Fatalf("SaveMemory: %v", err)
	}

	// Only importance is corrected.
	importance := 0.9
	if err := st.UpdateMemoryExplorerFields(ctx, "s1", mem.ID, MemoryExplorerPatch{Importance: &importance}); err != nil {
		t.Fatalf("UpdateMemoryExplorerFields: %v", err)
	}

	got, err := st.ListMemories(ctx, "s1", 0, 0)
	if err != nil {
		t.Fatalf("ListMemories: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("memories = %d, want 1", len(got))
	}
	if got[0].Importance != 0.9 {
		t.Errorf("importance = %v, want 0.9", got[0].Importance)
	}
	if got[0].SummaryJSON != `{"k":"old"}` {
		t.Errorf("summary_json = %q, an unpatched column must survive", got[0].SummaryJSON)
	}
	if got[0].PlaceWing != "east" || got[0].PlaceRoom != "attic" {
		t.Errorf("place = %q/%q, an unpatched column must survive", got[0].PlaceWing, got[0].PlaceRoom)
	}
	if !got[0].CreatedAt.Equal(mem.CreatedAt) {
		t.Errorf("created_at = %v, want the original %v: an Explorer edit is an in-place correction, not a new version", got[0].CreatedAt, mem.CreatedAt)
	}

	// A full patch writes every editable column at once.
	summary := `{"k":"new"}`
	wing, room := "west", "cellar"
	if err := st.UpdateMemoryExplorerFields(ctx, "s1", mem.ID, MemoryExplorerPatch{
		SummaryJSON: &summary, Importance: &importance, PlaceWing: &wing, PlaceRoom: &room,
	}); err != nil {
		t.Fatalf("full UpdateMemoryExplorerFields: %v", err)
	}
	got, err = st.ListMemories(ctx, "s1", 0, 0)
	if err != nil {
		t.Fatalf("ListMemories after full patch: %v", err)
	}
	if got[0].SummaryJSON != summary || got[0].PlaceWing != wing || got[0].PlaceRoom != room {
		t.Errorf("full patch = %+v, want summary %q and place %q/%q", got[0], summary, wing, room)
	}
}

// TestD1ExplorerMemoryPatchIsSessionScoped pins that an edit can never reach a
// row outside the caller's session, even when the row id is known.
func TestD1ExplorerMemoryPatchIsSessionScoped(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	own := &Memory{ChatSessionID: "s1", TurnIndex: 1, SummaryJSON: `{"k":"mine"}`, Importance: 0.1}
	other := &Memory{ChatSessionID: "s2", TurnIndex: 1, SummaryJSON: `{"k":"theirs"}`, Importance: 0.2}
	if err := st.SaveMemory(ctx, own); err != nil {
		t.Fatalf("SaveMemory own: %v", err)
	}
	if err := st.SaveMemory(ctx, other); err != nil {
		t.Fatalf("SaveMemory other: %v", err)
	}

	// A guessed id from the wrong session is a no-op, not an error: the HTTP
	// layer resolved the target first and owns the 404.
	summary := `{"k":"leaked"}`
	if err := st.UpdateMemoryExplorerFields(ctx, "s1", other.ID, MemoryExplorerPatch{SummaryJSON: &summary}); err != nil {
		t.Fatalf("cross-session update: %v", err)
	}

	var otherSummary string
	if err := conn.QueryRow(ctx, `SELECT summary_json FROM memories WHERE id = ?`, other.ID).Scan(&otherSummary); err != nil {
		t.Fatalf("read other session row: %v", err)
	}
	if otherSummary != `{"k":"theirs"}` {
		t.Errorf("the other session's summary_json = %q, a cross-session patch must not write", otherSummary)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memories`); got != 2 {
		t.Errorf("memories rows = %d, want 2 (no insert, no delete)", got)
	}
}

// TestD1ExplorerPatchRejectsEmptyPatch pins the no-op guard. A patch that names
// no supported field must be refused before any statement runs, so a malformed
// request can never be reinterpreted as "clear the row".
func TestD1ExplorerPatchRejectsEmptyPatch(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	mem := &Memory{ChatSessionID: "s1", TurnIndex: 1, SummaryJSON: `{"k":"keep"}`, Importance: 0.4}
	triple := &KGTriple{ChatSessionID: "s1", Subject: "a", Predicate: "b", Object: "c", ValidFrom: 1, ValidTo: 9}
	evidence := &DirectEvidence{
		ChatSessionID: "s1", EvidenceKind: "fact_event", EvidenceText: "keep me",
		SourceTurnStart: 1, SourceTurnEnd: 1, ArchiveState: "pending_capture",
		CaptureStage: "critic_extract", CaptureVerification: "pending",
	}
	if err := st.SaveMemory(ctx, mem); err != nil {
		t.Fatalf("SaveMemory: %v", err)
	}
	if err := st.SaveKGTriple(ctx, triple); err != nil {
		t.Fatalf("SaveKGTriple: %v", err)
	}
	if err := st.SaveEvidence(ctx, evidence); err != nil {
		t.Fatalf("SaveEvidence: %v", err)
	}

	if err := st.UpdateMemoryExplorerFields(ctx, "s1", mem.ID, MemoryExplorerPatch{}); !errors.Is(err, ErrNotEnabled) {
		t.Errorf("empty memory patch = %v, want ErrNotEnabled", err)
	}
	if err := st.UpdateKGTripleExplorerFields(ctx, "s1", triple.ID, KGTripleExplorerPatch{}); !errors.Is(err, ErrNotEnabled) {
		t.Errorf("empty KG patch = %v, want ErrNotEnabled", err)
	}
	if err := st.UpdateDirectEvidenceExplorerFields(ctx, "s1", evidence.ID, DirectEvidenceExplorerPatch{}); !errors.Is(err, ErrNotEnabled) {
		t.Errorf("empty evidence patch = %v, want ErrNotEnabled", err)
	}

	// The rejected patches must have changed nothing.
	var summary, text string
	if err := conn.QueryRow(ctx, `SELECT summary_json FROM memories WHERE id = ?`, mem.ID).Scan(&summary); err != nil {
		t.Fatalf("read memory: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT evidence_text FROM direct_evidence_records WHERE id = ?`, evidence.ID).Scan(&text); err != nil {
		t.Fatalf("read evidence: %v", err)
	}
	if summary != `{"k":"keep"}` || text != "keep me" {
		t.Errorf("a rejected patch wrote to the row: summary=%q text=%q", summary, text)
	}
}

// TestD1ExplorerKGTriplePatchTriStateValidity pins the OptionalIntPatch
// semantics: an omitted interval is not written, a set interval writes its
// value, and a cleared interval writes SQL NULL so "valid now" stays
// distinguishable from "valid from turn 0".
func TestD1ExplorerKGTriplePatchTriStateValidity(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	triple := &KGTriple{ChatSessionID: "s1", Subject: "ana", Predicate: "visited", Object: "library", ValidFrom: 2, ValidTo: 8}
	if err := st.SaveKGTriple(ctx, triple); err != nil {
		t.Fatalf("SaveKGTriple: %v", err)
	}

	// Omitted intervals are left alone even though the text fields change.
	subject := "ana"
	object := "archive"
	if err := st.UpdateKGTripleExplorerFields(ctx, "s1", triple.ID, KGTripleExplorerPatch{Subject: &subject, Object: &object}); err != nil {
		t.Fatalf("text-only patch: %v", err)
	}
	var validFrom, validTo int64
	var gotSubject, gotPredicate, gotObject string
	if err := conn.QueryRow(ctx, `SELECT subject, predicate, object, valid_from, valid_to FROM kg_triples WHERE id = ?`, triple.ID).
		Scan(&gotSubject, &gotPredicate, &gotObject, &validFrom, &validTo); err != nil {
		t.Fatalf("read triple: %v", err)
	}
	if gotSubject != "ana" || gotObject != "archive" {
		t.Errorf("subject/object = %q/%q, want ana/archive", gotSubject, gotObject)
	}
	if validFrom != 2 || validTo != 8 {
		t.Errorf("valid_from/valid_to = %d/%d, an omitted interval must not be rewritten", validFrom, validTo)
	}

	// An explicit value overwrites.
	from, to := 5, 12
	if err := st.UpdateKGTripleExplorerFields(ctx, "s1", triple.ID, KGTripleExplorerPatch{
		ValidFrom: OptionalIntPatch{Set: true, Value: &from},
		ValidTo:   OptionalIntPatch{Set: true, Value: &to},
	}); err != nil {
		t.Fatalf("interval value patch: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT valid_from, valid_to FROM kg_triples WHERE id = ?`, triple.ID).
		Scan(&validFrom, &validTo); err != nil {
		t.Fatalf("read intervals: %v", err)
	}
	if validFrom != 5 || validTo != 12 {
		t.Errorf("valid_from/valid_to = %d/%d, want 5/12", validFrom, validTo)
	}

	// A cleared interval is SQL NULL, not 0.
	cleared := OptionalIntPatch{Set: true}
	if err := st.UpdateKGTripleExplorerFields(ctx, "s1", triple.ID, KGTripleExplorerPatch{ValidTo: cleared}); err != nil {
		t.Fatalf("cleared interval patch: %v", err)
	}
	var nullValidTo *int64
	if err := conn.QueryRow(ctx, `SELECT valid_to FROM kg_triples WHERE id = ?`, triple.ID).Scan(&nullValidTo); err != nil {
		t.Fatalf("read cleared interval: %v", err)
	}
	if nullValidTo != nil {
		t.Errorf("valid_to = %v, a cleared interval must be NULL, not %d", *nullValidTo, *nullValidTo)
	}
}

// TestD1ExplorerEvidencePatchStoresReviewState pins that a fact correction
// carries the review state as well as the text: booleans land as the 0/1
// integers the canonical schema declares, and the supersession link is
// tri-state.
func TestD1ExplorerEvidencePatchStoresReviewState(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	evidence := &DirectEvidence{
		ChatSessionID: "s1", EvidenceKind: "fact_event", EvidenceText: "wrong fact",
		SourceTurnStart: 3, SourceTurnEnd: 3, ArchiveState: "pending_capture",
		CaptureStage: "critic_extract", CaptureVerification: "pending",
	}
	if err := st.SaveEvidence(ctx, evidence); err != nil {
		t.Fatalf("SaveEvidence: %v", err)
	}

	text := "corrected fact"
	state := "committed"
	verification := "verified"
	gate := "manual_revalidate"
	repairNeeded := false
	tombstoned := true
	superseded := 4242
	if err := st.UpdateDirectEvidenceExplorerFields(ctx, "s1", evidence.ID, DirectEvidenceExplorerPatch{
		EvidenceText:        &text,
		ArchiveState:        &state,
		CaptureVerification: &verification,
		CommittedGate:       &gate,
		RepairNeeded:        &repairNeeded,
		Tombstoned:          &tombstoned,
		SupersededByID:      OptionalIntPatch{Set: true, Value: &superseded},
	}); err != nil {
		t.Fatalf("UpdateDirectEvidenceExplorerFields: %v", err)
	}

	var gotText, gotState, gotVerification, gotGate string
	var gotRepair, gotTombstoned, gotSuperseded int64
	if err := conn.QueryRow(ctx, `SELECT evidence_text, archive_state, capture_verification, committed_gate,
			repair_needed, tombstoned, superseded_by_id
		FROM direct_evidence_records WHERE id = ?`, evidence.ID).
		Scan(&gotText, &gotState, &gotVerification, &gotGate, &gotRepair, &gotTombstoned, &gotSuperseded); err != nil {
		t.Fatalf("read evidence: %v", err)
	}
	if gotText != text || gotState != state || gotVerification != verification || gotGate != gate {
		t.Errorf("text/state/verification/gate = %q/%q/%q/%q, want %q/%q/%q/%q",
			gotText, gotState, gotVerification, gotGate, text, state, verification, gate)
	}
	if gotRepair != 0 || gotTombstoned != 1 {
		t.Errorf("repair_needed/tombstoned = %d/%d, want 0/1 as stored integers", gotRepair, gotTombstoned)
	}
	if gotSuperseded != int64(superseded) {
		t.Errorf("superseded_by_id = %d, want %d", gotSuperseded, superseded)
	}

	// The unpatched capture_stage must survive a full patch.
	var stage string
	if err := conn.QueryRow(ctx, `SELECT capture_stage FROM direct_evidence_records WHERE id = ?`, evidence.ID).Scan(&stage); err != nil {
		t.Fatalf("read capture_stage: %v", err)
	}
	if stage != "critic_extract" {
		t.Errorf("capture_stage = %q, an unpatched column must survive", stage)
	}

	// A cleared supersession link is NULL, so "superseded by nothing" stays
	// distinguishable from "superseded by row 0".
	cleared := OptionalIntPatch{Set: true}
	if err := st.UpdateDirectEvidenceExplorerFields(ctx, "s1", evidence.ID, DirectEvidenceExplorerPatch{SupersededByID: cleared}); err != nil {
		t.Fatalf("cleared supersession: %v", err)
	}
	var nullSuperseded *int64
	if err := conn.QueryRow(ctx, `SELECT superseded_by_id FROM direct_evidence_records WHERE id = ?`, evidence.ID).
		Scan(&nullSuperseded); err != nil {
		t.Fatalf("read cleared supersession: %v", err)
	}
	if nullSuperseded != nil {
		t.Errorf("superseded_by_id = %d, a cleared link must be NULL", *nullSuperseded)
	}
}

// TestD1ExplorerEvidencePatchWritesTextVerbatim pins that patch text is stored
// as given, including the empty string. The canonical path writes these values
// verbatim too, so an operator clearing a nullable text column produces the
// same transition on both engines.
func TestD1ExplorerEvidencePatchWritesTextVerbatim(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	evidence := &DirectEvidence{
		ChatSessionID: "s1", EvidenceKind: "fact_event", EvidenceText: "  spaced  ",
		SourceTurnStart: 1, SourceTurnEnd: 1, ArchiveState: "committed",
		CaptureStage: "critic_extract", CaptureVerification: "verified",
		CommittedGate: "auto",
	}
	if err := st.SaveEvidence(ctx, evidence); err != nil {
		t.Fatalf("SaveEvidence: %v", err)
	}

	gate := ""
	if err := st.UpdateDirectEvidenceExplorerFields(ctx, "s1", evidence.ID, DirectEvidenceExplorerPatch{CommittedGate: &gate}); err != nil {
		t.Fatalf("clear committed_gate: %v", err)
	}
	var gotGate string
	if err := conn.QueryRow(ctx, `SELECT committed_gate FROM direct_evidence_records WHERE id = ?`, evidence.ID).Scan(&gotGate); err != nil {
		t.Fatalf("read committed_gate: %v", err)
	}
	if gotGate != "" {
		t.Errorf("committed_gate = %q, an explicitly cleared value is stored verbatim as the empty string", gotGate)
	}
}

// TestD1ExplorerPatchOnAbsentRowIsNotAnError pins the not-found contract. The
// canonical statements do not inspect the affected row count, and the HTTP layer
// maps every error other than ErrNotEnabled to 500, so an absent row must stay
// a successful no-op rather than become an internal error.
func TestD1ExplorerPatchOnAbsentRowIsNotAnError(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	mem := &Memory{ChatSessionID: "s1", TurnIndex: 1, SummaryJSON: `{"k":"keep"}`}
	if err := st.SaveMemory(ctx, mem); err != nil {
		t.Fatalf("SaveMemory: %v", err)
	}

	importance := 0.7
	if err := st.UpdateMemoryExplorerFields(ctx, "s1", 999999, MemoryExplorerPatch{Importance: &importance}); err != nil {
		t.Errorf("patching an absent row = %v, want a successful no-op", err)
	}

	// The same id under a different session is equally a no-op.
	if err := st.UpdateMemoryExplorerFields(ctx, "other-session", mem.ID, MemoryExplorerPatch{Importance: &importance}); err != nil {
		t.Errorf("patching across sessions = %v, want a successful no-op", err)
	}
	subject := "nobody"
	if err := st.UpdateKGTripleExplorerFields(ctx, "s1", 999999, KGTripleExplorerPatch{Subject: &subject}); err != nil {
		t.Errorf("patching an absent triple = %v, want a successful no-op", err)
	}
	verification := "verified"
	if err := st.UpdateDirectEvidenceExplorerFields(ctx, "s1", 999999, DirectEvidenceExplorerPatch{CaptureVerification: &verification}); err != nil {
		t.Errorf("patching an absent record = %v, want a successful no-op", err)
	}

	// Nothing was inserted and the real row is untouched.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memories`); got != 1 {
		t.Errorf("memories rows = %d, want 1", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM kg_triples`); got != 0 {
		t.Errorf("kg_triples rows = %d, want 0", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM direct_evidence_records`); got != 0 {
		t.Errorf("direct_evidence_records rows = %d, want 0", got)
	}
	var summary string
	if err := conn.QueryRow(ctx, `SELECT summary_json FROM memories WHERE id = ?`, mem.ID).Scan(&summary); err != nil {
		t.Fatalf("read memory: %v", err)
	}
	if summary != `{"k":"keep"}` {
		t.Errorf("summary_json = %q, a wrong-session patch must not write", summary)
	}
}
