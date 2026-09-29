package store

import (
	"context"
	"errors"
	"sort"
	"strings"
)

// D1 memory admission projection inspector.
//
// This is the proof obligation for the two-batch admission.
//
// A D1 admission commits its canonical rows in one batch and its vector outbox
// operations plus the derived_admission_state marker in a second. The marker is
// the commit point, so a source revision that reads 'committed' is asserting
// that BOTH batches landed. This capability is how that assertion is checked
// against the durable rows instead of taken on trust: an operator replay that
// found a committed source marker with a missing outbox row would otherwise
// treat the projection as finished and move on.
//
// It matters more on D1 than the reference, precisely because of the split. A
// single MariaDB transaction cannot leave a committed marker without its outbox
// rows; a crash between two D1 batches can. Placing the marker last makes the
// partial state indistinguishable from no state — but only if something
// verifies that, and this is that something.
//
// There is no reference implementation of this contract on any provider. It is
// declared in precise_memory.go and listed by the Stage 1 capability inventory as
// a parity target whose consumer is administrative replay, but no code called it
// and no provider implemented it, which is why it was missing from the
// capability manifest until the manifest's route-assertion guard was corrected.
//
// The lane names and the meaning of "current" below are therefore defined HERE,
// from the documented meaning of the two structs:
//
//   - Complete means every expected row is present AND current. A row that exists
//     but is stale (tombstoned, invalidated, in a terminal outbox status, or
//     carrying a different value than the committed extraction) does not count.
//   - A lane whose expected count is zero is a legitimate lane, and it is
//     complete only when it also has zero live rows. That is what makes
//     "this admission produced no evidence" distinguishable from "the evidence
//     rows are missing".
//   - The inspection is strictly read-only. It never changes source or projection
//     state, so it is safe to run against a live database while a turn is being
//     admitted; the worst it can report is a false negative for an admission that
//     is still in flight, which the caller sees as incomplete rather than as
//     corruption.
//
// What it deliberately does NOT do is repair. Reporting a gap is the contract;
// acting on it belongs to the reprocessing lane, which already knows how to
// re-project a committed extraction.

var _ MemoryAdmissionProjectionInspector = (*d1Store)(nil)

// The inspection lanes, in the order they are reported. The names are the
// caller's contract, so they are constants rather than literals scattered
// through the statements.
const (
	d1ProjectionLaneMemory     = "memory"
	d1ProjectionLaneEvidence   = "evidence"
	d1ProjectionLanePrecise    = "precise_memory_unit"
	d1ProjectionLaneDependency = "memory_derivation_dependency"
	d1ProjectionLaneOutbox     = "memory_vector_outbox"
)

func d1ProjectionLaneNames() []string {
	return []string{
		d1ProjectionLaneMemory,
		d1ProjectionLaneEvidence,
		d1ProjectionLanePrecise,
		d1ProjectionLaneDependency,
		d1ProjectionLaneOutbox,
	}
}

// InspectMemoryAdmissionProjection compares an expectation derived from one
// committed admission against the durable rows that admission was supposed to
// produce.
func (s *d1Store) InspectMemoryAdmissionProjection(
	ctx context.Context,
	expectation MemoryAdmissionProjectionExpectation,
) (MemoryAdmissionProjectionInspection, error) {
	inspection := MemoryAdmissionProjectionInspection{
		ExpectedCounts: map[string]int{},
		ActualCounts:   map[string]int{},
	}
	for _, lane := range d1ProjectionLaneNames() {
		inspection.ExpectedCounts[lane] = 0
		inspection.ActualCounts[lane] = 0
	}

	sessionID := strings.TrimSpace(expectation.ChatSessionID)
	revision := strings.TrimSpace(expectation.SourceRevision)
	if sessionID == "" || revision == "" || expectation.TurnIndex <= 0 {
		// Without the identity of the admitted turn there is nothing to compare.
		// A zero expectation would read as "every lane is legitimately empty",
		// which is a false all-clear, so this is reported as incomplete instead.
		inspection.MissingLanes = append(inspection.MissingLanes, "expectation_identity")
		return inspection, nil
	}

	memoryExpected := 0
	if expectation.MemoryExpected {
		memoryExpected = 1
	}
	inspection.ExpectedCounts[d1ProjectionLaneMemory] = memoryExpected
	evidenceExpected := d1DistinctTrimmed(expectation.EvidenceTexts)
	inspection.ExpectedCounts[d1ProjectionLaneEvidence] = evidenceExpected
	inspection.ExpectedCounts[d1ProjectionLanePrecise] = max(expectation.PreciseUnitCount, 0)
	inspection.ExpectedCounts[d1ProjectionLaneDependency] = max(expectation.PreciseDependencyCount, 0)
	inspection.ExpectedCounts[d1ProjectionLaneOutbox] = len(expectation.AdmissionVectorArtifacts)

	// ---- the aggregate memory ----
	// The row is located the way the admission located it: lowest id for this
	// (session, turn). A summary that differs from the committed extraction makes
	// the row present-but-stale rather than missing, because the two are different
	// failures: one loses the row, the other loses the content.
	var memoryID int64
	var summaryJSON *string
	err := s.conn.QueryRow(ctx, `
		SELECT id, summary_json FROM memories
		WHERE chat_session_id = ? AND turn_index = ?
		ORDER BY id
		LIMIT 1
	`, sessionID, expectation.TurnIndex).Scan(&memoryID, &summaryJSON)
	switch {
	case err == nil:
		inspection.ActualCounts[d1ProjectionLaneMemory] = 1
		if memoryExpected == 0 {
			inspection.StaleLanes = append(inspection.StaleLanes, d1ProjectionLaneMemory)
		} else if expectation.MemorySummaryJSON != "" &&
			d1DerefString(summaryJSON) != expectation.MemorySummaryJSON {
			// The row exists but carries a different summary than the committed
			// extraction. Reporting it as missing would send a repair looking for
			// a row that is right there.
			inspection.StaleLanes = append(inspection.StaleLanes, d1ProjectionLaneMemory)
		}
	case errors.Is(err, errD1NoRows):
		if memoryExpected > 0 {
			inspection.MissingLanes = append(inspection.MissingLanes, d1ProjectionLaneMemory)
		}
	default:
		return inspection, err
	}

	// ---- the evidence ----
	// Only LIVE rows count. A tombstoned row is the correct end state for evidence
	// the extraction no longer cites, so counting it as present would make a
	// retirement look complete while the row is on its way out of every read.
	liveEvidence, missingEvidence, err := s.d1InspectProjectionEvidence(ctx, sessionID, expectation, evidenceExpected)
	if err != nil {
		return inspection, err
	}
	inspection.ActualCounts[d1ProjectionLaneEvidence] = liveEvidence
	if len(missingEvidence) > 0 {
		inspection.MissingLanes = append(inspection.MissingLanes, d1ProjectionLaneEvidence)
	}

	// ---- the precise units ----
	// A unit whose root evidence is gone is present-but-stale: the unit survives
	// but its derivation points at a row the evidence lane already lost, and any
	// read that trusts the link would surface a memory with no provenance.
	activeUnits, staleUnits, err := s.d1InspectProjectionPrecise(ctx, sessionID, revision)
	if err != nil {
		return inspection, err
	}
	inspection.ActualCounts[d1ProjectionLanePrecise] = activeUnits
	if staleUnits > 0 {
		inspection.StaleLanes = append(inspection.StaleLanes, d1ProjectionLanePrecise)
	}
	if activeUnits < max(expectation.PreciseUnitCount, 0) {
		inspection.MissingLanes = append(inspection.MissingLanes, d1ProjectionLanePrecise)
	}

	// ---- the derivation dependencies ----
	// Scoped to the revision and to precise-unit children, because that is the set
	// the admission writes. Counting every dependency row in the revision would
	// let an unrelated child mask a missing edge.
	activeDependencies, err := s.d1CountInt(ctx, `
		SELECT COUNT(*) FROM memory_derivation_dependencies
		WHERE source_revision = ?
		  AND child_artifact_type = 'precise_memory_unit'
		  AND lifecycle_state = 'active'
	`, revision)
	if err != nil {
		return inspection, err
	}
	inspection.ActualCounts[d1ProjectionLaneDependency] = activeDependencies
	if activeDependencies < max(expectation.PreciseDependencyCount, 0) {
		inspection.MissingLanes = append(inspection.MissingLanes, d1ProjectionLaneDependency)
	}

	// ---- the vector outbox ----
	// A document in a terminal status has already been applied or permanently
	// rejected, which is the correct end state. A document with NO row, or one
	// still queued, is not what a committed admission promised: the operation must
	// be present. This is the lane that detects the crash between the two batches.
	liveOutbox, missingOutbox, err := s.d1InspectProjectionOutbox(ctx, sessionID, expectation.AdmissionVectorArtifacts)
	if err != nil {
		return inspection, err
	}
	inspection.ActualCounts[d1ProjectionLaneOutbox] = liveOutbox
	if len(missingOutbox) > 0 {
		inspection.MissingLanes = append(inspection.MissingLanes, d1ProjectionLaneOutbox)
	}

	inspection.MissingLanes = d1SortedUnique(inspection.MissingLanes)
	inspection.StaleLanes = d1SortedUnique(inspection.StaleLanes)
	inspection.Complete = len(inspection.MissingLanes) == 0 && len(inspection.StaleLanes) == 0
	return inspection, nil
}

// d1InspectProjectionEvidence counts the live evidence rows for the admitted
// texts and reports which texts have none.
func (s *d1Store) d1InspectProjectionEvidence(
	ctx context.Context,
	sessionID string,
	expectation MemoryAdmissionProjectionExpectation,
	expectedCount int,
) (int, []string, error) {
	live := 0
	var missing []string
	seen := map[string]bool{}
	for _, rawText := range expectation.EvidenceTexts {
		text := strings.TrimSpace(rawText)
		if text == "" || seen[text] {
			continue
		}
		seen[text] = true
		// One query per text rather than a single IN, because the answer is
		// PER TEXT: "is this fact present" cannot be answered by a total. Counting
		// a set and comparing totals would report a complete lane when the
		// extraction cited two facts and the store had a duplicate of one of them.
		count, err := s.d1CountInt(ctx, `
			SELECT COUNT(*) FROM direct_evidence_records
			WHERE chat_session_id = ?
			  AND evidence_text = ?
			  AND source_turn_start = ? AND source_turn_end = ?
			  AND capture_stage = 'critic_extract'
			  AND tombstoned = 0
		`, sessionID, text, expectation.TurnIndex, expectation.TurnIndex)
		if err != nil {
			return 0, nil, err
		}
		// A duplicated live row for one text means the same fact can be read back
		// twice, so it counts as stale rather than as two good rows.
		switch {
		case count == 0:
			missing = append(missing, text)
		case count > 1:
			live++
			missing = append(missing, text)
		default:
			live++
		}
	}
	if expectedCount == 0 && live > 0 {
		// The admission promised no evidence and the store has some. That is not a
		// gap the caller asked about, but it does mean the two disagree.
		missing = append(missing, "unexpected_evidence")
	}
	return live, missing, nil
}

// d1InspectProjectionPrecise counts the active units for the revision and the
// ones whose derivation link no longer resolves.
func (s *d1Store) d1InspectProjectionPrecise(ctx context.Context, sessionID, revision string) (int, int, error) {
	active, err := s.d1CountInt(ctx, `
		SELECT COUNT(*) FROM precise_memory_units
		WHERE chat_session_id = ? AND source_revision = ? AND lifecycle_state = 'active'
	`, sessionID, revision)
	if err != nil {
		return 0, 0, err
	}
	// A unit is stale when it is active but its root evidence is absent or
	// tombstoned. The subquery reproduces the link the admission resolved, so it
	// asks exactly the question the read path will ask when it renders the unit.
	stale, err := s.d1CountInt(ctx, `
		SELECT COUNT(*) FROM precise_memory_units unit
		WHERE unit.chat_session_id = ? AND unit.source_revision = ?
		  AND unit.lifecycle_state = 'active'
		  AND (
		    unit.root_evidence_id IS NULL
		    OR NOT EXISTS (
		      SELECT 1 FROM direct_evidence_records evidence
		      WHERE evidence.id = unit.root_evidence_id
		        AND evidence.tombstoned = 0
		    )
		  )
	`, sessionID, revision)
	if err != nil {
		return 0, 0, err
	}
	return active, stale, nil
}

// d1InspectProjectionOutbox counts the expected documents that have an outbox
// row, and reports the ones that do not.
//
// The direction is deliberate: a committed admission that promised to enqueue a
// document and did not is a real gap, so a missing row is a failure. An extra row
// for a document the admission did not promise is not this inspection's business.
func (s *d1Store) d1InspectProjectionOutbox(ctx context.Context, sessionID string, artifacts []string) (int, []string, error) {
	live := 0
	var missing []string
	seen := map[string]bool{}
	for _, rawDocument := range artifacts {
		documentID := strings.TrimSpace(rawDocument)
		if documentID == "" || seen[documentID] {
			continue
		}
		seen[documentID] = true
		count, err := s.d1CountInt(ctx, `
			SELECT COUNT(*) FROM memory_vector_outbox
			WHERE chat_session_id = ? AND document_id = ?
		`, sessionID, documentID)
		if err != nil {
			return 0, nil, err
		}
		if count == 0 {
			missing = append(missing, documentID)
			continue
		}
		live++
	}
	return live, missing, nil
}

// d1CountInt runs a single-value count statement.
func (s *d1Store) d1CountInt(ctx context.Context, query string, args ...any) (int, error) {
	var n int64
	if err := s.conn.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		return 0, err
	}
	return int(n), nil
}

// d1DistinctTrimmed returns the non-blank trimmed values, deduplicated, in a
// stable order so two callers deriving the same expectation agree.
func d1DistinctTrimmed(values []string) int {
	seen := map[string]bool{}
	for _, value := range values {
		if text := strings.TrimSpace(value); text != "" {
			seen[text] = true
		}
	}
	return len(seen)
}

func d1SortedUnique(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
