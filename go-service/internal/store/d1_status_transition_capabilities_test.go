package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// D1 reversible status-transition and supersession-resolution tests.
//
// Every test runs against real SQLite through the D1 transport, so these assert
// the statements D1 actually executes — including the batch boundary, the
// upsert, and the SQLite form of every JSON_UNQUOTE predicate.
//
// The two capabilities under test are named for what they must prevent:
//
//   - "Reversible" means a transition records what it undid. The forward-only
//     failure — an implementation that commits a new current value and its
//     event but never records the prior observation it replaced — is invisible
//     in the happy path and is exactly what the dependency-parent and
//     explicit-repair tests below pin.
//   - Supersession resolution is only correct if the losing candidate is
//     excluded by the RIGHT predicate, so the winner-selection tests seed rows
//     that differ only in the tiebreak column.

// ---------------------------------------------------------------------------
// seeds
// ---------------------------------------------------------------------------

// d1StatusTransitionScene is one session prepared for reversible transitions: a
// status registry row the events and current values can point at, and an
// accepted source revision the fence can check.
type d1StatusTransitionScene struct {
	SessionID    string
	Revision     string
	LogicalTurn  string
	BodyRegistry int64
	Time         time.Time
}

func d1StatusTransitionTestTime() time.Time {
	return time.Date(2026, 7, 31, 10, 0, 0, 0, time.UTC)
}

// d1StatusTransitionSeedRegistry inserts one status_schema_registry row and
// returns its id. The registry row is not decoration: status_change_events and
// status_current_values both carry a foreign key to it, so without it the batch
// would fail on the write rather than on the missing registry.
func d1StatusTransitionSeedRegistry(t *testing.T, conn *sqliteD1Conn, sessionID, statusKey, ownerScope string) int64 {
	t.Helper()
	stamp := d1TimeValue(d1StatusTransitionTestTime())
	if _, err := conn.Exec(context.Background(), `INSERT INTO status_schema_registry (
		chat_session_id, schema_name, status_key, label, owner_scope, value_kind, registry_state, created_at, updated_at
	) VALUES (?, 'status_schema', ?, ?, ?, 'object', 'active', ?, ?)`,
		sessionID, statusKey, statusKey+" label", ownerScope, stamp, stamp); err != nil {
		t.Fatalf("seed status registry %s: %v", statusKey, err)
	}
	var id int64
	if err := conn.QueryRow(context.Background(),
		`SELECT id FROM status_schema_registry WHERE chat_session_id = ? AND status_key = ?`,
		sessionID, statusKey).Scan(&id); err != nil {
		t.Fatalf("read seeded registry id: %v", err)
	}
	return id
}

// d1StatusTransitionSeedRevision inserts one memory_source_revisions row. The
// logical turn id is explicit because the schema's generated
// active_logical_turn_slot column permits only one ACTIVE revision per logical
// turn, so a test with two live revisions has to give them two turns.
func d1StatusTransitionSeedRevision(t *testing.T, conn *sqliteD1Conn, sessionID, revision, logicalTurnID string, turnIndex int, lifecycle string) {
	t.Helper()
	stamp := d1TimeValue(d1StatusTransitionTestTime())
	if _, err := conn.Exec(context.Background(), `INSERT INTO memory_source_revisions (
		source_revision, chat_session_id, logical_turn_id, turn_index,
		raw_user_content, raw_assistant_content, combined_content_hash,
		hash_algorithm, host_observed_at_ms, lifecycle_state, created_at, updated_at
	) VALUES (?, ?, ?, ?, 'u', 'a', ?, 'sha256', 1, ?, ?, ?)`,
		revision, sessionID, logicalTurnID, turnIndex, "hash-"+revision, lifecycle, stamp, stamp); err != nil {
		t.Fatalf("seed source revision %s: %v", revision, err)
	}
}

func d1StatusTransitionSeedScene(t *testing.T, conn *sqliteD1Conn, sessionID string) d1StatusTransitionScene {
	t.Helper()
	scene := d1StatusTransitionScene{
		SessionID:    sessionID,
		Revision:     "rev-1",
		LogicalTurn:  "turn-3",
		Time:         d1StatusTransitionTestTime(),
		BodyRegistry: d1StatusTransitionSeedRegistry(t, conn, sessionID, "reversible_body_state", "fictional_entity"),
	}
	d1StatusTransitionSeedRevision(t, conn, sessionID, scene.Revision, scene.LogicalTurn, 3, "active")
	return scene
}

// d1StatusTransitionEvidence builds the evidence payload a transition carries.
//
// The whole idempotency contract of this capability lives inside this JSON: the
// source revision and unit id are not columns, so a reader that stops reading
// them from evidence silently makes every transition look like a new one.
func d1StatusTransitionEvidence(revision, unit string, extra map[string]any) string {
	evidence := map[string]any{
		"source_revision":    revision,
		"source_unit_id":     unit,
		"current_projection": true,
	}
	for key, value := range extra {
		evidence[key] = value
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

// d1StatusTransitionFixture builds an accepted-source transition for one owner
// slot. The unit id and source turn vary per call so a test can drive a slot
// forward across several observations.
func d1StatusTransitionFixture(scene d1StatusTransitionScene, unit string, sourceTurn int) ReversibleStatusTransition {
	evidence := d1StatusTransitionEvidence(scene.Revision, unit, map[string]any{"direct_evidence_ids": []int{9}})
	current := StatusCurrentValue{
		ChatSessionID: scene.SessionID,
		RegistryID:    scene.BodyRegistry,
		StatusKey:     "reversible_body_state",
		OwnerScope:    "fictional_entity",
		OwnerID:       "entity-1",
		OwnerLabel:    "Mina",
		ValueKind:     "object",
		ValueJSON:     fmt.Sprintf(`{"version":"reversible_state.v1","turn":%d}`, sourceTurn),
		EvidenceJSON:  evidence,
		SourceTurn:    sourceTurn,
		WriteState:    "current",
		CreatedAt:     scene.Time,
		UpdatedAt:     scene.Time,
	}
	return ReversibleStatusTransition{
		SourceContract: acceptedSourceObservationContract,
		SourceRevision: scene.Revision,
		SourceUnitID:   unit,
		CurrentValue:   &current,
		Event: StatusChangeEvent{
			ChatSessionID:     scene.SessionID,
			RegistryID:        scene.BodyRegistry,
			StatusKey:         "reversible_body_state",
			OwnerScope:        "fictional_entity",
			OwnerID:           "entity-1",
			EventKind:         "set",
			PreviousValueJSON: `{"version":"reversible_state.v1"}`,
			NewValueJSON:      current.ValueJSON,
			EvidenceJSON:      evidence,
			SourceTurn:        sourceTurn,
			StoryClockJSON:    `{"scene":"night-watch"}`,
			EventState:        "recorded",
			CreatedAt:         scene.Time,
		},
	}
}

// d1StatusTransitionSeedEvent inserts one status_change_events row directly, so
// a winner-selection test can control the exact id, turn and repair turn.
func d1StatusTransitionSeedEvent(t *testing.T, conn *sqliteD1Conn, id int64, sessionID string, registryID int64, statusKey, ownerScope, ownerID, evidence string, sourceTurn int) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO status_change_events (
		id, chat_session_id, registry_id, status_key, owner_scope, owner_id,
		event_kind, new_value_json, evidence_json, source_turn, event_state, created_at
	) VALUES (?, ?, ?, ?, ?, ?, 'set', ?, ?, ?, 'recorded', ?)`,
		id, sessionID, registryID, statusKey, ownerScope, ownerID,
		fmt.Sprintf(`{"turn":%d}`, sourceTurn), evidence, sourceTurn,
		d1TimeValue(d1StatusTransitionTestTime())); err != nil {
		t.Fatalf("seed status change event %d: %v", id, err)
	}
}

// d1StatusTransitionSeedCurrentValue inserts one status_current_values row.
func d1StatusTransitionSeedCurrentValue(t *testing.T, conn *sqliteD1Conn, id int64, sessionID string, registryID int64, statusKey, ownerScope, ownerID, evidence, writeState string, sourceTurn int) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO status_current_values (
		id, chat_session_id, registry_id, status_key, owner_scope, owner_id,
		owner_label, value_kind, value_json, evidence_json, source_turn, write_state, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, 'object', ?, ?, ?, ?, ?, ?)`,
		id, sessionID, registryID, statusKey, ownerScope, ownerID, ownerID,
		`{"seeded":true}`, evidence, sourceTurn, writeState,
		d1TimeValue(d1StatusTransitionTestTime()), d1TimeValue(d1StatusTransitionTestTime())); err != nil {
		t.Fatalf("seed status current value %d: %v", id, err)
	}
}

// d1StatusTransitionParentIDs returns the derivation parents recorded for one
// child event, as "kind:id" strings.
func d1StatusTransitionParentIDs(t *testing.T, conn *sqliteD1Conn, childID string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT parent_artifact_type, parent_artifact_id
		FROM memory_derivation_dependencies
		WHERE child_artifact_type = 'status_change_event' AND child_artifact_id = ?
		ORDER BY parent_artifact_type, parent_artifact_id`, childID)
	if err != nil {
		t.Fatalf("read dependency parents: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var kind, id string
		if err := rows.Scan(&kind, &id); err != nil {
			t.Fatalf("scan dependency parent: %v", err)
		}
		out = append(out, kind+":"+id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("dependency parents: %v", err)
	}
	return out
}

// d1StatusTransitionEventIDs reduces a list of events to their ids.
func d1StatusTransitionEventIDs(events []StatusChangeEvent) []string {
	out := make([]string, 0, len(events))
	for _, event := range events {
		out = append(out, fmt.Sprint(event.ID))
	}
	return out
}

// ---------------------------------------------------------------------------
// reversible transition: forward, replay, and the recorded prior observation
// ---------------------------------------------------------------------------

// TestD1StoreApplyReversibleStatusTransitionCommitsCurrentAndEventTogether is
// the atomicity test. The current value and its event must both be visible
// afterwards; a store that wrote the current row and then failed the event
// would leave a projection with no ledger entry behind it.
func TestD1StoreApplyReversibleStatusTransitionCommitsCurrentAndEventTogether(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")

	result, err := st.ApplyReversibleStatusTransition(ctx, d1StatusTransitionFixture(scene, "unit-1", 3))
	if err != nil {
		t.Fatalf("ApplyReversibleStatusTransition: %v", err)
	}
	if result.Replayed {
		t.Fatal("a first transition was reported as a replay")
	}
	if result.CurrentValue.ID == 0 || result.Event.ID == 0 {
		t.Fatalf("transition result carries no canonical ids: %+v", result)
	}
	// The event must point at the current value this same call wrote. That link
	// is what makes the ledger and the projection the same fact; it is written
	// through a scalar subquery because a D1 batch cannot return the id.
	if result.Event.StatusValueID != result.CurrentValue.ID {
		t.Fatalf("event status_value_id = %d, want %d", result.Event.StatusValueID, result.CurrentValue.ID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_current_values`); got != 1 {
		t.Fatalf("current value rows = %d, want 1", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_change_events`); got != 1 {
		t.Fatalf("change event rows = %d, want 1", got)
	}
	// Parents: the source revision and the cited direct evidence. There is no
	// prior event yet, so there is no status_change_event parent.
	parents := d1StatusTransitionParentIDs(t, conn, fmt.Sprint(result.Event.ID))
	want := []string{"direct_evidence:9", "source_revision:" + scene.Revision}
	if !equalStringSlices(parents, want) {
		t.Fatalf("dependency parents = %v, want %v", parents, want)
	}
}

// TestD1StoreApplyReversibleStatusTransitionRecordsTheObservationItReplaced is
// the "reversible" half. A second observation of the same owner slot must name
// the event it superseded as a derivation parent, otherwise nothing in the graph
// can reconstruct what a current value was before the transition, and the whole
// table cannot be rebuilt from accepted source history.
func TestD1StoreApplyReversibleStatusTransitionRecordsTheObservationItReplaced(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")

	first, err := st.ApplyReversibleStatusTransition(ctx, d1StatusTransitionFixture(scene, "unit-1", 3))
	if err != nil {
		t.Fatalf("first transition: %v", err)
	}
	second, err := st.ApplyReversibleStatusTransition(ctx, d1StatusTransitionFixture(scene, "unit-2", 4))
	if err != nil {
		t.Fatalf("second transition: %v", err)
	}

	if first.Event.ID == second.Event.ID {
		t.Fatal("two distinct source units produced the same event id")
	}
	// The slot is upserted in place, not duplicated: the second observation owns
	// the same row id.
	if second.CurrentValue.ID != first.CurrentValue.ID {
		t.Fatalf("current value id changed across observations: %d then %d",
			first.CurrentValue.ID, second.CurrentValue.ID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_current_values`); got != 1 {
		t.Fatalf("current value rows = %d, want 1 (one slot per owner)", got)
	}
	// THIS is the load-bearing assertion: the second event records the first.
	parents := d1StatusTransitionParentIDs(t, conn, fmt.Sprint(second.Event.ID))
	want := []string{
		"direct_evidence:9",
		"source_revision:" + scene.Revision,
		"status_change_event:" + fmt.Sprint(first.Event.ID),
	}
	if !equalStringSlices(parents, want) {
		t.Fatalf("dependency parents = %v, want %v", parents, want)
	}
	// The superseded event keeps no parent edge to its successor: the graph
	// points forward in time only.
	if firstParents := d1StatusTransitionParentIDs(t, conn, fmt.Sprint(first.Event.ID)); len(firstParents) != 2 {
		t.Fatalf("first event parents = %v, want the two original parents only", firstParents)
	}
}

// TestD1StoreApplyReversibleStatusTransitionExactReplayDoesNotWriteAgain pins
// idempotency. A retried turn is a routine occurrence (an HTTP retry, a
// re-delivered message, a replayed worker job), and it must not append a second
// event or bump the current row a second time.
func TestD1StoreApplyReversibleStatusTransitionExactReplayDoesNotWriteAgain(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")

	first, err := st.ApplyReversibleStatusTransition(ctx, d1StatusTransitionFixture(scene, "unit-1", 3))
	if err != nil {
		t.Fatalf("first transition: %v", err)
	}
	replay, err := st.ApplyReversibleStatusTransition(ctx, d1StatusTransitionFixture(scene, "unit-1", 3))
	if err != nil {
		t.Fatalf("replayed transition: %v", err)
	}
	if !replay.Replayed {
		t.Fatal("an exact replay was not detected")
	}
	if replay.Event.ID != first.Event.ID {
		t.Fatalf("replay reported event %d, want the stored %d", replay.Event.ID, first.Event.ID)
	}
	if replay.CurrentValue.ID != first.CurrentValue.ID {
		t.Fatalf("replay reported current value %d, want the stored %d",
			replay.CurrentValue.ID, first.CurrentValue.ID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_change_events`); got != 1 {
		t.Fatalf("change event rows = %d, want 1 (a replay must not duplicate)", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_current_values`); got != 1 {
		t.Fatalf("current value rows = %d, want 1", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_derivation_dependencies`); got != 2 {
		t.Fatalf("dependency rows = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// fences
// ---------------------------------------------------------------------------

// TestD1StoreApplyReversibleStatusTransitionRejectsStaleSourceRevision pins
// rule 5 for this capability: a source the user has already rolled back must
// admit nothing, and the refusal must happen before any statement is sent.
func TestD1StoreApplyReversibleStatusTransitionRejectsStaleSourceRevision(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(t *testing.T, conn *sqliteD1Conn, scene d1StatusTransitionScene)
	}{
		{
			name: "rolled_back_revision",
			configure: func(t *testing.T, conn *sqliteD1Conn, scene d1StatusTransitionScene) {
				d1StatusTransitionSeedRevision(t, conn, scene.SessionID, "rev-dead", "turn-9", 9, "invalidated")
			},
		},
		{
			name: "superseded_revision",
			configure: func(t *testing.T, conn *sqliteD1Conn, scene d1StatusTransitionScene) {
				d1StatusTransitionSeedRevision(t, conn, scene.SessionID, "rev-dead", "turn-9", 9, "superseded")
			},
		},
		{
			name:      "never_accepted_revision",
			configure: func(t *testing.T, conn *sqliteD1Conn, scene d1StatusTransitionScene) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			ctx := context.Background()
			scene := d1StatusTransitionSeedScene(t, conn, "session-1")
			tc.configure(t, conn, scene)

			transition := d1StatusTransitionFixture(scene, "unit-1", 3)
			transition.SourceRevision = "rev-dead"
			transition.Event.EvidenceJSON = d1StatusTransitionEvidence("rev-dead", "unit-1", nil)
			transition.CurrentValue.EvidenceJSON = transition.Event.EvidenceJSON

			if _, err := st.ApplyReversibleStatusTransition(ctx, transition); !errors.Is(err, ErrSourceRevisionStale) {
				t.Fatalf("error = %v, want ErrSourceRevisionStale", err)
			}
			if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_current_values`); got != 0 {
				t.Fatalf("current value rows = %d, want 0 (a refused write must write nothing)", got)
			}
			if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_change_events`); got != 0 {
				t.Fatalf("change event rows = %d, want 0", got)
			}
			if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_derivation_dependencies`); got != 0 {
				t.Fatalf("dependency rows = %d, want 0", got)
			}
		})
	}
}

// TestD1StoreApplyReversibleStatusTransitionRejectsStaleCurrentProjection pins
// the other direction of the same rule: a transition that observed an EARLIER
// turn than the slot already has must not overwrite it. Dropping this check is
// how a late-arriving source resurrects a superseded state.
func TestD1StoreApplyReversibleStatusTransitionRejectsStaleCurrentProjection(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")

	d1StatusTransitionSeedCurrentValue(t, conn, 501, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-1",
		d1StatusTransitionEvidence(scene.Revision, "unit-9", nil), "current", 9)

	if _, err := st.ApplyReversibleStatusTransition(ctx, d1StatusTransitionFixture(scene, "unit-1", 3)); !errors.Is(err, ErrStatusProjectionStale) {
		t.Fatalf("error = %v, want ErrStatusProjectionStale", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_change_events`); got != 0 {
		t.Fatalf("change event rows = %d, want 0", got)
	}
	// The stored slot must be untouched, not merely un-duplicated.
	var sourceTurn int
	if err := conn.QueryRow(ctx, `SELECT source_turn FROM status_current_values WHERE id = 501`).Scan(&sourceTurn); err != nil {
		t.Fatalf("read seeded current value: %v", err)
	}
	if sourceTurn != 9 {
		t.Fatalf("seeded current value source_turn = %d, want 9", sourceTurn)
	}
}

// ---------------------------------------------------------------------------
// the reverse path: explicit state repair
// ---------------------------------------------------------------------------

// d1StatusTransitionSeedPendingThread inserts one pending_threads row.
func d1StatusTransitionSeedPendingThread(t *testing.T, conn *sqliteD1Conn, id int64, sessionID, threadKey, description, status string, createdTurn, sourceTurn int, createdAt, updatedAt string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO pending_threads (
		id, chat_session_id, thread_key, description, status, created_turn, source_turn,
		hook_type, hook_metadata_json, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, 'promise', ?, ?, ?)`,
		id, sessionID, threadKey, description, status, createdTurn, sourceTurn,
		`{"lifecycle_state":"active"}`, createdAt, updatedAt); err != nil {
		t.Fatalf("seed pending thread %s: %v", threadKey, err)
	}
}

func d1StatusTransitionPendingSnapshot(scene d1StatusTransitionScene) PendingThread {
	return PendingThread{
		ID:               77,
		ChatSessionID:    scene.SessionID,
		ThreadKey:        "promise-occurrence-1",
		Description:      "Return the borrowed book",
		Status:           "open",
		CreatedTurn:      2,
		SourceTurn:       2,
		HookType:         "promise",
		HookMetadataJSON: `{"lifecycle_instance_id":"promise-occurrence-1","lifecycle_state":"active"}`,
		CreatedAt:        scene.Time.Add(-time.Hour),
		UpdatedAt:        scene.Time.Add(-time.Hour),
	}
}

// TestD1StoreApplyReversibleStatusTransitionRestoresTheExactPendingSnapshot
// exercises the full reverse path. A normal lifecycle projection only COALESCEs
// the fields it carries and leaves the manual trust flags alone; an explicit
// undo must reinstate the exact observed before-snapshot, including the flags
// the forward observation changed. An implementation that only ever writes
// forward would leave the thread resolved and suppressed here while reporting
// success.
func TestD1StoreApplyReversibleStatusTransitionRestoresTheExactPendingSnapshot(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")
	narrativeRegistry := d1StatusTransitionSeedRegistry(t, conn, scene.SessionID, "narrative_state", "fictional_entity")

	before := d1StatusTransitionPendingSnapshot(scene)

	// --- forward: an observed narrative transition that completes the promise.
	afterValue := before
	afterValue.ID = 0
	afterValue.ChatSessionID = scene.SessionID
	afterValue.Status = "resolved"
	afterValue.ResolvedTurn = 5
	afterValue.SourceTurn = 5
	afterValue.Description = ""
	afterValue.Pinned = true
	afterValue.Suppressed = true
	afterValue.UpdatedAt = scene.Time
	valueJSON, err := json.Marshal(map[string]any{"pending_thread": afterValue})
	if err != nil {
		t.Fatalf("marshal narrative value: %v", err)
	}
	forwardEvidence := d1StatusTransitionEvidence(scene.Revision, "unit-narrative", nil)
	forward := ReversibleStatusTransition{
		SourceContract: acceptedSourceObservationContract,
		SourceRevision: scene.Revision,
		SourceUnitID:   "unit-narrative",
		CurrentValue: &StatusCurrentValue{
			ChatSessionID: scene.SessionID,
			RegistryID:    narrativeRegistry,
			StatusKey:     "narrative_state",
			OwnerScope:    "fictional_entity",
			OwnerID:       "entity-1",
			OwnerLabel:    "Mina",
			ValueKind:     "object",
			ValueJSON:     string(valueJSON),
			EvidenceJSON:  forwardEvidence,
			SourceTurn:    5,
			WriteState:    "current",
			CreatedAt:     scene.Time,
			UpdatedAt:     scene.Time,
		},
		Event: StatusChangeEvent{
			ChatSessionID: scene.SessionID,
			RegistryID:    narrativeRegistry,
			StatusKey:     "narrative_state",
			OwnerScope:    "fictional_entity",
			OwnerID:       "entity-1",
			EventKind:     "set",
			NewValueJSON:  string(valueJSON),
			EvidenceJSON:  forwardEvidence,
			SourceTurn:    5,
			EventState:    "recorded",
			CreatedAt:     scene.Time,
		},
	}
	if _, err := st.ApplyReversibleStatusTransition(ctx, forward); err != nil {
		t.Fatalf("forward narrative transition: %v", err)
	}
	var pendingAfter int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM pending_threads WHERE chat_session_id = ? AND thread_key = ?`,
		scene.SessionID, before.ThreadKey).Scan(&pendingAfter); err != nil {
		t.Fatalf("count projected pending thread: %v", err)
	}
	if pendingAfter != 1 {
		t.Fatalf("pending thread rows = %d, want 1 (the forward projection must materialise it)", pendingAfter)
	}

	// --- reverse: an explicit repair with no accepted source revision.
	//
	// SourceRevision is empty on purpose. Imported history has no accepted
	// source row, which is the one case the reference allows past the source
	// fence, and the repair contract in the evidence is what keeps the
	// observation eligible for the current projection afterwards.
	repair := ReversibleStatusTransition{
		SourceContract:  StateRepairContract,
		SourceUnitID:    "repair-1",
		DeleteCurrent:   true,
		PendingSnapshot: &before,
		Event: StatusChangeEvent{
			ChatSessionID: scene.SessionID,
			RegistryID:    narrativeRegistry,
			StatusKey:     "narrative_state",
			OwnerScope:    "fictional_entity",
			OwnerID:       "entity-1",
			EventKind:     "repair_remove",
			NewValueJSON:  string(valueJSON),
			EvidenceJSON:  `{}`,
			SourceTurn:    7,
			EventState:    "recorded",
			CreatedAt:     scene.Time.Add(time.Minute),
		},
	}
	result, err := st.ApplyReversibleStatusTransition(ctx, repair)
	if err != nil {
		t.Fatalf("repair transition: %v", err)
	}
	if result.Replayed {
		t.Fatal("a first repair was reported as a replay")
	}
	if result.CurrentValue.ID != 0 {
		t.Fatalf("repair reported a current value %d, want none (the slot was removed)", result.CurrentValue.ID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_current_values WHERE status_key = 'narrative_state'`); got != 0 {
		t.Fatalf("narrative current value rows = %d, want 0 after an explicit removal", got)
	}

	// The removal is still a projection observation, and it must be marked as
	// one: this is what stops a delayed source from re-creating the value.
	removal, err := st.GetReversibleStatusEventBySourceUnit(ctx, scene.SessionID, "", "repair-1")
	if err != nil {
		t.Fatalf("read removal event: %v", err)
	}
	var evidencePayload map[string]any
	if err := json.Unmarshal([]byte(removal.EvidenceJSON), &evidencePayload); err != nil {
		t.Fatalf("removal evidence must be a JSON object: %v", err)
	}
	projection, _ := evidencePayload["current_projection"].(bool)
	action, _ := evidencePayload["projection_action"].(string)
	contract, _ := evidencePayload["source_contract"].(string)
	if !projection || action != "remove" || contract != StateRepairContract {
		t.Fatalf("removal evidence = %s, want current_projection=true projection_action=remove source_contract=%s",
			removal.EvidenceJSON, StateRepairContract)
	}

	// The exact before-snapshot, not a merged one.
	var (
		description, status, hookMetadata, createdAt string
		resolvedTurn                                 int64
		pinned, suppressed, userCorrected            int
	)
	if err := conn.QueryRow(ctx, `
		SELECT description, status, resolved_turn, hook_metadata_json,
		       pinned, suppressed, user_corrected, created_at
		FROM pending_threads WHERE chat_session_id = ? AND thread_key = ?`,
		scene.SessionID, before.ThreadKey).Scan(
		&description, &status, &resolvedTurn, &hookMetadata,
		&pinned, &suppressed, &userCorrected, &createdAt); err != nil {
		t.Fatalf("read restored pending thread: %v", err)
	}
	if description != before.Description || status != before.Status {
		t.Fatalf("restored thread = %q/%q, want %q/%q", description, status, before.Description, before.Status)
	}
	if resolvedTurn != 0 {
		t.Fatalf("restored resolved_turn = %d, want 0 (the promise is open again)", resolvedTurn)
	}
	if hookMetadata != before.HookMetadataJSON {
		t.Fatalf("restored hook_metadata_json = %q, want %q", hookMetadata, before.HookMetadataJSON)
	}
	if pinned != 0 || suppressed != 0 || userCorrected != 0 {
		t.Fatalf("restored manual flags = pinned %d suppressed %d user_corrected %d, want all 0",
			pinned, suppressed, userCorrected)
	}
	if createdAt != d1TimeValue(before.CreatedAt) {
		t.Fatalf("restored created_at = %q, want the observed %q", createdAt, d1TimeValue(before.CreatedAt))
	}

	// The removed slot must no longer read back as current, and the removal
	// event must be the latest projection observation for the slot.
	values, err := st.ListReversibleStatusCurrentValues(ctx, scene.SessionID, "fictional_entity", []string{"narrative_state"})
	if err != nil {
		t.Fatalf("ListReversibleStatusCurrentValues: %v", err)
	}
	if len(values) != 0 {
		t.Fatalf("current values = %+v, want none after an explicit removal", values)
	}
}

// TestD1StoreApplyReversibleStatusTransitionRepairAppliesRecordedArtifacts
// covers the artifact half of the repair branch. The repair plan records
// before/after per column; the store has to apply the AFTER to the live row and
// to record the BEFORE it actually observed, because that recorded pair is the
// only backup of the cleared content.
func TestD1StoreApplyReversibleStatusTransitionRepairAppliesRecordedArtifacts(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")

	if _, err := conn.Exec(ctx, `INSERT INTO memories (
		id, chat_session_id, turn_index, summary_json, evidence, embedding
	) VALUES (701, ?, 4, ?, '[]', '[]')`, scene.SessionID,
		`{"summary":"Mira was pregnant that night."}`); err != nil {
		t.Fatalf("seed memory: %v", err)
	}
	cleared := "{}"
	repair := ReversibleStatusTransition{
		SourceContract: StateRepairContract,
		SourceUnitID:   "repair-artifacts",
		ArtifactChanges: []StateRepairArtifactChange{{
			Table: "memories",
			ID:    701,
			After: map[string]*string{"summary_json": &cleared},
		}},
		Event: StatusChangeEvent{
			ChatSessionID: scene.SessionID,
			RegistryID:    scene.BodyRegistry,
			StatusKey:     "reversible_body_state",
			OwnerScope:    "fictional_entity",
			OwnerID:       "entity-1",
			EventKind:     "repair",
			EvidenceJSON:  `{}`,
			SourceTurn:    8,
			EventState:    "recorded",
			CreatedAt:     scene.Time,
		},
	}
	result, err := st.ApplyReversibleStatusTransition(ctx, repair)
	if err != nil {
		t.Fatalf("artifact repair: %v", err)
	}
	var summary string
	if err := conn.QueryRow(ctx, `SELECT summary_json FROM memories WHERE id = 701`).Scan(&summary); err != nil {
		t.Fatalf("read repaired memory: %v", err)
	}
	if summary != "{}" {
		t.Fatalf("repaired summary_json = %q, want %q", summary, "{}")
	}
	// The applied artifacts are recorded back into the event evidence, with the
	// BEFORE the store actually read rather than the one the plan proposed.
	stored, err := st.GetReversibleStatusEventBySourceUnit(ctx, scene.SessionID, "", "repair-artifacts")
	if err != nil {
		t.Fatalf("read repair event: %v", err)
	}
	var evidencePayload struct {
		RepairArtifacts []StateRepairArtifactChange `json:"repair_artifacts"`
	}
	if err := json.Unmarshal([]byte(stored.EvidenceJSON), &evidencePayload); err != nil {
		t.Fatalf("repair evidence must be a JSON object: %v", err)
	}
	if len(evidencePayload.RepairArtifacts) != 1 {
		t.Fatalf("repair_artifacts = %+v, want exactly the one applied change", evidencePayload.RepairArtifacts)
	}
	applied := evidencePayload.RepairArtifacts[0]
	if applied.Before["summary_json"] == nil ||
		*applied.Before["summary_json"] != `{"summary":"Mira was pregnant that night."}` {
		t.Fatalf("recorded before = %v, want the summary the row actually held", applied.Before)
	}
	if result.Event.ID == 0 {
		t.Fatal("the repair event was not committed")
	}
}

// ---------------------------------------------------------------------------
// reads
// ---------------------------------------------------------------------------

// TestD1StoreGetReversibleStatusEventBySourceUnit covers both outcomes. The
// not-found identity matters: routes branch on ErrNotFound, so a raw
// errD1NoRows would surface as an unknown 500.
func TestD1StoreGetReversibleStatusEventBySourceUnit(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")

	written, err := st.ApplyReversibleStatusTransition(ctx, d1StatusTransitionFixture(scene, "unit-1", 3))
	if err != nil {
		t.Fatalf("ApplyReversibleStatusTransition: %v", err)
	}
	got, err := st.GetReversibleStatusEventBySourceUnit(ctx, scene.SessionID, scene.Revision, "unit-1")
	if err != nil {
		t.Fatalf("GetReversibleStatusEventBySourceUnit: %v", err)
	}
	if got.ID != written.Event.ID || got.StatusKey != "reversible_body_state" || got.SourceTurn != 3 {
		t.Fatalf("event = %+v, want the written event %d", got, written.Event.ID)
	}
	// NULL round-trips as the empty string, not as a stale previous value.
	if got.PreviousValueJSON != `{"version":"reversible_state.v1"}` || got.StoryClockJSON != `{"scene":"night-watch"}` {
		t.Fatalf("event payloads = %q / %q", got.PreviousValueJSON, got.StoryClockJSON)
	}

	// Newest wins. The store itself will never write a second row under one
	// unit key — that is exactly what the replay check prevents — so the
	// duplicate is seeded directly: ORDER BY id DESC is what makes this read
	// report the LATEST observation rather than the first one ever written.
	d1StatusTransitionSeedEvent(t, conn, 610, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-1",
		d1StatusTransitionEvidence(scene.Revision, "unit-1", nil), 6)
	newest, err := st.GetReversibleStatusEventBySourceUnit(ctx, scene.SessionID, scene.Revision, "unit-1")
	if err != nil {
		t.Fatalf("read newest event: %v", err)
	}
	if newest.ID != 610 || newest.SourceTurn != 6 {
		t.Fatalf("newest event = %d at turn %d, want the id-610 turn-6 row", newest.ID, newest.SourceTurn)
	}

	if _, err := st.GetReversibleStatusEventBySourceUnit(ctx, scene.SessionID, scene.Revision, "unit-absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing event error = %v, want ErrNotFound", err)
	}
	// A repair event carries no source revision, and a blank revision is a
	// real lookup key rather than a wildcard.
	if _, err := st.GetReversibleStatusEventBySourceUnit(ctx, scene.SessionID, scene.Revision, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("blank unit error = %v, want ErrNotFound", err)
	}
}

// TestD1StoreListReversibleStatusCurrentValuesIsUncappedAndEligibilityFiltered
// pins two things at once. The read has NO limit and no bounded fallback: it is
// the "exact, uncapped rebuild" the capability is named for, so a silently
// truncated result would look complete and be wrong. And the eligibility
// predicate must drop a slot whose source revision was rolled back as well as
// one that is no longer materialised as current.
func TestD1StoreListReversibleStatusCurrentValuesIsUncappedAndEligibilityFiltered(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")
	socialRegistry := d1StatusTransitionSeedRegistry(t, conn, scene.SessionID, "reversible_social_state", "fictional_entity")
	d1StatusTransitionSeedRevision(t, conn, scene.SessionID, "rev-dead", "turn-9", 9, "invalidated")

	// 105 live slots: any bounded default (the sibling reads fall back to 100)
	// would truncate this to 100 and the test would fail.
	if _, err := conn.Exec(ctx, `
		WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 105)
		INSERT INTO status_current_values (
			id, chat_session_id, registry_id, status_key, owner_scope, owner_id,
			owner_label, value_kind, value_json, evidence_json, source_turn, write_state, created_at, updated_at
		)
		SELECT 9000 + seq.n, ?, ?, 'reversible_body_state', 'fictional_entity',
		       'entity-' || seq.n, 'e' || seq.n, 'object', '{"seeded":true}', ?, 1, 'current', ?, ?
		FROM seq`,
		scene.SessionID, scene.BodyRegistry,
		d1StatusTransitionEvidence(scene.Revision, "unit-bulk", nil),
		d1TimeValue(scene.Time), d1TimeValue(scene.Time)); err != nil {
		t.Fatalf("seed bulk current values: %v", err)
	}

	// One eligible social slot, one rolled-back slot, one non-current slot.
	d1StatusTransitionSeedCurrentValue(t, conn, 8001, scene.SessionID, socialRegistry,
		"reversible_social_state", "fictional_entity", "entity-zzz",
		d1StatusTransitionEvidence(scene.Revision, "unit-social", nil), "current", 4)
	d1StatusTransitionSeedCurrentValue(t, conn, 8002, scene.SessionID, socialRegistry,
		"reversible_social_state", "fictional_entity", "entity-dead",
		d1StatusTransitionEvidence("rev-dead", "unit-dead", nil), "current", 5)
	d1StatusTransitionSeedCurrentValue(t, conn, 8003, scene.SessionID, socialRegistry,
		"reversible_social_state", "fictional_entity", "entity-draft",
		d1StatusTransitionEvidence(scene.Revision, "unit-draft", nil), "superseded", 6)

	got, err := st.ListReversibleStatusCurrentValues(ctx, scene.SessionID, "fictional_entity",
		[]string{"reversible_body_state", "reversible_social_state"})
	if err != nil {
		t.Fatalf("ListReversibleStatusCurrentValues: %v", err)
	}
	if len(got) != 106 {
		t.Fatalf("rows = %d, want 106 (105 bulk + 1 social; the read is uncapped and filtered)", len(got))
	}
	// Ordering is (status_key, owner_scope, owner_id), not by recency: a
	// rebuild must be able to zip this against the ledger deterministically.
	if got[0].StatusKey != "reversible_body_state" || got[len(got)-1].StatusKey != "reversible_social_state" {
		t.Fatalf("order spans %q .. %q, want status_key ASC", got[0].StatusKey, got[len(got)-1].StatusKey)
	}
	if got[len(got)-1].OwnerID != "entity-zzz" {
		t.Fatalf("last owner = %q, want entity-zzz", got[len(got)-1].OwnerID)
	}
	for _, item := range got {
		if item.OwnerID == "entity-dead" || item.OwnerID == "entity-draft" {
			t.Fatalf("ineligible slot %q reached the current projection", item.OwnerID)
		}
	}

	// An empty key list short-circuits to a non-nil empty result, so the JSON
	// encoding is [] and not null.
	empty, err := st.ListReversibleStatusCurrentValues(ctx, scene.SessionID, "fictional_entity", nil)
	if err != nil {
		t.Fatalf("empty key read: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty key read = %#v, want a non-nil empty slice", empty)
	}
	blank, err := st.ListReversibleStatusCurrentValues(ctx, scene.SessionID, "fictional_entity", []string{"  ", ""})
	if err != nil {
		t.Fatalf("blank key read: %v", err)
	}
	if blank == nil || len(blank) != 0 {
		t.Fatalf("blank key read = %#v, want a non-nil empty slice", blank)
	}
	// A different owner scope is a different scope, not a wider one.
	scoped, err := st.ListReversibleStatusCurrentValues(ctx, scene.SessionID, "narrative_owner", []string{"reversible_body_state"})
	if err != nil {
		t.Fatalf("scoped read: %v", err)
	}
	if len(scoped) != 0 {
		t.Fatalf("rows for an unrelated owner scope = %d, want 0", len(scoped))
	}
}

// TestD1StoreListLatestReversibleCurrentProjectionEventsPicksTheRightWinner
// pins the supersession winner selection. The rows are built so that each group
// differs from the others ONLY in the tiebreak column, which is the only way a
// missing or added sort key can be caught:
//
//	group A: two events, identical observation turn -> the higher id must win.
//	group B: a later-id event with an EARLIER source turn -> the higher turn
//	         must win, so id alone is not enough.
//	group C: a repair whose repair_recorded_turn is far in the future, inserted
//	         first -> the repair must order after ordinary history instead of
//	         backdating itself to the historical cause turn.
//	group D: the newest event of all is rolled back -> it must not be chosen.
func TestD1StoreListLatestReversibleCurrentProjectionEventsPicksTheRightWinner(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")
	otherRegistry := d1StatusTransitionSeedRegistry(t, conn, scene.SessionID, "reversible_social_state", "fictional_entity")
	d1StatusTransitionSeedRevision(t, conn, scene.SessionID, "rev-dead", "turn-9", 9, "invalidated")

	live := func(unit string) string { return d1StatusTransitionEvidence(scene.Revision, unit, nil) }
	dead := func(unit string) string { return d1StatusTransitionEvidence("rev-dead", unit, nil) }

	// Group A — identical source turns, different ids.
	d1StatusTransitionSeedEvent(t, conn, 301, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-a", live("a-1"), 5)
	d1StatusTransitionSeedEvent(t, conn, 302, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-a", live("a-2"), 5)

	// Group B — the newer row has the EARLIER source turn.
	d1StatusTransitionSeedEvent(t, conn, 311, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-b", live("b-1"), 9)
	d1StatusTransitionSeedEvent(t, conn, 312, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-b", live("b-2"), 3)

	// Group C — a repair recorded late about an early turn.
	repairEvidence := d1StatusTransitionEvidence(scene.Revision, "c-1", map[string]any{
		"repair_recorded_turn": 20,
	})
	d1StatusTransitionSeedEvent(t, conn, 321, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-c", repairEvidence, 1)
	d1StatusTransitionSeedEvent(t, conn, 322, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-c", live("c-2"), 2)

	// Group D — the globally newest event belongs to a rolled-back source.
	d1StatusTransitionSeedEvent(t, conn, 331, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-d", live("d-1"), 4)
	d1StatusTransitionSeedEvent(t, conn, 332, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-d", dead("d-2"), 40)

	// A second status key so the presentation ORDER BY is exercised too.
	d1StatusTransitionSeedEvent(t, conn, 341, scene.SessionID, otherRegistry,
		"reversible_social_state", "fictional_entity", "entity-e", live("e-1"), 3)

	got, err := st.ListLatestReversibleCurrentProjectionEvents(ctx, scene.SessionID,
		[]string{"reversible_body_state", "reversible_social_state"})
	if err != nil {
		t.Fatalf("ListLatestReversibleCurrentProjectionEvents: %v", err)
	}
	want := []string{"302", "311", "321", "331", "341"}
	if !equalStringSlices(d1StatusTransitionEventIDs(got), want) {
		t.Fatalf("winners = %v, want %v (equal turns resolve by id; repair_recorded_turn beats source_turn; a rolled-back source never wins)",
			d1StatusTransitionEventIDs(got), want)
	}

	// Narrowing the key set narrows the read; it is not a union with a default.
	narrow, err := st.ListLatestReversibleCurrentProjectionEvents(ctx, scene.SessionID, []string{"reversible_social_state"})
	if err != nil {
		t.Fatalf("narrowed read: %v", err)
	}
	if !equalStringSlices(d1StatusTransitionEventIDs(narrow), []string{"341"}) {
		t.Fatalf("narrowed winners = %v, want [341]", d1StatusTransitionEventIDs(narrow))
	}
	empty, err := st.ListLatestReversibleCurrentProjectionEvents(ctx, scene.SessionID, nil)
	if err != nil {
		t.Fatalf("empty key read: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty key read = %#v, want a non-nil empty slice", empty)
	}
}

// TestD1StoreListLatestReversibleCurrentProjectionEventsExcludesNonProjections
// pins the second half of the predicate. An event that is not marked as a
// current projection is history, not a winner, however recent it is.
func TestD1StoreListLatestReversibleCurrentProjectionEventsExcludesNonProjections(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1StatusTransitionSeedScene(t, conn, "session-1")

	// No current_projection flag at all.
	d1StatusTransitionSeedEvent(t, conn, 401, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-a",
		`{"source_revision":"`+scene.Revision+`","source_unit_id":"a-1"}`, 3)
	// current_projection explicitly false.
	d1StatusTransitionSeedEvent(t, conn, 402, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-b",
		`{"source_revision":"`+scene.Revision+`","source_unit_id":"b-1","current_projection":false}`, 3)
	// The legacy JSON-string form, which SQLite's json_extract alone would drop.
	d1StatusTransitionSeedEvent(t, conn, 403, scene.SessionID, scene.BodyRegistry,
		"reversible_body_state", "fictional_entity", "entity-c",
		`{"source_revision":"`+scene.Revision+`","source_unit_id":"c-1","current_projection":"true"}`, 3)

	got, err := st.ListLatestReversibleCurrentProjectionEvents(ctx, scene.SessionID, []string{"reversible_body_state"})
	if err != nil {
		t.Fatalf("ListLatestReversibleCurrentProjectionEvents: %v", err)
	}
	if !equalStringSlices(d1StatusTransitionEventIDs(got), []string{"403"}) {
		t.Fatalf("winners = %v, want [403]: only a projection observation is a current value",
			d1StatusTransitionEventIDs(got))
	}
}

// ---------------------------------------------------------------------------
// supersession resolution
// ---------------------------------------------------------------------------

// TestD1StatusTransitionCapabilitiesAreAdvertised pins the delivery state
// itself. The HTTP layer gates these two features on optional interface
// assertions, so a D1 provider that implemented the methods but was not
// reachable through the assertion would leave the routes silently disabled —
// a parity gap that no behavioural test would ever surface.
func TestD1StatusTransitionCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var provider Store = st
	if _, ok := provider.(ReversibleStatusTransitionStore); !ok {
		t.Error("the D1 provider must expose ReversibleStatusTransitionStore")
	}
	if _, ok := provider.(SupersessionResolutionStore); !ok {
		t.Error("the D1 provider must expose SupersessionResolutionStore")
	}

	implemented := map[string]bool{}
	for _, status := range CapabilityReport(provider) {
		implemented[status.Name] = status.Implemented
	}
	for _, name := range []string{"ReversibleStatusTransitionStore", "SupersessionResolutionStore"} {
		if !implemented[name] {
			t.Errorf("capability report says %s is not implemented", name)
		}
	}
}

func d1SupersessionSeedEvidence(t *testing.T, conn *sqliteD1Conn, id int64, sessionID string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO direct_evidence_records (
		id, chat_session_id, evidence_kind, evidence_text, source_turn_start, source_turn_end,
		archive_state, capture_stage, capture_verification, committed_gate, repair_needed
	) VALUES (?, ?, 'fact_event', 'Mira was already in the vault.', 3, 3, 'pending_capture', 'critic_extract', 'pending', NULL, 1)`,
		id, sessionID); err != nil {
		t.Fatalf("seed direct evidence %d: %v", id, err)
	}
}

func d1SupersessionSeedTriple(t *testing.T, conn *sqliteD1Conn, id int64, sessionID string, validTo any) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO kg_triples (
		id, chat_session_id, subject, predicate, object, valid_from, source_turn
	) VALUES (?, ?, 'Mira', 'holds', 'iron key', 2, 2)`, id, sessionID); err != nil {
		t.Fatalf("seed kg triple %d: %v", id, err)
	}
	if validTo == nil {
		return
	}
	if _, err := conn.Exec(context.Background(), `UPDATE kg_triples SET valid_to = ? WHERE id = ?`, validTo, id); err != nil {
		t.Fatalf("stamp kg triple %d: %v", id, err)
	}
}

func d1SupersessionSeedPendingThread(t *testing.T, conn *sqliteD1Conn, id int64, sessionID, threadKey string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO pending_threads (
		id, chat_session_id, thread_key, description, status, created_turn, source_turn
	) VALUES (?, ?, ?, 'Recover the iron key', 'open', 2, 2)`, id, sessionID, threadKey); err != nil {
		t.Fatalf("seed pending thread %d: %v", id, err)
	}
}

// TestD1StoreSaveSupersessionResolutionAppliesTargetState covers all three
// target families and the normalisation that routes them through. The type and
// class aliases are not cosmetic: "kg" and "kg_triple" must reach the same
// UPDATE, and "stale" must demote rather than close.
func TestD1StoreSaveSupersessionResolutionAppliesTargetState(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	const sessionID = "session-supersede"

	d1SupersessionSeedEvidence(t, conn, 601, sessionID)
	d1SupersessionSeedTriple(t, conn, 611, sessionID, nil)
	d1SupersessionSeedTriple(t, conn, 612, sessionID, 1)
	d1SupersessionSeedPendingThread(t, conn, 621, sessionID, "key-thread")

	closed, err := st.SaveSupersessionResolution(ctx, &SupersessionResolutionDecision{
		ChatSessionID: sessionID, TargetType: "direct-evidence", TargetID: 601,
		SourceTurn: 5, ResolutionClass: "close", Reason: "  already covered  ", Operator: "critic",
	})
	if err != nil {
		t.Fatalf("close decision: %v", err)
	}
	if closed.TargetType != "direct_evidence" {
		t.Fatalf("target type = %q, want the normalised direct_evidence", closed.TargetType)
	}
	if closed.ResolutionClass != "close" || closed.Reason != "already covered" {
		t.Fatalf("decision = %+v, want the trimmed reason and normalised class", closed)
	}
	if closed.ID == 0 {
		t.Fatal("the audit row id was not read back after the commit")
	}
	if !strings.Contains(closed.DetailsJSON, SupersessionResolutionContractVersion) {
		t.Fatalf("details_json = %s, want the supersession contract version", closed.DetailsJSON)
	}
	if closed.Source != "critic" {
		t.Fatalf("source = %q, want the operator", closed.Source)
	}
	var archiveState, verification, gate string
	var repairNeeded int
	if err := conn.QueryRow(ctx, `
		SELECT archive_state, capture_verification, committed_gate, repair_needed
		FROM direct_evidence_records WHERE id = 601`).Scan(
		&archiveState, &verification, &gate, &repairNeeded); err != nil {
		t.Fatalf("read closed evidence: %v", err)
	}
	if archiveState != "closed_archive" || verification != "closed" || gate != "closed_by_resolution" {
		t.Fatalf("closed state = %q/%q/%q", archiveState, verification, gate)
	}
	if repairNeeded != 0 {
		t.Fatal("a resolved record must not stay in the repair queue")
	}

	d1SupersessionSeedEvidence(t, conn, 602, sessionID)
	if _, err := st.SaveSupersessionResolution(ctx, &SupersessionResolutionDecision{
		ChatSessionID: sessionID, TargetType: "direct_evidence", TargetID: 602,
		SourceTurn: 6, ResolutionClass: "refine", NewTargetID: 601,
	}); err != nil {
		t.Fatalf("refine decision: %v", err)
	}
	var supersededBy *int64
	if err := conn.QueryRow(ctx, `
		SELECT archive_state, superseded_by_id FROM direct_evidence_records WHERE id = 602`).Scan(
		&archiveState, &supersededBy); err != nil {
		t.Fatalf("read superseded evidence: %v", err)
	}
	if archiveState != "superseded_archive" {
		t.Fatalf("archive_state = %q, want superseded_archive", archiveState)
	}
	if supersededBy == nil || *supersededBy != 601 {
		t.Fatalf("superseded_by_id = %v, want 601", supersededBy)
	}

	// "kg" is an accepted alias for "kg_triple", and "stale" demotes rather
	// than closing: the triple's interval ends, the row survives.
	if _, err := st.SaveSupersessionResolution(ctx, &SupersessionResolutionDecision{
		ChatSessionID: sessionID, TargetType: "kg", TargetID: 611,
		SourceTurn: 7, ResolutionClass: "stale",
	}); err != nil {
		t.Fatalf("stale demote decision: %v", err)
	}
	var validTo *int64
	if err := conn.QueryRow(ctx, `SELECT valid_to FROM kg_triples WHERE id = 611`).Scan(&validTo); err != nil {
		t.Fatalf("read demoted triple: %v", err)
	}
	if validTo == nil || *validTo != 7 {
		t.Fatalf("valid_to = %v, want 7", validTo)
	}
	// The guard is one-directional: a triple already closed before this turn
	// keeps its end date, so re-resolving cannot drag it backwards.
	if _, err := st.SaveSupersessionResolution(ctx, &SupersessionResolutionDecision{
		ChatSessionID: sessionID, TargetType: "kg_triple", TargetID: 612,
		SourceTurn: 3, ResolutionClass: "close",
	}); err != nil {
		t.Fatalf("close on an already closed triple: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT valid_to FROM kg_triples WHERE id = 612`).Scan(&validTo); err != nil {
		t.Fatalf("read already closed triple: %v", err)
	}
	if validTo == nil || *validTo != 1 {
		t.Fatalf("valid_to = %v, want the earlier 1 to be preserved", validTo)
	}

	if _, err := st.SaveSupersessionResolution(ctx, &SupersessionResolutionDecision{
		ChatSessionID: sessionID, TargetType: "pending_threads", TargetID: 621,
		SourceTurn: 8, ResolutionClass: "close",
	}); err != nil {
		t.Fatalf("pending thread decision: %v", err)
	}
	var status string
	var resolvedTurn *int64
	if err := conn.QueryRow(ctx, `SELECT status, resolved_turn FROM pending_threads WHERE id = 621`).Scan(
		&status, &resolvedTurn); err != nil {
		t.Fatalf("read resolved thread: %v", err)
	}
	if status != "resolved" || resolvedTurn == nil || *resolvedTurn != 8 {
		t.Fatalf("thread = %q at turn %v, want resolved at 8", status, resolvedTurn)
	}

	// A soft demote leaves the record readable: that is the whole difference
	// between soft_demote and close.
	d1SupersessionSeedEvidence(t, conn, 603, sessionID)
	if _, err := st.SaveSupersessionResolution(ctx, &SupersessionResolutionDecision{
		ChatSessionID: sessionID, TargetType: "direct_evidence", TargetID: 603,
		SourceTurn: 9, ResolutionClass: "demote",
	}); err != nil {
		t.Fatalf("soft demote decision: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT archive_state FROM direct_evidence_records WHERE id = 603`).Scan(&archiveState); err != nil {
		t.Fatalf("read demoted evidence: %v", err)
	}
	if archiveState != "pending_capture" {
		t.Fatalf("archive_state = %q, want the soft demote to leave the record readable", archiveState)
	}
}

// TestD1StoreSaveSupersessionResolutionRejectsIncompleteDecisions pins the
// validation. A decision without a session, target or positive target id cannot
// be projected back out of details_json, so it must be refused before the audit
// row is written.
func TestD1StoreSaveSupersessionResolutionRejectsIncompleteDecisions(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for name, decision := range map[string]*SupersessionResolutionDecision{
		"missing_session": {TargetType: "kg_triple", TargetID: 1},
		"missing_target":  {ChatSessionID: "s", TargetID: 1},
		"non_positive_id": {ChatSessionID: "s", TargetType: "kg_triple", TargetID: 0},
		"nil_decision":    nil,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := st.SaveSupersessionResolution(ctx, decision); err == nil {
				t.Fatal("incomplete decision was accepted")
			}
		})
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM audit_logs`); got != 0 {
		t.Fatalf("audit rows = %d, want 0", got)
	}
}

// TestD1StoreListSupersessionResolutionsReadsTheAuditTrail newest-first pins
// the read model. The record is a VIEW over the audit row, so the ordering is
// recency and the projection has to survive a details_json round trip.
func TestD1StoreListSupersessionResolutionsReadsTheAuditTrailNewestFirst(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	const sessionID = "session-trail"

	for i, targetID := range []int64{901, 902, 903} {
		if _, err := st.SaveSupersessionResolution(ctx, &SupersessionResolutionDecision{
			ChatSessionID:   sessionID,
			TargetType:      "kg_triple",
			TargetID:        targetID,
			SourceTurn:      3 + i,
			ResolutionClass: "supersede",
			NewTargetType:   "kg_triple",
			NewTargetID:     targetID + 100,
			RelationshipKey: "holds",
			Reason:          "replaced",
		}); err != nil {
			t.Fatalf("save resolution %d: %v", targetID, err)
		}
	}
	if _, err := st.SaveSupersessionResolution(ctx, &SupersessionResolutionDecision{
		ChatSessionID: "other-session", TargetType: "kg_triple", TargetID: 950,
	}); err != nil {
		t.Fatalf("save resolution in another session: %v", err)
	}

	got, err := st.ListSupersessionResolutions(ctx, sessionID, 0)
	if err != nil {
		t.Fatalf("ListSupersessionResolutions: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("rows = %d, want 3", len(got))
	}
	// Text timestamps order chronologically, so created_at DESC is recency.
	for i := 1; i < len(got); i++ {
		if got[i].CreatedAt.After(got[i-1].CreatedAt) {
			t.Fatalf("rows are not newest-first: %v then %v", got[i-1].CreatedAt, got[i].CreatedAt)
		}
	}
	head := got[0]
	if head.TargetID != 903 || head.NewTargetID != 1003 || head.NewTargetType != "kg_triple" {
		t.Fatalf("head record = %+v, want the third decision", head)
	}
	if head.ResolutionClass != "supersede" || head.SourceTurn != 5 || head.RelationshipKey != "holds" || head.Reason != "replaced" {
		t.Fatalf("head record projection = %+v", head)
	}
	if head.ChatSessionID != sessionID || head.Source != "critic" {
		t.Fatalf("head record identity = %+v", head)
	}
	// A non-positive limit means "no limit" on this read: all three rows.
	if limited, err := st.ListSupersessionResolutions(ctx, sessionID, -1); err != nil || len(limited) != 3 {
		t.Fatalf("limit -1 = %d rows (err %v), want the complete trail", len(limited), err)
	}
	if limited, err := st.ListSupersessionResolutions(ctx, sessionID, 2); err != nil || len(limited) != 2 {
		t.Fatalf("limit 2 = %d rows (err %v), want 2", len(limited), err)
	}
	empty, err := st.ListSupersessionResolutions(ctx, "session-with-nothing", 0)
	if err != nil {
		t.Fatalf("empty read: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("empty read = %#v, want a non-nil empty slice", empty)
	}
}
