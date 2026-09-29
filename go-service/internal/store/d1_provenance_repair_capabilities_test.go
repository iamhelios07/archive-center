package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// D1 provenance-repair capability tests.
//
// Every test runs against the real SQLite engine through the same D1 transport
// the Worker uses, so these assert the statements D1 actually executes rather
// than a mock's idea of them.
//
// The seeds are shaped around the failure modes that matter for this slice:
//
//   - a repair that is replayed must not append a second snapshot or event, and
//     a repair whose audit insert fails must leave the snapshot unwritten;
//   - an artifact preview that offers a memory belonging to a rolled-back source
//     revision is asking the user to delete state that no longer exists;
//   - a recovery cache that returns a delete tombstone, or the newest
//     materialization first, silently re-embeds the wrong thing;
//   - a discovery job that answers "found" for an unknown id, or that stores an
//     untrimmed query, breaks the route's 404 and its resume-from-request path.

const d1ProvenanceRepairEntityID = "ent-mira"

// ---------------------------------------------------------------------------
// seeds
// ---------------------------------------------------------------------------

// d1StateRepairRegistry inserts the status_schema_registry row that status
// current values and change events must reference. The registry is per
// status_key, so a test seeds one per key it needs.
func d1StateRepairRegistry(t *testing.T, conn *sqliteD1Conn, sessionID, statusKey string) int64 {
	t.Helper()
	var id int64
	if err := conn.QueryRow(context.Background(), `INSERT INTO status_schema_registry
		(chat_session_id, status_key, label, owner_scope, value_kind)
		VALUES (?, ?, ?, 'character', 'object') RETURNING id`,
		sessionID, statusKey, statusKey).Scan(&id); err != nil {
		t.Fatalf("seed status registry %s: %v", statusKey, err)
	}
	return id
}

// d1StateRepairEventSeed describes one status_change_events row. The evidence
// payload is the whole point: it is where a body_tracking event records the
// source revision and the direct evidence ids that link it back to memories.
type d1StateRepairEventSeed struct {
	sessionID  string
	registryID int64
	statusKey  string
	ownerID    string
	eventKind  string
	sourceTurn int
	evidence   string
	previous   string
	current    string
}

func d1StateRepairEvent(t *testing.T, conn *sqliteD1Conn, seed d1StateRepairEventSeed) {
	t.Helper()
	if seed.eventKind == "" {
		seed.eventKind = "observed"
	}
	if _, err := conn.Exec(context.Background(), `INSERT INTO status_change_events
		(chat_session_id, registry_id, status_key, owner_scope, owner_id, event_kind,
		 previous_value_json, new_value_json, evidence_json, source_turn)
		VALUES (?, ?, ?, 'character', ?, ?, ?, ?, ?, ?)`,
		seed.sessionID, seed.registryID, seed.statusKey, seed.ownerID, seed.eventKind,
		d1NullableString(seed.previous), d1NullableString(seed.current), seed.evidence, seed.sourceTurn); err != nil {
		t.Fatalf("seed status event %s/%s: %v", seed.statusKey, seed.ownerID, err)
	}
}

// d1StateRepairCurrentValue inserts one status_current_values row.
func d1StateRepairCurrentValue(t *testing.T, conn *sqliteD1Conn, sessionID string, registryID int64, statusKey, ownerID, ownerLabel, valueJSON string) int64 {
	t.Helper()
	var id int64
	if err := conn.QueryRow(context.Background(), `INSERT INTO status_current_values
		(chat_session_id, registry_id, status_key, owner_scope, owner_id, owner_label,
		 value_kind, value_json, evidence_json)
		VALUES (?, ?, ?, 'character', ?, ?, 'object', ?, '{}') RETURNING id`,
		sessionID, registryID, statusKey, ownerID, ownerLabel, valueJSON).Scan(&id); err != nil {
		t.Fatalf("seed current value %s/%s: %v", statusKey, ownerID, err)
	}
	return id
}

// d1StateRepairKeys reduces an artifact plan to "table:id" keys, so an ordering
// or membership assertion names the rows it expects.
func d1StateRepairKeys(changes []StateRepairArtifactChange) []string {
	out := make([]string, 0, len(changes))
	for _, change := range changes {
		out = append(out, fmt.Sprintf("%s:%d", change.Table, change.ID))
	}
	return out
}

// d1StateRepairFind returns the change for one table row and fails when it is
// absent, so a test never silently asserts against the wrong row.
func d1StateRepairFind(t *testing.T, changes []StateRepairArtifactChange, table string, id int64) StateRepairArtifactChange {
	t.Helper()
	for _, change := range changes {
		if change.Table == table && change.ID == id {
			return change
		}
	}
	t.Fatalf("no repair artifact for %s:%d in %v", table, id, d1StateRepairKeys(changes))
	return StateRepairArtifactChange{}
}

func d1StateRepairValue(t *testing.T, change StateRepairArtifactChange, key string) string {
	t.Helper()
	value, ok := change.After[key]
	if !ok {
		t.Fatalf("repair artifact for %s:%d has no %q entry: after=%v", change.Table, change.ID, key, change.After)
	}
	if value == nil {
		return ""
	}
	return *value
}

func d1StateRepairBefore(t *testing.T, change StateRepairArtifactChange, key string) string {
	t.Helper()
	value, ok := change.Before[key]
	if !ok {
		t.Fatalf("repair artifact for %s:%d has no %q before entry", change.Table, change.ID, key)
	}
	if value == nil {
		return ""
	}
	return *value
}

// d1StateRepairVectorUpsert inserts an outbox row so a document id counts as
// already materialized.
func d1StateRepairVectorUpsert(t *testing.T, conn *sqliteD1Conn, revision, documentID, documentJSON string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO memory_vector_outbox
		(operation_key, operation, chat_session_id, source_revision, document_id, document_json,
		 embedding_ready, required_source_state, status)
		VALUES (?, 'upsert', 's1', ?, ?, ?, 1, 'active', 'completed')`,
		"key-"+documentID, revision, documentID, documentJSON); err != nil {
		t.Fatalf("seed vector outbox %s: %v", documentID, err)
	}
}

// ---------------------------------------------------------------------------
// 1. CharacterProvenanceRepairStore
// ---------------------------------------------------------------------------

// TestD1ApplyCharacterProvenanceRepairAppendsSnapshotAndEvent pins the success
// shape: the corrected snapshot and its audit event are both written, the event
// reports the row id the insert produced, and the recorded "after" is the
// snapshot that was actually stored rather than the caller's request.
func TestD1ApplyCharacterProvenanceRepairAppendsSnapshotAndEvent(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	before := CharacterState{ChatSessionID: "s1", CharacterName: "Mira", FieldProvenanceJSON: `{"appearance.hair":"source"}`, TurnIndex: 4}
	after := CharacterState{
		ChatSessionID: "s1", CharacterName: "Mira",
		AppearanceJSON: `{"hair":"black"}`, FieldProvenanceJSON: `{"appearance.hair":"operator"}`, TurnIndex: 4,
	}

	event, err := st.ApplyCharacterProvenanceRepair(ctx, before, after, "repair-coins")
	if err != nil {
		t.Fatalf("ApplyCharacterProvenanceRepair: %v", err)
	}
	if event.ID <= 0 {
		t.Fatalf("event id = %d, want the committed row id", event.ID)
	}
	if event.EventType != "field_provenance_repair" || event.ChatSessionID != "s1" || event.CharacterName != "Mira" {
		t.Errorf("event identity = %+v, want a Mira field_provenance_repair", event)
	}
	if event.TurnIndex != 4 {
		t.Errorf("event turn = %d, want 4 (the repaired snapshot's turn)", event.TurnIndex)
	}
	if event.CreatedAt.IsZero() {
		t.Error("event created_at must be parsed, not left zero")
	}

	var recorded struct {
		OperationID string         `json:"operation_id"`
		Before      CharacterState `json:"before"`
		After       CharacterState `json:"after"`
	}
	if err := json.Unmarshal([]byte(event.DetailsJSON), &recorded); err != nil {
		t.Fatalf("event details are not JSON: %v", err)
	}
	if recorded.OperationID != "repair-coins" {
		t.Errorf("recorded operation id = %q, want repair-coins", recorded.OperationID)
	}
	if recorded.Before.FieldProvenanceJSON != before.FieldProvenanceJSON {
		t.Errorf("recorded before = %+v, want the operator's supplied snapshot", recorded.Before)
	}
	if recorded.After.CreatedAt.IsZero() || !recorded.After.CreatedAt.Equal(event.CreatedAt) {
		t.Errorf("recorded after stamps = %s, want the repair instant %s; the audit and the row must agree",
			recorded.After.CreatedAt, event.CreatedAt)
	}

	// The snapshot is appended, not updated: the history table must keep the row
	// the repair replaced or the undo would have nothing to restore.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM character_states WHERE chat_session_id = 's1' AND character_name = 'Mira'`); got != 1 {
		t.Errorf("character_states rows = %d, want 1 appended snapshot", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM character_events WHERE event_type = 'field_provenance_repair'`); got != 1 {
		t.Errorf("repair events = %d, want 1", got)
	}

	// The stored snapshot is readable through the ordinary read path, so the
	// repaired provenance actually reaches prompt assembly.
	stored, err := st.GetCharacterState(ctx, "s1", "Mira")
	if err != nil {
		t.Fatalf("GetCharacterState after repair: %v", err)
	}
	if stored.FieldProvenanceJSON != `{"appearance.hair":"operator"}` || stored.AppearanceJSON != `{"hair":"black"}` {
		t.Errorf("stored snapshot = %+v, want the repaired provenance", stored)
	}
}

// TestD1ApplyCharacterProvenanceRepairStoresAbsentJSONAsNull pins the
// nullableJSONText mapping. Persisting the literal text "null" instead of SQL
// NULL would make a caller that checks for a non-empty field see a four
// character string where MariaDB reports an absent column.
func TestD1ApplyCharacterProvenanceRepairStoresAbsentJSONAsNull(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	after := CharacterState{
		ChatSessionID: "s1", CharacterName: "Mira", TurnIndex: 1,
		FieldProvenanceJSON: `{"a":1}`, StatusJSON: "null", PersonalityJSON: "   ",
	}
	if _, err := st.ApplyCharacterProvenanceRepair(ctx, CharacterState{ChatSessionID: "s1", CharacterName: "Mira"}, after, "op-null"); err != nil {
		t.Fatalf("ApplyCharacterProvenanceRepair: %v", err)
	}

	var status, personality, appearance *string
	if err := conn.QueryRow(ctx, `SELECT status_json, personality_json, appearance_json
		FROM character_states WHERE chat_session_id = 's1' AND character_name = 'Mira'`).Scan(
		&status, &personality, &appearance); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if status != nil || personality != nil || appearance != nil {
		t.Errorf("absent JSON columns = (%v, %v, %v), want all NULL", status, personality, appearance)
	}

	// Through the read path the same columns come back as empty text, not "null".
	stored, err := st.GetCharacterState(ctx, "s1", "Mira")
	if err != nil {
		t.Fatalf("GetCharacterState: %v", err)
	}
	if stored.StatusJSON != "" || stored.PersonalityJSON != "" || stored.AppearanceJSON != "" {
		t.Errorf("NULL JSON columns must read as empty text: %+v", stored)
	}
}

// TestD1ApplyCharacterProvenanceRepairReplaysByOperationID pins idempotency. A
// retried request must return the stored event and append nothing; a different
// operation id is a different repair and must append.
func TestD1ApplyCharacterProvenanceRepairReplaysByOperationID(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	before := CharacterState{ChatSessionID: "s1", CharacterName: "Mira", FieldProvenanceJSON: `{"a":"source"}`}
	after := CharacterState{ChatSessionID: "s1", CharacterName: "Mira", FieldProvenanceJSON: `{"a":"operator"}`, TurnIndex: 2}

	first, err := st.ApplyCharacterProvenanceRepair(ctx, before, after, "repair-one")
	if err != nil {
		t.Fatalf("first repair: %v", err)
	}
	replay, err := st.ApplyCharacterProvenanceRepair(ctx, before, after, "repair-one")
	if err != nil {
		t.Fatalf("replayed repair: %v", err)
	}
	if replay.ID != first.ID || replay.DetailsJSON != first.DetailsJSON {
		t.Errorf("replay = %+v, want the stored event %+v", replay, first)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM character_states`); got != 1 {
		t.Errorf("character_states rows = %d after a replay, want 1 (a replay must not append)", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM character_events WHERE event_type = 'field_provenance_repair'`); got != 1 {
		t.Errorf("repair events = %d after a replay, want 1", got)
	}

	// A different operation id is a different repair, even for the same character
	// and the same turn, so it must append rather than be swallowed as a replay.
	next := after
	next.FieldProvenanceJSON = `{"a":"operator-2"}`
	second, err := st.ApplyCharacterProvenanceRepair(ctx, after, next, "repair-two")
	if err != nil {
		t.Fatalf("second repair: %v", err)
	}
	if second.ID == first.ID {
		t.Error("a different operation id must not resolve to the first repair's event")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM character_states`); got != 2 {
		t.Errorf("character_states rows = %d, want 2 after a distinct operation", got)
	}
}

// TestD1ApplyCharacterProvenanceRepairIsolatesOperationsPerCharacter pins the
// lookup's session and character scope. A repair id is only a replay for the
// same character, or the same id applied to a second character would be
// silently dropped and that character would never be repaired.
func TestD1ApplyCharacterProvenanceRepairIsolatesOperationsPerCharacter(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	first, err := st.ApplyCharacterProvenanceRepair(ctx,
		CharacterState{ChatSessionID: "s1", CharacterName: "Mira"},
		CharacterState{ChatSessionID: "s1", CharacterName: "Mira", FieldProvenanceJSON: `{"a":1}`}, "shared-op")
	if err != nil {
		t.Fatalf("Mira repair: %v", err)
	}
	second, err := st.ApplyCharacterProvenanceRepair(ctx,
		CharacterState{ChatSessionID: "s1", CharacterName: "Sasha"},
		CharacterState{ChatSessionID: "s1", CharacterName: "Sasha", FieldProvenanceJSON: `{"a":1}`}, "shared-op")
	if err != nil {
		t.Fatalf("Sasha repair: %v", err)
	}
	if second.ID == first.ID {
		t.Error("the same operation id on another character must produce its own event")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM character_states`); got != 2 {
		t.Errorf("character_states rows = %d, want one per character", got)
	}
}

// TestD1ApplyCharacterProvenanceRepairWritesNothingWhenTheEventFails is the
// atomicity test. A trigger rejects the audit insert after the snapshot insert
// has already succeeded inside the same batch, so a provider that committed
// the statements independently would leave a corrected character with no undo
// record.
func TestD1ApplyCharacterProvenanceRepairWritesNothingWhenTheEventFails(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if _, err := conn.Exec(ctx, `CREATE TRIGGER d1_provenance_repair_reject
		BEFORE INSERT ON character_events WHEN NEW.event_type = 'field_provenance_repair'
		BEGIN SELECT RAISE(ABORT, 'audit insert rejected'); END`); err != nil {
		t.Fatalf("create rejection trigger: %v", err)
	}

	event, err := st.ApplyCharacterProvenanceRepair(ctx,
		CharacterState{ChatSessionID: "s1", CharacterName: "Mira"},
		CharacterState{ChatSessionID: "s1", CharacterName: "Mira", FieldProvenanceJSON: `{"a":1}`}, "op-fails")
	if err == nil {
		t.Fatal("a repair whose audit insert fails must report the failure")
	}
	if event.ID != 0 {
		t.Errorf("failed repair returned event %+v, want the zero value", event)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM character_states`); got != 0 {
		t.Errorf("character_states rows = %d, want 0: the snapshot must roll back with the event", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM character_events`); got != 0 {
		t.Errorf("character_events rows = %d, want 0", got)
	}

	// Once the trigger is gone the same operation succeeds, so the failure above
	// was the trigger and not a permanently poisoned statement.
	if _, err := conn.Exec(ctx, `DROP TRIGGER d1_provenance_repair_reject`); err != nil {
		t.Fatalf("drop trigger: %v", err)
	}
	if _, err := st.ApplyCharacterProvenanceRepair(ctx,
		CharacterState{ChatSessionID: "s1", CharacterName: "Mira"},
		CharacterState{ChatSessionID: "s1", CharacterName: "Mira", FieldProvenanceJSON: `{"a":1}`}, "op-fails"); err != nil {
		t.Fatalf("repair after dropping the trigger: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM character_states`); got != 1 {
		t.Errorf("character_states rows = %d, want 1 after the repair succeeded", got)
	}
}

// ---------------------------------------------------------------------------
// 2. StateRepairArtifactReader
// ---------------------------------------------------------------------------

// TestD1ListBodyRepairArtifactsIsEmptyNonNilWithoutData pins the JSON shape: a
// character with no stored body data yields an empty non-nil slice, so the
// delete plan encodes as [] on both providers.
func TestD1ListBodyRepairArtifactsIsEmptyNonNilWithoutData(t *testing.T) {
	st, _ := newD1TestStore(t)

	changes, err := st.ListBodyRepairArtifacts(context.Background(), "s1", d1ProvenanceRepairEntityID, "Mira")
	if err != nil {
		t.Fatalf("ListBodyRepairArtifacts: %v", err)
	}
	if changes == nil || len(changes) != 0 {
		t.Errorf("artifacts = %#v, want an empty non-nil slice", changes)
	}
	encoded, err := json.Marshal(changes)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(encoded) != "[]" {
		t.Errorf("encoded artifacts = %s, want []", encoded)
	}
}

// TestD1ListBodyRepairArtifactsSelectsTopicRowsAndSkipsCleanOnes pins the
// selection rule: a row is offered when its own text names the character or
// matches the body topic, and the planned after value is the family's clear
// value. It also pins the integer-column coercion, because pending_threads
// suppressed/user_corrected are INTEGER here and the plan is expressed in text.
func TestD1ListBodyRepairArtifactsSelectsTopicRowsAndSkipsCleanOnes(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	// Selection needs BOTH axes: the row's own text must match a body topic AND
	// name this character (by entity id or by display name, case-insensitively).
	// Dropping either half either scrubs every character or scrubs strangers.
	var bodyMemory, cleanMemory, strayMemory int64
	if err := conn.QueryRow(ctx, `INSERT INTO memories
		(chat_session_id, turn_index, summary_json) VALUES ('s1', 5, ?) RETURNING id`,
		`{"note":"Mira: the pregnancy is progressing"}`).Scan(&bodyMemory); err != nil {
		t.Fatalf("seed body memory: %v", err)
	}
	// Names the character but has no body content: the topic axis must exclude it.
	if err := conn.QueryRow(ctx, `INSERT INTO memories
		(chat_session_id, turn_index, summary_json) VALUES ('s1', 6, ?) RETURNING id`,
		`{"note":"Mira: the archive is quiet"}`).Scan(&cleanMemory); err != nil {
		t.Fatalf("seed clean memory: %v", err)
	}
	// Has body content but describes somebody else: the mention axis must exclude
	// it, so a shared scene is not silently rewritten into a Mira deletion.
	if err := conn.QueryRow(ctx, `INSERT INTO memories
		(chat_session_id, turn_index, summary_json) VALUES ('s1', 7, ?) RETURNING id`,
		`{"note":"Sasha: the pregnancy is progressing"}`).Scan(&strayMemory); err != nil {
		t.Fatalf("seed stray memory: %v", err)
	}
	// The mention may be the stable entity id instead of the display name.
	var namedMemory int64
	if err := conn.QueryRow(ctx, `INSERT INTO memories
		(chat_session_id, turn_index, summary_json) VALUES ('s1', 8, ?) RETURNING id`,
		`{"note":"`+d1ProvenanceRepairEntityID+` recorded the pregnancy"}`).Scan(&namedMemory); err != nil {
		t.Fatalf("seed named memory: %v", err)
	}

	var thread int64
	if err := conn.QueryRow(ctx, `INSERT INTO pending_threads
		(chat_session_id, thread_key, description, status, suppressed, user_corrected)
		VALUES ('s1', 'body-plan', ?, 'open', 0, 1) RETURNING id`,
		"Mira's postpartum recovery steps").Scan(&thread); err != nil {
		t.Fatalf("seed pending thread: %v", err)
	}
	// A thread with nothing to clear must not be offered at all.
	var quietThread int64
	if err := conn.QueryRow(ctx, `INSERT INTO pending_threads
		(chat_session_id, thread_key, description, status, suppressed, user_corrected)
		VALUES ('s1', 'quiet', ?, 'open', 1, 1) RETURNING id`,
		"Mira: the archive is quiet").Scan(&quietThread); err != nil {
		t.Fatalf("seed clean thread: %v", err)
	}

	changes, err := st.ListBodyRepairArtifacts(ctx, "s1", d1ProvenanceRepairEntityID, "Mira")
	if err != nil {
		t.Fatalf("ListBodyRepairArtifacts: %v", err)
	}
	keys := d1StateRepairKeys(changes)
	for _, want := range []string{
		fmt.Sprintf("memories:%d", bodyMemory), fmt.Sprintf("memories:%d", namedMemory),
		fmt.Sprintf("pending_threads:%d", thread),
	} {
		if !containsD1StateRepairKey(keys, want) {
			t.Errorf("missing artifact %s in %v", want, keys)
		}
	}
	if containsD1StateRepairKey(keys, fmt.Sprintf("memories:%d", cleanMemory)) {
		t.Errorf("a memory without body content must not be offered: %v", keys)
	}
	if containsD1StateRepairKey(keys, fmt.Sprintf("memories:%d", strayMemory)) {
		t.Errorf("another character's memory must not be offered: %v", keys)
	}
	if containsD1StateRepairKey(keys, fmt.Sprintf("pending_threads:%d", quietThread)) {
		t.Errorf("a thread with nothing to clear must not be offered: %v", keys)
	}

	body := d1StateRepairFind(t, changes, "memories", bodyMemory)
	if got := d1StateRepairBefore(t, body, "summary_json"); got != `{"note":"Mira: the pregnancy is progressing"}` {
		t.Errorf("memory before = %q, want the stored summary", got)
	}
	if got := d1StateRepairValue(t, body, "summary_json"); got != "{}" {
		t.Errorf("memory after = %q, want the cleared container", got)
	}

	// A NULL non-partial column is materialised by the plan: the reference
	// records Before as nil and still writes the clear value, because "absent"
	// and "cleared" are different states the undo has to be able to tell apart.
	if value, ok := body.Before["embedding"]; !ok || value != nil {
		t.Errorf("NULL embedding before = %v, want a recorded nil", value)
	}
	if got := d1StateRepairValue(t, body, "embedding"); got != "[]" {
		t.Errorf("embedding after = %q, want the cleared list", got)
	}

	// The integer columns must plan as the reference's decimal text. A driver
	// that kept them numeric would produce a different before value and a
	// silently different undo.
	plan := d1StateRepairFind(t, changes, "pending_threads", thread)
	if got := d1StateRepairBefore(t, plan, "suppressed"); got != "0" {
		t.Errorf("suppressed before = %q, want %q", got, "0")
	}
	if got := d1StateRepairValue(t, plan, "suppressed"); got != "1" {
		t.Errorf("suppressed after = %q, want %q", got, "1")
	}
	// user_corrected is already at its clear value, so it is absent from the
	// plan entirely rather than rewritten to itself.
	if _, ok := plan.After["user_corrected"]; ok {
		t.Errorf("an already-cleared column must not appear in the plan: %v", plan.After)
	}
	if got := d1StateRepairValue(t, plan, "description"); got != "" {
		t.Errorf("description after = %q, want it cleared", got)
	}
}

func containsD1StateRepairKey(keys []string, want string) bool {
	for _, key := range keys {
		if key == want {
			return true
		}
	}
	return false
}

// TestD1ListBodyRepairArtifactsPrunesSharedJSONContainers pins the partial
// families. A character snapshot and a status value hold unrelated fields in
// the same document, so the plan must remove only the body-related fields
// instead of replacing the whole container with "{}".
func TestD1ListBodyRepairArtifactsPrunesSharedJSONContainers(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	var pruned, emptied, untouched int64
	if err := conn.QueryRow(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, appearance_json, status_json, field_provenance_json, turn_index)
		VALUES ('s1', ?, '{"hair":"black"}', ?, '{"appearance.hair":"source"}', 3) RETURNING id`,
		d1ProvenanceRepairEntityID, `{"mood":"calm","body":"pregnancy recovery_started"}`).Scan(&pruned); err != nil {
		t.Fatalf("seed prunable snapshot: %v", err)
	}
	if err := conn.QueryRow(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, status_json, turn_index)
		VALUES ('s1', ?, '{"body":"pregnancy"}', 4) RETURNING id`, d1ProvenanceRepairEntityID).Scan(&emptied); err != nil {
		t.Fatalf("seed emptied snapshot: %v", err)
	}
	// Selected by the mention test, but every column is either NULL or already
	// free of body content, so there is nothing to plan and the row is dropped.
	if err := conn.QueryRow(ctx, `INSERT INTO character_states
		(chat_session_id, character_name, field_provenance_json, turn_index)
		VALUES ('s1', ?, '{"note":"kept"}', 5) RETURNING id`, d1ProvenanceRepairEntityID).Scan(&untouched); err != nil {
		t.Fatalf("seed untouched snapshot: %v", err)
	}

	registryID := d1StateRepairRegistry(t, conn, "s1", "hp")
	hpValue := d1StateRepairCurrentValue(t, conn, "s1", registryID, "hp", d1ProvenanceRepairEntityID, "Mira",
		`{"hp":12,"note":"pregnancy"}`)

	changes, err := st.ListBodyRepairArtifacts(ctx, "s1", d1ProvenanceRepairEntityID, "Mira")
	if err != nil {
		t.Fatalf("ListBodyRepairArtifacts: %v", err)
	}

	prunedChange := d1StateRepairFind(t, changes, "character_states", pruned)
	if got := d1StateRepairValue(t, prunedChange, "status_json"); got != `{"mood":"calm"}` {
		t.Errorf("pruned status = %q, want the unrelated mood field kept", got)
	}
	// Appearance carries no body content, so pruning leaves it byte-identical and
	// the already-equal test removes it from the plan.
	if _, ok := prunedChange.After["appearance_json"]; ok {
		t.Errorf("an unchanged appearance must not be planned: %v", prunedChange.After)
	}
	if _, ok := prunedChange.After["field_provenance_json"]; ok {
		t.Errorf("unchanged provenance must not be planned: %v", prunedChange.After)
	}

	emptiedChange := d1StateRepairFind(t, changes, "character_states", emptied)
	if got := d1StateRepairValue(t, emptiedChange, "status_json"); got != "{}" {
		t.Errorf("emptied status = %q, want the collapsed container", got)
	}
	if containsD1StateRepairKey(d1StateRepairKeys(changes), fmt.Sprintf("character_states:%d", untouched)) {
		t.Error("a row with nothing to clear must not be offered for repair")
	}

	valueChange := d1StateRepairFind(t, changes, "status_current_values", hpValue)
	if got := d1StateRepairValue(t, valueChange, "value_json"); got != `{"hp":12}` {
		t.Errorf("pruned current value = %q, want the unrelated hp field kept", got)
	}
}

// TestD1ListBodyRepairArtifactsExcludesBodyTrackingAndStoryClockStatusRows
// pins the two excluded status keys. Offering to clear the body_tracking record
// that describes the deletion would make the plan destroy the evidence of its
// own execution, and story_clock is the turn marker the repair records itself
// against.
func TestD1ListBodyRepairArtifactsExcludesBodyTrackingAndStoryClockStatusRows(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	bodyRegistry := d1StateRepairRegistry(t, conn, "s1", "body_tracking")
	clockRegistry := d1StateRepairRegistry(t, conn, "s1", "story_clock")

	d1StateRepairEvent(t, conn, d1StateRepairEventSeed{
		sessionID: "s1", registryID: bodyRegistry, statusKey: "body_tracking",
		ownerID: d1ProvenanceRepairEntityID, sourceTurn: 4,
		current:   `{"deleted":true,"body":"pregnancy"}`,
		evidence:  `{"source_revision":"s1-rev-live"}`,
		eventKind: "author_body_delete",
	})
	d1StateRepairEvent(t, conn, d1StateRepairEventSeed{
		sessionID: "s1", registryID: clockRegistry, statusKey: "story_clock",
		ownerID: d1ProvenanceRepairEntityID, sourceTurn: 4,
		previous:  `{"body":"pregnancy"}`,
		current:   `{"turn":9}`,
		evidence:  `{"source_revision":"s1-rev-live"}`,
		eventKind: "advance",
	})

	changes, err := st.ListBodyRepairArtifacts(ctx, "s1", d1ProvenanceRepairEntityID, "Mira")
	if err != nil {
		t.Fatalf("ListBodyRepairArtifacts: %v", err)
	}
	keys := d1StateRepairKeys(changes)
	if len(keys) != 0 {
		t.Fatalf("artifacts = %v, want none: the only body-bearing rows are the excluded status keys", keys)
	}
}

// TestD1ListBodyRepairArtifactsLinksOnlyActiveSourceRevisions pins the fence.
// A body_tracking event whose source revision was superseded links nothing, so
// the preview must not offer to delete the memories and evidence that were
// derived from state the user has already rolled back. An event that names no
// revision links nothing either: there is no admitted source to inherit from.
func TestD1ListBodyRepairArtifactsLinksOnlyActiveSourceRevisions(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-dead", "turn-2", "superseded")

	registryID := d1StateRepairRegistry(t, conn, "s1", "body_tracking")
	// The live revision links turn 5 and evidence row 41.
	d1StateRepairEvent(t, conn, d1StateRepairEventSeed{
		sessionID: "s1", registryID: registryID, statusKey: "body_tracking",
		ownerID: d1ProvenanceRepairEntityID, sourceTurn: 5,
		evidence: `{"source_revision":"s1-rev-live","direct_evidence_ids":[41]}`,
	})
	// The rolled-back revision links turn 6 and evidence row 42.
	d1StateRepairEvent(t, conn, d1StateRepairEventSeed{
		sessionID: "s1", registryID: registryID, statusKey: "body_tracking",
		ownerID: d1ProvenanceRepairEntityID, sourceTurn: 6,
		evidence: `{"source_revision":"s1-rev-dead","direct_evidence_ids":[42]}`,
	})
	// No revision at all links turn 7.
	d1StateRepairEvent(t, conn, d1StateRepairEventSeed{
		sessionID: "s1", registryID: registryID, statusKey: "body_tracking",
		ownerID: d1ProvenanceRepairEntityID, sourceTurn: 7, evidence: `{}`,
	})

	// Every memory is free of body wording, so only the link can select it.
	turns := map[int]int64{}
	for _, turn := range []int{5, 6, 7, 8} {
		var id int64
		if err := conn.QueryRow(ctx, `INSERT INTO memories
			(chat_session_id, turn_index, summary_json) VALUES ('s1', ?, ?) RETURNING id`,
			turn, `{"note":"the archive is quiet"}`).Scan(&id); err != nil {
			t.Fatalf("seed memory at turn %d: %v", turn, err)
		}
		turns[turn] = id
	}

	changes, err := st.ListBodyRepairArtifacts(ctx, "s1", d1ProvenanceRepairEntityID, "Nobody")
	if err != nil {
		t.Fatalf("ListBodyRepairArtifacts: %v", err)
	}
	keys := d1StateRepairKeys(changes)
	if !containsD1StateRepairKey(keys, fmt.Sprintf("memories:%d", turns[5])) {
		t.Errorf("the memory linked by an ACTIVE source revision must be offered: %v", keys)
	}
	for _, turn := range []int{6, 7, 8} {
		if containsD1StateRepairKey(keys, fmt.Sprintf("memories:%d", turns[turn])) {
			t.Errorf("memory at turn %d must not be offered: it is linked only by a superseded or absent revision (%v)", turn, keys)
		}
	}
}

// TestD1ListBodyRepairArtifactsLinksEvidenceAndPreciseUnitsById pins the two
// remaining link keys. direct_evidence_records is linked by its own row id and
// precise_memory_units by its root_evidence_id, so an evidence row the live
// revision cited is offered even though its text is clean, while a unit rooted
// on the rolled-back revision's evidence is not.
func TestD1ListBodyRepairArtifactsLinksEvidenceAndPreciseUnitsById(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-dead", "turn-2", "superseded")

	registryID := d1StateRepairRegistry(t, conn, "s1", "body_tracking")
	d1StateRepairEvent(t, conn, d1StateRepairEventSeed{
		sessionID: "s1", registryID: registryID, statusKey: "body_tracking",
		ownerID: d1ProvenanceRepairEntityID, sourceTurn: 5,
		evidence: `{"source_revision":"s1-rev-live","direct_evidence_ids":[41]}`,
	})
	d1StateRepairEvent(t, conn, d1StateRepairEventSeed{
		sessionID: "s1", registryID: registryID, statusKey: "body_tracking",
		ownerID: d1ProvenanceRepairEntityID, sourceTurn: 6,
		evidence: `{"source_revision":"s1-rev-dead","direct_evidence_ids":[42]}`,
	})

	// Explicit row ids, because the link key for evidence is the row id itself.
	if _, err := conn.Exec(ctx, `INSERT INTO direct_evidence_records
		(id, chat_session_id, evidence_kind, evidence_text, source_turn_start, source_turn_end)
		VALUES (41, 's1', 'fact_event', 'the archive is quiet', 5, 5)`); err != nil {
		t.Fatalf("seed linked evidence: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO direct_evidence_records
		(id, chat_session_id, evidence_kind, evidence_text, source_turn_start, source_turn_end)
		VALUES (42, 's1', 'fact_event', 'the archive is quiet', 6, 6)`); err != nil {
		t.Fatalf("seed rolled-back evidence: %v", err)
	}

	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-linked", sessionID: "s1", sourceRevision: "s1-rev-live", turnStart: 5,
		payload: `{"summary":"the archive is quiet"}`,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-rolled-back", sessionID: "s1", sourceRevision: "s1-rev-live", turnStart: 6,
		payload: `{"summary":"the archive is quiet"}`,
	})
	if _, err := conn.Exec(ctx, `UPDATE precise_memory_units SET root_evidence_id = 41 WHERE unit_id = 'u-linked'`); err != nil {
		t.Fatalf("root the linked unit: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE precise_memory_units SET root_evidence_id = 42 WHERE unit_id = 'u-rolled-back'`); err != nil {
		t.Fatalf("root the rolled-back unit: %v", err)
	}

	changes, err := st.ListBodyRepairArtifacts(ctx, "s1", d1ProvenanceRepairEntityID, "Nobody")
	if err != nil {
		t.Fatalf("ListBodyRepairArtifacts: %v", err)
	}
	keys := d1StateRepairKeys(changes)
	if !containsD1StateRepairKey(keys, "direct_evidence_records:41") {
		t.Errorf("evidence linked by an ACTIVE revision must be offered: %v", keys)
	}
	if containsD1StateRepairKey(keys, "direct_evidence_records:42") {
		t.Errorf("evidence linked only by a superseded revision must not be offered: %v", keys)
	}

	// precise_memory_units is addressed by unit_id in the plan's vector key,
	// because a unit's row id is a migration artifact rather than its identity.
	// The offered units are the ones rooted on evidence the live revision cited.
	offered := d1StateRepairUnitIDs(t, conn, changes)
	if !equalStringSlices(offered, []string{"u-linked"}) {
		t.Fatalf("offered precise units = %v, want only the one rooted on live evidence", offered)
	}
}

func d1StateRepairUnitIDs(t *testing.T, conn *sqliteD1Conn, changes []StateRepairArtifactChange) []string {
	t.Helper()
	out := []string{}
	for _, change := range changes {
		if change.Table != "precise_memory_units" {
			continue
		}
		var unitID string
		if err := conn.QueryRow(context.Background(),
			`SELECT unit_id FROM precise_memory_units WHERE id = ?`, change.ID).Scan(&unitID); err != nil {
			t.Fatalf("read unit id for artifact %d: %v", change.ID, err)
		}
		out = append(out, unitID)
	}
	return out
}

// TestD1ListBodyRepairArtifactsReportsLegacyVectorIDs pins the vector half. A
// document the outbox already knows about is left to the ordinary outbox path;
// one it does not know about predates the outbox and must be captured from the
// vector store before the row is cleared.
func TestD1ListBodyRepairArtifactsReportsLegacyVectorIDs(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")

	var legacy, materialized int64
	if err := conn.QueryRow(ctx, `INSERT INTO memories
		(chat_session_id, turn_index, summary_json) VALUES ('s1', 5, ?) RETURNING id`,
		`{"note":"Mira: the pregnancy progressed"}`).Scan(&legacy); err != nil {
		t.Fatalf("seed legacy memory: %v", err)
	}
	if err := conn.QueryRow(ctx, `INSERT INTO memories
		(chat_session_id, turn_index, summary_json) VALUES ('s1', 6, ?) RETURNING id`,
		`{"note":"Mira: the pregnancy progressed"}`).Scan(&materialized); err != nil {
		t.Fatalf("seed materialized memory: %v", err)
	}
	d1StateRepairVectorUpsert(t, conn, "s1-rev-live", fmt.Sprintf("memory:s1:%d", materialized), `{"ID":"doc"}`)

	changes, err := st.ListBodyRepairArtifacts(ctx, "s1", d1ProvenanceRepairEntityID, "Mira")
	if err != nil {
		t.Fatalf("ListBodyRepairArtifacts: %v", err)
	}

	legacyChange := d1StateRepairFind(t, changes, "memories", legacy)
	wantIDs := []string{fmt.Sprintf("memory:s1:%d", legacy), fmt.Sprintf("memory:%d", legacy)}
	if !equalStringSlices(legacyChange.VectorIDs, wantIDs) {
		t.Errorf("legacy vector ids = %v, want %v", legacyChange.VectorIDs, wantIDs)
	}
	if legacyChange.VectorAfterJSON != "[]" {
		t.Errorf("legacy vector after = %q, want the empty capture marker", legacyChange.VectorAfterJSON)
	}

	materializedChange := d1StateRepairFind(t, changes, "memories", materialized)
	if len(materializedChange.VectorIDs) != 0 {
		t.Errorf("an outbox-materialized document must not be reported as legacy: %v", materializedChange.VectorIDs)
	}
	if materializedChange.VectorAfterJSON != "" {
		t.Errorf("materialized vector after = %q, want empty", materializedChange.VectorAfterJSON)
	}
}

// TestD1ListBodyRepairArtifactsSurfacesTransportErrors pins the error half. The
// delete route treats an error as "cannot plan this deletion" and an empty
// result as "there is nothing to delete", so a broken D1 binding must not be
// reported as a clean empty plan.
func TestD1ListBodyRepairArtifactsSurfacesTransportErrors(t *testing.T) {
	st, conn := newD1ProbeStore(t)
	ctx := context.Background()
	conn.failOn = "FROM status_change_events e"

	if _, err := st.ListBodyRepairArtifacts(ctx, "s1", d1ProvenanceRepairEntityID, "Mira"); err == nil {
		t.Fatal("a failed source-link read must surface an error, not an empty plan")
	}
}

// ---------------------------------------------------------------------------
// 3. VectorRecoveryCacheReader
// ---------------------------------------------------------------------------

// d1StateRepairOutbox inserts one outbox row with an explicit updated_at, so a
// test controls the materialization order without depending on clock jitter.
func d1StateRepairOutbox(t *testing.T, conn *sqliteD1Conn, documentID, operation, documentJSON, updatedAt string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO memory_vector_outbox
		(operation_key, operation, chat_session_id, source_revision, document_id, document_json,
		 embedding_ready, required_source_state, status, updated_at)
		VALUES (?, ?, 's1', 's1-rev-live', ?, ?, 1, 'active', 'completed', ?)`,
		"key-"+documentID+"-"+operation, operation, documentID,
		d1NullableString(documentJSON), updatedAt); err != nil {
		t.Fatalf("seed outbox %s: %v", documentID, err)
	}
}

// TestD1ReadVectorRecoveryCacheReturnsUpsertsInMaterializationOrder pins the
// whole reader contract: only upsert rows with a saved document, ordered
// oldest materialization first with the row id as the tie-break.
func TestD1ReadVectorRecoveryCacheReturnsUpsertsInMaterializationOrder(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")

	d1StateRepairOutbox(t, conn, "memory:s1:1", "upsert", `{"ID":"memory:s1:1","Embedding":[1]}`, "2026-01-02T00:00:00Z")
	d1StateRepairOutbox(t, conn, "memory:s1:2", "upsert", `{"ID":"memory:s1:2","Embedding":[2]}`, "2026-01-01T00:00:00Z")
	d1StateRepairOutbox(t, conn, "memory:s1:3", "upsert", `{"ID":"memory:s1:3","Embedding":[3]}`, "2026-01-01T00:00:00Z")
	d1StateRepairOutbox(t, conn, "memory:s1:4", "delete", `{"ID":"memory:s1:4"}`, "2025-12-31T00:00:00Z")
	d1StateRepairOutbox(t, conn, "memory:s1:5", "upsert", "", "2026-01-03T00:00:00Z")
	d1StateRepairOutbox(t, conn, "memory:s1:6", "upsert", `{"ID":"memory:s1:6","Embedding":[6]}`, "2026-01-04T00:00:00Z")

	docs, err := st.ReadVectorRecoveryCache(ctx, []string{
		"memory:s1:6", "memory:s1:1", "memory:s1:2", "memory:s1:3", "memory:s1:4", "memory:s1:5", "memory:s1:absent",
	})
	if err != nil {
		t.Fatalf("ReadVectorRecoveryCache: %v", err)
	}
	// Ascending updated_at, then id: documents 2 and 3 share a stamp and resolve
	// by insertion order. Document 4 is a delete tombstone and document 5 has no
	// saved payload, so neither is recoverable.
	want := []string{
		`{"ID":"memory:s1:2","Embedding":[2]}`,
		`{"ID":"memory:s1:3","Embedding":[3]}`,
		`{"ID":"memory:s1:1","Embedding":[1]}`,
		`{"ID":"memory:s1:6","Embedding":[6]}`,
	}
	if !equalStringSlices(docs, want) {
		t.Fatalf("recovery cache = %v, want %v", docs, want)
	}
}

// TestD1ReadVectorRecoveryCacheEmptyInputIsNonNilEmpty pins the empty shape.
// Startup recovery branches on "no cached documents" versus "the cache failed",
// so an empty answer must be an empty list rather than a nil or an error.
func TestD1ReadVectorRecoveryCacheEmptyInputIsNonNilEmpty(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
	d1StateRepairOutbox(t, conn, "memory:s1:1", "upsert", `{"ID":"memory:s1:1"}`, "2026-01-02T00:00:00Z")

	for _, ids := range [][]string{nil, {}} {
		docs, err := st.ReadVectorRecoveryCache(ctx, ids)
		if err != nil {
			t.Fatalf("ReadVectorRecoveryCache(%v): %v", ids, err)
		}
		if docs == nil || len(docs) != 0 {
			t.Errorf("recovery cache for %v = %#v, want an empty non-nil slice", ids, docs)
		}
	}

	// A request that names only documents the cache does not hold is empty too.
	docs, err := st.ReadVectorRecoveryCache(ctx, []string{"memory:s1:absent"})
	if err != nil {
		t.Fatalf("ReadVectorRecoveryCache absent: %v", err)
	}
	if docs == nil || len(docs) != 0 {
		t.Errorf("absent document = %#v, want an empty non-nil slice", docs)
	}
}

// TestD1ReadVectorRecoveryCacheConcatenatesChunks pins the 200-id chunk
// boundary. The parameter cap is a transport requirement, so the reader issues
// one statement per chunk and concatenates; a caller that assumed one globally
// ordered stream would mis-order a request larger than a single chunk.
func TestD1ReadVectorRecoveryCacheConcatenatesChunks(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")

	// Oldest materialization, requested last so it lands in the second chunk.
	d1StateRepairOutbox(t, conn, "memory:s1:0", "upsert", `{"ID":"oldest"}`, "2026-01-01T00:00:00Z")
	d1StateRepairOutbox(t, conn, "memory:s1:1", "upsert", `{"ID":"newest"}`, "2026-02-01T00:00:00Z")

	ids := make([]string, 0, 201)
	ids = append(ids, "memory:s1:1")
	for i := 0; i < 199; i++ {
		ids = append(ids, fmt.Sprintf("memory:s1:absent-%d", i))
	}
	ids = append(ids, "memory:s1:0")

	docs, err := st.ReadVectorRecoveryCache(ctx, ids)
	if err != nil {
		t.Fatalf("ReadVectorRecoveryCache: %v", err)
	}
	want := []string{`{"ID":"newest"}`, `{"ID":"oldest"}`}
	if !equalStringSlices(docs, want) {
		t.Fatalf("recovery cache across chunks = %v, want %v (per-chunk concatenation)", docs, want)
	}
}

// ---------------------------------------------------------------------------
// 4. SourceDiscoveryStore
// ---------------------------------------------------------------------------

// TestD1SaveSourceDiscoveryJobValidatesAndRoundTrips pins the argument contract
// and the stored shape: a blank query or an unknown state is refused before any
// statement, the descriptive columns are trimmed, and the returned job is the
// stored row rather than an echo of the request.
func TestD1SaveSourceDiscoveryJobValidatesAndRoundTrips(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		input SourceDiscoveryInput
		state string
	}{
		{name: "blank query", input: SourceDiscoveryInput{WorkQuery: "   "}, state: "created"},
		{name: "missing query", input: SourceDiscoveryInput{}, state: "created"},
		{name: "unknown state", input: SourceDiscoveryInput{WorkQuery: "Dune"}, state: "not-a-state"},
		{name: "blank state", input: SourceDiscoveryInput{WorkQuery: "Dune"}, state: "   "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := st.SaveSourceDiscoveryJob(ctx, tc.input, tc.state, nil, nil); !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("error = %v, want ErrInvalidReference", err)
			}
		})
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM source_discovery_jobs`); got != 0 {
		t.Fatalf("source_discovery_jobs rows = %d, want 0 after every input was rejected", got)
	}

	job, err := st.SaveSourceDiscoveryJob(ctx, SourceDiscoveryInput{
		WorkID: "work-9", ContinuityID: "cont-3", WorkQuery: "  Dune  ",
		WorkTitle: "Dune", OriginalTitle: "  Le Dune  ", Language: " fr ", EditionHint: " 1965 ",
	}, "created", map[string]any{"sources": 2}, map[string]any{"reviewed": true})
	if err != nil {
		t.Fatalf("SaveSourceDiscoveryJob: %v", err)
	}
	if job.JobID == "" {
		t.Fatal("job id must be generated")
	}
	if job.Contract != SourceDiscoveryContract || job.State != "created" || job.Revision != 1 {
		t.Errorf("stored job = %+v, want the v1 contract at revision 1 in state created", job)
	}
	if job.CreatedAt.IsZero() || job.UpdatedAt.IsZero() {
		t.Errorf("column defaults must parse as timestamps: %+v", job)
	}
	// The request is stored verbatim so the pipeline can resume from it, while
	// the descriptive columns are trimmed for the search index.
	if job.Input["work_query"] != "  Dune  " || job.Input["work_id"] != "work-9" {
		t.Errorf("stored request = %v, want the submitted request verbatim", job.Input)
	}
	if job.Result["sources"] != float64(2) || job.CoverageReport["reviewed"] != true {
		t.Errorf("result/coverage did not round-trip: %+v %+v", job.Result, job.CoverageReport)
	}
	var workQuery, originalTitle, language, editionHint string
	if err := conn.QueryRow(ctx, `SELECT work_query, original_title, language_code, edition_hint
		FROM source_discovery_jobs WHERE job_id = ?`, job.JobID).Scan(
		&workQuery, &originalTitle, &language, &editionHint); err != nil {
		t.Fatalf("read job columns: %v", err)
	}
	for column, got := range map[string]string{
		"work_query": workQuery, "original_title": originalTitle,
		"language_code": language, "edition_hint": editionHint,
	} {
		if strings.TrimSpace(got) != got || got == "" {
			t.Errorf("%s = %q, want it trimmed and non-empty", column, got)
		}
	}
	if workQuery != "Dune" || originalTitle != "Le Dune" || language != "fr" || editionHint != "1965" {
		t.Errorf("trimmed columns = %q/%q/%q/%q", workQuery, originalTitle, language, editionHint)
	}
}

// TestD1GetSourceDiscoveryJobNotFoundAndTrimsID pins the read contract. The
// route answers 404 for an unknown job, so absence must be ErrNotFound rather
// than an empty job, and a padded id must resolve like a trimmed one.
func TestD1GetSourceDiscoveryJobNotFoundAndTrimsID(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if _, err := st.GetSourceDiscoveryJob(ctx, "no-such-job"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing job error = %v, want ErrNotFound", err)
	}

	saved, err := st.SaveSourceDiscoveryJob(ctx, SourceDiscoveryInput{WorkQuery: "Dune"}, "created", nil, nil)
	if err != nil {
		t.Fatalf("SaveSourceDiscoveryJob: %v", err)
	}
	padded, err := st.GetSourceDiscoveryJob(ctx, "  "+saved.JobID+"  ")
	if err != nil {
		t.Fatalf("GetSourceDiscoveryJob padded: %v", err)
	}
	if padded.JobID != saved.JobID || padded.Contract != SourceDiscoveryContract {
		t.Errorf("padded read = %+v, want the stored job %+v", padded, saved)
	}
}

// TestD1SourceDiscoveryJobsAreIndependentRows pins that two submissions create
// two jobs. A job ledger that collapsed on the work query would make the
// pipeline's resume step read a different submission's request.
func TestD1SourceDiscoveryJobsAreIndependentRows(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	first, err := st.SaveSourceDiscoveryJob(ctx, SourceDiscoveryInput{WorkQuery: "Dune", WorkID: "w1"}, "created", nil, nil)
	if err != nil {
		t.Fatalf("first save: %v", err)
	}
	second, err := st.SaveSourceDiscoveryJob(ctx, SourceDiscoveryInput{WorkQuery: "Dune", WorkID: "w2"}, "discovering", nil, nil)
	if err != nil {
		t.Fatalf("second save: %v", err)
	}
	if first.JobID == second.JobID {
		t.Fatalf("both saves produced job id %s; a job ledger must not collapse submissions", first.JobID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM source_discovery_jobs`); got != 2 {
		t.Errorf("source_discovery_jobs rows = %d, want 2", got)
	}
	reloaded, err := st.GetSourceDiscoveryJob(ctx, second.JobID)
	if err != nil {
		t.Fatalf("GetSourceDiscoveryJob: %v", err)
	}
	if reloaded.State != "discovering" || reloaded.Input["work_id"] != "w2" {
		t.Errorf("second job = %+v, want its own state and request", reloaded)
	}
}

// ---------------------------------------------------------------------------
// capability advertisement
// ---------------------------------------------------------------------------

// TestD1ProvenanceRepairCapabilitiesAreAdvertised pins the consequence the
// routes depend on. All four are discovered by a type assertion, so a provider
// that implemented the methods but was not discoverable would still answer
// "unavailable" while the capability manifest claimed parity.
func TestD1ProvenanceRepairCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)
	var asStore Store = st

	implemented := map[string]bool{}
	for _, status := range CapabilityReport(asStore) {
		implemented[status.Name] = status.Implemented
	}
	for _, name := range []string{
		"CharacterProvenanceRepairStore", "StateRepairArtifactReader",
		"VectorRecoveryCacheReader", "SourceDiscoveryStore",
	} {
		if !implemented[name] {
			t.Errorf("capability manifest reports %s as missing on the D1 provider", name)
		}
	}
}
