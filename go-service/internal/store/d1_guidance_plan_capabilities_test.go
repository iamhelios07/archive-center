package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestD1GuidancePlanStateUpsertThenGetRoundTrips pins the full read-after-write
// contract. The narrative-control rebuild writes the plan and a later request
// reads it back to decide whether the cache is still fresh, so every field the
// freshness check and the L3 snapshot consume has to survive the round trip
// unchanged, including the sentinel last_turn of a session that was never built.
func TestD1GuidancePlanStateUpsertThenGetRoundTrips(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	stamp := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	item := &GuidancePlanState{
		ChatSessionID: "s1",
		StoryPlanJSON: `{"current_arc":"act-one","next_beats":["beat"]}`,
		DirectorJSON:  `{"scene_mandate":"carry the promise forward"}`,
		StateStatus:   "ready",
		LastTurn:      12,
		WarningsJSON:  `["no active storylines"]`,
		UpdatedAt:     stamp,
	}
	if err := st.UpsertGuidancePlanState(ctx, item); err != nil {
		t.Fatalf("UpsertGuidancePlanState: %v", err)
	}

	got, err := st.GetGuidancePlanState(ctx, "s1")
	if err != nil {
		t.Fatalf("GetGuidancePlanState: %v", err)
	}
	if got.ID == 0 {
		t.Error("the read must report the persisted row id")
	}
	if got.ChatSessionID != "s1" {
		t.Errorf("chat_session_id = %q, want s1", got.ChatSessionID)
	}
	if got.StoryPlanJSON != item.StoryPlanJSON {
		t.Errorf("story_plan_json = %s, want %s", got.StoryPlanJSON, item.StoryPlanJSON)
	}
	if got.DirectorJSON != item.DirectorJSON {
		t.Errorf("director_json = %s, want %s", got.DirectorJSON, item.DirectorJSON)
	}
	if got.StateStatus != "ready" {
		t.Errorf("state_status = %q, want ready", got.StateStatus)
	}
	if got.LastTurn != 12 {
		t.Errorf("last_turn = %d, want 12", got.LastTurn)
	}
	if got.WarningsJSON != item.WarningsJSON {
		t.Errorf("warnings_json = %s, want %s", got.WarningsJSON, item.WarningsJSON)
	}
	if !got.CreatedAt.Equal(stamp) {
		t.Errorf("created_at = %s, want %s", got.CreatedAt, stamp)
	}
	if !got.UpdatedAt.Equal(stamp) {
		t.Errorf("updated_at = %s, want %s", got.UpdatedAt, stamp)
	}
}

// TestD1GuidancePlanStateMissingSessionIsNotFound pins the error contract the
// read routes branch on. buildL3GuidanceSnapshot treats ErrNotFound as "no cache,
// safe degrade" and every other error as a read failure, so an absent session
// must be distinguishable from a transport error.
func TestD1GuidancePlanStateMissingSessionIsNotFound(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	got, err := st.GetGuidancePlanState(ctx, "never-written")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetGuidancePlanState on a missing session = %v, want ErrNotFound", err)
	}
	if got != nil {
		t.Errorf("a missing session must return no row, got %+v", got)
	}
}

// TestD1GuidancePlanStateUpsertReplacesTheCachedSnapshot pins the upsert half of
// the contract: one row per session, the newest payload wins, and the row keeps
// its identity. A second row would make the bare-equality read non-deterministic,
// and a new id would make the created_at comparison meaningless.
func TestD1GuidancePlanStateUpsertReplacesTheCachedSnapshot(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	first := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	second := time.Date(2026, 3, 5, 6, 7, 8, 0, time.UTC)

	if err := st.UpsertGuidancePlanState(ctx, &GuidancePlanState{
		ChatSessionID: "s1", StoryPlanJSON: `{"current_arc":"one"}`,
		DirectorJSON: `{"scene_mandate":"first"}`, StateStatus: "ready",
		LastTurn: 3, UpdatedAt: first,
	}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := st.UpsertGuidancePlanState(ctx, &GuidancePlanState{
		ChatSessionID: "s1", StoryPlanJSON: `{"current_arc":"two"}`,
		DirectorJSON: `{"scene_mandate":"second"}`, StateStatus: "user_patched",
		LastTurn: 9, WarningsJSON: `["patched"]`, UpdatedAt: second,
	}); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	if rows := d1Count(t, conn, `SELECT COUNT(*) FROM guidance_plan_states`); rows != 1 {
		t.Fatalf("guidance_plan_states rows = %d, want 1 after an upsert", rows)
	}

	got, err := st.GetGuidancePlanState(ctx, "s1")
	if err != nil {
		t.Fatalf("GetGuidancePlanState: %v", err)
	}
	if got.StoryPlanJSON != `{"current_arc":"two"}` {
		t.Errorf("story_plan_json = %s, want the second payload", got.StoryPlanJSON)
	}
	if got.DirectorJSON != `{"scene_mandate":"second"}` {
		t.Errorf("director_json = %s, want the second payload", got.DirectorJSON)
	}
	if got.StateStatus != "user_patched" {
		t.Errorf("state_status = %q, want the second payload's user_patched", got.StateStatus)
	}
	if got.LastTurn != 9 {
		t.Errorf("last_turn = %d, want 9", got.LastTurn)
	}
	if got.WarningsJSON != `["patched"]` {
		t.Errorf("warnings_json = %s, want the second payload", got.WarningsJSON)
	}
	if !got.UpdatedAt.Equal(second) {
		t.Errorf("updated_at = %s, want the second write's %s", got.UpdatedAt, second)
	}
	// created_at is deliberately absent from the conflict branch, so a rewrite is
	// not a new row: the session's plan cache has one lifetime.
	if !got.CreatedAt.Equal(first) {
		t.Errorf("created_at = %s, want the first insert's %s", got.CreatedAt, first)
	}
}

// TestD1GuidancePlanStateUpsertStampsAnUnsetTimestamp pins the timestamp rule.
// The narrative-control rebuild always sets UpdatedAt, but the director patch
// does not, so an unset timestamp must be stamped with the current UTC time. The
// zero time is not an acceptable substitute: idx_guidance_plan_updated orders on
// this column, and a zero value would sort the row as older than every real
// write.
func TestD1GuidancePlanStateUpsertStampsAnUnsetTimestamp(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	before := time.Now().UTC().Add(-time.Second)

	if err := st.UpsertGuidancePlanState(ctx, &GuidancePlanState{
		ChatSessionID: "s1", StoryPlanJSON: `{}`, DirectorJSON: `{}`,
		StateStatus: "ready", LastTurn: 4,
	}); err != nil {
		t.Fatalf("UpsertGuidancePlanState: %v", err)
	}

	got, err := st.GetGuidancePlanState(ctx, "s1")
	if err != nil {
		t.Fatalf("GetGuidancePlanState: %v", err)
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("an unset UpdatedAt must be stamped, not persisted as the zero time")
	}
	if got.UpdatedAt.Before(before) || got.UpdatedAt.After(time.Now().UTC().Add(time.Second)) {
		t.Errorf("updated_at = %s, want a timestamp near now", got.UpdatedAt)
	}
	// One resolved timestamp feeds both columns, so a freshly created row reports
	// the same instant for creation and update.
	if !got.CreatedAt.Equal(got.UpdatedAt) {
		t.Errorf("created_at = %s and updated_at = %s must come from one resolved timestamp",
			got.CreatedAt, got.UpdatedAt)
	}
}

// TestD1GuidancePlanStateStoresEmptyTextVerbatim pins the NULL/empty distinction.
// The reference binds the caller's strings unchanged, so an empty plan is stored
// as an empty string and only a rollback leaves the column NULL. Collapsing "" to
// NULL would make the two providers disagree at the column level for any
// comparison that reads the table directly.
func TestD1GuidancePlanStateStoresEmptyTextVerbatim(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if err := st.UpsertGuidancePlanState(ctx, &GuidancePlanState{
		ChatSessionID: "s1", StateStatus: "empty", LastTurn: -1,
	}); err != nil {
		t.Fatalf("UpsertGuidancePlanState: %v", err)
	}

	var storyPlanJSON, directorJSON, warningsJSON *string
	if err := conn.QueryRow(ctx, `
		SELECT story_plan_json, director_json, warnings_json
		FROM guidance_plan_states WHERE chat_session_id = 's1'`).Scan(
		&storyPlanJSON, &directorJSON, &warningsJSON); err != nil {
		t.Fatalf("read raw text columns: %v", err)
	}
	if storyPlanJSON == nil || directorJSON == nil || warningsJSON == nil {
		t.Errorf("an empty payload must be stored as an empty string, got %v/%v/%v",
			storyPlanJSON, directorJSON, warningsJSON)
	}

	got, err := st.GetGuidancePlanState(ctx, "s1")
	if err != nil {
		t.Fatalf("GetGuidancePlanState: %v", err)
	}
	if got.StoryPlanJSON != "" || got.DirectorJSON != "" || got.WarningsJSON != "" {
		t.Errorf("empty payloads must read back as empty strings, got %q/%q/%q",
			got.StoryPlanJSON, got.DirectorJSON, got.WarningsJSON)
	}
	// -1 is the never-built sentinel the read routes compare against, so a zero
	// here would report a session that was never built as built at turn 0.
	if got.LastTurn != -1 {
		t.Errorf("last_turn = %d, want the -1 sentinel to survive", got.LastTurn)
	}
}

// TestD1GuidancePlanStateReadRendersRolledBackColumnsAsEmpty pins the read side of
// the rollback contract. DeleteGuidancePlanState NULLs the three JSON columns and
// resets the row to the empty state; the read must then report the same empty
// representation a never-built session has, or a rolled-back session would look
// different from one that was never built.
func TestD1GuidancePlanStateReadRendersRolledBackColumnsAsEmpty(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if err := st.UpsertGuidancePlanState(ctx, &GuidancePlanState{
		ChatSessionID: "s1", StoryPlanJSON: `{"current_arc":"one"}`,
		DirectorJSON: `{"scene_mandate":"first"}`, StateStatus: "ready",
		LastTurn: 7, WarningsJSON: `["w"]`,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Rewind below the cached turn so the rollback's last_turn guard matches.
	if err := st.DeleteGuidancePlanState(ctx, "s1", 5); err != nil {
		t.Fatalf("DeleteGuidancePlanState: %v", err)
	}

	var storyPlanJSON, directorJSON, warningsJSON *string
	if err := conn.QueryRow(ctx, `
		SELECT story_plan_json, director_json, warnings_json
		FROM guidance_plan_states WHERE chat_session_id = 's1'`).Scan(
		&storyPlanJSON, &directorJSON, &warningsJSON); err != nil {
		t.Fatalf("read raw text columns: %v", err)
	}
	if storyPlanJSON != nil || directorJSON != nil || warningsJSON != nil {
		t.Fatalf("the rollback must NULL the JSON columns, got %v/%v/%v",
			storyPlanJSON, directorJSON, warningsJSON)
	}

	got, err := st.GetGuidancePlanState(ctx, "s1")
	if err != nil {
		t.Fatalf("a rolled-back row still exists and must still be readable: %v", err)
	}
	if got.StoryPlanJSON != "" || got.DirectorJSON != "" || got.WarningsJSON != "" {
		t.Errorf("NULL columns must read back as empty strings, got %q/%q/%q",
			got.StoryPlanJSON, got.DirectorJSON, got.WarningsJSON)
	}
	if got.StateStatus != "empty" {
		t.Errorf("state_status = %q, want the rollback's empty", got.StateStatus)
	}
	if got.LastTurn != -1 {
		t.Errorf("last_turn = %d, want the rollback's -1", got.LastTurn)
	}
}

// TestD1GuidancePlanStateRejectsNilItem pins the one place this port is stricter
// than the statement it translates. The reference would dereference a nil item and
// panic, which inside a route handler is a process failure; the guard turns that
// into an ordinary error every caller can already handle.
func TestD1GuidancePlanStateRejectsNilItem(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if err := st.UpsertGuidancePlanState(ctx, nil); err == nil {
		t.Fatal("a nil item must be rejected rather than dereferenced")
	}
	if rows := d1Count(t, conn, `SELECT COUNT(*) FROM guidance_plan_states`); rows != 0 {
		t.Errorf("a rejected upsert must write nothing, found %d rows", rows)
	}
}

// TestD1GuidancePlanStateUpsertLeavesTheCallerItemUnmodified pins the write
// contract. The reference does not report the assigned id or the resolved
// timestamp back on the caller's struct, and the write sites ignore the error
// because a stale cache is always safe to rebuild, so the two providers must not
// diverge on whether the caller's value is rewritten.
func TestD1GuidancePlanStateUpsertLeavesTheCallerItemUnmodified(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	item := &GuidancePlanState{
		ChatSessionID: "s1", StoryPlanJSON: `{}`, DirectorJSON: `{}`,
		StateStatus: "ready", LastTurn: 2,
	}
	if err := st.UpsertGuidancePlanState(ctx, item); err != nil {
		t.Fatalf("UpsertGuidancePlanState: %v", err)
	}
	if item.ID != 0 {
		t.Errorf("the upsert must not write an id back onto the caller's item, got %d", item.ID)
	}
	if !item.UpdatedAt.IsZero() {
		t.Errorf("the upsert must not write a timestamp back onto the caller's item, got %s", item.UpdatedAt)
	}
	if !item.CreatedAt.IsZero() {
		t.Errorf("the upsert must not write a created_at back onto the caller's item, got %s", item.CreatedAt)
	}
}

// TestD1GuidancePlanStateIsSessionScoped confirms the equality predicate really is
// one session. The plan cache is keyed by session, and a read that crossed that
// boundary would serve one chat's narrative plan to another.
func TestD1GuidancePlanStateIsSessionScoped(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if err := st.UpsertGuidancePlanState(ctx, &GuidancePlanState{
		ChatSessionID: "s1", StoryPlanJSON: `{"current_arc":"one"}`,
		DirectorJSON: `{"scene_mandate":"first"}`, StateStatus: "ready", LastTurn: 2,
	}); err != nil {
		t.Fatalf("seed s1: %v", err)
	}
	if err := st.UpsertGuidancePlanState(ctx, &GuidancePlanState{
		ChatSessionID: "s2", StoryPlanJSON: `{"current_arc":"two"}`,
		DirectorJSON: `{"scene_mandate":"second"}`, StateStatus: "ready", LastTurn: 8,
	}); err != nil {
		t.Fatalf("seed s2: %v", err)
	}

	first, err := st.GetGuidancePlanState(ctx, "s1")
	if err != nil {
		t.Fatalf("read s1: %v", err)
	}
	if first.ChatSessionID != "s1" || first.StoryPlanJSON != `{"current_arc":"one"}` || first.LastTurn != 2 {
		t.Errorf("s1 read another session's snapshot: %+v", first)
	}

	second, err := st.GetGuidancePlanState(ctx, "s2")
	if err != nil {
		t.Fatalf("read s2: %v", err)
	}
	if second.ChatSessionID != "s2" || second.StoryPlanJSON != `{"current_arc":"two"}` || second.LastTurn != 8 {
		t.Errorf("s2 read another session's snapshot: %+v", second)
	}
}

// TestD1GuidancePlanStateStoreSatisfiesTheCapability records that implementing the
// two methods is what makes the D1 provider satisfy GuidancePlanStateStore. The
// routes assert that interface directly and degrade silently when it is absent, so
// a regression here would turn into a route that quietly stops caching rather than
// into a failure.
func TestD1GuidancePlanStateStoreSatisfiesTheCapability(t *testing.T) {
	d1, _ := newD1TestStore(t)

	// The routes assert the capability against a Store interface value, so the
	// probe is repeated the same way here. Asserting on the concrete *d1Store
	// would be true by construction and would prove nothing.
	var asStore Store = d1
	caps, ok := asStore.(GuidancePlanStateStore)
	if !ok {
		t.Fatal("the D1 store must satisfy GuidancePlanStateStore")
	}
	if err := caps.UpsertGuidancePlanState(context.Background(), &GuidancePlanState{
		ChatSessionID: "s1", StateStatus: "ready", LastTurn: 1,
	}); err != nil {
		t.Fatalf("capability upsert: %v", err)
	}
	if _, err := caps.GetGuidancePlanState(context.Background(), "s1"); err != nil {
		t.Fatalf("capability read: %v", err)
	}
}
