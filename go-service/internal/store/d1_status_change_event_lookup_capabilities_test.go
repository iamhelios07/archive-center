package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

// D1 status-event source-lookup tests.
//
// Every test runs against real SQLite through the D1 transport, so these assert
// the statements D1 actually executes, including the SQLite form of every
// JSON_UNQUOTE predicate.
//
// The capability exists to forbid one thing, so most of these tests are written
// as contrasts rather than as single expectations. A store without
// StatusChangeEventSourceLookupStore makes restoreStoryClockCurrentAfterRollback
// return (0, nil): the story clock is never rebuilt. A store that APPROXIMATES
// the lookup with a recent-row cap is worse than useless, because it returns a
// well-formed event carrying a clock the user already rolled back and the
// consumer writes it straight back to status_current_values as current truth.
// So each test below names the approximation this package already ships
// (d1StatusEventLookupNewestID, and the real ListStatusChangeEvents recency list)
// and asserts that it answers differently from the exact read. An exact read
// that agreed with the cap would not have proved anything.

// ---------------------------------------------------------------------------
// seeds
// ---------------------------------------------------------------------------

// d1StatusEventLookupTime is the fixed instant every seeded event is stamped
// with, so a test that wants a recency difference has to say so explicitly.
func d1StatusEventLookupTime() time.Time {
	return time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
}

// d1StatusEventLookupSeedRegistry inserts one status_schema_registry row and
// returns its id. It is not decoration: status_change_events carries a foreign key
// to it, and the test harness runs with PRAGMA foreign_keys = ON, so without the
// row every seed below would fail on the constraint instead of on the lookup.
func d1StatusEventLookupSeedRegistry(t *testing.T, conn *sqliteD1Conn, sessionID, statusKey, ownerScope string) int64 {
	t.Helper()
	stamp := d1TimeValue(d1StatusEventLookupTime())
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

// d1StatusEventLookupEvidence builds an event evidence envelope from exactly the
// keys a test supplies, because every predicate under test is an envelope
// predicate: a blank revision omits the key entirely, a nil currentProjection
// omits that key, and a string currentProjection writes the historic JSON string
// form rather than the boolean.
func d1StatusEventLookupEvidence(revision string, currentProjection any, extra map[string]any) string {
	evidence := map[string]any{}
	if revision != "" {
		evidence["source_revision"] = revision
	}
	if currentProjection != nil {
		evidence["current_projection"] = currentProjection
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

// d1StatusEventLookupEvent is one seeded ledger row. ID, SourceTurn, Evidence and
// CreatedAt are the four things the two statements order or filter on, so they
// are all spelled out rather than defaulted.
type d1StatusEventLookupEvent struct {
	ID         int64
	RegistryID int64
	StatusKey  string
	OwnerScope string
	OwnerID    string
	Evidence   string
	SourceTurn int
	EventState string
	CreatedAt  time.Time
}

// d1StatusEventLookupSeed inserts one status_change_events row directly, so a
// test can place an event at an exact id, turn and envelope position. The value
// JSONs carry the id as a marker, which is what lets a test prove it restored
// the surviving event rather than merely some event.
func d1StatusEventLookupSeed(t *testing.T, conn *sqliteD1Conn, sessionID string, event d1StatusEventLookupEvent) {
	t.Helper()
	marker := fmt.Sprintf(`{"marker":%d}`, event.ID)
	if _, err := conn.Exec(context.Background(), `INSERT INTO status_change_events (
		id, chat_session_id, registry_id, status_key, owner_scope, owner_id,
		event_kind, previous_value_json, new_value_json, evidence_json, source_turn,
		story_clock_json, event_state, created_at
	) VALUES (?, ?, ?, ?, ?, ?, 'set', ?, ?, ?, ?, ?, ?, ?)`,
		event.ID, sessionID, event.RegistryID, event.StatusKey, event.OwnerScope, event.OwnerID,
		marker, marker, event.Evidence, event.SourceTurn, marker,
		firstNonEmptyString(event.EventState, "recorded"), d1TimeValue(event.CreatedAt)); err != nil {
		t.Fatalf("seed status change event %d: %v", event.ID, err)
	}
}

// d1StatusEventLookupNewestID is the forbidden approximation, kept in the test
// so the exact read is measured against a real wrong answer rather than against
// the reader's imagination: the newest row under the key, with no evidence
// predicate at all.
func d1StatusEventLookupNewestID(t *testing.T, conn *sqliteD1Conn, sessionID, statusKey string) int64 {
	t.Helper()
	var id int64
	if err := conn.QueryRow(context.Background(), `
		SELECT id FROM status_change_events
		WHERE chat_session_id = ? AND status_key = ?
		ORDER BY id DESC LIMIT 1`, sessionID, statusKey).Scan(&id); err != nil {
		t.Fatalf("approximate newest-row lookup: %v", err)
	}
	return id
}

// d1StatusEventLookupUnit is the ordinary evidence filler for a source-backed
// projection event.
func d1StatusEventLookupUnit(id int64) map[string]any {
	return map[string]any{"source_unit_id": fmt.Sprintf("unit-%d", id)}
}

// ---------------------------------------------------------------------------
// exact source lookup
// ---------------------------------------------------------------------------

// TestD1StatusEventLookupBySourceRevisionResolvesTheExactEvent pins the identity
// of the lookup: session, status key, the evidence revision, and the source_turn
// COLUMN. It also pins the newest-wins rule, because one revision and one turn
// can emit several events for a key and a caller rebuilding that turn must see
// the last one.
func TestD1StatusEventLookupBySourceRevisionResolvesTheExactEvent(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "story_clock", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-1", "turn-1", "active")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-2", "turn-2", "active")

	// Ids 11 and 12 are the same (revision, turn, key) pair: the second is the
	// later observation of it and is the answer.
	for _, seed := range []d1StatusEventLookupEvent{
		{ID: 11, SourceTurn: 9, CreatedAt: d1StatusEventLookupTime()},
		{ID: 12, SourceTurn: 9, CreatedAt: d1StatusEventLookupTime().Add(time.Minute)},
		{ID: 13, SourceTurn: 9, CreatedAt: d1StatusEventLookupTime().Add(2 * time.Minute)},
		{ID: 14, SourceTurn: 10, CreatedAt: d1StatusEventLookupTime().Add(3 * time.Minute)},
	} {
		revision := "rev-1"
		if seed.ID == 13 {
			revision = "rev-2"
		}
		full := seed
		full.RegistryID = registry
		full.StatusKey = "story_clock"
		full.OwnerScope = "session"
		full.OwnerID = "current"
		full.Evidence = d1StatusEventLookupEvidence(revision, true, d1StatusEventLookupUnit(seed.ID))
		d1StatusEventLookupSeed(t, conn, "session-1", full)
	}
	full := d1StatusEventLookupEvent{
		ID: 15, RegistryID: registry, StatusKey: "narrative_state", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 9, CreatedAt: d1StatusEventLookupTime(),
		Evidence: d1StatusEventLookupEvidence("rev-1", true, d1StatusEventLookupUnit(15)),
	}
	d1StatusEventLookupSeed(t, conn, "session-1", full)

	event, err := st.GetStatusChangeEventBySourceRevision(ctx, "session-1", "story_clock", "rev-1", 9)
	if err != nil {
		t.Fatalf("GetStatusChangeEventBySourceRevision: %v", err)
	}
	if event.ID != 12 {
		t.Fatalf("resolved event %d, want 12 (the newest observation of rev-1 turn 9)", event.ID)
	}
	if event.SourceTurn != 9 {
		t.Errorf("source_turn = %d, want 9", event.SourceTurn)
	}
	if event.RegistryID != registry || event.StatusKey != "story_clock" || event.EventState != "recorded" {
		t.Errorf("resolved event carries the wrong registry/key/state: %+v", event)
	}
	// The optional columns have to survive the read; the consumer copies the
	// value and story-clock JSON straight back into the current projection.
	if event.NewValueJSON != `{"marker":12}` || event.StoryClockJSON != `{"marker":12}` {
		t.Errorf("value JSONs = %q / %q, want the seeded marker 12", event.NewValueJSON, event.StoryClockJSON)
	}
	if want := d1TimeInstant(d1StatusEventLookupTime().Add(time.Minute)); !event.CreatedAt.Equal(want) {
		t.Errorf("created_at = %s, want %s", event.CreatedAt, want)
	}

	// Every other member of the key has to narrow the result, or the lookup is a
	// status-key list with extra steps.
	for _, tc := range []struct {
		name         string
		sessionID    string
		statusKey    string
		revision     string
		sourceTurn   int
		wantEventID  int64
		wantNotFound bool
	}{
		{name: "other_revision", sessionID: "session-1", statusKey: "story_clock", revision: "rev-2", sourceTurn: 9, wantEventID: 13},
		{name: "other_turn", sessionID: "session-1", statusKey: "story_clock", revision: "rev-1", sourceTurn: 10, wantEventID: 14},
		{name: "other_key", sessionID: "session-1", statusKey: "narrative_state", revision: "rev-1", sourceTurn: 9, wantEventID: 15},
		{name: "other_session", sessionID: "session-2", statusKey: "story_clock", revision: "rev-1", sourceTurn: 9, wantNotFound: true},
		{name: "unknown_revision", sessionID: "session-1", statusKey: "story_clock", revision: "rev-missing", sourceTurn: 9, wantNotFound: true},
		{name: "unknown_turn", sessionID: "session-1", statusKey: "story_clock", revision: "rev-1", sourceTurn: 99, wantNotFound: true},
		{name: "unknown_key", sessionID: "session-1", statusKey: "absent_key", revision: "rev-1", sourceTurn: 9, wantNotFound: true},
		{name: "zero_turn", sessionID: "session-1", statusKey: "story_clock", revision: "rev-1", sourceTurn: 0, wantNotFound: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := st.GetStatusChangeEventBySourceRevision(ctx, tc.sessionID, tc.statusKey, tc.revision, tc.sourceTurn)
			if tc.wantNotFound {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("error = %v, want ErrNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetStatusChangeEventBySourceRevision: %v", err)
			}
			if got.ID != tc.wantEventID {
				t.Fatalf("resolved event %d, want %d", got.ID, tc.wantEventID)
			}
		})
	}
}

// TestD1StatusEventLookupBySourceRevisionIsNotAWindow is the "unbounded by
// window" half of the interface doc, measured against a real cap. The exact event
// sits far back in the id order behind 150 newer rows, which is exactly the shape
// a session reaches after a long conversation: any implementation that scans the
// most recent rows first would have already given up.
func TestD1StatusEventLookupBySourceRevisionIsNotAWindow(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "story_clock", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-1", "turn-1", "active")

	target := d1StatusEventLookupEvent{
		ID: 10, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 4, CreatedAt: d1StatusEventLookupTime(),
		Evidence: d1StatusEventLookupEvidence("rev-1", true, d1StatusEventLookupUnit(10)),
	}
	d1StatusEventLookupSeed(t, conn, "session-1", target)
	for id := int64(200); id < 350; id++ {
		d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
			ID: id, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
			SourceTurn: 4, CreatedAt: d1StatusEventLookupTime().Add(time.Duration(id-10) * time.Minute),
			Evidence: d1StatusEventLookupEvidence("rev-other", true, d1StatusEventLookupUnit(id)),
		})
	}

	event, err := st.GetStatusChangeEventBySourceRevision(ctx, "session-1", "story_clock", "rev-1", 4)
	if err != nil {
		t.Fatalf("GetStatusChangeEventBySourceRevision: %v", err)
	}
	if event.ID != 10 {
		t.Fatalf("resolved event %d, want 10: the lookup is bounded by recency, not by identity", event.ID)
	}
	if got := d1StatusEventLookupNewestID(t, conn, "session-1", "story_clock"); got != 349 {
		t.Fatalf("approximate newest row = %d, want 349: the control no longer answers differently", got)
	}
	// The list read this package already exposes is the cap the interface rules
	// out: the exact event is not reachable through it at all.
	capped, err := st.ListStatusChangeEvents(ctx, "session-1", "", "", "story_clock", 100)
	if err != nil {
		t.Fatalf("ListStatusChangeEvents: %v", err)
	}
	if len(capped) != 100 {
		t.Fatalf("capped list returned %d events, want the 100-row default", len(capped))
	}
	for _, item := range capped {
		if item.ID == 10 {
			t.Fatal("the exact event is reachable from a 100-row window, so this seed no longer proves the point")
		}
	}
}

// TestD1StatusEventLookupBySourceRevisionResolvesRolledBackRevision pins the
// asymmetry between the two reads. This one is an evidence lookup, so a revision
// the user rolled back must STILL resolve: that is the situation a rollback audit
// is performed in. The projection sibling in the next test is the one that fences
// the same revision out.
func TestD1StatusEventLookupBySourceRevisionResolvesRolledBackRevision(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "audit_ledger", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-dead", "turn-1", "invalidated")

	d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
		ID: 20, RegistryID: registry, StatusKey: "audit_ledger", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 7, CreatedAt: d1StatusEventLookupTime(),
		Evidence: d1StatusEventLookupEvidence("rev-dead", true, d1StatusEventLookupUnit(20)),
	})

	event, err := st.GetStatusChangeEventBySourceRevision(ctx, "session-1", "audit_ledger", "rev-dead", 7)
	if err != nil {
		t.Fatalf("a rolled-back revision must still resolve here: %v", err)
	}
	if event.ID != 20 {
		t.Fatalf("resolved event %d, want 20", event.ID)
	}
	// The contrast that keeps this from reading as an accident: the SAME event is
	// not a current projection, because that read fences a rolled-back source.
	if _, err := st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", "audit_ledger"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("projection lookup error = %v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// latest current-projection lookup
// ---------------------------------------------------------------------------

// TestD1StatusEventLookupLatestCurrentProjectionExcludesRolledBackSource is the
// headline case. The newest event under the key belongs to a source the user
// rolled back, so it is the answer a recent-row cap gives and the answer this
// capability exists to refuse. Restoring it would put discarded state back into
// the current projection.
func TestD1StatusEventLookupLatestCurrentProjectionExcludesRolledBackSource(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "story_clock", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-live", "turn-1", "active")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-dead", "turn-2", "invalidated")

	for _, seed := range []struct {
		id       int64
		revision string
		turn     int
	}{
		{id: 30, revision: "rev-live", turn: 9},
		{id: 31, revision: "rev-dead", turn: 12},
	} {
		d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
			ID: seed.id, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
			SourceTurn: seed.turn, CreatedAt: d1StatusEventLookupTime().Add(time.Duration(seed.id-30) * time.Hour),
			Evidence: d1StatusEventLookupEvidence(seed.revision, true, d1StatusEventLookupUnit(seed.id)),
		})
	}

	event, err := st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", "story_clock")
	if err != nil {
		t.Fatalf("GetLatestCurrentProjectionStatusChangeEvent: %v", err)
	}
	if event.ID != 30 {
		t.Fatalf("latest current projection = %d, want 30 (the rolled-back source is not the current state)", event.ID)
	}
	// Both forbidden approximations must disagree, or this test proves nothing.
	if got := d1StatusEventLookupNewestID(t, conn, "session-1", "story_clock"); got != 31 {
		t.Fatalf("approximate newest row = %d, want 31: the control no longer returns the wrong event", got)
	}
	recency, err := st.ListStatusChangeEvents(ctx, "session-1", "", "", "story_clock", 10)
	if err != nil {
		t.Fatalf("ListStatusChangeEvents: %v", err)
	}
	if len(recency) == 0 || recency[0].ID != 31 {
		t.Fatalf("recency list head = %+v, want the rolled-back event 31", recency)
	}
}

// TestD1StatusEventLookupLatestCurrentProjectionOrdersByObservationTurn pins the
// ORDER BY. The winner here was written FIRST and is superseded by a delayed
// source that observed an earlier turn, so both recency and the id order point at
// the wrong row. The projection writer would have refused to install that delayed
// observation at all (ErrStatusProjectionStale), so a read that reports it hands
// the consumer a value the store itself rejected.
func TestD1StatusEventLookupLatestCurrentProjectionOrdersByObservationTurn(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "story_clock", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-live", "turn-1", "active")

	d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
		ID: 40, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 9, CreatedAt: d1StatusEventLookupTime().Add(2 * time.Hour),
		Evidence: d1StatusEventLookupEvidence("rev-live", true, d1StatusEventLookupUnit(40)),
	})
	d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
		ID: 41, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 3, CreatedAt: d1StatusEventLookupTime().Add(time.Hour),
		Evidence: d1StatusEventLookupEvidence("rev-live", true, d1StatusEventLookupUnit(41)),
	})

	event, err := st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", "story_clock")
	if err != nil {
		t.Fatalf("GetLatestCurrentProjectionStatusChangeEvent: %v", err)
	}
	if event.ID != 40 {
		t.Fatalf("latest current projection = %d, want 40 (turn 9 outranks the later-written turn 3)", event.ID)
	}
	if got := d1StatusEventLookupNewestID(t, conn, "session-1", "story_clock"); got != 41 {
		t.Fatalf("approximate newest row = %d, want 41: the control no longer returns the wrong event", got)
	}
}

// TestD1StatusEventLookupLatestCurrentProjectionRepairTurnOutranksHistory pins
// d1StatusObservationTurnSQL on the read side. An explicit repair keeps the
// historical cause turn in source_turn and records when the correction entered the
// projection in repair_recorded_turn, so without the repair turn this event would
// lose to the very history it corrects and the consumer would restore the state
// the user fixed.
func TestD1StatusEventLookupLatestCurrentProjectionRepairTurnOutranksHistory(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "story_clock", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-live", "turn-1", "active")

	repair := map[string]any{"source_unit_id": "unit-50", "source_contract": StateRepairContract,
		"repair_recorded_turn": 20}
	d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
		ID: 50, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 3, CreatedAt: d1StatusEventLookupTime().Add(2 * time.Hour),
		Evidence: d1StatusEventLookupEvidence("", true, repair),
	})
	d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
		ID: 51, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 9, CreatedAt: d1StatusEventLookupTime(),
		Evidence: d1StatusEventLookupEvidence("rev-live", true, d1StatusEventLookupUnit(51)),
	})
	d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
		ID: 52, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 3, CreatedAt: d1StatusEventLookupTime().Add(3 * time.Hour),
		Evidence: d1StatusEventLookupEvidence("rev-live", true, d1StatusEventLookupUnit(52)),
	})

	event, err := st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", "story_clock")
	if err != nil {
		t.Fatalf("GetLatestCurrentProjectionStatusChangeEvent: %v", err)
	}
	if event.ID != 50 {
		t.Fatalf("latest current projection = %d, want 50 (the repair recorded at turn 20)", event.ID)
	}
	// Remove the repair and the same three rows must resolve to turn 9, which is
	// what proves the repair_recorded_turn carried the win rather than the id.
	if _, err := conn.Exec(ctx, `DELETE FROM status_change_events WHERE id = 50`); err != nil {
		t.Fatalf("delete repair event: %v", err)
	}
	event, err = st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", "story_clock")
	if err != nil {
		t.Fatalf("GetLatestCurrentProjectionStatusChangeEvent after delete: %v", err)
	}
	if event.ID != 51 {
		t.Fatalf("latest current projection without the repair = %d, want 51 (turn 9 beats turn 3)", event.ID)
	}
}

// TestD1StatusEventLookupLatestCurrentProjectionEligibilityForms walks every way
// an event can be admitted or excluded, so one broken disjunct cannot hide behind
// the others. A JSON string "true" has to be admitted next to a JSON boolean true
// because the boolean is what ApplyReversibleStatusTransition writes and the
// string is what older rows carry; dropping the string form reports the projection
// missing for a session that plainly has one.
func TestD1StatusEventLookupLatestCurrentProjectionEligibilityForms(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "story_clock", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-live", "turn-1", "active")

	for id, statusKey := range map[int64]string{
		60: "boolean_flag", 61: "string_flag", 62: "false_flag",
		63: "absent_flag", 64: "orphan_flag", 65: "repair_flag",
	} {
		var evidence string
		switch statusKey {
		case "boolean_flag":
			evidence = d1StatusEventLookupEvidence("rev-live", true, d1StatusEventLookupUnit(id))
		case "string_flag":
			evidence = d1StatusEventLookupEvidence("rev-live", "true", d1StatusEventLookupUnit(id))
		case "false_flag":
			evidence = d1StatusEventLookupEvidence("rev-live", false, d1StatusEventLookupUnit(id))
		case "absent_flag":
			evidence = d1StatusEventLookupEvidence("rev-live", nil, d1StatusEventLookupUnit(id))
		case "orphan_flag":
			// Cites a source that was never accepted at all.
			evidence = d1StatusEventLookupEvidence("rev-never-accepted", true, d1StatusEventLookupUnit(id))
		case "repair_flag":
			// An explicit correction of an imported history: no source revision
			// exists, so only the state_repair.v1 disjunct can admit it.
			evidence = d1StatusEventLookupEvidence("", true, map[string]any{
				"source_unit_id": "unit-65", "source_contract": StateRepairContract, "repair_recorded_turn": 5,
			})
		}
		d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
			ID: id, RegistryID: registry, StatusKey: statusKey, OwnerScope: "session", OwnerID: "current",
			SourceTurn: 5, CreatedAt: d1StatusEventLookupTime(),
			Evidence: evidence,
		})
	}

	for _, tc := range []struct {
		statusKey    string
		wantEventID  int64
		wantNotFound bool
	}{
		{statusKey: "boolean_flag", wantEventID: 60},
		{statusKey: "string_flag", wantEventID: 61},
		{statusKey: "false_flag", wantNotFound: true},
		{statusKey: "absent_flag", wantNotFound: true},
		{statusKey: "orphan_flag", wantNotFound: true},
		{statusKey: "repair_flag", wantEventID: 65},
	} {
		t.Run(tc.statusKey, func(t *testing.T) {
			event, err := st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", tc.statusKey)
			if tc.wantNotFound {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("error = %v, want ErrNotFound", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetLatestCurrentProjectionStatusChangeEvent: %v", err)
			}
			if event.ID != tc.wantEventID {
				t.Fatalf("resolved event %d, want %d", event.ID, tc.wantEventID)
			}
		})
	}
}

// TestD1StatusEventLookupLatestCurrentProjectionTiebreakPrefersHigherID pins the
// trailing tiebreak. Two observations sharing one observation turn must have a
// deterministic winner, and it must be the same one the rebuild anti-join in
// d1StatusTransitionLatestProjectionEvents picks, or the two reads would disagree
// about the current state of the same slot.
func TestD1StatusEventLookupLatestCurrentProjectionTiebreakPrefersHigherID(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "story_clock", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-live", "turn-1", "active")

	for _, id := range []int64{70, 71} {
		d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
			ID: id, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
			SourceTurn: 6, CreatedAt: d1StatusEventLookupTime().Add(time.Duration(id-70) * time.Hour),
			Evidence: d1StatusEventLookupEvidence("rev-live", true, d1StatusEventLookupUnit(id)),
		})
	}

	event, err := st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", "story_clock")
	if err != nil {
		t.Fatalf("GetLatestCurrentProjectionStatusChangeEvent: %v", err)
	}
	if event.ID != 71 {
		t.Fatalf("latest current projection = %d, want 71 (higher id breaks the observation-turn tie)", event.ID)
	}
	// The rebuild read must name the same winner for the same slot, or the ledger
	// and the projection have drifted apart.
	projection, err := st.ListLatestReversibleCurrentProjectionEvents(ctx, "session-1", []string{"story_clock"})
	if err != nil {
		t.Fatalf("ListLatestReversibleCurrentProjectionEvents: %v", err)
	}
	if len(projection) != 1 || projection[0].ID != 71 {
		t.Fatalf("rebuild read = %+v, want the single event 71 the lookup chose", projection)
	}
}

// TestD1StatusEventLookupLatestCurrentProjectionNotFoundWhenNothingQualifies
// covers the session that simply has no current projection. ErrNotFound is the
// signal the consumer turns into "nothing to restore", so anything else here would
// abort a rollback with a 500 instead of completing it.
func TestD1StatusEventLookupLatestCurrentProjectionNotFoundWhenNothingQualifies(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "story_clock", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-live", "turn-1", "active")

	// An audit event: right key, right revision, but not a projection.
	d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
		ID: 80, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 6, CreatedAt: d1StatusEventLookupTime(), EventState: "retracted",
		Evidence: d1StatusEventLookupEvidence("rev-live", nil, d1StatusEventLookupUnit(80)),
	})

	if _, err := st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", "story_clock"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("error = %v, want ErrNotFound", err)
	}
	if _, err := st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", "never_seen"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown key error = %v, want ErrNotFound", err)
	}
	// The evidence lookup still sees the same row, which is the asymmetry the
	// two methods exist to express.
	event, err := st.GetStatusChangeEventBySourceRevision(ctx, "session-1", "story_clock", "rev-live", 6)
	if err != nil {
		t.Fatalf("GetStatusChangeEventBySourceRevision: %v", err)
	}
	if event.ID != 80 || event.EventState != "retracted" {
		t.Fatalf("evidence lookup returned %+v, want the retracted audit event 80", event)
	}
}

// TestD1StatusEventLookupRestoresTheSurvivingClockAfterRollback is the consumer
// test. turn_story_clock.restoreStoryClockCurrentAfterRollback rebuilds the story
// clock from this event after a non-canonical rollback, copying its registry id,
// value JSON, evidence, turn and creation time into status_current_values; when
// the store does not implement the interface that function returns (0, nil) and
// the clock is never rebuilt at all. So the field set asserted here is exactly
// what the consumer persists, and it has to be the surviving event: restoring the
// rolled-back one would put state the user discarded back into the current
// projection, with no error anywhere to show for it.
func TestD1StatusEventLookupRestoresTheSurvivingClockAfterRollback(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	registry := d1StatusEventLookupSeedRegistry(t, conn, "session-1", "story_clock", "session")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-live", "turn-1", "active")
	d1SeedIdentityRevision(t, conn, "session-1", "rev-dead", "turn-2", "invalidated")

	surviving := d1StatusEventLookupTime()
	d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
		ID: 90, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 9, CreatedAt: surviving,
		Evidence: d1StatusEventLookupEvidence("rev-live", true, d1StatusEventLookupUnit(90)),
	})
	d1StatusEventLookupSeed(t, conn, "session-1", d1StatusEventLookupEvent{
		ID: 91, RegistryID: registry, StatusKey: "story_clock", OwnerScope: "session", OwnerID: "current",
		SourceTurn: 12, CreatedAt: surviving.Add(time.Hour),
		Evidence: d1StatusEventLookupEvidence("rev-dead", true, d1StatusEventLookupUnit(91)),
	})

	latest, err := st.GetLatestCurrentProjectionStatusChangeEvent(ctx, "session-1", "story_clock")
	if err != nil {
		t.Fatalf("GetLatestCurrentProjectionStatusChangeEvent: %v", err)
	}
	if latest.ID == 0 || latest.NewValueJSON == "" {
		t.Fatalf("the consumer restores from NewValueJSON, so an empty value means no restore: %+v", latest)
	}
	if latest.ID != 90 {
		t.Fatalf("the consumer would restore event %d, want the surviving 90", latest.ID)
	}
	if latest.RegistryID != registry {
		t.Errorf("registry id = %d, want %d (the consumer copies it onto the current value)", latest.RegistryID, registry)
	}
	if latest.SourceTurn != 9 {
		t.Errorf("source turn = %d, want 9", latest.SourceTurn)
	}
	if latest.NewValueJSON != `{"marker":90}` {
		t.Errorf("new value = %s, want the surviving clock marker 90", latest.NewValueJSON)
	}
	if want := d1TimeInstant(surviving); !latest.CreatedAt.Equal(want) {
		t.Errorf("created_at = %s, want %s", latest.CreatedAt, want)
	}
	// One more step of the consumer: the value it writes has to be the surviving
	// clock, not the rolled-back one, or the rollback silently undid itself.
	if _, err := st.SaveStatusCurrentValue(ctx, StatusCurrentValue{
		ChatSessionID: "session-1", RegistryID: latest.RegistryID, StatusKey: latest.StatusKey,
		OwnerScope: "session", OwnerID: "current", OwnerLabel: "Current story clock",
		ValueKind: "object", ValueJSON: latest.NewValueJSON, EvidenceJSON: latest.EvidenceJSON,
		SourceTurn: latest.SourceTurn, WriteState: "current", CreatedAt: latest.CreatedAt, UpdatedAt: d1StatusEventLookupTime(),
	}); err != nil {
		t.Fatalf("SaveStatusCurrentValue: %v", err)
	}
	values, err := st.ListStatusCurrentValues(ctx, "session-1", "session", "current", "story_clock", 10)
	if err != nil {
		t.Fatalf("ListStatusCurrentValues: %v", err)
	}
	if len(values) != 1 || values[0].ValueJSON != `{"marker":90}` {
		t.Fatalf("restored current value = %+v, want the surviving clock marker 90", values)
	}
}
