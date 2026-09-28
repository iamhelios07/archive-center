package store

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// D1 typed-lane precise-memory read tests.
//
// Every test runs against the real SQLite engine through the same D1 transport
// the Worker uses, so these assert the statements D1 actually executes. The
// seeds deliberately carry one row per way a filter can exclude a row, because
// the failure mode that matters for these three readers is a read that silently
// widens: a unit whose source revision was superseded, or whose privacy scope
// excludes it, would then be rendered as current fact about who knows what.

// ---------------------------------------------------------------------------
// seeds
// ---------------------------------------------------------------------------

// d1PreciseUnitSeed describes one precise_memory_units row. Zero-valued string
// fields fall back to the values the admission path writes, so a test states
// only the columns it is actually about.
type d1PreciseUnitSeed struct {
	unitID         string
	sessionID      string
	sourceRevision string
	turnStart      int
	turnEnd        int
	kind           string
	subtype        string
	payload        string
	actor          string
	subject        string
	affected       string
	location       string
	object         string
	relationship   string
	truthScope     string
	epistemicMode  string
	authority      string
	admission      string
	review         string
	visibility     string
	holder         string
	reveal         string
	confidence     float64
	lifecycle      string
}

// d1PreciseUnitDefaults returns the admission path's default projection so each
// seed states only what it is testing.
func d1PreciseUnitDefaults(seed d1PreciseUnitSeed) d1PreciseUnitSeed {
	if seed.sessionID == "" {
		seed.sessionID = "s1"
	}
	if seed.sourceRevision == "" {
		seed.sourceRevision = "s1-rev-live"
	}
	if seed.turnEnd == 0 {
		seed.turnEnd = seed.turnStart
	}
	if seed.kind == "" {
		seed.kind = "event"
	}
	if seed.payload == "" {
		seed.payload = `{"summary":"seeded"}`
	}
	if seed.truthScope == "" {
		seed.truthScope = "objective"
	}
	if seed.epistemicMode == "" {
		seed.epistemicMode = "direct"
	}
	if seed.authority == "" {
		seed.authority = "objective_world_state"
	}
	if seed.admission == "" {
		seed.admission = "committed"
	}
	if seed.review == "" {
		seed.review = "source_observed"
	}
	if seed.visibility == "" {
		seed.visibility = "public"
	}
	if seed.lifecycle == "" {
		seed.lifecycle = "active"
	}
	return seed
}

// d1SeedPreciseUnit inserts one precise_memory_units row. Every optional
// identity column is written as NULL when the seed leaves it empty, so the
// COALESCE read path is exercised exactly as the canonical schema stores it.
func d1SeedPreciseUnit(t *testing.T, conn *sqliteD1Conn, seed d1PreciseUnitSeed) {
	t.Helper()
	seed = d1PreciseUnitDefaults(seed)
	if _, err := conn.Exec(context.Background(), `INSERT INTO precise_memory_units (
		unit_id, contract_version, chat_session_id, source_turn_start, source_turn_end,
		source_contract, source_revision, source_content_hash, source_role,
		source_span_start, source_span_end, evidence_excerpt, evidence_hash,
		direct_evidence_ids_json, memory_kind, memory_subtype, payload_json,
		actor_entity_id, subject_entity_id, affected_entity_id, location_entity_id,
		object_entity_id, relationship_key, truth_scope, epistemic_mode,
		authority_class, admission_state, review_state, visibility,
		knowledge_holder_entity_id, reveal_condition, confidence, idempotency_key,
		lifecycle_state
	) VALUES (
		?, ?, ?, ?, ?,
		'source_acceptance_observation.v1', ?, ?, 'combined_turn_pair',
		0, 1, 'excerpt', 'evidence-hash',
		'[]', ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?, ?,
		?, ?, ?,
		?, ?
	)`,
		seed.unitID, PreciseMemoryUnitContract, seed.sessionID, seed.turnStart, seed.turnEnd,
		seed.sourceRevision, "content-hash-"+seed.unitID,
		seed.kind, d1NullableString(seed.subtype), seed.payload,
		d1NullableString(seed.actor), d1NullableString(seed.subject),
		d1NullableString(seed.affected), d1NullableString(seed.location),
		d1NullableString(seed.object), d1NullableString(seed.relationship),
		seed.truthScope, seed.epistemicMode, seed.authority, seed.admission, seed.review,
		seed.visibility, d1NullableString(seed.holder), d1NullableString(seed.reveal),
		seed.confidence, "ik-"+seed.unitID, seed.lifecycle,
	); err != nil {
		t.Fatalf("seed precise unit %s: %v", seed.unitID, err)
	}
}

// d1PreciseUnitIDs reduces a unit list to its ids for order assertions.
func d1PreciseUnitIDs(units []PreciseMemoryUnit) []string {
	out := make([]string, 0, len(units))
	for _, unit := range units {
		out = append(out, unit.UnitID)
	}
	return out
}

// equalStringSlices compares two string slices element by element.
func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// d1PreciseScene holds the entity ids a typed-lane scene points at. They are
// session-qualified because entity_identities.stable_entity_id is a global
// primary key on both providers, not a per-session column.
type d1PreciseScene struct {
	HolderA   string
	HolderB   string
	Actor     string
	Subject   string
	Affected  string
	Location  string
	LiveRev   string
	Supersede string
	GoneRev   string
	NoRev     string
}

// d1SeedPreciseScene seeds the identities the typed lanes point at plus one
// live, one superseded, and one invalidated source revision, so every fence in
// these tests has something to reject. NoRev names a revision that does not
// exist at all, covering a unit written against a source that was never
// accepted.
func d1SeedPreciseScene(t *testing.T, conn *sqliteD1Conn, sessionID string) d1PreciseScene {
	t.Helper()
	scene := d1PreciseScene{
		HolderA:   sessionID + "-holder-a",
		HolderB:   sessionID + "-holder-b",
		Actor:     sessionID + "-actor",
		Subject:   sessionID + "-subject",
		Affected:  sessionID + "-affected",
		Location:  sessionID + "-location",
		LiveRev:   sessionID + "-rev-live",
		Supersede: sessionID + "-rev-dead",
		GoneRev:   sessionID + "-rev-gone",
		NoRev:     sessionID + "-rev-never-accepted",
	}
	d1SeedIdentity(t, conn, scene.HolderA, sessionID, "session_npc", "character", "Rowan", "active", EntityIdentityReviewStateSourceObserved, scene.LiveRev, 1, 1)
	d1SeedIdentity(t, conn, scene.HolderB, sessionID, "session_npc", "character", "Sasha", "active", EntityIdentityReviewStateSourceObserved, scene.LiveRev, 1, 1)
	d1SeedIdentity(t, conn, scene.Actor, sessionID, "session_npc", "character", "Alex", "active", EntityIdentityReviewStateSourceObserved, scene.LiveRev, 1, 1)
	d1SeedIdentity(t, conn, scene.Subject, sessionID, "session_npc", "character", "Bell", "active", EntityIdentityReviewStateSourceObserved, scene.LiveRev, 1, 1)
	d1SeedIdentity(t, conn, scene.Affected, sessionID, "session_npc", "character", "Cora", "active", EntityIdentityReviewStateSourceObserved, scene.LiveRev, 1, 1)
	d1SeedIdentity(t, conn, scene.Location, sessionID, "session_place", "place", "Archive", "active", EntityIdentityReviewStateSourceObserved, scene.LiveRev, 1, 1)
	// Two live revisions in one session would need two logical turns: the schema
	// permits only one active revision per logical turn. The dead ones are not
	// constrained, but distinct ids keep the fixture readable.
	d1SeedIdentityRevision(t, conn, sessionID, scene.LiveRev, "turn-1", "active")
	d1SeedIdentityRevision(t, conn, sessionID, scene.Supersede, "turn-2", "superseded")
	d1SeedIdentityRevision(t, conn, sessionID, scene.GoneRev, "turn-3", "invalidated")
	return scene
}

// ---------------------------------------------------------------------------
// character perspective lane
// ---------------------------------------------------------------------------

// TestD1ListCharacterPerspectiveMemoryUnitsFencesOrdersAndScopes pins the
// perspective lane's whole contract: the active-source-revision join, the exact
// knowledge-holder equality, the observation-only kind, the closed epistemic
// set, the active lifecycle, and the source_turn_start/unit_id ordering.
//
// A single row per exclusion axis is included in the seed, because the
// dangerous regression is a widened read: a unit written about one character's
// knowledge leaking into another character's view, or a superseded extraction
// still answering as current fact.
func TestD1ListCharacterPerspectiveMemoryUnitsFencesOrdersAndScopes(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	scene := d1SeedPreciseScene(t, conn, "s1")
	d1SeedPreciseScene(t, conn, "s2")

	// Included: three holder-relative units seeded out of turn order, with a
	// same-turn tie so the tie-break is exercised.
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-turn-3", sourceRevision: scene.LiveRev, turnStart: 3,
		kind: "observation", subtype: "access", epistemicMode: "suspected",
		truthScope: "owner_scoped", authority: "subjective_episodic",
		admission: "review_required", review: "needs_review", visibility: "owner_private",
		actor: scene.Actor, subject: scene.Subject, holder: scene.HolderA,
		payload: `{"contract_version":"perspective_memory.v1"}`,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-turn-1b", sourceRevision: scene.LiveRev, turnStart: 1,
		kind: "observation", epistemicMode: "hidden", truthScope: "owner_scoped",
		authority: "subjective_episodic", visibility: "owner_private",
		actor: scene.Actor, holder: scene.HolderA,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-turn-1", sourceRevision: scene.LiveRev, turnStart: 1,
		kind: "observation", epistemicMode: "known", truthScope: "owner_scoped",
		authority: "subjective_episodic", visibility: "owner_private",
		actor: scene.Actor, holder: scene.HolderA, reveal: "after_turn_9",
	})
	// Excluded, one per axis.
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // epistemic mode outside the closed set
		unitID: "u-direct", sourceRevision: scene.LiveRev, turnStart: 2,
		kind: "observation", epistemicMode: "direct", holder: scene.HolderA,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // not an observation
		unitID: "u-boundary", sourceRevision: scene.LiveRev, turnStart: 2,
		kind: "boundary", epistemicMode: "known", holder: scene.HolderA,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // retired unit
		unitID: "u-retired", sourceRevision: scene.LiveRev, turnStart: 2,
		kind: "observation", epistemicMode: "known", holder: scene.HolderA, lifecycle: "invalidated",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // superseded source revision
		unitID: "u-superseded", sourceRevision: scene.Supersede, turnStart: 2,
		kind: "observation", epistemicMode: "known", holder: scene.HolderA,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // invalidated source revision
		unitID: "u-invalidated", sourceRevision: scene.GoneRev, turnStart: 2,
		kind: "observation", epistemicMode: "known", holder: scene.HolderA,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // never-accepted source revision
		unitID: "u-orphan", sourceRevision: scene.NoRev, turnStart: 2,
		kind: "observation", epistemicMode: "known", holder: scene.HolderA,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // another character's knowledge
		unitID: "u-other-holder", sourceRevision: scene.LiveRev, turnStart: 2,
		kind: "observation", epistemicMode: "known", holder: scene.HolderB,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // no knowledge holder at all
		unitID: "u-no-holder", sourceRevision: scene.LiveRev, turnStart: 2,
		kind: "observation", epistemicMode: "known",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // another session
		unitID: "u-other-session", sessionID: "s2", sourceRevision: "s2-rev-live", turnStart: 2,
		kind: "observation", epistemicMode: "known", holder: scene.HolderA,
	})

	units, err := st.ListCharacterPerspectiveMemoryUnits(ctx, "s1", scene.HolderA)
	if err != nil {
		t.Fatalf("ListCharacterPerspectiveMemoryUnits: %v", err)
	}

	// source_turn_start ASC, unit_id ASC: the turn-1 pair ordered by unit id,
	// then turn 3. Insertion order would have put the turn-3 unit first.
	want := []string{"u-turn-1", "u-turn-1b", "u-turn-3"}
	if got := d1PreciseUnitIDs(units); !equalStringSlices(got, want) {
		t.Fatalf("perspective units = %v, want %v", got, want)
	}

	first := units[0]
	if first.ChatSessionID != "s1" || first.KnowledgeHolderEntityID != scene.HolderA ||
		first.Kind != "observation" || first.EpistemicMode != "known" ||
		first.SourceRevision != scene.LiveRev || first.LifecycleState != "active" {
		t.Errorf("perspective projection = %+v", first)
	}
	if first.ActorEntityID != scene.Actor {
		t.Errorf("actor identity lost: %+v", first)
	}
	if first.RevealCondition != "after_turn_9" {
		t.Errorf("present reveal_condition must round-trip, got %q", first.RevealCondition)
	}
	// A NULL optional column reads as empty text on both providers, never as a
	// leftover NULL marker the caller would have to special-case.
	if first.Subtype != "" || first.SubjectEntityID != "" {
		t.Errorf("NULL optional columns must read as empty text: %+v", first)
	}
	if first.CreatedAt.IsZero() || first.UpdatedAt.IsZero() {
		t.Errorf("timestamps must be parsed, not left zero: %+v", first)
	}
	// Review metadata is returned verbatim: an unreviewed unit still describes
	// the occurrence, and only the caller decides what to do about it.
	if third := units[2]; third.AdmissionState != "review_required" || third.ReviewState != "needs_review" {
		t.Errorf("review metadata was rewritten: %+v", third)
	}

	// The other holder sees only its own units, never a neighbour's.
	other, err := st.ListCharacterPerspectiveMemoryUnits(ctx, "s1", scene.HolderB)
	if err != nil {
		t.Fatalf("other holder read: %v", err)
	}
	if got := d1PreciseUnitIDs(other); !equalStringSlices(got, []string{"u-other-holder"}) {
		t.Errorf("second holder read = %v, want only its own unit", got)
	}

	// A holder with no perspective units is an empty result, not another
	// holder's rows and not an error, and the slice is non-nil so a JSON response
	// encodes [] rather than null on either provider.
	none, err := st.ListCharacterPerspectiveMemoryUnits(ctx, "s1", scene.Actor)
	if err != nil {
		t.Fatalf("holder without perspective units: %v", err)
	}
	if none == nil || len(none) != 0 {
		t.Errorf("empty perspective read = %#v, want an empty non-nil slice", none)
	}
}

// TestD1ListCharacterPerspectiveMemoryUnitsRejectsBlankScope pins the argument
// contract. prepare-turn filters ErrNotFound out of its read diagnostics but
// records any other error as a degraded lane, so a blank session or holder must
// stay the sentinel and must not reach the database at all.
func TestD1ListCharacterPerspectiveMemoryUnitsRejectsBlankScope(t *testing.T) {
	st, conn := newD1ProbeStore(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name   string
		sid    string
		holder string
	}{
		{name: "blank session", sid: "", holder: "s1-holder-a"},
		{name: "whitespace session", sid: "   ", holder: "s1-holder-a"},
		{name: "blank holder", sid: "s1", holder: ""},
		{name: "whitespace holder", sid: "s1", holder: "\t\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := len(conn.queries)
			units, err := st.ListCharacterPerspectiveMemoryUnits(ctx, tc.sid, tc.holder)
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("error = %v, want ErrNotFound", err)
			}
			if units != nil {
				t.Errorf("rejected read returned units: %+v", units)
			}
			if len(conn.queries) != before {
				t.Errorf("rejected read sent %d statements, want none", len(conn.queries)-before)
			}
		})
	}

	// A padded but real scope is trimmed rather than rejected, so a caller that
	// padded its session id still reads its units.
	scene := d1SeedPreciseScene(t, conn.sqliteD1Conn, "s1")
	d1SeedPreciseUnit(t, conn.sqliteD1Conn, d1PreciseUnitSeed{
		unitID: "u-trim", sourceRevision: scene.LiveRev, turnStart: 1,
		kind: "observation", epistemicMode: "known", holder: scene.HolderA,
	})
	units, err := st.ListCharacterPerspectiveMemoryUnits(ctx, "  s1  ", "  "+scene.HolderA+"  ")
	if err != nil {
		t.Fatalf("padded scope read: %v", err)
	}
	if got := d1PreciseUnitIDs(units); !equalStringSlices(got, []string{"u-trim"}) {
		t.Errorf("padded scope read = %v, want the seeded unit", got)
	}
}

// ---------------------------------------------------------------------------
// active interaction lane
// ---------------------------------------------------------------------------

// TestD1ListActiveInteractionMemoryUnitsReturnsObservationsAndBoundariesAcrossReview
// pins the interaction lane: relationship observations and interaction
// boundaries are returned, every other kind is not, the relationship identities
// hydrate, review metadata does not erase the occurrence, and the same active
// source-revision fence and turn ordering apply.
func TestD1ListActiveInteractionMemoryUnitsReturnsObservationsAndBoundariesAcrossReview(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	scene := d1SeedPreciseScene(t, conn, "s1")

	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-boundary-3", sourceRevision: scene.LiveRev, turnStart: 3,
		kind: "boundary", subtype: "withdrawn", epistemicMode: "explicit_boundary",
		truthScope: "actor_scoped", authority: "subjective_episodic",
		admission: "review_required", review: "needs_review", visibility: "owner_private",
		actor: scene.Actor, affected: scene.Affected,
		relationship: scene.Actor + "->" + scene.Affected + "/touch",
		payload:      `{"contract_version":"interaction_boundary.v1"}`,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-relation-2", sourceRevision: scene.LiveRev, turnStart: 2,
		kind: "observation", subtype: "relationship_trust", epistemicMode: "direct",
		truthScope: "source_scoped", authority: "subjective_episodic",
		actor: scene.Actor, affected: scene.Affected,
		relationship: scene.Actor + "->" + scene.Affected + "/trust",
		payload:      `{"contract_version":"relationship_observation.v1"}`,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-relation-2b", sourceRevision: scene.LiveRev, turnStart: 2,
		kind: "observation", epistemicMode: "direct", actor: scene.Actor, object: scene.Subject,
	})
	// Excluded, one per axis.
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-event", sourceRevision: scene.LiveRev, turnStart: 1, kind: "event",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-state", sourceRevision: scene.LiveRev, turnStart: 1, kind: "state",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-utterance", sourceRevision: scene.LiveRev, turnStart: 1, kind: "utterance",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-profile", sourceRevision: scene.LiveRev, turnStart: 1, kind: "profile",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-retired", sourceRevision: scene.LiveRev, turnStart: 1,
		kind: "observation", lifecycle: "invalidated",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-superseded", sourceRevision: scene.Supersede, turnStart: 1,
		kind: "observation",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{
		unitID: "u-orphan", sourceRevision: scene.NoRev, turnStart: 1, kind: "observation",
	})

	units, err := st.ListActiveInteractionMemoryUnits(ctx, "s1")
	if err != nil {
		t.Fatalf("ListActiveInteractionMemoryUnits: %v", err)
	}

	// source_turn_start ASC, unit_id ASC, and the boundary kind included.
	want := []string{"u-relation-2", "u-relation-2b", "u-boundary-3"}
	if got := d1PreciseUnitIDs(units); !equalStringSlices(got, want) {
		t.Fatalf("interaction units = %v, want %v", got, want)
	}

	relation := units[0]
	if relation.ActorEntityID != scene.Actor || relation.AffectedEntityID != scene.Affected ||
		relation.RelationshipKey != scene.Actor+"->"+scene.Affected+"/trust" ||
		relation.Subtype != "relationship_trust" {
		t.Errorf("relationship projection = %+v", relation)
	}
	// A NULL identity column reads as empty text rather than dropping the row.
	if relation.SubjectEntityID != "" || relation.ObjectEntityID != "" || relation.KnowledgeHolderEntityID != "" {
		t.Errorf("NULL identities must read as empty text: %+v", relation)
	}
	if units[1].ObjectEntityID != scene.Subject {
		t.Errorf("object identity lost: %+v", units[1])
	}
	// The boundary keeps its review metadata and its owner-private visibility:
	// the lane reports the occurrence, the caller decides what to render.
	boundary := units[2]
	if boundary.Kind != "boundary" || boundary.AdmissionState != "review_required" ||
		boundary.ReviewState != "needs_review" || boundary.Visibility != "owner_private" {
		t.Errorf("boundary projection = %+v", boundary)
	}
	if boundary.CreatedAt.IsZero() || boundary.UpdatedAt.IsZero() {
		t.Errorf("timestamps must be parsed: %+v", boundary)
	}

	empty, err := st.ListActiveInteractionMemoryUnits(ctx, "s1-empty")
	if err != nil {
		t.Fatalf("empty session read: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Errorf("empty interaction read = %#v, want an empty non-nil slice", empty)
	}
}

// TestD1ListActiveInteractionMemoryUnitsRejectsBlankSession pins the sentinel
// error and the no-statement guarantee for the interaction lane.
func TestD1ListActiveInteractionMemoryUnitsRejectsBlankSession(t *testing.T) {
	st, conn := newD1ProbeStore(t)
	ctx := context.Background()

	for _, sid := range []string{"", "   ", "\t"} {
		before := len(conn.queries)
		units, err := st.ListActiveInteractionMemoryUnits(ctx, sid)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("session %q error = %v, want ErrNotFound", sid, err)
		}
		if units != nil {
			t.Errorf("session %q returned units: %+v", sid, units)
		}
		if len(conn.queries) != before {
			t.Errorf("session %q sent %d statements, want none", sid, len(conn.queries)-before)
		}
	}
}

// ---------------------------------------------------------------------------
// general vector lane
// ---------------------------------------------------------------------------

// TestD1ListGeneralVectorPreciseMemoryUnitsAppliesCurrentEligibilityInInsertionOrder
// pins the general-vector lane's two halves: the SQL fence (session, active
// unit, active source revision, no memory-kind filter) and the Go-side
// eligibility rule, which must come from the shared function the write path uses
// so the indexed set and the read-back inventory cannot disagree.
//
// The seeds are deliberately inserted out of turn order, because this lane
// orders by id, not by the scene chronology: a turn-ordered read would look
// correct on one fixture and silently reorder the caller's candidate budget.
func TestD1ListGeneralVectorPreciseMemoryUnitsAppliesCurrentEligibilityInInsertionOrder(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	scene := d1SeedPreciseScene(t, conn, "s1")

	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // inserted first, latest turn
		unitID: "u-late-turn", sourceRevision: scene.LiveRev, turnStart: 9,
		kind: "event", subtype: "observed_event", location: scene.Location, confidence: 0.9,
		payload: `{"summary":"public event"}`,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // awaiting review, still eligible
		unitID: "u-review", sourceRevision: scene.LiveRev, turnStart: 2,
		kind: "event", authority: "support_hypothesis", admission: "review_required",
		review: "needs_review", location: scene.Location, confidence: 0.6,
		payload: `{"summary":"review event"}`,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // a boundary is in scope on this lane
		unitID: "u-boundary", sourceRevision: scene.LiveRev, turnStart: 5,
		kind: "boundary", epistemicMode: "explicit_boundary", actor: scene.Actor,
	})
	// Excluded by eligibility, one per axis.
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // private visibility
		unitID: "u-private", sourceRevision: scene.LiveRev, turnStart: 3,
		kind: "observation", epistemicMode: "direct", visibility: "owner_private",
		actor: scene.Actor, subject: scene.Subject,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // user profile lane
		unitID: "u-profile", sourceRevision: scene.LiveRev, turnStart: 3,
		kind: "profile", subtype: "user_interaction", visibility: "user_private",
		epistemicMode: "explicit_ooc_setting",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // a knowledge holder owns a typed lane
		unitID: "u-held", sourceRevision: scene.LiveRev, turnStart: 3,
		kind: "observation", epistemicMode: "direct", holder: scene.HolderA,
		visibility: "owner_private", truthScope: "actor_scoped",
		authority: "subjective_episodic",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // holder-relative epistemic mode
		unitID: "u-known", sourceRevision: scene.LiveRev, turnStart: 3,
		kind: "observation", epistemicMode: "known",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // holder-relative epistemic mode
		unitID: "u-suspected", sourceRevision: scene.LiveRev, turnStart: 3,
		kind: "observation", epistemicMode: "suspected",
	})
	// Excluded by the SQL fence.
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // retired unit
		unitID: "u-retired", sourceRevision: scene.LiveRev, turnStart: 1,
		lifecycle: "invalidated",
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // superseded source revision
		unitID: "u-superseded", sourceRevision: scene.Supersede, turnStart: 1,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // never-accepted source revision
		unitID: "u-orphan", sourceRevision: scene.NoRev, turnStart: 1,
	})
	d1SeedPreciseUnit(t, conn, d1PreciseUnitSeed{ // another session
		unitID: "u-other-session", sessionID: "s2", sourceRevision: "s2-rev-live", turnStart: 1,
	})

	units, err := st.ListGeneralVectorPreciseMemoryUnits(ctx, "s1")
	if err != nil {
		t.Fatalf("ListGeneralVectorPreciseMemoryUnits: %v", err)
	}

	// Insertion order, not turn order: u-late-turn (turn 9) precedes u-review
	// (turn 2) and u-boundary (turn 5).
	want := []string{"u-late-turn", "u-review", "u-boundary"}
	got := d1PreciseUnitIDs(units)
	if !equalStringSlices(got, want) {
		t.Fatalf("general vector units = %v, want %v (id ASC, not turn ASC)", got, want)
	}

	first := units[0]
	if first.ID <= 0 || first.SourceTurnStart != 9 || first.Kind != "event" || first.Subtype != "observed_event" {
		t.Errorf("general vector projection lost the canonical fact: %+v", first)
	}
	if !strings.Contains(first.PayloadJSON, "public event") || first.LocationEntityID != scene.Location {
		t.Errorf("fact text or location identity lost: %+v", first)
	}
	if first.Confidence != 0.9 {
		t.Errorf("confidence = %v, want 0.9", first.Confidence)
	}
	// Ids strictly ascend, which is the ordering the caller's budget depends on.
	for i := 1; i < len(units); i++ {
		if units[i-1].ID >= units[i].ID {
			t.Fatalf("id ASC ordering broken at %d: %d then %d", i, units[i-1].ID, units[i].ID)
		}
	}
	// Review metadata is preserved on the eligible unit, not used to exclude it.
	if units[1].AdmissionState != "review_required" || units[1].ReviewState != "needs_review" {
		t.Errorf("review metadata was rewritten: %+v", units[1])
	}
	// Relationship and epistemic identities hydrate for every eligible row.
	if units[2].ActorEntityID != scene.Actor || units[2].EpistemicMode != "explicit_boundary" {
		t.Errorf("boundary projection = %+v", units[2])
	}
	// An empty inventory is a non-nil empty slice on both providers.
	empty, err := st.ListGeneralVectorPreciseMemoryUnits(ctx, "s1-empty")
	if err != nil {
		t.Fatalf("empty session read: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Errorf("empty general vector read = %#v, want an empty non-nil slice", empty)
	}
}

// TestD1ListGeneralVectorPreciseMemoryUnitsRejectsBlankSession pins the sentinel
// error and the no-statement guarantee for the general-vector lane.
func TestD1ListGeneralVectorPreciseMemoryUnitsRejectsBlankSession(t *testing.T) {
	st, conn := newD1ProbeStore(t)
	ctx := context.Background()

	for _, sid := range []string{"", "  ", "\n"} {
		before := len(conn.queries)
		units, err := st.ListGeneralVectorPreciseMemoryUnits(ctx, sid)
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("session %q error = %v, want ErrNotFound", sid, err)
		}
		if units != nil {
			t.Errorf("session %q returned units: %+v", sid, units)
		}
		if len(conn.queries) != before {
			t.Errorf("session %q sent %d statements, want none", sid, len(conn.queries)-before)
		}
	}
}

// ---------------------------------------------------------------------------
// statement parity with the MariaDB lane
// ---------------------------------------------------------------------------

// d1ClauseAfter returns the part of a statement from marker onwards, so an
// assertion about a WHERE predicate is not accidentally satisfied by the same
// column name appearing in the projection list.
func d1ClauseAfter(query, marker string) string {
	if idx := strings.Index(query, marker); idx >= 0 {
		return query[idx:]
	}
	return query
}

// TestD1PreciseMemoryLanesKeepTheMariaDBFenceAndShape records the statement
// contract the three D1 lanes share, asserted against the text D1 actually
// receives and compared to the predicates in mariadb_precise_memory.go.
//
// A future edit to one provider that drops the fence, the closed epistemic set,
// the ordering, or the absence of a limit then shows up as a failing expectation
// here instead of as a stale scene on Cloudflare.
func TestD1PreciseMemoryLanesKeepTheMariaDBFenceAndShape(t *testing.T) {
	st, conn := newD1ProbeStore(t)
	ctx := context.Background()
	d1SeedPreciseScene(t, conn.sqliteD1Conn, "s1")

	read := func(call func() error) string {
		t.Helper()
		before := len(conn.queries)
		if err := call(); err != nil {
			t.Fatalf("read failed: %v", err)
		}
		if len(conn.queries) != before+1 {
			t.Fatalf("read sent %d statements, want exactly 1", len(conn.queries)-before)
		}
		return conn.queries[len(conn.queries)-1]
	}
	perspective := read(func() error {
		_, err := st.ListCharacterPerspectiveMemoryUnits(ctx, "s1", "s1-holder-a")
		return err
	})
	interaction := read(func() error {
		_, err := st.ListActiveInteractionMemoryUnits(ctx, "s1")
		return err
	})
	general := read(func() error {
		_, err := st.ListGeneralVectorPreciseMemoryUnits(ctx, "s1")
		return err
	})

	// The fence, predicate for predicate, in all three lanes.
	for name, query := range map[string]string{
		"perspective": perspective, "interaction": interaction, "general": general,
	} {
		for _, want := range []string{
			"FROM precise_memory_units unit",
			"JOIN memory_source_revisions source_revision",
			"ON source_revision.chat_session_id = unit.chat_session_id",
			"AND source_revision.source_revision = unit.source_revision",
			"AND source_revision.lifecycle_state = 'active'",
			"unit.chat_session_id = ?",
			"unit.lifecycle_state = 'active'",
		} {
			if !strings.Contains(query, want) {
				t.Errorf("%s lane is missing the MariaDB predicate %q:\n%s", name, want, query)
			}
		}
		// No LIMIT: the interfaces take none, and prepare-turn applies its own
		// history-scope filter and candidate budget after the read. A page here
		// would truncate the candidate set rather than bound a response.
		if strings.Contains(strings.ToUpper(query), "LIMIT") {
			t.Errorf("%s lane must not page the result set:\n%s", name, query)
		}
		// No de-duplication: source_revision is UNIQUE on both providers, so the
		// join is one-to-one and a DISTINCT would mask a schema regression.
		if strings.Contains(strings.ToUpper(query), "SELECT DISTINCT") {
			t.Errorf("%s lane must not de-duplicate a one-to-one join:\n%s", name, query)
		}
		// Review metadata is returned, never filtered on.
		where := d1ClauseAfter(query, "WHERE")
		for _, forbidden := range []string{"unit.review_state =", "unit.admission_state ="} {
			if strings.Contains(where, forbidden) {
				t.Errorf("%s lane must not filter on review metadata (%s):\n%s", name, forbidden, query)
			}
		}
	}

	// The perspective lane's private scope.
	for _, want := range []string{
		"unit.memory_kind = 'observation'",
		"unit.knowledge_holder_entity_id = ?",
		"unit.epistemic_mode IN ('known', 'suspected', 'unknown', 'misinformed', 'hidden', 'revealed')",
		"ORDER BY unit.source_turn_start ASC, unit.unit_id ASC",
	} {
		if !strings.Contains(perspective, want) {
			t.Errorf("perspective lane is missing %q:\n%s", want, perspective)
		}
	}

	// The interaction lane's two kinds and its scene ordering.
	for _, want := range []string{
		"unit.memory_kind IN ('observation', 'boundary')",
		"ORDER BY unit.source_turn_start ASC, unit.unit_id ASC",
	} {
		if !strings.Contains(interaction, want) {
			t.Errorf("interaction lane is missing %q:\n%s", want, interaction)
		}
	}

	// The general-vector lane filters only session, lifecycle, and the fence, and
	// orders by the candidate inventory's own key.
	if !strings.Contains(general, "ORDER BY unit.id ASC") {
		t.Errorf("general vector lane must order by id ASC:\n%s", general)
	}
	if strings.Contains(d1ClauseAfter(general, "WHERE"), "unit.memory_kind") {
		t.Errorf("general vector lane must not filter on memory_kind:\n%s", general)
	}
	for _, column := range []string{
		"unit.id", "unit.location_entity_id", "unit.confidence", "unit.epistemic_mode",
	} {
		if !strings.Contains(d1ClauseAfter(general, "SELECT"), column) {
			t.Errorf("general vector lane is missing %q from its projection:\n%s", column, general)
		}
	}
}

// TestD1PreciseMemoryLanesSurfaceTransportErrors pins the error half of the
// contract. A failed read must reach the caller as an error rather than as an
// empty slice: prepare-turn treats an empty result as "this session has no
// precise units" and a real error as a degraded lane, and conflating the two
// would hide a broken D1 binding behind a confident "nothing to recall".
func TestD1PreciseMemoryLanesSurfaceTransportErrors(t *testing.T) {
	readers := map[string]func(*d1Store, context.Context) error{
		"perspective": func(st *d1Store, ctx context.Context) error {
			_, err := st.ListCharacterPerspectiveMemoryUnits(ctx, "s1", "s1-holder-a")
			return err
		},
		"interaction": func(st *d1Store, ctx context.Context) error {
			_, err := st.ListActiveInteractionMemoryUnits(ctx, "s1")
			return err
		},
		"general": func(st *d1Store, ctx context.Context) error {
			_, err := st.ListGeneralVectorPreciseMemoryUnits(ctx, "s1")
			return err
		},
	}
	for name, call := range readers {
		t.Run(name, func(t *testing.T) {
			st, conn := newD1ProbeStore(t)
			ctx := context.Background()
			d1SeedPreciseScene(t, conn.sqliteD1Conn, "s1")
			conn.failOn = "FROM precise_memory_units unit"

			err := call(st, ctx)
			if err == nil {
				t.Fatal("a failed read must surface an error, not an empty slice")
			}
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotEnabled) {
				t.Fatalf("a transport failure must not be reported as a sentinel error: %v", err)
			}
		})
	}
}

// TestD1PreciseMemoryLanesAreAdvertisedAsCapabilities checks the consequence the
// routes actually depend on. Every one of these lanes is discovered by a type
// assertion, so a provider that implements the method but is not discoverable
// through the capability manifest would still answer
// "general_precise_memory_reader_unavailable" in the prepare-turn trace.
func TestD1PreciseMemoryLanesAreAdvertisedAsCapabilities(t *testing.T) {
	st, _ := newD1TestStore(t)
	var asStore Store = st

	implemented := map[string]bool{}
	for _, status := range CapabilityReport(asStore) {
		implemented[status.Name] = status.Implemented
	}
	if _, ok := asStore.(CharacterPerspectiveMemoryReader); !ok {
		t.Error("D1 store must satisfy CharacterPerspectiveMemoryReader")
	}
	if _, ok := asStore.(ActiveInteractionMemoryReader); !ok {
		t.Error("D1 store must satisfy ActiveInteractionMemoryReader")
	}
	if _, ok := asStore.(GeneralVectorPreciseMemoryReader); !ok {
		t.Error("D1 store must satisfy GeneralVectorPreciseMemoryReader")
	}
	for _, name := range []string{
		"CharacterPerspectiveMemoryReader", "ActiveInteractionMemoryReader", "GeneralVectorPreciseMemoryReader",
	} {
		if !implemented[name] {
			t.Errorf("capability manifest reports %s as missing", name)
		}
	}

	// The read-only wrapper must keep forwarding all three, since the shadow and
	// read-only deployment paths wrap the selected store in it.
	ro := &readOnlyStore{delegate: st}
	if _, err := ro.ListCharacterPerspectiveMemoryUnits(context.Background(), "s1", "s1-holder-a"); err != nil {
		t.Errorf("read-only perspective read: %v", err)
	}
	if _, err := ro.ListActiveInteractionMemoryUnits(context.Background(), "s1"); err != nil {
		t.Errorf("read-only interaction read: %v", err)
	}
	if _, err := ro.ListGeneralVectorPreciseMemoryUnits(context.Background(), "s1"); err != nil {
		t.Errorf("read-only general vector read: %v", err)
	}
}
