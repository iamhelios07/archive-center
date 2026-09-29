package store

import (
	"context"
	"errors"
	"sort"
	"testing"
)

// D1 entity identity read-capability tests.
//
// Every test runs against the real SQLite engine through the same D1 transport
// the Worker uses, so these assert the statements D1 actually executes rather
// than a mock's idea of them. The seeds deliberately include one row per way
// each filter can exclude a row, because a read that silently widens is how a
// stale extraction keeps answering through a retired revision.

var _ EntityIdentityCatalogReader = (*d1Store)(nil)
var _ ReviewedEntityIdentityResolver = (*d1Store)(nil)
var _ UniqueActiveEntitySurfaceIdentityResolver = (*d1Store)(nil)

// d1SeedIdentityRevision seeds a memory_source_revisions row. The logical turn
// id is explicit because the schema permits only one ACTIVE revision per logical
// turn in a session, so a test that needs two live revisions must give them two
// turns.
func d1SeedIdentityRevision(t *testing.T, conn *sqliteD1Conn, sessionID, revision, logicalTurnID, lifecycle string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO memory_source_revisions (
		source_revision, chat_session_id, logical_turn_id, turn_index,
		raw_user_content, raw_assistant_content, combined_content_hash,
		hash_algorithm, host_observed_at_ms, lifecycle_state
	) VALUES (?, ?, ?, 1, 'u', 'a', ?, 'sha256', 1, ?)`, revision, sessionID, logicalTurnID, revision, lifecycle); err != nil {
		t.Fatalf("seed source revision %s: %v", revision, err)
	}
}

// d1SeedIdentity inserts one entity_identities row. The optional source ids
// stay NULL so the COALESCE read path is exercised, and the idempotency key is
// derived from the stable id to satisfy the per-session unique key.
func d1SeedIdentity(t *testing.T, conn *sqliteD1Conn, stableEntityID, sessionID, namespace, entityKind, label, lifecycle, review, revision string, sourceTurn, firstSeenTurn int) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO entity_identities (
		stable_entity_id, chat_session_id, identity_namespace, entity_kind,
		canonical_label, lifecycle_state, review_state, presence_authority,
		occurrence_authority, source_contract, source_revision, source_content_hash,
		source_turn, source_index, idempotency_key, mapping_revision,
		first_seen_turn, last_seen_turn
	) VALUES (?, ?, ?, ?, ?, ?, ?, 'source_presence', 'source_occurrence',
		'source_acceptance_observation.v1', ?, ?, ?, 0, ?, 2, ?, ?)`,
		stableEntityID, sessionID, namespace, entityKind, label, lifecycle, review, revision,
		"hash-"+stableEntityID, sourceTurn, "ik-"+stableEntityID, firstSeenTurn, firstSeenTurn); err != nil {
		t.Fatalf("seed identity %s: %v", stableEntityID, err)
	}
}

// d1SeedIdentitySurface inserts one entity_identity_surfaces row.
func d1SeedIdentitySurface(t *testing.T, conn *sqliteD1Conn, surfaceID, stableEntityID, sessionID, namespace, surfaceKind, surfaceText, normalized, scope, revision, review string, sourceTurn int) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO entity_identity_surfaces (
		surface_id, stable_entity_id, chat_session_id, identity_namespace,
		surface_kind, surface_text, normalized_surface, surface_scope,
		valid_from_turn, source_contract, source_revision, source_turn,
		review_state, idempotency_key
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'source_acceptance_observation.v1', ?, ?, ?, ?)`,
		surfaceID, stableEntityID, sessionID, namespace, surfaceKind, surfaceText,
		normalized, scope, sourceTurn, revision, sourceTurn, review, "iks-"+surfaceID); err != nil {
		t.Fatalf("seed surface %s: %v", surfaceID, err)
	}
}

// d1SeedIdentityLink inserts one entity_identity_links row. An empty createdAt
// takes the schema default.
func d1SeedIdentityLink(t *testing.T, conn *sqliteD1Conn, linkID, sessionID, sourceEntityID, targetEntityID, linkKind, linkState, createdAt string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `INSERT INTO entity_identity_links (
		link_id, chat_session_id, source_entity_id, target_entity_id,
		link_kind, link_state, evidence_json, mapping_revision
	) VALUES (?, ?, ?, ?, ?, ?, ?, 2)`, linkID, sessionID, sourceEntityID, targetEntityID,
		linkKind, linkState, `{"evidence_excerpt":"reviewed by hand"}`); err != nil {
		t.Fatalf("seed link %s: %v", linkID, err)
	}
	if createdAt == "" {
		return
	}
	if _, err := conn.Exec(context.Background(),
		`UPDATE entity_identity_links SET created_at = ? WHERE link_id = ?`, createdAt, linkID); err != nil {
		t.Fatalf("stamp link %s: %v", linkID, err)
	}
}

// d1SortedMapKeys returns a map's keys in sorted order so a seed loop inserts
// rows in a stable sequence and a failure message is reproducible.
func d1SortedMapKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// d1EntityIdentityByID returns the seeded identity row a surface belongs to, so
// the surface's namespace always matches its owner.
func d1EntityIdentityByID(identities []EntityIdentity, stableEntityID string) EntityIdentity {
	for _, item := range identities {
		if item.StableEntityID == stableEntityID {
			return item
		}
	}
	return EntityIdentity{}
}

// TestD1EntityIdentityReadCapabilitiesAreAdvertised pins the delivery state
// itself: the D1 provider must answer the exact assertions the HTTP routes
// perform, otherwise a route silently falls back to its no-identity path.
func TestD1EntityIdentityReadCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var store Store = st
	if _, ok := store.(EntityIdentityCatalogReader); !ok {
		t.Error("the D1 provider must expose EntityIdentityCatalogReader")
	}
	if _, ok := store.(ReviewedEntityIdentityResolver); !ok {
		t.Error("the D1 provider must expose ReviewedEntityIdentityResolver")
	}
	if _, ok := store.(UniqueActiveEntitySurfaceIdentityResolver); !ok {
		t.Error("the D1 provider must expose UniqueActiveEntitySurfaceIdentityResolver")
	}

	// The read-only wrapper must forward all three instead of reporting
	// ErrNotEnabled, since every caller of these capabilities is a read path.
	// A blank session is used because the provider's own guard answer is what
	// proves the call was forwarded rather than short-circuited.
	readOnly := NewReadOnlyStore(store)
	if _, err := readOnly.(EntityIdentityCatalogReader).ListActiveEntityIdentities(context.Background(), "  "); !errors.Is(err, ErrNotFound) {
		t.Errorf("read-only catalog forward error = %v, want the provider's ErrNotFound", err)
	}
	if _, err := readOnly.(EntityIdentityCatalogReader).ListActiveEntityIdentitySurfaces(context.Background(), "  "); !errors.Is(err, ErrNotFound) {
		t.Errorf("read-only surface catalog forward error = %v, want the provider's ErrNotFound", err)
	}
	if _, err := readOnly.(EntityIdentityCatalogReader).ListReviewedEntityIdentityLinks(context.Background(), "  "); !errors.Is(err, ErrNotFound) {
		t.Errorf("read-only link catalog forward error = %v, want the provider's ErrNotFound", err)
	}
	if _, err := readOnly.(ReviewedEntityIdentityResolver).ResolveReviewedCanonicalEntityID(context.Background(), "  ", "e1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("read-only resolver forward error = %v, want the provider's ErrNotFound", err)
	}
	if _, err := readOnly.(UniqueActiveEntitySurfaceIdentityResolver).ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "  ", "alex"); !errors.Is(err, ErrNotFound) {
		t.Errorf("read-only surface forward error = %v, want the provider's ErrNotFound", err)
	}
}

// TestD1ListActiveEntityIdentitiesEligibilityAndOrder pins the catalog's three
// filters (session, lifecycle, review state) plus the source-revision liveness
// join, and its first_seen_turn then stable-id ordering.
func TestD1ListActiveEntityIdentitiesEligibilityAndOrder(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	// memory_source_revisions.source_revision is a global unique key in both
	// schemas, so a revision id is session-qualified here for the same reason
	// production ids are.
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-dead", "turn-2", "superseded")
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-invalidated", "turn-3", "invalidated")
	d1SeedIdentityRevision(t, conn, "s2", "s2-rev-live", "turn-1", "active")

	d1SeedIdentity(t, conn, "e-bob", "s1", "session_npc", "character", "Bob", "active", EntityIdentityReviewStateSourceObserved, "s1-rev-live", 2, 1)
	d1SeedIdentity(t, conn, "e-alex", "s1", "session_npc", "character", "Alex", "active", EntityIdentityReviewStateReviewed, "s1-rev-live", 4, 3)
	// A tie on first_seen_turn must fall back to the stable id, deterministically.
	d1SeedIdentity(t, conn, "e-tie-2", "s1", "session_npc", "character", "Tie", "active", EntityIdentityReviewStateSourceObserved, "s1-rev-live", 6, 5)
	d1SeedIdentity(t, conn, "e-tie-1", "s1", "session_npc", "item", "Tie", "active", EntityIdentityReviewStateSourceObserved, "s1-rev-live", 6, 5)
	// Excluded: retired lifecycle, unreviewed state, dead revision, other session.
	d1SeedIdentity(t, conn, "e-retired", "s1", "session_npc", "character", "Gone", "retired", EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
	d1SeedIdentity(t, conn, "e-unreviewed", "s1", "session_npc", "character", "Draft", "active", "needs_review", "s1-rev-live", 1, 1)
	d1SeedIdentity(t, conn, "e-superseded", "s1", "session_npc", "character", "Stale", "active", EntityIdentityReviewStateSourceObserved, "s1-rev-dead", 1, 1)
	d1SeedIdentity(t, conn, "e-invalidated", "s1", "session_npc", "character", "Dead", "active", EntityIdentityReviewStateSourceObserved, "s1-rev-invalidated", 1, 1)
	d1SeedIdentity(t, conn, "e-orphan-revision", "s1", "session_npc", "character", "Orphan", "active", EntityIdentityReviewStateSourceObserved, "s1-rev-missing", 1, 1)
	// A live revision owned by ANOTHER session must not vouch for this row: the
	// join requires the session to match, not just the revision id.
	d1SeedIdentity(t, conn, "e-cross-revision", "s1", "session_npc", "character", "Borrowed", "active", EntityIdentityReviewStateSourceObserved, "s2-rev-live", 1, 1)
	d1SeedIdentity(t, conn, "e-other", "s2", "session_npc", "character", "Elsewhere", "active", EntityIdentityReviewStateSourceObserved, "s2-rev-live", 1, 1)

	// An identity carrying all three optional source ids must round-trip them
	// instead of collapsing to the empty string the COALESCE default produces.
	if _, err := conn.Exec(ctx, `INSERT INTO entity_identities (
		stable_entity_id, chat_session_id, identity_namespace, entity_kind, canonical_label,
		lifecycle_state, review_state, presence_authority, occurrence_authority,
		source_contract, source_revision, source_logical_turn_id, source_message_id,
		source_generation_id, source_content_hash, source_turn, source_index,
		idempotency_key, mapping_revision, first_seen_turn, last_seen_turn
	) VALUES ('e-sourced', 's1', 'session_npc', 'character', 'Sourced', 'active', 'reviewed',
		'unverified', 'none', 'source_acceptance_observation.v1', 's1-rev-live', 'lt-7', 'msg-7', 'gen-7',
		'hash-sourced', 7, 0, 'ik-sourced', 3, 7, 9)`); err != nil {
		t.Fatalf("seed sourced identity: %v", err)
	}

	identities, err := st.ListActiveEntityIdentities(ctx, "s1")
	if err != nil {
		t.Fatalf("ListActiveEntityIdentities: %v", err)
	}

	got := make([]string, 0, len(identities))
	byID := map[string]EntityIdentity{}
	for _, item := range identities {
		got = append(got, item.StableEntityID)
		byID[item.StableEntityID] = item
	}
	want := []string{"e-bob", "e-alex", "e-tie-1", "e-tie-2", "e-sourced"}
	if len(got) != len(want) {
		t.Fatalf("catalog = %v, want exactly %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("catalog = %v, want %v (first_seen_turn asc, stable_entity_id asc)", got, want)
		}
	}

	bob := byID["e-bob"]
	if bob.ChatSessionID != "s1" || bob.IdentityNamespace != "session_npc" || bob.EntityKind != "character" ||
		bob.CanonicalLabel != "Bob" || bob.LifecycleState != "active" || bob.ReviewState != EntityIdentityReviewStateSourceObserved {
		t.Errorf("identity projection = %+v", bob)
	}
	if bob.PresenceAuthority != "source_presence" || bob.OccurrenceAuthority != "source_occurrence" {
		t.Errorf("authority columns = %q/%q, want the seeded values", bob.PresenceAuthority, bob.OccurrenceAuthority)
	}
	if bob.SourceContract != "source_acceptance_observation.v1" || bob.SourceRevision != "s1-rev-live" {
		t.Errorf("source contract/revision = %q/%q", bob.SourceContract, bob.SourceRevision)
	}
	if bob.SourceContentHash != "hash-e-bob" || bob.SourceTurn != 2 || bob.SourceIndex != 0 || bob.IdempotencyKey != "ik-e-bob" {
		t.Errorf("source occurrence projection = %+v", bob)
	}
	if bob.MappingRevision != 2 || bob.FirstSeenTurn != 1 || bob.LastSeenTurn != 1 {
		t.Errorf("turn and revision projection = %+v", bob)
	}
	if bob.CreatedAt.IsZero() || bob.UpdatedAt.IsZero() {
		t.Errorf("timestamps must come back parsed, got %v/%v", bob.CreatedAt, bob.UpdatedAt)
	}
	// The three optional source ids were left NULL, so the COALESCE default
	// must be the empty string rather than a leftover NULL marker.
	if bob.SourceLogicalTurnID != "" || bob.SourceMessageID != "" || bob.SourceGenerationID != "" {
		t.Errorf("NULL source ids must read as empty strings, got %q/%q/%q",
			bob.SourceLogicalTurnID, bob.SourceMessageID, bob.SourceGenerationID)
	}

	sourced := byID["e-sourced"]
	if sourced.SourceLogicalTurnID != "lt-7" || sourced.SourceMessageID != "msg-7" || sourced.SourceGenerationID != "gen-7" {
		t.Errorf("present source ids must round-trip, got %q/%q/%q",
			sourced.SourceLogicalTurnID, sourced.SourceMessageID, sourced.SourceGenerationID)
	}
	if sourced.MappingRevision != 3 || sourced.FirstSeenTurn != 7 || sourced.LastSeenTurn != 9 {
		t.Errorf("sourced identity turns/revision = %+v", sourced)
	}

	// A session with no identities is an empty catalog, not a failure, and the
	// slice must be non-nil because the review routes range over it.
	empty, err := st.ListActiveEntityIdentities(ctx, "s2-empty")
	if err != nil {
		t.Fatalf("empty session read: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Errorf("empty session catalog = %#v, want an empty non-nil slice", empty)
	}

	// Another session's identities never leak into this catalog.
	if _, present := byID["e-other"]; present {
		t.Error("an identity from another session was returned")
	}
}

// TestD1ListActiveEntityIdentitiesRejectsBlankSession pins the fail-closed
// guard: a blank session must not degrade into "every session".
func TestD1ListActiveEntityIdentitiesRejectsBlankSession(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
	d1SeedIdentity(t, conn, "e-bob", "s1", "session_npc", "character", "Bob", "active",
		EntityIdentityReviewStateSourceObserved, "s1-rev-live", 2, 1)

	for _, blank := range []string{"", "   ", "\t"} {
		if _, err := st.ListActiveEntityIdentities(ctx, blank); !errors.Is(err, ErrNotFound) {
			t.Errorf("ListActiveEntityIdentities(%q) error = %v, want ErrNotFound", blank, err)
		}
	}
}

// TestD1ListActiveEntityIdentitySurfacesEligibilityAndOrder pins the surface
// catalog: an active owning identity, a live source revision, and the
// source-observed review state, ordered by source turn then surface id.
func TestD1ListActiveEntityIdentitySurfacesEligibilityAndOrder(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-dead", "turn-2", "superseded")
	d1SeedIdentityRevision(t, conn, "s2", "s2-rev-live", "turn-1", "active")

	d1SeedIdentity(t, conn, "e-alex", "s1", "session_npc", "character", "Alex", "active", EntityIdentityReviewStateSourceObserved, "s1-rev-live", 3, 3)
	d1SeedIdentity(t, conn, "e-retired", "s1", "session_npc", "character", "Old", "retired", EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
	d1SeedIdentity(t, conn, "e-other", "s2", "session_npc", "character", "Elsewhere", "active", EntityIdentityReviewStateSourceObserved, "s2-rev-live", 1, 1)

	// Ordered by source_turn ASC: the two turn-1 rows come first, tie-broken by
	// surface id, then the turn-5 row.
	d1SeedIdentitySurface(t, conn, "sf-2", "e-alex", "s1", "session_npc", "alias_0", "Jiyu", "jiyu", EntityIdentitySurfaceScope39, "s1-rev-live", EntityIdentityReviewStateSourceObserved, 1)
	d1SeedIdentitySurface(t, conn, "sf-1", "e-alex", "s1", "session_npc", "display_name", "Alex", "alex", EntityIdentitySurfaceScopeCurrent, "s1-rev-live", EntityIdentityReviewStateSourceObserved, 1)
	d1SeedIdentitySurface(t, conn, "sf-3", "e-alex", "s1", "session_npc", "display_name", "Alexander", "alexander", EntityIdentitySurfaceScope39, "s1-rev-live", EntityIdentityReviewStateSourceObserved, 5)
	// Excluded: retired identity, dead revision, already-reviewed surface, other
	// session.
	d1SeedIdentitySurface(t, conn, "sf-retired", "e-retired", "s1", "session_npc", "display_name", "Old", "old", EntityIdentitySurfaceScope39, "s1-rev-live", EntityIdentityReviewStateSourceObserved, 1)
	d1SeedIdentitySurface(t, conn, "sf-dead", "e-alex", "s1", "session_npc", "display_name", "Stale", "stale", EntityIdentitySurfaceScope39, "s1-rev-dead", EntityIdentityReviewStateSourceObserved, 1)
	d1SeedIdentitySurface(t, conn, "sf-cross-revision", "e-alex", "s1", "session_npc", "display_name", "Borrowed", "borrowed", EntityIdentitySurfaceScope39, "s2-rev-live", EntityIdentityReviewStateSourceObserved, 1)
	d1SeedIdentitySurface(t, conn, "sf-reviewed", "e-alex", "s1", "session_npc", "display_name", "Judged", "judged", EntityIdentitySurfaceScope39, "s1-rev-live", EntityIdentityReviewStateReviewed, 1)
	d1SeedIdentitySurface(t, conn, "sf-other", "e-other", "s2", "session_npc", "display_name", "Elsewhere", "elsewhere", EntityIdentitySurfaceScope39, "s2-rev-live", EntityIdentityReviewStateSourceObserved, 1)

	// A surface carrying every optional column, to pin the COALESCE defaults on
	// the rows that leave them NULL.
	if _, err := conn.Exec(ctx, `INSERT INTO entity_identity_surfaces (
		surface_id, stable_entity_id, chat_session_id, identity_namespace,
		surface_kind, surface_text, normalized_surface, surface_scope,
		valid_from_turn, source_contract, source_revision, source_turn,
		review_state, idempotency_key
	) VALUES ('sf-bare', 'e-alex', 's1', 'session_npc', 'display_name', 'Bare', 'bare',
		'source_turn', 6, 'source_acceptance_observation.v1', 's1-rev-live', 6, 'source_observed', 'iks-bare')`); err != nil {
		t.Fatalf("seed bare surface: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE entity_identity_surfaces
		SET valid_to_turn = 9, source_span_start = 4, source_span_end = 11, evidence_excerpt = 'Bare.'
		WHERE surface_id = 'sf-bare'`); err != nil {
		t.Fatalf("fill optional surface columns: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE entity_identity_surfaces
		SET valid_to_turn = NULL, source_span_start = NULL, source_span_end = NULL, evidence_excerpt = NULL
		WHERE surface_id = 'sf-1'`); err != nil {
		t.Fatalf("clear optional surface columns: %v", err)
	}

	surfaces, err := st.ListActiveEntityIdentitySurfaces(ctx, "s1")
	if err != nil {
		t.Fatalf("ListActiveEntityIdentitySurfaces: %v", err)
	}
	got := make([]string, 0, len(surfaces))
	byID := map[string]EntityIdentitySurface{}
	for _, item := range surfaces {
		got = append(got, item.SurfaceID)
		byID[item.SurfaceID] = item
	}
	want := []string{"sf-1", "sf-2", "sf-3", "sf-bare"}
	if len(got) != len(want) {
		t.Fatalf("surfaces = %v, want exactly %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("surfaces = %v, want %v (source_turn asc, surface_id asc)", got, want)
		}
	}

	first := byID["sf-1"]
	if first.StableEntityID != "e-alex" || first.ChatSessionID != "s1" || first.IdentityNamespace != "session_npc" {
		t.Errorf("surface owner projection = %+v", first)
	}
	if first.SurfaceKind != "display_name" || first.SurfaceText != "Alex" || first.NormalizedSurface != "alex" {
		t.Errorf("surface text projection = %+v", first)
	}
	if first.Scope != EntityIdentitySurfaceScopeCurrent || first.ValidFromTurn != 1 || first.SourceTurn != 1 {
		t.Errorf("surface scope/turn projection = %+v", first)
	}
	if first.SourceContract != "source_acceptance_observation.v1" || first.SourceRevision != "s1-rev-live" {
		t.Errorf("surface source projection = %+v", first)
	}
	if first.ReviewState != EntityIdentityReviewStateSourceObserved || first.IdempotencyKey != "iks-sf-1" {
		t.Errorf("surface review projection = %+v", first)
	}
	// NULL optional columns must read as the documented defaults, never as a
	// sentinel the caller has to special-case.
	if first.ValidToTurn != 0 || first.SourceSpanStart != -1 || first.SourceSpanEnd != -1 || first.EvidenceExcerpt != "" {
		t.Errorf("NULL optional surface columns = %d/%d/%d/%q, want 0/-1/-1/empty",
			first.ValidToTurn, first.SourceSpanStart, first.SourceSpanEnd, first.EvidenceExcerpt)
	}

	bare := byID["sf-bare"]
	if bare.ValidToTurn != 9 || bare.SourceSpanStart != 4 || bare.SourceSpanEnd != 11 || bare.EvidenceExcerpt != "Bare." {
		t.Errorf("present optional surface columns = %+v", bare)
	}

	empty, err := st.ListActiveEntityIdentitySurfaces(ctx, "s2-empty")
	if err != nil {
		t.Fatalf("empty session surface read: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Errorf("empty session surfaces = %#v, want an empty non-nil slice", empty)
	}
	for _, blank := range []string{"", "  "} {
		if _, err := st.ListActiveEntityIdentitySurfaces(ctx, blank); !errors.Is(err, ErrNotFound) {
			t.Errorf("ListActiveEntityIdentitySurfaces(%q) error = %v, want ErrNotFound", blank, err)
		}
	}
}

// TestD1ListReviewedEntityIdentityLinksFiltersAndOrder pins the link catalog:
// reviewed canonical-equivalence links only, oldest first, tie-broken by link
// id.
func TestD1ListReviewedEntityIdentityLinksFiltersAndOrder(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
	d1SeedIdentityRevision(t, conn, "s2", "s2-rev-live", "turn-1", "active")

	// entity_identities.stable_entity_id is a global primary key in both
	// schemas, so each session's identities carry distinct ids.
	for _, id := range []string{"e-a", "e-b", "e-c", "e-d", "e-e", "e-f", "e-g", "e-h"} {
		d1SeedIdentity(t, conn, "s1-"+id, "s1", "session_npc", "character", "X", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
	}
	d1SeedIdentity(t, conn, "s2-a", "s2", "session_npc", "character", "X", "active",
		EntityIdentityReviewStateSourceObserved, "s2-rev-live", 1, 1)
	d1SeedIdentity(t, conn, "s2-b", "s2", "session_npc", "character", "X", "active",
		EntityIdentityReviewStateSourceObserved, "s2-rev-live", 1, 1)

	d1SeedIdentityLink(t, conn, "ln-2", "s1", "s1-e-a", "s1-e-b", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "2026-01-02T00:00:00Z")
	d1SeedIdentityLink(t, conn, "ln-1", "s1", "s1-e-c", "s1-e-d", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "2026-01-01T00:00:00Z")
	// Same created_at as ln-1: the link id must break the tie.
	d1SeedIdentityLink(t, conn, "ln-0", "s1", "s1-e-e", "s1-e-f", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "2026-01-01T00:00:00Z")
	// Excluded: revoked state, a non-equivalence kind, and another session.
	d1SeedIdentityLink(t, conn, "ln-revoked", "s1", "s1-e-g", "s1-e-h", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateRevoked, "2025-12-31T00:00:00Z")
	d1SeedIdentityLink(t, conn, "ln-other-kind", "s1", "s1-e-a", "s1-e-c", "name_variant", EntityIdentityLinkStateReviewed, "2025-12-31T00:00:00Z")
	d1SeedIdentityLink(t, conn, "ln-other-session", "s2", "s2-a", "s2-b", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "2025-12-31T00:00:00Z")

	links, err := st.ListReviewedEntityIdentityLinks(ctx, "s1")
	if err != nil {
		t.Fatalf("ListReviewedEntityIdentityLinks: %v", err)
	}
	got := make([]string, 0, len(links))
	byID := map[string]EntityIdentityLink{}
	for _, item := range links {
		got = append(got, item.LinkID)
		byID[item.LinkID] = item
	}
	want := []string{"ln-0", "ln-1", "ln-2"}
	if len(got) != len(want) {
		t.Fatalf("links = %v, want exactly %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("links = %v, want %v (created_at asc, link_id asc)", got, want)
		}
	}

	first := byID["ln-0"]
	if first.ChatSessionID != "s1" || first.SourceEntityID != "s1-e-e" || first.TargetEntityID != "s1-e-f" {
		t.Errorf("link projection = %+v", first)
	}
	if first.LinkKind != EntityIdentityLinkKindCanonicalEquivalence || first.LinkState != EntityIdentityLinkStateReviewed {
		t.Errorf("link kind/state = %q/%q", first.LinkKind, first.LinkState)
	}
	// evidence_json is TEXT in D1 and JSON in MariaDB; the read projects it
	// verbatim, so the stored document must come back byte-identical.
	if first.EvidenceJSON != `{"evidence_excerpt":"reviewed by hand"}` {
		t.Errorf("evidence_json = %q, want the stored document verbatim", first.EvidenceJSON)
	}
	if first.MappingRevision != 2 {
		t.Errorf("mapping_revision = %d, want 2", first.MappingRevision)
	}
	if first.CreatedAt.IsZero() || first.UpdatedAt.IsZero() {
		t.Errorf("link timestamps must be parsed, got %v/%v", first.CreatedAt, first.UpdatedAt)
	}
	if byID["ln-1"].CreatedAt.After(byID["ln-0"].CreatedAt) {
		t.Errorf("created_at ordering did not survive: ln-0=%v ln-1=%v",
			byID["ln-0"].CreatedAt, byID["ln-1"].CreatedAt)
	}

	empty, err := st.ListReviewedEntityIdentityLinks(ctx, "s2-empty")
	if err != nil {
		t.Fatalf("empty session link read: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Errorf("empty session links = %#v, want an empty non-nil slice", empty)
	}
	for _, blank := range []string{"", " "} {
		if _, err := st.ListReviewedEntityIdentityLinks(ctx, blank); !errors.Is(err, ErrNotFound) {
			t.Errorf("ListReviewedEntityIdentityLinks(%q) error = %v, want ErrNotFound", blank, err)
		}
	}
}

// TestD1ResolveReviewedCanonicalEntityID covers the whole reviewed-link
// contract, including the two fail-closed cases that keep a wrong answer from
// reaching a caller.
func TestD1ResolveReviewedCanonicalEntityID(t *testing.T) {
	// seedLinkGraph builds a session whose reviewed equivalence graph is given by
	// edges, with every node active, reviewed, and backed by a live revision.
	seedLinkGraph := func(t *testing.T, edges map[string][]string) (*d1Store, *sqliteD1Conn) {
		t.Helper()
		st, conn := newD1TestStore(t)
		d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
		d1SeedIdentityRevision(t, conn, "s1", "s1-rev-dead", "turn-2", "superseded")
		nodes := map[string]bool{}
		for source, targets := range edges {
			nodes[source] = true
			for _, target := range targets {
				nodes[target] = true
			}
		}
		for _, node := range d1SortedMapKeys(nodes) {
			d1SeedIdentity(t, conn, node, "s1", "session_npc", "character", "N-"+node, "active",
				EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		}
		for _, source := range d1SortedMapKeys(edges) {
			for i, target := range edges[source] {
				d1SeedIdentityLink(t, conn, "ln-"+source+"-"+string(rune('a'+i)), "s1", source, target,
					EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")
			}
		}
		return st, conn
	}

	t.Run("follows a multi-hop chain to the root", func(t *testing.T) {
		st, _ := seedLinkGraph(t, map[string][]string{
			"abel":      {"abelstein"},
			"abelstein": {"canonical-abel"},
		})
		got, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "abel")
		if err != nil || got != "canonical-abel" {
			t.Fatalf("resolved = %q, err = %v, want canonical-abel", got, err)
		}
	})

	t.Run("a single hop returns the target", func(t *testing.T) {
		st, _ := seedLinkGraph(t, map[string][]string{"abel": {"abelstein"}})
		got, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "abel")
		if err != nil || got != "abelstein" {
			t.Fatalf("resolved = %q, err = %v, want abelstein", got, err)
		}
	})

	t.Run("a root occurrence is not found", func(t *testing.T) {
		st, _ := seedLinkGraph(t, map[string][]string{"abel": {"abelstein"}})
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "abelstein"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("root resolution error = %v, want ErrNotFound", err)
		}
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "absent"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("absent resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("a revoked link is not an identity", func(t *testing.T) {
		st, conn := seedLinkGraph(t, map[string][]string{})
		d1SeedIdentity(t, conn, "e-a", "s1", "session_npc", "character", "A", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentity(t, conn, "e-b", "s1", "session_npc", "character", "B", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentityLink(t, conn, "ln-revoked", "s1", "e-a", "e-b",
			EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateRevoked, "")
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "e-a"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("revoked-only resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("a non-equivalence link kind is not an identity", func(t *testing.T) {
		st, conn := seedLinkGraph(t, map[string][]string{})
		d1SeedIdentity(t, conn, "e-a", "s1", "session_npc", "character", "A", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentity(t, conn, "e-b", "s1", "session_npc", "character", "B", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentityLink(t, conn, "ln-kind", "s1", "e-a", "e-b", "name_variant",
			EntityIdentityLinkStateReviewed, "")
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "e-a"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("wrong-kind resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("a retired target is not a canonical target", func(t *testing.T) {
		st, conn := seedLinkGraph(t, map[string][]string{})
		d1SeedIdentity(t, conn, "e-a", "s1", "session_npc", "character", "A", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentity(t, conn, "e-retired", "s1", "session_npc", "character", "R", "retired",
			EntityIdentityReviewStateReviewed, "s1-rev-live", 1, 1)
		d1SeedIdentityLink(t, conn, "ln-retired", "s1", "e-a", "e-retired",
			EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "e-a"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("retired-target resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("an unreviewed target is not a canonical target", func(t *testing.T) {
		st, conn := seedLinkGraph(t, map[string][]string{})
		d1SeedIdentity(t, conn, "e-a", "s1", "session_npc", "character", "A", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentity(t, conn, "e-draft", "s1", "session_npc", "character", "D", "active",
			"needs_review", "s1-rev-live", 1, 1)
		d1SeedIdentityLink(t, conn, "ln-draft", "s1", "e-a", "e-draft",
			EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "e-a"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unreviewed-target resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("a target whose revision is superseded is not a canonical target", func(t *testing.T) {
		st, conn := seedLinkGraph(t, map[string][]string{})
		d1SeedIdentity(t, conn, "e-a", "s1", "session_npc", "character", "A", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentity(t, conn, "e-stale", "s1", "session_npc", "character", "S", "active",
			EntityIdentityReviewStateReviewed, "s1-rev-dead", 1, 1)
		d1SeedIdentityLink(t, conn, "ln-stale", "s1", "e-a", "e-stale",
			EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "e-a"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("stale-target resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("two reviewed targets are ambiguous", func(t *testing.T) {
		st, _ := seedLinkGraph(t, map[string][]string{"abel": {"abelstein", "abelstone"}})
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "abel"); !errors.Is(err, ErrReviewedEntityIdentityAmbiguous) {
			t.Fatalf("ambiguous resolution error = %v, want ErrReviewedEntityIdentityAmbiguous", err)
		}
	})

	t.Run("a cycle is reported instead of looping", func(t *testing.T) {
		st, _ := seedLinkGraph(t, map[string][]string{
			"abel":      {"abelstein"},
			"abelstein": {"abel"},
		})
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "abel"); !errors.Is(err, ErrReviewedEntityIdentityCycle) {
			t.Fatalf("cycle resolution error = %v, want ErrReviewedEntityIdentityCycle", err)
		}
	})

	t.Run("a self loop is excluded by the query", func(t *testing.T) {
		st, conn := seedLinkGraph(t, map[string][]string{})
		d1SeedIdentity(t, conn, "e-a", "s1", "session_npc", "character", "A", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentityLink(t, conn, "ln-self", "s1", "e-a", "e-a",
			EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "e-a"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("self-loop resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("a link from another session is not followed", func(t *testing.T) {
		st, conn := seedLinkGraph(t, map[string][]string{})
		d1SeedIdentityRevision(t, conn, "s2", "s2-rev-live", "turn-1", "active")
		d1SeedIdentity(t, conn, "e-a", "s1", "session_npc", "character", "A", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentity(t, conn, "e-b", "s2", "session_npc", "character", "B", "active",
			EntityIdentityReviewStateSourceObserved, "s2-rev-live", 1, 1)
		d1SeedIdentityLink(t, conn, "ln-cross", "s2", "e-a", "e-b",
			EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")
		if _, err := st.ResolveReviewedCanonicalEntityID(context.Background(), "s1", "e-a"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross-session resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("blank arguments fail closed", func(t *testing.T) {
		st, _ := seedLinkGraph(t, map[string][]string{"abel": {"abelstein"}})
		ctx := context.Background()
		for _, tc := range []struct{ session, source string }{
			{"", "abel"}, {"  ", "abel"}, {"s1", ""}, {"s1", "  "},
		} {
			if _, err := st.ResolveReviewedCanonicalEntityID(ctx, tc.session, tc.source); !errors.Is(err, ErrNotFound) {
				t.Errorf("(%q, %q) error = %v, want ErrNotFound", tc.session, tc.source, err)
			}
		}
	})
}

// TestD1ResolveUniqueActiveEntityIdentityBySurface pins the collapse rules that
// stop a display name from acting as a merge key.
func TestD1ResolveUniqueActiveEntityIdentityBySurface(t *testing.T) {
	// seedSurfaceGraph builds a session with one live revision and the given
	// identities, then attaches one surface per surfaceSpec.
	type surfaceSpec struct {
		SurfaceID  string
		EntityID   string
		Kind       string
		Text       string
		Normalized string
		Scope      string
		Revision   string
		Review     string
		Turn       int
	}
	seedSurfaceGraph := func(t *testing.T, identities []EntityIdentity, surfaces []surfaceSpec) (*d1Store, *sqliteD1Conn) {
		t.Helper()
		st, conn := newD1TestStore(t)
		d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
		d1SeedIdentityRevision(t, conn, "s1", "s1-rev-dead", "turn-2", "superseded")
		for _, item := range identities {
			d1SeedIdentity(t, conn, item.StableEntityID, "s1", item.IdentityNamespace, item.EntityKind,
				item.CanonicalLabel, item.LifecycleState, item.ReviewState, item.SourceRevision,
				item.SourceTurn, item.SourceTurn)
		}
		for _, spec := range surfaces {
			owner := d1EntityIdentityByID(identities, spec.EntityID)
			d1SeedIdentitySurface(t, conn, spec.SurfaceID, spec.EntityID, "s1", owner.IdentityNamespace,
				spec.Kind, spec.Text, spec.Normalized, spec.Scope, spec.Revision, spec.Review, spec.Turn)
		}
		return st, conn
	}

	active := func(id, namespace, kind, label string, turn int) EntityIdentity {
		return EntityIdentity{
			StableEntityID: id, IdentityNamespace: namespace, EntityKind: kind,
			CanonicalLabel: label, LifecycleState: "active",
			ReviewState:    EntityIdentityReviewStateSourceObserved,
			SourceRevision: "s1-rev-live", SourceTurn: turn,
		}
	}

	t.Run("two occurrences collapse onto the reviewed canonical target", func(t *testing.T) {
		st, conn := seedSurfaceGraph(t,
			[]EntityIdentity{
				active("e-occ-1", "session_unknown", "speaker", "Alex", 2),
				active("e-occ-2", "session_unknown", "speaker", "Alex", 3),
				active("e-canon", "session_npc", "character", "Alexander", 1),
			},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-occ-1", Kind: "alias_0", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 2},
				{SurfaceID: "sf-2", EntityID: "e-occ-2", Kind: "alias_0", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 3},
			})
		d1SeedIdentityLink(t, conn, "ln-1", "s1", "e-occ-1", "e-canon", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")
		d1SeedIdentityLink(t, conn, "ln-2", "s1", "e-occ-2", "e-canon", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")

		got, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex")
		want := ResolvedEntityIdentity{StableEntityID: "e-canon", IdentityNamespace: "session_npc", EntityKind: "character", CanonicalLabel: "Alexander"}
		if err != nil || got != want {
			t.Fatalf("resolved = %#v, err = %v, want %#v", got, err, want)
		}
	})

	t.Run("a chain resolves to the root's namespace, not the link target's", func(t *testing.T) {
		st, conn := seedSurfaceGraph(t,
			[]EntityIdentity{
				active("e-occ", "session_unknown", "speaker", "Alex", 4),
				active("e-mid", "session_npc", "character", "Alexander", 2),
				active("e-root", "session_world", "character", "Alexandra", 1),
			},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-occ", Kind: "alias_0", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 4},
			})
		d1SeedIdentityLink(t, conn, "ln-1", "s1", "e-occ", "e-mid", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")
		d1SeedIdentityLink(t, conn, "ln-2", "s1", "e-mid", "e-root", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")

		got, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex")
		want := ResolvedEntityIdentity{StableEntityID: "e-root", IdentityNamespace: "session_world", EntityKind: "character", CanonicalLabel: "Alexandra"}
		if err != nil || got != want {
			t.Fatalf("resolved = %#v, err = %v, want the chain root %#v", got, err, want)
		}
	})

	t.Run("a link to an unreviewed target drops the row", func(t *testing.T) {
		draft := active("e-draft", "session_npc", "character", "Alex", 1)
		draft.ReviewState = "needs_review"
		st, conn := seedSurfaceGraph(t,
			[]EntityIdentity{active("e-occ", "session_unknown", "speaker", "Alex", 2), draft},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-occ", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 2},
			})
		d1SeedIdentityLink(t, conn, "ln-1", "s1", "e-occ", "e-draft", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")

		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unreviewed-target resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("a link to a target whose revision is superseded drops the row", func(t *testing.T) {
		stale := active("e-stale", "session_npc", "character", "Alex", 1)
		stale.SourceRevision = "s1-rev-dead"
		st, conn := seedSurfaceGraph(t,
			[]EntityIdentity{active("e-occ", "session_unknown", "speaker", "Alex", 2), stale},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-occ", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 2},
			})
		d1SeedIdentityLink(t, conn, "ln-1", "s1", "e-occ", "e-stale", EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed, "")

		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("stale-target resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("duplicate exact canonical tuples keep the earliest observation", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{
				active("e-later", "session_npc", "character", "Alex", 4),
				active("e-earlier", "session_npc", "character", "Alex", 1),
			},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-later", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 4},
				{SurfaceID: "sf-2", EntityID: "e-earlier", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
			})
		got, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex")
		want := ResolvedEntityIdentity{StableEntityID: "e-earlier", IdentityNamespace: "session_npc", EntityKind: "character", CanonicalLabel: "Alex"}
		if err != nil || got != want {
			t.Fatalf("resolved = %#v, err = %v, want the earliest observation %#v", got, err, want)
		}
	})

	t.Run("the same label in two namespaces is ambiguous", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{
				active("e-npc", "session_npc", "character", "Alex", 1),
				active("e-player", "session_player", "character", "Alex", 2),
			},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-npc", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
				{SurfaceID: "sf-2", EntityID: "e-player", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 2},
			})
		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrReviewedEntityIdentityAmbiguous) {
			t.Fatalf("cross-namespace resolution error = %v, want ErrReviewedEntityIdentityAmbiguous", err)
		}
	})

	t.Run("different entity kinds never collapse", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{
				active("e-char", "session_npc", "character", "Alex", 1),
				active("e-item", "session_npc", "item", "Alex", 2),
			},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-char", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
				{SurfaceID: "sf-2", EntityID: "e-item", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 2},
			})
		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrReviewedEntityIdentityAmbiguous) {
			t.Fatalf("cross-kind resolution error = %v, want ErrReviewedEntityIdentityAmbiguous", err)
		}
	})

	t.Run("an alias surface never collapses two occurrences", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{
				active("e-1", "session_npc", "character", "Alexander", 1),
				active("e-2", "session_npc", "character", "Alexander", 2),
			},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "alias_0", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
				{SurfaceID: "sf-2", EntityID: "e-2", Kind: "alias_0", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 2},
			})
		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrReviewedEntityIdentityAmbiguous) {
			t.Fatalf("alias resolution error = %v, want ErrReviewedEntityIdentityAmbiguous", err)
		}
	})

	t.Run("one display name beside an alias is ambiguous", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{
				active("e-1", "session_npc", "character", "Alex", 1),
				active("e-2", "session_npc", "character", "Alex", 2),
			},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
				{SurfaceID: "sf-2", EntityID: "e-2", Kind: "alias_0", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 2},
			})
		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrReviewedEntityIdentityAmbiguous) {
			t.Fatalf("mixed-surface resolution error = %v, want ErrReviewedEntityIdentityAmbiguous", err)
		}
	})

	t.Run("both in-scope surface kinds resolve to one identity", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{active("e-1", "session_npc", "character", "Alex", 1)},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "display_name", Text: "Alex", Normalized: "scoped", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
				{SurfaceID: "sf-2", EntityID: "e-1", Kind: "alias_0", Text: "Alec", Normalized: "scoped", Scope: EntityIdentitySurfaceScopeCurrent, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 2},
			})
		got, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "scoped")
		want := ResolvedEntityIdentity{StableEntityID: "e-1", IdentityNamespace: "session_npc", EntityKind: "character", CanonicalLabel: "Alex"}
		if err != nil || got != want {
			t.Fatalf("resolved = %#v, err = %v, want %#v", got, err, want)
		}
	})

	t.Run("an out-of-scope surface is not a candidate", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{active("e-1", "session_npc", "character", "Alex", 1)},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: "session", Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
			})
		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("out-of-scope resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("an already-reviewed surface is not a candidate", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{active("e-1", "session_npc", "character", "Alex", 1)},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateReviewed, Turn: 1},
			})
		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("reviewed-surface resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("a surface of a retired identity is not a candidate", func(t *testing.T) {
		retired := active("e-1", "session_npc", "character", "Alex", 1)
		retired.LifecycleState = "retired"
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{retired},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
			})
		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("retired-identity resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("an unreviewed owning identity is not a candidate", func(t *testing.T) {
		draft := active("e-1", "session_npc", "character", "Alex", 1)
		draft.ReviewState = "needs_review"
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{draft},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
			})
		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unreviewed-identity resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("a surface whose revision is superseded is not a candidate", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{active("e-1", "session_npc", "character", "Alex", 1)},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-dead", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
			})
		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(context.Background(), "s1", "alex"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("stale-surface resolution error = %v, want ErrNotFound", err)
		}
	})

	t.Run("another session's surface is not a candidate", func(t *testing.T) {
		st, conn := newD1TestStore(t)
		ctx := context.Background()
		d1SeedIdentityRevision(t, conn, "s1", "s1-rev-live", "turn-1", "active")
		d1SeedIdentityRevision(t, conn, "s2", "s2-rev-live", "turn-1", "active")
		d1SeedIdentity(t, conn, "e-1", "s1", "session_npc", "character", "Alex", "active",
			EntityIdentityReviewStateSourceObserved, "s1-rev-live", 1, 1)
		d1SeedIdentity(t, conn, "e-2", "s2", "session_npc", "character", "Alex", "active",
			EntityIdentityReviewStateSourceObserved, "s2-rev-live", 1, 1)
		d1SeedIdentitySurface(t, conn, "sf-1", "e-1", "s1", "session_npc", "display_name", "Alex", "alex",
			EntityIdentitySurfaceScope39, "s1-rev-live", EntityIdentityReviewStateSourceObserved, 1)
		d1SeedIdentitySurface(t, conn, "sf-2", "e-2", "s2", "session_npc", "display_name", "Alex", "alex",
			EntityIdentitySurfaceScope39, "s2-rev-live", EntityIdentityReviewStateSourceObserved, 1)

		if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(ctx, "s1", "alex"); err != nil {
			t.Fatalf("s1 resolution: %v", err)
		}
		got, err := st.ResolveUniqueActiveEntityIdentityBySurface(ctx, "s2", "alex")
		want := ResolvedEntityIdentity{StableEntityID: "e-2", IdentityNamespace: "session_npc", EntityKind: "character", CanonicalLabel: "Alex"}
		if err != nil || got != want {
			t.Fatalf("s2 resolved = %#v, err = %v, want %#v", got, err, want)
		}
	})

	t.Run("the surface match is an exact normalised comparison", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{active("e-1", "session_npc", "character", "Alex", 1)},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
			})
		ctx := context.Background()
		want := ResolvedEntityIdentity{StableEntityID: "e-1", IdentityNamespace: "session_npc", EntityKind: "character", CanonicalLabel: "Alex"}

		// Surrounding whitespace is trimmed on both providers, so a padded
		// argument still resolves. This is the caller's normalised-form
		// contract, not a case-insensitive match.
		for _, padded := range []string{"alex", " alex", "alex ", "  alex  "} {
			got, err := st.ResolveUniqueActiveEntityIdentityBySurface(ctx, "s1", padded)
			if err != nil || got != want {
				t.Errorf("resolve(%q) = %#v, err = %v, want %#v", padded, got, err, want)
			}
		}
		// A different normalised spelling is a different surface, not a prefix
		// match and not a fuzzy one.
		for _, other := range []string{"alexander", "al", "a lex", "Alex"} {
			if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(ctx, "s1", other); !errors.Is(err, ErrNotFound) {
				t.Errorf("resolve(%q) error = %v, want ErrNotFound", other, err)
			}
		}
	})

	t.Run("blank arguments fail closed", func(t *testing.T) {
		st, _ := seedSurfaceGraph(t,
			[]EntityIdentity{active("e-1", "session_npc", "character", "Alex", 1)},
			[]surfaceSpec{
				{SurfaceID: "sf-1", EntityID: "e-1", Kind: "display_name", Text: "Alex", Normalized: "alex", Scope: EntityIdentitySurfaceScope39, Revision: "s1-rev-live", Review: EntityIdentityReviewStateSourceObserved, Turn: 1},
			})
		ctx := context.Background()
		for _, tc := range []struct{ session, surface string }{
			{"", "alex"}, {"  ", "alex"}, {"s1", ""}, {"s1", "   "},
		} {
			if _, err := st.ResolveUniqueActiveEntityIdentityBySurface(ctx, tc.session, tc.surface); !errors.Is(err, ErrNotFound) {
				t.Errorf("(%q, %q) error = %v, want ErrNotFound", tc.session, tc.surface, err)
			}
		}
	})
}
