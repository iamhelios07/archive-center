package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func d1IdentityWriteSeed(id string) *EntityIdentity {
	return &EntityIdentity{StableEntityID: id, ChatSessionID: "identity-write-s", IdentityNamespace: "session_npc", EntityKind: "character", CanonicalLabel: id, LifecycleState: "active", ReviewState: EntityIdentityReviewStateSourceObserved, PresenceAuthority: "observed", OccurrenceAuthority: "observed", SourceContract: acceptedSourceObservationContract, SourceRevision: "identity-write-rev", SourceContentHash: "hash", SourceTurn: 1, SourceIndex: 0, IdempotencyKey: "ik-" + id, MappingRevision: 1, FirstSeenTurn: 1, LastSeenTurn: 1, CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}
}

func TestD1EntityIdentityWritesPersistAllProjections(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedIdentityRevision(t, conn, "identity-write-s", "identity-write-rev", "turn-1", "active")
	one := d1IdentityWriteSeed("identity-write-one")
	if err := st.SaveEntityIdentity(ctx, one); err != nil {
		t.Fatalf("save identity: %v", err)
	}
	one.LastSeenTurn = 4
	one.UpdatedAt = one.UpdatedAt.Add(time.Hour)
	if err := st.SaveEntityIdentity(ctx, one); err != nil {
		t.Fatalf("upsert identity: %v", err)
	}
	var last int
	if err := conn.QueryRow(ctx, `SELECT last_seen_turn FROM entity_identities WHERE stable_entity_id = ?`, one.StableEntityID).Scan(&last); err != nil || last != 4 {
		t.Fatalf("identity last seen = %d, %v; want 4", last, err)
	}

	surface := &EntityIdentitySurface{SurfaceID: "identity-write-surface", StableEntityID: one.StableEntityID, ChatSessionID: one.ChatSessionID, IdentityNamespace: one.IdentityNamespace, SurfaceKind: "display_name", SurfaceText: "One", NormalizedSurface: "one", Scope: EntityIdentitySurfaceScope39, ValidFromTurn: 1, ValidToTurn: 0, SourceContract: one.SourceContract, SourceRevision: one.SourceRevision, SourceTurn: 1, SourceSpanStart: -1, SourceSpanEnd: -1, ReviewState: EntityIdentityReviewStateSourceObserved, IdempotencyKey: "ik-surface"}
	if err := st.SaveEntityIdentitySurface(ctx, surface); err != nil {
		t.Fatalf("save surface: %v", err)
	}
	var validToNull, spanStartNull int
	if err := conn.QueryRow(ctx, `SELECT valid_to_turn IS NULL, source_span_start IS NULL FROM entity_identity_surfaces WHERE surface_id = ?`, surface.SurfaceID).Scan(&validToNull, &spanStartNull); err != nil || validToNull != 1 || spanStartNull != 1 {
		t.Fatalf("surface nullable fields = %d %d, %v; want both NULL", validToNull, spanStartNull, err)
	}

	binding := &EntityIdentityArtifactBinding{BindingID: "identity-write-binding", StableEntityID: one.StableEntityID, ChatSessionID: one.ChatSessionID, ArtifactKind: "memory", ArtifactRole: "actor", ArtifactOrdinal: 0, SurfaceText: "One", ReviewState: "needs_review", SourceContract: one.SourceContract, SourceRevision: one.SourceRevision, SourceTurn: 1, IdempotencyKey: "ik-binding"}
	if err := st.SaveEntityIdentityArtifactBinding(ctx, binding); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	binding.ReviewState = EntityIdentityReviewStateReviewed
	if err := st.SaveEntityIdentityArtifactBinding(ctx, binding); err != nil {
		t.Fatalf("upsert binding: %v", err)
	}
	var review string
	if err := conn.QueryRow(ctx, `SELECT review_state FROM entity_identity_artifact_bindings WHERE binding_id = ?`, binding.BindingID).Scan(&review); err != nil || review != EntityIdentityReviewStateReviewed {
		t.Fatalf("binding review = %q, %v", review, err)
	}

	attribution := &SpeakerAttribution{AttributionID: "identity-write-attribution", ChatSessionID: one.ChatSessionID, SpeakerEntityID: one.StableEntityID, IdentityNamespace: one.IdentityNamespace, SourceRole: "assistant", AttributionKind: "explicit", AttributionState: "active", ReviewState: EntityIdentityReviewStateSourceObserved, Confidence: .8, SourceContract: one.SourceContract, SourceRevision: one.SourceRevision, SourceContentHash: "hash", SourceTurn: 1, SourceSpanStart: 0, SourceSpanEnd: 1, EvidenceExcerpt: "One", IdempotencyKey: "ik-attribution"}
	if err := st.SaveSpeakerAttribution(ctx, attribution); err != nil {
		t.Fatalf("save attribution: %v", err)
	}
	if err := st.SaveEntityIdentity(ctx, d1IdentityWriteSeed("identity-write-two")); err != nil {
		t.Fatalf("save target identity: %v", err)
	}
	link := &EntityIdentityLink{LinkID: "identity-write-link", ChatSessionID: one.ChatSessionID, SourceEntityID: one.StableEntityID, TargetEntityID: "identity-write-two", LinkKind: EntityIdentityLinkKindCanonicalEquivalence, LinkState: "needs_review", EvidenceJSON: `{"v":1}`, MappingRevision: 1, SourceContract: one.SourceContract, SourceRevision: one.SourceRevision}
	if err := st.SaveEntityIdentityLink(ctx, link); err != nil {
		t.Fatalf("save link: %v", err)
	}
	link.LinkState, link.MappingRevision = EntityIdentityLinkStateReviewed, 3
	if err := st.SaveEntityIdentityLink(ctx, link); err != nil {
		t.Fatalf("upsert link: %v", err)
	}
	var revision int
	if err := conn.QueryRow(ctx, `SELECT link_state, mapping_revision FROM entity_identity_links WHERE link_id = ?`, link.LinkID).Scan(&review, &revision); err != nil || review != EntityIdentityLinkStateReviewed || revision != 3 {
		t.Fatalf("link = %q/%d, %v", review, revision, err)
	}
}

func TestD1EntityIdentityWritesRejectStaleSourceAndInvalidLink(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	item := d1IdentityWriteSeed("identity-write-stale")
	if err := st.SaveEntityIdentity(ctx, item); !errors.Is(err, ErrSourceRevisionStale) {
		t.Fatalf("missing revision error = %v, want stale", err)
	}
	d1SeedIdentityRevision(t, conn, item.ChatSessionID, item.SourceRevision, "turn-1", "invalidated")
	if err := st.SaveEntityIdentity(ctx, item); !errors.Is(err, ErrSourceRevisionStale) {
		t.Fatalf("invalid revision error = %v, want stale", err)
	}
	if err := st.SaveEntityIdentityLink(ctx, &EntityIdentityLink{ChatSessionID: "s", SourceEntityID: "same", TargetEntityID: "same"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("self link error = %v, want ErrNotFound", err)
	}
	if !st.EntityIdentityWritesEnabled() {
		t.Fatal("D1 identity writes must be advertised as enabled")
	}
}
