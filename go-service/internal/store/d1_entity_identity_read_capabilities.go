package store

import (
	"context"
	"errors"
	"strings"
)

// D1 entity identity read capabilities.
//
// Three read-only entity identity capabilities are ported here, and only these
// three:
//
//   - EntityIdentityCatalogReader  (ListActiveEntityIdentities,
//     ListActiveEntityIdentitySurfaces, ListReviewedEntityIdentityLinks)
//   - ReviewedEntityIdentityResolver  (ResolveReviewedCanonicalEntityID)
//   - UniqueActiveEntitySurfaceIdentityResolver
//     (ResolveUniqueActiveEntityIdentityBySurface)
//
// The write side (EntityIdentityWriter, EntityIdentityLinkWriter,
// EntityIdentityWriteAvailability) is deliberately NOT ported. A D1 deployment
// cannot invent the entity identity projection these rows describe, so a
// half-ported writer would let extraction write identity occurrences the D1
// catalog could never review. The read side is safe because a D1 database
// seeded through the migration lane is readable by exactly the same
// predicates.
//
// The three capabilities share one domain rule, and the SQL restates it
// literally rather than relying on a shared helper: a row is visible only when
// the source revision it names is still 'active' in memory_source_revisions. A
// superseded or invalidated revision removes the row from every identity read,
// so a stale extraction cannot keep answering through its old provenance.
//
// SQLite parity notes, all verified against the SQLite engine in the tests:
//
//   - MariaDB's COALESCE(col, '') and COALESCE(col, 0) exist unchanged in
//     SQLite, so the nullable source/message/generation ids, the optional turn
//     bounds, the optional span bounds, and the optional evidence excerpt are
//     read with the identical expressions.
//   - The canonical schema stores evidence_json as TEXT rather than MariaDB's
//     JSON. Neither read path inspects it: the value is projected verbatim into
//     EntityIdentityLink.EvidenceJSON, so no JSON function is involved and no
//     normalisation difference can appear.
//   - entity_identities.stable_entity_id is the primary key in both schemas, so
//     the source_revision join against memory_source_revisions is the only
//     place a row can be filtered, and it is reproduced unchanged.
//   - Timestamps are TEXT in D1. created_at ordering on the link catalog keeps
//     the same meaning because the schema default and d1TimeValue both write
//     zero-padded RFC3339 UTC, which sorts lexicographically in chronological
//     order; link_id breaks ties exactly as the MariaDB ORDER BY does.

var _ EntityIdentityCatalogReader = (*d1Store)(nil)
var _ ReviewedEntityIdentityResolver = (*d1Store)(nil)
var _ UniqueActiveEntitySurfaceIdentityResolver = (*d1Store)(nil)

// d1ActiveEntityIdentityColumns is the shared projection for identity reads.
// COALESCE reproduces MariaDB's nullable-column reads: a NULL source turn id,
// message id, or generation id becomes the empty string the struct field uses.
const d1ActiveEntityIdentityColumns = `
		identity.stable_entity_id, identity.chat_session_id,
		identity.identity_namespace, identity.entity_kind, identity.canonical_label,
		identity.lifecycle_state, identity.review_state, identity.presence_authority,
		identity.occurrence_authority, identity.source_contract, identity.source_revision,
		COALESCE(identity.source_logical_turn_id, ''), COALESCE(identity.source_message_id, ''),
		COALESCE(identity.source_generation_id, ''), identity.source_content_hash,
		identity.source_turn, identity.source_index, identity.idempotency_key,
		identity.mapping_revision, identity.first_seen_turn, identity.last_seen_turn,
		identity.created_at, identity.updated_at`

// d1ActiveEntityIdentityJoin is the source-revision liveness join shared by the
// identity catalog and the single-identity root read. A row whose named
// revision is missing, superseded, or invalidated is not an active identity.
const d1ActiveEntityIdentityJoin = `
		JOIN memory_source_revisions revision
		  ON revision.chat_session_id = identity.chat_session_id
		 AND revision.source_revision = identity.source_revision
		 AND revision.lifecycle_state = 'active'`

// d1ActiveEntityIdentityEligible is the shared row filter: the session scope,
// an active lifecycle, and a review state the review UI can act on.
const d1ActiveEntityIdentityEligible = `
		WHERE identity.chat_session_id = ?
		  AND identity.lifecycle_state = 'active'
		  AND identity.review_state IN (?, ?)`

// d1ActiveEntityIdentityOrder is the catalog order: earliest first observation,
// then the stable id, so a repeated turn cannot reorder the same row.
const d1ActiveEntityIdentityOrder = ` ORDER BY identity.first_seen_turn ASC, identity.stable_entity_id ASC`

// d1ActiveEntityIdentityArgs is the shared argument list for the eligible read.
func d1ActiveEntityIdentityArgs(chatSessionID string) []any {
	return []any{chatSessionID, EntityIdentityReviewStateSourceObserved, EntityIdentityReviewStateReviewed}
}

// ListActiveEntityIdentities returns the active, source-backed identity catalog
// for one session, ordered by first observation then stable id.
//
// A blank session is rejected with ErrNotFound rather than returning every
// session's identities, matching the MariaDB guard.
func (s *d1Store) ListActiveEntityIdentities(ctx context.Context, chatSessionID string) ([]EntityIdentity, error) {
	chatSessionID = strings.TrimSpace(chatSessionID)
	if chatSessionID == "" {
		return nil, ErrNotFound
	}
	rows, err := s.conn.Query(ctx, `
		SELECT`+d1ActiveEntityIdentityColumns+`
		FROM entity_identities identity`+d1ActiveEntityIdentityJoin+d1ActiveEntityIdentityEligible+d1ActiveEntityIdentityOrder,
		d1ActiveEntityIdentityArgs(chatSessionID)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// An empty, non-nil slice is part of the contract: the review routes range
	// over the result and distinguish "no rows" from "read failed" by the error.
	out := []EntityIdentity{}
	for rows.Next() {
		item, err := scanEntityIdentity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ListActiveEntityIdentitySurfaces returns the source-linked names and aliases
// of the session's active identities, ordered by source turn then surface id.
//
// The identity join is INNER, so a surface whose owning identity is not active
// is not offered as a review candidate; the revision join is INNER for the same
// liveness reason. Only 'source_observed' surfaces qualify: a surface is a
// candidate for the reviewer to judge, and a reviewed surface is already
// resolved.
func (s *d1Store) ListActiveEntityIdentitySurfaces(ctx context.Context, chatSessionID string) ([]EntityIdentitySurface, error) {
	chatSessionID = strings.TrimSpace(chatSessionID)
	if chatSessionID == "" {
		return nil, ErrNotFound
	}
	rows, err := s.conn.Query(ctx, `
		SELECT surface.surface_id, surface.stable_entity_id, surface.chat_session_id,
		       surface.identity_namespace, surface.surface_kind, surface.surface_text,
		       surface.normalized_surface, surface.surface_scope, surface.valid_from_turn,
		       COALESCE(surface.valid_to_turn, 0), surface.source_contract, surface.source_revision,
		       surface.source_turn, COALESCE(surface.source_span_start, -1),
		       COALESCE(surface.source_span_end, -1), COALESCE(surface.evidence_excerpt, ''),
		       surface.review_state, surface.idempotency_key, surface.created_at, surface.updated_at
		FROM entity_identity_surfaces surface
		JOIN entity_identities identity
		  ON identity.chat_session_id = surface.chat_session_id
		 AND identity.stable_entity_id = surface.stable_entity_id
		 AND identity.lifecycle_state = 'active'
		JOIN memory_source_revisions revision
		  ON revision.chat_session_id = surface.chat_session_id
		 AND revision.source_revision = surface.source_revision
		 AND revision.lifecycle_state = 'active'
		WHERE surface.chat_session_id = ?
		  AND surface.review_state = ?
		ORDER BY surface.source_turn ASC, surface.surface_id ASC
	`, chatSessionID, EntityIdentityReviewStateSourceObserved)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []EntityIdentitySurface{}
	for rows.Next() {
		var item EntityIdentitySurface
		if err := rows.Scan(
			&item.SurfaceID, &item.StableEntityID, &item.ChatSessionID,
			&item.IdentityNamespace, &item.SurfaceKind, &item.SurfaceText,
			&item.NormalizedSurface, &item.Scope, &item.ValidFromTurn,
			&item.ValidToTurn, &item.SourceContract, &item.SourceRevision,
			&item.SourceTurn, &item.SourceSpanStart, &item.SourceSpanEnd,
			&item.EvidenceExcerpt, &item.ReviewState, &item.IdempotencyKey,
			&item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ListReviewedEntityIdentityLinks returns the reviewed canonical-equivalence
// links of one session, oldest first.
//
// Only 'reviewed' links are listed, and only the canonical-equivalence kind: a
// revoked link and a non-equivalence link are not something the review UI may
// present as an active equivalence. The projection is filtered in SQL rather
// than in Go so the ordering covers exactly the returned rows.
func (s *d1Store) ListReviewedEntityIdentityLinks(ctx context.Context, chatSessionID string) ([]EntityIdentityLink, error) {
	chatSessionID = strings.TrimSpace(chatSessionID)
	if chatSessionID == "" {
		return nil, ErrNotFound
	}
	rows, err := s.conn.Query(ctx, `
		SELECT link_id, chat_session_id, source_entity_id, target_entity_id,
		       link_kind, link_state, evidence_json, mapping_revision, created_at, updated_at
		FROM entity_identity_links
		WHERE chat_session_id = ?
		  AND link_kind = ?
		  AND link_state = ?
		ORDER BY created_at ASC, link_id ASC
	`, chatSessionID, EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []EntityIdentityLink{}
	for rows.Next() {
		var item EntityIdentityLink
		if err := rows.Scan(
			&item.LinkID, &item.ChatSessionID, &item.SourceEntityID, &item.TargetEntityID,
			&item.LinkKind, &item.LinkState, &item.EvidenceJSON, &item.MappingRevision,
			&item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ResolveReviewedCanonicalEntityID follows the reviewed, directional link chain
// from a source occurrence to its canonical target.
//
// The chain is walked one hop at a time and the visited set is retained across
// hops, so a cycle is reported as ErrReviewedEntityIdentityCycle instead of
// looping forever. Absence is distinguished at each hop: a hop that resolves
// nothing terminates the walk and returns the last resolved id when at least
// one hop was taken, or ErrNotFound when the source is already the root. Two
// distinct reviewed targets from one hop are a contradiction the caller must
// resolve, so they fail closed with ErrReviewedEntityIdentityAmbiguous.
func (s *d1Store) ResolveReviewedCanonicalEntityID(ctx context.Context, chatSessionID, sourceEntityID string) (string, error) {
	chatSessionID = strings.TrimSpace(chatSessionID)
	sourceEntityID = strings.TrimSpace(sourceEntityID)
	if chatSessionID == "" || sourceEntityID == "" {
		return "", ErrNotFound
	}
	current := sourceEntityID
	visited := map[string]bool{}
	resolvedAny := false
	for {
		if visited[current] {
			return "", ErrReviewedEntityIdentityCycle
		}
		visited[current] = true
		next, err := s.resolveReviewedCanonicalEntityIDOneHop(ctx, chatSessionID, current)
		if errors.Is(err, ErrNotFound) {
			if resolvedAny {
				return current, nil
			}
			return "", ErrNotFound
		}
		if err != nil {
			return "", err
		}
		resolvedAny = true
		current = next
	}
}

// resolveReviewedCanonicalEntityIDOneHop reads the single reviewed canonical
// target of one source occurrence, or reports absence and ambiguity.
//
// The three INNER joins are the fail-closed part: a link is only honoured when
// its target is an active, reviewable identity AND that target's own source
// revision is still active. A link pointing at a retired or unreviewed target
// is therefore indistinguishable from no link at all.
func (s *d1Store) resolveReviewedCanonicalEntityIDOneHop(ctx context.Context, chatSessionID, sourceEntityID string) (string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT DISTINCT identity_link.target_entity_id
		FROM entity_identity_links identity_link
		JOIN entity_identities canonical_target
		  ON canonical_target.stable_entity_id = identity_link.target_entity_id
		 AND canonical_target.chat_session_id = identity_link.chat_session_id
		JOIN memory_source_revisions canonical_revision
		  ON canonical_revision.chat_session_id = canonical_target.chat_session_id
		 AND canonical_revision.source_revision = canonical_target.source_revision
		 AND canonical_revision.lifecycle_state = 'active'
		WHERE identity_link.chat_session_id = ?
		  AND identity_link.source_entity_id = ?
		  AND identity_link.target_entity_id <> identity_link.source_entity_id
		  AND identity_link.link_kind = ?
		  AND identity_link.link_state = ?
		  AND canonical_target.lifecycle_state = 'active'
		  AND canonical_target.review_state IN (?, ?)
		ORDER BY identity_link.target_entity_id ASC
	`, chatSessionID, sourceEntityID, EntityIdentityLinkKindCanonicalEquivalence,
		EntityIdentityLinkStateReviewed, EntityIdentityReviewStateSourceObserved, EntityIdentityReviewStateReviewed)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	targets := map[string]struct{}{}
	for rows.Next() {
		var targetEntityID string
		if err := rows.Scan(&targetEntityID); err != nil {
			return "", err
		}
		if targetEntityID = strings.TrimSpace(targetEntityID); targetEntityID != "" {
			targets[targetEntityID] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(targets) == 0 {
		return "", ErrNotFound
	}
	if len(targets) != 1 {
		return "", ErrReviewedEntityIdentityAmbiguous
	}
	for targetEntityID := range targets {
		return targetEntityID, nil
	}
	return "", ErrNotFound
}

// d1ActiveEntityIdentityByID reads one active, source-backed identity by its
// stable id. It exists to read the canonical root's namespace-owned fields
// after a reviewed link chain has been followed; a model must never supply
// them.
func (s *d1Store) d1ActiveEntityIdentityByID(ctx context.Context, chatSessionID, stableEntityID string) (EntityIdentity, error) {
	item, err := scanEntityIdentity(s.conn.QueryRow(ctx, `
		SELECT`+d1ActiveEntityIdentityColumns+`
		FROM entity_identities identity`+d1ActiveEntityIdentityJoin+`
		WHERE identity.chat_session_id = ?
		  AND identity.stable_entity_id = ?
		  AND identity.lifecycle_state = 'active'
		  AND identity.review_state IN (?, ?)
	`, chatSessionID, stableEntityID, EntityIdentityReviewStateSourceObserved, EntityIdentityReviewStateReviewed))
	if errors.Is(err, errD1NoRows) {
		return EntityIdentity{}, ErrNotFound
	}
	return item, err
}

// ResolveUniqueActiveEntityIdentityBySurface resolves a source-observed
// surface to the one active identity every matching occurrence converges on,
// and returns the database-owned namespace, kind, and label with it.
//
// The decision rules are the ones that keep a display name from acting as a
// merge key:
//
//  1. Only source-observed surfaces in the 3.9 or current scope are candidates,
//     and only from an active, reviewable identity whose source revision is
//     still active.
//  2. A reviewed canonical-equivalence link moves the candidate to its target,
//     and a link whose target is missing, retired, or unreviewed drops the row
//     entirely rather than resolving to an unverified target. This is why the
//     canonical target and its revision are LEFT joined and then required.
//  3. Candidates collapse by resolved id AND namespace, so the same id reached
//     through different link states is one candidate, not two.
//  4. A candidate that arrived through a link is re-resolved to its chain root
//     and re-read from the database, so the returned namespace is the root's.
//  5. A single remaining candidate wins. Several candidates win only when every
//     contributing surface is an exact 'display_name' row and all of them carry
//     the identical namespace, entity kind, and canonical label; otherwise the
//     resolution is ambiguous and fails closed. An alias surface therefore
//     never collapses two occurrences, and neither does a name shared by two
//     different kinds.
func (s *d1Store) ResolveUniqueActiveEntityIdentityBySurface(ctx context.Context, chatSessionID, normalizedSurface string) (ResolvedEntityIdentity, error) {
	chatSessionID = strings.TrimSpace(chatSessionID)
	normalizedSurface = strings.TrimSpace(normalizedSurface)
	if chatSessionID == "" || normalizedSurface == "" {
		return ResolvedEntityIdentity{}, ErrNotFound
	}
	rows, err := s.conn.Query(ctx, `
		SELECT
			surface.stable_entity_id,
			surface.surface_kind,
			source_identity.identity_namespace,
			source_identity.entity_kind,
			source_identity.canonical_label,
			source_identity.source_turn,
			COALESCE(identity_link.target_entity_id, ''),
			COALESCE(canonical_target.identity_namespace, ''),
			COALESCE(canonical_target.entity_kind, ''),
			COALESCE(canonical_target.canonical_label, ''),
			COALESCE(canonical_target.source_turn, 0)
		FROM entity_identity_surfaces surface
		JOIN entity_identities source_identity
		  ON source_identity.chat_session_id = surface.chat_session_id
		 AND source_identity.stable_entity_id = surface.stable_entity_id
		 AND source_identity.lifecycle_state = 'active'
		 AND source_identity.review_state IN ('source_observed', 'reviewed')
		JOIN memory_source_revisions source_revision
		  ON source_revision.chat_session_id = surface.chat_session_id
		 AND source_revision.source_revision = surface.source_revision
		 AND source_revision.lifecycle_state = 'active'
		LEFT JOIN entity_identity_links identity_link
		  ON identity_link.chat_session_id = surface.chat_session_id
		 AND identity_link.source_entity_id = surface.stable_entity_id
		 AND identity_link.target_entity_id <> identity_link.source_entity_id
		 AND identity_link.link_kind = ?
		 AND identity_link.link_state = ?
		LEFT JOIN entity_identities canonical_target
		  ON canonical_target.chat_session_id = identity_link.chat_session_id
		 AND canonical_target.stable_entity_id = identity_link.target_entity_id
		 AND canonical_target.lifecycle_state = 'active'
		 AND canonical_target.review_state IN (?, ?)
		LEFT JOIN memory_source_revisions canonical_revision
		  ON canonical_revision.chat_session_id = canonical_target.chat_session_id
		 AND canonical_revision.source_revision = canonical_target.source_revision
		 AND canonical_revision.lifecycle_state = 'active'
		WHERE surface.chat_session_id = ?
		  AND surface.normalized_surface = ?
		  AND surface.surface_scope IN (?, ?)
		  AND surface.review_state = 'source_observed'
		  AND (identity_link.target_entity_id IS NULL OR canonical_revision.source_revision IS NOT NULL)
		ORDER BY surface.stable_entity_id ASC, identity_link.target_entity_id ASC
	`, EntityIdentityLinkKindCanonicalEquivalence, EntityIdentityLinkStateReviewed,
		EntityIdentityReviewStateSourceObserved, EntityIdentityReviewStateReviewed,
		chatSessionID, normalizedSurface, EntityIdentitySurfaceScope39, EntityIdentitySurfaceScopeCurrent)
	if err != nil {
		return ResolvedEntityIdentity{}, err
	}
	defer rows.Close()

	// candidate is one resolved occurrence plus the turn that justifies its
	// precedence: a lower turn means an earlier observation and therefore the
	// winner when two candidates would otherwise tie.
	type candidate struct {
		identity   ResolvedEntityIdentity
		sourceTurn int
		needsChain bool
	}
	resolved := map[string]candidate{}
	exactDisplayTuple := ""
	exactDisplayOnly := true
	for rows.Next() {
		var sourceEntityID, surfaceKind, sourceNamespace, sourceKind, sourceLabel string
		var targetEntityID, targetNamespace, targetKind, targetLabel string
		var sourceTurn, targetTurn int
		if err := rows.Scan(
			&sourceEntityID, &surfaceKind, &sourceNamespace, &sourceKind, &sourceLabel, &sourceTurn,
			&targetEntityID, &targetNamespace, &targetKind, &targetLabel, &targetTurn,
		); err != nil {
			return ResolvedEntityIdentity{}, err
		}
		entityID := strings.TrimSpace(targetEntityID)
		namespace := strings.TrimSpace(targetNamespace)
		entityKind := strings.TrimSpace(targetKind)
		label := strings.TrimSpace(targetLabel)
		identityTurn := targetTurn
		if entityID == "" {
			entityID = strings.TrimSpace(sourceEntityID)
			namespace = strings.TrimSpace(sourceNamespace)
			entityKind = strings.TrimSpace(sourceKind)
			label = strings.TrimSpace(sourceLabel)
			identityTurn = sourceTurn
		}
		// An identity missing its namespace or kind is not addressable, so it
		// never becomes a candidate and cannot contribute to the display-name
		// tuple either.
		if entityID == "" || namespace == "" || entityKind == "" {
			continue
		}
		identity := ResolvedEntityIdentity{
			StableEntityID: entityID, IdentityNamespace: namespace,
			EntityKind: entityKind, CanonicalLabel: label,
		}
		key := entityID + "\x1f" + namespace
		current, exists := resolved[key]
		if !exists || identityTurn < current.sourceTurn ||
			(identityTurn == current.sourceTurn && entityID < current.identity.StableEntityID) {
			resolved[key] = candidate{
				identity: identity, sourceTurn: identityTurn,
				needsChain: strings.TrimSpace(targetEntityID) != "",
			}
		} else if strings.TrimSpace(targetEntityID) != "" {
			current.needsChain = true
			resolved[key] = current
		}
		tuple := namespace + "\x1f" + entityKind + "\x1f" + label
		if strings.TrimSpace(surfaceKind) != "display_name" {
			exactDisplayOnly = false
		} else if exactDisplayTuple == "" {
			exactDisplayTuple = tuple
		} else if exactDisplayTuple != tuple {
			exactDisplayOnly = false
		}
	}
	if err := rows.Err(); err != nil {
		return ResolvedEntityIdentity{}, err
	}
	// Closing before the follow-up reads releases the cursor: a D1 result set
	// holds the connection until it is closed, so leaving it open would
	// serialise the chain lookups behind the cursor that is already drained.
	if err := rows.Close(); err != nil {
		return ResolvedEntityIdentity{}, err
	}

	// collapsed holds the same candidates after every link-bearing one has been
	// moved to its chain root, so two occurrences of one person are counted
	// once under the root's namespace.
	collapsed := map[string]candidate{}
	for _, item := range resolved {
		if !item.needsChain {
			key := item.identity.StableEntityID + "\x1f" + item.identity.IdentityNamespace
			existing, exists := collapsed[key]
			if !exists || item.sourceTurn < existing.sourceTurn ||
				(item.sourceTurn == existing.sourceTurn && item.identity.StableEntityID < existing.identity.StableEntityID) {
				collapsed[key] = item
			}
			continue
		}
		rootID, err := s.ResolveReviewedCanonicalEntityID(ctx, chatSessionID, item.identity.StableEntityID)
		switch {
		case err == nil && strings.TrimSpace(rootID) != "":
			root, getErr := s.d1ActiveEntityIdentityByID(ctx, chatSessionID, rootID)
			if getErr != nil {
				return ResolvedEntityIdentity{}, getErr
			}
			item.identity = ResolvedEntityIdentity{
				StableEntityID: root.StableEntityID, IdentityNamespace: root.IdentityNamespace,
				EntityKind: root.EntityKind, CanonicalLabel: root.CanonicalLabel,
			}
			item.sourceTurn = root.SourceTurn
		case errors.Is(err, ErrNotFound):
			// This identity is already a canonical root: the link led to a node
			// with no further reviewed target, so the identity stands as-is.
		case err != nil:
			return ResolvedEntityIdentity{}, err
		}
		key := item.identity.StableEntityID + "\x1f" + item.identity.IdentityNamespace
		current, exists := collapsed[key]
		if !exists || item.sourceTurn < current.sourceTurn ||
			(item.sourceTurn == current.sourceTurn && item.identity.StableEntityID < current.identity.StableEntityID) {
			collapsed[key] = item
		}
	}
	resolved = collapsed

	if len(resolved) == 0 {
		return ResolvedEntityIdentity{}, ErrNotFound
	}
	if len(resolved) == 1 {
		for _, item := range resolved {
			return item.identity, nil
		}
	}
	if exactDisplayOnly && exactDisplayTuple != "" {
		var selected candidate
		for _, item := range resolved {
			if selected.identity.StableEntityID == "" || item.sourceTurn < selected.sourceTurn ||
				(item.sourceTurn == selected.sourceTurn && item.identity.StableEntityID < selected.identity.StableEntityID) {
				selected = item
			}
		}
		if selected.identity.StableEntityID != "" {
			return selected.identity, nil
		}
	}
	return ResolvedEntityIdentity{}, ErrReviewedEntityIdentityAmbiguous
}
