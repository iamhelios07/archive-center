package store

import (
	"context"
	"errors"
	"strings"
)

// D1 entity-identity write capabilities. Identity projections accepted from the
// source-observation contract must be fenced to a currently active revision;
// other contracts retain the MariaDB writer's unrestricted projection behavior.
var _ EntityIdentityWriter = (*d1Store)(nil)
var _ EntityIdentityLinkWriter = (*d1Store)(nil)
var _ EntityIdentityWriteAvailability = (*d1Store)(nil)

func (s *d1Store) EntityIdentityWritesEnabled() bool { return s != nil && s.conn != nil }

// d1WithActiveEntitySourceWrite serializes the source-state check and following
// write inside this service instance. D1 has no transaction handle across bridge
// calls; the D1 binding executes each statement atomically, and the mutex avoids
// an in-process stale-source admission between the check and mutation.
func (s *d1Store) d1WithActiveEntitySourceWrite(ctx context.Context, contract, sessionID, revision string, write func() error) error {
	if strings.TrimSpace(contract) != acceptedSourceObservationContract {
		return write()
	}
	sessionID = strings.TrimSpace(sessionID)
	revision = strings.TrimSpace(revision)
	if sessionID == "" || revision == "" {
		return ErrSourceRevisionStale
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()
	var lifecycle string
	err := s.conn.QueryRow(ctx, `SELECT lifecycle_state FROM memory_source_revisions
		WHERE chat_session_id = ? AND source_revision = ?`, sessionID, revision).Scan(&lifecycle)
	if errors.Is(err, ErrNotFound) || errors.Is(err, errD1NoRows) {
		return ErrSourceRevisionStale
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(lifecycle) != "active" {
		return ErrSourceRevisionStale
	}
	return write()
}

func (s *d1Store) SaveEntityIdentity(ctx context.Context, item *EntityIdentity) error {
	if item == nil {
		return errors.New("entity identity is required")
	}
	return s.d1WithActiveEntitySourceWrite(ctx, item.SourceContract, item.ChatSessionID, item.SourceRevision, func() error {
		_, err := s.conn.Exec(ctx, `INSERT INTO entity_identities (
			stable_entity_id, chat_session_id, identity_namespace, entity_kind,
			canonical_label, lifecycle_state, review_state, presence_authority,
			occurrence_authority, source_contract, source_revision,
			source_logical_turn_id, source_message_id, source_generation_id,
			source_content_hash, source_turn, source_index, idempotency_key,
			mapping_revision, first_seen_turn, last_seen_turn, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (stable_entity_id) DO UPDATE SET
			last_seen_turn = MAX(entity_identities.last_seen_turn, excluded.last_seen_turn),
			updated_at = excluded.updated_at`, item.StableEntityID, item.ChatSessionID,
			item.IdentityNamespace, item.EntityKind, item.CanonicalLabel, item.LifecycleState,
			item.ReviewState, item.PresenceAuthority, item.OccurrenceAuthority,
			item.SourceContract, item.SourceRevision, d1NullableString(item.SourceLogicalTurnID),
			d1NullableString(item.SourceMessageID), d1NullableString(item.SourceGenerationID),
			item.SourceContentHash, item.SourceTurn, item.SourceIndex, item.IdempotencyKey,
			item.MappingRevision, item.FirstSeenTurn, item.LastSeenTurn, d1TimeValue(item.CreatedAt), d1TimeValue(item.UpdatedAt))
		return err
	})
}

func (s *d1Store) SaveEntityIdentitySurface(ctx context.Context, item *EntityIdentitySurface) error {
	if item == nil {
		return errors.New("entity identity surface is required")
	}
	return s.d1WithActiveEntitySourceWrite(ctx, item.SourceContract, item.ChatSessionID, item.SourceRevision, func() error {
		_, err := s.conn.Exec(ctx, `INSERT INTO entity_identity_surfaces (
			surface_id, stable_entity_id, chat_session_id, identity_namespace,
			surface_kind, surface_text, normalized_surface, surface_scope,
			valid_from_turn, valid_to_turn, source_contract, source_revision,
			source_turn, source_span_start, source_span_end, evidence_excerpt,
			review_state, idempotency_key, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (surface_id) DO UPDATE SET updated_at = excluded.updated_at`,
			item.SurfaceID, item.StableEntityID, item.ChatSessionID, item.IdentityNamespace,
			item.SurfaceKind, item.SurfaceText, item.NormalizedSurface, item.Scope,
			item.ValidFromTurn, d1NullablePositiveInt(item.ValidToTurn), item.SourceContract,
			item.SourceRevision, item.SourceTurn, d1NullableNonNegativeInt(item.SourceSpanStart),
			d1NullableNonNegativeInt(item.SourceSpanEnd), d1NullableString(item.EvidenceExcerpt),
			item.ReviewState, item.IdempotencyKey, d1TimeValue(item.CreatedAt), d1TimeValue(item.UpdatedAt))
		return err
	})
}

func (s *d1Store) SaveEntityIdentityArtifactBinding(ctx context.Context, item *EntityIdentityArtifactBinding) error {
	if item == nil {
		return errors.New("entity identity artifact binding is required")
	}
	return s.d1WithActiveEntitySourceWrite(ctx, item.SourceContract, item.ChatSessionID, item.SourceRevision, func() error {
		_, err := s.conn.Exec(ctx, `INSERT INTO entity_identity_artifact_bindings (
			binding_id, stable_entity_id, chat_session_id, artifact_kind, artifact_role,
			artifact_ordinal, surface_text, review_state, source_contract, source_revision,
			source_turn, idempotency_key, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (binding_id) DO UPDATE SET review_state = excluded.review_state`,
			item.BindingID, item.StableEntityID, item.ChatSessionID, item.ArtifactKind,
			item.ArtifactRole, item.ArtifactOrdinal, item.SurfaceText, item.ReviewState,
			item.SourceContract, item.SourceRevision, item.SourceTurn, item.IdempotencyKey,
			d1TimeValue(item.CreatedAt))
		return err
	})
}

func (s *d1Store) SaveSpeakerAttribution(ctx context.Context, item *SpeakerAttribution) error {
	if item == nil {
		return errors.New("speaker attribution is required")
	}
	return s.d1WithActiveEntitySourceWrite(ctx, item.SourceContract, item.ChatSessionID, item.SourceRevision, func() error {
		_, err := s.conn.Exec(ctx, `INSERT INTO speaker_attributions (
			attribution_id, chat_session_id, speaker_entity_id, identity_namespace,
			source_role, attribution_kind, attribution_state, review_state, confidence,
			source_contract, source_revision, source_logical_turn_id, source_message_id,
			source_generation_id, source_content_hash, source_turn, source_span_start,
			source_span_end, evidence_excerpt, idempotency_key, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (attribution_id) DO UPDATE SET updated_at = excluded.updated_at`,
			item.AttributionID, item.ChatSessionID, item.SpeakerEntityID, item.IdentityNamespace,
			item.SourceRole, item.AttributionKind, item.AttributionState, item.ReviewState,
			item.Confidence, item.SourceContract, item.SourceRevision, d1NullableString(item.SourceLogicalTurn),
			d1NullableString(item.SourceMessageID), d1NullableString(item.SourceGeneration),
			item.SourceContentHash, item.SourceTurn, item.SourceSpanStart, item.SourceSpanEnd,
			item.EvidenceExcerpt, item.IdempotencyKey, d1TimeValue(item.CreatedAt), d1TimeValue(item.UpdatedAt))
		return err
	})
}

func (s *d1Store) SaveEntityIdentityLink(ctx context.Context, item *EntityIdentityLink) error {
	if item == nil || strings.TrimSpace(item.ChatSessionID) == "" || strings.TrimSpace(item.SourceEntityID) == "" || strings.TrimSpace(item.TargetEntityID) == "" || item.SourceEntityID == item.TargetEntityID {
		return ErrNotFound
	}
	return s.d1WithActiveEntitySourceWrite(ctx, item.SourceContract, item.ChatSessionID, item.SourceRevision, func() error {
		_, err := s.conn.Exec(ctx, `INSERT INTO entity_identity_links (
			link_id, chat_session_id, source_entity_id, target_entity_id, link_kind,
			link_state, evidence_json, mapping_revision, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (chat_session_id, source_entity_id, target_entity_id, link_kind) DO UPDATE SET
			link_state = excluded.link_state, evidence_json = excluded.evidence_json,
			mapping_revision = MAX(entity_identity_links.mapping_revision, excluded.mapping_revision),
			updated_at = excluded.updated_at`, item.LinkID, item.ChatSessionID, item.SourceEntityID,
			item.TargetEntityID, item.LinkKind, item.LinkState, item.EvidenceJSON, item.MappingRevision,
			d1TimeValue(item.CreatedAt), d1TimeValue(item.UpdatedAt))
		return err
	})
}

func d1NullablePositiveInt(value int) any {
	if value <= 0 {
		return nil
	}
	return value
}
func d1NullableNonNegativeInt(value int) any {
	if value < 0 {
		return nil
	}
	return value
}
