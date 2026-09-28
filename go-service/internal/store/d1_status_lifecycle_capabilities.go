package store

import (
	"context"
	"strings"
)

// D1 status lifecycle capability: the change-event ledger and the effect ledger.
//
// Two limit rules differ between these lists and must not be unified:
// the change-event list treats -1 as "the complete ledger" while any other
// non-positive value falls back to the bounded default, whereas the effect list
// has no unbounded mode at all.

var _ StatusLifecycleStore = (*d1Store)(nil)

const d1StatusChangeEventSelect = `
		SELECT id, chat_session_id, registry_id, status_value_id, status_key, owner_scope, owner_id,
		       event_kind, previous_value_json, new_value_json, evidence_json, source_turn,
		       story_clock_json, event_state, created_at
		FROM status_change_events`

func (s *d1Store) ListStatusChangeEvents(ctx context.Context, chatSessionID, ownerScope, ownerID, statusKey string, limit int) ([]StatusChangeEvent, error) {
	// -1 keeps the whole ledger; any other non-positive limit uses the bounded
	// default, and only a positive limit emits a LIMIT clause.
	if limit <= 0 && limit != -1 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	sql := d1StatusChangeEventSelect + ` WHERE chat_session_id = ?`
	args := []any{chatSessionID}
	if trimmed := strings.TrimSpace(ownerScope); trimmed != "" {
		sql += ` AND owner_scope = ?`
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(ownerID); trimmed != "" {
		sql += ` AND owner_id = ?`
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(statusKey); trimmed != "" {
		sql += ` AND status_key = ?`
		args = append(args, trimmed)
	}
	sql += ` ORDER BY created_at DESC, id DESC`
	if limit > 0 {
		sql += ` LIMIT ?`
		args = append(args, limit)
	}

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []StatusChangeEvent
	for rows.Next() {
		var item StatusChangeEvent
		var statusValueID, sourceTurn *int64
		var previousValueJSON, newValueJSON, storyClockJSON *string
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.RegistryID, &statusValueID, &item.StatusKey, &item.OwnerScope, &item.OwnerID,
			&item.EventKind, &previousValueJSON, &newValueJSON, &item.EvidenceJSON, &sourceTurn,
			&storyClockJSON, &item.EventState, &item.CreatedAt,
		); err != nil {
			return nil, err
		}
		item.StatusValueID = d1DerefInt64(statusValueID)
		item.PreviousValueJSON = d1DerefString(previousValueJSON)
		item.NewValueJSON = d1DerefString(newValueJSON)
		item.StoryClockJSON = d1DerefString(storyClockJSON)
		item.SourceTurn = int(d1DerefInt64(sourceTurn))
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) SaveStatusChangeEvent(ctx context.Context, event StatusChangeEvent) (StatusChangeEvent, error) {
	now := d1TimeValue(event.CreatedAt)
	state := strings.TrimSpace(event.EventState)
	if state == "" {
		state = "recorded"
	}

	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO status_change_events (
			chat_session_id, registry_id, status_value_id, status_key, owner_scope, owner_id,
			event_kind, previous_value_json, new_value_json, evidence_json, source_turn,
			story_clock_json, event_state, created_at
		) VALUES (?, ?, NULLIF(?, 0), ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), ?, ?, ?)
		RETURNING id`,
		event.ChatSessionID, event.RegistryID, event.StatusValueID, event.StatusKey, event.OwnerScope, event.OwnerID,
		event.EventKind, d1NullableString(event.PreviousValueJSON), d1NullableString(event.NewValueJSON),
		event.EvidenceJSON, event.SourceTurn, d1NullableString(event.StoryClockJSON), state, now).Scan(&id); err != nil {
		return event, err
	}
	event.ID = id
	event.EventState = state
	event.CreatedAt = parseD1TimeOrZero(now)
	return event, nil
}

// ---------------------------------------------------------------------------
// status effects
// ---------------------------------------------------------------------------

const d1StatusEffectSelect = `
		SELECT id, chat_session_id, registry_id, status_key, owner_scope, owner_id,
		       effect_kind, effect_label, effect_payload_json, evidence_json, source_turn,
		       start_clock_json, duration_json, expires_at_clock_json, effect_state,
		       cleared_evidence_json, cleared_turn, created_at, updated_at
		FROM status_effects`

func (s *d1Store) ListStatusEffects(ctx context.Context, chatSessionID, ownerScope, ownerID, effectState string, limit int) ([]StatusEffect, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	sql := d1StatusEffectSelect + ` WHERE chat_session_id = ?`
	args := []any{chatSessionID}
	if trimmed := strings.TrimSpace(ownerScope); trimmed != "" {
		sql += ` AND owner_scope = ?`
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(ownerID); trimmed != "" {
		sql += ` AND owner_id = ?`
		args = append(args, trimmed)
	}
	if trimmed := strings.TrimSpace(effectState); trimmed != "" {
		sql += ` AND effect_state = ?`
		args = append(args, trimmed)
	}
	sql += ` ORDER BY updated_at DESC, id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []StatusEffect
	for rows.Next() {
		var item StatusEffect
		var effectLabel, payloadJSON, durationJSON, expiresJSON, clearedEvidence *string
		var sourceTurn, clearedTurn *int64
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.RegistryID, &item.StatusKey, &item.OwnerScope, &item.OwnerID,
			&item.EffectKind, &effectLabel, &payloadJSON, &item.EvidenceJSON, &sourceTurn,
			&item.StartClockJSON, &durationJSON, &expiresJSON, &item.EffectState,
			&clearedEvidence, &clearedTurn, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.EffectLabel = d1DerefString(effectLabel)
		item.EffectPayloadJSON = d1DerefString(payloadJSON)
		item.DurationJSON = d1DerefString(durationJSON)
		item.ExpiresAtClockJSON = d1DerefString(expiresJSON)
		item.ClearedEvidenceJSON = d1DerefString(clearedEvidence)
		item.SourceTurn = int(d1DerefInt64(sourceTurn))
		item.ClearedTurn = int(d1DerefInt64(clearedTurn))
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) SaveStatusEffect(ctx context.Context, effect StatusEffect) (StatusEffect, error) {
	now := d1TimeValue(effect.CreatedAt)
	state := strings.TrimSpace(effect.EffectState)
	if state == "" {
		state = "active"
	}

	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO status_effects (
			chat_session_id, registry_id, status_key, owner_scope, owner_id,
			effect_kind, effect_label, effect_payload_json, evidence_json, source_turn,
			start_clock_json, duration_json, expires_at_clock_json, effect_state, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		effect.ChatSessionID, effect.RegistryID, effect.StatusKey, effect.OwnerScope, effect.OwnerID,
		effect.EffectKind, d1NullableString(effect.EffectLabel), d1NullableString(effect.EffectPayloadJSON),
		effect.EvidenceJSON, effect.SourceTurn, effect.StartClockJSON,
		d1NullableString(effect.DurationJSON), d1NullableString(effect.ExpiresAtClockJSON), state, now, now).
		Scan(&id); err != nil {
		return effect, err
	}
	effect.ID = id
	effect.EffectState = state
	effect.CreatedAt = parseD1TimeOrZero(now)
	effect.UpdatedAt = effect.CreatedAt
	return effect, nil
}

// UpdateStatusEffectState records an effect transition. A cleared evidence value
// or a zero cleared turn is stored as NULL.
func (s *d1Store) UpdateStatusEffectState(ctx context.Context, id int64, effectState, clearedEvidenceJSON string, clearedTurn int) error {
	affected, err := s.conn.Exec(ctx, `
		UPDATE status_effects
		SET effect_state = ?, cleared_evidence_json = NULLIF(?, ''), cleared_turn = NULLIF(?, 0),
		    updated_at = `+d1NowExpression+`
		WHERE id = ?`,
		strings.TrimSpace(effectState), strings.TrimSpace(clearedEvidenceJSON), clearedTurn, id)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}
