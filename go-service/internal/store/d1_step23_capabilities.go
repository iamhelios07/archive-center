package store

import (
	"context"
	"errors"
	"strings"
)

// D1 Step-23 support-only capabilities: consequence records, psychology
// branches, and theme/offscreen carries.
//
// They share one shape, so they are implemented together: a bounded list ordered
// by recency, an insert that defaults an empty status and maps a zero turn to
// NULL, and a status update that reports ErrNotFound when no row matched.
//
// These tables are also the ones the no-op store advertises, which is why a route
// could previously believe the capability existed while nothing persisted.

var _ ConsequenceRecordStore = (*d1Store)(nil)
var _ PsychologyBranchStore = (*d1Store)(nil)
var _ ThemeOffscreenCarryStore = (*d1Store)(nil)

// d1ClampListLimit mirrors the MariaDB list bounds: a non-positive limit becomes
// the default and an oversized one is capped.
func d1ClampListLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

// d1RequireStatus normalises an update status and rejects an empty one, matching
// the MariaDB guard.
func d1RequireStatus(status string) (string, error) {
	trimmed := strings.TrimSpace(status)
	if trimmed == "" {
		return "", errors.New("status is required")
	}
	return trimmed, nil
}

// ---------------------------------------------------------------------------
// consequence records
// ---------------------------------------------------------------------------

const d1ConsequenceRecordSelect = `
		SELECT id, chat_session_id, source_turn_start, source_turn_end, decision, immediate_result,
		       delayed_effect, affected_relations, affected_world, status, importance, confidence,
		       foreground_eligible, quiet_turns, last_seen_turn, paid_turn, expires_after_quiet_turns,
		       source_hash, evidence_json, created_at, updated_at
		FROM consequence_records`

func (s *d1Store) ListConsequenceRecords(ctx context.Context, chatSessionID string, limit int) ([]ConsequenceRecord, error) {
	rows, err := s.conn.Query(ctx, d1ConsequenceRecordSelect+`
		WHERE chat_session_id = ?
		ORDER BY updated_at DESC, id DESC
		LIMIT ?`, chatSessionID, d1ClampListLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ConsequenceRecord
	for rows.Next() {
		var item ConsequenceRecord
		var affectedRelations, affectedWorld, sourceHash, evidenceJSON *string
		var lastSeenTurn, paidTurn *int64
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.SourceTurnStart, &item.SourceTurnEnd,
			&item.Decision, &item.ImmediateResult, &item.DelayedEffect,
			&affectedRelations, &affectedWorld, &item.Status, &item.Importance, &item.Confidence,
			&item.ForegroundEligible, &item.QuietTurns, &lastSeenTurn, &paidTurn, &item.ExpiresAfterQuietTurns,
			&sourceHash, &evidenceJSON, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.AffectedRelationsJSON = d1DerefString(affectedRelations)
		item.AffectedWorldJSON = d1DerefString(affectedWorld)
		item.SourceHash = d1DerefString(sourceHash)
		item.EvidenceJSON = d1DerefString(evidenceJSON)
		item.LastSeenTurn = int(d1DerefInt64(lastSeenTurn))
		item.PaidTurn = int(d1DerefInt64(paidTurn))
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) SaveConsequenceRecord(ctx context.Context, record ConsequenceRecord) (ConsequenceRecord, error) {
	now := d1TimeValue(record.CreatedAt)
	status := strings.TrimSpace(record.Status)
	if status == "" {
		status = "active"
	}

	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO consequence_records (
			chat_session_id, source_turn_start, source_turn_end, decision, immediate_result,
			delayed_effect, affected_relations, affected_world, status, importance, confidence,
			foreground_eligible, quiet_turns, last_seen_turn, paid_turn, expires_after_quiet_turns,
			source_hash, evidence_json, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), NULLIF(?, 0), ?, ?, ?, ?, ?)
		RETURNING id`,
		record.ChatSessionID, record.SourceTurnStart, record.SourceTurnEnd, record.Decision,
		record.ImmediateResult, record.DelayedEffect, d1NullableString(record.AffectedRelationsJSON),
		d1NullableString(record.AffectedWorldJSON), status, record.Importance, record.Confidence,
		d1BoolValue(record.ForegroundEligible), record.QuietTurns, record.LastSeenTurn, record.PaidTurn,
		record.ExpiresAfterQuietTurns, d1NullableString(record.SourceHash),
		d1NullableString(record.EvidenceJSON), now, now).Scan(&id); err != nil {
		return record, err
	}
	record.ID = id
	// The returned record mirrors the MariaDB return values, which report the
	// attempted timestamps rather than re-reading the row.
	record.CreatedAt = parseD1TimeOrZero(now)
	record.UpdatedAt = record.CreatedAt
	record.Status = status
	return record, nil
}

func (s *d1Store) UpdateConsequenceRecordStatus(ctx context.Context, id int64, status string, paidTurn int) error {
	trimmed, err := d1RequireStatus(status)
	if err != nil {
		return err
	}
	sql := `UPDATE consequence_records SET status = ?`
	args := []any{trimmed}
	if paidTurn > 0 {
		sql += `, paid_turn = NULLIF(?, 0)`
		args = append(args, paidTurn)
	}
	sql += ` WHERE id = ?`
	args = append(args, id)

	affected, err := s.conn.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------------------------------------------------------------------------
// psychology branches
// ---------------------------------------------------------------------------

const d1PsychologyBranchSelect = `
		SELECT id, chat_session_id, character_name, branch_type, axis_name, summary, status,
		       confidence, confidence_label, source_kind, source_turn_start, source_turn_end,
		       source_hash, evidence_json, quiet_turns, last_seen_turn, dormant_after_quiet_turns,
		       created_at, updated_at
		FROM psychology_branches`

func (s *d1Store) ListPsychologyBranches(ctx context.Context, chatSessionID string, limit int) ([]PsychologyBranch, error) {
	rows, err := s.conn.Query(ctx, d1PsychologyBranchSelect+`
		WHERE chat_session_id = ?
		ORDER BY updated_at DESC, id DESC
		LIMIT ?`, chatSessionID, d1ClampListLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PsychologyBranch
	for rows.Next() {
		var item PsychologyBranch
		var confidenceLabel, sourceKind, sourceHash, evidenceJSON *string
		var lastSeenTurn *int64
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.CharacterName, &item.BranchType, &item.AxisName,
			&item.Summary, &item.Status, &item.Confidence, &confidenceLabel, &sourceKind,
			&item.SourceTurnStart, &item.SourceTurnEnd, &sourceHash, &evidenceJSON,
			&item.QuietTurns, &lastSeenTurn, &item.DormantAfterQuietTurns, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.ConfidenceLabel = d1DerefString(confidenceLabel)
		item.SourceKind = d1DerefString(sourceKind)
		item.SourceHash = d1DerefString(sourceHash)
		item.EvidenceJSON = d1DerefString(evidenceJSON)
		item.LastSeenTurn = int(d1DerefInt64(lastSeenTurn))
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) SavePsychologyBranch(ctx context.Context, branch PsychologyBranch) (PsychologyBranch, error) {
	now := d1TimeValue(branch.CreatedAt)
	status := strings.TrimSpace(branch.Status)
	if status == "" {
		status = "active"
	}

	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO psychology_branches (
			chat_session_id, character_name, branch_type, axis_name, summary, status,
			confidence, confidence_label, source_kind, source_turn_start, source_turn_end,
			source_hash, evidence_json, quiet_turns, last_seen_turn, dormant_after_quiet_turns,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), ?, ?, ?)
		RETURNING id`,
		branch.ChatSessionID, branch.CharacterName, branch.BranchType, branch.AxisName, branch.Summary,
		status, branch.Confidence, d1NullableString(branch.ConfidenceLabel),
		d1NullableString(branch.SourceKind), branch.SourceTurnStart, branch.SourceTurnEnd,
		d1NullableString(branch.SourceHash), d1NullableString(branch.EvidenceJSON), branch.QuietTurns,
		branch.LastSeenTurn, branch.DormantAfterQuietTurns, now, now).Scan(&id); err != nil {
		return branch, err
	}
	branch.ID = id
	branch.CreatedAt = parseD1TimeOrZero(now)
	branch.UpdatedAt = branch.CreatedAt
	branch.Status = status
	return branch, nil
}

func (s *d1Store) UpdatePsychologyBranchStatus(ctx context.Context, id int64, status string, quietTurns int) error {
	trimmed, err := d1RequireStatus(status)
	if err != nil {
		return err
	}
	affected, err := s.conn.Exec(ctx, `
		UPDATE psychology_branches SET status = ?, quiet_turns = ? WHERE id = ?`,
		trimmed, quietTurns, id)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------------------------------------------------------------------------
// theme and offscreen carries
// ---------------------------------------------------------------------------

const d1ThemeOffscreenCarrySelect = `
		SELECT id, chat_session_id, surface_type, label, summary, status, confidence,
		       confidence_label, source_kind, source_turn_start, source_turn_end,
		       source_hash, evidence_json, quiet_turns, last_seen_turn,
		       dormant_after_quiet_turns, foreground_eligible, foreground_reason_json,
		       created_at, updated_at
		FROM theme_offscreen_carries`

func (s *d1Store) ListThemeOffscreenCarries(ctx context.Context, chatSessionID, surfaceType string, limit int) ([]ThemeOffscreenCarryRecord, error) {
	// Foreground-eligible carries lead, then recency.
	rows, err := s.conn.Query(ctx, d1ThemeOffscreenCarrySelect+`
		WHERE chat_session_id = ? AND (? = '' OR surface_type = ?)
		ORDER BY foreground_eligible DESC, updated_at DESC, id DESC
		LIMIT ?`, chatSessionID, surfaceType, surfaceType, d1ClampListLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ThemeOffscreenCarryRecord
	for rows.Next() {
		var item ThemeOffscreenCarryRecord
		var confidenceLabel, sourceKind, sourceHash, evidenceJSON, foregroundReasonJSON *string
		var lastSeenTurn *int64
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.SurfaceType, &item.Label, &item.Summary,
			&item.Status, &item.Confidence, &confidenceLabel, &sourceKind,
			&item.SourceTurnStart, &item.SourceTurnEnd, &sourceHash, &evidenceJSON,
			&item.QuietTurns, &lastSeenTurn, &item.DormantAfterQuietTurns, &item.ForegroundEligible,
			&foregroundReasonJSON, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.ConfidenceLabel = d1DerefString(confidenceLabel)
		item.SourceKind = d1DerefString(sourceKind)
		item.SourceHash = d1DerefString(sourceHash)
		item.EvidenceJSON = d1DerefString(evidenceJSON)
		item.LastSeenTurn = int(d1DerefInt64(lastSeenTurn))
		item.ForegroundReasonJSON = d1DerefString(foregroundReasonJSON)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) SaveThemeOffscreenCarry(ctx context.Context, record ThemeOffscreenCarryRecord) (ThemeOffscreenCarryRecord, error) {
	now := d1TimeValue(record.CreatedAt)
	status := strings.TrimSpace(record.Status)
	if status == "" {
		status = "active"
	}

	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO theme_offscreen_carries (
			chat_session_id, surface_type, label, summary, status, confidence,
			confidence_label, source_kind, source_turn_start, source_turn_end,
			source_hash, evidence_json, quiet_turns, last_seen_turn,
			dormant_after_quiet_turns, foreground_eligible, foreground_reason_json,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), ?, ?, ?, ?, ?)
		RETURNING id`,
		record.ChatSessionID, record.SurfaceType, record.Label, record.Summary,
		status, record.Confidence, d1NullableString(record.ConfidenceLabel),
		d1NullableString(record.SourceKind), record.SourceTurnStart, record.SourceTurnEnd,
		d1NullableString(record.SourceHash), d1NullableString(record.EvidenceJSON), record.QuietTurns,
		record.LastSeenTurn, record.DormantAfterQuietTurns, d1BoolValue(record.ForegroundEligible),
		d1NullableString(record.ForegroundReasonJSON), now, now).Scan(&id); err != nil {
		return record, err
	}
	record.ID = id
	record.CreatedAt = parseD1TimeOrZero(now)
	record.UpdatedAt = record.CreatedAt
	record.Status = status
	return record, nil
}

func (s *d1Store) UpdateThemeOffscreenCarryStatus(ctx context.Context, id int64, status string, quietTurns int) error {
	trimmed, err := d1RequireStatus(status)
	if err != nil {
		return err
	}
	affected, err := s.conn.Exec(ctx, `
		UPDATE theme_offscreen_carries SET status = ?, quiet_turns = ? WHERE id = ?`,
		trimmed, quietTurns, id)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}
