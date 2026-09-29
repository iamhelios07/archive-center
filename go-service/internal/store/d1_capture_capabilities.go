package store

import (
	"context"
	"errors"
	"strings"
	"time"
)

// D1 capture-verification capability.
//
// This is the per-turn capture integrity ledger. Its update path is not a plain
// status write: repair success may not be recorded without evidence, a degraded
// state must carry a reason, and losing the user's input is only acceptable in a
// degraded state. Those invariants are enforced here exactly as the MariaDB path
// enforces them, because a silently "repaired" record would hide a lost turn.

var _ CaptureVerificationStore = (*d1Store)(nil)

// d1NullableTime renders a zero time as NULL, matching MariaDB's
// NULLIF(..., '0000-00-00 00:00:00.000') treatment of an unset repair time.
func d1NullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(d1TimeLayout)
}

// d1NowExpression is the inline timestamp expression used where the MariaDB path
// relies on CURRENT_TIMESTAMP(3). SQLite has no equivalent clause, so the
// RFC3339 form the schema default also uses is rendered explicitly.
const d1NowExpression = `strftime('%Y-%m-%dT%H:%M:%fZ','now')`

const d1CaptureVerificationSelect = `
		SELECT id, chat_session_id, turn_index, stage_name, verification_state, degraded_reason,
		       compact_metadata_json, content_hash, evidence_json, previous_record_id, repaired_by_record_id,
		       repair_attempt_count, repair_evidence_json, repaired_at, user_input_preserved, payload_rewrite,
		       created_at, updated_at
		FROM capture_verification_records`

func (s *d1Store) ListCaptureVerifications(ctx context.Context, chatSessionID string, limit int) ([]CaptureVerificationRecord, error) {
	rows, err := s.conn.Query(ctx, d1CaptureVerificationSelect+`
		WHERE chat_session_id = ?
		ORDER BY updated_at DESC, id DESC
		LIMIT ?`, chatSessionID, d1ClampListLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CaptureVerificationRecord
	for rows.Next() {
		var item CaptureVerificationRecord
		var degradedReason, compactMetadata, contentHash, evidenceJSON, repairEvidence *string
		var previousID, repairedByID *int64
		var repairedAt *time.Time
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.TurnIndex, &item.StageName, &item.VerificationState, &degradedReason,
			&compactMetadata, &contentHash, &evidenceJSON, &previousID, &repairedByID,
			&item.RepairAttemptCount, &repairEvidence, &repairedAt, &item.UserInputPreserved, &item.PayloadRewrite,
			&item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, err
		}
		item.DegradedReason = d1DerefString(degradedReason)
		item.CompactMetadataJSON = d1DerefString(compactMetadata)
		item.ContentHash = d1DerefString(contentHash)
		item.EvidenceJSON = d1DerefString(evidenceJSON)
		item.PreviousRecordID = d1DerefInt64(previousID)
		item.RepairedByRecordID = d1DerefInt64(repairedByID)
		item.RepairEvidenceJSON = d1DerefString(repairEvidence)
		if repairedAt != nil {
			item.RepairedAt = *repairedAt
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) SaveCaptureVerification(ctx context.Context, record CaptureVerificationRecord) (CaptureVerificationRecord, error) {
	now := d1TimeValue(record.CreatedAt)
	stageName := strings.TrimSpace(record.StageName)
	if stageName == "" {
		stageName = "afterRequest"
	}
	verificationState := strings.TrimSpace(record.VerificationState)
	if verificationState == "" {
		verificationState = "single-stage"
	}

	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO capture_verification_records (
			chat_session_id, turn_index, stage_name, verification_state, degraded_reason,
			compact_metadata_json, content_hash, evidence_json, previous_record_id, repaired_by_record_id,
			repair_attempt_count, repair_evidence_json, repaired_at, user_input_preserved, payload_rewrite,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, 0), NULLIF(?, 0), ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		record.ChatSessionID, record.TurnIndex, stageName, verificationState,
		d1NullableString(record.DegradedReason), d1NullableString(record.CompactMetadataJSON),
		d1NullableString(record.ContentHash), d1NullableString(record.EvidenceJSON),
		record.PreviousRecordID, record.RepairedByRecordID, record.RepairAttemptCount,
		d1NullableString(record.RepairEvidenceJSON), d1NullableTime(record.RepairedAt),
		d1BoolValue(record.UserInputPreserved), d1BoolValue(record.PayloadRewrite), now, now).Scan(&id); err != nil {
		return record, err
	}
	record.ID = id
	record.StageName = stageName
	record.VerificationState = verificationState
	record.CreatedAt = parseD1TimeOrZero(now)
	record.UpdatedAt = record.CreatedAt
	return record, nil
}

func (s *d1Store) UpdateCaptureVerificationRepair(ctx context.Context, id int64, state, degradedReason, repairEvidenceJSON string, repairedByID int64, userInputPreserved bool) error {
	state = strings.TrimSpace(state)
	if state == "" {
		return errors.New("state is required")
	}
	// Do not mark repair success without evidence.
	if state == "verified" || state == "verified-final" {
		if strings.TrimSpace(repairEvidenceJSON) == "" {
			return errors.New("repair success state requires repair_evidence_json")
		}
	}
	if state == "degraded" && strings.TrimSpace(degradedReason) == "" {
		return errors.New("degraded state requires degraded_reason")
	}
	if !userInputPreserved && state != "degraded" {
		return errors.New("user_input_preserved=false requires degraded verification_state")
	}

	sql := `UPDATE capture_verification_records
		SET verification_state = ?, degraded_reason = NULLIF(?, ''), repair_evidence_json = NULLIF(?, ''),
		    repaired_by_record_id = NULLIF(?, 0), user_input_preserved = ?, updated_at = ` + d1NowExpression
	args := []any{state, degradedReason, repairEvidenceJSON, repairedByID, d1BoolValue(userInputPreserved)}
	if state == "verified" || state == "verified-final" || strings.TrimSpace(repairEvidenceJSON) != "" {
		sql += `, repaired_at = ` + d1NowExpression
	}
	if strings.TrimSpace(repairEvidenceJSON) != "" {
		sql += `, repair_attempt_count = repair_attempt_count + 1`
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
