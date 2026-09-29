package store

import (
	"context"
	"errors"
	"testing"
)

func TestD1CaptureVerificationSaveDefaultsAndList(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	saved, err := st.SaveCaptureVerification(ctx, CaptureVerificationRecord{
		ChatSessionID: "s1", TurnIndex: 3, ContentHash: "abc",
	})
	if err != nil {
		t.Fatalf("SaveCaptureVerification: %v", err)
	}
	if saved.ID == 0 {
		t.Fatal("save must report an id")
	}
	if saved.StageName != "afterRequest" {
		t.Errorf("stage_name = %q, want the default afterRequest", saved.StageName)
	}
	if saved.VerificationState != "single-stage" {
		t.Errorf("verification_state = %q, want the default single-stage", saved.VerificationState)
	}

	// Zero record references and an unset repair time must be NULL, not 0 or the
	// zero timestamp.
	var previousID, repairedByID *int64
	var repairedAt *string
	if err := conn.QueryRow(ctx, `SELECT previous_record_id, repaired_by_record_id, repaired_at
		FROM capture_verification_records WHERE id = ?`, saved.ID).Scan(&previousID, &repairedByID, &repairedAt); err != nil {
		t.Fatalf("read record refs: %v", err)
	}
	if previousID != nil || repairedByID != nil || repairedAt != nil {
		t.Errorf("zero refs and unset repair time must be NULL, got %v/%v/%v", previousID, repairedByID, repairedAt)
	}

	if _, err := st.SaveCaptureVerification(ctx, CaptureVerificationRecord{
		ChatSessionID: "s2", TurnIndex: 1,
	}); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	rows, err := st.ListCaptureVerifications(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("ListCaptureVerifications: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1 (session isolation)", len(rows))
	}
	if rows[0].ContentHash != "abc" {
		t.Errorf("content_hash = %q, want abc", rows[0].ContentHash)
	}
	if !rows[0].RepairedAt.IsZero() {
		t.Errorf("an unset repaired_at must read as the zero time, got %v", rows[0].RepairedAt)
	}
}

// TestD1CaptureVerificationRepairInvariants pins the guard rails: a silently
// "repaired" record would hide a lost turn, so each rule is enforced.
func TestD1CaptureVerificationRepairInvariants(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	saved, err := st.SaveCaptureVerification(ctx, CaptureVerificationRecord{ChatSessionID: "s1", TurnIndex: 1})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Repair success without evidence.
	if err := st.UpdateCaptureVerificationRepair(ctx, saved.ID, "verified", "", "", 0, true); err == nil {
		t.Error("a verified state without repair evidence must be rejected")
	}
	if err := st.UpdateCaptureVerificationRepair(ctx, saved.ID, "verified-final", "", "", 0, true); err == nil {
		t.Error("a verified-final state without repair evidence must be rejected")
	}
	// A degraded state must explain itself.
	if err := st.UpdateCaptureVerificationRepair(ctx, saved.ID, "degraded", "  ", "", 0, true); err == nil {
		t.Error("a degraded state without a reason must be rejected")
	}
	// Losing the user's input is only acceptable when degraded.
	if err := st.UpdateCaptureVerificationRepair(ctx, saved.ID, "single-stage", "", "", 0, false); err == nil {
		t.Error("user_input_preserved=false must require a degraded state")
	}
	if err := st.UpdateCaptureVerificationRepair(ctx, saved.ID, "  ", "", "", 0, true); err == nil {
		t.Error("an empty state must be rejected")
	}

	// A rejected update must not have changed anything.
	var state string
	var attemptCount int
	if err := conn.QueryRow(ctx, `SELECT verification_state, repair_attempt_count
		FROM capture_verification_records WHERE id = ?`, saved.ID).Scan(&state, &attemptCount); err != nil {
		t.Fatalf("read after rejects: %v", err)
	}
	if state != "single-stage" || attemptCount != 0 {
		t.Errorf("rejected updates must leave the row alone: state=%q attempts=%d", state, attemptCount)
	}

	// A valid repair records the evidence, the repair time, and one attempt.
	if err := st.UpdateCaptureVerificationRepair(ctx, saved.ID, "verified", "", `{"why":"hash matched"}`, saved.ID, true); err != nil {
		t.Fatalf("valid repair: %v", err)
	}
	var repairedAt *string
	var repairedBy *int64
	if err := conn.QueryRow(ctx, `SELECT verification_state, repair_attempt_count, repaired_at, repaired_by_record_id
		FROM capture_verification_records WHERE id = ?`, saved.ID).Scan(&state, &attemptCount, &repairedAt, &repairedBy); err != nil {
		t.Fatalf("read after repair: %v", err)
	}
	if state != "verified" {
		t.Errorf("state = %q, want verified", state)
	}
	if attemptCount != 1 {
		t.Errorf("repair_attempt_count = %d, want 1", attemptCount)
	}
	if repairedAt == nil {
		t.Error("a verified repair must stamp repaired_at")
	}
	if repairedBy == nil || *repairedBy != saved.ID {
		t.Errorf("repaired_by_record_id = %v, want %d", repairedBy, saved.ID)
	}

	// A second evidenced repair increments the attempt counter again.
	if err := st.UpdateCaptureVerificationRepair(ctx, saved.ID, "verified", "", `{"why":"second pass"}`, saved.ID, true); err != nil {
		t.Fatalf("second repair: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT repair_attempt_count FROM capture_verification_records WHERE id = ?`, saved.ID).
		Scan(&attemptCount); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	if attemptCount != 2 {
		t.Errorf("repair_attempt_count = %d, want 2", attemptCount)
	}

	// A degraded update with no evidence is allowed and must not count an attempt.
	capture, err := st.SaveCaptureVerification(ctx, CaptureVerificationRecord{ChatSessionID: "s1", TurnIndex: 2})
	if err != nil {
		t.Fatalf("seed second record: %v", err)
	}
	if err := st.UpdateCaptureVerificationRepair(ctx, capture.ID, "degraded", "stream interrupted", "", 0, false); err != nil {
		t.Fatalf("degraded update: %v", err)
	}
	var reason *string
	var preserved bool
	if err := conn.QueryRow(ctx, `SELECT degraded_reason, user_input_preserved, repair_attempt_count
		FROM capture_verification_records WHERE id = ?`, capture.ID).Scan(&reason, &preserved, &attemptCount); err != nil {
		t.Fatalf("read degraded row: %v", err)
	}
	if reason == nil || *reason != "stream interrupted" {
		t.Errorf("degraded_reason = %v, want stream interrupted", reason)
	}
	if preserved {
		t.Error("user_input_preserved must be stored as false")
	}
	if attemptCount != 0 {
		t.Errorf("an unevidenced update must not count a repair attempt, got %d", attemptCount)
	}

	if err := st.UpdateCaptureVerificationRepair(ctx, 999999, "degraded", "reason", "", 0, true); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing record = %v, want ErrNotFound", err)
	}
}
