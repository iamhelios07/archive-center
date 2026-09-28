package store

import (
	"context"
	"errors"
	"testing"
)

func TestD1ConsequenceRecordStore(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	first, err := st.SaveConsequenceRecord(ctx, ConsequenceRecord{
		ChatSessionID: "s1", SourceTurnStart: 1, SourceTurnEnd: 2,
		Decision: "open the gate", ImmediateResult: "it opens",
	})
	if err != nil {
		t.Fatalf("SaveConsequenceRecord: %v", err)
	}
	if first.ID == 0 {
		t.Fatal("save must report an id")
	}
	if first.Status != "active" {
		t.Errorf("status = %q, want the default active", first.Status)
	}
	if first.CreatedAt.IsZero() || first.UpdatedAt.IsZero() {
		t.Error("the returned record must carry timestamps")
	}

	// A zero turn must be stored as NULL rather than as 0.
	var lastSeen, paid *int64
	if err := conn.QueryRow(ctx, `SELECT last_seen_turn, paid_turn FROM consequence_records WHERE id = ?`, first.ID).
		Scan(&lastSeen, &paid); err != nil {
		t.Fatalf("read turns: %v", err)
	}
	if lastSeen != nil || paid != nil {
		t.Errorf("zero turns must be NULL, got last_seen=%v paid=%v", lastSeen, paid)
	}

	second, err := st.SaveConsequenceRecord(ctx, ConsequenceRecord{
		ChatSessionID: "s1", SourceTurnStart: 3, SourceTurnEnd: 4,
		Decision: "close it", ImmediateResult: "it closes", Status: "paid", LastSeenTurn: 9,
	})
	if err != nil {
		t.Fatalf("second save: %v", err)
	}
	if second.Status != "paid" {
		t.Errorf("explicit status = %q, want paid", second.Status)
	}
	if _, err := st.SaveConsequenceRecord(ctx, ConsequenceRecord{
		ChatSessionID: "s2", SourceTurnStart: 1, SourceTurnEnd: 1, Decision: "d", ImmediateResult: "r",
	}); err != nil {
		t.Fatalf("seed other session: %v", err)
	}

	rows, err := st.ListConsequenceRecords(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("ListConsequenceRecords: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (session isolation)", len(rows))
	}
	// Recency first; both were written in the same millisecond, so id DESC decides.
	if rows[0].ID != second.ID {
		t.Errorf("first row id = %d, want the newest %d", rows[0].ID, second.ID)
	}

	limited, err := st.ListConsequenceRecords(ctx, "s1", 1)
	if err != nil {
		t.Fatalf("limited list: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("limit 1 returned %d rows", len(limited))
	}

	if err := st.UpdateConsequenceRecordStatus(ctx, first.ID, "expired", 7); err != nil {
		t.Fatalf("UpdateConsequenceRecordStatus: %v", err)
	}
	var status string
	var paidTurn int64
	if err := conn.QueryRow(ctx, `SELECT status, paid_turn FROM consequence_records WHERE id = ?`, first.ID).
		Scan(&status, &paidTurn); err != nil {
		t.Fatalf("read updated row: %v", err)
	}
	if status != "expired" || paidTurn != 7 {
		t.Errorf("after update status=%q paid_turn=%d, want expired/7", status, paidTurn)
	}

	if err := st.UpdateConsequenceRecordStatus(ctx, first.ID, "   ", 0); err == nil {
		t.Error("an empty status must be rejected")
	}
	if err := st.UpdateConsequenceRecordStatus(ctx, 999999, "expired", 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("updating a missing row = %v, want ErrNotFound", err)
	}
}

func TestD1PsychologyBranchStore(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	branch, err := st.SavePsychologyBranch(ctx, PsychologyBranch{
		ChatSessionID: "s1", CharacterName: "hero", BranchType: "doubt", AxisName: "trust",
		Summary: "hesitates", Confidence: 0.6, SourceTurnStart: 1, SourceTurnEnd: 2,
		DormantAfterQuietTurns: 5,
	})
	if err != nil {
		t.Fatalf("SavePsychologyBranch: %v", err)
	}
	if branch.ID == 0 || branch.Status != "active" {
		t.Errorf("branch = %+v, want an id and the default active status", branch)
	}

	if _, err := st.SavePsychologyBranch(ctx, PsychologyBranch{
		ChatSessionID: "s1", CharacterName: "rival", BranchType: "resolve", AxisName: "pride",
		Summary: "steadies", Status: "dormant", SourceTurnStart: 3, SourceTurnEnd: 3,
	}); err != nil {
		t.Fatalf("second branch: %v", err)
	}

	rows, err := st.ListPsychologyBranches(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("ListPsychologyBranches: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("branches = %d, want 2", len(rows))
	}

	if err := st.UpdatePsychologyBranchStatus(ctx, branch.ID, "dormant", 12); err != nil {
		t.Fatalf("UpdatePsychologyBranchStatus: %v", err)
	}
	after, err := st.ListPsychologyBranches(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("list after update: %v", err)
	}
	for _, item := range after {
		if item.ID == branch.ID {
			if item.Status != "dormant" || item.QuietTurns != 12 {
				t.Errorf("updated branch = %+v, want dormant/12", item)
			}
		}
	}

	if err := st.UpdatePsychologyBranchStatus(ctx, 999999, "dormant", 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing row = %v, want ErrNotFound", err)
	}
}

func TestD1ThemeOffscreenCarryStore(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	background, err := st.SaveThemeOffscreenCarry(ctx, ThemeOffscreenCarryRecord{
		ChatSessionID: "s1", SurfaceType: "theme", Label: "loss", Summary: "keeps circling",
		Confidence: 0.4, SourceTurnStart: 1, SourceTurnEnd: 2,
	})
	if err != nil {
		t.Fatalf("SaveThemeOffscreenCarry: %v", err)
	}
	foreground, err := st.SaveThemeOffscreenCarry(ctx, ThemeOffscreenCarryRecord{
		ChatSessionID: "s1", SurfaceType: "offscreen", Label: "alliance", Summary: "forms elsewhere",
		Confidence: 0.8, SourceTurnStart: 2, SourceTurnEnd: 2, ForegroundEligible: true,
	})
	if err != nil {
		t.Fatalf("second carry: %v", err)
	}

	all, err := st.ListThemeOffscreenCarries(ctx, "s1", "", 0)
	if err != nil {
		t.Fatalf("ListThemeOffscreenCarries: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("carries = %d, want 2", len(all))
	}
	// Foreground-eligible carries lead regardless of recency.
	if all[0].ID != foreground.ID || !all[0].ForegroundEligible {
		t.Errorf("first carry = %+v, want the foreground-eligible one", all[0])
	}

	themed, err := st.ListThemeOffscreenCarries(ctx, "s1", "theme", 0)
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	if len(themed) != 1 || themed[0].ID != background.ID {
		t.Errorf("surface_type filter = %+v, want only the theme carry", themed)
	}
	none, err := st.ListThemeOffscreenCarries(ctx, "s1", "absent", 0)
	if err != nil {
		t.Fatalf("absent filter: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("absent surface_type = %d rows, want 0", len(none))
	}

	if err := st.UpdateThemeOffscreenCarryStatus(ctx, background.ID, "resolved", 3); err != nil {
		t.Fatalf("UpdateThemeOffscreenCarryStatus: %v", err)
	}
	if err := st.UpdateThemeOffscreenCarryStatus(ctx, 999999, "resolved", 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing row = %v, want ErrNotFound", err)
	}
	if err := st.UpdateThemeOffscreenCarryStatus(ctx, background.ID, "  ", 0); err == nil {
		t.Error("an empty status must be rejected")
	}
}
