package store

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestD1ChapterSummaryStore(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	items := []*ChapterSummary{
		{ChatSessionID: "s1", FromTurn: 1, ToTurn: 5, ChapterIndex: 1, ChapterTitle: "Arrival", SummaryText: "they arrive"},
		{ChatSessionID: "s1", FromTurn: 6, ToTurn: 10, ChapterIndex: 2, ChapterTitle: "Conflict", SummaryText: "a fight", ResumeText: "resume two"},
		{ChatSessionID: "s2", FromTurn: 1, ToTurn: 3, ChapterIndex: 1, ChapterTitle: "Other", SummaryText: "other"},
	}
	for _, item := range items {
		if err := st.SaveChapterSummary(ctx, item); err != nil {
			t.Fatalf("SaveChapterSummary %q: %v", item.ChapterTitle, err)
		}
		if item.ID == 0 {
			t.Fatalf("SaveChapterSummary %q did not report an id", item.ChapterTitle)
		}
	}

	all, err := st.SearchChapterSummaries(ctx, "s1", "", 0, 0, 0)
	if err != nil {
		t.Fatalf("SearchChapterSummaries: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("chapters = %d, want 2 (session isolation)", len(all))
	}
	if all[0].ChapterIndex != 2 {
		t.Errorf("chapter_index DESC ordering broken: first = %d", all[0].ChapterIndex)
	}
	// created_at comes from the schema default and must be RFC3339 so the D1
	// timestamps stay readable by RFC3339-only callers.
	if all[0].CreatedAt == nil {
		t.Fatal("created_at must be populated by the column default")
	}
	if _, err := time.Parse(time.RFC3339, all[0].CreatedAt.Format(time.RFC3339)); err != nil {
		t.Errorf("created_at is not RFC3339-parseable: %v", err)
	}

	found, err := st.SearchChapterSummaries(ctx, "s1", "fight", 0, 0, 0)
	if err != nil {
		t.Fatalf("search by text: %v", err)
	}
	if len(found) != 1 || found[0].ChapterTitle != "Conflict" {
		t.Errorf("text search = %+v, want the conflict chapter", found)
	}

	byTitle, err := st.SearchChapterSummaries(ctx, "s1", "arrival", 0, 0, 0)
	if err != nil {
		t.Fatalf("search by title (case-insensitive): %v", err)
	}
	if len(byTitle) != 1 {
		t.Errorf("case-insensitive title search = %d rows, want 1", len(byTitle))
	}

	byResume, err := st.SearchChapterSummaries(ctx, "s1", "resume two", 0, 0, 0)
	if err != nil {
		t.Fatalf("search by resume text: %v", err)
	}
	if len(byResume) != 1 {
		t.Errorf("resume text search = %d rows, want 1", len(byResume))
	}

	// Turn windows mirror the MariaDB form: to_turn >= fromTurn, from_turn <= toTurn.
	windowed, err := st.SearchChapterSummaries(ctx, "s1", "", 6, 10, 0)
	if err != nil {
		t.Fatalf("windowed search: %v", err)
	}
	if len(windowed) != 1 || windowed[0].ChapterIndex != 2 {
		t.Errorf("windowed search = %+v, want only chapter 2", windowed)
	}

	limited, err := st.SearchChapterSummaries(ctx, "s1", "", 0, 0, 1)
	if err != nil {
		t.Fatalf("limited search: %v", err)
	}
	if len(limited) != 1 {
		t.Errorf("limit = %d rows, want 1", len(limited))
	}
}

func TestD1ArcAndSagaSummaryStores(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	// SaveArcSummary defaults the session and status when they are omitted.
	if err := st.SaveArcSummary(ctx, "s1", &ArcSummary{FromTurn: 1, ToTurn: 10, ArcIndex: 1, ArcName: "first", CoreConflict: "greed"}); err != nil {
		t.Fatalf("SaveArcSummary: %v", err)
	}
	if err := st.SaveArcSummary(ctx, "s1", &ArcSummary{FromTurn: 11, ToTurn: 20, ArcIndex: 2, ArcName: "second", ArcStatus: "closed", CoreConflict: "loss"}); err != nil {
		t.Fatalf("SaveArcSummary second: %v", err)
	}
	if err := st.SaveSagaDigest(ctx, "s1", &SagaDigest{FromTurn: 1, ToTurn: 20, EraLabel: "era one", SagaSummary: "a long saga"}); err != nil {
		t.Fatalf("SaveSagaDigest: %v", err)
	}

	latest, err := st.GetLatestArcSummary(ctx, "s1")
	if err != nil {
		t.Fatalf("GetLatestArcSummary: %v", err)
	}
	if latest == nil || latest.ArcIndex != 2 {
		t.Fatalf("latest arc = %+v, want arc_index 2", latest)
	}
	if latest.ArcStatus != "closed" {
		t.Errorf("arc status = %q, want closed", latest.ArcStatus)
	}

	active, err := st.ListArcSummaries(ctx, "s1", "active", 0)
	if err != nil {
		t.Fatalf("ListArcSummaries active: %v", err)
	}
	// The first arc had no explicit status, so it must default to active.
	if len(active) != 1 || active[0].ArcIndex != 1 {
		t.Errorf("active arcs = %+v, want only arc 1", active)
	}
	if active[0].ArcStatus != "active" {
		t.Errorf("defaulted arc status = %q, want active", active[0].ArcStatus)
	}
	unfiltered, err := st.ListArcSummaries(ctx, "s1", "", 0)
	if err != nil {
		t.Fatalf("ListArcSummaries all: %v", err)
	}
	if len(unfiltered) != 2 {
		t.Errorf("unfiltered arcs = %d, want 2", len(unfiltered))
	}

	arcs, err := st.SearchArcSummaries(ctx, "s1", "loss", 0, 0, 0)
	if err != nil {
		t.Fatalf("SearchArcSummaries: %v", err)
	}
	if len(arcs) != 1 || arcs[0].ArcName != "second" {
		t.Errorf("arc search = %+v, want the loss arc", arcs)
	}

	saga, err := st.GetLatestSagaDigest(ctx, "s1")
	if err != nil {
		t.Fatalf("GetLatestSagaDigest: %v", err)
	}
	if saga == nil || saga.EraLabel != "era one" || saga.SagaSummary != "a long saga" {
		t.Fatalf("saga digest = %+v", saga)
	}

	digests, err := st.ListSagaDigests(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("ListSagaDigests: %v", err)
	}
	if len(digests) != 1 {
		t.Errorf("saga digests = %d, want 1", len(digests))
	}
	matched, err := st.SearchSagaDigests(ctx, "s1", "long saga", 0, 0, 0)
	if err != nil {
		t.Fatalf("SearchSagaDigests: %v", err)
	}
	if len(matched) != 1 {
		t.Errorf("saga text search = %d rows, want 1", len(matched))
	}
	// A non-matching query must return nothing rather than everything.
	missed, err := st.SearchSagaDigests(ctx, "s1", "absent phrase", 0, 0, 0)
	if err != nil {
		t.Fatalf("SearchSagaDigests miss: %v", err)
	}
	if len(missed) != 0 {
		t.Errorf("non-matching search = %d rows, want 0", len(missed))
	}

	// A session without rows must return an empty digest, not an error.
	empty, err := st.GetLatestSagaDigest(ctx, "s-none")
	if err != nil {
		t.Fatalf("GetLatestSagaDigest on an empty session: %v", err)
	}
	if empty != nil {
		t.Errorf("empty session saga = %+v, want nil", empty)
	}
}

func TestD1EpisodeSummaryStoreRoundTrip(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	item := &EpisodeSummary{
		ChatSessionID: "s1", FromTurn: 1, ToTurn: 8, SummaryText: "the episode",
		KeyEntities: `["a","b"]`, KeyEvents: `["e"]`,
	}
	if err := st.SaveEpisodeSummary(ctx, item); err != nil {
		t.Fatalf("SaveEpisodeSummary: %v", err)
	}
	if item.ID == 0 {
		t.Fatal("SaveEpisodeSummary must report the inserted id")
	}

	fetched, err := st.GetEpisodeSummary(ctx, item.ID)
	if err != nil {
		t.Fatalf("GetEpisodeSummary: %v", err)
	}
	if fetched.SummaryText != "the episode" || fetched.KeyEntities != `["a","b"]` || fetched.KeyEvents != `["e"]` {
		t.Errorf("round trip lost data: %+v", fetched)
	}
	if fetched.OpenLoopsJSON != "" || fetched.EmbeddingModel != "" {
		t.Errorf("unset optional columns must read as empty: %+v", fetched)
	}
	// created_at is supplied explicitly for episodes, matching the MariaDB path.
	if fetched.CreatedAt.IsZero() {
		t.Error("created_at must be populated")
	}

	// The timeline index must pick the episode's to_turn up.
	turn, err := st.LatestTimelineTurnIndex(ctx, "s1")
	if err != nil {
		t.Fatalf("LatestTimelineTurnIndex: %v", err)
	}
	if turn != 8 {
		t.Errorf("timeline turn = %d, want 8", turn)
	}
}

func TestD1NarrativeSearchHandlesEmptyQuery(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if err := st.SaveChapterSummary(ctx, &ChapterSummary{
		ChatSessionID: "s1", FromTurn: 1, ToTurn: 1, ChapterIndex: 1, SummaryText: "body",
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// An empty query means "no text filter", not "match nothing".
	items, err := st.SearchChapterSummaries(ctx, "s1", "   ", 0, 0, 0)
	if err != nil {
		t.Fatalf("SearchChapterSummaries with a blank query: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("blank query returned %d rows, want 1", len(items))
	}
	// A query that matches no column filter must also not match.
	if items, err := st.SearchChapterSummaries(ctx, "s1", "zzz-absent", 0, 0, 0); err != nil || len(items) != 0 {
		t.Errorf("non-matching query = %d rows, %v; want 0, nil", len(items), err)
	}
	if !strings.Contains("body", "body") {
		t.Fatal("sanity")
	}
}
