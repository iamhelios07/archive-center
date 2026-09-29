package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// D1 lorebook reference tests.
//
// Every test here runs against the real SQLite engine through the same D1
// transport the Worker uses, so these assert the statements D1 actually
// executes rather than a mock's idea of them.
//
// The failure modes these pin are specific to this lane. Host lore is rendered
// verbatim to the player, so the three that matter are:
//
//   - a scope lookup that widens. lorebook_reference_scopes is keyed by a JSON
//     identity rather than by a column, so a lookup that picks the first
//     candidate row, or one that loses the NULL-safe comparison on the host
//     indices, answers with another module set's lore while looking correct.
//   - a lifecycle that promotes the wrong thing. A partial or unavailable
//     observation, or one that arrived out of order, must never become the
//     current catalog; only an explicitly complete observed snapshot may.
//   - a lost tri-state. "The Host did not expose alwaysActive" and "the Host
//     exposed alwaysActive:false" are different observations of the same book,
//     and collapsing them rewrites what the panel claims the host reported.

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

// d1LorebookRefPtr returns a pointer to value. The Host fields are tri-state
// pointers, and spelling out a local for each of them in every test buries the
// behaviour the test is about.
func d1LorebookRefPtr[T any](value T) *T { return &value }

// d1LorebookRefExactScope is the scope a complete observation addresses: a named
// character and chat, with an observed module set. The shared validator refuses
// a complete, consent-active, observed snapshot whose chat scope is incomplete,
// which is why the complete fixtures cannot use the aggregate scope below.
func d1LorebookRefExactScope(sessionID string, characterIndex, chatIndex int64, modules ...string) LorebookReferenceScope {
	return LorebookReferenceScope{
		ChatSessionID:          sessionID,
		CharacterIndex:         d1LorebookRefPtr(characterIndex),
		ChatIndex:              d1LorebookRefPtr(chatIndex),
		EnabledModuleIDs:       modules,
		EnabledModulesObserved: true,
	}
}

// d1LorebookRefAggregateScope is a scope whose indices the Host did not expose.
// It is the shape a partial, unavailable, or consent report carries, and the
// shape the route sends when its query string omits character_index and
// chat_index.
func d1LorebookRefAggregateScope(sessionID string, modules ...string) LorebookReferenceScope {
	return LorebookReferenceScope{
		ChatSessionID:          sessionID,
		EnabledModuleIDs:       modules,
		EnabledModulesObserved: true,
	}
}

// d1LorebookRefSnapshotID builds a distinct 32-character snapshot id, the width
// the observation ledger's key expects.
func d1LorebookRefSnapshotID(seed string) string {
	return strings.Repeat("0", 32-len(seed)) + seed
}

// d1LorebookRefComplete builds an observed, complete, consent-active snapshot:
// the only kind of observation that may replace the current projection.
func d1LorebookRefComplete(snapshotID string, scope LorebookReferenceScope, observedAt time.Time, entries ...LorebookReferenceEntryObservation) *LorebookReferenceSnapshot {
	return &LorebookReferenceSnapshot{
		SnapshotID: snapshotID, ContractVersion: LorebookReferenceSnapshotContractV1,
		ConsentState: LorebookConsentActive, ObservationState: LorebookObservationObserved,
		CompleteSnapshot: true,
		Scope:            scope, ProvenanceJSON: `{"source":"test"}`, ObservedAt: observedAt,
		Entries: entries,
	}
}

// d1LorebookRefPartial builds a partial observation: recorded in the ledger,
// never allowed to replace the current projection.
func d1LorebookRefPartial(snapshotID string, scope LorebookReferenceScope, observedAt time.Time, entries ...LorebookReferenceEntryObservation) *LorebookReferenceSnapshot {
	return &LorebookReferenceSnapshot{
		SnapshotID: snapshotID, ContractVersion: LorebookReferenceSnapshotContractV1,
		ConsentState: LorebookConsentActive, ObservationState: LorebookObservationPartial,
		Scope: scope, ProvenanceJSON: `{}`, ObservedAt: observedAt,
		Entries: entries,
	}
}

// d1LorebookRefEntry is one Host entry with the shape the panel renders.
func d1LorebookRefEntry(hostEntryID, key, content string) LorebookReferenceEntryObservation {
	return LorebookReferenceEntryObservation{HostEntryID: hostEntryID, Key: key, Content: content}
}

// d1LorebookRefStoredEntry is one row of lorebook_reference_entries, reduced to
// the columns a lifecycle assertion needs.
type d1LorebookRefStoredEntry struct {
	HostEntryID    string
	EntryOrdinal   int
	Normalized     string
	ContentHash    string
	LifecycleState string
	IsCurrent      bool
}

// d1LorebookRefStored reads the entry rows of one scope in insertion order, so a
// test can assert the lifecycle the writer chose rather than inferring it from
// the read projection, which only ever shows current rows.
func d1LorebookRefStored(t *testing.T, conn *sqliteD1Conn, scopeID int64) []d1LorebookRefStoredEntry {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT COALESCE(host_entry_id, ''), entry_ordinal, normalized_search_text,
		       COALESCE(content_hash, ''), lifecycle_state, is_current
		FROM lorebook_reference_entries WHERE scope_id = ? ORDER BY entry_record_id ASC`, scopeID)
	if err != nil {
		t.Fatalf("read stored lorebook entries: %v", err)
	}
	defer rows.Close()
	out := []d1LorebookRefStoredEntry{}
	for rows.Next() {
		var item d1LorebookRefStoredEntry
		if err := rows.Scan(&item.HostEntryID, &item.EntryOrdinal, &item.Normalized,
			&item.ContentHash, &item.LifecycleState, &item.IsCurrent); err != nil {
			t.Fatalf("scan stored lorebook entry: %v", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate stored lorebook entries: %v", err)
	}
	return out
}

// d1LorebookRefScopeID resolves a scope through the store's own resolution, so a
// test can name the row it created. Re-deriving the identity rule in the test
// would let it pass while the store resolved a different scope.
func d1LorebookRefScopeID(t *testing.T, conn *sqliteD1Conn, scope LorebookReferenceScope) int64 {
	t.Helper()
	st, err := NewD1Store(conn)
	if err != nil {
		t.Fatalf("NewD1Store: %v", err)
	}
	row, err := st.(*d1Store).d1LorebookRefFindScope(context.Background(), scope)
	if err != nil {
		t.Fatalf("resolve stored lorebook scope: %v", err)
	}
	return row.ScopeID
}

// ---------------------------------------------------------------------------
// write path: scope creation and projection replacement
// ---------------------------------------------------------------------------

// TestD1LorebookCompleteSnapshotReplacesOnlyTheExactScope pins the core write:
// the first observation creates the scope, a complete one replaces the current
// projection, and a second scope for the SAME session and host indices with a
// different enabled-module set gets its own row and its own projection.
//
// The last half is the one worth keeping. lorebook_reference_scopes has no
// unique key over (chat_session_id, character_index, chat_index), so a lookup
// that ignored the module set would let the second observation overwrite the
// first book's catalog. The caller would then be shown one module set's lore
// under the other module set's name, with nothing in the response to say so.
func TestD1LorebookCompleteSnapshotReplacesOnlyTheExactScope(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	// Unsorted, duplicated module ids: the canonical form is what is stored and
	// compared, so this scope must be the same scope as ["module-a", "module-b"].
	scopeA := d1LorebookRefExactScope("session-a", 2, 7, "module-b", "module-a", "module-a")
	scopeB := d1LorebookRefExactScope("session-a", 2, 7, "module-c")

	first, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("a1"), scopeA, base,
		d1LorebookRefEntry("entry-1", "Han-eol", "Han-eol will take the exam."),
		d1LorebookRefEntry("entry-2", "Archive", "The archive is in the north."),
	))
	if err != nil {
		t.Fatalf("first ApplyLorebookReferenceSnapshot: %v", err)
	}
	if first.LifecycleAction != "current_projection_replaced" {
		t.Errorf("first lifecycle action = %q, want current_projection_replaced", first.LifecycleAction)
	}
	// A freshly created scope has nothing current to retire, and a promotion
	// makes every entry of this snapshot the current one.
	if first.PreviousCurrentCount != 0 || first.CurrentEntryCount != 2 || first.ObservedEntryCount != 2 {
		t.Errorf("first result = %+v, want previous 0, current 2, observed 2", first)
	}
	if first.SnapshotID != d1LorebookRefSnapshotID("a1") || first.ScopeID <= 0 {
		t.Errorf("first result identity = %+v", first)
	}
	if first.ObservationState != LorebookObservationObserved {
		t.Errorf("observation state = %q, want observed", first.ObservationState)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM lorebook_reference_scopes`); got != 1 {
		t.Errorf("scope rows = %d, want 1", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM lorebook_reference_session_locks`); got != 1 {
		t.Errorf("session lock rows = %d, want 1", got)
	}

	// A second observation of the SAME scope replaces the projection. The
	// previous current rows are retired, not deleted, and the reported count is
	// the number the demote actually retired.
	second, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("a2"), scopeA, base.Add(time.Minute),
		d1LorebookRefEntry("entry-3", "Han-eol", "Han-eol will take the final exam."),
	))
	if err != nil {
		t.Fatalf("second ApplyLorebookReferenceSnapshot: %v", err)
	}
	if second.ScopeID != first.ScopeID {
		t.Errorf("same scope got a new id: %d then %d", first.ScopeID, second.ScopeID)
	}
	if second.PreviousCurrentCount != 2 || second.CurrentEntryCount != 1 {
		t.Errorf("second result = %+v, want previous 2 and current 1", second)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM lorebook_reference_scopes`); got != 1 {
		t.Errorf("scope rows = %d after a re-observation, want 1 (the scope is reused)", got)
	}
	storedA := d1LorebookRefStored(t, conn, second.ScopeID)
	if len(storedA) != 3 {
		t.Fatalf("stored entries = %d, want 3 (the ledger is append-only)", len(storedA))
	}
	if storedA[0].LifecycleState != LorebookLifecycleStale || storedA[0].IsCurrent {
		t.Errorf("superseded entry = %s/current=%t, want catalog_stale and not current",
			storedA[0].LifecycleState, storedA[0].IsCurrent)
	}
	if storedA[2].LifecycleState != LorebookLifecycleCurrent || !storedA[2].IsCurrent {
		t.Errorf("promoted entry = %s/current=%t, want catalog_current and current",
			storedA[2].LifecycleState, storedA[2].IsCurrent)
	}

	// A different module set for the same session and indices is a DIFFERENT
	// scope. It must not retire the first scope's catalog.
	third, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("b1"), scopeB, base.Add(2*time.Minute),
		d1LorebookRefEntry("entry-9", "Sasha", "Sasha keeps the ledger."),
	))
	if err != nil {
		t.Fatalf("second-scope ApplyLorebookReferenceSnapshot: %v", err)
	}
	if third.ScopeID == first.ScopeID {
		t.Fatalf("a different enabled-module set reused scope %d", third.ScopeID)
	}
	if third.PreviousCurrentCount != 0 {
		t.Errorf("the second scope retired %d rows of another module set", third.PreviousCurrentCount)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM lorebook_reference_scopes`); got != 2 {
		t.Errorf("scope rows = %d, want 2", got)
	}

	// Each scope still reads back its own catalog. The module list here is
	// reordered and deduplicated differently from what the writer was given,
	// which also pins that the comparison runs on the canonical form.
	pageA, err := st.GetLorebookReferenceCurrentPage(ctx, d1LorebookRefExactScope("session-a", 2, 7, "module-a", "module-b"), 20, 0)
	if err != nil {
		t.Fatalf("page for the first module set: %v", err)
	}
	if pageA.Total != 1 || len(pageA.Entries) != 1 || pageA.Entries[0].HostEntryID != "entry-3" {
		t.Errorf("first module set page = %+v, want entry-3 only", pageA.Entries)
	}
	pageB, err := st.GetLorebookReferenceCurrentPage(ctx, scopeB, 20, 0)
	if err != nil {
		t.Fatalf("page for the second module set: %v", err)
	}
	if pageB.Total != 1 || len(pageB.Entries) != 1 || pageB.Entries[0].HostEntryID != "entry-9" {
		t.Errorf("second module set page = %+v, want entry-9 only", pageB.Entries)
	}
}

// TestD1LorebookUnobservedIndicesAreTheirOwnScope pins the NULL-safe equality of
// the scope lookup. A Host that did not expose character_index or chat_index has
// NULL there, and `IS ?` is what makes that a scope of its own rather than an
// invisible one. Rewriting it as `= ?` would make every such scope unreachable,
// because the route sends an absent query parameter as a nil index.
func TestD1LorebookUnobservedIndicesAreTheirOwnScope(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)

	indexed := d1LorebookRefExactScope("session-a", 2, 7, "module-a")
	aggregate := d1LorebookRefAggregateScope("session-a", "module-a")

	indexedResult, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("i1"), indexed, base, d1LorebookRefEntry("e-indexed", "Index", "indexed scope")))
	if err != nil {
		t.Fatalf("indexed scope apply: %v", err)
	}
	// The aggregate scope is created by a partial report, which is the only kind
	// of observation the validator lets carry an incomplete chat scope.
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefPartial(
		d1LorebookRefSnapshotID("g1"), aggregate, base,
		d1LorebookRefEntry("e-aggregate", "Aggregate", "aggregate scope"))); err != nil {
		t.Fatalf("aggregate scope apply: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM lorebook_reference_scopes`); got != 2 {
		t.Errorf("scope rows = %d, want 2 (NULL indices are their own scope)", got)
	}
	aggregateID := d1LorebookRefScopeID(t, conn, aggregate)
	if aggregateID == indexedResult.ScopeID {
		t.Fatalf("the NULL-index scope resolved to the indexed scope %d", indexedResult.ScopeID)
	}
	// A lookup with NULL indices must never fall back to the indexed scope.
	aggregatePage, err := st.GetLorebookReferenceCurrentPage(ctx, aggregate, 20, 0)
	if err != nil {
		t.Fatalf("aggregate page: %v", err)
	}
	if aggregatePage.ScopeID != aggregateID {
		t.Errorf("aggregate page resolved scope %d, want %d", aggregatePage.ScopeID, aggregateID)
	}
	if aggregatePage.Total != 0 || len(aggregatePage.Entries) != 0 {
		t.Errorf("aggregate page = %+v, want no current entries (a partial report promotes nothing)", aggregatePage.Entries)
	}
	// And the indexed scope is untouched by the aggregate scope's existence.
	indexedPage, err := st.GetLorebookReferenceCurrentPage(ctx, indexed, 20, 0)
	if err != nil {
		t.Fatalf("indexed page: %v", err)
	}
	if len(indexedPage.Entries) != 1 || indexedPage.Entries[0].HostEntryID != "e-indexed" {
		t.Errorf("indexed page = %+v, want e-indexed only", indexedPage.Entries)
	}
	// A scope that was never observed at all is the not-found sentinel the
	// route turns into a 404.
	if _, err := st.GetLorebookReferenceCurrentPage(ctx, d1LorebookRefExactScope("session-a", 9, 9, "module-a"), 20, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown host index error = %v, want ErrNotFound", err)
	}
}

// TestD1LorebookPartialAndUnavailableObservationsKeepTheCurrentProjection pins
// the two non-authoritative observation states. Both are recorded in the
// ledger, both leave the catalog alone, and both store their entries as
// observed_partial and non-current. A partial read that promoted itself would
// make "the host only returned half the book" indistinguishable from a
// confirmed catalog.
func TestD1LorebookPartialAndUnavailableObservationsKeepTheCurrentProjection(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-a", 2, 7, "module-a")

	complete, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("c1"), scope, base,
		d1LorebookRefEntry("e-1", "one", "first body"),
		d1LorebookRefEntry("e-2", "two", "second body"),
	))
	if err != nil {
		t.Fatalf("complete apply: %v", err)
	}

	partial, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefPartial(
		d1LorebookRefSnapshotID("p1"), scope, base.Add(time.Minute),
		d1LorebookRefEntry("e-partial", "partial", "partial body")))
	if err != nil {
		t.Fatalf("partial apply: %v", err)
	}
	if partial.LifecycleAction != "observation_recorded" {
		t.Errorf("partial lifecycle action = %q, want observation_recorded", partial.LifecycleAction)
	}
	if partial.ObservationState != LorebookObservationPartial {
		t.Errorf("partial observation state = %q", partial.ObservationState)
	}
	// A partial observation retires nothing and replaces nothing.
	if partial.PreviousCurrentCount != 0 || partial.CurrentEntryCount != 2 {
		t.Errorf("partial result = %+v, want previous 0 and current 2 (the confirmed projection)", partial)
	}

	// A missing observed_at is a caller that did not timestamp the request, not
	// the epoch. Comparing the zero time against the newest authority would mark
	// this as out of order, which for a non-authoritative observation is the
	// difference between "recorded" and "recorded as stale" for no reason.
	unavailable, err := st.ApplyLorebookReferenceSnapshot(ctx, &LorebookReferenceSnapshot{
		SnapshotID: d1LorebookRefSnapshotID("u1"), ContractVersion: LorebookReferenceSnapshotContractV1,
		ConsentState: LorebookConsentActive, ObservationState: LorebookObservationUnavailable,
		Scope: scope,
	})
	if err != nil {
		t.Fatalf("unavailable apply: %v", err)
	}
	if unavailable.LifecycleAction != "observation_recorded" || unavailable.CurrentEntryCount != 2 {
		t.Errorf("unavailable result = %+v, want observation_recorded with current 2", unavailable)
	}
	if unavailable.ObservedEntryCount != 0 {
		t.Errorf("unavailable observation reported %d entries, want 0", unavailable.ObservedEntryCount)
	}

	stored := d1LorebookRefStored(t, conn, complete.ScopeID)
	if len(stored) != 3 {
		t.Fatalf("stored entries = %d, want 3 (the ledger records every observation)", len(stored))
	}
	for i, want := range []struct {
		lifecycle string
		current   bool
	}{
		{LorebookLifecycleCurrent, true},
		{LorebookLifecycleCurrent, true},
		{LorebookLifecyclePartial, false},
	} {
		if stored[i].LifecycleState != want.lifecycle || stored[i].IsCurrent != want.current {
			t.Errorf("entry %d = %s/current=%t, want %s/current=%t",
				i, stored[i].LifecycleState, stored[i].IsCurrent, want.lifecycle, want.current)
		}
	}

	// The catalog itself is unchanged: still the two confirmed entries.
	current, err := st.GetLorebookReferenceCurrent(ctx, scope)
	if err != nil {
		t.Fatalf("GetLorebookReferenceCurrent: %v", err)
	}
	if len(current.Entries) != 2 {
		t.Errorf("current entries = %+v, want the two confirmed ones", current.Entries)
	}
	if current.Entries == nil {
		t.Error("the entry slice must be non-nil so it encodes as [] rather than null")
	}
	// The latest SNAPSHOT is the unavailable one, because the page reports what
	// the host last said, including that it could not answer.
	if current.LatestSnapshot == nil || current.LatestSnapshot.SnapshotID != d1LorebookRefSnapshotID("u1") {
		t.Errorf("latest snapshot = %+v, want the unavailable observation", current.LatestSnapshot)
	}
	if current.LatestSnapshot.ObservationState != LorebookObservationUnavailable {
		t.Errorf("latest observation state = %q, want unavailable", current.LatestSnapshot.ObservationState)
	}
	if current.LatestSnapshot.CompleteSnapshot {
		t.Error("the latest snapshot must report that it was not complete")
	}
	if current.LatestSnapshot.ContractVersion != LorebookReferenceSnapshotContractV1 {
		t.Errorf("latest snapshot contract = %q", current.LatestSnapshot.ContractVersion)
	}
	if current.LatestSnapshot.Scope.ChatSessionID != "session-a" {
		t.Errorf("the latest snapshot lost the requested scope: %+v", current.LatestSnapshot.Scope)
	}
	if current.LatestSnapshot.ObservedAt.Before(base.Add(time.Minute)) {
		t.Errorf("a zero observed_at must become the current time, got %s", current.LatestSnapshot.ObservedAt)
	}
}

// TestD1LorebookConsentRevocationRetiresTheExactScopeOnly pins revocation. It
// is the one observation that disables a catalog without replacing it, so the
// reported previous count is the number of entries that were serving, and the
// current count afterwards is zero.
func TestD1LorebookConsentRevocationRetiresTheExactScopeOnly(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-a", 1, 2, "module-a")
	other := d1LorebookRefExactScope("session-a", 4, 9, "module-a")

	live, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("r1"), scope, base,
		d1LorebookRefEntry("e-1", "one", "body one"),
		d1LorebookRefEntry("e-2", "two", "body two"),
		d1LorebookRefEntry("e-3", "three", "body three"),
	))
	if err != nil {
		t.Fatalf("complete apply: %v", err)
	}
	neighbour, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("r2"), other, base, d1LorebookRefEntry("e-n", "neighbour", "neighbour body")))
	if err != nil {
		t.Fatalf("neighbour apply: %v", err)
	}

	revoked, err := st.ApplyLorebookReferenceSnapshot(ctx, &LorebookReferenceSnapshot{
		SnapshotID: d1LorebookRefSnapshotID("r3"), ContractVersion: LorebookReferenceSnapshotContractV1,
		ConsentState: LorebookConsentRevoked, ObservationState: LorebookObservationObserved,
		Scope: scope, ProvenanceJSON: `{}`, ObservedAt: base.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("revocation apply: %v", err)
	}
	if revoked.ScopeID != live.ScopeID {
		t.Errorf("revocation hit scope %d, want %d", revoked.ScopeID, live.ScopeID)
	}
	if revoked.LifecycleAction != "consent_revoked" {
		t.Errorf("lifecycle action = %q, want consent_revoked", revoked.LifecycleAction)
	}
	// The count is what the demote actually retired, which is the number the
	// caller reports as "no longer served".
	if revoked.PreviousCurrentCount != 3 {
		t.Errorf("previous current = %d, want 3", revoked.PreviousCurrentCount)
	}
	if revoked.CurrentEntryCount != 0 {
		t.Errorf("current after revocation = %d, want 0", revoked.CurrentEntryCount)
	}

	// The revocation is scoped by scope, not by session: a neighbouring scope
	// of the same session keeps serving its catalog.
	stored := d1LorebookRefStored(t, conn, live.ScopeID)
	for i, item := range stored {
		if item.LifecycleState != LorebookLifecycleConsentRevoked || item.IsCurrent {
			t.Errorf("revoked entry %d = %s/current=%t, want consent_revoked and not current",
				i, item.LifecycleState, item.IsCurrent)
		}
	}
	neighbourRows := d1LorebookRefStored(t, conn, neighbour.ScopeID)
	if len(neighbourRows) != 1 || neighbourRows[0].LifecycleState != LorebookLifecycleCurrent || !neighbourRows[0].IsCurrent {
		t.Errorf("a revocation reached a neighbour scope: %+v", neighbourRows)
	}

	// The paged view is now empty, and encodes as an empty list rather than a
	// missing one.
	page, err := st.GetLorebookReferenceCurrentPage(ctx, scope, 20, 0)
	if err != nil {
		t.Fatalf("page after revocation: %v", err)
	}
	if page.Total != 0 {
		t.Errorf("total after revocation = %d, want 0", page.Total)
	}
	if page.Entries == nil || len(page.Entries) != 0 {
		t.Errorf("entries after revocation = %#v, want an empty non-nil slice", page.Entries)
	}
}

// TestD1LorebookOutOfOrderObservationIsRecordedButNotApplied pins the ordering
// fence. A complete observation older than the newest authority observation is
// still written to the append-only ledger — the host did report it — but it must
// not retire the newer catalog, and its own entries must be stored as
// catalog_stale and non-current.
func TestD1LorebookOutOfOrderObservationIsRecordedButNotApplied(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-a", 3, 8, "module-a")

	newer, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("n1"), scope, base, d1LorebookRefEntry("e-new", "new", "newer body")))
	if err != nil {
		t.Fatalf("newer apply: %v", err)
	}

	older, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("o1"), scope, base.Add(-time.Hour),
		d1LorebookRefEntry("e-old", "old", "older body")))
	if err != nil {
		t.Fatalf("older apply: %v", err)
	}
	if older.LifecycleAction != "out_of_order_observation_recorded" {
		t.Errorf("lifecycle action = %q, want out_of_order_observation_recorded", older.LifecycleAction)
	}
	if older.PreviousCurrentCount != 0 {
		t.Errorf("an out-of-order observation retired %d current rows", older.PreviousCurrentCount)
	}
	if older.CurrentEntryCount != 1 {
		t.Errorf("current = %d, want the newer projection to survive", older.CurrentEntryCount)
	}

	stored := d1LorebookRefStored(t, conn, newer.ScopeID)
	if len(stored) != 2 {
		t.Fatalf("stored entries = %d, want 2", len(stored))
	}
	if stored[0].LifecycleState != LorebookLifecycleCurrent || !stored[0].IsCurrent {
		t.Errorf("the newer entry was demoted: %+v", stored[0])
	}
	if stored[1].LifecycleState != LorebookLifecycleStale || stored[1].IsCurrent {
		t.Errorf("the out-of-order entry = %s/current=%t, want catalog_stale and not current",
			stored[1].LifecycleState, stored[1].IsCurrent)
	}

	// A partial observation is not an authority, so it must not make a later,
	// genuinely newer complete observation look out of order.
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefPartial(
		d1LorebookRefSnapshotID("n2"), scope, base.Add(time.Hour),
		d1LorebookRefEntry("e-mid", "mid", "partial body"))); err != nil {
		t.Fatalf("partial apply: %v", err)
	}
	newest, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("n3"), scope, base.Add(2*time.Hour),
		d1LorebookRefEntry("e-newest", "newest", "newest body")))
	if err != nil {
		t.Fatalf("newest apply: %v", err)
	}
	if newest.LifecycleAction != "current_projection_replaced" {
		t.Errorf("the newest complete observation = %q, want current_projection_replaced", newest.LifecycleAction)
	}
	if newest.PreviousCurrentCount != 1 {
		t.Errorf("previous current = %d, want 1", newest.PreviousCurrentCount)
	}
	if newest.CurrentEntryCount != 1 {
		t.Errorf("current = %d, want 1", newest.CurrentEntryCount)
	}
}

// TestD1LorebookSnapshotReplayIsRejectedAndWritesNothing pins the ledger's
// idempotency rule. snapshot_id is the primary key of an append-only ledger, so
// re-submitting one observation must not produce a second row that would then
// compete with the first for "latest observation". The collision surfaces as the
// database's own duplicate-key failure, with the whole batch rolled back — never
// as a silent overwrite of the stored row.
func TestD1LorebookSnapshotReplayIsRejectedAndWritesNothing(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-a", 1, 2, "module-a")

	snapshotID := d1LorebookRefSnapshotID("dup1")
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		snapshotID, scope, base, d1LorebookRefEntry("e-1", "one", "body one"))); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	// An exact replay of the same observation.
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		snapshotID, scope, base, d1LorebookRefEntry("e-1", "one", "body one"))); err == nil {
		t.Fatal("a replayed snapshot id must be rejected, not silently accepted")
	}
	// A different observation that collides on the id is a conflict, not a
	// rewrite: the stored row must keep the body it had.
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		snapshotID, scope, base, d1LorebookRefEntry("e-x", "x", "replacement body"))); err == nil {
		t.Fatal("a colliding snapshot id with different content must be rejected")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM lorebook_reference_snapshots`); got != 1 {
		t.Errorf("snapshot rows = %d after two rejections, want 1", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM lorebook_reference_entries`); got != 1 {
		t.Errorf("entry rows = %d after two rejections, want 1 (the batch must roll back)", got)
	}
	page, err := st.GetLorebookReferenceCurrentPage(ctx, scope, 20, 0)
	if err != nil {
		t.Fatalf("page after rejection: %v", err)
	}
	if len(page.Entries) != 1 || page.Entries[0].Content != "body one" {
		t.Errorf("the stored observation was overwritten: %+v", page.Entries)
	}

	// A distinct id is a distinct observation and must succeed, so the rejection
	// above is the key and not a broken writer.
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("dup2"), scope, base.Add(time.Minute),
		d1LorebookRefEntry("e-2", "two", "body two"))); err != nil {
		t.Fatalf("a distinct snapshot id must apply: %v", err)
	}
}

// TestD1LorebookMalformedPayloadIsRejectedBeforeAnythingIsStored pins the
// validation order. A malformed JSON payload is a caller error: nothing may be
// stored, and the caller must see ErrInvalidLorebookReference, which the route
// turns into a 400 rather than a 500.
func TestD1LorebookMalformedPayloadIsRejectedBeforeAnythingIsStored(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-a", 1, 2, "module-a")

	badProvenance := d1LorebookRefComplete(d1LorebookRefSnapshotID("v1"), scope, base,
		d1LorebookRefEntry("e-1", "one", "body"))
	badProvenance.ProvenanceJSON = "{not json"

	badExtensionsEntry := d1LorebookRefEntry("e-1", "one", "body")
	badExtensionsEntry.ExtensionsJSON = "{also not json"
	badExtensions := d1LorebookRefComplete(d1LorebookRefSnapshotID("v2"), scope, base, badExtensionsEntry)

	for name, item := range map[string]*LorebookReferenceSnapshot{
		"malformed provenance": badProvenance,
		"malformed extensions": badExtensions,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := st.ApplyLorebookReferenceSnapshot(ctx, item)
			if !errors.Is(err, ErrInvalidLorebookReference) {
				t.Fatalf("error = %v, want ErrInvalidLorebookReference", err)
			}
		})
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM lorebook_reference_snapshots`); got != 0 {
		t.Errorf("snapshot rows = %d, want 0 after two rejected observations", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM lorebook_reference_entries`); got != 0 {
		t.Errorf("entry rows = %d, want 0 after two rejected observations", got)
	}

	// A blank provenance and a blank extensions payload are NOT malformed: they
	// are stored as the empty JSON object, because both columns are NOT NULL and
	// must stay decodable by a reader that decodes them unconditionally.
	blankEntry := d1LorebookRefEntry("e-1", "one", "body")
	blankEntry.ExtensionsJSON = "   "
	blank := d1LorebookRefComplete(d1LorebookRefSnapshotID("v3"), scope, base, blankEntry)
	blank.ProvenanceJSON = "  "
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, blank); err != nil {
		t.Fatalf("blank provenance and extensions must be accepted: %v", err)
	}
	var provenance string
	if err := conn.QueryRow(ctx, `SELECT provenance_json FROM lorebook_reference_snapshots`).Scan(&provenance); err != nil {
		t.Fatalf("read provenance: %v", err)
	}
	if provenance != "{}" {
		t.Errorf("provenance_json = %q, want the empty object", provenance)
	}
}

// TestD1LorebookInvalidSnapshotIsRejectedBeforeAnyStatement pins the shared
// validator's role. The route validates before it calls the store, and so must
// the provider, so a snapshot that pretends to be a complete active observation
// without the chat scope the official API would have supplied never reaches the
// database.
func TestD1LorebookInvalidSnapshotIsRejectedBeforeAnyStatement(t *testing.T) {
	st, conn := newD1ProbeStore(t)
	ctx := context.Background()
	scope := d1LorebookRefExactScope("session-a", 1, 2, "module-a")

	wrongContract := d1LorebookRefComplete("x", scope, time.Now().UTC())
	wrongContract.ContractVersion = "lorebook_reference_snapshot.v2"

	cases := map[string]*LorebookReferenceSnapshot{
		"nil snapshot":           nil,
		"wrong contract version": wrongContract,
		"unavailable claiming to be complete": {
			SnapshotID: "x", ContractVersion: LorebookReferenceSnapshotContractV1,
			ConsentState: LorebookConsentActive, ObservationState: LorebookObservationUnavailable,
			CompleteSnapshot: true, Scope: scope,
		},
		"complete observed without the chat scope": {
			SnapshotID: "x", ContractVersion: LorebookReferenceSnapshotContractV1,
			ConsentState: LorebookConsentActive, ObservationState: LorebookObservationObserved,
			CompleteSnapshot: true, Scope: d1LorebookRefAggregateScope("session-a", "module-a"),
		},
		"unknown consent state": {
			SnapshotID: "x", ContractVersion: LorebookReferenceSnapshotContractV1,
			ConsentState: "maybe", ObservationState: LorebookObservationUnavailable,
			Scope: scope,
		},
	}
	for name, item := range cases {
		t.Run(name, func(t *testing.T) {
			before := len(conn.queries) + len(conn.batches)
			_, err := st.ApplyLorebookReferenceSnapshot(ctx, item)
			if !errors.Is(err, ErrInvalidLorebookReference) {
				t.Fatalf("error = %v, want ErrInvalidLorebookReference", err)
			}
			if after := len(conn.queries) + len(conn.batches); after != before {
				t.Errorf("a rejected snapshot sent %d statements, want none", after-before)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// read path
// ---------------------------------------------------------------------------

// TestD1LorebookCurrentReadPreservesTheObservedTriState pins the read
// projection. Host lore is rendered as "what the host reported", so every
// optional column must come back exactly as it was stored: NULL as the empty
// string for text, and still nil for the flags, so the panel can tell "not
// reported" from "reported false".
func TestD1LorebookCurrentReadPreservesTheObservedTriState(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-a", 2, 7, "module-a")

	// A fully populated entry, including a false flag and a zero insert order:
	// both are observed values and must not collapse into their NULL form.
	populated := d1LorebookRefEntry("entry-1", "Han-eol", "Han-eol will take the exam.")
	populated.SecondKey = "exam"
	populated.Comment = "profile"
	populated.Mode = "normal"
	populated.AlwaysActive = d1LorebookRefPtr(false)
	populated.Selective = d1LorebookRefPtr(true)
	populated.UseRegex = d1LorebookRefPtr(false)
	populated.InsertOrder = d1LorebookRefPtr(0)
	populated.ActivationPct = d1LorebookRefPtr(0.0)
	populated.BookVersion = d1LorebookRefPtr(int64(3))
	populated.Folder = "cast"
	populated.SourceIdentity = "risu_cast"
	populated.ExtensionsJSON = `{"risu_case_sensitive":false}`

	// A bare entry: every optional column is NULL.
	bare := d1LorebookRefEntry("", "", "bare body")

	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("t1"), scope, base, populated, bare)); err != nil {
		t.Fatalf("apply: %v", err)
	}

	current, err := st.GetLorebookReferenceCurrent(ctx, scope)
	if err != nil {
		t.Fatalf("GetLorebookReferenceCurrent: %v", err)
	}
	if current.Entries == nil {
		t.Fatal("the entry slice must be non-nil so it encodes as [] rather than null")
	}
	if len(current.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(current.Entries))
	}
	if current.ScopeID <= 0 || current.Scope.ChatSessionID != "session-a" {
		t.Errorf("current scope = %+v", current.Scope)
	}

	first := current.Entries[0]
	if first.HostEntryID != "entry-1" || first.Key != "Han-eol" || first.Content != "Han-eol will take the exam." {
		t.Errorf("populated entry lost its Host fields: %+v", first)
	}
	if first.SecondKey != "exam" || first.Comment != "profile" || first.Mode != "normal" || first.Folder != "cast" {
		t.Errorf("populated entry lost its optional text: %+v", first)
	}
	if first.SourceIdentity != "risu_cast" || first.SourceKind != "current_host_aggregate" {
		t.Errorf("populated entry lost its source: %+v", first)
	}
	// observed false, not "unobserved".
	if first.AlwaysActive == nil || *first.AlwaysActive {
		t.Errorf("always_active = %v, want an observed false", first.AlwaysActive)
	}
	if first.Selective == nil || !*first.Selective {
		t.Errorf("selective = %v, want an observed true", first.Selective)
	}
	if first.UseRegex == nil || *first.UseRegex {
		t.Errorf("use_regex = %v, want an observed false", first.UseRegex)
	}
	// A zero insert order and a zero activation percentage are observations.
	if first.InsertOrder == nil || *first.InsertOrder != 0 {
		t.Errorf("insert_order = %v, want an observed 0", first.InsertOrder)
	}
	if first.ActivationPct == nil || *first.ActivationPct != 0 {
		t.Errorf("activation_percent = %v, want an observed 0", first.ActivationPct)
	}
	if first.BookVersion == nil || *first.BookVersion != 3 {
		t.Errorf("book_version = %v, want 3", first.BookVersion)
	}
	if first.EntryOrdinal != 0 {
		t.Errorf("entry_ordinal = %d, want 0", first.EntryOrdinal)
	}
	if first.ExtensionsJSON != `{"risu_case_sensitive":false}` {
		t.Errorf("extensions_json = %q", first.ExtensionsJSON)
	}
	if first.NormalizedSearch == "" || first.ContentHash == "" {
		t.Errorf("derived diagnostic columns must be stored: %+v", first)
	}

	// The bare entry: NULL reads as the empty string for text and stays nil for
	// the flags. A nil here is what tells the renderer the host said nothing.
	second := current.Entries[1]
	if second.HostEntryID != "" || second.Mode != "" || second.Folder != "" || second.SourceIdentity != "" {
		t.Errorf("NULL text must read as the empty string: %+v", second)
	}
	if second.AlwaysActive != nil || second.Selective != nil || second.UseRegex != nil {
		t.Errorf("NULL flags must stay nil: %+v", second)
	}
	if second.InsertOrder != nil || second.ActivationPct != nil || second.BookVersion != nil {
		t.Errorf("NULL numerics must stay nil: %+v", second)
	}
	if second.Key != "" || second.Content != "bare body" {
		t.Errorf("NOT NULL entry columns must round-trip: %+v", second)
	}
	if second.EntryOrdinal != 1 {
		t.Errorf("entry_ordinal = %d, want 1 (ordinals are positional, not Host-supplied)", second.EntryOrdinal)
	}
	// A blank extensions payload is stored as the empty object, so a reader that
	// decodes it unconditionally never has to special-case a NULL.
	if second.ExtensionsJSON != "{}" {
		t.Errorf("extensions_json = %q, want the empty object", second.ExtensionsJSON)
	}
}

// TestD1LorebookCurrentReadOrdersByOrdinalThenRecordID pins the page ordering.
// The Host's own ordering is the catalog's ordering, and entry_record_id breaks
// a tie between rows that claim the same ordinal, so a snapshot that carries two
// entries at one ordinal still renders in a stable, insertion-derived order and
// a paged window is a real offset into that order.
func TestD1LorebookCurrentReadOrdersByOrdinalThenRecordID(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-order", 1, 1, "module-a")

	// The two entries are sent out of alphabetical order, so a reader that
	// sorted by key or by host id would disagree with the Host's own order.
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("ord1"), scope, base,
		d1LorebookRefEntry("c", "third", "third body"),
		d1LorebookRefEntry("a", "first", "first body"),
		d1LorebookRefEntry("b", "second", "second body"),
	)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	current, err := st.GetLorebookReferenceCurrent(ctx, scope)
	if err != nil {
		t.Fatalf("unbounded read: %v", err)
	}
	wantOrder := []string{"c", "a", "b"}
	if len(current.Entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(current.Entries))
	}
	for i, want := range wantOrder {
		if current.Entries[i].HostEntryID != want {
			t.Errorf("entry %d = %q, want %q (Host order, not sorted)", i, current.Entries[i].HostEntryID, want)
		}
	}
	// Ordinals are assigned by position, never taken from the observation: a
	// Host-supplied ordinal would let a reordered payload renumber the catalog.
	for i, entry := range current.Entries {
		if entry.EntryOrdinal != i {
			t.Errorf("entry %d reports ordinal %d, want the positional %d", i, entry.EntryOrdinal, i)
		}
	}
	// A window of that order is a real offset.
	window, err := st.GetLorebookReferenceCurrentPage(ctx, scope, 2, 1)
	if err != nil {
		t.Fatalf("paged read: %v", err)
	}
	if len(window.Entries) != 2 || window.Entries[0].HostEntryID != "a" || window.Entries[1].HostEntryID != "b" {
		t.Errorf("window = %+v, want a and b", window.Entries)
	}
}

// TestD1LorebookCurrentPageBoundsTheWindowAndReportsTheTotal pins the paged
// projection: a bounded window, a total that is the size of the whole set rather
// than of the window, and the request guard.
//
// The guard is the reason this test exists in this form. A page that silently
// ignored an oversized limit would answer with everything, and the response
// would look like a successful request that happened to return a lot.
func TestD1LorebookCurrentPageBoundsTheWindowAndReportsTheTotal(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-page", 1, 1, "module-a")

	entries := make([]LorebookReferenceEntryObservation, 0, 5)
	for i := 0; i < 5; i++ {
		entries = append(entries, d1LorebookRefEntry(
			fmt.Sprintf("entry-%d", i), fmt.Sprintf("key-%d", i), fmt.Sprintf("body %d", i)))
	}
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("pg1"), scope, base, entries...)); err != nil {
		t.Fatalf("apply: %v", err)
	}

	all, err := st.GetLorebookReferenceCurrent(ctx, scope)
	if err != nil {
		t.Fatalf("unbounded read: %v", err)
	}
	if len(all.Entries) != 5 {
		t.Fatalf("unbounded entries = %d, want 5", len(all.Entries))
	}

	window, err := st.GetLorebookReferenceCurrentPage(ctx, scope, 2, 1)
	if err != nil {
		t.Fatalf("paged read: %v", err)
	}
	// The total is the size of the paged SET, not of the window; that is what
	// the route's has_more computation depends on.
	if window.Total != 5 {
		t.Errorf("total = %d, want 5 (the whole current set)", window.Total)
	}
	if window.Limit != 2 || window.Offset != 1 {
		t.Errorf("window echo = limit %d offset %d, want 2 and 1", window.Limit, window.Offset)
	}
	if len(window.Entries) != 2 {
		t.Fatalf("window entries = %d, want 2", len(window.Entries))
	}
	if window.Entries[0].HostEntryID != all.Entries[1].HostEntryID ||
		window.Entries[1].HostEntryID != all.Entries[2].HostEntryID {
		t.Errorf("window = %q,%q, want %q,%q", window.Entries[0].HostEntryID, window.Entries[1].HostEntryID,
			all.Entries[1].HostEntryID, all.Entries[2].HostEntryID)
	}
	// The page echoes the scope the caller asked for, and reports the snapshot
	// that produced the projection.
	if window.Scope.ChatSessionID != "session-page" || window.ScopeID <= 0 {
		t.Errorf("page scope = %+v", window.Scope)
	}
	if window.LatestSnapshot == nil || window.LatestSnapshot.SnapshotID != d1LorebookRefSnapshotID("pg1") {
		t.Errorf("page snapshot = %+v", window.LatestSnapshot)
	}

	// An offset past the end is an empty window, not an error and not a wrap.
	past, err := st.GetLorebookReferenceCurrentPage(ctx, scope, 2, 99)
	if err != nil {
		t.Fatalf("page past the end: %v", err)
	}
	if past.Total != 5 || past.Entries == nil || len(past.Entries) != 0 {
		t.Errorf("page past the end = total %d entries %#v, want total 5 and an empty non-nil slice",
			past.Total, past.Entries)
	}

	// The request guard runs before the database is touched.
	probe, probeConn := newD1ProbeStore(t)
	for _, tc := range []struct {
		name   string
		scope  LorebookReferenceScope
		limit  int
		offset int
	}{
		{"zero limit", scope, 0, 0},
		{"negative limit", scope, -1, 0},
		{"oversized limit", scope, 101, 0},
		{"negative offset", scope, 20, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := len(probeConn.queries) + len(probeConn.batches)
			page, err := probe.GetLorebookReferenceCurrentPage(ctx, tc.scope, tc.limit, tc.offset)
			if !errors.Is(err, ErrInvalidLorebookReference) {
				t.Fatalf("error = %v, want ErrInvalidLorebookReference", err)
			}
			if page != nil {
				t.Errorf("a rejected page returned %+v", page)
			}
			if after := len(probeConn.queries) + len(probeConn.batches); after != before {
				t.Errorf("a rejected page sent %d statements, want none", after-before)
			}
		})
	}

	// The same guard on the unbounded prepare-turn read is the blank session,
	// which must be rejected rather than answered with ErrNotFound, because the
	// route turns ErrNotFound into a 404 for a session that does not exist.
	before := len(probeConn.queries) + len(probeConn.batches)
	for _, sid := range []string{"", "   ", "\t\n"} {
		current, err := probe.GetLorebookReferenceCurrent(ctx, LorebookReferenceScope{ChatSessionID: sid})
		if !errors.Is(err, ErrInvalidLorebookReference) {
			t.Errorf("blank session %q error = %v, want ErrInvalidLorebookReference", sid, err)
		}
		if current != nil {
			t.Errorf("blank session %q returned %+v", sid, current)
		}
	}
	if after := len(probeConn.queries) + len(probeConn.batches); after != before {
		t.Errorf("blank-session reads sent %d statements, want none", after-before)
	}
}

// TestD1LorebookLatestSessionPageResolvesTheNewestObservedScope pins the
// session-only lookup: it resolves the scope whose newest observation is the
// newest, and its answer is a paged window of that scope.
func TestD1LorebookLatestSessionPageResolvesTheNewestObservedScope(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)

	oldScope := d1LorebookRefExactScope("session-b", 3, 8, "module-b")
	newScope := d1LorebookRefExactScope("session-b", 5, 10, "module-c")

	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("ls1"), oldScope, base,
		d1LorebookRefEntry("e-old", "old", "older session lore"))); err != nil {
		t.Fatalf("older scope apply: %v", err)
	}
	newest, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("ls2"), newScope, base.Add(time.Hour),
		d1LorebookRefEntry("e-new", "new", "newer session lore")))
	if err != nil {
		t.Fatalf("newer scope apply: %v", err)
	}

	page, err := st.GetLorebookReferenceLatestSessionPage(ctx, "session-b", 20, 0)
	if err != nil {
		t.Fatalf("GetLorebookReferenceLatestSessionPage: %v", err)
	}
	if page.ScopeID != newest.ScopeID {
		t.Errorf("resolved scope %d, want the newest observed scope %d", page.ScopeID, newest.ScopeID)
	}
	if len(page.Entries) != 1 || page.Entries[0].Content != "newer session lore" {
		t.Errorf("page = %+v, want the newer scope's entry", page.Entries)
	}
	// The decoded identity is the one the writer stored, and it names this
	// session: a stored identity naming another session would be paging that
	// session's lore under this request.
	if page.Scope.ChatSessionID != "session-b" {
		t.Errorf("resolved scope = %+v, want session-b", page.Scope)
	}
	if page.Scope.CharacterIndex == nil || *page.Scope.CharacterIndex != 5 ||
		page.Scope.ChatIndex == nil || *page.Scope.ChatIndex != 10 {
		t.Errorf("the resolved scope lost its host indices: %+v", page.Scope)
	}
	if page.LatestSnapshot == nil || page.LatestSnapshot.SnapshotID != d1LorebookRefSnapshotID("ls2") {
		t.Errorf("page snapshot = %+v", page.LatestSnapshot)
	}
	// A bounded window applies here too.
	window, err := st.GetLorebookReferenceLatestSessionPage(ctx, "session-b", 1, 0)
	if err != nil {
		t.Fatalf("bounded latest-session page: %v", err)
	}
	if window.Limit != 1 || window.Total != 1 || len(window.Entries) != 1 {
		t.Errorf("bounded latest-session page = %+v", window)
	}

	// A session with no observation is the sentinel the route turns into an
	// empty page, not an error it has to special-case.
	if _, err := st.GetLorebookReferenceLatestSessionPage(ctx, "session-absent", 20, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("unobserved session error = %v, want ErrNotFound", err)
	}

	// A session that exists as a scope but has never been observed must NOT
	// resolve: the join is against observations, so the caller is told there is
	// nothing to show rather than shown an unverified catalog.
	if _, err := conn.Exec(ctx, `INSERT INTO lorebook_reference_scopes
		(chat_session_id, character_index, chat_index, enabled_modules_json, scope_identity_json)
		VALUES ('session-c', NULL, NULL, '[]', ?)`,
		`{"chat_session_id":"session-c","enabled_module_ids":[],"enabled_modules_observed":true}`); err != nil {
		t.Fatalf("seed unobserved scope: %v", err)
	}
	if _, err := st.GetLorebookReferenceLatestSessionPage(ctx, "session-c", 20, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("unobserved scope error = %v, want ErrNotFound", err)
	}

	// The request guard is the same as the exact page's, and it also runs before
	// the database is touched.
	for _, tc := range []struct {
		name    string
		session string
		limit   int
		offset  int
	}{
		{"blank session", "   ", 20, 0},
		{"zero limit", "session-b", 0, 0},
		{"negative limit", "session-b", -1, 0},
		{"oversized limit", "session-b", 101, 0},
		{"negative offset", "session-b", 20, -1},
	} {
		probe, probeConn := newD1ProbeStore(t)
		before := len(probeConn.queries) + len(probeConn.batches)
		if _, err := probe.GetLorebookReferenceLatestSessionPage(ctx, tc.session, tc.limit, tc.offset); !errors.Is(err, ErrInvalidLorebookReference) {
			t.Errorf("%s error = %v, want ErrInvalidLorebookReference", tc.name, err)
		}
		if after := len(probeConn.queries) + len(probeConn.batches); after != before {
			t.Errorf("%s sent %d statements, want none", tc.name, after-before)
		}
	}
}

// ---------------------------------------------------------------------------
// derived columns and statement shape
// ---------------------------------------------------------------------------

// TestD1LorebookDerivedColumnsUseTheSharedHelpers pins the diagnostic columns
// to the same helpers the MariaDB provider uses. These two columns are what a
// later session migration and a diff tool compare across providers, so a
// divergence here is invisible in the UI and expensive later.
func TestD1LorebookDerivedColumnsUseTheSharedHelpers(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-hash", 1, 1, "module-a")

	// A key, a second key, a comment, and a body, so the normalisation has
	// something to fold: extra whitespace and mixed case must collapse.
	entry := d1LorebookRefEntry("entry-1", "  Han-Eol  ", "He  will   take the exam.")
	entry.SecondKey = "EXAM"
	entry.Comment = "profile"
	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("h1"), scope, base, entry)); err != nil {
		t.Fatalf("apply: %v", err)
	}

	stored := d1LorebookRefStored(t, conn, d1LorebookRefScopeID(t, conn, scope))
	if len(stored) != 1 {
		t.Fatalf("stored entries = %d, want 1", len(stored))
	}
	if want := normalizeLorebookSearchText(entry); stored[0].Normalized != want {
		t.Errorf("normalized_search_text = %q, want %q", stored[0].Normalized, want)
	}
	if want := lorebookDiagnosticContentHash(entry.Content); stored[0].ContentHash != want {
		t.Errorf("content_hash = %q, want %q", stored[0].ContentHash, want)
	}
	// The normalisation must have actually done something, or the assertion
	// above is vacuous.
	if stored[0].Normalized == entry.Content {
		t.Error("the search text was not normalized; the shared helper was not exercised")
	}
	if !strings.Contains(stored[0].Normalized, "han-eol") {
		t.Errorf("the search text must be lowercased: %q", stored[0].Normalized)
	}

	// The raw Host key is stored verbatim, not the normalised text: the panel
	// shows what the host sent, and the normalised column is a search aid.
	var storedKey string
	if err := conn.QueryRow(ctx, `SELECT entry_key FROM lorebook_reference_entries`).Scan(&storedKey); err != nil {
		t.Fatalf("read entry_key: %v", err)
	}
	if storedKey != entry.Key {
		t.Errorf("entry_key = %q, want the raw Host key %q", storedKey, entry.Key)
	}
}

// TestD1LorebookStatementsKeepTheMariaDBShape records the statement contract the
// D1 provider sends, asserted on the text itself.
//
// Three of these are MariaDB idioms a future edit could "simplify" away without
// failing any behavioural test, which is exactly why they are pinned:
//
//   - the session lock is a MySQL upsert, spelled with ON CONFLICT here;
//   - the scope lookup uses IS rather than = for the host indices, because
//     MariaDB's <=> is NULL-safe and = is not;
//   - the demote sits inside the same batch as the snapshot and the entries, so
//     a partially applied observation cannot exist.
func TestD1LorebookStatementsKeepTheMariaDBShape(t *testing.T) {
	st, conn := newD1ProbeStore(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	scope := d1LorebookRefExactScope("session-shape", 1, 2, "module-a")

	if _, err := st.ApplyLorebookReferenceSnapshot(ctx, d1LorebookRefComplete(
		d1LorebookRefSnapshotID("sh1"), scope, base,
		d1LorebookRefEntry("e-1", "one", "body one"),
		d1LorebookRefEntry("e-2", "two", "body two"))); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(conn.batches) != 1 {
		t.Fatalf("batches = %d, want exactly 1 (one observation is one atomic unit)", len(conn.batches))
	}
	statements := conn.batches[0]
	if len(statements) != 5 {
		t.Errorf("batch statements = %d, want 5 (lock, snapshot, demote, two entries)", len(statements))
	}

	// The session lock, as a SQLite upsert.
	lock := statements[0]
	if !strings.Contains(lock.SQL, "lorebook_reference_session_locks") {
		t.Fatalf("first statement is not the session lock:\n%s", lock.SQL)
	}
	if !strings.Contains(lock.SQL, "ON CONFLICT(chat_session_id) DO UPDATE") {
		t.Errorf("the session lock must upsert, not fail on a repeat observation:\n%s", lock.SQL)
	}
	if strings.Contains(strings.ToUpper(lock.SQL), "ON DUPLICATE KEY") {
		t.Errorf("MySQL upsert syntax is not SQLite:\n%s", lock.SQL)
	}

	joined := ""
	for _, stmt := range statements {
		joined += stmt.SQL + "\n"
	}
	for _, want := range []string{
		"INSERT INTO lorebook_reference_snapshots",
		"UPDATE lorebook_reference_entries",
		"INSERT INTO lorebook_reference_entries",
		"WHERE scope_id = ? AND is_current = TRUE",
		"SET is_current = FALSE, lifecycle_state = ?, last_seen_at = ?",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("the batch is missing %q:\n%s", want, joined)
		}
	}
	// The demote is scoped by scope, never by snapshot, so one module set's
	// projection replacement cannot retire another module set's entries.
	if strings.Contains(joined, "snapshot_id = ? AND is_current = TRUE") {
		t.Errorf("the demote must be scope-scoped, not snapshot-scoped:\n%s", joined)
	}

	// The scope lookup is NULL-safe on the host indices and returns every
	// candidate for the Go-side identity comparison.
	var scopeSelect string
	for _, query := range conn.queries {
		if strings.Contains(query, "FROM lorebook_reference_scopes") {
			scopeSelect = query
			break
		}
	}
	if scopeSelect == "" {
		t.Fatalf("no scope lookup statement was recorded: %v", conn.queries)
	}
	if !strings.Contains(scopeSelect, "character_index IS ?") || !strings.Contains(scopeSelect, "chat_index IS ?") {
		t.Errorf("the scope lookup must use NULL-safe equality on both host indices:\n%s", scopeSelect)
	}
	if strings.Contains(scopeSelect, "LIMIT") {
		t.Errorf("the scope lookup must return every candidate for the identity comparison:\n%s", scopeSelect)
	}

	// The entry read pages with a bound window and orders by the Host's ordinal
	// with a stable tiebreak.
	page, err := st.GetLorebookReferenceCurrentPage(ctx, scope, 5, 0)
	if err != nil {
		t.Fatalf("page: %v", err)
	}
	if len(page.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(page.Entries))
	}
	entryQuery := ""
	for i := len(conn.queries) - 1; i >= 0; i-- {
		if strings.Contains(conn.queries[i], "FROM lorebook_reference_entries") {
			entryQuery = conn.queries[i]
			break
		}
	}
	for _, want := range []string{
		"WHERE scope_id = ? AND is_current = TRUE AND lifecycle_state = ?",
		"ORDER BY entry_ordinal ASC, entry_record_id ASC",
		"LIMIT ? OFFSET ?",
	} {
		if !strings.Contains(entryQuery, want) {
			t.Errorf("the entry read is missing %q:\n%s", want, entryQuery)
		}
	}

	// The unbounded prepare-turn read is genuinely unbounded: it adds no LIMIT,
	// because the caller ranks and budgets every candidate itself.
	if _, err := st.GetLorebookReferenceCurrent(ctx, scope); err != nil {
		t.Fatalf("unbounded read: %v", err)
	}
	unbounded := conn.queries[len(conn.queries)-1]
	if strings.Contains(strings.ToUpper(unbounded), "LIMIT") {
		t.Errorf("the prepare-turn read must not truncate the candidate set:\n%s", unbounded)
	}
}

// TestD1LorebookCapabilitiesAreAdvertised pins the consequence the routes
// actually depend on. Both capabilities are discovered by a type assertion, so
// a provider that implemented the methods but was not discoverable would still
// answer "lorebook_reference_store_unavailable" (503) on Cloudflare while every
// behavioural test above passed.
func TestD1LorebookCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)
	var asStore Store = st

	if _, ok := asStore.(LorebookReferenceStore); !ok {
		t.Error("D1 store must satisfy LorebookReferenceStore")
	}
	if _, ok := asStore.(LorebookReferenceExplorerStore); !ok {
		t.Error("D1 store must satisfy LorebookReferenceExplorerStore")
	}
	implemented := map[string]bool{}
	for _, status := range CapabilityReport(asStore) {
		implemented[status.Name] = status.Implemented
	}
	for _, name := range []string{"LorebookReferenceStore", "LorebookReferenceExplorerStore"} {
		if !implemented[name] {
			t.Errorf("the capability manifest reports %s as missing", name)
		}
	}
}
