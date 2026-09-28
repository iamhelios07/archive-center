package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestD1ForkLineageManualRecord(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	imported := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)

	saved, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", ScopeID: "scope-a", ForkTurn: 3,
		ForkSourceMessageID: "m-1", DivergenceMarker: "mark-1", ImportedAt: imported,
	})
	if err != nil {
		t.Fatalf("SaveForkLineageRecord: %v", err)
	}
	if saved.ID == 0 {
		t.Fatal("save must report an id")
	}
	// Defaults mirror the MariaDB entry point.
	if saved.ContractVersion != ForkLineageContractVersion || saved.LineageState != "manual" {
		t.Errorf("defaults = %q/%q, want v1/manual", saved.ContractVersion, saved.LineageState)
	}
	// The MariaDB manual path returns the caller's provenance/inheritance fields
	// unchanged, even though it persists these defaults.
	if saved.ProvenanceSource != "" || saved.InheritanceMode != "" {
		t.Errorf("manual return provenance/inheritance = %q/%q, want unchanged empty fields", saved.ProvenanceSource, saved.InheritanceMode)
	}
	if saved.UpdatedAt.IsZero() {
		t.Error("the returned record must carry updated_at")
	}
	if !saved.ImportedAt.Equal(imported) {
		t.Errorf("imported_at = %v, want %v", saved.ImportedAt, imported)
	}

	// The manual path stores no idempotency key, and a non-positive fork turn
	// becomes NULL rather than 0.
	var key *string
	var forkTurn *int64
	var provenance, inheritance string
	if err := conn.QueryRow(ctx, `SELECT idempotency_key, fork_turn, provenance_source, inheritance_mode FROM session_fork_lineage WHERE id = ?`, saved.ID).
		Scan(&key, &forkTurn, &provenance, &inheritance); err != nil {
		t.Fatalf("read manual row: %v", err)
	}
	if provenance != "manual" || inheritance != "conservative_import" {
		t.Errorf("persisted provenance/inheritance = %q/%q, want manual/conservative_import", provenance, inheritance)
	}
	if key != nil {
		t.Errorf("manual path idempotency_key = %q, want NULL", *key)
	}
	if forkTurn == nil || *forkTurn != 3 {
		t.Errorf("fork_turn = %v, want 3", forkTurn)
	}
	zeroTurn, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", ScopeID: "scope-b", ImportedAt: imported.Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("second save: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT fork_turn FROM session_fork_lineage WHERE id = ?`, zeroTurn.ID).Scan(&forkTurn); err != nil {
		t.Fatalf("read zero turn: %v", err)
	}
	if forkTurn != nil {
		t.Errorf("zero fork turn = %d, want NULL", *forkTurn)
	}

	// Validation contract.
	if _, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{ScopeID: "x"}); err == nil {
		t.Error("an empty chat_session_id must be rejected")
	}
	if _, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{ChatSessionID: "s1", ForkSourceRole: "narrator"}); err == nil {
		t.Error("an unknown fork_source_role must be rejected")
	}
	if _, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k", ContractVersion: RisuWorldlineForkLineageContractVersion,
		LineageState: "confirmed",
	}); err == nil || !strings.Contains(err.Error(), "fork_source_role") {
		t.Errorf("a confirmed Risu worldline lineage without a role = %v, want rejection", err)
	}

	// Listing: newest import first, optional scope filter.
	all, err := st.ListForkLineageRecords(ctx, "s1", "", 0)
	if err != nil {
		t.Fatalf("ListForkLineageRecords: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("records = %d, want 2", len(all))
	}
	if all[0].ScopeID != "scope-b" || all[1].ScopeID != "scope-a" {
		t.Errorf("ordering = %q then %q, want scope-b (newer import) first", all[0].ScopeID, all[1].ScopeID)
	}
	scoped, err := st.ListForkLineageRecords(ctx, "s1", "scope-a", 0)
	if err != nil {
		t.Fatalf("scoped list: %v", err)
	}
	if len(scoped) != 1 || scoped[0].ID != saved.ID {
		t.Errorf("scope filter = %+v", scoped)
	}
	absent, err := st.ListForkLineageRecords(ctx, "s1", "missing", 0)
	if err != nil {
		t.Fatalf("absent scope list: %v", err)
	}
	if len(absent) != 0 {
		t.Errorf("absent scope = %d rows, want 0", len(absent))
	}
	if _, err := st.ListForkLineageRecords(ctx, "other", "", 0); err != nil {
		t.Fatalf("list other session: %v", err)
	}
}

// TestD1ForkLineageAutomaticMerge walks the dominance matrix the idempotent path
// must preserve: replayed imports converge instead of duplicating, a confirmed
// lineage resists rewriting, the v1->v2 upgrade is the one sanctioned overwrite,
// and a confirmed winner still receives the first observed message-origin map.
func TestD1ForkLineageAutomaticMerge(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)

	first, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k1", ScopeID: "scope-1",
		DivergenceMarker: "first", ImportedAt: base,
	})
	if err != nil {
		t.Fatalf("first automatic save: %v", err)
	}
	if first.ProvenanceSource != "automatic_hook" || first.InheritanceMode != "none" {
		t.Errorf("automatic defaults = %q/%q, want automatic_hook/none", first.ProvenanceSource, first.InheritanceMode)
	}
	if first.DivergenceMarker != "first" {
		t.Errorf("readback marker = %q, want first", first.DivergenceMarker)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_fork_lineage WHERE chat_session_id = 's1'`); got != 1 {
		t.Fatalf("rows after first save = %d, want 1 (replays converge, not duplicate)", got)
	}

	// An equal-or-newer replay overwrites the non-confirmed row.
	replay, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k1", ScopeID: "scope-1",
		DivergenceMarker: "second", ImportedAt: base.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("newer replay: %v", err)
	}
	if replay.DivergenceMarker != "second" {
		t.Errorf("newer replay marker = %q, want second", replay.DivergenceMarker)
	}

	// An older replay must not regress the row.
	regressed, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k1", ScopeID: "scope-1",
		DivergenceMarker: "older-attempt", ImportedAt: base.Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("older replay: %v", err)
	}
	if regressed.DivergenceMarker != "second" {
		t.Errorf("older replay returned %q, want the newer stored marker second", regressed.DivergenceMarker)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_fork_lineage WHERE chat_session_id = 's1'`); got != 1 {
		t.Fatalf("rows after older replay = %d, want 1", got)
	}

	// A confirmed lineage wins even against a newer non-upgrade write.
	confirmed, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k2", ScopeID: "scope-2",
		LineageState: "confirmed", DivergenceMarker: "confirmed-1", ImportedAt: base,
	})
	if err != nil {
		t.Fatalf("confirmed save: %v", err)
	}
	if confirmed.LineageState != "confirmed" {
		t.Fatalf("confirmed readback state = %q", confirmed.LineageState)
	}
	newer, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k2", ScopeID: "scope-2",
		DivergenceMarker: "newer-but-losing", ImportedAt: base.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("newer non-upgrade save: %v", err)
	}
	if newer.DivergenceMarker != "confirmed-1" {
		t.Errorf("confirmed row was rewritten to %q; it must win over a newer non-upgrade write", newer.DivergenceMarker)
	}

	// The v1-confirmed -> v2-confirmed upgrade is the sanctioned overwrite.
	upgrade, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k2", ScopeID: "scope-2",
		ContractVersion: RisuWorldlineForkLineageContractVersion, LineageState: "confirmed",
		ForkSourceRole: "user", DivergenceMarker: "upgraded", ImportedAt: base.Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("upgrade save: %v", err)
	}
	if upgrade.ContractVersion != RisuWorldlineForkLineageContractVersion || upgrade.ForkSourceRole != "user" {
		t.Errorf("upgrade readback = %q/%q, want v2 with role user", upgrade.ContractVersion, upgrade.ForkSourceRole)
	}
	if upgrade.DivergenceMarker != "upgraded" || !upgrade.ImportedAt.Equal(base.Add(-time.Hour)) {
		t.Errorf("upgrade must overwrite even an older timestamp: %+v", upgrade)
	}

	// A confirmed winner receives the first observed message-origin map while
	// keeping every other confirmed field.
	withItems, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k2", ScopeID: "scope-2",
		LineageState: "confirmed", InheritedItemsJSON: `["m1"]`,
		DivergenceMarker: "must-not-land", ImportedAt: base.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatalf("attach save: %v", err)
	}
	if withItems.InheritedItemsJSON != `["m1"]` {
		t.Errorf("inherited items = %q, want [\"m1\"] attached to the confirmed winner", withItems.InheritedItemsJSON)
	}
	if withItems.DivergenceMarker != "upgraded" {
		t.Errorf("attach must not rewrite other confirmed fields, marker = %q", withItems.DivergenceMarker)
	}

	// The map is attached once; a later replay cannot replace it.
	replayedItems, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k2", ScopeID: "scope-2",
		LineageState: "confirmed", InheritedItemsJSON: `["m2"]`,
		DivergenceMarker: "still-losing", ImportedAt: base.Add(4 * time.Hour),
	})
	if err != nil {
		t.Fatalf("second attach save: %v", err)
	}
	if replayedItems.InheritedItemsJSON != `["m1"]` {
		t.Errorf("first observed map must stick, got %q", replayedItems.InheritedItemsJSON)
	}

	// Both automatic keys coexist; the ledger is per key.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_fork_lineage WHERE chat_session_id = 's1'`); got != 2 {
		t.Errorf("distinct keys must produce distinct rows, got %d", got)
	}
	missing, err := st.ListForkLineageRecords(ctx, "s1", "", 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(missing) != 2 {
		t.Errorf("list rows = %d, want 2", len(missing))
	}
}

// d1ForkUpsertArgs builds the guarded upsert parameters the same way the store
// does, so the test can fire the SQL directly against a row the Go-side decision
// would never choose to overwrite.
func d1ForkUpsertArgs(record ForkLineageRecord) []any {
	record.ContractVersion = firstNonEmptyString(record.ContractVersion, ForkLineageContractVersion)
	record.LineageState = firstNonEmptyString(record.LineageState, "manual")
	record.ChatSessionID = strings.TrimSpace(record.ChatSessionID)
	record.ForkSourceRole = strings.TrimSpace(record.ForkSourceRole)
	record.IdempotencyKey = strings.TrimSpace(record.IdempotencyKey)
	return []any{
		record.ContractVersion, record.LineageState, record.ChatSessionID,
		d1NullableString(record.ScopeID), d1NullableString(record.ParentScopeID),
		d1NullableString(record.CopiedFromScopeID), d1NullableString(record.CopiedFromSessionID),
		nullableForkTurn(record.ForkTurn), d1NullableString(record.ForkSourceMessageID),
		d1NullableString(record.ForkSourceRole), record.IdempotencyKey,
		d1TimeValue(record.ImportedAt), d1NullableString(record.DivergenceMarker),
		firstNonEmptyString(record.ProvenanceSource, "automatic_hook"),
		firstNonEmptyString(record.InheritanceMode, "none"),
		d1NullableString(record.InheritedItemsJSON), d1TimeValue(record.CreatedAt),
		ForkLineageContractVersion, RisuWorldlineForkLineageContractVersion,
	}
}

// TestD1ForkLineageUpsertGuardProtectsConfirmed proves the SQL-side guard without
// concurrency: MariaDB relies on FOR UPDATE row locks, D1 has none, so the
// dominance decision itself must live in the statement a stale decision fires.
func TestD1ForkLineageUpsertGuardProtectsConfirmed(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)

	seed := ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k-guard", ScopeID: "scope-g",
		LineageState: "confirmed", DivergenceMarker: "confirmed-marker", ImportedAt: base,
	}
	if _, err := st.SaveForkLineageRecord(ctx, seed); err != nil {
		t.Fatalf("seed confirmed row: %v", err)
	}

	clobber := ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k-guard", ScopeID: "scope-g",
		LineageState: "manual", DivergenceMarker: "clobber", ImportedAt: base.Add(2 * time.Hour),
	}
	changes, err := conn.Exec(ctx, d1ForkLineageUpsertSQL, d1ForkUpsertArgs(clobber)...)
	if err != nil {
		t.Fatalf("guarded upsert against confirmed row: %v", err)
	}
	if changes != 0 {
		t.Errorf("a non-upgrade write reported %d changes against a confirmed row, want 0", changes)
	}
	var marker string
	if err := conn.QueryRow(ctx, `SELECT divergence_marker FROM session_fork_lineage WHERE idempotency_key = 'k-guard'`).
		Scan(&marker); err != nil {
		t.Fatalf("read after rejected write: %v", err)
	}
	if marker != "confirmed-marker" {
		t.Errorf("confirmed marker = %q, want confirmed-marker (row must be untouched)", marker)
	}

	// A confirmed v1 challenger without the v2 upgrade is still not sanctioned.
	confirmedChallenger := clobber
	confirmedChallenger.LineageState = "confirmed"
	confirmedChallenger.ContractVersion = ForkLineageContractVersion
	changes, err = conn.Exec(ctx, d1ForkLineageUpsertSQL, d1ForkUpsertArgs(confirmedChallenger)...)
	if err != nil {
		t.Fatalf("guarded upsert with confirmed v1 challenger: %v", err)
	}
	if changes != 0 {
		t.Errorf("same-version confirmed challenger reported %d changes, want 0 (only the v1->v2 upgrade is sanctioned)", changes)
	}

	// A non-confirmed row accepts a newer write through the guard.
	loose := ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k-loose", ScopeID: "scope-l",
		DivergenceMarker: "loose-1", ImportedAt: base,
	}
	if _, err := conn.Exec(ctx, d1ForkLineageUpsertSQL, d1ForkUpsertArgs(loose)...); err != nil {
		t.Fatalf("insert loose row: %v", err)
	}
	loose.DivergenceMarker = "loose-2"
	loose.ImportedAt = base.Add(time.Hour)
	changes, err = conn.Exec(ctx, d1ForkLineageUpsertSQL, d1ForkUpsertArgs(loose)...)
	if err != nil {
		t.Fatalf("guarded upsert on loose row: %v", err)
	}
	if changes != 1 {
		t.Errorf("newer write on non-confirmed row = %d changes, want 1", changes)
	}
	if err := conn.QueryRow(ctx, `SELECT divergence_marker FROM session_fork_lineage WHERE idempotency_key = 'k-loose'`).
		Scan(&marker); err != nil {
		t.Fatalf("read loose row: %v", err)
	}
	if marker != "loose-2" {
		t.Errorf("loose marker = %q, want loose-2", marker)
	}

	// But it rejects an older replay at the statement level.
	loose.DivergenceMarker = "loose-old"
	loose.ImportedAt = base.Add(-time.Hour)
	changes, err = conn.Exec(ctx, d1ForkLineageUpsertSQL, d1ForkUpsertArgs(loose)...)
	if err != nil {
		t.Fatalf("guarded upsert with older replay: %v", err)
	}
	if changes != 0 {
		t.Errorf("older replay on loose row = %d changes, want 0", changes)
	}
	if err := conn.QueryRow(ctx, `SELECT divergence_marker FROM session_fork_lineage WHERE idempotency_key = 'k-loose'`).
		Scan(&marker); err != nil {
		t.Fatalf("read loose row after replay: %v", err)
	}
	if marker != "loose-2" {
		t.Errorf("older replay changed the row to %q; the guard must reject it", marker)
	}
}

// d1ForkRaceConn injects one completed mutation after the first lineage row has
// been scanned. It models the D1 interleaving that can occur between the
// optimistic dominance read and its readback without needing a second process.
type d1ForkRaceConn struct {
	D1Conn
	mutateNextLineageRead func(context.Context) error
}

func (c *d1ForkRaceConn) QueryRow(ctx context.Context, query string, args ...any) D1Row {
	row := c.D1Conn.QueryRow(ctx, query, args...)
	if c.mutateNextLineageRead == nil || !strings.Contains(query, "FROM session_fork_lineage") {
		return row
	}
	mutate := c.mutateNextLineageRead
	c.mutateNextLineageRead = nil
	return d1ForkRaceRow{D1Row: row, afterScan: func() error { return mutate(ctx) }}
}

type d1ForkRaceRow struct {
	D1Row
	afterScan func() error
}

func (r d1ForkRaceRow) Scan(dest ...any) error {
	if err := r.D1Row.Scan(dest...); err != nil {
		return err
	}
	return r.afterScan()
}

// TestD1ForkLineageReadbackMismatchFailsLoudly verifies the last line of defence
// for a D1 interleaving: a dominant row may change after the optimistic decision,
// so the save must fail instead of returning an unexpected merged record.
func TestD1ForkLineageReadbackMismatchFailsLoudly(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	base := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)

	seed := ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k-race", ScopeID: "scope-r",
		LineageState: "confirmed", DivergenceMarker: "winner", ImportedAt: base.Add(time.Hour),
	}
	if _, err := st.SaveForkLineageRecord(ctx, seed); err != nil {
		t.Fatalf("seed confirmed row: %v", err)
	}

	// The first SELECT obtains the confirmed winner. Immediately afterwards a
	// simulated concurrent writer changes it; the following readback must reject
	// the result because it differs from the selected winner.
	st.conn = &d1ForkRaceConn{
		D1Conn: conn,
		mutateNextLineageRead: func(ctx context.Context) error {
			_, err := conn.Exec(ctx, `UPDATE session_fork_lineage
				SET divergence_marker = 'changed-by-race' WHERE idempotency_key = 'k-race'`)
			return err
		},
	}
	if _, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k-race", ScopeID: "scope-r",
		LineageState: "confirmed", DivergenceMarker: "loser",
		ImportedAt: base, // older: the stored confirmed row wins
	}); err == nil {
		t.Fatal("a readback that disagrees with the decision must fail loudly")
	} else if !strings.Contains(err.Error(), "readback mismatch") {
		t.Errorf("drift surfaced as %v, want a readback mismatch", err)
	}

	// Restore the normal connector. With no drift, the confirmed winner remains
	// stable and the same write returns it successfully.
	st.conn = conn
	restored, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{
		ChatSessionID: "s1", IdempotencyKey: "k-race", ScopeID: "scope-r",
		LineageState: "confirmed", DivergenceMarker: "loser",
		ImportedAt: base.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("undrifted save: %v", err)
	}
	if restored.DivergenceMarker != "changed-by-race" {
		t.Errorf("confirmed winner = %q, want changed-by-race", restored.DivergenceMarker)
	}
	if _, err := st.SaveForkLineageRecord(ctx, ForkLineageRecord{ChatSessionID: "", IdempotencyKey: "k"}); err == nil {
		t.Error("an empty chat_session_id must still be rejected")
	}
	if _, err := st.d1ForkLineageByIdempotencyKey(ctx, "s1", "missing-key"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing key = %v, want ErrNotFound", err)
	}
}
