package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// seeds
// ---------------------------------------------------------------------------

// d1WorldlineLineageSeed carries only the columns an ordering assertion varies.
// Everything else gets the values an automatic worldline import writes, so a test
// row reads like a real one and the assertions stay about order. An empty
// ForkSourceMessageID writes NULL, which the store must read back as the empty
// string; that is the same projection the NULL-mapping case asserts explicitly.
type d1WorldlineLineageSeed struct {
	ID                  int64
	SessionID           string
	ParentSessionID     string
	ImportedAt          string
	IdempotencyKey      string
	ForkTurn            int
	ForkSourceMessageID string
	ContractVersion     string
	LineageState        string
}

// d1WorldlineImportAt renders one lineage import instant as the canonical store
// writes it. The base is fixed so an ordering assertion reads as an offset
// instead of a wall-clock guess, and the rendering goes through d1TimeValue so the
// seeded text is the fixed-millisecond form the read orders lexicographically.
func d1WorldlineImportAt(step int) string {
	base := time.Date(2026, 8, 19, 1, 2, 3, 0, time.UTC)
	return d1TimeValue(base.Add(time.Duration(step) * time.Minute))
}

// d1WorldlineSeedLineage writes one fork-lineage row with an explicit id, because
// the id is the final tiebreak of the topology lineage order and a test that
// cannot choose an id cannot pin that order.
func d1WorldlineSeedLineage(t *testing.T, conn D1Conn, seed d1WorldlineLineageSeed) {
	t.Helper()
	contractVersion := seed.ContractVersion
	if contractVersion == "" {
		contractVersion = RisuWorldlineForkLineageContractVersion
	}
	lineageState := seed.LineageState
	if lineageState == "" {
		lineageState = "confirmed"
	}
	d1Exec(t, conn, `
		INSERT INTO session_fork_lineage (
			id, contract_version, lineage_state, chat_session_id,
			copied_from_session_id, fork_turn, fork_source_message_id, fork_source_role,
			idempotency_key, imported_at, divergence_marker,
			provenance_source, inheritance_mode, created_at, updated_at
		) VALUES (?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, 0), NULLIF(?, ''), 'user',
		          NULLIF(?, ''), ?, NULL, 'automatic_hook', 'none', ?, ?)`,
		seed.ID, contractVersion, lineageState, seed.SessionID,
		seed.ParentSessionID, seed.ForkTurn, seed.ForkSourceMessageID,
		seed.IdempotencyKey, seed.ImportedAt, seed.ImportedAt, seed.ImportedAt)
}

// d1WorldlineSeedCompleteTurns fills one session with complete exchanges up to
// maxTurn, using a recursive CTE so a case that needs thousands of turns costs
// one statement rather than thousands of round trips. The roles are written
// padded and mixed case on purpose: the completed-turn read normalises both, and
// a seed that normalised them here would hide a dropped LOWER or TRIM.
func d1WorldlineSeedCompleteTurns(t *testing.T, conn D1Conn, sessionID string, maxTurn int) {
	t.Helper()
	for _, role := range []string{"  User  ", "  Assistant  "} {
		d1Exec(t, conn, `
			WITH RECURSIVE seq(turn_index) AS (
				SELECT 1 UNION ALL SELECT turn_index + 1 FROM seq WHERE turn_index < ?
			)
			INSERT INTO chat_logs (chat_session_id, turn_index, role, content)
			SELECT ?, turn_index, ?, 'body' FROM seq`,
			maxTurn, sessionID, role)
	}
}

// d1WorldlineTurnsDigest renders the completed turns as a single ordered string
// so an ordering assertion reads as the exact expected sequence.
func d1WorldlineTurnsDigest(turns []WorldlineCompletedTurn) string {
	parts := make([]string, 0, len(turns))
	for _, turn := range turns {
		parts = append(parts, fmt.Sprintf("%s@%d", turn.ChatSessionID, turn.TurnIndex))
	}
	return strings.Join(parts, ",")
}

// d1WorldlineLineageIDDigest renders the lineage ids in the order returned, which
// is the order the topology read promises.
func d1WorldlineLineageIDDigest(records []ForkLineageRecord) string {
	parts := make([]string, 0, len(records))
	for _, record := range records {
		parts = append(parts, fmt.Sprintf("%d:%s", record.ID, record.ChatSessionID))
	}
	return strings.Join(parts, ",")
}

// d1WorldlineSeedFamily binds one stable character to each listed session. The
// host chat id is derived from the session so the unique (stable_character_id,
// host_chat_id) key never collides inside one family.
func d1WorldlineSeedFamily(t *testing.T, conn D1Conn, stableCharacterID string, sessionIDs ...string) {
	t.Helper()
	for _, sessionID := range sessionIDs {
		d1SeedRouteBinding(t, conn, SessionRouteBindingContractVersion,
			stableCharacterID, "chat-"+sessionID, sessionID, "active", "manual_attach", 1)
	}
}

// ---------------------------------------------------------------------------
// capability advertisement
// ---------------------------------------------------------------------------

// TestD1WorldlineTopologyCapabilityIsAdvertised pins that the read is reachable
// by the type assertion the presentation route performs. Implemented but not
// reachable would leave the route silently on its unavailable branch.
func TestD1WorldlineTopologyCapabilityIsAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var _ WorldlineTopologySnapshotStore = st

	implemented := map[string]bool{}
	for _, status := range CapabilityReport(st) {
		implemented[status.Name] = status.Implemented
	}
	if !implemented["WorldlineTopologySnapshotStore"] {
		t.Error("D1 provider must advertise WorldlineTopologySnapshotStore")
	}
}

// ---------------------------------------------------------------------------
// anchor resolution and error contract
// ---------------------------------------------------------------------------

// TestD1GetWorldlineTopologySnapshotRequiresARoutedAnchor pins the three ways the
// read can refuse before it assembles anything. The route maps ErrNotFound onto
// current_session_not_routed, so returning a bare error there would serialise as
// topology_snapshot_read_unavailable and tell the user the wrong thing.
func TestD1GetWorldlineTopologySnapshotRequiresARoutedAnchor(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if _, err := st.GetWorldlineTopologySnapshot(ctx, "   ", 10); err == nil {
		t.Error("a blank anchor session id must be refused")
	}
	if _, err := st.GetWorldlineTopologySnapshot(ctx, "never-routed", 10); !errors.Is(err, ErrNotFound) {
		t.Errorf("unrouted anchor err = %v, want ErrNotFound", err)
	}

	// A retired binding must not re-open a family: only active bindings decide
	// which stable character an anchor belongs to.
	d1SeedRouteBinding(t, conn, SessionRouteBindingContractVersion,
		"character-stable", "chat-retired", "retired-anchor", "retired", "manual_attach", 1)
	if _, err := st.GetWorldlineTopologySnapshot(ctx, "retired-anchor", 10); !errors.Is(err, ErrNotFound) {
		t.Errorf("retired binding err = %v, want ErrNotFound", err)
	}

	// Two active characters on one anchor is a corrupt ledger, not a family.
	d1SeedRouteBinding(t, conn, SessionRouteBindingContractVersion,
		"character-a", "chat-one", "ambiguous-anchor", "active", "manual_attach", 1)
	d1SeedRouteBinding(t, conn, SessionRouteBindingContractVersion,
		"character-b", "chat-two", "ambiguous-anchor", "active", "manual_attach", 1)
	if _, err := st.GetWorldlineTopologySnapshot(ctx, "ambiguous-anchor", 10); err == nil ||
		strings.Contains(err.Error(), "multiple stable characters") == false {
		t.Errorf("ambiguous anchor err = %v, want the multiple-characters refusal", err)
	}
}

// TestD1GetWorldlineTopologySnapshotIgnoresOtherCharacters pins that the family is
// the anchor character family and not every bound session: a second character
// with its own sessions must not appear in the graph.
func TestD1GetWorldlineTopologySnapshotIgnoresOtherCharacters(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-anchor", "m-anchor", "a-parent")
	d1WorldlineSeedFamily(t, conn, "character-other", "z-other")

	snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(snapshot.SessionIDs, ","); got != "m-anchor,a-parent" {
		t.Errorf("family = %s, want m-anchor,a-parent", got)
	}
	if snapshot.StableCharacterID != "character-anchor" {
		t.Errorf("stable character = %q, want character-anchor", snapshot.StableCharacterID)
	}
}

// ---------------------------------------------------------------------------
// family order and truncation
// ---------------------------------------------------------------------------

// TestD1GetWorldlineTopologySnapshotOrdersFamilyAnchorFirst pins the anchor-first
// sort key. The anchor sorts lexicographically in the middle of the family on
// purpose: with the CASE key dropped the anchor would be the third row, and with
// the lexicographic key dropped the order would be whatever the group-by emitted.
func TestD1GetWorldlineTopologySnapshotOrdersFamilyAnchorFirst(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-stable",
		"z-branch", "m-anchor", "a-parent", "b-other")

	snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(snapshot.SessionIDs, ","); got != "m-anchor,a-parent,b-other,z-branch" {
		t.Errorf("family order = %s, want m-anchor,a-parent,b-other,z-branch", got)
	}
	if snapshot.Truncated {
		t.Error("a four-session family read with limit 10 must not be truncated")
	}
	if snapshot.AnchorSessionID != "m-anchor" {
		t.Errorf("anchor = %q, want m-anchor", snapshot.AnchorSessionID)
	}
}

// TestD1GetWorldlineTopologySnapshotTruncatesFamilyBeforeLineageAndTurnReads pins
// that the cut bounds the two reads that follow it. A family read with no
// truncation would pull lineage and turns for a session the snapshot does not
// report, and the ViewModel would then resolve a fork edge to a session outside
// the family instead of reporting parent_outside_current_family.
func TestD1GetWorldlineTopologySnapshotTruncatesFamilyBeforeLineageAndTurnReads(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-stable",
		"m-anchor", "a-parent", "b-other", "z-dropped")

	// The dropped session is lexicographically last, which is exactly the row the
	// anchor-first ordering pushes past the limit. The anchor record is a complete
	// confirmed worldline tuple, so the lineage that survives the cut is a row the
	// presentation layer can actually turn into a fork edge.
	d1WorldlineSeedLineage(t, conn, d1WorldlineLineageSeed{
		ID: 41, SessionID: "m-anchor", ParentSessionID: "a-parent", ImportedAt: d1WorldlineImportAt(1),
		IdempotencyKey: "risu-worldline:anchor", ForkTurn: 7, ForkSourceMessageID: "fork-message",
	})
	d1WorldlineSeedLineage(t, conn, d1WorldlineLineageSeed{
		ID: 42, SessionID: "z-dropped", ParentSessionID: "a-parent", ImportedAt: d1WorldlineImportAt(1),
		IdempotencyKey: "risu-worldline:dropped",
	})
	d1SeedChatLog(t, conn, "z-dropped", 1, "user", "body")
	d1SeedChatLog(t, conn, "z-dropped", 1, "assistant", "body")
	d1SeedChatLog(t, conn, "m-anchor", 1, "user", "body")
	d1SeedChatLog(t, conn, "m-anchor", 1, "assistant", "body")

	snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(snapshot.SessionIDs, ","); got != "m-anchor,a-parent" {
		t.Errorf("truncated family = %s, want m-anchor,a-parent", got)
	}
	if !snapshot.Truncated {
		t.Error("a four-session family read with limit 2 must report truncation")
	}
	if got := d1WorldlineLineageIDDigest(snapshot.LineageRecords); got != "41:m-anchor" {
		t.Errorf("lineage = %s, want only the bounded family record 41", got)
	}
	if got := d1WorldlineTurnsDigest(snapshot.CompletedTurns); got != "m-anchor@1" {
		t.Errorf("completed turns = %s, want only m-anchor@1", got)
	}
}

// TestD1GetWorldlineTopologySnapshotClampsLimitAndDefault pins the limit contract.
// A non-positive limit means the documented default, and an over-large limit is
// clamped, so the same caller cannot ask either provider for an unbounded graph.
func TestD1GetWorldlineTopologySnapshotClampsLimitAndDefault(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-stable", "m-anchor", "a-parent", "b-other")

	for _, limit := range []int{0, -5} {
		snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", limit)
		if err != nil {
			t.Fatalf("limit %d: %v", limit, err)
		}
		if got := strings.Join(snapshot.SessionIDs, ","); got != "m-anchor,a-parent,b-other" {
			t.Errorf("limit %d family = %s, want the full three-session family", limit, got)
		}
		if snapshot.Truncated {
			t.Errorf("limit %d must not truncate a three-session family", limit)
		}
	}

	// A clamp must not change the meaning of a limit that fits: 1 keeps the anchor
	// alone and 5000 is clamped to the shared maximum, which is still far above
	// this family.
	bounded, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(bounded.SessionIDs, ","); got != "m-anchor" {
		t.Errorf("limit 1 family = %s, want m-anchor", got)
	}
	if !bounded.Truncated {
		t.Error("limit 1 over a three-session family must report truncation")
	}
	clamped, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 5000)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(clamped.SessionIDs, ","); got != "m-anchor,a-parent,b-other" {
		t.Errorf("clamped family = %s, want the full three-session family", got)
	}
}

// ---------------------------------------------------------------------------
// lineage order
// ---------------------------------------------------------------------------

// TestD1GetWorldlineTopologySnapshotOrdersLineageBySessionThenImportedAtThenID
// pins all three keys of the lineage order. Rows are chosen so each key is the
// only thing that can decide between two neighbours: the two sessions differ only
// by name, the two same-session rows at the older import differ only by id, and
// the two rows of the second session differ only by import time.
func TestD1GetWorldlineTopologySnapshotOrdersLineageBySessionThenImportedAtThenID(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-stable", "a-parent", "m-anchor")

	// Step 1 is the earliest import, step 3 the latest.
	early := d1WorldlineImportAt(1)
	mid := d1WorldlineImportAt(2)
	late := d1WorldlineImportAt(3)

	d1WorldlineSeedLineage(t, conn, d1WorldlineLineageSeed{
		ID: 11, SessionID: "a-parent", ImportedAt: early, IdempotencyKey: "k-11",
	})
	d1WorldlineSeedLineage(t, conn, d1WorldlineLineageSeed{
		ID: 13, SessionID: "a-parent", ImportedAt: early, IdempotencyKey: "k-13",
	})
	d1WorldlineSeedLineage(t, conn, d1WorldlineLineageSeed{
		ID: 12, SessionID: "a-parent", ImportedAt: mid, IdempotencyKey: "k-12",
	})
	d1WorldlineSeedLineage(t, conn, d1WorldlineLineageSeed{
		ID: 14, SessionID: "m-anchor", ImportedAt: mid, IdempotencyKey: "k-14",
	})
	d1WorldlineSeedLineage(t, conn, d1WorldlineLineageSeed{
		ID: 15, SessionID: "m-anchor", ImportedAt: late, IdempotencyKey: "k-15",
	})

	snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "a-parent", 10)
	if err != nil {
		t.Fatal(err)
	}
	// a-parent first by name; inside it the newest import first, with the highest
	// id breaking the tie at the older import; inside m-anchor the newest import
	// first, and there the later id also has the later import.
	if got := d1WorldlineLineageIDDigest(snapshot.LineageRecords); got != "12:a-parent,13:a-parent,11:a-parent,15:m-anchor,14:m-anchor" {
		t.Errorf("lineage order = %s", got)
	}
}

// TestD1GetWorldlineTopologySnapshotMapsNullLineageColumnsToZeroValues pins the
// NULL projection. A legacy manual record stores no idempotency key, no parent,
// and no fork turn, and the consumer distinguishes "has no key" by the empty
// string, so NULL must arrive as the zero value on both providers.
func TestD1GetWorldlineTopologySnapshotMapsNullLineageColumnsToZeroValues(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-stable", "m-anchor")
	d1WorldlineSeedLineage(t, conn, d1WorldlineLineageSeed{
		ID: 51, SessionID: "m-anchor", ImportedAt: d1WorldlineImportAt(1),
		ContractVersion: ForkLineageContractVersion, LineageState: "manual",
	})

	snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.LineageRecords) != 1 {
		t.Fatalf("lineage = %+v, want one record", snapshot.LineageRecords)
	}
	record := snapshot.LineageRecords[0]
	if record.ContractVersion != ForkLineageContractVersion || record.LineageState != "manual" {
		t.Errorf("record identity = %q/%q, want %q/manual", record.ContractVersion, record.LineageState, ForkLineageContractVersion)
	}
	if record.IdempotencyKey != "" || record.CopiedFromSessionID != "" || record.ForkTurn != 0 ||
		record.ScopeID != "" || record.ParentScopeID != "" || record.CopiedFromScopeID != "" ||
		record.ForkSourceMessageID != "" || record.DivergenceMarker != "" || record.InheritedItemsJSON != "" {
		t.Errorf("NULL columns must read as zero values, got %+v", record)
	}
	if record.ForkSourceRole != "user" || record.ProvenanceSource != "automatic_hook" || record.InheritanceMode != "none" {
		t.Errorf("populated columns were not read: %+v", record)
	}
}

// ---------------------------------------------------------------------------
// completed turns
// ---------------------------------------------------------------------------

// TestD1GetWorldlineTopologySnapshotCompletedTurnsRequireBothNonEmptyRoles pins the
// HAVING gate. A turn is a node only when it carries a real user side AND a real
// assistant side, so every half-finished or empty half must be excluded by SQL.
// Returning them would draw turn nodes for exchanges that never happened.
func TestD1GetWorldlineTopologySnapshotCompletedTurnsRequireBothNonEmptyRoles(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-stable", "m-anchor")

	// turn 1: a complete exchange.
	d1SeedChatLog(t, conn, "m-anchor", 1, "user", "hello")
	d1SeedChatLog(t, conn, "m-anchor", 1, "assistant", "hi")
	// turn 2: the user side only.
	d1SeedChatLog(t, conn, "m-anchor", 2, "user", "hello")
	// turn 3: both sides present, assistant side blank.
	d1SeedChatLog(t, conn, "m-anchor", 3, "user", "hello")
	d1SeedChatLog(t, conn, "m-anchor", 3, "assistant", "    ")
	// turn 4: both sides present, user side blank.
	d1SeedChatLog(t, conn, "m-anchor", 4, "user", "  ")
	d1SeedChatLog(t, conn, "m-anchor", 4, "assistant", "hi")
	// turn 5: padded and mixed-case roles with padded content, which is the form
	// the LOWER, TRIM and length tests exist for.
	d1SeedChatLog(t, conn, "m-anchor", 5, "  User  ", "  hello  ")
	d1SeedChatLog(t, conn, "m-anchor", 5, "  ASSISTANT  ", "  hi  ")
	// turn 0 and turn -1 are not conversation turns.
	d1SeedChatLog(t, conn, "m-anchor", 0, "user", "starter")
	d1SeedChatLog(t, conn, "m-anchor", 0, "assistant", "starter")
	d1SeedChatLog(t, conn, "m-anchor", -1, "user", "user")
	d1SeedChatLog(t, conn, "m-anchor", -1, "assistant", "assistant")

	snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := d1WorldlineTurnsDigest(snapshot.CompletedTurns); got != "m-anchor@1,m-anchor@5" {
		t.Errorf("completed turns = %s, want m-anchor@1,m-anchor@5", got)
	}
	if snapshot.TurnsTruncated {
		t.Error("two completed turns must not report turn truncation")
	}
}

// TestD1GetWorldlineTopologySnapshotOrdersCompletedTurnsByTurnThenSession pins the
// turn read order, including the priority of the two keys. Both sessions carry
// turns 1 and 2, so ordering by turn groups them into interleaved pairs and
// ordering by session groups them into per-session runs; only
// turn_index ASC then chat_session_id ASC produces the expected sequence. That
// sequence is also what decides which 4096 rows survive the shared cap.
func TestD1GetWorldlineTopologySnapshotOrdersCompletedTurnsByTurnThenSession(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-stable", "m-anchor", "a-parent")
	d1WorldlineSeedCompleteTurns(t, conn, "m-anchor", 2)
	d1WorldlineSeedCompleteTurns(t, conn, "a-parent", 2)

	snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 10)
	if err != nil {
		t.Fatal(err)
	}
	want := "a-parent@1,m-anchor@1,a-parent@2,m-anchor@2"
	if got := d1WorldlineTurnsDigest(snapshot.CompletedTurns); got != want {
		t.Errorf("completed turn order = %s, want %s", got, want)
	}
}

// TestD1GetWorldlineTopologySnapshotTruncatesCompletedTurnsAtTheSharedCap pins the
// shared turn cap. The read takes one row past the cap so truncation is decided
// from the result, and what must survive is the LOWEST turn indices: a cap that
// kept an arbitrary 4096 groups would show up as a hole in the turn range that
// the presentation layer reports as completed_turn_gap on every read.
func TestD1GetWorldlineTopologySnapshotTruncatesCompletedTurnsAtTheSharedCap(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-stable", "m-anchor")
	d1WorldlineSeedCompleteTurns(t, conn, "m-anchor", worldlineTopologyCompletedTurnLimit+1)

	snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.TurnsTruncated {
		t.Error("a family one turn past the shared cap must report turn truncation")
	}
	if got := len(snapshot.CompletedTurns); got != worldlineTopologyCompletedTurnLimit {
		t.Fatalf("completed turns = %d, want %d", got, worldlineTopologyCompletedTurnLimit)
	}
	if last := snapshot.CompletedTurns[len(snapshot.CompletedTurns)-1]; last.TurnIndex != worldlineTopologyCompletedTurnLimit {
		t.Errorf("last kept turn = %d, want the lowest %d turns to be kept",
			last.TurnIndex, worldlineTopologyCompletedTurnLimit)
	}
	// The family truncation flag is independent of the turn cap, and a family of
	// one session read with limit 1 is not truncated.
	if snapshot.Truncated {
		t.Error("a one-session family read with limit 1 must not report family truncation")
	}
}

// ---------------------------------------------------------------------------
// response shape
// ---------------------------------------------------------------------------

// TestD1GetWorldlineTopologySnapshotEmptyCollectionsEncodeAsArrays pins the zero
// case of a routed family with no lineage and no turns. The presentation route
// hands these collections straight to the encoder, so a nil slice would serialise
// as null where the contract promises an array, and a client rendering the graph
// would have to special-case it.
func TestD1GetWorldlineTopologySnapshotEmptyCollectionsEncodeAsArrays(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	d1WorldlineSeedFamily(t, conn, "character-stable", "m-anchor")

	snapshot, err := st.GetWorldlineTopologySnapshot(ctx, "m-anchor", 10)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SessionIDs == nil || snapshot.LineageRecords == nil || snapshot.CompletedTurns == nil {
		t.Fatalf("every collection must be non-nil: %+v", snapshot)
	}
	// Only the two collections that are empty here are encoded: a family with one
	// member must still report that member, and asserting [] for SessionIDs would
	// be asserting the wrong thing.
	for name, value := range map[string]any{
		"lineage_records": snapshot.LineageRecords,
		"completed_turns": snapshot.CompletedTurns,
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		if string(encoded) != "[]" {
			t.Errorf("%s encodes as %s, want []", name, encoded)
		}
	}
	if got := strings.Join(snapshot.SessionIDs, ","); got != "m-anchor" {
		t.Errorf("session ids = %s, want m-anchor", got)
	}
	if snapshot.StableCharacterID != "character-stable" || snapshot.AnchorSessionID != "m-anchor" {
		t.Errorf("snapshot envelope = %+v", snapshot)
	}
	if snapshot.Truncated || snapshot.TurnsTruncated {
		t.Errorf("an untruncated family must report no truncation: %+v", snapshot)
	}
}
