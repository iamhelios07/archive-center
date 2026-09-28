package store

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Tests for the D1 canonical-tail replacement and rollback.
//
// The assertions are deliberately about what SURVIVES, not about what the
// replacement wrote. A replacement that only proves "the new text is there" can
// pass while a derived row from the discarded turn is still being served to
// recall, and that is the failure these tests exist to catch.

// ---------------------------------------------------------------------------
// seeds
// ---------------------------------------------------------------------------

// d1LogicalTurnScene is one session with three canonical turns and a derived
// row at every table the tail sweep is supposed to reach. The middle turn is
// what a replacement must leave alone, so every assertion can be written as a
// pair: the tail turn is gone, the earlier turn is intact.
type d1LogicalTurnScene struct {
	SessionID  string
	PriorRev   string
	TailRev    string
	Successor  string
	EvidenceID int64
	IdentityID string
	UnitID     string
}

// d1LogicalTurnSeed builds the scene. Turn 3 is the canonical tail.
func d1LogicalTurnSeed(t *testing.T, conn *sqliteD1Conn, sessionID string) d1LogicalTurnScene {
	t.Helper()
	ctx := context.Background()
	scene := d1LogicalTurnScene{
		SessionID: sessionID,
		PriorRev:  sessionID + "-rev-2",
		TailRev:   sessionID + "-rev-3",
		Successor: sessionID + "-rev-3b",
		UnitID:    sessionID + "-unit-3",
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("seed scene: %v\n%s", err, query)
		}
	}
	for turn := 1; turn <= 3; turn++ {
		exec(`INSERT INTO chat_logs (chat_session_id, turn_index, role, content, created_at) VALUES (?, ?, 'user', ?, ?)`,
			sessionID, turn, "user turn "+string(rune('0'+turn)), "2026-01-0"+string(rune('0'+turn))+"T00:00:00.000Z")
		exec(`INSERT INTO chat_logs (chat_session_id, turn_index, role, content, created_at) VALUES (?, ?, 'assistant', ?, ?)`,
			sessionID, turn, "assistant turn "+string(rune('0'+turn)), "2026-01-0"+string(rune('0'+turn))+"T00:00:00.000Z")
	}
	// One accepted source per canonical turn. Turn 3 carries the text the
	// replacement is about to replace.
	exec(`INSERT INTO memory_source_revisions (
		source_revision, chat_session_id, logical_turn_id, turn_index,
		raw_user_content, raw_assistant_content, combined_content_hash,
		hash_algorithm, host_observed_at_ms, lifecycle_state
	) VALUES (?, ?, 'turn-2', 2, 'u2', 'a2', 'hash-2', 'sha256', 2, 'active')`, scene.PriorRev, sessionID)
	exec(`INSERT INTO memory_source_revisions (
		source_revision, chat_session_id, logical_turn_id, turn_index,
		raw_user_content, raw_assistant_content, combined_content_hash,
		hash_algorithm, host_observed_at_ms, lifecycle_state
	) VALUES (?, ?, 'turn-3', 3, 'u3', 'a3', 'hash-3', 'sha256', 3, 'active')`, scene.TailRev, sessionID)

	registryID := d1SeedStatusRegistry(t, conn, sessionID, "mood")
	for turn := 2; turn <= 3; turn++ {
		revision := scene.PriorRev
		if turn == 3 {
			revision = scene.TailRev
		}
		exec(`INSERT INTO memories (chat_session_id, turn_index, summary_json) VALUES (?, ?, ?)`,
			sessionID, turn, `{"summary":"derived at turn `+string(rune('0'+turn))+`"}`)
		exec(`INSERT INTO direct_evidence_records (
			chat_session_id, evidence_text, source_turn_start, source_turn_end
		) VALUES (?, ?, ?, ?)`, sessionID, "evidence at turn "+string(rune('0'+turn)), turn, turn)
		exec(`INSERT INTO effective_input_logs (chat_session_id, turn_index, effective_input) VALUES (?, ?, ?)`,
			sessionID, turn, "effective "+string(rune('0'+turn)))
		exec(`INSERT INTO character_states (chat_session_id, character_name, turn_index) VALUES (?, ?, ?)`,
			sessionID, "Mira", turn)
		exec(`INSERT INTO world_rules (chat_session_id, scope, category, "key", source_turn) VALUES (?, 'global', 'rule', ?, ?)`,
			sessionID, "rule at turn "+string(rune('0'+turn)), turn)
		// The current-value slot is keyed by owner, not by turn, so the two
		// turns need two owners: otherwise the second insert would collide on
		// the schema's UNIQUE and the "turn 2 survives" assertion would be
		// vacuous.
		exec(`INSERT INTO status_current_values (
			chat_session_id, registry_id, status_key, owner_scope, owner_id,
			value_kind, value_json, evidence_json, source_turn
		) VALUES (?, ?, 'mood', 'character', ?, 'text', '"mood"',
			'{"source_revision":"`+revision+`"}', ?)`,
			sessionID, registryID, "Mira"+string(rune('0'+turn)), turn)
	}
	if err := conn.QueryRow(ctx, `SELECT id FROM direct_evidence_records WHERE chat_session_id = ? AND source_turn_end = 3`,
		sessionID).Scan(&scene.EvidenceID); err != nil {
		t.Fatalf("read seeded evidence id: %v", err)
	}

	// A precise unit and its derivation edge, both owned by the tail revision.
	// The companion identity exists at turn 1 so the link below is genuinely
	// cross-turn: the sweep must delete the link because it TOUCHES the retired
	// identity, not because both endpoints happen to be in the replaced range.
	scene.IdentityID = sessionID + "-entity-mira"
	companionID := sessionID + "-entity-companion"
	d1SeedIdentity(t, conn, scene.IdentityID, sessionID, "local", "character", "Mira", "active", "accepted", scene.TailRev, 3, 3)
	d1SeedIdentity(t, conn, companionID, sessionID, "local", "character", "Companion", "active", "accepted", scene.PriorRev, 1, 1)
	d1SeedIdentitySurface(t, conn, sessionID+"-surface-3", scene.IdentityID, sessionID, "local", "alias", "Mira", "mira", "source_turn", scene.TailRev, "accepted", 3)
	d1SeedIdentityLink(t, conn, sessionID+"-link-3", sessionID, scene.IdentityID, companionID, "knows", "accepted", "")
	exec(`INSERT INTO precise_memory_units (
		unit_id, chat_session_id, source_turn_start, source_turn_end, source_contract,
		source_revision, source_content_hash, source_role, source_span_start, source_span_end,
		evidence_excerpt, evidence_hash, root_evidence_id, direct_evidence_ids_json,
		memory_kind, payload_json, truth_scope, epistemic_mode, authority_class,
		admission_state, review_state, visibility, idempotency_key
	) VALUES (?, ?, 3, 3, 'source_acceptance_observation.v1', ?, 'ch3', 'combined_turn_pair', 0, 1,
		'evidence at turn 3', 'eh3', ?, '[]', 'event', '{"summary":"unit at turn 3"}',
		'objective', 'direct', 'objective_world_state', 'committed', 'source_observed', 'public', ?)`,
		scene.UnitID, sessionID, scene.TailRev, scene.EvidenceID, "ik-"+scene.UnitID)
	exec(`INSERT INTO memory_derivation_dependencies (
		contract_version, chat_session_id, source_revision, root_source_pointer,
		child_artifact_type, child_artifact_id, parent_artifact_type, parent_artifact_id,
		derivation_version, extractor_version, index_version
	) VALUES ('memory_derivation_dependency.v1', ?, ?, ?, 'precise_memory_unit', ?, 'direct_evidence', ?,
		'v1', 'x', 'i')`,
		sessionID, scene.TailRev, "source_revision:"+scene.TailRev, scene.UnitID,
		strconv.FormatInt(scene.EvidenceID, 10))
	return scene
}

// d1LogicalTurnSuccessor builds the source revision a replacement registers.
func d1LogicalTurnSuccessor(scene d1LogicalTurnScene, turn int, user, assistant string, revision string) *MemorySourceRevision {
	return &MemorySourceRevision{
		SourceRevision:      revision,
		ChatSessionID:       scene.SessionID,
		LogicalTurnID:       "turn-" + strconv.Itoa(turn),
		TurnIndex:           turn,
		UserContent:         user,
		AssistantContent:    assistant,
		CombinedContentHash: "hash-" + revision,
		HashAlgorithm:       "sha256",
		HostObservedAtMS:    4242,
		CreatedAt:           time.Date(2026, 7, 22, 1, 2, 3, 0, time.UTC),
	}
}

func d1LogicalTurnSQL(commands []D1Statement) []string {
	out := make([]string, 0, len(commands))
	for _, command := range commands {
		out = append(out, command.SQL)
	}
	return out
}

// d1LogicalTurnAssertGone fails with the surviving row count for each table the
// replacement or rollback was supposed to clear. Reporting every table at once
// matters: a test that checks them one at a time stops at the first failure and
// hides how much of the invalidation set actually ran.
func d1LogicalTurnAssertGone(t *testing.T, conn *sqliteD1Conn, label string, checks map[string]struct {
	query string
	args  []any
}) {
	t.Helper()
	for name, check := range checks {
		if got := d1Count(t, conn, check.query, check.args...); got != 0 {
			t.Errorf("%s: %s still has %d rows; a derived row that survives points at a turn that no longer exists", label, name, got)
		}
	}
}

// ---------------------------------------------------------------------------
// ReplaceLogicalTurn
// ---------------------------------------------------------------------------

// TestD1ReplaceLogicalTurnInvalidatesEveryDerivedRow is the core contract: the
// tail turn is regenerated and NOTHING derived from the discarded turn survives
// as live data, while the earlier turn is untouched.
func TestD1ReplaceLogicalTurnInvalidatesEveryDerivedRow(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-invalidate")

	if err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID:    scene.SessionID,
		TurnIndex:        3,
		UserContent:      "regenerated user",
		AssistantContent: "regenerated assistant",
		CreatedAt:        time.Date(2026, 7, 22, 1, 2, 3, 0, time.UTC),
		SourceRevision:   d1LogicalTurnSuccessor(scene, 3, "regenerated user", "regenerated assistant", scene.Successor),
	}); err != nil {
		t.Fatalf("ReplaceLogicalTurn: %v", err)
	}

	// The canonical tail now carries only the replacement.
	rows, err := conn.Query(ctx, `SELECT role, content FROM chat_logs WHERE chat_session_id = ? AND turn_index = 3 ORDER BY role`, scene.SessionID)
	if err != nil {
		t.Fatalf("read replaced turn: %v", err)
	}
	var roles []string
	for rows.Next() {
		var role, content string
		if err := rows.Scan(&role, &content); err != nil {
			t.Fatalf("scan replaced turn: %v", err)
		}
		roles = append(roles, role+":"+content)
	}
	_ = rows.Close()
	if len(roles) != 2 || roles[0] != "assistant:regenerated assistant" || roles[1] != "user:regenerated user" {
		t.Errorf("replaced turn rows = %#v, want the two regenerated rows", roles)
	}

	d1LogicalTurnAssertGone(t, conn, "replacement at turn 3", map[string]struct {
		query string
		args  []any
	}{
		"memories at turn 3":              {`SELECT COUNT(*) FROM memories WHERE chat_session_id = ? AND turn_index >= 3`, []any{scene.SessionID}},
		"evidence at turn 3":              {`SELECT COUNT(*) FROM direct_evidence_records WHERE chat_session_id = ? AND source_turn_end >= 3`, []any{scene.SessionID}},
		"effective inputs at turn 3":      {`SELECT COUNT(*) FROM effective_input_logs WHERE chat_session_id = ? AND turn_index >= 3`, []any{scene.SessionID}},
		"character states at turn 3":      {`SELECT COUNT(*) FROM character_states WHERE chat_session_id = ? AND turn_index >= 3`, []any{scene.SessionID}},
		"world rules at turn 3":           {`SELECT COUNT(*) FROM world_rules WHERE chat_session_id = ? AND source_turn >= 3`, []any{scene.SessionID}},
		"status current values at turn 3": {`SELECT COUNT(*) FROM status_current_values WHERE chat_session_id = ? AND source_turn >= 3`, []any{scene.SessionID}},
		"identities at turn 3":            {`SELECT COUNT(*) FROM entity_identities WHERE chat_session_id = ? AND source_turn >= 3`, []any{scene.SessionID}},
		"surfaces at turn 3":              {`SELECT COUNT(*) FROM entity_identity_surfaces WHERE chat_session_id = ? AND source_turn >= 3`, []any{scene.SessionID}},
		"links touching turn 3":           {`SELECT COUNT(*) FROM entity_identity_links WHERE chat_session_id = ? AND source_entity_id = ?`, []any{scene.SessionID, scene.IdentityID}},
	})

	// The precise unit is RETAINED but invalidated, not deleted: its derivation
	// history is what makes a rebuild auditable. It must however stop being
	// live, or a reader would keep serving text from the discarded turn.
	var unitLifecycle string
	if err := conn.QueryRow(ctx, `SELECT lifecycle_state FROM precise_memory_units WHERE unit_id = ?`, scene.UnitID).Scan(&unitLifecycle); err != nil {
		t.Fatalf("read precise unit lifecycle: %v", err)
	}
	if unitLifecycle != "invalidated" {
		t.Errorf("precise unit lifecycle = %q, want invalidated", unitLifecycle)
	}
	var depLifecycle string
	if err := conn.QueryRow(ctx, `SELECT lifecycle_state FROM memory_derivation_dependencies WHERE child_artifact_id = ?`, scene.UnitID).Scan(&depLifecycle); err != nil {
		t.Fatalf("read dependency lifecycle: %v", err)
	}
	if depLifecycle != "invalidated" {
		t.Errorf("derivation edge lifecycle = %q, want invalidated", depLifecycle)
	}

	// Turn 2 must be entirely intact: a replacement is a tail operation.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memories WHERE chat_session_id = ? AND turn_index = 2`, scene.SessionID); got != 1 {
		t.Errorf("turn 2 memories = %d, want 1", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = ? AND turn_index = 2`, scene.SessionID); got != 2 {
		t.Errorf("turn 2 chat logs = %d, want 2", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM entity_identities WHERE chat_session_id = ? AND source_turn < 3`, scene.SessionID); got != 1 {
		t.Errorf("identities before turn 3 = %d, want 1; the sweep reached past the replaced turn", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM status_current_values WHERE chat_session_id = ? AND source_turn < 3`, scene.SessionID); got != 1 {
		t.Errorf("status current values before turn 3 = %d, want 1; the sweep reached past the replaced turn", got)
	}
}

// TestD1ReplaceLogicalTurnSupersedesPriorRevisionAndFencesTheSuccessor pins the
// source-revision chain: the discarded revision must point at the accepted
// successor, and the successor must be the live one.
func TestD1ReplaceLogicalTurnSupersedesPriorRevisionAndFencesTheSuccessor(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-successor")

	successor := d1LogicalTurnSuccessor(scene, 3, "regenerated user", "regenerated assistant", scene.Successor)
	if err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: scene.SessionID, TurnIndex: 3,
		UserContent: successor.UserContent, AssistantContent: successor.AssistantContent,
		CreatedAt: successor.CreatedAt, SourceRevision: successor,
	}); err != nil {
		t.Fatalf("ReplaceLogicalTurn: %v", err)
	}
	if successor.ID == 0 {
		t.Error("replacement must read the registered source revision id back")
	}

	var lifecycle, supersededBy, reason string
	if err := conn.QueryRow(ctx, `SELECT lifecycle_state, COALESCE(superseded_by_revision,''), COALESCE(invalidation_reason,'')
		FROM memory_source_revisions WHERE source_revision = ?`, scene.TailRev).Scan(&lifecycle, &supersededBy, &reason); err != nil {
		t.Fatalf("read discarded revision: %v", err)
	}
	if lifecycle != "superseded" || supersededBy != scene.Successor || reason != "logical_turn_replaced" {
		t.Errorf("discarded revision = (%q, %q, %q), want (superseded, %s, logical_turn_replaced)", lifecycle, supersededBy, reason, scene.Successor)
	}
	var active int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM memory_source_revisions WHERE chat_session_id = ? AND lifecycle_state = 'active' AND turn_index = 3`,
		scene.SessionID).Scan(&active); err != nil {
		t.Fatalf("read active revisions: %v", err)
	}
	if active != 1 {
		t.Errorf("active revisions at turn 3 = %d, want exactly the accepted successor", active)
	}
}

// TestD1ReplaceLogicalTurnIsIdempotentByNaturalKey pins the replay contract: the
// same acceptance twice must converge on one row set, and must not supersede
// the successor it is re-registering.
func TestD1ReplaceLogicalTurnIsIdempotentByNaturalKey(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-replay")

	for attempt := 1; attempt <= 2; attempt++ {
		successor := d1LogicalTurnSuccessor(scene, 3, "regenerated user", "regenerated assistant", scene.Successor)
		if err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
			ChatSessionID: scene.SessionID, TurnIndex: 3,
			UserContent: successor.UserContent, AssistantContent: successor.AssistantContent,
			CreatedAt: successor.CreatedAt, SourceRevision: successor,
		}); err != nil {
			t.Fatalf("ReplaceLogicalTurn attempt %d: %v", attempt, err)
		}
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_source_revisions WHERE source_revision = ?`, scene.Successor); got != 1 {
		t.Errorf("successor rows after replay = %d, want 1", got)
	}
	var lifecycle string
	if err := conn.QueryRow(ctx, `SELECT lifecycle_state FROM memory_source_revisions WHERE source_revision = ?`, scene.Successor).Scan(&lifecycle); err != nil {
		t.Fatalf("read successor lifecycle: %v", err)
	}
	if lifecycle != "active" {
		t.Errorf("successor lifecycle after replay = %q, want active; the replacement superseded its own acceptance", lifecycle)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = ? AND turn_index = 3`, scene.SessionID); got != 2 {
		t.Errorf("replaced turn rows after replay = %d, want 2", got)
	}
}

// TestD1ReplaceLogicalTurnRefusesSourceRevisionCollision pins the other half of
// idempotency: a stored revision under the same natural key that records a
// different acceptance is a conflict, never a silent overwrite.
func TestD1ReplaceLogicalTurnRefusesSourceRevisionCollision(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-collision")

	first := d1LogicalTurnSuccessor(scene, 3, "first user", "first assistant", scene.Successor)
	if err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: scene.SessionID, TurnIndex: 3,
		UserContent: first.UserContent, AssistantContent: first.AssistantContent,
		CreatedAt: first.CreatedAt, SourceRevision: first,
	}); err != nil {
		t.Fatalf("first ReplaceLogicalTurn: %v", err)
	}
	// The tail is now turn 3 with the first text, so a second, different
	// acceptance under the same revision id is what the fence must refuse.
	second := d1LogicalTurnSuccessor(scene, 3, "different user", "different assistant", scene.Successor)
	err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: scene.SessionID, TurnIndex: 3,
		UserContent: second.UserContent, AssistantContent: second.AssistantContent,
		CreatedAt: second.CreatedAt, SourceRevision: second,
	})
	if err == nil {
		t.Fatal("a same-key source revision with different content must be refused")
	}
	var content string
	if err := conn.QueryRow(ctx, `SELECT raw_assistant_content FROM memory_source_revisions WHERE source_revision = ?`, scene.Successor).Scan(&content); err != nil {
		t.Fatalf("read stored acceptance: %v", err)
	}
	if content != "first assistant" {
		t.Errorf("stored acceptance = %q, want the first one preserved; the collision overwrote it", content)
	}
}

// TestD1ReplaceLogicalTurnRefusesStaleSuccessor pins the fence in the other
// direction: a revision that was rolled back after it was accepted must not be
// resurrected by a replay.
func TestD1ReplaceLogicalTurnRefusesStaleSuccessor(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-stale")

	successor := d1LogicalTurnSuccessor(scene, 3, "regenerated user", "regenerated assistant", scene.Successor)
	if err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: scene.SessionID, TurnIndex: 3,
		UserContent: successor.UserContent, AssistantContent: successor.AssistantContent,
		CreatedAt: successor.CreatedAt, SourceRevision: successor,
	}); err != nil {
		t.Fatalf("first ReplaceLogicalTurn: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE memory_source_revisions SET lifecycle_state = 'invalidated' WHERE source_revision = ?`, scene.Successor); err != nil {
		t.Fatalf("invalidate successor: %v", err)
	}
	replay := d1LogicalTurnSuccessor(scene, 3, "regenerated user", "regenerated assistant", scene.Successor)
	err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: scene.SessionID, TurnIndex: 3,
		UserContent: replay.UserContent, AssistantContent: replay.AssistantContent,
		CreatedAt: replay.CreatedAt, SourceRevision: replay,
	})
	if err == nil {
		t.Fatal("replaying an invalidated source revision must be refused")
	}
}

// TestD1ReplaceLogicalTurnRefusesHistoricalTurn pins the tail guard.
func TestD1ReplaceLogicalTurnRefusesHistoricalTurn(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-historical")

	err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: scene.SessionID, TurnIndex: 2,
		UserContent: "u", AssistantContent: "a",
	})
	var typed *LogicalTurnReplacementError
	if !errors.As(err, &typed) || typed.Code != "logical_turn_not_current_tail" || typed.Retryable || typed.CommitState != "not_committed" {
		t.Fatalf("historical replacement error is not terminal and typed: %+v", err)
	}
	// Nothing may have been written: the historical turn still has its text.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = ? AND turn_index = 2 AND content = 'assistant turn 2'`, scene.SessionID); got != 1 {
		t.Errorf("turn 2 was modified by a refused replacement")
	}
}

// TestD1ReplaceLogicalTurnRecreatesDeletedImmediateTail pins the RisuAI
// sequence where the old assistant turn was already deleted.
func TestD1ReplaceLogicalTurnRecreatesDeletedImmediateTail(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-missing-tail")

	if err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: scene.SessionID, TurnIndex: 4,
		UserContent: "regenerated user", AssistantContent: "regenerated assistant",
	}); err != nil {
		t.Fatalf("ReplaceLogicalTurn of a missing immediate tail: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = ? AND turn_index = 4`, scene.SessionID); got != 2 {
		t.Errorf("recreated turn rows = %d, want 2", got)
	}
}

// TestD1ReplaceLogicalTurnRecreatesFirstTurnInEmptySession pins the empty-tail
// case, where the newest turn is 0 and turn 1 is the recreatable first turn.
func TestD1ReplaceLogicalTurnRecreatesFirstTurnInEmptySession(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	if err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: "lt-empty", TurnIndex: 1,
		UserContent: "first user", AssistantContent: "first assistant",
	}); err != nil {
		t.Fatalf("ReplaceLogicalTurn in an empty session: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'lt-empty' AND turn_index = 1`); got != 2 {
		t.Errorf("first turn rows = %d, want 2", got)
	}
}

// TestD1ReplaceLogicalTurnValidatesRequest pins the preflight refusals, which
// must happen before any statement runs.
func TestD1ReplaceLogicalTurnValidatesRequest(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()
	cases := []struct {
		name        string
		replacement LogicalTurnReplacement
		code        string
	}{
		{"no session", LogicalTurnReplacement{TurnIndex: 1, UserContent: "u", AssistantContent: "a"}, "logical_turn_request_invalid"},
		{"no turn", LogicalTurnReplacement{ChatSessionID: "s", UserContent: "u", AssistantContent: "a"}, "logical_turn_request_invalid"},
		{"no user text", LogicalTurnReplacement{ChatSessionID: "s", TurnIndex: 1, AssistantContent: "a"}, "logical_turn_request_invalid"},
		{"no assistant text", LogicalTurnReplacement{ChatSessionID: "s", TurnIndex: 1, UserContent: "u"}, "logical_turn_request_invalid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := st.ReplaceLogicalTurn(ctx, tc.replacement)
			var typed *LogicalTurnReplacementError
			if !errors.As(err, &typed) || typed.Code != tc.code {
				t.Fatalf("error = %v, want %s", err, tc.code)
			}
		})
	}
	// A source revision that disagrees with the replacement about the turn is a
	// revision conflict, not an invalid request: the two are different failures
	// and the host decides differently on each.
	mismatch := &MemorySourceRevision{
		SourceRevision: "r", ChatSessionID: "s", LogicalTurnID: "turn-9", TurnIndex: 9,
		UserContent: "u", AssistantContent: "a", CombinedContentHash: "h",
		HashAlgorithm: "sha256", HostObservedAtMS: 1,
	}
	err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: "s", TurnIndex: 1, UserContent: "u", AssistantContent: "a",
		SourceRevision: mismatch,
	})
	var typed *LogicalTurnReplacementError
	if !errors.As(err, &typed) || typed.Code != "logical_turn_revision_conflict" {
		t.Fatalf("mismatched source revision error = %v, want logical_turn_revision_conflict", err)
	}
}

// TestD1ReplaceLogicalTurnPreservesLegacyCharacterManualEdits pins the manual
// override preservation that runs before the character-state sweep. Without it
// an operator's saved voice overrides would be deleted by the very replacement
// that is supposed to carry them forward.
func TestD1ReplaceLogicalTurnPreservesLegacyCharacterManualEdits(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-manual")

	if _, err := conn.Exec(ctx, `UPDATE character_states SET speech_style_json = ?
		WHERE chat_session_id = ? AND character_name = 'Mira' AND turn_index = 3`,
		`{"principles":[],"manual_overrides":[{"trait_domain":"voice","value":"clipped"}]}`, scene.SessionID); err != nil {
		t.Fatalf("seed manual overrides: %v", err)
	}
	if err := st.ReplaceLogicalTurn(ctx, LogicalTurnReplacement{
		ChatSessionID: scene.SessionID, TurnIndex: 3,
		UserContent: "u", AssistantContent: "a",
	}); err != nil {
		t.Fatalf("ReplaceLogicalTurn: %v", err)
	}
	var details string
	if err := conn.QueryRow(ctx, `SELECT details_json FROM character_events
		WHERE chat_session_id = ? AND event_type = 'manual_character_override'`, scene.SessionID).Scan(&details); err != nil {
		t.Fatalf("manual override was not preserved: %v", err)
	}
	if !strings.Contains(details, "manual_overrides") {
		t.Errorf("preserved override details = %s, want the manual_overrides path", details)
	}
}

// TestD1LogicalTurnTailSweepShape pins the sweep itself, independently of the
// transaction that runs it. A missing table, a changed range, or a dropped
// reset must be visible here rather than only in an integration run.
func TestD1LogicalTurnTailSweepShape(t *testing.T) {
	commands := d1LogicalTurnCanonicalTailCommands("session-1", 4, false, true)
	joined := strings.Join(d1LogicalTurnSQL(commands), "\n")
	for _, required := range []string{
		"DELETE FROM direct_evidence_records",
		"DELETE FROM status_current_values",
		"DELETE FROM status_change_events",
		"json_type(evidence_json",
		"DELETE FROM episode_summaries",
		"DELETE FROM chapter_summaries",
		"DELETE FROM arc_summaries",
		"DELETE FROM saga_digests",
		"UPDATE status_effects SET effect_state = 'active'",
		"DELETE FROM status_effects",
		"DELETE FROM session_active_scopes",
		"DELETE FROM protagonist_entity_memories",
		"DELETE FROM capture_verification_records",
		"DELETE FROM chat_logs WHERE chat_session_id = ? AND turn_index >= ?",
	} {
		if !strings.Contains(joined, required) {
			t.Errorf("tail sweep missing %q", required)
		}
	}
	// The lifecycle shape keeps invalidated history. Physically removing the
	// precise units or rewriting evidence here would destroy the record a
	// rebuild needs.
	for _, forbidden := range []string{
		"DELETE FROM precise_memory_units",
		"UPDATE direct_evidence_records",
	} {
		if strings.Contains(joined, forbidden) {
			t.Errorf("tail sweep destroys invalidated history via %q", forbidden)
		}
	}
	// The replacement narrows the chat_logs delete to the one turn, so an
	// earlier turn can never be removed by a regeneration.
	replacement := strings.Join(d1LogicalTurnSQL(d1LogicalTurnCanonicalTailCommands("session-1", 4, true, false)), "\n")
	if !strings.Contains(replacement, "DELETE FROM chat_logs WHERE chat_session_id = ? AND turn_index = ?") {
		t.Error("replacement sweep must delete only the replaced turn's raw rows")
	}
	// The legacy physical cleanup is reachable only for a replacement that
	// carries no source revision, where there is no derivation history to keep.
	if !strings.Contains(replacement, "DELETE FROM precise_memory_units") {
		t.Error("legacy replacement sweep must keep the precise-unit compatibility delete")
	}
	if strings.Contains(joined, "DELETE FROM precise_memory_units") {
		t.Error("rollback sweep must not physically remove precise units")
	}
	// Every hierarchy summary must delete an overlapping RANGE, not a single
	// turn: a summary spanning turns 1..5 still describes turn 4.
	for _, table := range []string{"episode_summaries", "chapter_summaries", "arc_summaries", "saga_digests"} {
		found := false
		for _, command := range commands {
			if strings.Contains(command.SQL, "DELETE FROM "+table) {
				found = true
				if !strings.Contains(command.SQL, "(to_turn >= ? OR from_turn >= ?)") {
					t.Errorf("%s cleanup does not delete ranges overlapping the rollback turn", table)
				}
				if len(command.Args) != 3 {
					t.Errorf("%s cleanup binds %d args, want session and turn twice", table, len(command.Args))
				}
			}
		}
		if !found {
			t.Errorf("tail sweep missing %s", table)
		}
	}
}

// TestD1LogicalTurnErrorClassification pins the code mapping the HTTP layer
// decodes. D1 has no MySQL error numbers, so the constraint codes are recovered
// from SQLite's own stable message; the permission code has no counterpart and
// is therefore unreachable rather than guessed at.
func TestD1LogicalTurnErrorClassification(t *testing.T) {
	cases := []struct {
		name            string
		err             error
		commitAttempted bool
		code            string
		retryable       bool
		commitState     string
	}{
		{"not enabled", ErrNotEnabled, false, "logical_turn_store_unavailable", false, "not_committed"},
		{"unique", errors.New("UNIQUE constraint failed: memory_source_revisions.source_revision"), false, "logical_turn_revision_conflict", false, "not_committed"},
		{"foreign key", errors.New("FOREIGN KEY constraint failed"), false, "logical_turn_constraint_conflict", false, "not_committed"},
		{"commit unknown", errors.New("bridge closed mid-batch"), true, "logical_turn_commit_outcome_unknown", false, "unknown"},
		{"other", errors.New("disk image is malformed"), false, "logical_turn_transaction_failed", true, "not_committed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := d1LogicalTurnClassifyError(tc.err, "test_stage", tc.commitAttempted)
			var typed *LogicalTurnReplacementError
			if !errors.As(err, &typed) {
				t.Fatalf("error is not typed: %v", err)
			}
			if typed.Code != tc.code || typed.Retryable != tc.retryable || typed.CommitState != tc.commitState {
				t.Fatalf("classification = %+v, want %s/%v/%s", typed, tc.code, tc.retryable, tc.commitState)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// RollbackCanonicalTail
// ---------------------------------------------------------------------------

// TestD1LogicalTurnStatementsBindEveryPlaceholder guards a failure mode the
// engine hides. modernc.org/sqlite accepts a statement with MORE bound
// arguments than placeholders and executes it anyway, so a mistyped argument
// list does not fail the batch: the extra values are dropped and the statement
// silently binds the WRONG columns. An extra CASE argument once left the
// source-revision invalidation a statement that reported success and updated
// nothing, and no assertion in this file would have caught it on its own.
func TestD1LogicalTurnStatementsBindEveryPlaceholder(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-placeholders")

	successor := d1LogicalTurnSuccessor(scene, 3, "regenerated user", "regenerated assistant", scene.Successor)
	invalidations, err := st.d1LogicalTurnActiveRevisions(ctx, scene.SessionID, 3, "")
	if err != nil {
		t.Fatalf("read revisions: %v", err)
	}
	restores, err := st.d1LogicalTurnStatusCurrentRestore(ctx, scene.SessionID)
	if err != nil {
		t.Fatalf("build status restore: %v", err)
	}
	swept := d1LogicalTurnCanonicalTailCommands(scene.SessionID, 3, true, false)
	var all []D1Statement
	all = append(all, d1LogicalTurnSourceRevisionInsert(successor, "2026-07-22T01:02:03.000Z"))
	all = append(all, d1LogicalTurnInvalidationStatements(scene.SessionID, invalidations, "deleted", "turn_rollback", "2026-07-22T01:02:03.000Z", scene.Successor)...)
	all = append(all, d1LogicalTurnVectorDeleteStatements(scene.SessionID, []d1VectorDelete{{documentID: "memory:x:1", sourceRevision: "r"}}, "turn_rollback", "2026-07-22T01:02:03.000Z")...)
	all = append(all, swept...)
	all = append(all, restores...)
	if len(all) < 30 {
		t.Fatalf("only %d statements collected; the sweep is incomplete", len(all))
	}
	for i, statement := range all {
		if want, got := strings.Count(statement.SQL, "?"), len(statement.Args); want != got {
			t.Errorf("statement %d binds %d placeholders with %d args:\n%s", i, want, got, statement.SQL)
		}
	}
}

// TestD1RollbackCanonicalTailInvalidatesDerivedRows pins the invalidation set of
// the delete path and its idempotency. An invalidated (not deleted) source
// keeps its raw text, because the reversible path is what distinguishes it from
// a permanent delete.
func TestD1RollbackCanonicalTailInvalidatesDerivedRows(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-rollback")

	rollback := LogicalTurnRollback{
		ChatSessionID: scene.SessionID, TurnIndex: 3,
		Reason: "turn_rollback", CreatedAt: time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC),
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err := st.RollbackCanonicalTail(ctx, rollback); err != nil {
			t.Fatalf("RollbackCanonicalTail attempt %d: %v", attempt, err)
		}
	}

	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = ? AND turn_index >= 3`, scene.SessionID); got != 0 {
		t.Errorf("rolled back chat rows = %d, want 0", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = ? AND turn_index = 2`, scene.SessionID); got != 2 {
		t.Errorf("turn 2 chat rows = %d, want 2; the rollback reached before its turn", got)
	}
	d1LogicalTurnAssertGone(t, conn, "rollback at turn 3", map[string]struct {
		query string
		args  []any
	}{
		"memories":              {`SELECT COUNT(*) FROM memories WHERE chat_session_id = ? AND turn_index >= 3`, []any{scene.SessionID}},
		"evidence":              {`SELECT COUNT(*) FROM direct_evidence_records WHERE chat_session_id = ? AND source_turn_end >= 3`, []any{scene.SessionID}},
		"effective inputs":      {`SELECT COUNT(*) FROM effective_input_logs WHERE chat_session_id = ? AND turn_index >= 3`, []any{scene.SessionID}},
		"character states":      {`SELECT COUNT(*) FROM character_states WHERE chat_session_id = ? AND turn_index >= 3`, []any{scene.SessionID}},
		"identities":            {`SELECT COUNT(*) FROM entity_identities WHERE chat_session_id = ? AND source_turn >= 3`, []any{scene.SessionID}},
		"identity surfaces":     {`SELECT COUNT(*) FROM entity_identity_surfaces WHERE chat_session_id = ? AND source_turn >= 3`, []any{scene.SessionID}},
		"status current values": {`SELECT COUNT(*) FROM status_current_values WHERE chat_session_id = ? AND source_turn >= 3`, []any{scene.SessionID}},
	})

	var lifecycle, rawUser string
	if err := conn.QueryRow(ctx, `SELECT lifecycle_state, raw_user_content FROM memory_source_revisions WHERE source_revision = ?`,
		scene.TailRev).Scan(&lifecycle, &rawUser); err != nil {
		t.Fatalf("read rolled back revision: %v", err)
	}
	if lifecycle != "invalidated" {
		t.Errorf("rolled back revision lifecycle = %q, want invalidated", lifecycle)
	}
	if rawUser != "u3" {
		t.Errorf("rolled back revision raw user content = %q, want the original; an invalidation is reversible and must keep its text", rawUser)
	}
}

// TestD1RollbackCanonicalTailDeletedSurrendersDerivedContent pins the delete
// action's contract: the raw text, the Critic extraction and the evidence links
// are surrendered, because a permanently deleted turn must leave nothing a
// reader could mistake for live data.
func TestD1RollbackCanonicalTailDeletedSurrendersDerivedContent(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-deleted")
	if _, err := conn.Exec(ctx, `UPDATE memory_source_revisions SET derived_result_json = '{"facts":[]}' WHERE source_revision = ?`,
		scene.TailRev); err != nil {
		t.Fatalf("seed critic result: %v", err)
	}

	if err := st.RollbackCanonicalTail(ctx, LogicalTurnRollback{
		ChatSessionID: scene.SessionID, TurnIndex: 3,
		LifecycleAction: LogicalTurnLifecycleDeleted, Reason: "turn_rollback",
	}); err != nil {
		t.Fatalf("RollbackCanonicalTail deleted: %v", err)
	}
	var lifecycle, rawUser, rawAssistant string
	var derived *string
	if err := conn.QueryRow(ctx, `SELECT lifecycle_state, raw_user_content, raw_assistant_content, derived_result_json
		FROM memory_source_revisions WHERE source_revision = ?`, scene.TailRev).
		Scan(&lifecycle, &rawUser, &rawAssistant, &derived); err != nil {
		t.Fatalf("read deleted revision: %v", err)
	}
	if lifecycle != "deleted" {
		t.Errorf("lifecycle = %q, want deleted", lifecycle)
	}
	if rawUser != "" || rawAssistant != "" {
		t.Errorf("deleted revision kept raw content (%q, %q); a deleted turn must surrender it", rawUser, rawAssistant)
	}
	if derived != nil {
		t.Errorf("deleted revision kept derived_result_json %q; a deleted turn must surrender it", *derived)
	}
	var unitExcerpt, unitEvidence, unitPayload string
	if err := conn.QueryRow(ctx, `SELECT evidence_excerpt, direct_evidence_ids_json, payload_json
		FROM precise_memory_units WHERE unit_id = ?`, scene.UnitID).Scan(&unitExcerpt, &unitEvidence, &unitPayload); err != nil {
		t.Fatalf("read deleted precise unit: %v", err)
	}
	if unitExcerpt != "" || unitEvidence != "[]" || unitPayload != "{}" {
		t.Errorf("deleted precise unit kept (%q, %q, %q); want empty, [], {}", unitExcerpt, unitEvidence, unitPayload)
	}
}

// TestD1RollbackCanonicalTailEnqueuesVectorDeletes pins the durable delete
// receipts. The vector index is a rebuildable projection, but a delete left
// unrecorded leaves the accelerator serving text the user discarded forever.
func TestD1RollbackCanonicalTailEnqueuesVectorDeletes(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-vectors")

	if err := st.RollbackCanonicalTail(ctx, LogicalTurnRollback{
		ChatSessionID: scene.SessionID, TurnIndex: 3, Reason: "turn_rollback",
	}); err != nil {
		t.Fatalf("RollbackCanonicalTail: %v", err)
	}
	var deletes int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM memory_vector_outbox
		WHERE chat_session_id = ? AND operation = 'delete' AND required_source_state = 'inactive'`, scene.SessionID).Scan(&deletes); err != nil {
		t.Fatalf("read vector deletes: %v", err)
	}
	if deletes == 0 {
		t.Error("rollback must enqueue at least one durable vector delete for the retired documents")
	}
	// Replaying must not double the receipts: the same document invalidated
	// twice is one pending delete, not two racing ones.
	if err := st.RollbackCanonicalTail(ctx, LogicalTurnRollback{
		ChatSessionID: scene.SessionID, TurnIndex: 3, Reason: "turn_rollback",
	}); err != nil {
		t.Fatalf("replayed RollbackCanonicalTail: %v", err)
	}
	var after int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM memory_vector_outbox
		WHERE chat_session_id = ? AND operation = 'delete' AND required_source_state = 'inactive'`, scene.SessionID).Scan(&after); err != nil {
		t.Fatalf("re-read vector deletes: %v", err)
	}
	if after != deletes {
		t.Errorf("vector delete receipts after replay = %d, want %d; a replay duplicated them", after, deletes)
	}
}

// TestD1RollbackCanonicalTailGuardsTailAndLifecycle pins the two preflight
// refusals, which must leave the session untouched.
func TestD1RollbackCanonicalTailGuardsTailAndLifecycle(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	scene := d1LogicalTurnSeed(t, conn, "lt-guards")

	beyondTail := st.RollbackCanonicalTail(ctx, LogicalTurnRollback{ChatSessionID: scene.SessionID, TurnIndex: 9})
	var typed *LogicalTurnReplacementError
	if !errors.As(beyondTail, &typed) || typed.Code != "logical_turn_not_current_tail" {
		t.Fatalf("rollback past the tail = %v, want logical_turn_not_current_tail", beyondTail)
	}
	badLifecycle := st.RollbackCanonicalTail(ctx, LogicalTurnRollback{
		ChatSessionID: scene.SessionID, TurnIndex: 3, LifecycleAction: "obliterated",
	})
	if !errors.As(badLifecycle, &typed) || typed.Code != "logical_turn_lifecycle_action_invalid" {
		t.Fatalf("unknown lifecycle = %v, want logical_turn_lifecycle_action_invalid", badLifecycle)
	}
	badRequest := st.RollbackCanonicalTail(ctx, LogicalTurnRollback{ChatSessionID: scene.SessionID})
	if !errors.As(badRequest, &typed) || typed.Code != "logical_turn_request_invalid" {
		t.Fatalf("missing turn = %v, want logical_turn_request_invalid", badRequest)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = ?`, scene.SessionID); got != 6 {
		t.Errorf("chat rows after three refusals = %d, want 6; a refusal wrote something", got)
	}
	// A rollback may start at the tail itself and at the turn after it, because
	// the host is reporting a turn it already saw disappear. Each case needs its
	// own store: once turn 3 is rolled back the tail is 2, and turn 4 is then
	// genuinely beyond it, which is the refusal the first case already pinned.
	for _, turn := range []int{3, 4} {
		atTail, atTailConn := newD1TestStore(t)
		session := "lt-guard-" + strconv.Itoa(turn)
		d1LogicalTurnSeed(t, atTailConn, session)
		if err := atTail.RollbackCanonicalTail(ctx, LogicalTurnRollback{
			ChatSessionID: session, TurnIndex: turn, Reason: "turn_rollback",
		}); err != nil {
			t.Fatalf("rollback at turn %d: %v", turn, err)
		}
	}
}

// d1SessionStitchSeedChat writes one session's canonical turns, two rows per
// turn. The rows within a session differ only in turn_index, which is what
// makes the segment offsets a test of the stitch order rather than of content.
func d1SessionStitchSeedChat(t *testing.T, conn *sqliteD1Conn, sessionID string, throughTurn int) {
	t.Helper()
	ctx := context.Background()
	for turn := 1; turn <= throughTurn; turn++ {
		for _, role := range []string{"user", "assistant"} {
			content := sessionID + " turn " + strconv.Itoa(turn) + " " + role
			if _, err := conn.Exec(ctx, `INSERT INTO chat_logs (chat_session_id, turn_index, role, content, created_at)
				VALUES (?, ?, ?, ?, ?)`, sessionID, turn, role, content,
				"2026-02-0"+strconv.Itoa(turn)+"T00:00:00.000Z"); err != nil {
				t.Fatalf("seed chat log %s/%d: %v", sessionID, turn, err)
			}
		}
	}
}

func d1SessionStitchSegmentKey(segment SessionStitchSegment) string {
	return segment.SessionID + "@" + strconv.Itoa(segment.Ordinal) + "+" +
		strconv.Itoa(segment.Offset) + ".." + strconv.Itoa(segment.ThroughTurn)
}

func d1SessionStitchSegmentKeys(result *SessionStitchResult) []string {
	if result == nil {
		return nil
	}
	keys := make([]string, 0, len(result.Segments))
	for _, segment := range result.Segments {
		keys = append(keys, d1SessionStitchSegmentKey(segment))
	}
	return keys
}

// TestD1SessionStitchOrdersSegmentsByRequestOrder pins the stitch order.
//
// The two source sessions are shaped identically apart from how far they run,
// so a sort by session id, by segment count, or by anything but the request
// order gives a different answer. The second half swaps the request order and
// asserts the offsets move with it: a dropped ordinal, a re-sorted segment
// list, or an offset accumulated in session-id order all fail here.
func TestD1SessionStitchOrdersSegmentsByRequestOrder(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionStitchSeedChat(t, conn, "stitch-short", 2)
	d1SessionStitchSeedChat(t, conn, "stitch-long", 3)
	d1SessionStitchSeedChat(t, conn, "stitch-current", 1)

	result, err := st.StitchSessions(ctx, SessionStitchRequest{
		SourceSessionIDs: []string{"stitch-short", "stitch-long"},
		CurrentSessionID: "stitch-current",
		OperationID:      "op-order",
	})
	if err != nil {
		t.Fatalf("StitchSessions: %v", err)
	}
	want := []string{
		"stitch-short@1+0..2",
		"stitch-long@2+2..5",
		"stitch-current@3+5..6",
	}
	if got := d1SessionStitchSegmentKeys(result); !equalStringSlices(want, got) {
		t.Fatalf("segments = %#v, want %#v", got, want)
	}
	if result.CurrentOffset != 5 {
		t.Errorf("current offset = %d, want 5; it is where the live chat starts, not where it ends", result.CurrentOffset)
	}
	if !equalStringSlices([]string{"stitch-current"}, result.CurrentSourceSessionIDs) {
		t.Errorf("current source session ids = %#v, want the live chat only", result.CurrentSourceSessionIDs)
	}

	// The same three sessions, the reverse request order. Only the first two
	// entries change, and they change completely.
	reversed, err := st.StitchSessions(ctx, SessionStitchRequest{
		SourceSessionIDs: []string{"stitch-long", "stitch-short"},
		CurrentSessionID: "stitch-current",
		OperationID:      "op-order-reversed",
	})
	if err != nil {
		t.Fatalf("reversed StitchSessions: %v", err)
	}
	wantReversed := []string{
		"stitch-long@1+0..3",
		"stitch-short@2+3..5",
		"stitch-current@3+5..6",
	}
	if got := d1SessionStitchSegmentKeys(reversed); !equalStringSlices(wantReversed, got) {
		t.Fatalf("reversed segments = %#v, want %#v; the stitch order is the request order", got, wantReversed)
	}
}

// TestD1SessionStitchOffsetsRenumberCopiedTurns pins the other half of the
// order: the copied rows carry the renumbered turn, so a session joined after
// another starts where the previous one stopped.
func TestD1SessionStitchOffsetsRenumberCopiedTurns(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionStitchSeedChat(t, conn, "stitch-first", 2)
	d1SessionStitchSeedChat(t, conn, "stitch-second", 2)
	d1SessionStitchSeedChat(t, conn, "stitch-live", 1)

	result, err := st.StitchSessions(ctx, SessionStitchRequest{
		SourceSessionIDs: []string{"stitch-first", "stitch-second"},
		CurrentSessionID: "stitch-live",
		OperationID:      "op-renumber",
	})
	if err != nil {
		t.Fatalf("StitchSessions: %v", err)
	}
	rows, err := conn.Query(ctx, `SELECT turn_index, content FROM chat_logs
		WHERE chat_session_id = ? ORDER BY id`, result.TargetSessionID)
	if err != nil {
		t.Fatalf("read target chat logs: %v", err)
	}
	defer rows.Close()
	var turns []int
	var contents []string
	for rows.Next() {
		var turn int
		var content string
		if err := rows.Scan(&turn, &content); err != nil {
			t.Fatalf("scan target chat log: %v", err)
		}
		turns = append(turns, turn)
		contents = append(contents, content)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read target chat logs: %v", err)
	}
	wantTurns := []int{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}
	if len(turns) != len(wantTurns) {
		t.Fatalf("target turn indexes = %#v, want %#v", turns, wantTurns)
	}
	for i := range wantTurns {
		if turns[i] != wantTurns[i] {
			t.Fatalf("target turn indexes = %#v, want %#v; the second session did not start where the first stopped", turns, wantTurns)
		}
	}
	if !strings.Contains(contents[0], "stitch-first") || !strings.Contains(contents[len(contents)-1], "stitch-live") {
		t.Errorf("target contents = %#v, want the first session first and the live chat last", contents)
	}
	// The source sessions are read, never rewritten.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = 'stitch-first'`); got != 4 {
		t.Errorf("source chat rows = %d, want 4; the stitch must not rewrite its sources", got)
	}
}

// TestD1SessionStitchPrefersNewestLedgerRow pins the one sort key in the path.
//
// Two non-reverted ledger rows for the same target coexist after a retry.
// ORDER BY id DESC is what makes the retry report the offsets of the copy that
// actually produced the target, and it is the assertion a dropped or reversed
// ORDER BY fails.
func TestD1SessionStitchPrefersNewestLedgerRow(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionStitchSeedChat(t, conn, "stitch-older", 1)
	d1SessionStitchSeedChat(t, conn, "stitch-live", 1)

	first, err := st.StitchSessions(ctx, SessionStitchRequest{
		SourceSessionIDs: []string{"stitch-older"},
		CurrentSessionID: "stitch-live",
		OperationID:      "op-newest",
	})
	if err != nil {
		t.Fatalf("StitchSessions: %v", err)
	}
	// A later attempt for the same operation, recorded by hand with a
	// different note. The next call must report THIS row, not the first one.
	if _, err := conn.Exec(ctx, `INSERT INTO session_migrations (
		source_session_id, target_session_id, mode, status, operator_note, counts_json, chroma_reindexed_count, errors_json
	) VALUES ('stitch-live', ?, 'stitch_keep_sources', 'copied', ?, '{}', 0, '[]')`,
		first.TargetSessionID, `{"migration_id":0,"target_session_id":"stitch-target",`+
			`"segments":[{"session_id":"stitch-older","ordinal":7,"offset":41,"through_turn":42}],`+
			`"current_offset":99,"current_source_session_id":"stitch-live"}`); err != nil {
		t.Fatalf("seed newer ledger row: %v", err)
	}
	var newestRowID int64
	if err := conn.QueryRow(ctx, `SELECT id FROM session_migrations
		WHERE target_session_id = ? AND mode = 'stitch_keep_sources' ORDER BY id DESC LIMIT 1`,
		first.TargetSessionID).Scan(&newestRowID); err != nil {
		t.Fatalf("read newest ledger id: %v", err)
	}

	replay, err := st.StitchSessions(ctx, SessionStitchRequest{
		SourceSessionIDs: []string{"stitch-older"},
		CurrentSessionID: "stitch-live",
		OperationID:      "op-newest",
	})
	if err != nil {
		t.Fatalf("replayed StitchSessions: %v", err)
	}
	if replay.MigrationID != newestRowID {
		t.Errorf("replay migration id = %d, want the newest ledger row %d", replay.MigrationID, newestRowID)
	}
	if replay.CurrentOffset != 99 || len(replay.Segments) != 1 || replay.Segments[0].Ordinal != 7 {
		t.Errorf("replay = (offset %d, segments %#v), want the newest note's values", replay.CurrentOffset, replay.Segments)
	}
	if replay.TargetSessionID != first.TargetSessionID {
		t.Errorf("replay target = %q, want the deterministic %q", replay.TargetSessionID, first.TargetSessionID)
	}
}

// TestD1SessionStitchReplayDoesNotDuplicateRows pins idempotency by the
// operation's natural key: the second call returns the stored result and
// writes nothing.
func TestD1SessionStitchReplayDoesNotDuplicateRows(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionStitchSeedChat(t, conn, "stitch-a", 2)
	d1SessionStitchSeedChat(t, conn, "stitch-b", 1)

	request := SessionStitchRequest{
		SourceSessionIDs: []string{"stitch-a", "stitch-b"},
		CurrentSessionID: "stitch-b-live",
		OperationID:      "op-replay",
	}
	first, err := st.StitchSessions(ctx, request)
	if err != nil {
		t.Fatalf("StitchSessions: %v", err)
	}
	before := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = ?`, first.TargetSessionID)

	second, err := st.StitchSessions(ctx, request)
	if err != nil {
		t.Fatalf("replayed StitchSessions: %v", err)
	}
	if second.MigrationID != first.MigrationID || second.TargetSessionID != first.TargetSessionID {
		t.Errorf("replay = (%d, %s), want the first operation (%d, %s)",
			second.MigrationID, second.TargetSessionID, first.MigrationID, first.TargetSessionID)
	}
	after := d1Count(t, conn, `SELECT COUNT(*) FROM chat_logs WHERE chat_session_id = ?`, first.TargetSessionID)
	if after != before {
		t.Errorf("target chat rows after replay = %d, want %d", after, before)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_migrations WHERE target_session_id = ?`, first.TargetSessionID); got != 1 {
		t.Errorf("ledger rows for the target = %d, want 1; a replay created a second operation", got)
	}
}

// TestD1SessionStitchRefusesInFlightOperation pins the D1 substitute for the
// reference's GET_LOCK contention path: a ledger row still copying is reported
// as in progress, never as a finished result.
func TestD1SessionStitchRefusesInFlightOperation(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionStitchSeedChat(t, conn, "stitch-inflight-src", 1)
	d1SessionStitchSeedChat(t, conn, "stitch-inflight-live", 1)

	target := "stitch_" + sessionMigrationStringHash("session-stitch.v1", "op-inflight",
		strings.Join([]string{"stitch-inflight-src", "stitch-inflight-live"}, "\x1f"))[:40]
	if _, err := conn.Exec(ctx, `INSERT INTO session_migrations (
		source_session_id, target_session_id, mode, status, operator_note, counts_json, chroma_reindexed_count, errors_json
	) VALUES ('stitch-inflight-live', ?, 'stitch_keep_sources', 'copying', '{"migration_id":0}', '{}', 0, '[]')`, target); err != nil {
		t.Fatalf("seed in-flight ledger row: %v", err)
	}
	_, err := st.StitchSessions(ctx, SessionStitchRequest{
		SourceSessionIDs: []string{"stitch-inflight-src"},
		CurrentSessionID: "stitch-inflight-live",
		OperationID:      "op-inflight",
	})
	if err == nil {
		t.Fatal("an in-flight stitch must not report a result")
	}
	if !strings.Contains(err.Error(), "still processing") {
		t.Errorf("in-flight error = %v, want the retry message the reference produces under contention", err)
	}
}

// TestD1SessionStitchValidatesRequest pins the preflight refusals.
func TestD1SessionStitchValidatesRequest(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionStitchSeedChat(t, conn, "stitch-validate", 1)
	cases := []struct {
		name    string
		request SessionStitchRequest
		want    string
	}{
		{"no current session", SessionStitchRequest{SourceSessionIDs: []string{"stitch-validate"}, OperationID: "op"}, "current_session_id and operation_id are required"},
		{"no operation", SessionStitchRequest{SourceSessionIDs: []string{"stitch-validate"}, CurrentSessionID: "stitch-validate"}, "current_session_id and operation_id are required"},
		{"no sources", SessionStitchRequest{CurrentSessionID: "stitch-validate", OperationID: "op"}, "select at least one previous session"},
		{"only the current session", SessionStitchRequest{SourceSessionIDs: []string{"stitch-validate"}, CurrentSessionID: "stitch-validate", OperationID: "op"}, "select at least one previous session"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.StitchSessions(ctx, tc.request)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestD1SessionStitchFencesPublicProjectionOnActiveSourceRevision pins the
// source-revision fence of the stitch.
//
// The stitch accepts a source-derived projection: RebuildPublicProjection is
// the caller's function turning a committed Critic extraction into the public
// document text, and it may only be fed an extraction whose source revision is
// still ACTIVE. A memory whose turn was superseded has no live authority, and
// projecting it would publish text the user discarded under a target session
// they never saw it in.
// The fence is asserted on PRECISE MEMORY UNITS rather than on the turn's
// aggregate Memory, and that is not a convenience. The aggregate lane refuses the
// stitch outright on BOTH providers when a public projection authority is missing
// (mariadb_migration.go:719), and the stitch does not supply one on either
// provider — mariadb_session_stitch.go:145 omits RebuildPublicProjection from the
// copy request exactly as this file's caller does. So a session holding an
// aggregate memory with a vector projection cannot be stitched at all today, on
// MariaDB and on D1 alike, and asserting a rebuild neither provider performs
// would pin a behaviour that does not exist.
//
// The precise-unit lane is where the fence is actually decidable during a stitch,
// and it is the same rule the reference applies
// (sessionMigrationPreciseVectorSourceActive). That is what this test pins.
func TestD1SessionStitchFencesPublicProjectionOnActiveSourceRevision(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SessionStitchSeedChat(t, conn, "stitch-fence", 2)
	d1SessionStitchSeedChat(t, conn, "stitch-fence-live", 1)

	seedRevision := func(revision, logicalTurn string, turn int, lifecycle string) {
		t.Helper()
		// A committed admission carries its full contract: the three versions and
		// the result hash over the extraction. A half-filled row is refused by the
		// shared migration validator, which is the right refusal: a target that
		// copied an unverifiable extraction would publish text no reader could
		// trace back to a source.
		resultJSON := `{"facts":["` + revision + `"]}`
		resultHash := memoryAdmissionExpectedResultHash(&MemoryAdmission{
			SourceRevision:    revision,
			DerivationVersion: "admission.v1",
			ExtractorVersion:  "critic.v1",
			IndexVersion:      MemoryPublicProjectionIndex,
			ResultJSON:        resultJSON,
		})
		if _, err := conn.Exec(ctx, `INSERT INTO memory_source_revisions (
			source_revision, chat_session_id, logical_turn_id, turn_index,
			raw_user_content, raw_assistant_content, combined_content_hash,
			hash_algorithm, host_observed_at_ms, lifecycle_state, derived_admission_state,
			derived_admission_version, derived_extractor_version, derived_index_version,
			derived_result_hash, derived_result_json
		) VALUES (?, 'stitch-fence', ?, ?, 'u', 'a', ?, 'sha256', ?, ?, 'committed',
			'admission.v1', 'critic.v1', ?, ?, ?)`,
			revision, logicalTurn, turn, "hash-"+revision, turn, lifecycle,
			MemoryPublicProjectionIndex, resultHash, resultJSON); err != nil {
			t.Fatalf("seed revision %s: %v", revision, err)
		}
	}
	seedRevision("fence-rev-1", "turn-1", 1, "active")
	seedRevision("fence-rev-2", "turn-2", 2, "superseded")

	// One general-vector eligible precise unit per revision. They differ ONLY in
	// which source revision they carry, so a fence that ignores the revision — or
	// one that ignores the lifecycle — fails this test.
	seedUnit := func(unitID, revision string, turn int) {
		t.Helper()
		if _, err := conn.Exec(ctx, `INSERT INTO precise_memory_units (
			unit_id, contract_version, chat_session_id, source_turn_start, source_turn_end,
			source_contract, source_revision, source_content_hash, source_role,
			source_span_start, source_span_end, evidence_excerpt, evidence_hash,
			direct_evidence_ids_json, memory_kind, payload_json,
			truth_scope, epistemic_mode, authority_class, admission_state, review_state,
			visibility, confidence, idempotency_key, lifecycle_state
		) VALUES (?, ?, 'stitch-fence', ?, ?, 'source_acceptance_observation.v1', ?,
			?, 'combined_turn_pair', 0, 1, 'excerpt', ?, '[]', 'event', ?,
			'objective', 'direct', 'objective_world_state', 'committed', 'source_observed',
			'public', 0.9, ?, 'active')`,
			unitID, PreciseMemoryUnitContract, turn, turn, revision,
			"hash-"+unitID, "eh-"+unitID, `{"summary":"`+unitID+` happened"}`,
			"ik-"+unitID); err != nil {
			t.Fatalf("seed precise unit %s: %v", unitID, err)
		}
	}
	seedUnit("u-fence-active", "fence-rev-1", 1)
	seedUnit("u-fence-superseded", "fence-rev-2", 2)

	var projected []string
	result, err := st.StitchSessions(ctx, SessionStitchRequest{
		SourceSessionIDs: []string{"stitch-fence"},
		CurrentSessionID: "stitch-fence-live",
		OperationID:      "op-fence",
		RebuildPublicProjection: func(derived string) string {
			projected = append(projected, derived)
			return "public text"
		},
	})
	if err != nil {
		t.Fatalf("StitchSessions: %v", err)
	}
	// The snapshot phase may only be handed the ACTIVE revision's extraction: a
	// superseded extraction is text the user replaced.
	for _, input := range projected {
		if strings.Contains(input, "fence-rev-2") {
			t.Errorf("projection input = %q, want no SUPERSEDED extraction", input)
		}
	}

	// Both units are copied: the fence is on the VECTOR projection, not on the
	// canonical row, because the canonical row is the turn's record.
	//
	// The copied unit_id is a FRESH UUID (the manifest marks unit_id as a
	// generated key), so the target rows are identified by the copied
	// payload_json, which is the one column here the migration carries verbatim.
	unitIDByPayload := map[string]string{}
	rows, err := conn.Query(ctx, `SELECT payload_json, unit_id FROM precise_memory_units
		WHERE chat_session_id = ?`, result.TargetSessionID)
	if err != nil {
		t.Fatalf("read the target units: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var payload, unitID string
		if err := rows.Scan(&payload, &unitID); err != nil {
			t.Fatalf("scan the target unit: %v", err)
		}
		unitIDByPayload[payload] = unitID
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read the target units: %v", err)
	}
	if len(unitIDByPayload) != 2 {
		t.Fatalf("target precise units = %d, want 2", len(unitIDByPayload))
	}
	activeUnit, ok := unitIDByPayload[`{"summary":"u-fence-active happened"}`]
	if !ok {
		t.Fatalf("the target is missing the unit copied from the ACTIVE revision: %v", unitIDByPayload)
	}
	supersededUnit, ok := unitIDByPayload[`{"summary":"u-fence-superseded happened"}`]
	if !ok {
		t.Fatalf("the target is missing the unit copied from the SUPERSEDED revision: %v", unitIDByPayload)
	}

	// The vector fence is what the source-revision lookup decides: only the unit
	// carried by the still-active revision may be EXPECTED in the target's vector
	// ledger. The stitch records expected ids rather than enqueueing outbox rows,
	// because the target session's own workers own that.
	var live, superseded int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM session_migration_vector_expected_ids e
		JOIN session_migrations m ON m.id = e.migration_id
		WHERE m.target_session_id = ? AND e.document_id = ?`,
		result.TargetSessionID, d1AdmissionPreciseDocumentID(result.TargetSessionID, activeUnit)).
		Scan(&live); err != nil {
		t.Fatalf("read the active unit's expected document: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM session_migration_vector_expected_ids e
		JOIN session_migrations m ON m.id = e.migration_id
		WHERE m.target_session_id = ? AND e.document_id = ?`,
		result.TargetSessionID, d1AdmissionPreciseDocumentID(result.TargetSessionID, supersededUnit)).
		Scan(&superseded); err != nil {
		t.Fatalf("read the superseded unit's expected document: %v", err)
	}
	if live != 1 {
		t.Errorf("the ACTIVE revision's unit reached the target vector ledger %d times, want 1", live)
	}
	if superseded != 0 {
		t.Errorf("the SUPERSEDED revision's unit reached the target vector ledger %d times, want 0", superseded)
	}
}
