package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// d1SessionStateSnapshotTrace is the trace the MariaDB aggregate reports, copied
// here independently so the D1 list is checked against the MariaDB contract
// rather than against itself.
var d1SessionStateSnapshotTrace = []string{
	"ListActiveStates",
	"ListCanonicalStateLayers",
	"ListStorylines",
	"ListCharacterStates",
	"ListWorldRules",
	"ListPendingThreads",
	"ListCharacterEvents",
	"ListChatLogs",
}

// d1SessionStateSnapshotComponents pairs each component projection with the SQL
// fragment that identifies its read, used to assert the assembly order.
var d1SessionStateSnapshotComponents = []struct {
	method string
	marker string
}{
	{"ListActiveStates", "FROM active_states"},
	{"ListCanonicalStateLayers", "FROM canonical_state_layers"},
	{"ListStorylines", "FROM storylines"},
	{"ListCharacterStates", "FROM character_states"},
	{"ListWorldRules", "FROM world_rules"},
	{"ListPendingThreads", "FROM pending_threads"},
	{"ListCharacterEvents", "event_type, details_json, created_at"},
	{"ListChatLogs", "FROM chat_logs"},
}

// d1CharacterManualEditOverlay is the statement the character-states component
// issues right after its own read, to apply durable operator overrides that live
// in character_events. It is marked separately because it reads the same table
// the ListCharacterEvents component reads, so matching on the table name alone
// would misattribute the overlay to the later component and make the
// assembly-order assertion meaningless.
const d1CharacterManualEditOverlay = "e.details_json"

// d1SeedSessionStateAggregate writes one row into each of the eight component
// tables so the aggregate has something to assemble.
func d1SeedSessionStateAggregate(t *testing.T, conn *sqliteD1Conn, sid string) {
	t.Helper()
	ctx := context.Background()
	statements := []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO active_states (chat_session_id, state_type, content, turn_index)
			VALUES (?, 'mood', 'tense', 12)`, []any{sid}},
		{`INSERT INTO canonical_state_layers
			(chat_session_id, layer_type, content, source_state_type, turn_index, source_turn, confidence)
			VALUES (?, 'emotion', 'calm', 'mood', 12, 12, 0.7)`, []any{sid}},
		{`INSERT INTO storylines
			(chat_session_id, name, status, entities_json, current_context, key_points_json,
			 ongoing_tensions_json, confidence, evidence_count, first_turn, last_turn)
			VALUES (?, 'Rooftop', 'active', '[]', 'confession', '["hesitation"]', '["answer"]',
			 0.8, 3, 1, 12)`, []any{sid}},
		{`INSERT INTO character_states
			(chat_session_id, character_name, status_json, turn_index)
			VALUES (?, 'Rooftop Girl', '{"mood":"auto"}', 12)`, []any{sid}},
		{`INSERT INTO world_rules
			(chat_session_id, scope, category, "key", value_json, source_turn)
			VALUES (?, 'root', 'society', 'status_matters', '{"statement":"status matters"}', 2)`, []any{sid}},
		{`INSERT INTO pending_threads
			(chat_session_id, thread_key, description, status, created_turn, source_turn, priority, hook_type,
			 hook_metadata_json)
			VALUES (?, 'thread_answer', 'Need an answer', 'open', 10, 10, 2, 'open_question',
			 '{"title":"Need an answer"}')`, []any{sid}},
		{`INSERT INTO character_events
			(chat_session_id, character_name, turn_index, event_type, details_json)
			VALUES (?, 'Rooftop Girl', 12, 'hesitated', '{"manner":"slow"}')`, []any{sid}},
		{`INSERT INTO chat_logs (chat_session_id, turn_index, role, content)
			VALUES (?, 12, 'assistant', 'She hesitated.')`, []any{sid}},
	}
	for _, stmt := range statements {
		if _, err := conn.Exec(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatalf("seed %s: %v", stmt.sql, err)
		}
	}
}

// d1SnapshotProbeConn records the statement text of every read so a test can
// inspect the order of the aggregate and inject a failure at a chosen component.
//
// It embeds the SQLite transport, so the dialect under test is the dialect D1
// runs. Only Query and Batch are intercepted: the aggregate issues no Exec and
// no QueryRow, and the single-row handles the embedded transport serves bypass
// the recorder by design.
type d1SnapshotProbeConn struct {
	*sqliteD1Conn
	queries  []string
	batches  [][]D1Statement
	failOn   string
	failErr  error
	batchErr error
}

func (c *d1SnapshotProbeConn) Query(ctx context.Context, query string, args ...any) (D1Rows, error) {
	c.queries = append(c.queries, query)
	if c.failOn != "" && strings.Contains(query, c.failOn) {
		if c.failErr == nil {
			c.failErr = errors.New("d1 read failed")
		}
		return nil, c.failErr
	}
	return c.sqliteD1Conn.Query(ctx, query, args...)
}

func (c *d1SnapshotProbeConn) Batch(ctx context.Context, statements ...D1Statement) error {
	c.batches = append(c.batches, statements)
	return c.sqliteD1Conn.Batch(ctx, statements...)
}

// newD1ProbeStore returns a D1 store whose transport records and can fail.
func newD1ProbeStore(t *testing.T) (*d1Store, *d1SnapshotProbeConn) {
	t.Helper()
	conn := &d1SnapshotProbeConn{sqliteD1Conn: &sqliteD1Conn{db: d1ApplyAllMigrations(t)}}
	st, err := NewD1Store(conn)
	if err != nil {
		t.Fatalf("NewD1Store: %v", err)
	}
	d1, ok := st.(*d1Store)
	if !ok {
		t.Fatalf("NewD1Store returned %T, want *d1Store", st)
	}
	return d1, conn
}

// firstQueryIndex reports where a component read first appears, or -1.
func (c *d1SnapshotProbeConn) firstQueryIndex(marker string) int {
	for i, query := range c.queries {
		if strings.Contains(query, marker) {
			return i
		}
	}
	return -1
}

// TestD1ReadSessionStateSnapshotReturnsEveryComponent is the base parity case:
// the D1 aggregate carries the same eight projections the MariaDB aggregate
// carries, with the same values.
func TestD1ReadSessionStateSnapshotReturnsEveryComponent(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionStateAggregate(t, conn, "sess-agg")

	snapshot, err := st.ReadSessionStateSnapshot(ctx, "sess-agg")
	if err != nil {
		t.Fatalf("ReadSessionStateSnapshot: %v", err)
	}

	counts := map[string]int{
		"active states":    len(snapshot.ActiveStates),
		"canonical layers": len(snapshot.CanonicalStateLayers),
		"storylines":       len(snapshot.Storylines),
		"characters":       len(snapshot.CharacterStates),
		"world rules":      len(snapshot.WorldRules),
		"pending threads":  len(snapshot.PendingThreads),
		"character events": len(snapshot.CharacterEvents),
		"recent chat logs": len(snapshot.RecentChatLogs),
	}
	for name, got := range counts {
		if got != 1 {
			t.Errorf("%s = %d, want 1", name, got)
		}
	}
	if snapshot.ActiveStates[0].StateType != "mood" || snapshot.ActiveStates[0].Content != "tense" {
		t.Errorf("active state = %+v, want the seeded mood state", snapshot.ActiveStates[0])
	}
	if snapshot.CanonicalStateLayers[0].LayerType != "emotion" || snapshot.CanonicalStateLayers[0].SourceTurn != 12 {
		t.Errorf("canonical layer = %+v, want the seeded emotion layer at turn 12", snapshot.CanonicalStateLayers[0])
	}
	if snapshot.Storylines[0].Name != "Rooftop" || snapshot.Storylines[0].LastTurn != 12 {
		t.Errorf("storyline = %+v, want the seeded rooftop storyline", snapshot.Storylines[0])
	}
	if snapshot.CharacterStates[0].CharacterName != "Rooftop Girl" || snapshot.CharacterStates[0].TurnIndex != 12 {
		t.Errorf("character = %+v, want the seeded character at turn 12", snapshot.CharacterStates[0])
	}
	if snapshot.WorldRules[0].Key != "status_matters" || snapshot.WorldRules[0].SourceTurn != 2 {
		t.Errorf("world rule = %+v, want the seeded world rule", snapshot.WorldRules[0])
	}
	if snapshot.PendingThreads[0].ThreadKey != "thread_answer" || snapshot.PendingThreads[0].Status != "open" {
		t.Errorf("pending thread = %+v, want the seeded open thread", snapshot.PendingThreads[0])
	}
	if snapshot.CharacterEvents[0].EventType != "hesitated" {
		t.Errorf("character event = %+v, want the seeded hesitation event", snapshot.CharacterEvents[0])
	}
	if snapshot.RecentChatLogs[0].TurnIndex != 12 || snapshot.RecentChatLogs[0].Content != "She hesitated." {
		t.Errorf("recent chat log = %+v, want the seeded turn-12 log", snapshot.RecentChatLogs[0])
	}
}

// TestD1ReadSessionStateSnapshotMatchesComponentReadsExactly is the strongest
// parity guard available: every field of the aggregate must be byte-identical to
// the standalone read of that projection. Because the aggregate delegates to
// those methods, a filter, ordering, NULL, or overlay difference cannot survive
// this comparison.
func TestD1ReadSessionStateSnapshotMatchesComponentReadsExactly(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionStateAggregate(t, conn, "sess-agg")
	// A second turn in the same session proves the aggregate keeps the component
	// ordering rather than collapsing to a single row per projection.
	if _, err := conn.Exec(ctx, `INSERT INTO chat_logs (chat_session_id, turn_index, role, content)
		VALUES ('sess-agg', 11, 'user', 'What now?')`); err != nil {
		t.Fatalf("seed second chat log: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO active_states (chat_session_id, state_type, content, turn_index)
		VALUES ('sess-agg', 'mood', 'calm', 5)`); err != nil {
		t.Fatalf("seed second active state: %v", err)
	}

	snapshot, err := st.ReadSessionStateSnapshot(ctx, "sess-agg")
	if err != nil {
		t.Fatalf("ReadSessionStateSnapshot: %v", err)
	}

	wantActive, err := st.ListActiveStates(ctx, "sess-agg", "")
	if err != nil {
		t.Fatalf("ListActiveStates: %v", err)
	}
	wantLayers, err := st.ListCanonicalStateLayers(ctx, "sess-agg", "")
	if err != nil {
		t.Fatalf("ListCanonicalStateLayers: %v", err)
	}
	wantStorylines, err := st.ListStorylines(ctx, "sess-agg")
	if err != nil {
		t.Fatalf("ListStorylines: %v", err)
	}
	wantCharacters, err := st.ListCharacterStates(ctx, "sess-agg")
	if err != nil {
		t.Fatalf("ListCharacterStates: %v", err)
	}
	wantWorldRules, err := st.ListWorldRules(ctx, "sess-agg")
	if err != nil {
		t.Fatalf("ListWorldRules: %v", err)
	}
	wantThreads, err := st.ListPendingThreads(ctx, "sess-agg", "")
	if err != nil {
		t.Fatalf("ListPendingThreads: %v", err)
	}
	wantEvents, err := st.ListCharacterEvents(ctx, "sess-agg", "")
	if err != nil {
		t.Fatalf("ListCharacterEvents: %v", err)
	}
	wantLogs, err := st.ListChatLogs(ctx, "sess-agg", 0, 0)
	if err != nil {
		t.Fatalf("ListChatLogs: %v", err)
	}

	pairs := []struct {
		name      string
		got, want any
	}{
		{"ActiveStates", snapshot.ActiveStates, wantActive},
		{"CanonicalStateLayers", snapshot.CanonicalStateLayers, wantLayers},
		{"Storylines", snapshot.Storylines, wantStorylines},
		{"CharacterStates", snapshot.CharacterStates, wantCharacters},
		{"WorldRules", snapshot.WorldRules, wantWorldRules},
		{"PendingThreads", snapshot.PendingThreads, wantThreads},
		{"CharacterEvents", snapshot.CharacterEvents, wantEvents},
		{"RecentChatLogs", snapshot.RecentChatLogs, wantLogs},
	}
	for _, pair := range pairs {
		if !reflect.DeepEqual(pair.got, pair.want) {
			t.Errorf("%s = %+v, want %+v", pair.name, pair.got, pair.want)
		}
	}
	if len(snapshot.ActiveStates) != 2 || len(snapshot.RecentChatLogs) != 2 {
		t.Fatalf("component multiplicity was lost: active=%d logs=%d, want 2 and 2",
			len(snapshot.ActiveStates), len(snapshot.RecentChatLogs))
	}
}

// TestD1ReadSessionStateSnapshotDoesNotClaimASingleConnection pins the one
// place where the D1 aggregate deliberately differs from MariaDB.
//
// MariaDB sets SingleConnection because database/sql pins one connection to a
// read-only transaction. D1 cannot: the transport has no transaction at all.
// Reporting true here would be an unbacked consistency claim that a caller
// (handleSessionState) is entitled to act on, so this asserts the honest false.
func TestD1ReadSessionStateSnapshotDoesNotClaimASingleConnection(t *testing.T) {
	st, conn := newD1TestStore(t)
	d1SeedSessionStateAggregate(t, conn, "sess-agg")

	snapshot, err := st.ReadSessionStateSnapshot(context.Background(), "sess-agg")
	if err != nil {
		t.Fatalf("ReadSessionStateSnapshot: %v", err)
	}
	if snapshot.SingleConnection {
		t.Error("the D1 transport issues one statement per bridge request and has no read " +
			"transaction, so the snapshot must not claim a single connection")
	}

	// The claim could only become true if the transport grew a transaction. This
	// is the guard for that: while D1Conn has no such method, SingleConnection
	// cannot be truthfully set, and the test fails loudly if one is added without
	// revisiting this decision.
	transport := reflect.TypeOf((*D1Conn)(nil)).Elem()
	for _, forbidden := range []string{"Begin", "BeginTx", "Tx", "Conn", "Session"} {
		if _, found := transport.MethodByName(forbidden); found {
			t.Errorf("D1Conn now exposes %s; a read transaction may be possible, so the "+
				"SingleConnection decision in d1_session_snapshot_capabilities.go must be revisited", forbidden)
		}
	}
}

// TestD1ReadSessionStateSnapshotTraceMatchesMariaDB keeps the trace honest. The
// field is what proves the response came from one bounded aggregate read rather
// than independent per-component calls, so the names and their order are the
// contract with the MariaDB provider.
func TestD1ReadSessionStateSnapshotTraceMatchesMariaDB(t *testing.T) {
	st, conn := newD1TestStore(t)
	d1SeedSessionStateAggregate(t, conn, "sess-agg")

	snapshot, err := st.ReadSessionStateSnapshot(context.Background(), "sess-agg")
	if err != nil {
		t.Fatalf("ReadSessionStateSnapshot: %v", err)
	}
	if !reflect.DeepEqual(snapshot.TraceMethods, d1SessionStateSnapshotTrace) {
		t.Errorf("TraceMethods = %v, want the MariaDB trace %v", snapshot.TraceMethods, d1SessionStateSnapshotTrace)
	}
}

// TestD1ReadSessionStateSnapshotReturnsAFreshTrace guards the per-call copy: a
// caller that reorders the returned slice must not change what the next snapshot
// reports.
func TestD1ReadSessionStateSnapshotReturnsAFreshTrace(t *testing.T) {
	st, conn := newD1TestStore(t)
	d1SeedSessionStateAggregate(t, conn, "sess-agg")
	ctx := context.Background()

	first, err := st.ReadSessionStateSnapshot(ctx, "sess-agg")
	if err != nil {
		t.Fatalf("first ReadSessionStateSnapshot: %v", err)
	}
	first.TraceMethods[0] = "tampered"
	first.TraceMethods = append(first.TraceMethods, "extra")

	second, err := st.ReadSessionStateSnapshot(ctx, "sess-agg")
	if err != nil {
		t.Fatalf("second ReadSessionStateSnapshot: %v", err)
	}
	if !reflect.DeepEqual(second.TraceMethods, d1SessionStateSnapshotTrace) {
		t.Errorf("a mutated trace leaked into the next snapshot: %v", second.TraceMethods)
	}
}

// TestD1ReadSessionStateSnapshotReadsComponentsInOrder pins the assembly order
// to the MariaDB order, so the first component to fail is the same on both
// providers, and asserts that nothing wraps the read in a batch.
//
// The batch assertion matters because Batch is the only D1 transaction boundary.
// A read aggregate that issued one would be claiming an atomicity the Worker does
// not provide for reads: it reports row counts, not rows.
func TestD1ReadSessionStateSnapshotReadsComponentsInOrder(t *testing.T) {
	st, conn := newD1ProbeStore(t)
	d1SeedSessionStateAggregate(t, conn.sqliteD1Conn, "sess-agg")

	if _, err := st.ReadSessionStateSnapshot(context.Background(), "sess-agg"); err != nil {
		t.Fatalf("ReadSessionStateSnapshot: %v", err)
	}

	previous := -1
	indexes := map[string]int{}
	for _, component := range d1SessionStateSnapshotComponents {
		index := conn.firstQueryIndex(component.marker)
		if index < 0 {
			t.Errorf("component %s was never read (marker %q)", component.method, component.marker)
			continue
		}
		if index <= previous {
			t.Errorf("component %s was read at statement %d, after the previous component at %d; "+
				"the aggregate order diverged from the MariaDB order", component.method, index, previous)
		}
		indexes[component.method] = index
		previous = index
	}

	// The character component issues its own overlay read right after its
	// snapshots, inside its own step. Pinning it there keeps the component
	// boundary honest: the overlay must not drift into a later step, and it must
	// not be dropped, because dropping it would silently remove operator
	// corrections from the response.
	overlay := conn.firstQueryIndex(d1CharacterManualEditOverlay)
	if overlay < 0 {
		t.Error("the character manual-edit overlay was never applied; operator corrections would be lost")
	} else if states, rules := indexes["ListCharacterStates"], indexes["ListWorldRules"]; states >= 0 && rules >= 0 {
		if overlay <= states || overlay >= rules {
			t.Errorf("the manual-edit overlay ran at statement %d, outside the character-states step [%d, %d)",
				overlay, states, rules)
		}
	}

	if len(conn.batches) != 0 {
		t.Errorf("the aggregate issued %d batch(es); a D1 read must not claim a transaction", len(conn.batches))
	}
	if len(conn.queries) == 0 {
		t.Fatal("the aggregate issued no statements; the probe is not observing the transport")
	}
}

// TestD1ReadSessionStateSnapshotStopsAtTheFirstFailingComponent pins the error
// contract to the MariaDB read, which returns on the first failing query and
// rolls the transaction back. D1 has nothing to roll back, because each component
// read is a standalone statement that committed nothing, so the observable rule
// is the same one thing: the first error wins and nothing after it is read.
func TestD1ReadSessionStateSnapshotStopsAtTheFirstFailingComponent(t *testing.T) {
	st, conn := newD1ProbeStore(t)
	d1SeedSessionStateAggregate(t, conn.sqliteD1Conn, "sess-agg")
	conn.failOn = "FROM character_states"

	snapshot, err := st.ReadSessionStateSnapshot(context.Background(), "sess-agg")
	if err == nil {
		t.Fatal("a failing component read must fail the aggregate")
	}
	if snapshot != nil {
		t.Error("a failed aggregate must return no snapshot, never a partial one")
	}
	if !errors.Is(err, conn.failErr) {
		t.Errorf("error = %v, want the transport failure %v", err, conn.failErr)
	}
	if conn.firstQueryIndex("FROM world_rules") >= 0 {
		t.Error("a component after the failure was still read; the aggregate did not stop at the first error")
	}
	if conn.firstQueryIndex("FROM chat_logs") >= 0 {
		t.Error("the chat-log component was read despite an earlier failure")
	}
}

// TestD1ReadSessionStateSnapshotAppliesCharacterManualEdits proves the aggregate
// carries the same operator overlay the standalone character read applies.
//
// The edits live in character_events as durable overrides rather than in the
// derived turn snapshots, so an aggregate that read only character_states would
// silently drop operator corrections from the response.
func TestD1ReadSessionStateSnapshotAppliesCharacterManualEdits(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionStateAggregate(t, conn, "sess-agg")

	if _, err := conn.Exec(ctx, `INSERT INTO character_events
		(chat_session_id, character_name, turn_index, event_type, details_json)
		VALUES ('sess-agg', 'Rooftop Girl', 0, 'manual_character_override',
			'{"edits":[{"path":["status","mood"],"value":"manual"}]}')`); err != nil {
		t.Fatalf("seed manual override: %v", err)
	}

	snapshot, err := st.ReadSessionStateSnapshot(ctx, "sess-agg")
	if err != nil {
		t.Fatalf("ReadSessionStateSnapshot: %v", err)
	}
	if len(snapshot.CharacterStates) != 1 {
		t.Fatalf("character states = %d, want 1", len(snapshot.CharacterStates))
	}
	if !strings.Contains(snapshot.CharacterStates[0].StatusJSON, `"mood":"manual"`) {
		t.Errorf("status = %s, want the operator override applied", snapshot.CharacterStates[0].StatusJSON)
	}
	if !strings.Contains(snapshot.CharacterStates[0].FieldProvenanceJSON, "manual_edit") {
		t.Errorf("field provenance = %s, want the manual-edit authority recorded",
			snapshot.CharacterStates[0].FieldProvenanceJSON)
	}
}

// TestD1ReadSessionStateSnapshotIsSessionScoped guards against the aggregate
// leaking one session's state into another's response: the route passes the id
// straight through to the aggregate, and every component read must be filtered
// by it.
func TestD1ReadSessionStateSnapshotIsSessionScoped(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1SeedSessionStateAggregate(t, conn, "sess-agg")
	d1SeedSessionStateAggregate(t, conn, "sess-other")

	snapshot, err := st.ReadSessionStateSnapshot(ctx, "sess-other")
	if err != nil {
		t.Fatalf("ReadSessionStateSnapshot: %v", err)
	}
	leaked := 0
	for _, item := range snapshot.ActiveStates {
		if item.ChatSessionID != "sess-other" {
			leaked++
		}
	}
	for _, item := range snapshot.Storylines {
		if item.ChatSessionID != "sess-other" {
			leaked++
		}
	}
	for _, item := range snapshot.CharacterStates {
		if item.ChatSessionID != "sess-other" {
			leaked++
		}
	}
	for _, item := range snapshot.RecentChatLogs {
		if item.ChatSessionID != "sess-other" {
			leaked++
		}
	}
	if leaked != 0 {
		t.Errorf("%d component rows belong to another session", leaked)
	}
	if d1Count(t, conn, `SELECT COUNT(*) FROM active_states WHERE chat_session_id = 'sess-other'`) != 1 {
		t.Error("the other session should still hold its own single row; the seed is wrong, not the aggregate")
	}
}

// TestD1ReadSessionStateSnapshotForUnknownSession pins the empty-session case,
// which the route treats as a normal response rather than an error.
func TestD1ReadSessionStateSnapshotForUnknownSession(t *testing.T) {
	st, _ := newD1TestStore(t)

	snapshot, err := st.ReadSessionStateSnapshot(context.Background(), "sess-missing")
	if err != nil {
		t.Fatalf("an unknown session must not be an error: %v", err)
	}
	if snapshot == nil {
		t.Fatal("an unknown session must still return a snapshot")
	}
	total := len(snapshot.ActiveStates) + len(snapshot.CanonicalStateLayers) + len(snapshot.Storylines) +
		len(snapshot.CharacterStates) + len(snapshot.WorldRules) + len(snapshot.PendingThreads) +
		len(snapshot.CharacterEvents) + len(snapshot.RecentChatLogs)
	if total != 0 {
		t.Errorf("unknown session returned %d component rows, want 0", total)
	}
	if !reflect.DeepEqual(snapshot.TraceMethods, d1SessionStateSnapshotTrace) {
		t.Errorf("an empty session must still report the aggregate trace, got %v", snapshot.TraceMethods)
	}
}

// TestD1StoreAdvertisesSessionStateSnapshotReader records the capability gain in
// the machine-readable manifest. The HTTP route asserts this interface, so the
// D1 provider previously fell back to assembling the response component by
// component; the manifest is where that gap becomes observable.
func TestD1StoreAdvertisesSessionStateSnapshotReader(t *testing.T) {
	st, _ := newD1TestStore(t)

	var found bool
	for _, status := range CapabilityReport(st) {
		if status.Name != "SessionStateSnapshotReader" {
			continue
		}
		found = true
		if !status.Implemented {
			t.Error("the D1 provider implements ReadSessionStateSnapshot but the manifest reports it missing")
		}
	}
	if !found {
		t.Fatal("SessionStateSnapshotReader is missing from the capability manifest")
	}
}
