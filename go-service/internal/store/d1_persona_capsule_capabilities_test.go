package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// D1 persona capsule capability tests.
//
// Every test runs against the real SQLite engine through the same D1 transport
// the Worker uses (newD1TestStore applies every tracked D1 migration to a real
// SQLite database with foreign keys on), so these assert the statement D1
// actually executes rather than a mock's idea of it.
//
// The seeds are chosen so that each way this lane can go wrong is a DISTINCT row,
// because the failure modes here are all silent. A create that writes the capsule
// but loses an entry produces a recollection bundle that opens empty and looks
// like the user forgot to save. An overlay that drops its NULLIF guard replaces a
// surviving evidence line with an empty string. An attached-entries read that
// widens injects one session's private recollections into another session's
// prompt. None of those raise an error, so each gets its own row.

// ---------------------------------------------------------------------------
// probe transport
// ---------------------------------------------------------------------------

// d1CapsuleProbeConn wraps the real SQLite D1 transport and records what was
// sent. It exists because the two load-bearing properties of this lane are not
// observable in the returned values: that the capsule and its entries travel in
// ONE batch, and that the statement text still carries the COALESCE/NULLIF
// overlay. Both are asserted against the recorded statements below rather than
// against a re-implementation of the query in the test.
type d1CapsuleProbeConn struct {
	*sqliteD1Conn
	queries []string
	batches [][]D1Statement
	// failOn aborts a batch whose statement contains this fragment, standing in
	// for a D1 batch that rolls the whole sequence back.
	failOn string
}

func (c *d1CapsuleProbeConn) QueryRow(ctx context.Context, query string, args ...any) D1Row {
	c.queries = append(c.queries, query)
	return c.sqliteD1Conn.QueryRow(ctx, query, args...)
}

func (c *d1CapsuleProbeConn) Query(ctx context.Context, query string, args ...any) (D1Rows, error) {
	c.queries = append(c.queries, query)
	return c.sqliteD1Conn.Query(ctx, query, args...)
}

func (c *d1CapsuleProbeConn) Batch(ctx context.Context, statements ...D1Statement) error {
	c.batches = append(c.batches, statements)
	for _, statement := range statements {
		if c.failOn != "" && strings.Contains(statement.SQL, c.failOn) {
			return fmt.Errorf("store: injected batch failure at %q", c.failOn)
		}
	}
	return c.sqliteD1Conn.Batch(ctx, statements...)
}

// d1CapsuleProbeStore builds a D1 store over the recording transport.
func d1CapsuleProbeStore(t *testing.T) (*d1Store, *d1CapsuleProbeConn) {
	t.Helper()
	probe := &d1CapsuleProbeConn{sqliteD1Conn: &sqliteD1Conn{db: d1ApplyAllMigrations(t)}}
	st, err := NewD1Store(probe)
	if err != nil {
		t.Fatalf("NewD1Store: %v", err)
	}
	d1, ok := st.(*d1Store)
	if !ok {
		t.Fatalf("NewD1Store returned %T, want *d1Store", st)
	}
	return d1, probe
}

// ---------------------------------------------------------------------------
// seeds
// ---------------------------------------------------------------------------

// d1CapsuleTime renders a seed timestamp in the store's own format, substituting
// a fixed base time for a blank one. The base time is fixed rather than the wall
// clock so an ordering assertion never depends on when the test ran.
func d1CapsuleTime(t *testing.T, text string) string {
	t.Helper()
	if strings.TrimSpace(text) == "" {
		text = "2026-02-01T00:00:00Z"
	}
	parsed, err := parseD1Time(text)
	if err != nil {
		t.Fatalf("parse capsule test timestamp %q: %v", text, err)
	}
	return d1TimeValue(parsed)
}

// d1CapsuleNullableInt64 and d1CapsuleNullableFloat64 turn an absent optional
// value into a SQL NULL. The overlay's COALESCE arms are only distinguishable
// from the entry snapshot when the live column really is NULL, so a seed has to
// be able to store NULL rather than 0.
func d1CapsuleNullableInt64(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func d1CapsuleNullableFloat64(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

// d1CapsuleInt64 and d1CapsuleFloat64 are literals for the pointer fields above,
// so a seed line reads as the value it stores.
func d1CapsuleInt64(v int64) *int64       { return &v }
func d1CapsuleFloat64(v float64) *float64 { return &v }

// d1CapsuleSeed describes one persona_memory_capsules row.
type d1CapsuleSeed struct {
	personaKey string
	sessionID  string
	sourceChar string
	title      string
	mode       string
	summary    string
	createdAt  string
	updatedAt  string
}

// d1SeedPersonaCapsule inserts one capsule and returns its id. The two
// NOT NULL-with-default text columns fall back to the schema defaults so a seed
// can leave them out.
func d1SeedPersonaCapsule(t *testing.T, conn *sqliteD1Conn, seed d1CapsuleSeed) int64 {
	t.Helper()
	if seed.title == "" {
		seed.title = "Persona Memory Capsule"
	}
	if seed.mode == "" {
		seed.mode = "manual"
	}
	var id int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO persona_memory_capsules
			(persona_key, source_chat_session_id, source_character_name, title, mode, summary, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		seed.personaKey, seed.sessionID, d1NullableString(seed.sourceChar),
		seed.title, seed.mode, d1NullableString(seed.summary),
		d1CapsuleTime(t, seed.createdAt), d1CapsuleTime(t, seed.updatedAt),
	).Scan(&id); err != nil {
		t.Fatalf("seed persona capsule (%q/%q): %v", seed.personaKey, seed.sessionID, err)
	}
	return id
}

// d1CapsuleEntrySeed describes one persona_memory_entries row: the snapshot the
// capsule froze at build time.
type d1CapsuleEntrySeed struct {
	capsuleID        int64
	sourceMemoryType string
	sourceMemoryID   int64
	sourceTurn       int
	memoryText       string
	emotionalWeight  float64
	importance10     float64
	portability      string
	tagsJSON         string
	evidence         string
	injectionPolicy  string
	createdAt        string
}

// d1SeedPersonaCapsuleEntry inserts one entry and returns its id.
func d1SeedPersonaCapsuleEntry(t *testing.T, conn *sqliteD1Conn, seed d1CapsuleEntrySeed) int64 {
	t.Helper()
	if seed.portability == "" {
		seed.portability = "same_chat"
	}
	if seed.injectionPolicy == "" {
		seed.injectionPolicy = "support_only"
	}
	var id int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO persona_memory_entries
			(capsule_id, source_memory_type, source_memory_id, source_turn_index, memory_text,
			 emotional_weight, importance_10, portability, tags_json, evidence_excerpt, injection_policy, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		seed.capsuleID, d1NullableString(seed.sourceMemoryType), d1CapsuleNullableInt64(d1CapsuleID(seed.sourceMemoryID)),
		seed.sourceTurn, seed.memoryText, seed.emotionalWeight, seed.importance10,
		seed.portability, d1NullableString(seed.tagsJSON), d1NullableString(seed.evidence),
		seed.injectionPolicy, d1CapsuleTime(t, seed.createdAt),
	).Scan(&id); err != nil {
		t.Fatalf("seed persona capsule entry (capsule=%d): %v", seed.capsuleID, err)
	}
	return id
}

// d1CapsuleID renders a non-positive entry source id as NULL, mirroring what the
// create path stores for a snapshot-only entry.
func d1CapsuleID(id int64) *int64 {
	if id <= 0 {
		return nil
	}
	return &id
}

// d1CapsuleAttachmentSeed describes one persona_capsule_attachments row.
type d1CapsuleAttachmentSeed struct {
	capsuleID     int64
	targetSID     string
	injectionMode string
	enabled       bool
	createdAt     string
	updatedAt     string
}

// d1SeedPersonaCapsuleAttachment inserts one attachment and returns its id.
func d1SeedPersonaCapsuleAttachment(t *testing.T, conn *sqliteD1Conn, seed d1CapsuleAttachmentSeed) int64 {
	t.Helper()
	if seed.injectionMode == "" {
		seed.injectionMode = "subtle_deja_vu"
	}
	var id int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO persona_capsule_attachments
			(capsule_id, target_chat_session_id, injection_mode, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		RETURNING id`,
		seed.capsuleID, seed.targetSID, seed.injectionMode, d1BoolValue(seed.enabled),
		d1CapsuleTime(t, seed.createdAt), d1CapsuleTime(t, seed.updatedAt),
	).Scan(&id); err != nil {
		t.Fatalf("seed persona capsule attachment (capsule=%d target=%q): %v", seed.capsuleID, seed.targetSID, err)
	}
	return id
}

// d1CapsuleMemorySeed describes the protagonist_entity_memories row an entry may
// point at. The pointer fields exist because "the live memory has nothing to say
// about this column" is a NULL, and a seed that wrote 0 instead would silently
// make the entry's own snapshot unreachable through the overlay.
type d1CapsuleMemorySeed struct {
	personaKey      string
	personaName     string
	sessionID       string
	sourceTurn      *int64
	memoryText      string
	evidence        string
	portability     string
	tagsJSON        string
	importance10    *float64
	emotionalWeight *float64
	// blankPortability stores '' instead of the schema default. portability is
	// NOT NULL, so an unusable value can only be an empty string, and that is
	// precisely the row the NULLIF(p.portability, '') guard exists for: without
	// it, a memory with no portability of its own would overwrite the capsule's
	// snapshot with an empty string.
	blankPortability bool
}

// d1CapsuleSeedMemory inserts one subjective memory and returns its id. A blank
// text field is stored as NULL, which is the "absent" case the overlay must fall
// back from.
func d1CapsuleSeedMemory(t *testing.T, conn *sqliteD1Conn, seed d1CapsuleMemorySeed) int64 {
	t.Helper()
	if seed.personaKey == "" {
		seed.personaKey = "owner-key"
	}
	if seed.personaName == "" {
		seed.personaName = "Mira"
	}
	if seed.sessionID == "" {
		seed.sessionID = "s1"
	}
	if seed.portability == "" {
		seed.portability = "portable_persona_recollection"
	}
	if seed.blankPortability {
		seed.portability = ""
	}
	var id int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO protagonist_entity_memories
			(persona_entity_key, persona_entity_name, source_chat_session_id, source_turn_index,
			 memory_text, evidence_excerpt, portability, tags_json, importance_10, emotional_weight)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		seed.personaKey, seed.personaName, seed.sessionID, seed.sourceTurn,
		seed.memoryText, d1NullableString(seed.evidence), seed.portability,
		d1NullableString(seed.tagsJSON), d1CapsuleNullableFloat64(seed.importance10),
		d1CapsuleNullableFloat64(seed.emotionalWeight),
	).Scan(&id); err != nil {
		t.Fatalf("seed subjective memory for capsule overlay: %v", err)
	}
	return id
}

// ---------------------------------------------------------------------------
// small result helpers
// ---------------------------------------------------------------------------

// d1CapsuleIDs reduces a capsule list to its ids, in result order.
func d1CapsuleIDs(capsules []PersonaMemoryCapsule) []int64 {
	out := make([]int64, 0, len(capsules))
	for _, capsule := range capsules {
		out = append(out, capsule.ID)
	}
	return out
}

// d1CapsuleEntryTexts reduces an entry list to its memory text, in result order.
func d1CapsuleEntryTexts(entries []PersonaMemoryEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.MemoryText)
	}
	return out
}

// d1CapsuleEntryIDs reduces an entry list to its ids, in result order.
func d1CapsuleEntryIDs(entries []PersonaMemoryEntry) []int64 {
	out := make([]int64, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.ID)
	}
	return out
}

// d1CapsuleAttachmentTargets reduces an attachment list to its capsule ids.
func d1CapsuleAttachmentTargets(attachments []PersonaCapsuleAttachment) []int64 {
	out := make([]int64, 0, len(attachments))
	for _, attachment := range attachments {
		out = append(out, attachment.CapsuleID)
	}
	return out
}

// ---------------------------------------------------------------------------
// create
// ---------------------------------------------------------------------------

// TestD1PersonaCapsuleCreateCommitsCapsuleAndEntriesAsOneBatch pins the write
// half of the lane: the defaults, the exact bind set, and — the property the
// MariaDB transaction existed for — that the capsule and every entry travel in
// one batch so a failure cannot leave a half-built bundle behind.
func TestD1PersonaCapsuleCreateCommitsCapsuleAndEntriesAsOneBatch(t *testing.T) {
	st, probe := d1CapsuleProbeStore(t)
	ctx := context.Background()

	created, err := st.CreatePersonaMemoryCapsule(ctx,
		&PersonaMemoryCapsule{
			PersonaKey:          "owner-key",
			SourceChatSessionID: "s1",
			SourceCharacterName: "Mira",
			Title:               "   ",
			Mode:                "\t ",
		},
		[]PersonaMemoryEntry{
			{
				SourceMemoryType: "subjective_entity_memory", SourceMemoryID: 7, SourceTurn: 4,
				MemoryText: "linked recollection", EmotionalWeight: 0.5, Importance10: 8,
				Portability: "cross_world", TagsJSON: `["kept"]`, EvidenceExcerpt: "the evidence line",
				InjectionPolicy: "direct",
			},
			{MemoryText: "snapshot only"},
		},
	)
	if err != nil {
		t.Fatalf("CreatePersonaMemoryCapsule: %v", err)
	}

	// The returned record reports what was persisted, not what was requested: a
	// caller that re-reads the capsule must see the same defaults.
	if created.ID <= 0 {
		t.Fatalf("create must report the stored id, got %d", created.ID)
	}
	if created.Title != "Persona Memory Capsule" || created.Mode != "manual" {
		t.Errorf("defaults not applied on the return value: title=%q mode=%q", created.Title, created.Mode)
	}
	if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() || !created.CreatedAt.Equal(created.UpdatedAt) {
		t.Errorf("timestamps must be set and equal: created=%s updated=%s", created.CreatedAt, created.UpdatedAt)
	}

	// One batch, capsule first then one statement per entry. Anything else would
	// mean the entries are no longer atomic with their capsule.
	if len(probe.batches) != 1 {
		t.Fatalf("create sent %d batches, want exactly 1", len(probe.batches))
	}
	statements := probe.batches[0]
	if len(statements) != 3 {
		t.Fatalf("create batch carried %d statements, want 3 (capsule + 2 entries)", len(statements))
	}
	for _, want := range []string{
		"INSERT INTO persona_memory_capsules",
		"persona_key, source_chat_session_id, source_character_name, title, mode, summary, created_at, updated_at",
	} {
		if !strings.Contains(statements[0].SQL, want) {
			t.Errorf("capsule insert is missing %q:\n%s", want, statements[0].SQL)
		}
	}
	for _, want := range []string{
		"INSERT INTO persona_memory_entries",
		"capsule_id, source_memory_type, source_memory_id, source_turn_index, memory_text, " +
			"emotional_weight, importance_10, portability, tags_json, evidence_excerpt, injection_policy, created_at",
	} {
		if !strings.Contains(statements[1].SQL, want) {
			t.Errorf("entry insert is missing %q:\n%s", want, statements[1].SQL)
		}
	}
	// Both entry statements must name the capsule the batch just reserved,
	// otherwise a second capsule could adopt the entries.
	if got := statements[1].Args[0]; got != created.ID {
		t.Errorf("entry capsule_id arg = %v, want the created capsule %d", got, created.ID)
	}
	if got := statements[2].Args[0]; got != created.ID {
		t.Errorf("second entry capsule_id arg = %v, want the created capsule %d", got, created.ID)
	}

	if got := d1Count(t, probe.sqliteD1Conn, `SELECT COUNT(*) FROM persona_memory_capsules`); got != 1 {
		t.Errorf("capsules = %d, want 1", got)
	}
	if got := d1Count(t, probe.sqliteD1Conn, `SELECT COUNT(*) FROM persona_memory_entries`); got != 2 {
		t.Fatalf("entries = %d, want 2", got)
	}

	// The linked entry keeps every value it was given.
	var portability, tagsJSON, evidence, injectionPolicy string
	var sourceTurn int
	if err := probe.QueryRow(ctx, `
		SELECT portability, tags_json, evidence_excerpt, injection_policy, source_turn_index
		FROM persona_memory_entries WHERE memory_text = ?`, "linked recollection").
		Scan(&portability, &tagsJSON, &evidence, &injectionPolicy, &sourceTurn); err != nil {
		t.Fatalf("read linked entry: %v", err)
	}
	if portability != "cross_world" || tagsJSON != `["kept"]` ||
		evidence != "the evidence line" || injectionPolicy != "direct" || sourceTurn != 4 {
		t.Errorf("linked entry lost a supplied value: %+v", []any{portability, tagsJSON, evidence, injectionPolicy, sourceTurn})
	}

	// The snapshot-only entry takes the reference defaults, and its absent source
	// reference is stored as NULL. That NULL is what keeps the overlay join from
	// matching it against an unrelated memory row.
	var sourceMemoryType *string
	var sourceMemoryID *int64
	if err := probe.QueryRow(ctx, `
		SELECT portability, injection_policy, tags_json, source_memory_type, source_memory_id, source_turn_index
		FROM persona_memory_entries WHERE memory_text = ?`, "snapshot only").
		Scan(&portability, &injectionPolicy, &tagsJSON, &sourceMemoryType, &sourceMemoryID, &sourceTurn); err != nil {
		t.Fatalf("read snapshot entry: %v", err)
	}
	if portability != "same_chat" || injectionPolicy != "support_only" || tagsJSON != "[]" {
		t.Errorf("entry defaults = %q/%q/%q, want same_chat/support_only/[]", portability, injectionPolicy, tagsJSON)
	}
	if sourceMemoryType != nil || sourceMemoryID != nil {
		t.Errorf("a snapshot-only entry must store NULL source reference, got %v/%v", sourceMemoryType, sourceMemoryID)
	}
	// A missing turn is a stored 0 on both providers: the reference binds the
	// value unconditionally rather than storing NULL.
	if sourceTurn != 0 {
		t.Errorf("absent source_turn_index = %d, want 0 (bound unconditionally, like the reference)", sourceTurn)
	}
}

// TestD1PersonaCapsuleCreateRollsBackTheWholeBatchOnFailure pins the atomicity
// the MariaDB BeginTx/Commit provided. A capsule that exists without its entries
// is not an error the caller can see: it opens empty in the browser and looks
// like nothing was ever saved.
func TestD1PersonaCapsuleCreateRollsBackTheWholeBatchOnFailure(t *testing.T) {
	st, probe := d1CapsuleProbeStore(t)
	probe.failOn = "INSERT INTO persona_memory_entries"
	ctx := context.Background()

	capsule, err := st.CreatePersonaMemoryCapsule(ctx,
		&PersonaMemoryCapsule{PersonaKey: "owner-key", SourceChatSessionID: "s1", Title: "capsule"},
		[]PersonaMemoryEntry{{MemoryText: "one"}, {MemoryText: "two"}},
	)
	if err == nil {
		t.Fatal("a failing entry insert must fail the whole create")
	}
	if capsule != nil {
		t.Errorf("a failed create must not return a capsule: %+v", capsule)
	}
	if got := d1Count(t, probe.sqliteD1Conn, `SELECT COUNT(*) FROM persona_memory_capsules`); got != 0 {
		t.Errorf("capsules = %d after a failed batch, want 0", got)
	}
	if got := d1Count(t, probe.sqliteD1Conn, `SELECT COUNT(*) FROM persona_memory_entries`); got != 0 {
		t.Errorf("entries = %d after a failed batch, want 0", got)
	}

	// The next create still succeeds and gets the first id, so the failed attempt
	// did not burn an id or leave the sequence in a state that breaks the retry.
	if _, err := st.CreatePersonaMemoryCapsule(ctx,
		&PersonaMemoryCapsule{PersonaKey: "owner-key", SourceChatSessionID: "s1"}, nil); err != nil {
		t.Fatalf("create after a rolled-back create: %v", err)
	}
	if got := d1Count(t, probe.sqliteD1Conn, `SELECT COUNT(*) FROM persona_memory_capsules`); got != 1 {
		t.Errorf("capsules = %d after the retry, want 1", got)
	}
}

// TestD1PersonaCapsuleIDIsNotReusedAfterDelete pins the allocator's one job
// beyond naming a row. MariaDB's AUTO_INCREMENT never reissues an id, so a client
// holding a deleted capsule id must not suddenly find a different capsule there.
func TestD1PersonaCapsuleIDIsNotReusedAfterDelete(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	first, err := st.CreatePersonaMemoryCapsule(ctx,
		&PersonaMemoryCapsule{PersonaKey: "owner-key", SourceChatSessionID: "s1"}, nil)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	if err := st.DeletePersonaMemoryCapsule(ctx, first.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	second, err := st.CreatePersonaMemoryCapsule(ctx,
		&PersonaMemoryCapsule{PersonaKey: "owner-key", SourceChatSessionID: "s1"}, nil)
	if err != nil {
		t.Fatalf("create after delete: %v", err)
	}
	if second.ID <= first.ID {
		t.Errorf("capsule id %d reused a deleted id %d; AUTO_INCREMENT never reissues", second.ID, first.ID)
	}
}

// ---------------------------------------------------------------------------
// list and get
// ---------------------------------------------------------------------------

// TestD1PersonaCapsuleListFiltersConjunctivelyAndOrdersByRecency pins the capsule
// list: the two filters are independent, a blank filter applies no predicate at
// all, and the ordering is recency with a stable id tiebreak so two capsules
// touched in the same instant still have a defined order.
func TestD1PersonaCapsuleListFiltersConjunctivelyAndOrdersByRecency(t *testing.T) {
	st, probe := d1CapsuleProbeStore(t)
	ctx := context.Background()

	// Two capsules share a persona and a session; one is newer.
	older := d1SeedPersonaCapsule(t, probe.sqliteD1Conn, d1CapsuleSeed{
		personaKey: "owner-a", sessionID: "s1", title: "older", mode: "manual",
		createdAt: "2026-03-01T00:00:00Z", updatedAt: "2026-03-01T00:00:00Z",
	})
	newer := d1SeedPersonaCapsule(t, probe.sqliteD1Conn, d1CapsuleSeed{
		personaKey: "owner-a", sessionID: "s1", title: "newer", mode: "full_loop_memory",
		createdAt: "2026-03-02T00:00:00Z", updatedAt: "2026-03-02T00:00:00Z",
	})
	// A same-instant pair, so the id DESC tiebreak is what decides their order.
	tieLow := d1SeedPersonaCapsule(t, probe.sqliteD1Conn, d1CapsuleSeed{
		personaKey: "owner-a", sessionID: "s1", title: "tie-low",
		createdAt: "2026-03-03T00:00:00Z", updatedAt: "2026-03-03T00:00:00Z",
	})
	tieHigh := d1SeedPersonaCapsule(t, probe.sqliteD1Conn, d1CapsuleSeed{
		personaKey: "owner-a", sessionID: "s1", title: "tie-high",
		createdAt: "2026-03-03T00:00:00Z", updatedAt: "2026-03-03T00:00:00Z",
	})
	otherPersona := d1SeedPersonaCapsule(t, probe.sqliteD1Conn, d1CapsuleSeed{
		personaKey: "owner-b", sessionID: "s1", title: "other persona",
		createdAt: "2026-03-04T00:00:00Z", updatedAt: "2026-03-04T00:00:00Z",
	})
	otherSession := d1SeedPersonaCapsule(t, probe.sqliteD1Conn, d1CapsuleSeed{
		personaKey: "owner-a", sessionID: "s2", title: "other session",
		createdAt: "2026-03-05T00:00:00Z", updatedAt: "2026-03-05T00:00:00Z",
	})

	all, err := st.ListPersonaMemoryCapsules(ctx, PersonaCapsuleFilter{})
	if err != nil {
		t.Fatalf("ListPersonaMemoryCapsules: %v", err)
	}
	// updated_at DESC, then id DESC: the other-session capsule is newest, the tie
	// pair resolves to the higher id first.
	want := []int64{otherSession, otherPersona, tieHigh, tieLow, newer, older}
	if got := d1CapsuleIDs(all); !equalInt64Slices(got, want) {
		t.Fatalf("all capsules = %v, want %v", got, want)
	}

	byPersona, err := st.ListPersonaMemoryCapsules(ctx, PersonaCapsuleFilter{PersonaKey: "  owner-a  "})
	if err != nil {
		t.Fatalf("persona filter: %v", err)
	}
	// The persona filter is one predicate, not a session predicate: owner-a's
	// capsule in ANOTHER session still belongs to owner-a and must be returned.
	// This is what makes the next assertion meaningful: only the second filter
	// narrows the result.
	if got := d1CapsuleIDs(byPersona); !equalInt64Slices(got, []int64{otherSession, tieHigh, tieLow, newer, older}) {
		t.Errorf("persona filter = %v, want every owner-a capsule", got)
	}

	both, err := st.ListPersonaMemoryCapsules(ctx, PersonaCapsuleFilter{
		PersonaKey: "owner-a", SourceChatSessionID: "s1",
	})
	if err != nil {
		t.Fatalf("both filters: %v", err)
	}
	if got := d1CapsuleIDs(both); !equalInt64Slices(got, []int64{tieHigh, tieLow, newer, older}) {
		t.Errorf("conjunctive filters = %v, want owner-a in s1", got)
	}

	// The filters are conjunctive, so a persona that exists in another session
	// must not leak in through the session filter alone.
	sessionOnly, err := st.ListPersonaMemoryCapsules(ctx, PersonaCapsuleFilter{SourceChatSessionID: "s2"})
	if err != nil {
		t.Fatalf("session filter: %v", err)
	}
	if got := d1CapsuleIDs(sessionOnly); !equalInt64Slices(got, []int64{otherSession}) {
		t.Errorf("session filter = %v, want only the s2 capsule", got)
	}

	// A whitespace-only filter value applies NO predicate, exactly as a blank one
	// does; treating it as a literal would return an empty list instead of all of
	// them, which is the difference between "no capsules" and "every capsule".
	blank, err := st.ListPersonaMemoryCapsules(ctx, PersonaCapsuleFilter{PersonaKey: "   ", SourceChatSessionID: "\t"})
	if err != nil {
		t.Fatalf("blank filter: %v", err)
	}
	if len(blank) != len(all) {
		t.Errorf("a blank filter changed the result set: %d rows, want %d", len(blank), len(all))
	}

	// Nullable columns round-trip, and a NULL reads as empty text on both
	// providers rather than as a value the caller has to special-case.
	if all[0].SourceCharacterName != "" || all[0].Summary != "" {
		t.Errorf("NULL capsule text must read as empty: %+v", all[0])
	}
	if all[0].CreatedAt.IsZero() || all[0].UpdatedAt.IsZero() {
		t.Errorf("capsule timestamps must be parsed: %+v", all[0])
	}

	none, err := st.ListPersonaMemoryCapsules(ctx, PersonaCapsuleFilter{PersonaKey: "owner-absent"})
	if err != nil {
		t.Fatalf("absent persona: %v", err)
	}
	if none == nil || len(none) != 0 {
		t.Errorf("empty capsule list = %#v, want an empty non-nil slice", none)
	}
}

// equalInt64Slices compares two id slices element by element.
func equalInt64Slices(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestD1PersonaCapsuleGetOverlaysLiveSubjectiveMemoryOntoEntrySnapshots pins the
// whole read half of the lane, which is where the COALESCE/NULLIF layer lives.
//
// Four rows, one per overlay rule:
//   - a fully populated live memory takes over every resolved column;
//   - a live memory with an EMPTY memory_text must fall back to the snapshot,
//     which is the NULLIF guard — without it the surviving text would be erased;
//   - a live memory whose optional numbers are NULL falls back to the entry's;
//   - a snapshot-only entry whose source_memory_id COLLIDES with a memory id must
//     keep its own text, because the join is guarded by source_memory_type.
func TestD1PersonaCapsuleGetOverlaysLiveSubjectiveMemoryOntoEntrySnapshots(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	full := d1CapsuleSeedMemory(t, conn, d1CapsuleMemorySeed{
		sourceTurn: d1CapsuleInt64(1), memoryText: "live text wins",
		evidence: "live evidence", portability: "cross_world", tagsJSON: `["live"]`,
		importance10: d1CapsuleFloat64(9), emotionalWeight: d1CapsuleFloat64(0.9),
	})
	// A live memory whose user-editable text was cleared to empty, whose
	// portability was never set, and whose optional columns are NULL. Leaving
	// source_turn_index NULL is what makes the entry's own turn the resolved one.
	blanked := d1CapsuleSeedMemory(t, conn, d1CapsuleMemorySeed{
		memoryText: "", importance10: nil, emotionalWeight: nil, blankPortability: true,
	})

	capsuleID := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{
		personaKey: "owner-key", sessionID: "s1", title: "capsule", updatedAt: "2026-03-01T00:00:00Z",
	})
	// Inserted out of order on purpose: the read orders by the RESOLVED turn, so a
	// snapshot turn of 9 that the live memory moves to 1 must lead.
	d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{
		capsuleID: capsuleID, sourceMemoryType: "subjective_entity_memory", sourceMemoryID: full,
		sourceTurn: 9, memoryText: "snapshot text loses", importance10: 1, emotionalWeight: 0.1,
		tagsJSON: `["snapshot"]`, evidence: "snapshot evidence", portability: "same_chat",
		injectionPolicy: "support_only",
	})
	d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{
		capsuleID: capsuleID, sourceMemoryType: "subjective_entity_memory", sourceMemoryID: blanked,
		sourceTurn: 4, memoryText: "snapshot survives a cleared live text", importance10: 6, emotionalWeight: 0.4,
		tagsJSON: `["snapshot"]`, evidence: "snapshot evidence survives", portability: "same_chat",
	})
	// A snapshot-only entry that deliberately points at a real memory id. Only the
	// source_memory_type guard in the join keeps it from being rewritten.
	d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{
		capsuleID: capsuleID, sourceMemoryType: "manual_note", sourceMemoryID: full,
		sourceTurn: 3, memoryText: "snapshot is authoritative", importance10: 5,
	})

	capsule, entries, err := st.GetPersonaMemoryCapsule(ctx, capsuleID)
	if err != nil {
		t.Fatalf("GetPersonaMemoryCapsule: %v", err)
	}
	if capsule.ID != capsuleID || capsule.Title != "capsule" || capsule.Mode != "manual" {
		t.Errorf("capsule projection = %+v", capsule)
	}
	// Resolved turn ASC, then e.id ASC. The first entry's own snapshot says turn
	// 9 and the live memory moves it to 1, so a read that ordered by the snapshot
	// would put it last; the third entry's live turn is NULL and resolves to its
	// own 4.
	want := []string{
		"live text wins",
		"snapshot is authoritative",
		"snapshot survives a cleared live text",
	}
	if got := d1CapsuleEntryTexts(entries); !equalStringSlices(got, want) {
		t.Fatalf("entry order = %v, want %v (resolved source turn ASC, then e.id ASC)", got, want)
	}

	// The live memory takes over every resolved column...
	overlaid := entries[0]
	if overlaid.SourceTurn != 1 || overlaid.EmotionalWeight != 0.9 || overlaid.Importance10 != 9 {
		t.Errorf("live numbers not overlaid: %+v", overlaid)
	}
	if overlaid.Portability != "cross_world" || overlaid.TagsJSON != `["live"]` || overlaid.EvidenceExcerpt != "live evidence" {
		t.Errorf("live text not overlaid: %+v", overlaid)
	}
	// ...but the injection policy is the capsule's own decision and is read from
	// the entry, because the live memory row has no injection policy to give.
	if overlaid.InjectionPolicy != "support_only" {
		t.Errorf("injection_policy must come from the entry snapshot, got %q", overlaid.InjectionPolicy)
	}
	if overlaid.SourceMemoryType != "subjective_entity_memory" || overlaid.SourceMemoryID != full {
		t.Errorf("source reference lost: %+v", overlaid)
	}

	// A snapshot-only entry that collides on id with a live memory is untouched.
	authoritative := entries[1]
	if authoritative.MemoryText != "snapshot is authoritative" || authoritative.Importance10 != 5 {
		t.Errorf("a non-subjective entry was rewritten by the overlay: %+v", authoritative)
	}

	// The NULLIF guard: an empty live memory_text must leave the snapshot text in
	// place, an empty live portability must leave the snapshot's, and NULL live
	// columns and numbers must leave the snapshot's own values in place.
	fellBack := entries[2]
	if fellBack.MemoryText != "snapshot survives a cleared live text" {
		t.Errorf("cleared live text erased the snapshot: %+v", fellBack)
	}
	if fellBack.EvidenceExcerpt != "snapshot evidence survives" || fellBack.Portability != "same_chat" {
		t.Errorf("empty live columns must not overwrite the snapshot: %+v", fellBack)
	}
	if fellBack.TagsJSON != `["snapshot"]` {
		t.Errorf("NULL live tags must fall back to the snapshot: %+v", fellBack)
	}
	if fellBack.SourceTurn != 4 || fellBack.Importance10 != 6 || fellBack.EmotionalWeight != 0.4 {
		t.Errorf("NULL live numbers must fall back to the snapshot: %+v", fellBack)
	}
	for _, entry := range entries {
		if entry.CapsuleID != capsuleID || entry.CreatedAt.IsZero() {
			t.Errorf("entry lost its capsule or timestamp: %+v", entry)
		}
	}

	// A capsule with no entries is a capsule, not a missing one: ErrNotFound is
	// reserved for an id that addressed no row at all.
	emptyCapsule := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{personaKey: "owner-key", sessionID: "s1"})
	found, emptyEntries, err := st.GetPersonaMemoryCapsule(ctx, emptyCapsule)
	if err != nil {
		t.Fatalf("get a capsule with no entries: %v", err)
	}
	if found.ID != emptyCapsule {
		t.Errorf("empty capsule projection = %+v", found)
	}
	if emptyEntries == nil || len(emptyEntries) != 0 {
		t.Errorf("entries of an empty capsule = %#v, want an empty non-nil slice", emptyEntries)
	}

	if _, _, err := st.GetPersonaMemoryCapsule(ctx, emptyCapsule+9999); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing capsule error = %v, want ErrNotFound", err)
	}
}

// TestD1PersonaCapsuleReadsKeepTheMariaDBOverlayAndOrdering pins the statement
// text itself. The overlay is a stack of COALESCE and NULLIF, and every layer of
// it is a fallback the caller depends on; a future edit that drops one would look
// like a tidy-up in review and would show up in production as a recollection that
// quietly loses its evidence line.
func TestD1PersonaCapsuleReadsKeepTheMariaDBOverlayAndOrdering(t *testing.T) {
	st, probe := d1CapsuleProbeStore(t)
	ctx := context.Background()

	capsuleID := d1SeedPersonaCapsule(t, probe.sqliteD1Conn, d1CapsuleSeed{personaKey: "owner-key", sessionID: "s1"})
	probe.queries = nil

	if _, _, err := st.GetPersonaMemoryCapsule(ctx, capsuleID); err != nil {
		t.Fatalf("GetPersonaMemoryCapsule: %v", err)
	}
	if len(probe.queries) != 2 {
		t.Fatalf("get sent %d statements, want 2 (capsule then entries)", len(probe.queries))
	}
	entryQuery := probe.queries[1]
	for _, want := range []string{
		"FROM persona_memory_entries e",
		"LEFT JOIN protagonist_entity_memories p",
		"ON e.source_memory_type = 'subjective_entity_memory'",
		"AND e.source_memory_id = p.id",
		"COALESCE(p.source_turn_index, e.source_turn_index)",
		"COALESCE(NULLIF(p.memory_text, ''), e.memory_text)",
		"COALESCE(p.emotional_weight, e.emotional_weight)",
		"COALESCE(p.importance_10, e.importance_10)",
		"COALESCE(NULLIF(p.portability, ''), e.portability)",
		"COALESCE(p.tags_json, e.tags_json)",
		"COALESCE(NULLIF(p.evidence_excerpt, ''), e.evidence_excerpt)",
		"e.injection_policy",
		"WHERE e.capsule_id = ?",
		"ORDER BY COALESCE(p.source_turn_index, e.source_turn_index) ASC, e.id ASC",
	} {
		if !strings.Contains(entryQuery, want) {
			t.Errorf("per-capsule entry read is missing %q:\n%s", want, entryQuery)
		}
	}
	// The per-capsule read must NOT join the attachment table: the browser shows
	// a capsule's own entries whether or not it is attached anywhere.
	if strings.Contains(entryQuery, "persona_capsule_attachments") {
		t.Errorf("the per-capsule read must not join attachments:\n%s", entryQuery)
	}

	probe.queries = nil
	if _, err := st.ListAttachedPersonaMemoryEntries(ctx, "s1", 2); err != nil {
		t.Fatalf("ListAttachedPersonaMemoryEntries: %v", err)
	}
	if len(probe.queries) != 1 {
		t.Fatalf("attached read sent %d statements, want 1", len(probe.queries))
	}
	attachedQuery := probe.queries[0]
	for _, want := range []string{
		"INNER JOIN persona_capsule_attachments a ON a.capsule_id = e.capsule_id",
		"WHERE a.target_chat_session_id = ? AND a.enabled = TRUE",
		"ORDER BY COALESCE(p.importance_10, e.importance_10) DESC, e.id ASC",
		"LIMIT ?",
	} {
		if !strings.Contains(attachedQuery, want) {
			t.Errorf("attached read is missing %q:\n%s", want, attachedQuery)
		}
	}
	// The shared projection must be byte-identical between the two reads, or the
	// browser and prepare-turn would show two versions of one recollection.
	for _, want := range []string{
		"COALESCE(NULLIF(p.memory_text, ''), e.memory_text)",
		"COALESCE(NULLIF(p.evidence_excerpt, ''), e.evidence_excerpt)",
	} {
		if !strings.Contains(attachedQuery, want) {
			t.Errorf("attached read lost the shared projection %q:\n%s", want, attachedQuery)
		}
	}

	// A non-positive limit is "no limit": prepare-turn calls this with 0 and then
	// applies its own candidate budget, so a page here would truncate the
	// candidate set rather than bound a response.
	probe.queries = nil
	if _, err := st.ListAttachedPersonaMemoryEntries(ctx, "s1", 0); err != nil {
		t.Fatalf("unbounded attached read: %v", err)
	}
	if strings.Contains(strings.ToUpper(probe.queries[0]), "LIMIT") {
		t.Errorf("a non-positive limit must not page the result set:\n%s", probe.queries[0])
	}
}

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

// TestD1PersonaCapsuleDeleteCascadesAndReportsNotFound pins the delete sentinel
// and the cascade. A capsule whose entries survive it is a bundle the user can
// no longer reach but that still occupies the attachment table, and a second
// delete that silently succeeds hides a genuine double-submit from the route.
func TestD1PersonaCapsuleDeleteCascadesAndReportsNotFound(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	capsuleID := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{personaKey: "owner-key", sessionID: "s1"})
	otherCapsuleID := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{personaKey: "owner-key", sessionID: "s1"})
	d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{capsuleID: capsuleID, memoryText: "entry one"})
	d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{capsuleID: capsuleID, memoryText: "entry two"})
	d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{capsuleID: otherCapsuleID, memoryText: "other entry"})
	d1SeedPersonaCapsuleAttachment(t, conn, d1CapsuleAttachmentSeed{capsuleID: capsuleID, targetSID: "s2"})
	d1SeedPersonaCapsuleAttachment(t, conn, d1CapsuleAttachmentSeed{capsuleID: otherCapsuleID, targetSID: "s2"})

	if err := st.DeletePersonaMemoryCapsule(ctx, capsuleID); err != nil {
		t.Fatalf("DeletePersonaMemoryCapsule: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM persona_memory_capsules`); got != 1 {
		t.Errorf("capsules = %d after delete, want 1", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM persona_memory_entries`); got != 1 {
		t.Errorf("entries = %d after delete, want 1 (the other capsule's entry only)", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM persona_capsule_attachments`); got != 1 {
		t.Errorf("attachments = %d after delete, want 1 (the other capsule's only)", got)
	}

	if err := st.DeletePersonaMemoryCapsule(ctx, capsuleID); !errors.Is(err, ErrNotFound) {
		t.Errorf("second delete error = %v, want ErrNotFound", err)
	}
	if err := st.DeletePersonaMemoryCapsule(ctx, capsuleID+9999); !errors.Is(err, ErrNotFound) {
		t.Errorf("delete of an unknown id = %v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// attach / detach
// ---------------------------------------------------------------------------

// TestD1PersonaCapsuleAttachUpdatesTheSameRowOnReplay pins the MySQL
// ON DUPLICATE KEY UPDATE translation. A capsule is enabled for a target session
// by a toggle, so re-attaching must update the one row in place — a second row
// would make the injection read pick an arbitrary one of the two.
func TestD1PersonaCapsuleAttachUpdatesTheSameRowOnReplay(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	capsuleID := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{personaKey: "owner-key", sessionID: "s1"})
	// A pre-existing attachment with a known creation time, so the assertion can
	// tell "updated in place" from "replaced".
	existing := d1SeedPersonaCapsuleAttachment(t, conn, d1CapsuleAttachmentSeed{
		capsuleID: capsuleID, targetSID: "s2", injectionMode: "subtle_deja_vu", enabled: true,
		createdAt: "2026-01-01T00:00:00Z", updatedAt: "2026-01-01T00:00:00Z",
	})

	// A blank injection mode takes the reference default rather than storing ''.
	if err := st.AttachPersonaMemoryCapsule(ctx, &PersonaCapsuleAttachment{
		CapsuleID: capsuleID, TargetChatSessionID: "s1", Enabled: true,
	}); err != nil {
		t.Fatalf("AttachPersonaMemoryCapsule: %v", err)
	}
	attachments, err := st.ListPersonaCapsuleAttachments(ctx, "s1")
	if err != nil {
		t.Fatalf("ListPersonaCapsuleAttachments: %v", err)
	}
	if len(attachments) != 1 || attachments[0].InjectionMode != "subtle_deja_vu" || !attachments[0].Enabled {
		t.Errorf("defaulted attachment = %+v", attachments)
	}
	if attachments[0].CreatedAt.IsZero() || attachments[0].UpdatedAt.IsZero() {
		t.Errorf("attachment timestamps must be parsed: %+v", attachments[0])
	}

	// Re-attaching the same pair is a deliberate last-write-wins toggle, not a
	// second row: created_at is preserved and only updated_at moves.
	if err := st.AttachPersonaMemoryCapsule(ctx, &PersonaCapsuleAttachment{
		CapsuleID: capsuleID, TargetChatSessionID: "s2", InjectionMode: "direct_reveal", Enabled: false,
	}); err != nil {
		t.Fatalf("re-attach: %v", err)
	}
	replayed, err := st.ListPersonaCapsuleAttachments(ctx, "s2")
	if err != nil {
		t.Fatalf("list after re-attach: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("re-attach produced %d rows, want 1", len(replayed))
	}
	if replayed[0].ID != existing {
		t.Errorf("re-attach changed the row identity: %d, want %d", replayed[0].ID, existing)
	}
	if replayed[0].InjectionMode != "direct_reveal" || replayed[0].Enabled {
		t.Errorf("re-attach did not apply the new mode/enabled: %+v", replayed[0])
	}
	seededAt, err := parseD1Time("2026-01-01T00:00:00Z")
	if err != nil {
		t.Fatalf("parse the seeded attachment time: %v", err)
	}
	if !replayed[0].CreatedAt.Equal(seededAt) {
		t.Errorf("created_at = %s, want the original %s preserved by the upsert", replayed[0].CreatedAt, seededAt)
	}
	if !replayed[0].UpdatedAt.After(replayed[0].CreatedAt) {
		t.Errorf("updated_at = %s, want it to move past created_at %s", replayed[0].UpdatedAt, replayed[0].CreatedAt)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM persona_capsule_attachments`); got != 2 {
		t.Errorf("attachments = %d, want 2 (one per target session)", got)
	}

	// Ordering across targets is recency, so the just-touched s2 attachment leads
	// its own list and the s1 attachment is unaffected.
	s1, err := st.ListPersonaCapsuleAttachments(ctx, "s1")
	if err != nil {
		t.Fatalf("list s1 attachments: %v", err)
	}
	if len(s1) != 1 || s1[0].InjectionMode != "subtle_deja_vu" || !s1[0].Enabled {
		t.Errorf("touching s2 must not change the s1 attachment: %+v", s1)
	}

	none, err := st.ListPersonaCapsuleAttachments(ctx, "s9")
	if err != nil {
		t.Fatalf("list unknown target: %v", err)
	}
	if none == nil || len(none) != 0 {
		t.Errorf("unknown target attachments = %#v, want an empty non-nil slice", none)
	}
}

// TestD1PersonaCapsuleDetachRemovesOnlyTheNamedTarget pins the narrow scope of a
// detach: it must not touch the same capsule's attachment to another session, and
// it reports success when nothing matched, because "this capsule is not attached
// there" is the state the caller asked for.
func TestD1PersonaCapsuleDetachRemovesOnlyTheNamedTarget(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	capsuleID := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{personaKey: "owner-key", sessionID: "s1"})
	otherCapsuleID := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{personaKey: "owner-key", sessionID: "s1"})
	d1SeedPersonaCapsuleAttachment(t, conn, d1CapsuleAttachmentSeed{capsuleID: capsuleID, targetSID: "s2"})
	d1SeedPersonaCapsuleAttachment(t, conn, d1CapsuleAttachmentSeed{capsuleID: capsuleID, targetSID: "s3"})
	d1SeedPersonaCapsuleAttachment(t, conn, d1CapsuleAttachmentSeed{capsuleID: otherCapsuleID, targetSID: "s2"})

	if err := st.DetachPersonaMemoryCapsule(ctx, capsuleID, "s2"); err != nil {
		t.Fatalf("DetachPersonaMemoryCapsule: %v", err)
	}
	remaining, err := st.ListPersonaCapsuleAttachments(ctx, "s2")
	if err != nil {
		t.Fatalf("list s2 after detach: %v", err)
	}
	if got := d1CapsuleAttachmentTargets(remaining); !equalInt64Slices(got, []int64{otherCapsuleID}) {
		t.Errorf("s2 attachments = %v, want only the other capsule", got)
	}
	other, err := st.ListPersonaCapsuleAttachments(ctx, "s3")
	if err != nil {
		t.Fatalf("list s3: %v", err)
	}
	if got := d1CapsuleAttachmentTargets(other); !equalInt64Slices(got, []int64{capsuleID}) {
		t.Errorf("s3 attachments = %v, want the untouched capsule", got)
	}

	// Detaching a pair that was never attached is not an error.
	if err := st.DetachPersonaMemoryCapsule(ctx, capsuleID, "s2"); err != nil {
		t.Errorf("detaching a missing attachment must succeed, got %v", err)
	}
	if err := st.DetachPersonaMemoryCapsule(ctx, capsuleID+9999, "s2"); err != nil {
		t.Errorf("detaching an unknown capsule must succeed, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// attached entries (the prepare-turn read)
// ---------------------------------------------------------------------------

// TestD1PersonaCapsuleListAttachedEntriesFencesOrdersAndLimits pins the one call
// that decides what a session's prompt may contain.
//
// The seeds put each exclusion on its own row: a disabled attachment, an
// attachment to a different target session, and a snapshot-only entry that points
// at a live memory id. Widening any of those injects one session's private
// recollections into another session's turn, which is the failure this read must
// never have.
func TestD1PersonaCapsuleListAttachedEntriesFencesOrdersAndLimits(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	// The live memory raises one entry's importance from 1 to 10, so the read can
	// only order by the RESOLVED importance if this row leads.
	boosted := d1CapsuleSeedMemory(t, conn, d1CapsuleMemorySeed{
		memoryText: "boosted live memory", importance10: d1CapsuleFloat64(10),
		emotionalWeight: d1CapsuleFloat64(0.7), sourceTurn: d1CapsuleInt64(2),
	})

	enabled := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{personaKey: "owner-a", sessionID: "s1"})
	disabled := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{personaKey: "owner-a", sessionID: "s1"})
	elsewhere := d1SeedPersonaCapsule(t, conn, d1CapsuleSeed{personaKey: "owner-a", sessionID: "s1"})

	low := d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{
		capsuleID: enabled, sourceTurn: 1, memoryText: "t1-low", importance10: 2,
	})
	high := d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{
		capsuleID: enabled, sourceTurn: 2, memoryText: "t1-high", importance10: 9,
	})
	// A same-importance sibling inserted after `high`, so the e.id ASC tiebreak
	// is what decides their order.
	tie := d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{
		capsuleID: enabled, sourceTurn: 3, memoryText: "t1-tie", importance10: 9,
	})
	overlaid := d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{
		capsuleID: enabled, sourceMemoryType: "subjective_entity_memory", sourceMemoryID: boosted,
		sourceTurn: 9, memoryText: "t1-overlay", importance10: 1,
	})
	// Excluded: the capsule is attached but disabled.
	d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{
		capsuleID: disabled, sourceTurn: 1, memoryText: "t1-disabled", importance10: 20,
	})
	// Excluded: the capsule is enabled but attached to another session.
	d1SeedPersonaCapsuleEntry(t, conn, d1CapsuleEntrySeed{
		capsuleID: elsewhere, sourceTurn: 1, memoryText: "s2-entry", importance10: 20,
	})

	d1SeedPersonaCapsuleAttachment(t, conn, d1CapsuleAttachmentSeed{capsuleID: enabled, targetSID: "s2", enabled: true})
	d1SeedPersonaCapsuleAttachment(t, conn, d1CapsuleAttachmentSeed{capsuleID: disabled, targetSID: "s2", enabled: false})
	d1SeedPersonaCapsuleAttachment(t, conn, d1CapsuleAttachmentSeed{capsuleID: elsewhere, targetSID: "s3", enabled: true})

	entries, err := st.ListAttachedPersonaMemoryEntries(ctx, "s2", 0)
	if err != nil {
		t.Fatalf("ListAttachedPersonaMemoryEntries: %v", err)
	}
	// Resolved importance DESC (10, 9, 9, 2) then e.id ASC for the 9/9 tie. The
	// overlay row leads even though its own snapshot says 1, and it carries the
	// LIVE memory text because that is what the overlay resolves it to. The
	// 20-importance rows are fenced out by the enabled predicate and the target
	// session.
	want := []string{"boosted live memory", "t1-high", "t1-tie", "t1-low"}
	if got := d1CapsuleEntryTexts(entries); !equalStringSlices(got, want) {
		t.Fatalf("attached entries = %v, want %v", got, want)
	}
	if got := d1CapsuleEntryIDs(entries); !equalInt64Slices(got, []int64{overlaid, high, tie, low}) {
		t.Errorf("entry ids = %v, want the resolved-importance order", got)
	}
	if entries[0].Importance10 != 10 || entries[0].MemoryText != "boosted live memory" ||
		entries[0].EmotionalWeight != 0.7 || entries[0].SourceTurn != 2 {
		t.Errorf("the overlay row did not resolve against the live memory: %+v", entries[0])
	}
	for _, entry := range entries {
		if entry.CapsuleID != enabled {
			t.Errorf("entry from capsule %d leaked into the s2 read", entry.CapsuleID)
		}
	}

	// A positive limit is a row bound applied after the ordering, so what
	// survives is the most important entry rather than an arbitrary prefix.
	limited, err := st.ListAttachedPersonaMemoryEntries(ctx, "s2", 2)
	if err != nil {
		t.Fatalf("limited read: %v", err)
	}
	if got := d1CapsuleEntryTexts(limited); !equalStringSlices(got, want[:2]) {
		t.Errorf("limit 2 = %v, want the two most important entries", got)
	}
	// A negative limit is not a bound either; it must return the whole set rather
	// than an error or a LIMIT that SQLite would reject.
	negative, err := st.ListAttachedPersonaMemoryEntries(ctx, "s2", -1)
	if err != nil {
		t.Fatalf("negative limit: %v", err)
	}
	if got := d1CapsuleEntryTexts(negative); !equalStringSlices(got, want) {
		t.Errorf("limit -1 = %v, want the full set", got)
	}

	// A target with no attachments is an empty result, not an error, and the
	// slice is non-nil so the route encodes [] rather than null.
	none, err := st.ListAttachedPersonaMemoryEntries(ctx, "s9", 0)
	if err != nil {
		t.Fatalf("unknown target: %v", err)
	}
	if none == nil || len(none) != 0 {
		t.Errorf("empty attached read = %#v, want an empty non-nil slice", none)
	}

	// Re-enabling the disabled capsule is the only thing that lets its entry in,
	// and it does so through the upsert rather than a new attachment row.
	if err := st.AttachPersonaMemoryCapsule(ctx, &PersonaCapsuleAttachment{
		CapsuleID: disabled, TargetChatSessionID: "s2", Enabled: true,
	}); err != nil {
		t.Fatalf("re-enable: %v", err)
	}
	reEnabled, err := st.ListAttachedPersonaMemoryEntries(ctx, "s2", 0)
	if err != nil {
		t.Fatalf("read after re-enable: %v", err)
	}
	if len(reEnabled) != len(entries)+1 {
		t.Fatalf("re-enabled read = %d entries, want %d", len(reEnabled), len(entries)+1)
	}
	// Its importance of 20 puts it first, which is the ordering the caller's
	// budget depends on.
	if reEnabled[0].MemoryText != "t1-disabled" {
		t.Errorf("re-enabled entry did not lead the budget: %+v", reEnabled[0])
	}

	// Detaching removes the capsule from the read without touching the rows.
	if err := st.DetachPersonaMemoryCapsule(ctx, disabled, "s2"); err != nil {
		t.Fatalf("detach: %v", err)
	}
	afterDetach, err := st.ListAttachedPersonaMemoryEntries(ctx, "s2", 0)
	if err != nil {
		t.Fatalf("read after detach: %v", err)
	}
	if got := d1CapsuleEntryTexts(afterDetach); !equalStringSlices(got, want) {
		t.Errorf("after detach = %v, want the pre-detach set", got)
	}
}

// ---------------------------------------------------------------------------
// capability advertisement
// ---------------------------------------------------------------------------

// TestD1PersonaCapsuleIsAdvertisedAsCapability checks the consequence the routes
// actually depend on. Every one of these endpoints is discovered by a type
// assertion, so a provider that implemented the methods but was not reachable
// through the manifest would answer "persona_capsule_store_not_enabled" while
// prepare-turn silently injected nothing.
func TestD1PersonaCapsuleIsAdvertisedAsCapability(t *testing.T) {
	st, _ := newD1TestStore(t)
	var asStore Store = st

	if _, ok := asStore.(PersonaCapsuleStore); !ok {
		t.Fatal("D1 store must satisfy PersonaCapsuleStore")
	}
	implemented := map[string]bool{}
	for _, status := range CapabilityReport(asStore) {
		implemented[status.Name] = status.Implemented
	}
	if !implemented["PersonaCapsuleStore"] {
		t.Error("capability manifest reports PersonaCapsuleStore as missing")
	}
}
