package store

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
)

// D1 protagonist entity memory capability tests.
//
// Every test runs against the real SQLite engine through the same D1 transport
// the Worker uses (newD1TestStore applies every tracked D1 migration to a real
// SQLite database), so these assert the statement D1 actually executes rather
// than a mock's idea of it.
//
// This table is owner-private subjective memory. Two ways it can go wrong are
// silent and therefore get one seed row each: a filter that widens returns
// another owner's memory into a prompt, and an identity cascade that resolves in
// the wrong ORDER stores a row under a name the caller can no longer address by
// the key it asked with.

// d1ProtagonistMemorySeed describes one protagonist_entity_memories row.
//
// The four columns that are NOT NULL with a schema default — owner role,
// visibility, portability, and reveal policy — are bound directly, so a blank in
// the seed is stored as an empty string. That is what a row written before those columns
// existed looks like, and it is the input the read-side fallback exists for.
// Binding them as NULL instead would violate the constraint: SQLite applies a
// column default only when the column is omitted, not when it is given NULL.
//
// The genuinely optional text columns (source character, evidence, tags) are
// bound through d1NullableString so a seed can store an actual NULL and pin the
// NULL-to-empty-string read.
type d1ProtagonistMemorySeed struct {
	personaKey      string
	personaName     string
	ownerKey        string
	ownerName       string
	role            string
	visibility      string
	sessionID       string
	sourceChar      string
	sourceTurn      int
	memoryText      string
	evidence        string
	secretGuard     bool
	portability     string
	revealPolicy    string
	tagsJSON        string
	importance10    float64
	emotionalWeight float64
	createdAt       string
	updatedAt       string
}

// d1SeedProtagonistMemoryRow inserts one row and returns its id.
func d1SeedProtagonistMemoryRow(t *testing.T, conn *sqliteD1Conn, seed d1ProtagonistMemorySeed) int64 {
	t.Helper()
	var id int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO protagonist_entity_memories (
			persona_entity_key, persona_entity_name, owner_entity_key, owner_entity_name,
			owner_entity_role, owner_visibility, source_chat_session_id, source_character_name,
			source_turn_index, memory_text, evidence_excerpt, secret_guard, portability, tags_json,
			target_reveal_policy, importance_10, emotional_weight, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		seed.personaKey, seed.personaName, seed.ownerKey, seed.ownerName,
		seed.role, seed.visibility,
		seed.sessionID, d1NullableString(seed.sourceChar),
		seed.sourceTurn, seed.memoryText, d1NullableString(seed.evidence),
		d1BoolValue(seed.secretGuard), seed.portability, d1NullableString(seed.tagsJSON),
		seed.revealPolicy, seed.importance10, seed.emotionalWeight,
		d1TimeValue(mustParseD1TestTime(t, seed.createdAt)), d1TimeValue(mustParseD1TestTime(t, seed.updatedAt)),
	).Scan(&id); err != nil {
		t.Fatalf("seed protagonist memory (session=%q owner=%q/%q): %v",
			seed.sessionID, seed.ownerKey, seed.ownerName, err)
	}
	return id
}

// d1TestProtagonistTimes is the fixed recency ladder the ordering tests walk, so
// two rows written in the same test always have a deterministic order.
var d1TestProtagonistTimes = []string{
	"2026-03-01T00:00:00Z",
	"2026-03-02T00:00:00Z",
	"2026-03-03T00:00:00Z",
	"2026-03-04T00:00:00Z",
}

// d1SeedBaseTime is the fallback recency for a seed that does not care about
// ordering. It is a fixed, sortable value rather than the schema default,
// because a row stamped with the wall clock would make an ordering assertion
// depend on when the test happened to run.
const d1SeedBaseTime = "2026-02-01T00:00:00Z"

// mustParseD1TestTime resolves a seed timestamp, substituting the fixed base
// time for a blank one.
func mustParseD1TestTime(t *testing.T, text string) time.Time {
	t.Helper()
	if strings.TrimSpace(text) == "" {
		text = d1SeedBaseTime
	}
	parsed, err := parseD1Time(text)
	if err != nil {
		t.Fatalf("parse test timestamp %q: %v", text, err)
	}
	return parsed
}

// d1MemoryByID returns the row with the given id from a list result.
func d1MemoryByID(items []ProtagonistEntityMemory, id int64) (ProtagonistEntityMemory, bool) {
	for _, item := range items {
		if item.ID == id {
			return item, true
		}
	}
	return ProtagonistEntityMemory{}, false
}

// d1MemoryTexts returns the memory text of each row, in result order.
func d1MemoryTexts(items []ProtagonistEntityMemory) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.MemoryText)
	}
	return out
}

func d1SortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

// d1RequireMemories runs one list and fails the test on error.
func d1RequireMemories(t *testing.T, st *d1Store, filter ProtagonistEntityMemoryFilter) []ProtagonistEntityMemory {
	t.Helper()
	items, err := st.ListProtagonistEntityMemories(context.Background(), filter)
	if err != nil {
		t.Fatalf("ListProtagonistEntityMemories: %v", err)
	}
	return items
}

// TestD1ProtagonistEntityMemoryCapabilitiesAreAdvertised pins the delivery state
// through the manifest a deployment actually reports, not just the method set.
func TestD1ProtagonistEntityMemoryCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var store Store = st
	for _, capability := range []string{
		"ProtagonistEntityMemoryStore",
		"ProtagonistEntityMemoryRepairStore",
		"ProtagonistEntityMemoryManagementStore",
	} {
		reported := false
		for _, status := range CapabilityReport(st) {
			if status.Name != capability {
				continue
			}
			reported = true
			if !status.Implemented {
				t.Errorf("the capability manifest reports %s as missing", capability)
			}
		}
		if !reported {
			t.Errorf("%s is not probed by the capability manifest", capability)
		}
	}

	// The owner index is the read prepare-turn uses to decide whose memory to
	// look at. It is only consulted inside the ProtagonistEntityMemoryStore
	// branch, so advertising the index without the store would be a read for a
	// writer that cannot write.
	if _, ok := store.(ProtagonistEntityMemoryOwnerIndexStore); !ok {
		t.Error("the D1 provider must keep exposing ProtagonistEntityMemoryOwnerIndexStore")
	}
}

// TestD1CreateProtagonistEntityMemoryIdentityCascade pins every step of the
// two-step identity resolution with a row that no other step could satisfy.
//
// The ordering is the contract: the owner name falls back to the raw persona
// name, not to the already-resolved persona name, and only then to a key. If a
// future change resolves names through keys, "only the persona name given"
// starts storing a key as the display name and the owner index reports it.
func TestD1CreateProtagonistEntityMemoryIdentityCascade(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	// Step 1: owner key falls back to the persona key.
	onlyPersona, err := st.CreateProtagonistEntityMemory(ctx, &ProtagonistEntityMemory{
		PersonaEntityKey: "ent-mira", PersonaEntityName: "Mira",
		SourceChatSessionID: "s1", MemoryText: "owner key from persona key",
	})
	if err != nil {
		t.Fatalf("create with only a persona key: %v", err)
	}
	if onlyPersona.OwnerEntityKey != "ent-mira" {
		t.Errorf("owner key = %q, want the persona key %q", onlyPersona.OwnerEntityKey, "ent-mira")
	}

	// Step 2: persona key falls back to the owner key.
	onlyOwner, err := st.CreateProtagonistEntityMemory(ctx, &ProtagonistEntityMemory{
		OwnerEntityKey: "ent-rook", OwnerEntityName: "Rook",
		SourceChatSessionID: "s1", MemoryText: "persona key from owner key",
	})
	if err != nil {
		t.Fatalf("create with only an owner key: %v", err)
	}
	if onlyOwner.PersonaEntityKey != "ent-rook" {
		t.Errorf("persona key = %q, want the owner key %q", onlyOwner.PersonaEntityKey, "ent-rook")
	}

	// Step 3: each name falls back to the OTHER RAW name. This is the ordering
	// the cascade exists for: personaEntityName is read from the request here,
	// not from the resolved ownerEntityName.
	oneName, err := st.CreateProtagonistEntityMemory(ctx, &ProtagonistEntityMemory{
		PersonaEntityKey: "ent-ilse", PersonaEntityName: "Ilse",
		SourceChatSessionID: "s1", MemoryText: "name from the other raw name",
	})
	if err != nil {
		t.Fatalf("create with one name: %v", err)
	}
	if oneName.PersonaEntityName != "Ilse" || oneName.OwnerEntityName != "Ilse" {
		t.Errorf("names = %q/%q, want both %q", oneName.PersonaEntityName, oneName.OwnerEntityName, "Ilse")
	}

	// Step 4: with no name at all, each falls back to its own key, so the row is
	// still addressable by the only handle the caller has.
	noName, err := st.CreateProtagonistEntityMemory(ctx, &ProtagonistEntityMemory{
		PersonaEntityKey: "ent-bryn", SourceChatSessionID: "s1", MemoryText: "name from key",
	})
	if err != nil {
		t.Fatalf("create with no name: %v", err)
	}
	if noName.PersonaEntityName != "ent-bryn" || noName.OwnerEntityName != "ent-bryn" {
		t.Errorf("names = %q/%q, want both the key %q", noName.PersonaEntityName, noName.OwnerEntityName, "ent-bryn")
	}

	// A whitespace-only value is blank, so it takes the same fallback. Padding
	// is never stored.
	padded, err := st.CreateProtagonistEntityMemory(ctx, &ProtagonistEntityMemory{
		PersonaEntityKey: "  ent-mira  ", PersonaEntityName: "   ", OwnerEntityKey: "\t",
		SourceChatSessionID: "s1", MemoryText: "padded input",
	})
	if err != nil {
		t.Fatalf("create with padded identity: %v", err)
	}
	if padded.PersonaEntityKey != "ent-mira" || padded.PersonaEntityKey != strings.TrimSpace(padded.PersonaEntityKey) {
		t.Errorf("persona key = %q, want the trimmed %q", padded.PersonaEntityKey, "ent-mira")
	}
	if padded.PersonaEntityName != "ent-mira" {
		t.Errorf("persona name = %q, want the key fallback %q", padded.PersonaEntityName, "ent-mira")
	}

	// What the writer returns is what the row holds. If these disagreed, a
	// caller comparing its request against the created record would be told the
	// fallback was stored when it was not.
	items := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"})
	if len(items) != 5 {
		t.Fatalf("stored rows = %d, want 5", len(items))
	}
	byText := map[string]ProtagonistEntityMemory{}
	for _, item := range items {
		byText[item.MemoryText] = item
	}
	for _, want := range []struct {
		text                    string
		personaKey, personaName string
		ownerKey, ownerName     string
	}{
		{"owner key from persona key", "ent-mira", "Mira", "ent-mira", "Mira"},
		{"persona key from owner key", "ent-rook", "Rook", "ent-rook", "Rook"},
		{"name from the other raw name", "ent-ilse", "Ilse", "ent-ilse", "Ilse"},
		{"name from key", "ent-bryn", "ent-bryn", "ent-bryn", "ent-bryn"},
		{"padded input", "ent-mira", "ent-mira", "ent-mira", "ent-mira"},
	} {
		got, ok := byText[want.text]
		if !ok {
			t.Errorf("row %q is missing from the store", want.text)
			continue
		}
		if got.PersonaEntityKey != want.personaKey || got.PersonaEntityName != want.personaName ||
			got.OwnerEntityKey != want.ownerKey || got.OwnerEntityName != want.ownerName {
			t.Errorf("row %q stored as %q/%q owner %q/%q, want %q/%q owner %q/%q",
				want.text, got.PersonaEntityKey, got.PersonaEntityName, got.OwnerEntityKey, got.OwnerEntityName,
				want.personaKey, want.personaName, want.ownerKey, want.ownerName)
		}
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM protagonist_entity_memories`); got != 5 {
		t.Errorf("protagonist_entity_memories rows = %d, want 5", got)
	}
}

// TestD1CreateProtagonistEntityMemoryDefaultsAndRoundTrip pins the four column
// defaults a complete write fills in, and that the returned record is readable
// back with the same values.
//
// The weights, the secret guard, and the turn are the load-bearing ones: they
// are the columns a dialect mistake would silently zero, and a memory that
// silently loses its weight is a memory that stops being selected.
func TestD1CreateProtagonistEntityMemoryDefaultsAndRoundTrip(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	created, err := st.CreateProtagonistEntityMemory(ctx, &ProtagonistEntityMemory{
		PersonaEntityKey: "ent-mira", PersonaEntityName: "Mira",
		SourceChatSessionID: "s1", SourceCharacterName: "Mira", SourceTurn: 7,
		MemoryText: "I still owe Ilse a lantern.", EvidenceExcerpt: "owes a lantern",
		SecretGuard: true, TagsJSON: `["debt"]`,
		Importance10: 7.5, EmotionalWeight: -2.25,
	})
	if err != nil {
		t.Fatalf("CreateProtagonistEntityMemory: %v", err)
	}
	if created.ID == 0 {
		t.Fatal("the created memory must report its stored id")
	}
	if created.CreatedAt.IsZero() || created.UpdatedAt.IsZero() {
		t.Errorf("created/updated = %v/%v, want both set", created.CreatedAt, created.UpdatedAt)
	}
	// Defaults filled in on the way out, so a caller that echoes the response
	// back to the UI sees the values the row will be filtered by.
	if created.OwnerEntityRole != "protagonist" {
		t.Errorf("owner role = %q, want the %q default", created.OwnerEntityRole, "protagonist")
	}
	if created.OwnerVisibility != "player_known" {
		t.Errorf("owner visibility = %q, want the %q default", created.OwnerVisibility, "player_known")
	}
	if created.Portability != "portable_persona_recollection" {
		t.Errorf("portability = %q, want the default", created.Portability)
	}
	if created.TargetRevealPolicy != "requires_explicit_attachment" {
		t.Errorf("target reveal policy = %q, want the default", created.TargetRevealPolicy)
	}

	reread, ok := d1MemoryByID(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"}), created.ID)
	if !ok {
		t.Fatal("the created memory is not readable back")
	}
	if reread.MemoryText != created.MemoryText {
		t.Errorf("memory text = %q, want %q", reread.MemoryText, created.MemoryText)
	}
	if !reread.SecretGuard {
		t.Error("secret guard must round-trip as true; the INTEGER column was read as false")
	}
	if reread.SourceTurn != 7 {
		t.Errorf("source turn = %d, want 7", reread.SourceTurn)
	}
	if reread.SourceCharacterName != "Mira" {
		t.Errorf("source character = %q, want %q", reread.SourceCharacterName, "Mira")
	}
	if reread.EvidenceExcerpt != "owes a lantern" {
		t.Errorf("evidence = %q, want %q", reread.EvidenceExcerpt, "owes a lantern")
	}
	if reread.TagsJSON != `["debt"]` {
		t.Errorf("tags = %q, want %q", reread.TagsJSON, `["debt"]`)
	}
	if math.Abs(reread.Importance10-7.5) > 1e-9 || math.Abs(reread.EmotionalWeight-(-2.25)) > 1e-9 {
		t.Errorf("weights = %v/%v, want 7.5/-2.25", reread.Importance10, reread.EmotionalWeight)
	}
	// The returned timestamps are the ones written, at the stated precision.
	if !reread.CreatedAt.Equal(created.CreatedAt) {
		t.Errorf("created_at = %v, want the written %v", reread.CreatedAt, created.CreatedAt)
	}
}

// TestD1CreateProtagonistEntityMemoryRejectsNil pins the nil-body contract. A
// nil item is a caller error, and reporting it as a disabled store would send
// the route down a "feature not available" path that is factually wrong.
func TestD1CreateProtagonistEntityMemoryRejectsNil(t *testing.T) {
	st, _ := newD1TestStore(t)

	item, err := st.CreateProtagonistEntityMemory(context.Background(), nil)
	if !errors.Is(err, ErrNotEnabled) {
		t.Errorf("nil item error = %v, want ErrNotEnabled", err)
	}
	if item != nil {
		t.Errorf("nil item returned %v, want no record", item)
	}
}

// TestD1ListProtagonistEntityMemoriesOwnerFilterPrecedence pins the three-way
// choice the owner filter makes.
//
// They are alternatives, not conjunctions. Treating OwnerEntityKeys as merely
// another predicate would let a stale multi-key list override the single key the
// caller actually meant, returning another owner's memory.
func TestD1ListProtagonistEntityMemoriesOwnerFilterPrecedence(t *testing.T) {
	st, conn := newD1TestStore(t)

	// Two memories for Mira, one of which has no owner column of its own: the
	// resolved-key form must select it either way.
	miraOwned := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		sessionID: "s1", memoryText: "mira owned", updatedAt: d1TestProtagonistTimes[1],
	})
	miraUnowned := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira",
		sessionID: "s1", memoryText: "mira unowned", updatedAt: d1TestProtagonistTimes[0],
	})
	rookID := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-rook", personaName: "Rook", ownerKey: "ent-rook", ownerName: "Rook",
		sessionID: "s1", memoryText: "rook", updatedAt: d1TestProtagonistTimes[2],
	})
	_ = rookID

	all := d1SortedStrings(d1MemoryTexts(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"})))
	if len(all) != 3 {
		t.Fatalf("unfiltered rows = %v, want 3", all)
	}

	// OwnerEntityKeys wins over OwnerEntityKey, and the list is deduplicated and
	// stripped of blanks. A blank entry must not become a filter that matches
	// nothing, which would return an empty result and read as "no memories".
	byKeys := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityKey: "ent-rook", OwnerEntityKeys: []string{"ent-mira", " ent-mira ", "", "   "},
		SourceChatSessionID: "s1",
	})
	gotKeys := d1SortedStrings(d1MemoryTexts(byKeys))
	if len(gotKeys) != 2 {
		t.Fatalf("OwnerEntityKeys rows = %v, want exactly Mira's two", gotKeys)
	}
	for _, id := range []int64{miraOwned, miraUnowned} {
		if _, ok := d1MemoryByID(byKeys, id); !ok {
			t.Errorf("row %d is missing from the resolved-key filter result", id)
		}
	}

	// A single OwnerEntityKey also matches through the RESOLVED key, so it
	// returns the unowned row too. This is the disjunction the MariaDB
	// statement spells out, reproduced rather than approximated with `=`.
	byOwnerKey := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityKey: "ent-mira", SourceChatSessionID: "s1",
	})
	if len(byOwnerKey) != 2 {
		t.Errorf("single OwnerEntityKey rows = %v, want Mira's two", d1MemoryTexts(byOwnerKey))
	}

	// PersonaEntityKey is the last branch and admits the row through either
	// column.
	byPersonaKey := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		PersonaEntityKey: "ent-mira", SourceChatSessionID: "s1",
	})
	if len(byPersonaKey) != 2 {
		t.Errorf("PersonaEntityKey rows = %v, want Mira's two", d1MemoryTexts(byPersonaKey))
	}

	// A key nobody owns selects nothing, and an empty result is a non-nil empty
	// slice so the response encodes identically on both providers.
	missing := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityKey: "ent-nobody", SourceChatSessionID: "s1",
	})
	if missing == nil {
		t.Error("an empty result must be a non-nil empty slice")
	}
	if len(missing) != 0 {
		t.Errorf("unknown owner rows = %v, want none", d1MemoryTexts(missing))
	}
}

// TestD1ListProtagonistEntityMemoriesScopeFilters pins the conjunctive filters
// and the rule that a whitespace-only value applies no filter at all.
//
// A blank filter applying an empty-string equality instead of nothing is the
// failure that matters:
// the caller would get a confidently empty list for a session full of memories.
func TestD1ListProtagonistEntityMemoriesScopeFilters(t *testing.T) {
	st, conn := newD1TestStore(t)

	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "private npc", updatedAt: d1TestProtagonistTimes[0],
	})
	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "player_known", sessionID: "s1",
		memoryText: "public npc", updatedAt: d1TestProtagonistTimes[1],
	})
	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "player", visibility: "player_known", sessionID: "s1",
		memoryText: "player", updatedAt: d1TestProtagonistTimes[2],
	})
	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "player_known", sessionID: "s2",
		memoryText: "other session", updatedAt: d1TestProtagonistTimes[3],
	})

	// A blank filter value applies NO predicate.
	blank := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "   ", OwnerVisibility: "\t", SourceChatSessionID: "  ",
	})
	if len(blank) != 4 {
		t.Errorf("whitespace-only filters returned %d rows, want all 4", len(blank))
	}

	scoped := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
	})
	if got := d1MemoryTexts(scoped); len(got) != 1 || got[0] != "private npc" {
		t.Errorf("fully scoped rows = %v, want only the private npc memory", got)
	}

	// The session filter alone is the prepare-turn scoping read: it must not
	// reach into another session.
	sessionOnly := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s2"})
	if got := d1MemoryTexts(sessionOnly); len(got) != 1 || got[0] != "other session" {
		t.Errorf("session-scoped rows = %v, want only the other session's memory", got)
	}
}

// TestD1ListProtagonistEntityMemoriesOrderingAndLimit pins the single ORDER BY
// and that a positive Limit truncates the MOST RECENT rows.
//
// A limit applied before the ordering would return an arbitrary prefix, so the
// caller would see a plausible-looking but wrong slice of the owner's memory.
func TestD1ListProtagonistEntityMemoriesOrderingAndLimit(t *testing.T) {
	st, conn := newD1TestStore(t)

	for i, text := range []string{"oldest", "middle", "newest"} {
		d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
			personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
			sessionID: "s1", memoryText: text, updatedAt: d1TestProtagonistTimes[i],
		})
	}

	ordered := d1MemoryTexts(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"}))
	if len(ordered) != 3 || ordered[0] != "newest" || ordered[1] != "middle" || ordered[2] != "oldest" {
		t.Fatalf("ordered rows = %v, want newest, middle, oldest", ordered)
	}

	limited := d1MemoryTexts(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		SourceChatSessionID: "s1", Limit: 2,
	}))
	if len(limited) != 2 || limited[0] != "newest" || limited[1] != "middle" {
		t.Errorf("limited rows = %v, want the two most recent", limited)
	}

	// A non-positive limit means "every matching row", not "no rows".
	all := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1", Limit: 0})
	if len(all) != 3 {
		t.Errorf("zero limit returned %d rows, want all 3", len(all))
	}
	negative := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1", Limit: -5})
	if len(negative) != 3 {
		t.Errorf("negative limit returned %d rows, want all 3", len(negative))
	}
}

// TestD1ListProtagonistEntityMemoriesOrderingTieBreak pins the id tiebreak.
//
// Two memories can share an updated_at when one turn writes both, and without a
// stable second key their order is whatever the index happens to produce — which
// would make prepare-turn's memory block differ between identical runs.
func TestD1ListProtagonistEntityMemoriesOrderingTieBreak(t *testing.T) {
	st, conn := newD1TestStore(t)

	for _, text := range []string{"tied first", "tied second", "tied third"} {
		d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
			personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
			sessionID: "s1", memoryText: text, updatedAt: d1TestProtagonistTimes[0],
		})
	}

	got := d1MemoryTexts(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"}))
	want := []string{"tied third", "tied second", "tied first"}
	for i := range want {
		if i >= len(got) || got[i] != want[i] {
			t.Fatalf("tied rows = %v, want %v (descending id)", got, want)
		}
	}
}

// TestD1ListProtagonistEntityMemoriesScanFallbacks pins the read-side defaults
// for a row whose optional columns are NULL or blank.
//
// The row is written with the identity columns blank and every optional column
// NULL, which is how data written before those columns existed reads. The
// fallbacks are what keep such a memory addressable: without them the owner key
// would come back empty and the caller would have no way to scope to it.
func TestD1ListProtagonistEntityMemoriesScanFallbacks(t *testing.T) {
	st, conn := newD1TestStore(t)

	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "", ownerName: "   ",
		sessionID: "s1", memoryText: "legacy row", updatedAt: d1TestProtagonistTimes[0],
	})

	items := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"})
	if len(items) != 1 {
		t.Fatalf("rows = %d, want 1", len(items))
	}
	got := items[0]
	if got.OwnerEntityKey != "ent-mira" {
		t.Errorf("owner key = %q, want the persona key fallback %q", got.OwnerEntityKey, "ent-mira")
	}
	if got.OwnerEntityName != "Mira" {
		t.Errorf("owner name = %q, want the persona name fallback %q", got.OwnerEntityName, "Mira")
	}
	// Role, visibility, and reveal policy fall back to their write defaults.
	if got.OwnerEntityRole != "protagonist" {
		t.Errorf("owner role = %q, want the default %q", got.OwnerEntityRole, "protagonist")
	}
	if got.OwnerVisibility != "player_known" {
		t.Errorf("owner visibility = %q, want the default %q", got.OwnerVisibility, "player_known")
	}
	if got.TargetRevealPolicy != "requires_explicit_attachment" {
		t.Errorf("reveal policy = %q, want the default", got.TargetRevealPolicy)
	}
	// NULL text is the empty string, NULL numbers are zero: the same
	// dereferences the reference scan performs.
	if got.SourceCharacterName != "" || got.EvidenceExcerpt != "" || got.TagsJSON != "" {
		t.Errorf("optional text = %q/%q/%q, want all empty",
			got.SourceCharacterName, got.EvidenceExcerpt, got.TagsJSON)
	}
	if got.SourceTurn != 0 || got.Importance10 != 0 || got.EmotionalWeight != 0 {
		t.Errorf("optional numbers = %d/%v/%v, want zeros", got.SourceTurn, got.Importance10, got.EmotionalWeight)
	}
	if got.SecretGuard {
		t.Error("secret guard must default to false")
	}
}

// TestD1ListProtagonistEntityMemoriesOwnerLookupAgreesWithOwnerIndex is the
// cross-capability contract.
//
// prepare-turn resolves an owner through the owner index and then reads that
// owner's memories. If the two disagreed about which key a row belongs to — for
// instance because one used the resolved key and the other a raw column — the
// prompt would receive a memory for an owner it never asked about, or none at
// all. The rows below are exactly the shapes that make the two disagree.
func TestD1ListProtagonistEntityMemoriesOwnerLookupAgreesWithOwnerIndex(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	// Owned, unowned, blank-owned, and another session's row.
	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "owned", updatedAt: d1TestProtagonistTimes[0],
	})
	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "unowned", updatedAt: d1TestProtagonistTimes[1],
	})
	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "  ",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "blank owned", updatedAt: d1TestProtagonistTimes[2],
	})
	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-rook", personaName: "Rook", ownerKey: "ent-rook", ownerName: "Rook",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "other owner", updatedAt: d1TestProtagonistTimes[3],
	})
	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s2",
		memoryText: "other session", updatedAt: d1TestProtagonistTimes[0],
	})

	owners, err := st.ListProtagonistEntityMemoryOwners(ctx, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
	})
	if err != nil {
		t.Fatalf("ListProtagonistEntityMemoryOwners: %v", err)
	}
	if len(owners) != 2 {
		t.Fatalf("owners = %v, want Mira and Rook", owners)
	}

	// Everything the index names must be readable back by that same key.
	for _, owner := range owners {
		items := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
			OwnerEntityKeys:     []string{owner.OwnerEntityKey},
			OwnerEntityRole:     "npc",
			OwnerVisibility:     "owner_private",
			SourceChatSessionID: "s1",
		})
		if len(items) == 0 {
			t.Errorf("owner %q is indexed but its memory read returned nothing", owner.OwnerEntityKey)
		}
		for _, item := range items {
			resolved := item.OwnerEntityKey
			if resolved == "" {
				resolved = item.PersonaEntityKey
			}
			if resolved != owner.OwnerEntityKey {
				t.Errorf("index listed owner %q but the memory read returned a row owned by %q",
					owner.OwnerEntityKey, resolved)
			}
		}
	}

	// Mira's three same-session rows all come back under her one key, and the
	// other session's row never does.
	miraRows := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityKeys:     []string{"ent-mira"},
		OwnerEntityRole:     "npc",
		OwnerVisibility:     "owner_private",
		SourceChatSessionID: "s1",
	})
	if got := d1SortedStrings(d1MemoryTexts(miraRows)); len(got) != 3 {
		t.Errorf("Mira's rows = %v, want owned, unowned, and blank-owned", got)
	}
}

// TestD1UpdateProtagonistEntityMemoryOwnerPreservesOmittedPolicy pins the
// conditional write in the repair path.
//
// This is the reason repair is a separate method from management: an alias
// canonicalization carries no visibility, and a repair that wrote the default
// would silently republish every "owner_private" memory it touched. The guard is
// in the statement, so the test proves the stored row is untouched.
func TestD1UpdateProtagonistEntityMemoryOwnerPreservesOmittedPolicy(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	id := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira-alias", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "I remember the bridge.", evidence: "the bridge",
		portability: "persona_private", revealPolicy: "never_without_ask",
		importance10: 8, updatedAt: d1TestProtagonistTimes[0],
	})

	// A repair that names only the owner identity: the row is renamed and
	// everything else survives.
	if err := st.UpdateProtagonistEntityMemoryOwner(ctx, ProtagonistEntityMemoryOwnerUpdate{
		ID: id, OwnerEntityKey: "ent-mira", OwnerEntityName: "Mira",
	}); err != nil {
		t.Fatalf("UpdateProtagonistEntityMemoryOwner: %v", err)
	}

	items := d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"})
	got, ok := d1MemoryByID(items, id)
	if !ok {
		t.Fatal("the repaired memory disappeared")
	}
	if got.OwnerEntityKey != "ent-mira" || got.OwnerEntityName != "Mira" {
		t.Errorf("owner = %q/%q, want the canonical %q/%q", got.OwnerEntityKey, got.OwnerEntityName, "ent-mira", "Mira")
	}
	// The persona key was omitted, so it follows the owner key.
	if got.PersonaEntityKey != "ent-mira" {
		t.Errorf("persona key = %q, want it to follow the owner key %q", got.PersonaEntityKey, "ent-mira")
	}
	// Omitted policy, role, and visibility are preserved, not defaulted.
	if got.OwnerVisibility != "owner_private" {
		t.Errorf("owner visibility = %q, want the preserved %q", got.OwnerVisibility, "owner_private")
	}
	if got.OwnerEntityRole != "npc" {
		t.Errorf("owner role = %q, want the preserved %q", got.OwnerEntityRole, "npc")
	}
	if got.Portability != "persona_private" {
		t.Errorf("portability = %q, want the untouched %q", got.Portability, "persona_private")
	}
	if got.TargetRevealPolicy != "never_without_ask" {
		t.Errorf("reveal policy = %q, want the untouched %q", got.TargetRevealPolicy, "never_without_ask")
	}
	// Repair cannot rewrite the memory itself. If it could, a bulk alias fix
	// would be able to rewrite the user's own words.
	if got.MemoryText != "I remember the bridge." {
		t.Errorf("memory text = %q, want it unchanged", got.MemoryText)
	}
	if got.EvidenceExcerpt != "the bridge" {
		t.Errorf("evidence = %q, want it unchanged", got.EvidenceExcerpt)
	}
	if math.Abs(got.Importance10-8) > 1e-9 {
		t.Errorf("importance = %v, want it unchanged", got.Importance10)
	}
}

// TestD1UpdateProtagonistEntityMemoryOwnerAppliesSuppliedPolicy pins the other
// half of the guard: a repair that DOES carry a role and visibility writes them.
func TestD1UpdateProtagonistEntityMemoryOwnerAppliesSuppliedPolicy(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	id := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "policy change", tagsJSON: "[]", updatedAt: d1TestProtagonistTimes[0],
	})

	if err := st.UpdateProtagonistEntityMemoryOwner(ctx, ProtagonistEntityMemoryOwnerUpdate{
		ID: id, OwnerEntityKey: "ent-mira", OwnerEntityName: "Mira",
		OwnerEntityRole: "player", OwnerVisibility: "player_known", TagsJSON: `["repaired"]`,
	}); err != nil {
		t.Fatalf("UpdateProtagonistEntityMemoryOwner: %v", err)
	}

	got, ok := d1MemoryByID(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"}), id)
	if !ok {
		t.Fatal("the repaired memory disappeared")
	}
	if got.OwnerEntityRole != "player" {
		t.Errorf("owner role = %q, want the supplied %q", got.OwnerEntityRole, "player")
	}
	if got.OwnerVisibility != "player_known" {
		t.Errorf("owner visibility = %q, want the supplied %q", got.OwnerVisibility, "player_known")
	}
	if got.TagsJSON != `["repaired"]` {
		t.Errorf("tags = %q, want the supplied %q", got.TagsJSON, `["repaired"]`)
	}
}

// TestD1UpdateProtagonistEntityMemoryOwnerRejectsUnknownRow pins the not-found
// contract. A repair over a vanished row must say so; silently succeeding would
// make a dry-run plan report rows it did not actually change.
func TestD1UpdateProtagonistEntityMemoryOwnerRejectsUnknownRow(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	for _, id := range []int64{0, -1, 987654} {
		err := st.UpdateProtagonistEntityMemoryOwner(ctx, ProtagonistEntityMemoryOwnerUpdate{ID: id})
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("id %d: error = %v, want ErrNotFound", id, err)
		}
	}
}

// TestD1UpdateProtagonistEntityMemoryAppliesSuppliedFields pins the management
// edit, which unlike repair DOES take a complete row and therefore defaults the
// columns it was not given.
func TestD1UpdateProtagonistEntityMemoryAppliesSuppliedFields(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	id := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "before", evidence: "old evidence", sourceChar: "Mira", sourceTurn: 3,
		secretGuard: false, importance10: 1, emotionalWeight: 1,
		portability: "portable_persona_recollection", revealPolicy: "requires_explicit_attachment",
		updatedAt: d1TestProtagonistTimes[0],
	})

	if err := st.UpdateProtagonistEntityMemory(ctx, ProtagonistEntityMemoryUpdate{
		ID: id, PersonaEntityKey: "ent-mira", PersonaEntityName: "Mira",
		OwnerEntityKey: "ent-mira", OwnerEntityName: "Mira",
		SourceCharacterName: "Ilse", MemoryText: "after", EvidenceExcerpt: "new evidence",
		SecretGuard: true, TagsJSON: `["edited"]`, Importance10: 9.5, EmotionalWeight: -3,
		// Role, visibility, portability, and reveal policy are omitted, so a
		// complete write fills them with their defaults rather than keeping the
		// stored values. That is the difference from repair.
	}); err != nil {
		t.Fatalf("UpdateProtagonistEntityMemory: %v", err)
	}

	got, ok := d1MemoryByID(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"}), id)
	if !ok {
		t.Fatal("the edited memory disappeared")
	}
	if got.MemoryText != "after" || got.EvidenceExcerpt != "new evidence" {
		t.Errorf("text/evidence = %q/%q, want the edited values", got.MemoryText, got.EvidenceExcerpt)
	}
	if got.SourceCharacterName != "Ilse" {
		t.Errorf("source character = %q, want %q", got.SourceCharacterName, "Ilse")
	}
	if !got.SecretGuard {
		t.Error("secret guard must be settable through the management edit")
	}
	if got.TagsJSON != `["edited"]` {
		t.Errorf("tags = %q, want %q", got.TagsJSON, `["edited"]`)
	}
	if math.Abs(got.Importance10-9.5) > 1e-9 || math.Abs(got.EmotionalWeight-(-3)) > 1e-9 {
		t.Errorf("weights = %v/%v, want 9.5/-3", got.Importance10, got.EmotionalWeight)
	}
	// source_turn_index is not editable through this contract and is therefore
	// left alone, so an edit cannot detach a memory from the turn that produced
	// it.
	if got.SourceTurn != 3 {
		t.Errorf("source turn = %d, want it unchanged at 3", got.SourceTurn)
	}
	// The source session is likewise not editable: a memory cannot be moved
	// between sessions by editing it.
	if got.SourceChatSessionID != "s1" {
		t.Errorf("source session = %q, want it unchanged at %q", got.SourceChatSessionID, "s1")
	}
	if got.OwnerEntityRole != "protagonist" || got.OwnerVisibility != "player_known" {
		t.Errorf("role/visibility = %q/%q, want the write defaults",
			got.OwnerEntityRole, got.OwnerVisibility)
	}
	if got.Portability != "portable_persona_recollection" ||
		got.TargetRevealPolicy != "requires_explicit_attachment" {
		t.Errorf("portability/reveal = %q/%q, want the write defaults",
			got.Portability, got.TargetRevealPolicy)
	}
}

// TestD1UpdateProtagonistEntityMemoryRejectsUnknownRow pins the same not-found
// contract on the management path.
func TestD1UpdateProtagonistEntityMemoryRejectsUnknownRow(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	for _, id := range []int64{0, -1, 987654} {
		err := st.UpdateProtagonistEntityMemory(ctx, ProtagonistEntityMemoryUpdate{ID: id, MemoryText: "x"})
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("id %d: error = %v, want ErrNotFound", id, err)
		}
	}
}

// TestD1UpdateProtagonistEntityMemoryNoOpSucceeds pins the one deliberate
// divergence from the reference provider, in the direction that keeps the
// observable result correct.
//
// No DSN in this repository sets clientFoundRows, so MariaDB reports zero
// affected rows for an UPDATE that writes the values a row already holds, and
// the reference path turns that into ErrNotFound for a row that exists.
// SQLite counts every row the UPDATE processed, so the same call reports
// success here. This test exists so the difference is a recorded decision rather
// than a surprise discovered from a 404.
func TestD1UpdateProtagonistEntityMemoryNoOpSucceeds(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	id := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "unchanged", updatedAt: d1TestProtagonistTimes[0],
	})

	// The exact values the row already holds, so nothing changes.
	unchanged := ProtagonistEntityMemoryUpdate{
		ID: id, PersonaEntityKey: "ent-mira", PersonaEntityName: "Mira",
		OwnerEntityKey: "ent-mira", OwnerEntityName: "Mira",
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private",
		SourceCharacterName: "", MemoryText: "unchanged", EvidenceExcerpt: "",
		SecretGuard: false, Portability: "portable_persona_recollection",
		TagsJSON: "", TargetRevealPolicy: "requires_explicit_attachment",
		Importance10: 0, EmotionalWeight: 0,
	}
	if err := st.UpdateProtagonistEntityMemory(ctx, unchanged); err != nil {
		t.Fatalf("rewriting a row with its own values: %v", err)
	}
	if _, ok := d1MemoryByID(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		SourceChatSessionID: "s1"}), id); !ok {
		t.Error("the row must still exist after a no-op edit")
	}

	// The repair path behaves the same way, and the row is still reported as
	// present rather than missing.
	if err := st.UpdateProtagonistEntityMemoryOwner(ctx, ProtagonistEntityMemoryOwnerUpdate{
		ID: id, OwnerEntityKey: "ent-mira", OwnerEntityName: "Mira",
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private",
	}); err != nil {
		t.Fatalf("repairing a row to its own identity: %v", err)
	}
}

// TestD1DeleteProtagonistEntityMemory pins the delete, including that a second
// delete of the same id is reported rather than silently accepted.
func TestD1DeleteProtagonistEntityMemory(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	keep := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		sessionID: "s1", memoryText: "keep", updatedAt: d1TestProtagonistTimes[0],
	})
	remove := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		sessionID: "s1", memoryText: "remove", updatedAt: d1TestProtagonistTimes[1],
	})

	if err := st.DeleteProtagonistEntityMemory(ctx, remove); err != nil {
		t.Fatalf("DeleteProtagonistEntityMemory: %v", err)
	}
	remaining := d1MemoryTexts(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"}))
	if len(remaining) != 1 || remaining[0] != "keep" {
		t.Fatalf("rows after delete = %v, want only the kept memory", remaining)
	}

	// A second delete must report the row as already gone rather than succeed.
	if err := st.DeleteProtagonistEntityMemory(ctx, remove); !errors.Is(err, ErrNotFound) {
		t.Errorf("second delete = %v, want ErrNotFound", err)
	}
	// A non-positive id is a missing row, not an id to scan for.
	for _, id := range []int64{0, -1} {
		if err := st.DeleteProtagonistEntityMemory(ctx, id); !errors.Is(err, ErrNotFound) {
			t.Errorf("delete id %d = %v, want ErrNotFound", id, err)
		}
	}
	if _, ok := d1MemoryByID(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		SourceChatSessionID: "s1"}), keep); !ok {
		t.Error("the delete must not touch any row but the addressed one")
	}
}

// TestD1DeleteProtagonistEntityMemoryReleasesTheOwnerIndex pins that a deleted
// memory stops being offered to prepare-turn.
//
// A delete that left the row in the owner index would keep an owner visible for
// a memory that no longer exists, which is a private-memory retention bug
// disguised as a stale cache.
func TestD1DeleteProtagonistEntityMemoryReleasesTheOwnerIndex(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	other := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-rook", personaName: "Rook", ownerKey: "ent-rook", ownerName: "Rook",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "rook", updatedAt: d1TestProtagonistTimes[0],
	})
	mira := d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "Mira", ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		memoryText: "mira", updatedAt: d1TestProtagonistTimes[1],
	})

	owners, err := st.ListProtagonistEntityMemoryOwners(ctx, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
	})
	if err != nil {
		t.Fatalf("ListProtagonistEntityMemoryOwners: %v", err)
	}
	if len(owners) != 2 {
		t.Fatalf("owners before delete = %v, want two", owners)
	}

	if err := st.DeleteProtagonistEntityMemory(ctx, mira); err != nil {
		t.Fatalf("DeleteProtagonistEntityMemory: %v", err)
	}

	owners, err = st.ListProtagonistEntityMemoryOwners(ctx, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
	})
	if err != nil {
		t.Fatalf("ListProtagonistEntityMemoryOwners after delete: %v", err)
	}
	if len(owners) != 1 || owners[0].OwnerEntityKey != "ent-rook" {
		t.Errorf("owners after delete = %v, want only Rook", owners)
	}
	if _, ok := d1MemoryByID(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityKeys:     []string{"ent-mira"},
		SourceChatSessionID: "s1",
	}), other); ok {
		t.Error("Rook's memory must not be reachable through Mira's deleted key")
	}
}

// TestD1ProtagonistEntityMemoryTextComparisonIsCaseSensitive pins the other
// deliberate divergence.
//
// The MariaDB columns are utf8mb4_unicode_ci, so "Mira" and "mira" are the same
// owner there. SQLite's TEXT equality is binary, so they are two owners here.
// D1 is the stricter of the two: it never merges two entities the user has
// written differently, at the cost of not matching a differently-cased spelling
// of the same one. Every other D1 reader of these columns behaves identically,
// and making only this one case-insensitive would make the owner index and the
// memory read it feeds disagree.
func TestD1ProtagonistEntityMemoryTextComparisonIsCaseSensitive(t *testing.T) {
	st, conn := newD1TestStore(t)

	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-Mira", personaName: "Mira", ownerKey: "ent-Mira", ownerName: "Mira",
		sessionID: "s1", memoryText: "capitalised", updatedAt: d1TestProtagonistTimes[0],
	})
	d1SeedProtagonistMemoryRow(t, conn, d1ProtagonistMemorySeed{
		personaKey: "ent-mira", personaName: "mira", ownerKey: "ent-mira", ownerName: "mira",
		sessionID: "s1", memoryText: "lowercase", updatedAt: d1TestProtagonistTimes[1],
	})

	exact := d1MemoryTexts(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityKey: "ent-mira", SourceChatSessionID: "s1",
	}))
	if len(exact) != 1 || exact[0] != "lowercase" {
		t.Errorf("exact-case lookup = %v, want only the lowercase row", exact)
	}

	// Both rows are still returned when no owner is named, so nothing is lost:
	// only an exact-spelling lookup is stricter.
	all := d1MemoryTexts(d1RequireMemories(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"}))
	if len(all) != 2 {
		t.Errorf("unscoped rows = %v, want both spellings kept separate", all)
	}
}
