package store

import (
	"context"
	"sort"
	"testing"
)

// D1 protagonist entity memory owner index tests.
//
// Every test runs against the real SQLite engine through the same D1 transport
// the Worker uses (newD1TestStore applies every tracked D1 migration to a real
// SQLite database), so these assert the statement D1 actually executes rather
// than a mock's idea of it.
//
// The seeds are chosen so that each way the index can go wrong is a distinct
// row, because this read is what decides whose memory prepare-turn is willing to
// look at: a filter that silently widens leaks another session's owner, and a
// projection that silently drops a fallback loses an owner the caller can no
// longer address by key.

// d1ProtagonistOwnerSeed describes one protagonist_entity_memories row. An empty
// role or visibility is stored as NULL so the schema default ('protagonist' and
// 'player_known') applies, which is how a row that predates those columns reads.
type d1ProtagonistOwnerSeed struct {
	personaKey  string
	personaName string
	ownerKey    string
	ownerName   string
	role        string
	visibility  string
	sessionID   string
	updatedAt   string
}

// d1SeedProtagonistOwnerRow inserts one row and returns its id.
//
// Only the identity and scoping columns are written. The rest keep their schema
// defaults, because the owner index never reads memory_text, evidence, tags, or
// the numeric weights: a row that could be separated by a column this read does
// not project would be a seed testing the wrong thing.
func d1SeedProtagonistOwnerRow(t *testing.T, conn *sqliteD1Conn, seed d1ProtagonistOwnerSeed) int64 {
	t.Helper()
	var id int64
	if err := conn.QueryRow(context.Background(), `
		INSERT INTO protagonist_entity_memories (
			persona_entity_key, persona_entity_name,
			owner_entity_key, owner_entity_name, owner_entity_role, owner_visibility,
			source_chat_session_id, memory_text, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, 'seeded', ?)
		RETURNING id`,
		seed.personaKey, seed.personaName, seed.ownerKey, seed.ownerName,
		d1NullableString(seed.role), d1NullableString(seed.visibility),
		seed.sessionID, seed.updatedAt).Scan(&id); err != nil {
		t.Fatalf("seed protagonist memory (session=%q owner=%q/%q): %v",
			seed.sessionID, seed.ownerKey, seed.ownerName, err)
	}
	return id
}

// d1OwnerIndexKeys returns the resolved owner keys of a result, in result order.
func d1OwnerIndexKeys(owners []ProtagonistEntityMemoryOwner) []string {
	out := make([]string, 0, len(owners))
	for _, owner := range owners {
		out = append(out, owner.OwnerEntityKey)
	}
	return out
}

// d1SortedOwnerIndexKeys returns the same list sorted, so a test that asserts a
// SET does not also assert an order it does not mean to pin.
func d1SortedOwnerIndexKeys(owners []ProtagonistEntityMemoryOwner) []string {
	out := d1OwnerIndexKeys(owners)
	sort.Strings(out)
	return out
}

// d1RequireOwnerIndex runs one owner-index read and fails the test on error.
func d1RequireOwnerIndex(t *testing.T, st *d1Store, filter ProtagonistEntityMemoryFilter) []ProtagonistEntityMemoryOwner {
	t.Helper()
	owners, err := st.ListProtagonistEntityMemoryOwners(context.Background(), filter)
	if err != nil {
		t.Fatalf("ListProtagonistEntityMemoryOwners: %v", err)
	}
	return owners
}

// d1RequireOwnerKeys compares a result against the expected keys, preserving order.
func d1RequireOwnerKeys(t *testing.T, label string, owners []ProtagonistEntityMemoryOwner, want ...string) {
	t.Helper()
	got := d1OwnerIndexKeys(owners)
	if len(got) != len(want) {
		t.Fatalf("%s: owners = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: owners = %v, want %v", label, got, want)
		}
	}
}

// d1OwnerByKey returns the indexed owner with the given key, or a zero value.
func d1OwnerByKey(owners []ProtagonistEntityMemoryOwner, key string) ProtagonistEntityMemoryOwner {
	for _, owner := range owners {
		if owner.OwnerEntityKey == key {
			return owner
		}
	}
	return ProtagonistEntityMemoryOwner{}
}

// TestD1ProtagonistEntityMemoryOwnerIndexIsAdvertised pins the delivery state
// itself. The capability manifest is what a deployment reports, so a capability
// that is implemented but invisible there would still look like a parity gap.
func TestD1ProtagonistEntityMemoryOwnerIndexIsAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var store Store = st
	if _, ok := store.(ProtagonistEntityMemoryOwnerIndexStore); !ok {
		t.Fatal("the D1 provider must expose ProtagonistEntityMemoryOwnerIndexStore")
	}

	reported := false
	for _, status := range CapabilityReport(st) {
		if status.Name != "ProtagonistEntityMemoryOwnerIndexStore" {
			continue
		}
		reported = true
		if !status.Implemented {
			t.Error("the capability manifest reports the owner index as missing")
		}
	}
	if !reported {
		t.Fatal("ProtagonistEntityMemoryOwnerIndexStore is not probed by the capability manifest")
	}

	// Scope guard: this slice ports the index only. The write and edit lanes
	// over the same table are still absent, and advertising them would let a
	// route believe protagonist memory can be persisted on D1.
	if _, ok := store.(ProtagonistEntityMemoryStore); ok {
		t.Error("ProtagonistEntityMemoryStore is out of scope and must not be advertised yet")
	}
	if _, ok := store.(ProtagonistEntityMemoryRepairStore); ok {
		t.Error("ProtagonistEntityMemoryRepairStore is out of scope and must not be advertised yet")
	}
	if _, ok := store.(ProtagonistEntityMemoryManagementStore); ok {
		t.Error("ProtagonistEntityMemoryManagementStore is out of scope and must not be advertised yet")
	}
}

// TestD1ListProtagonistEntityMemoryOwnerIndexFilters pins the three predicates
// that narrow this read, and the one that does not exist.
func TestD1ListProtagonistEntityMemoryOwnerIndexFilters(t *testing.T) {
	st, conn := newD1TestStore(t)

	// One row per exclusion cause, plus a second row for the owner that must
	// survive so the caller sees a group rather than a single row.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-mira", personaName: "Mira",
		ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-03-01T00:00:00.000Z",
	})
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-mira", personaName: "Mira",
		ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-03-02T00:00:00.000Z",
	})
	// Excluded by visibility.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-rook", personaName: "Rook",
		ownerKey: "ent-rook", ownerName: "Rook",
		role: "npc", visibility: "player_known", sessionID: "s1",
		updatedAt: "2026-03-03T00:00:00.000Z",
	})
	// Excluded by role.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-ilse", personaName: "Ilse",
		ownerKey: "ent-ilse", ownerName: "Ilse",
		role: "player", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-03-04T00:00:00.000Z",
	})
	// Excluded by session.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-wren", personaName: "Wren",
		ownerKey: "ent-wren", ownerName: "Wren",
		role: "npc", visibility: "owner_private", sessionID: "s2",
		updatedAt: "2026-03-05T00:00:00.000Z",
	})

	// The exact filter prepare-turn sends for the character-recollection lane.
	d1RequireOwnerKeys(t, "prepare-turn filter",
		d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
			OwnerEntityRole:     "npc",
			OwnerVisibility:     "owner_private",
			SourceChatSessionID: "s1",
		}), "ent-mira")

	// Each predicate on its own, so a failure names the predicate that widened.
	// Only one predicate is active per call, so every row the other two would
	// have excluded is still admitted here. The expected lists are in recency
	// order, which also shows the groups were recomputed for each filter.
	d1RequireOwnerKeys(t, "role only",
		d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{OwnerEntityRole: "npc"}),
		"ent-wren", "ent-rook", "ent-mira")
	d1RequireOwnerKeys(t, "visibility only",
		d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{OwnerVisibility: "owner_private"}),
		"ent-wren", "ent-ilse", "ent-mira")
	d1RequireOwnerKeys(t, "session only",
		d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "s1"}),
		"ent-ilse", "ent-rook", "ent-mira")

	// An empty filter admits every session, which is the default the index uses
	// when the caller has no scope to offer.
	all := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{})
	if got, want := len(all), 4; got != want {
		t.Fatalf("unfiltered owners = %d, want %d (%v)", got, want, d1SortedOwnerIndexKeys(all))
	}

	// A whitespace-only value applies NO filter, not an impossible one. Pinning
	// this is the difference between "no scope offered" and "scope that matches
	// nothing", which would silently report an empty owner index.
	blank := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityRole:     "   ",
		OwnerVisibility:     "\t\n",
		SourceChatSessionID: "  ",
	})
	if got, want := len(blank), 4; got != want {
		t.Errorf("whitespace-only filter returned %d owners, want %d (a blank scope must not filter)",
			got, want)
	}

	// A padded value is trimmed before it is compared, so it selects the same
	// rows as the trimmed literal rather than nothing.
	d1RequireOwnerKeys(t, "padded filter",
		d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
			OwnerEntityRole:     "  npc  ",
			OwnerVisibility:     " owner_private ",
			SourceChatSessionID: " s1 ",
		}), "ent-mira")

	// A value that matches no row is an empty index, not a fallback to every
	// row: a wrong scope must never read as "no scope".
	unmatched := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "nobody_knows",
	})
	if len(unmatched) != 0 {
		t.Errorf("unmatched visibility returned %d owners, want 0 (%v)",
			len(unmatched), d1OwnerIndexKeys(unmatched))
	}
}

// TestD1ListProtagonistEntityMemoryOwnerIndexResolvesOwnerIdentity pins the
// three-step fallback and the drop of a row with no recoverable identity.
//
// Each seed differs from the others only in which of the four identity columns
// is blank, so a failure names the fallback step that was skipped.
func TestD1ListProtagonistEntityMemoryOwnerIndexResolvesOwnerIdentity(t *testing.T) {
	st, conn := newD1TestStore(t)

	// A complete owner row: key and name are taken as stored.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-mira", personaName: "Mira",
		ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-01T00:00:00.000Z",
	})
	// A whitespace-only owner key and name are blank, so both fall back to the
	// persona identity. This is the case a rewrite that tested for '' instead of
	// trimming would get wrong.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-rowan", personaName: "Rowan",
		ownerKey: "   ", ownerName: "  ",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-02T00:00:00.000Z",
	})
	// The key survives on owner_entity_key while the name has to fall through
	// both name columns and land on the resolved key: a memory whose owner is
	// addressable but unnamed still has to be offered.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-ash", personaName: "   ",
		ownerKey: "ent-ash", ownerName: "   ",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-03T00:00:00.000Z",
	})
	// The name comes from the persona while the key comes from the owner: the
	// two projections resolve independently.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-sage", personaName: "Sage",
		ownerKey: "ent-sage-key", ownerName: "",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-04T00:00:00.000Z",
	})
	// No owner identity anywhere: the resolved key and name are both blank, so
	// the row is dropped rather than returned as an empty owner.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "   ", personaName: "  ",
		ownerKey: "", ownerName: "",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-05T00:00:00.000Z",
	})

	owners := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
	})
	if got, want := len(owners), 4; got != want {
		t.Fatalf("owners = %d (%v), want %d: the identity-less row must be dropped, not returned",
			got, d1OwnerIndexKeys(owners), want)
	}
	for _, owner := range owners {
		if owner.OwnerEntityKey == "" {
			t.Errorf("a blank owner key reached the caller: %+v", owner)
		}
	}

	if got := d1OwnerByKey(owners, "ent-mira"); got.OwnerEntityName != "Mira" {
		t.Errorf("complete row = %+v, want name Mira", got)
	}
	// The persona identity is what a memory written before the owner columns
	// existed resolves to, so the alias map still gets a usable name.
	if got := d1OwnerByKey(owners, "ent-rowan"); got.OwnerEntityName != "Rowan" {
		t.Errorf("blank owner row = %+v, want the persona identity ent-rowan/Rowan", got)
	}
	// The last fallback is the resolved key, so an unnamed owner is still
	// addressable by the only handle the caller has.
	if got := d1OwnerByKey(owners, "ent-ash"); got.OwnerEntityName != "ent-ash" {
		t.Errorf("unnamed owner = %+v, want the resolved key as the name", got)
	}
	// Key and name resolve from different columns; collapsing them would change
	// which alias the caller's scope query matches.
	if got := d1OwnerByKey(owners, "ent-sage-key"); got.OwnerEntityName != "Sage" {
		t.Errorf("owner-key owner = %+v, want ent-sage-key/Sage", got)
	}
}

// TestD1ListProtagonistEntityMemoryOwnerIndexGroupsByIdentityAndName pins the
// group key. One owner recorded under two display names is two identities for
// the caller's alias map, and one owner recorded twice is one.
func TestD1ListProtagonistEntityMemoryOwnerIndexGroupsByIdentityAndName(t *testing.T) {
	st, conn := newD1TestStore(t)

	// Two memories, one owner: they collapse into a single owner.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-mira", personaName: "Mira",
		ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-01T00:00:00.000Z",
	})
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-mira", personaName: "Mira",
		ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-02T00:00:00.000Z",
	})
	// The same owner key under a different display name stays a separate group.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-mira", personaName: "Mira",
		ownerKey: "ent-mira", ownerName: "Mira of the Vale",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-03T00:00:00.000Z",
	})
	// A different owner that merely shares a prefix is a different group.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-mirabel", personaName: "Mirabel",
		ownerKey: "ent-mirabel", ownerName: "Mirabel",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-04T00:00:00.000Z",
	})

	owners := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
	})
	if got, want := len(owners), 3; got != want {
		t.Fatalf("owners = %d (%v), want 3: two memories of one owner collapse, one owner under two names does not",
			got, d1OwnerIndexKeys(owners))
	}
	got := d1SortedOwnerIndexKeys(owners)
	want := []string{"ent-mira", "ent-mira", "ent-mirabel"}
	if len(got) != len(want) {
		t.Fatalf("owner keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("owner keys = %v, want %v", got, want)
		}
	}
	// The two groups that share a key must carry their own display names, since
	// the caller maps the name onto an alias set.
	names := map[string]string{}
	for _, owner := range owners {
		names[owner.OwnerEntityName] = owner.OwnerEntityKey
	}
	for name, key := range map[string]string{"Mira": "ent-mira", "Mira of the Vale": "ent-mira", "Mirabel": "ent-mirabel"} {
		if names[name] != key {
			t.Errorf("display name %q resolved to key %q, want %q", name, names[name], key)
		}
	}
}

// TestD1ListProtagonistEntityMemoryOwnerIndexOrdersByNewestUpdate pins the
// recency key: an owner is placed by MAX(updated_at) over its group, so an owner
// with one very recent memory outranks an owner whose rows are all older, and an
// older row inside a group cannot pull that group down.
func TestD1ListProtagonistEntityMemoryOwnerIndexOrdersByNewestUpdate(t *testing.T) {
	st, conn := newD1TestStore(t)

	// "newer" group: newest row is 2026-03-01.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-a", personaName: "A",
		ownerKey: "ent-a", ownerName: "A",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-01T00:00:00.000Z",
	})
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-a", personaName: "A",
		ownerKey: "ent-a", ownerName: "A",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-03-01T00:00:00.000Z",
	})
	// "oldest" group: its first row is ancient but its second is the newest row
	// in the table, so MAX must place it first. A MIN, a FIRST-VALUE, or an
	// ordering by the group's oldest row would invert this.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-b", personaName: "B",
		ownerKey: "ent-b", ownerName: "B",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2020-01-01T00:00:00.000Z",
	})
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-b", personaName: "B",
		ownerKey: "ent-b", ownerName: "B",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-06-01T00:00:00.000Z",
	})
	// "middle" group: entirely between the two.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-c", personaName: "C",
		ownerKey: "ent-c", ownerName: "C",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-05-01T00:00:00.000Z",
	})

	owners := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
	})
	// Order is asserted, not just membership: prepare-turn takes the first
	// matching owners from this list when it trims the candidate pool.
	d1RequireOwnerKeys(t, "recency order", owners, "ent-b", "ent-c", "ent-a")
}

// TestD1ListProtagonistEntityMemoryOwnerIndexIgnoresMemoryScopeFields pins the
// filter contract that makes this an index rather than a scoped read.
//
// prepare-turn builds its memory filter from exactly these fields AFTER reading
// the index, so honouring them here would make the index answer "which owners
// does this key list contain" instead of "which owners exist". The Limit case is
// the dangerous one: a truncated owner list would silently narrow the memory
// read that consumes it.
func TestD1ListProtagonistEntityMemoryOwnerIndexIgnoresMemoryScopeFields(t *testing.T) {
	st, conn := newD1TestStore(t)

	for i, key := range []string{"ent-a", "ent-b", "ent-c"} {
		d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
			personaKey: key, personaName: key,
			ownerKey: key, ownerName: key,
			role: "npc", visibility: "owner_private", sessionID: "s1",
			updatedAt: d1ProtagonistOwnerStamp(i),
		})
	}

	scope := ProtagonistEntityMemoryFilter{
		OwnerEntityRole:     "npc",
		OwnerVisibility:     "owner_private",
		SourceChatSessionID: "s1",
		// Every one of these is an input to the memory read that follows.
		OwnerEntityKey:   "ent-a",
		OwnerEntityKeys:  []string{"ent-a", "ent-b"},
		PersonaEntityKey: "ent-a",
		Limit:            1,
	}
	owners := d1RequireOwnerIndex(t, st, scope)
	if got, want := len(owners), 3; got != want {
		t.Fatalf("owners = %d (%v), want %d: OwnerEntityKey, OwnerEntityKeys, PersonaEntityKey and Limit are not index filters",
			got, d1OwnerIndexKeys(owners), want)
	}

	// A limit of zero or a negative value is the same no-limit case.
	for _, limit := range []int{0, -5} {
		scoped := scope
		scoped.Limit = limit
		if got := len(d1RequireOwnerIndex(t, st, scoped)); got != 3 {
			t.Errorf("limit %d returned %d owners, want 3", limit, got)
		}
	}

	// OwnerEntityKeys must not narrow the read either when it is the only
	// non-empty scope field, which is the shape a future caller would send.
	keyOnly := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityKeys: []string{"ent-a", "ent-b"},
	})
	if got, want := len(keyOnly), 3; got != want {
		t.Errorf("OwnerEntityKeys-only filter returned %d owners (%v), want %d",
			got, d1OwnerIndexKeys(keyOnly), want)
	}
}

// TestD1ListProtagonistEntityMemoryOwnerIndexEmptyResultIsNonNil pins the empty
// contract on both routes to it: a table with no rows, and a filter that admits
// none. prepare-turn distinguishes "no owners exist" (owner_index_empty_...) from
// "the read failed", so the empty answer must be a real answer.
func TestD1ListProtagonistEntityMemoryOwnerIndexEmptyResultIsNonNil(t *testing.T) {
	st, conn := newD1TestStore(t)
	filter := ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
	}

	owners := d1RequireOwnerIndex(t, st, filter)
	if owners == nil {
		t.Error("an empty table must return a non-nil empty slice")
	}
	if len(owners) != 0 {
		t.Errorf("owners = %v, want none", d1OwnerIndexKeys(owners))
	}

	// The same must hold when rows exist but the filter admits none, so a caller
	// cannot tell "no such owner" from "no read happened".
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-a", personaName: "A",
		ownerKey: "ent-a", ownerName: "A",
		role: "npc", visibility: "player_known", sessionID: "s2",
		updatedAt: "2026-01-01T00:00:00.000Z",
	})
	filtered := d1RequireOwnerIndex(t, st, filter)
	if filtered == nil {
		t.Error("a filter that admits nothing must return a non-nil empty slice")
	}
	if len(filtered) != 0 {
		t.Errorf("owners = %v, want none", d1OwnerIndexKeys(filtered))
	}
}

// TestD1ProtagonistEntityMemoryOwnerIndexBinaryTextComparison pins the one
// deliberate divergence from MariaDB: these columns are utf8mb4_unicode_ci
// there, so the comparison and the grouping are case-insensitive, while SQLite
// compares and groups TEXT by bytes.
//
// It is pinned rather than left implicit so the behaviour cannot change
// silently. The reason it is acceptable: the filter values arrive as exact
// literals from the same constants the writer and the sibling memory read use,
// so the compared value is the stored value byte for byte. If that ever stops
// holding, this test is the place that says so.
func TestD1ProtagonistEntityMemoryOwnerIndexBinaryTextComparison(t *testing.T) {
	st, conn := newD1TestStore(t)

	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-mira", personaName: "Mira",
		ownerKey: "ent-mira", ownerName: "Mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-01T00:00:00.000Z",
	})

	// A differently-cased scope value matches nothing here, where MariaDB would
	// match the row. Recorded, not endorsed: it is the price of agreeing with
	// every other D1 reader of these columns.
	if owners := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{OwnerEntityRole: "NPC"}); len(owners) != 0 {
		t.Errorf("role filter \"NPC\" matched %d owners (%v); D1 text equality is byte-exact",
			len(owners), d1OwnerIndexKeys(owners))
	}
	if owners := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{SourceChatSessionID: "S1"}); len(owners) != 0 {
		t.Errorf("session filter \"S1\" matched %d owners (%v); D1 text equality is byte-exact",
			len(owners), d1OwnerIndexKeys(owners))
	}
	// The exact literal still matches, which is the case that actually occurs.
	d1RequireOwnerKeys(t, "exact literal",
		d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
			OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
		}), "ent-mira")

	// Grouping is byte-exact too: two spellings of one name are two owners on
	// D1, where MariaDB's collation would fold them into one.
	d1SeedProtagonistOwnerRow(t, conn, d1ProtagonistOwnerSeed{
		personaKey: "ent-mira", personaName: "Mira",
		ownerKey: "ent-mira", ownerName: "mira",
		role: "npc", visibility: "owner_private", sessionID: "s1",
		updatedAt: "2026-01-02T00:00:00.000Z",
	})
	owners := d1RequireOwnerIndex(t, st, ProtagonistEntityMemoryFilter{
		OwnerEntityRole: "npc", OwnerVisibility: "owner_private", SourceChatSessionID: "s1",
	})
	if got, want := len(owners), 2; got != want {
		t.Fatalf("owners = %d (%v), want 2: D1 groups \"Mira\" and \"mira\" separately",
			got, d1OwnerIndexKeys(owners))
	}
	// The two groups carry their own spelling, so the caller's alias map sees
	// both exactly as stored.
	names := map[string]bool{}
	for _, owner := range owners {
		names[owner.OwnerEntityName] = true
	}
	if !names["Mira"] || !names["mira"] {
		t.Errorf("display names = %v, want both Mira and mira preserved", names)
	}
}

// d1ProtagonistOwnerStamp renders a distinct canonical RFC3339 millisecond stamp
// so recency assertions are about the ordering, not about insertion timing.
func d1ProtagonistOwnerStamp(index int) string {
	return []string{
		"2026-01-01T00:00:00.000Z",
		"2026-02-01T00:00:00.000Z",
		"2026-03-01T00:00:00.000Z",
	}[index]
}
