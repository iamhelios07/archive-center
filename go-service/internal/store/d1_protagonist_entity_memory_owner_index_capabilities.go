package store

import (
	"context"
	"strings"
)

// D1 protagonist entity memory owner index capability.
//
// ProtagonistEntityMemoryOwnerIndexStore answers one question for prepare-turn:
// which subjectively-owning entities hold a memory in this session, and what are
// they called. It is a read optimization in front of the much larger
// ListProtagonistEntityMemories scan — prepare-turn narrows the expensive memory
// read to the owners that are actually in the scene instead of semantic-scanning
// every owner-private row (see group_turn_prepare.go, the
// "owner_index_*" read-policy labels). The read returns identities only, never
// memory text, so the owner index cannot leak the content it indexes.
//
// The write and edit lanes over the same table
// (ProtagonistEntityMemoryStore, ProtagonistEntityMemoryRepairStore,
// ProtagonistEntityMemoryManagementStore) are ported in
// d1_protagonist_entity_memory_capabilities.go, which had to land first: a read
// lane is only meaningful once the rows it reads can be written, and prepare-turn
// asserts the owner index only inside its ProtagonistEntityMemoryStore branch.
// PersonaCapsuleStore is still absent; it owns a different table
// (persona_memory_capsules) and is not in this read's path.
//
// What is translated rather than re-derived
//
// The statement below is the MariaDB statement from mariadb_persona.go with no
// semantic rewrite. The load-bearing details that a rewrite would lose:
//
//   - The owner identity is a three-step fallback, not a plain column read.
//     resolved_owner_key is owner_entity_key unless it is blank after TRIM, in
//     which case it is persona_entity_key; resolved_owner_name then falls back
//     through owner_entity_name, persona_entity_name, and finally the resolved
//     key. The final key fallback is what keeps a row with no usable name
//     addressable by key, which is the only handle the caller has.
//   - TRIM runs BEFORE the blank test. A stored owner key of "   " is blank, so
//     it falls back to the persona identity exactly as an empty string would.
//     NULLIF(TRIM(col), '') is what expresses "blank", and the alias is reused
//     inside the name expression rather than re-derived, so the two projections
//     cannot disagree about which row is the owner.
//   - The group is (resolved key, resolved name), NOT the key alone. One owner
//     recorded under two display names is two identities for the caller's alias
//     map, and collapsing them here would silently drop one alias.
//   - The recency key is MAX(updated_at) over the group, so an owner is ordered
//     by its most recent memory, not by its oldest row or by insertion.
//   - Rows that resolve to a blank key AND a blank name are dropped in Go after
//     the scan, exactly as on the MariaDB path. A memory with no owner identity
//     at all is not a candidate owner, and returning it would put an empty key
//     into the caller's OwnerEntityKeys list.
//
// Filter parity
//
// Only OwnerEntityRole, OwnerVisibility, and SourceChatSessionID narrow this
// read, and each is applied only when its trimmed value is non-empty; a
// whitespace-only value therefore applies NO filter rather than matching nothing.
// The remaining ProtagonistEntityMemoryFilter fields are deliberately ignored,
// which is why this method is not a subset of the memory list: OwnerEntityKey,
// OwnerEntityKeys, PersonaEntityKey, and Limit are inputs to the memory read
// that follows, and applying them here would make the index answer a different
// question than the one prepare-turn asks. In particular Limit is not honoured,
// because a truncated owner list would silently narrow the memory read that
// consumes it.
//
// SQLite notes
//
//   - COALESCE, NULLIF, TRIM, and GROUP BY on a result-column alias all behave as
//     they do in MariaDB, so the projection and the grouping are reproduced
//     literally rather than emulated. The aliases do not collide with a column
//     of protagonist_entity_memories, which is what makes SQLite's
//     alias-in-GROUP-BY rule apply to the expression and not to a column.
//   - owner_entity_key, owner_entity_name, persona_entity_key, and
//     persona_entity_name are declared NOT NULL in the canonical schema, so the
//     innermost COALESCE fallback is never NULL and the projection is non-NULL
//     by construction. That is the same assumption the MariaDB scan of these two
//     columns into plain strings makes, and it is why the scan needs no nullable
//     handling that would silently turn a NULL into an empty owner.
//   - updated_at is TEXT and is ordered as TEXT. The column default
//     (strftime('%Y-%m-%dT%H:%M:%fZ','now')) and d1TimeValue both write
//     zero-padded RFC3339 UTC, which sorts lexicographically in chronological
//     order, so MAX and the DESC ordering mean recency exactly as the MariaDB
//     DATETIME(3) does. This is the same timestamp convention the other D1
//     readers that order by updated_at rely on; a row carrying a non-canonical
//     timestamp text is a data defect, not a dialect difference.
//   - The three filter comparisons are SQLite's binary TEXT equality, where the
//     MariaDB columns are utf8mb4_unicode_ci and therefore case-insensitive, and
//     GROUP BY likewise separates "Mira" from "mira" where MariaDB would merge
//     them. This matches every other D1 reader of these columns, and it is the
//     right trade here because the values arrive as exact literals: prepare-turn
//     passes the constants "npc" and "owner_private" and the session id it is
//     already working with, and the same entity key the sibling memory read will
//     later match byte for byte. Making only this reader case-insensitive would
//     disagree with the read it feeds. The divergence is pinned by a test so it
//     cannot change silently.
//
// An empty result is a non-nil empty slice on both providers, so a session with
// no eligible owner encodes identically whichever backend served it.

var _ ProtagonistEntityMemoryOwnerIndexStore = (*d1Store)(nil)

// d1ProtagonistEntityMemoryOwnerIndexSelect is the MariaDB projection, unchanged.
// Both COALESCE chains and the MAX are load-bearing, so the expressions are kept
// whole instead of being rewritten as nullable scans or Go-side fallbacks.
const d1ProtagonistEntityMemoryOwnerIndexSelect = `
		SELECT
			COALESCE(NULLIF(TRIM(owner_entity_key), ''), TRIM(persona_entity_key)) AS resolved_owner_key,
			COALESCE(NULLIF(TRIM(owner_entity_name), ''), NULLIF(TRIM(persona_entity_name), ''), COALESCE(NULLIF(TRIM(owner_entity_key), ''), TRIM(persona_entity_key))) AS resolved_owner_name,
			MAX(updated_at) AS latest_update
		FROM protagonist_entity_memories
		WHERE 1 = 1`

// ListProtagonistEntityMemoryOwners returns the distinct subjectively-owning
// entities of the rows a filter admits, most recently updated owner first.
//
// The filter narrows by owner role, owner visibility, and source session only;
// the identity fields and the limit of ProtagonistEntityMemoryFilter are inputs
// to the memory read this index feeds, not to the index itself. An owner whose
// key and name both resolve to blank is dropped, so a memory with no recoverable
// owner identity never becomes a candidate.
func (s *d1Store) ListProtagonistEntityMemoryOwners(ctx context.Context, filter ProtagonistEntityMemoryFilter) ([]ProtagonistEntityMemoryOwner, error) {
	query := d1ProtagonistEntityMemoryOwnerIndexSelect
	args := []any{}
	// Each predicate is added only for a non-empty trimmed value, and the value
	// bound is the trimmed one, so a padded or whitespace-only filter argument
	// behaves the same here as it does on the MariaDB path.
	if role := strings.TrimSpace(filter.OwnerEntityRole); role != "" {
		query += " AND owner_entity_role = ?"
		args = append(args, role)
	}
	if visibility := strings.TrimSpace(filter.OwnerVisibility); visibility != "" {
		query += " AND owner_visibility = ?"
		args = append(args, visibility)
	}
	if sessionID := strings.TrimSpace(filter.SourceChatSessionID); sessionID != "" {
		query += " AND source_chat_session_id = ?"
		args = append(args, sessionID)
	}
	// The group is the resolved identity pair, and recency is the group's newest
	// update. No secondary sort key is added: the MariaDB statement has none, so
	// adding one here would invent an order the reference does not promise.
	query += " GROUP BY resolved_owner_key, resolved_owner_name ORDER BY latest_update DESC"

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []ProtagonistEntityMemoryOwner{}
	for rows.Next() {
		var owner ProtagonistEntityMemoryOwner
		// latest_update is scanned only to consume its column: the ordering is
		// already applied by SQL, and the D1 column is TEXT rather than a
		// timestamp, so it is not worth parsing a value the caller never sees.
		var latestUpdate string
		if err := rows.Scan(&owner.OwnerEntityKey, &owner.OwnerEntityName, &latestUpdate); err != nil {
			return nil, err
		}
		if strings.TrimSpace(owner.OwnerEntityKey) == "" && strings.TrimSpace(owner.OwnerEntityName) == "" {
			continue
		}
		out = append(out, owner)
	}
	return out, rows.Err()
}
