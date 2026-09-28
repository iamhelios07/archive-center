package store

import (
	"context"
	"strings"
	"time"
)

// D1 protagonist entity memory capabilities.
//
// protagonist_entity_memories is the owner-private subjective memory lane: what
// an entity remembers about itself, stored apart from the portable persona
// capsule bundles. Three optional capabilities live on this table and all three
// are ported here together, because they are one user-visible surface (the
// Entity Memory Browser) plus one prepare-turn read:
//
//   - ProtagonistEntityMemoryStore          create + filtered list
//   - ProtagonistEntityMemoryRepairStore    alias canonicalization (owner only)
//   - ProtagonistEntityMemoryManagementStore user edit + delete
//
// Porting the read without its siblings would be the failure mode this file
// avoids: prepare-turn would resolve an owner through the owner index, find no
// memory store, and silently drop every subjective memory from the prompt. The
// owner index (d1_protagonist_entity_memory_owner_index_capabilities.go) was
// therefore unreachable until this file landed, and it becomes reachable now
// that group_turn_prepare.go finds ProtagonistEntityMemoryStore.
//
// Identity normalization is the load-bearing part
//
// The four identity fields (persona key/name, owner key/name) are normalized by
// a two-step cascade, and the ORDER of the fallbacks is part of the contract
// rather than an implementation detail:
//
//  1. Each key falls back to the other key. A memory recorded for a persona but
//     not attributed to an owner is still addressable by that one key.
//  2. Each name falls back to the other name, and only then to its own key.
//     Falling back to the KEY is what keeps a row with no usable display name
//     addressable, since the key is the only handle the caller ever has.
//
// The name step reads the raw incoming PersonaEntityName, not the already
// resolved personaEntityName. Resolving in the opposite order would let a name
// inherit a key that was itself derived, which produces a different stored row
// whenever only one of the two names is present. d1NormalizeProtagonistIdentity
// therefore performs the cascade in exactly the MariaDB order and returns the
// four values together, so the create, repair, and management paths cannot drift
// apart from each other or from the reference.
//
// Defaults are applied by the caller, not here, because the three write paths
// disagree on purpose. Create and management write a complete row and fill
// owner_entity_role, owner_visibility, portability, and target_reveal_policy
// with their defaults. Repair leaves role and visibility ALONE when the update
// carries none: an alias canonicalization must not silently reset a memory from
// "owner_private" back to "player_known". That conditional write is expressed
// with CASE WHEN in the statement, not by a Go-side "was it empty?" test, so
// the decision is made against the stored row in one statement.
//
// Filter parity in ListProtagonistEntityMemories
//
// The owner filter is a three-way choice, not three independent predicates, and
// collapsing it would change which rows a caller sees:
//
//   - OwnerEntityKeys (deduplicated, blanks dropped) wins and is an IN list over
//     the RESOLVED owner key, i.e. COALESCE(NULLIF(TRIM(owner_entity_key),''),
//     TRIM(persona_entity_key)). A key that matches the list therefore matches
//     whether or not the row has an owner of its own.
//   - Only when that list is empty does a single OwnerEntityKey apply, and it
//     admits the same resolved key through an equivalent disjunction.
//   - Only when both are empty does PersonaEntityKey apply.
//
// The remaining filters are conjunctive. Limit is honored only when positive,
// and a positive Limit also selects the ordering, because the MariaDB statement
// has exactly one ORDER BY and a truncated result must be the most recent rows
// rather than an arbitrary prefix of them.
//
// SQLite notes
//
//   - The statements are the MariaDB statements. TRIM, COALESCE, NULLIF, the
//     CASE WHEN role/visibility guard, and the parameterized LIMIT are all
//     SQLite syntax, so nothing here is emulated in Go.
//   - secret_guard is stored as INTEGER by the canonical schema; d1BoolValue
//     and the *bool scan destination convert it, and d1Assign treats any
//     non-zero integer as true exactly as the MariaDB TINYINT scan does.
//   - source_turn_index, importance_10, and emotional_weight are written as the
//     caller's values, including zero, because the MariaDB writer binds them
//     unconditionally and a missing turn is stored as 0 rather than NULL. The
//     scan therefore reads them as *int64/*float64 and reproduces the same
//     NULL-to-zero dereference the reference scan performs.
//   - created_at and updated_at are TEXT. d1TimeValue writes zero-padded RFC3339
//     UTC, which sorts lexicographically in chronological order, so
//     "ORDER BY updated_at DESC, id DESC" means recency first with a stable
//     tiebreak — the same order the MariaDB DATETIME(3) columns produce.
//   - Text comparisons are SQLite's binary equality where the MariaDB columns
//     are utf8mb4_unicode_ci, so D1 separates "Mira" from "mira" where MariaDB
//     would merge them. This is consistent with every other D1 reader of these
//     columns and with the sibling owner index, and disagreeing here would make
//     the owner lookup and the memory read it feeds see different rows. The
//     divergence is pinned by a test.
//
// A known, deliberate divergence: rows affected on a no-op update.
//
// MariaDB reports "rows affected" as rows whose stored bytes actually changed,
// because no DSN in this repository sets clientFoundRows. Re-writing a memory
// with the values it already holds therefore returns 0 on MariaDB, and the
// reference path turns that into ErrNotFound for a row that plainly exists.
// SQLite's changes() counts every row the UPDATE processed, so the same call
// reports 1 here and correctly reports success. Reproducing the MariaDB artifact
// would mean reporting a missing row that is present, so the D1 path keeps the
// correct behavior and the difference is pinned by a test instead of hidden.

var _ ProtagonistEntityMemoryStore = (*d1Store)(nil)
var _ ProtagonistEntityMemoryRepairStore = (*d1Store)(nil)
var _ ProtagonistEntityMemoryManagementStore = (*d1Store)(nil)

// d1ProtagonistEntityMemoryColumns is the MariaDB projection of
// ListProtagonistEntityMemories, unchanged and in the same order, so the scan
// below and the statement above cannot drift apart.
const d1ProtagonistEntityMemoryColumns = `id, persona_entity_key, persona_entity_name, owner_entity_key, owner_entity_name,
		       owner_entity_role, owner_visibility, source_chat_session_id, source_character_name,
		       source_turn_index, memory_text, evidence_excerpt, secret_guard, portability, tags_json,
		       target_reveal_policy, importance_10, emotional_weight, created_at, updated_at`

// d1NormalizeProtagonistIdentity applies the two-step identity cascade described
// above and returns the four values in the order the writers bind them.
func d1NormalizeProtagonistIdentity(personaKey, personaName, ownerKey, ownerName string) (string, string, string, string) {
	personaEntityKey := strings.TrimSpace(personaKey)
	ownerEntityKey := strings.TrimSpace(ownerKey)
	if ownerEntityKey == "" {
		ownerEntityKey = personaEntityKey
	}
	if personaEntityKey == "" {
		personaEntityKey = ownerEntityKey
	}
	// Both names are read from the raw input before either is resolved, which is
	// what keeps "only the persona name given" from resolving through a key.
	ownerEntityName := strings.TrimSpace(ownerName)
	personaEntityName := strings.TrimSpace(personaName)
	if ownerEntityName == "" {
		ownerEntityName = personaEntityName
	}
	if personaEntityName == "" {
		personaEntityName = ownerEntityName
	}
	if ownerEntityName == "" {
		ownerEntityName = ownerEntityKey
	}
	if personaEntityName == "" {
		personaEntityName = personaEntityKey
	}
	return personaEntityKey, personaEntityName, ownerEntityKey, ownerEntityName
}

// d1ProtagonistEntityMemoryDefault trims a value and substitutes fallback when
// nothing is left, so a whitespace-only argument is treated as absent exactly
// as the MariaDB path treats it.
func d1ProtagonistEntityMemoryDefault(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

// scanD1ProtagonistEntityMemory mirrors scanProtagonistEntityMemory. The
// identity, role, visibility, and reveal-policy fallbacks are reproduced here
// rather than pushed into SQL, because they exist to give a NULL or blank column
// a usable value for the caller and the same fallback must apply to a column
// that was never NULL.
func scanD1ProtagonistEntityMemory(scanner interface{ Scan(dest ...any) error }) (ProtagonistEntityMemory, error) {
	var item ProtagonistEntityMemory
	var sourceCharacterName, evidenceExcerpt, tagsJSON *string
	var targetRevealPolicy string
	var sourceTurn *int64
	var importance10, emotionalWeight *float64
	if err := scanner.Scan(
		&item.ID, &item.PersonaEntityKey, &item.PersonaEntityName, &item.OwnerEntityKey, &item.OwnerEntityName,
		&item.OwnerEntityRole, &item.OwnerVisibility, &item.SourceChatSessionID, &sourceCharacterName,
		&sourceTurn, &item.MemoryText, &evidenceExcerpt, &item.SecretGuard, &item.Portability,
		&tagsJSON, &targetRevealPolicy, &importance10, &emotionalWeight, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return item, err
	}
	item.OwnerEntityKey = strings.TrimSpace(item.OwnerEntityKey)
	if item.OwnerEntityKey == "" {
		item.OwnerEntityKey = item.PersonaEntityKey
	}
	item.OwnerEntityName = strings.TrimSpace(item.OwnerEntityName)
	if item.OwnerEntityName == "" {
		item.OwnerEntityName = item.PersonaEntityName
	}
	item.OwnerEntityRole = d1ProtagonistEntityMemoryDefault(item.OwnerEntityRole, "protagonist")
	item.OwnerVisibility = d1ProtagonistEntityMemoryDefault(item.OwnerVisibility, "player_known")
	item.SourceCharacterName = d1DerefString(sourceCharacterName)
	item.SourceTurn = int(d1DerefInt64(sourceTurn))
	item.EvidenceExcerpt = d1DerefString(evidenceExcerpt)
	item.TagsJSON = d1DerefString(tagsJSON)
	item.TargetRevealPolicy = d1ProtagonistEntityMemoryDefault(targetRevealPolicy, "requires_explicit_attachment")
	item.Importance10 = d1DerefFloat64(importance10)
	item.EmotionalWeight = d1DerefFloat64(emotionalWeight)
	return item, nil
}

// CreateProtagonistEntityMemory inserts one subjective memory and returns it
// with the stored id and the identity cascade that was actually persisted.
//
// The returned record reports the normalized values rather than echoing the
// request, so a caller that immediately re-reads the row compares like with
// like. A nil item is rejected with ErrNotEnabled, matching the reference: the
// route asserts this capability and a nil body is a caller error, not an empty
// memory.
func (s *d1Store) CreateProtagonistEntityMemory(ctx context.Context, item *ProtagonistEntityMemory) (*ProtagonistEntityMemory, error) {
	if item == nil {
		return nil, ErrNotEnabled
	}
	personaEntityKey, personaEntityName, ownerEntityKey, ownerEntityName := d1NormalizeProtagonistIdentity(
		item.PersonaEntityKey, item.PersonaEntityName, item.OwnerEntityKey, item.OwnerEntityName)
	portability := d1ProtagonistEntityMemoryDefault(item.Portability, "portable_persona_recollection")
	ownerEntityRole := d1ProtagonistEntityMemoryDefault(item.OwnerEntityRole, "protagonist")
	ownerVisibility := d1ProtagonistEntityMemoryDefault(item.OwnerVisibility, "player_known")
	targetRevealPolicy := d1ProtagonistEntityMemoryDefault(item.TargetRevealPolicy, "requires_explicit_attachment")
	now := time.Now().UTC()

	// RETURNING id is the D1 replacement for MariaDB's LastInsertId: D1 has no
	// connection-scoped insert cursor, so the identity comes back with the row.
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO protagonist_entity_memories
			(persona_entity_key, persona_entity_name, owner_entity_key, owner_entity_name,
			 owner_entity_role, owner_visibility, source_chat_session_id, source_character_name,
			 source_turn_index, memory_text, evidence_excerpt, secret_guard, portability, tags_json,
			 target_reveal_policy, importance_10, emotional_weight, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		personaEntityKey, personaEntityName, ownerEntityKey, ownerEntityName, ownerEntityRole, ownerVisibility,
		strings.TrimSpace(item.SourceChatSessionID), d1NullableString(item.SourceCharacterName),
		item.SourceTurn, strings.TrimSpace(item.MemoryText), d1NullableString(item.EvidenceExcerpt),
		d1BoolValue(item.SecretGuard), portability, d1NullableString(item.TagsJSON), targetRevealPolicy,
		item.Importance10, item.EmotionalWeight, d1TimeValue(now), d1TimeValue(now)).Scan(&id); err != nil {
		return nil, err
	}

	out := *item
	out.ID = id
	out.PersonaEntityKey = personaEntityKey
	out.PersonaEntityName = personaEntityName
	out.OwnerEntityKey = ownerEntityKey
	out.OwnerEntityName = ownerEntityName
	out.OwnerEntityRole = ownerEntityRole
	out.OwnerVisibility = ownerVisibility
	out.Portability = portability
	out.TargetRevealPolicy = targetRevealPolicy
	out.SourceChatSessionID = strings.TrimSpace(item.SourceChatSessionID)
	out.SourceCharacterName = strings.TrimSpace(item.SourceCharacterName)
	out.MemoryText = strings.TrimSpace(item.MemoryText)
	out.EvidenceExcerpt = strings.TrimSpace(item.EvidenceExcerpt)
	out.TagsJSON = strings.TrimSpace(item.TagsJSON)
	out.CreatedAt = now
	out.UpdatedAt = now
	return &out, nil
}

// ListProtagonistEntityMemories returns the rows a filter admits, most recently
// updated first.
//
// An empty result is a non-nil empty slice, so a session with no subjective
// memory encodes identically on both providers.
func (s *d1Store) ListProtagonistEntityMemories(ctx context.Context, filter ProtagonistEntityMemoryFilter) ([]ProtagonistEntityMemory, error) {
	query := `SELECT ` + d1ProtagonistEntityMemoryColumns + `
		FROM protagonist_entity_memories
		WHERE 1 = 1`
	args := []any{}

	// The owner filter is a precedence chain, not a conjunction. Only the first
	// applicable branch is applied, because each branch answers a different
	// question about the resolved owner and MariaDB applies exactly one.
	ownerKeys := make([]string, 0, len(filter.OwnerEntityKeys))
	seenOwnerKeys := map[string]bool{}
	for _, rawKey := range filter.OwnerEntityKeys {
		key := strings.TrimSpace(rawKey)
		if key == "" || seenOwnerKeys[key] {
			continue
		}
		seenOwnerKeys[key] = true
		ownerKeys = append(ownerKeys, key)
	}
	switch {
	case len(ownerKeys) > 0:
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ownerKeys)), ",")
		// The IN list runs against the RESOLVED owner key so one key selects the
		// memory whether or not the row carries its own owner column.
		query += " AND COALESCE(NULLIF(TRIM(owner_entity_key), ''), TRIM(persona_entity_key)) IN (" + placeholders + ")"
		for _, key := range ownerKeys {
			args = append(args, key)
		}
	case strings.TrimSpace(filter.OwnerEntityKey) != "":
		query += " AND (owner_entity_key = ? OR (owner_entity_key = '' AND persona_entity_key = ?))"
		key := strings.TrimSpace(filter.OwnerEntityKey)
		args = append(args, key, key)
	case strings.TrimSpace(filter.PersonaEntityKey) != "":
		query += " AND (persona_entity_key = ? OR owner_entity_key = ?)"
		key := strings.TrimSpace(filter.PersonaEntityKey)
		args = append(args, key, key)
	}

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
	// One ORDER BY with a stable tiebreak: updated_at is TEXT but zero-padded
	// RFC3339 UTC, so it orders chronologically, and id breaks ties between rows
	// written in the same instant.
	query += " ORDER BY updated_at DESC, id DESC"
	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []ProtagonistEntityMemory{}
	for rows.Next() {
		item, err := scanD1ProtagonistEntityMemory(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// UpdateProtagonistEntityMemoryOwner canonicalizes the owner identity of one
// memory. It cannot change memory text or evidence, which is what makes it safe
// to run as a bulk alias repair.
//
// owner_entity_role and owner_visibility are written only when the update
// carries a value. The guard is a CASE WHEN inside the statement so the decision
// is made against the stored row: a repair that omits the visibility must leave
// "owner_private" private instead of resetting it to the default.
func (s *d1Store) UpdateProtagonistEntityMemoryOwner(ctx context.Context, update ProtagonistEntityMemoryOwnerUpdate) error {
	if update.ID <= 0 {
		return ErrNotFound
	}
	personaEntityKey, personaEntityName, ownerEntityKey, ownerEntityName := d1NormalizeProtagonistIdentity(
		update.PersonaEntityKey, update.PersonaEntityName, update.OwnerEntityKey, update.OwnerEntityName)
	ownerRole := strings.TrimSpace(update.OwnerEntityRole)
	ownerVisibility := strings.TrimSpace(update.OwnerVisibility)

	affected, err := s.conn.Exec(ctx, `
		UPDATE protagonist_entity_memories
		SET persona_entity_key = ?,
		    persona_entity_name = ?,
		    owner_entity_key = ?,
		    owner_entity_name = ?,
		    owner_entity_role = CASE WHEN ? = '' THEN owner_entity_role ELSE ? END,
		    owner_visibility = CASE WHEN ? = '' THEN owner_visibility ELSE ? END,
		    tags_json = ?,
		    updated_at = ?
		WHERE id = ?
	`, personaEntityKey, personaEntityName, ownerEntityKey, ownerEntityName,
		ownerRole, ownerRole, ownerVisibility, ownerVisibility, d1NullableString(update.TagsJSON),
		d1TimeValue(time.Now().UTC()), update.ID)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateProtagonistEntityMemory rewrites the user-editable fields of one memory.
// The row is addressed by id and nothing cascades: derived memories, chat logs,
// and evidence rows are untouched, exactly as the reference contract promises.
func (s *d1Store) UpdateProtagonistEntityMemory(ctx context.Context, update ProtagonistEntityMemoryUpdate) error {
	if update.ID <= 0 {
		return ErrNotFound
	}
	personaEntityKey, personaEntityName, ownerEntityKey, ownerEntityName := d1NormalizeProtagonistIdentity(
		update.PersonaEntityKey, update.PersonaEntityName, update.OwnerEntityKey, update.OwnerEntityName)
	ownerRole := d1ProtagonistEntityMemoryDefault(update.OwnerEntityRole, "protagonist")
	ownerVisibility := d1ProtagonistEntityMemoryDefault(update.OwnerVisibility, "player_known")
	portability := d1ProtagonistEntityMemoryDefault(update.Portability, "portable_persona_recollection")
	targetRevealPolicy := d1ProtagonistEntityMemoryDefault(update.TargetRevealPolicy, "requires_explicit_attachment")

	affected, err := s.conn.Exec(ctx, `
		UPDATE protagonist_entity_memories
		SET persona_entity_key = ?,
		    persona_entity_name = ?,
		    owner_entity_key = ?,
		    owner_entity_name = ?,
		    owner_entity_role = ?,
		    owner_visibility = ?,
		    source_character_name = ?,
		    memory_text = ?,
		    evidence_excerpt = ?,
		    secret_guard = ?,
		    portability = ?,
		    tags_json = ?,
		    target_reveal_policy = ?,
		    importance_10 = ?,
		    emotional_weight = ?,
		    updated_at = ?
		WHERE id = ?
	`, personaEntityKey, personaEntityName, ownerEntityKey, ownerEntityName, ownerRole, ownerVisibility,
		d1NullableString(update.SourceCharacterName), strings.TrimSpace(update.MemoryText),
		d1NullableString(update.EvidenceExcerpt), d1BoolValue(update.SecretGuard), portability,
		d1NullableString(update.TagsJSON), targetRevealPolicy, update.Importance10, update.EmotionalWeight,
		d1TimeValue(time.Now().UTC()), update.ID)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteProtagonistEntityMemory removes one memory and reports ErrNotFound when
// the id addressed nothing, so a double delete is visible to the caller instead
// of silently succeeding.
func (s *d1Store) DeleteProtagonistEntityMemory(ctx context.Context, id int64) error {
	if id <= 0 {
		return ErrNotFound
	}
	affected, err := s.conn.Exec(ctx, `DELETE FROM protagonist_entity_memories WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}
