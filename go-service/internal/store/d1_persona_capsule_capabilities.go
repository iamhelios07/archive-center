package store

import (
	"context"
	"errors"
	"strings"
	"time"
)

// D1 persona capsule capabilities.
//
// A persona memory capsule is the portable bundle that lets a protagonist's or a
// player's recollections follow them into a session they never happened in. It is
// three tables, not one:
//
//	persona_memory_capsules       the bundle itself (persona, title, mode)
//	persona_memory_entries        the recollections inside it
//	persona_capsule_attachments   which target session may receive them
//
// All three are ported here together, because porting any subset is a silently
// broken feature rather than a missing one. Without the write, the /persona-capsules
// routes answer 501. Without the attachment write, a capsule can be created but
// can never reach prepare-turn, which discovers it through exactly one call —
// ListAttachedPersonaMemoryEntries in group_turn_prepare.go. That call is the
// reason this lane matters: it is the only path by which a support-only capsule
// reaches a prompt at all.
//
// Why the entry read re-joins protagonist_entity_memories
//
// persona_memory_entries stores a SNAPSHOT of a subjective memory at the moment
// the capsule was built (source_memory_type + source_memory_id plus a copy of the
// text, turn, weights, portability, tags, and evidence). The reference read does
// not serve the snapshot: it LEFT JOINs protagonist_entity_memories and overlays
// every non-empty live value over it, keeping the snapshot only where the live row
// has nothing to say. That is the difference between a capsule that ages with the
// source memory and one that freezes at build time, so the whole COALESCE/NULLIF
// layer is load-bearing:
//
//   - COALESCE(p.col, e.col) means "prefer the live memory". Dropping the join and
//     reading e.col alone would silently serve stale text for the entire life of
//     the capsule.
//   - NULLIF(p.col, '') is NOT redundant with COALESCE. emotional_weight and
//     importance_10 are nullable and rely on plain COALESCE, but memory_text,
//     portability, and evidence_excerpt are NOT NULL in the schema, so an empty
//     live value is indistinguishable from "nothing recorded" without NULLIF.
//     Without it, clearing a memory's evidence excerpt in the browser would
//     replace the capsule's copy with an empty string and the recollection would
//     lose its only surviving evidence line.
//   - injection_policy and created_at are read from e, not overlaid. The live
//     memory row has no injection policy — it is a capsule-local decision made at
//     build time — so overlaying it would erase the support-only/direct contract
//     the capsule was assembled under.
//   - The join is guarded by e.source_memory_type = 'subjective_entity_memory'.
//     Without that predicate the join would also match any entry whose numeric
//     source_memory_id happens to collide with a memory row id, and a
//     snapshot-only entry would be silently rewritten from an unrelated memory.
//
// Ordering is the other half of the contract. The two entry reads order by
// DIFFERENT keys and both tiebreak on e.id, and neither ordering is arbitrary:
//
//   - ListAttachedPersonaMemoryEntries (prepare-turn) orders by the resolved
//     importance DESC, then e.id ASC. The resolved importance is the COALESCE, not
//     e.importance_10, so the budget prepare-turn truncates is spent on what the
//     entry is worth NOW rather than what it was worth when the capsule was built.
//     Ordering by e.id ASC instead would hand the budget to insertion order.
//   - the per-capsule read orders by the resolved source turn ASC, then e.id ASC,
//     so the browser shows a recollection in the order the events happened.
//
// MariaDB idioms replaced
//
//   - LastInsertId + BeginTx. A D1 batch returns only an error, never a per-
//     statement row, so the capsule id cannot come back out of the transaction that
//     created it. The id is therefore allocated before the batch and bound
//     explicitly, which keeps the capsule and every entry inside ONE batch — the
//     exact atomicity the MariaDB transaction provided. See
//     d1PersonaCapsuleNextID for the allocator and why it reads sqlite_sequence.
//   - ON DUPLICATE KEY UPDATE. SQLite spells the same statement
//     ON CONFLICT (capsule_id, target_chat_session_id) DO UPDATE SET ... = excluded
//     ..., and the canonical schema declares exactly that UNIQUE key, so the two
//     are the same statement. Re-attaching a capsule is a deliberate last-write-wins
//     enablement toggle, not a data overwrite: the row it updates holds only the
//     injection mode and the enabled flag.
//   - No SELECT ... FOR UPDATE and no multi-statement BEGIN appear here. The batch
//     IS the transaction.
//
// No source-revision fence
//
// Unlike the precise-memory and entity-identity writers, this write accepts no
// source-derived projection. PersonaMemoryCapsule carries a source_chat_session_id
// but no source revision, and the entries' source_memory_* pair is a pointer that
// is resolved at READ time through the overlay above — the reference performs no
// lifecycle check on it either. Adding a memory_source_revisions fence here would
// make D1 reject capsules MariaDB accepts, which is a behaviour change, not parity.
//
// The three accepted provider divergences
//
//   - Text equality is SQLite BINARY where the MariaDB columns are
//     utf8mb4_unicode_ci, so persona_key and target_chat_session_id separate "Mira"
//     from "mira" here and merge them there. Consistent with every other D1 reader.
//   - Rows affected on a no-op UPDATE differ; none of this file updates, and
//     DeletePersonaMemoryCapsule counts only matched rows, so the DELETE sentinel
//     below behaves identically on both providers.
//   - created_at/updated_at are zero-padded RFC3339 UTC TEXT, so
//     "ORDER BY updated_at DESC, id DESC" means recency first with a stable
//     tiebreak. No CAST is added.

var _ PersonaCapsuleStore = (*d1Store)(nil)

// d1PersonaCapsuleColumns is the MariaDB projection of the capsule read,
// unchanged and in the same order, so the scan and the statement cannot drift.
const d1PersonaCapsuleColumns = `id, persona_key, source_chat_session_id, source_character_name, title, mode, summary, created_at, updated_at`

// d1PersonaCapsuleEntryColumns is the MariaDB entry projection. Every resolved
// column is an alias over the live memory and the entry snapshot; the two
// columns that are read straight off the entry are injection_policy and created_at
// (see the file header for why they are not overlaid).
const d1PersonaCapsuleEntryColumns = `e.id, e.capsule_id, e.source_memory_type, e.source_memory_id,
		       COALESCE(p.source_turn_index, e.source_turn_index),
		       COALESCE(NULLIF(p.memory_text, ''), e.memory_text),
		       COALESCE(p.emotional_weight, e.emotional_weight),
		       COALESCE(p.importance_10, e.importance_10),
		       COALESCE(NULLIF(p.portability, ''), e.portability),
		       COALESCE(p.tags_json, e.tags_json),
		       COALESCE(NULLIF(p.evidence_excerpt, ''), e.evidence_excerpt),
		       e.injection_policy,
		       e.created_at`

// d1PersonaCapsuleInsert is the MariaDB capsule INSERT with one addition: the id
// is supplied instead of generated. The batch cannot return a generated id, and
// the entries in the same batch need it, so the statement is given the id the
// allocator already reserved.
const d1PersonaCapsuleInsert = `
	INSERT INTO persona_memory_capsules
		(id, persona_key, source_chat_session_id, source_character_name, title, mode, summary, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

// d1PersonaCapsuleEntryInsert is the MariaDB entry INSERT unchanged. The twelve
// columns and their order are the entry's canonical shape; reordering them would
// silently transpose two adjacent fields.
const d1PersonaCapsuleEntryInsert = `
	INSERT INTO persona_memory_entries
		(capsule_id, source_memory_type, source_memory_id, source_turn_index, memory_text, emotional_weight, importance_10, portability, tags_json, evidence_excerpt, injection_policy, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// d1PersonaCapsuleScanner is the one-row and cursor shape both entry scans accept.
type d1PersonaCapsuleScanner interface {
	Scan(dest ...any) error
}

// d1PersonaCapsuleDefault trims a value and substitutes a fallback when nothing
// is left, so a whitespace-only title or mode is treated as absent exactly as
// the MariaDB path treats it before it decides on its default.
func d1PersonaCapsuleDefault(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

// scanD1PersonaMemoryCapsule mirrors scanPersonaMemoryCapsule. The two nullable
// text columns are read as **string so a stored NULL stays distinct from a stored
// empty string all the way to d1DerefString, which is the NULL-to-empty
// conversion stringFromNull performs. Reading them as *string would collapse the
// two representations before the caller ever sees the row — harmless for JSON
// today, but it would make a future "did the author leave this blank" question
// unanswerable.
func scanD1PersonaMemoryCapsule(scanner d1PersonaCapsuleScanner) (PersonaMemoryCapsule, error) {
	var item PersonaMemoryCapsule
	var sourceCharacterName, summary *string
	if err := scanner.Scan(&item.ID, &item.PersonaKey, &item.SourceChatSessionID,
		&sourceCharacterName, &item.Title, &item.Mode, &summary,
		&item.CreatedAt, &item.UpdatedAt); err != nil {
		return item, err
	}
	item.SourceCharacterName = d1DerefString(sourceCharacterName)
	item.Summary = d1DerefString(summary)
	return item, nil
}

// scanD1PersonaMemoryEntry mirrors scanPersonaMemoryEntry. Every column the
// MariaDB scan reads as a sql.Null* is nullable in the canonical schema, so each
// is read as its pointer form and dereferenced with the same rule the reference
// applies: NULL becomes the zero value, never an error and never a leftover NULL
// the caller would have to special-case.
func scanD1PersonaMemoryEntry(scanner d1PersonaCapsuleScanner) (PersonaMemoryEntry, error) {
	var item PersonaMemoryEntry
	var sourceMemoryType *string
	var sourceMemoryID, sourceTurn *int64
	var emotionalWeight, importance10 *float64
	var tagsJSON, evidenceExcerpt *string
	if err := scanner.Scan(&item.ID, &item.CapsuleID, &sourceMemoryType, &sourceMemoryID,
		&sourceTurn, &item.MemoryText, &emotionalWeight, &importance10, &item.Portability,
		&tagsJSON, &evidenceExcerpt, &item.InjectionPolicy, &item.CreatedAt); err != nil {
		return item, err
	}
	item.SourceMemoryType = d1DerefString(sourceMemoryType)
	item.SourceMemoryID = d1DerefInt64(sourceMemoryID)
	item.SourceTurn = int(d1DerefInt64(sourceTurn))
	item.EmotionalWeight = d1DerefFloat64(emotionalWeight)
	item.Importance10 = d1DerefFloat64(importance10)
	item.TagsJSON = d1DerefString(tagsJSON)
	item.EvidenceExcerpt = d1DerefString(evidenceExcerpt)
	return item, nil
}

// scanD1PersonaCapsuleAttachment mirrors scanPersonaCapsuleAttachment. The
// schema stores enabled as INTEGER, and d1Assign treats any non-zero integer as
// true, which is what the MariaDB BOOLEAN scan produced.
func scanD1PersonaCapsuleAttachment(scanner d1PersonaCapsuleScanner) (PersonaCapsuleAttachment, error) {
	var item PersonaCapsuleAttachment
	if err := scanner.Scan(&item.ID, &item.CapsuleID, &item.TargetChatSessionID,
		&item.InjectionMode, &item.Enabled, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return item, err
	}
	return item, nil
}

// d1PersonaCapsuleNextID reserves the next capsule id.
//
// The MariaDB path learns the id from LAST_INSERT_ID() inside the transaction it
// just opened. A D1 batch reports only success or failure, so an id generated by
// the batch could never reach the entry statements that need it. The id is
// therefore reserved HERE, before the batch, and bound explicitly to both
// statements — which preserves the property the transaction existed for: the
// capsule and all of its entries commit together or not at all.
//
// The allocator reads sqlite_sequence (the AUTOINCREMENT counter) rather than
// MAX(id), because MAX(id)+1 would reissue the id of a capsule that was just
// deleted, and MariaDB's AUTO_INCREMENT never reuses one. MAX(id) is kept as the
// fallback for a table that has never been inserted into, where no sequence row
// exists yet.
//
// The read is exact under a single writer, and s.memoryDerivationWriteMu holds the
// reservation and the batch in one critical section so nothing in this process
// can take the same id in between. Two Cloudflare Containers racing on one id is
// the same unresolved cross-container boundary the precise-memory writer
// documents: the batch fails with a primary-key conflict, which surfaces as an
// error, never as a silent overwrite or a capsule filed under the wrong id.
func (s *d1Store) d1PersonaCapsuleNextID(ctx context.Context) (int64, error) {
	var nextID int64
	if err := s.conn.QueryRow(ctx, `
		SELECT 1 + MAX(
			COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'persona_memory_capsules'), 0),
			COALESCE((SELECT MAX(id) FROM persona_memory_capsules), 0)
		)`).Scan(&nextID); err != nil {
		return 0, err
	}
	if nextID <= 0 {
		return 0, errors.New("store: d1 could not allocate a persona capsule id")
	}
	return nextID, nil
}

// CreatePersonaMemoryCapsule writes a capsule and its entries as ONE atomic batch.
//
// The defaults are applied in the reference's order — mode first, then title —
// because both are returned to the caller, and a caller that immediately re-reads
// the capsule must see the same strings the create reported.
//
// The returned record reports the stored id and the values that were actually
// persisted rather than echoing the request, matching the reference.
func (s *d1Store) CreatePersonaMemoryCapsule(ctx context.Context, capsule *PersonaMemoryCapsule, entries []PersonaMemoryEntry) (*PersonaMemoryCapsule, error) {
	mode := d1PersonaCapsuleDefault(capsule.Mode, "manual")
	title := d1PersonaCapsuleDefault(capsule.Title, "Persona Memory Capsule")
	now := time.Now().UTC()

	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	capsuleID, err := s.d1PersonaCapsuleNextID(ctx)
	if err != nil {
		return nil, err
	}

	statements := make([]D1Statement, 0, len(entries)+1)
	statements = append(statements, D1Statement{
		SQL: d1PersonaCapsuleInsert,
		Args: []any{
			capsuleID, capsule.PersonaKey, capsule.SourceChatSessionID,
			capsule.SourceCharacterName, title, mode, capsule.Summary,
			d1TimeValue(now), d1TimeValue(now),
		},
	})
	for _, entry := range entries {
		statements = append(statements, d1PersonaCapsuleEntryStatement(capsuleID, entry, now))
	}

	// The batch is the transaction. A failure anywhere in it — including a
	// constraint the caller never saw, such as a NOT NULL violation on a future
	// column — leaves neither the capsule nor a single entry behind, which is the
	// whole reason the reference wrapped these inserts in BeginTx/Commit.
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return nil, err
	}

	out := *capsule
	out.ID = capsuleID
	out.Title = title
	out.Mode = mode
	out.CreatedAt = now
	out.UpdatedAt = now
	return &out, nil
}

// d1PersonaCapsuleEntryStatement binds one entry insert.
//
// Three columns are normalized exactly as the reference normalizes them and the
// rest are bound verbatim, which is what keeps the stored row identical:
//
//   - source_memory_type goes through d1NullableString, reproducing the
//     reference's nullableString, so a snapshot-only entry stores NULL rather
//     than the empty string. That NULL is what keeps the overlay join from
//     matching: the join predicate requires the literal 'subjective_entity_memory',
//     and a snapshot entry must never be rewritten from an unrelated memory row.
//   - source_memory_id is NULL unless positive, matching the reference's
//     "any" variable. A zero id is not a row id, and writing 0 would make the
//     join predicate true against a memory whose id happens to be 0.
//   - source_turn_index, memory_text, emotional_weight, importance_10, and
//     evidence_excerpt are bound as given, including zero and empty string,
//     because the reference binds them unconditionally and a missing turn is a
//     stored 0 rather than a NULL.
func d1PersonaCapsuleEntryStatement(capsuleID int64, entry PersonaMemoryEntry, now time.Time) D1Statement {
	portability := d1PersonaCapsuleDefault(entry.Portability, "same_chat")
	injectionPolicy := d1PersonaCapsuleDefault(entry.InjectionPolicy, "support_only")
	tagsJSON := d1PersonaCapsuleDefault(entry.TagsJSON, "[]")
	var sourceMemoryID any
	if entry.SourceMemoryID > 0 {
		sourceMemoryID = entry.SourceMemoryID
	}
	return D1Statement{
		SQL: d1PersonaCapsuleEntryInsert,
		Args: []any{
			capsuleID, d1NullableString(entry.SourceMemoryType), sourceMemoryID,
			entry.SourceTurn, entry.MemoryText, entry.EmotionalWeight, entry.Importance10,
			portability, tagsJSON, entry.EvidenceExcerpt, injectionPolicy, d1TimeValue(now),
		},
	}
}

// ListPersonaMemoryCapsules returns the capsules a filter admits, most recently
// updated first.
//
// Both filters are independent and conjunctive, and each is applied only when its
// trimmed value is non-empty, so a whitespace-only filter value applies NO
// predicate rather than matching nothing. An empty result is a non-nil empty
// slice so a session with no capsules encodes as [] and not null.
func (s *d1Store) ListPersonaMemoryCapsules(ctx context.Context, filter PersonaCapsuleFilter) ([]PersonaMemoryCapsule, error) {
	query := `SELECT ` + d1PersonaCapsuleColumns + `
		FROM persona_memory_capsules
		WHERE 1 = 1`
	args := []any{}
	if personaKey := strings.TrimSpace(filter.PersonaKey); personaKey != "" {
		query += " AND persona_key = ?"
		args = append(args, personaKey)
	}
	if sessionID := strings.TrimSpace(filter.SourceChatSessionID); sessionID != "" {
		query += " AND source_chat_session_id = ?"
		args = append(args, sessionID)
	}
	query += " ORDER BY updated_at DESC, id DESC"

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []PersonaMemoryCapsule{}
	for rows.Next() {
		item, err := scanD1PersonaMemoryCapsule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// GetPersonaMemoryCapsule returns one capsule and its entries with the live
// subjective memory overlaid onto each entry snapshot.
//
// A capsule with no entries is a valid capsule, not an error: the reference
// returns the empty non-nil entry list, and the route encodes it as [].
func (s *d1Store) GetPersonaMemoryCapsule(ctx context.Context, capsuleID int64) (*PersonaMemoryCapsule, []PersonaMemoryEntry, error) {
	capsule, err := scanD1PersonaMemoryCapsule(s.conn.QueryRow(ctx,
		`SELECT `+d1PersonaCapsuleColumns+`
		FROM persona_memory_capsules
		WHERE id = ?
	`, capsuleID))
	if err != nil {
		// The capsule read is the only thing that can report absence; a capsule
		// with no entries is found, not missing.
		if errors.Is(err, errD1NoRows) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	entries, err := s.d1PersonaCapsuleListEntriesByCapsule(ctx, capsuleID)
	if err != nil {
		return nil, nil, err
	}
	return &capsule, entries, nil
}

// DeletePersonaMemoryCapsule removes a capsule and reports ErrNotFound when the
// id addressed nothing, so a double delete is visible to the caller instead of
// silently succeeding. The entries and attachments go with it through the schema's
// ON DELETE CASCADE, which D1 enforces because the canonical schema declares the
// same foreign keys MariaDB does.
func (s *d1Store) DeletePersonaMemoryCapsule(ctx context.Context, capsuleID int64) error {
	affected, err := s.conn.Exec(ctx, "DELETE FROM persona_memory_capsules WHERE id = ?", capsuleID)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// AttachPersonaMemoryCapsule enables a capsule for a target session, or updates
// that enablement if the pair is already attached.
//
// The upsert is the reference's ON DUPLICATE KEY UPDATE over the schema's
// UNIQUE (capsule_id, target_chat_session_id) key, so a re-attach updates the one
// row in place and can never produce a second attachment. created_at is
// deliberately absent from the UPDATE list, matching the reference: re-attaching
// a capsule must not rewrite when the attachment was first made, and updated_at
// alone is what the recency ordering reads.
func (s *d1Store) AttachPersonaMemoryCapsule(ctx context.Context, attachment *PersonaCapsuleAttachment) error {
	mode := d1PersonaCapsuleDefault(attachment.InjectionMode, "subtle_deja_vu")
	now := time.Now().UTC()
	_, err := s.conn.Exec(ctx, `
		INSERT INTO persona_capsule_attachments
			(capsule_id, target_chat_session_id, injection_mode, enabled, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (capsule_id, target_chat_session_id) DO UPDATE SET
			injection_mode = excluded.injection_mode,
			enabled = excluded.enabled,
			updated_at = excluded.updated_at
	`, attachment.CapsuleID, attachment.TargetChatSessionID, mode, d1BoolValue(attachment.Enabled),
		d1TimeValue(now), d1TimeValue(now))
	return err
}

// DetachPersonaMemoryCapsule removes one attachment. It reports success when
// nothing matched, exactly as the reference does: detaching a capsule that was
// never attached is the caller's intended end state, not a failure, and the route
// answers 200 either way.
func (s *d1Store) DetachPersonaMemoryCapsule(ctx context.Context, capsuleID int64, targetChatSessionID string) error {
	_, err := s.conn.Exec(ctx,
		"DELETE FROM persona_capsule_attachments WHERE capsule_id = ? AND target_chat_session_id = ?",
		capsuleID, targetChatSessionID)
	return err
}

// ListPersonaCapsuleAttachments returns the capsules attached to a target
// session, most recently touched first. Disabled attachments are listed: this
// read is the operator's view of what is configured, and the enabled predicate
// belongs to the injection read, not here.
func (s *d1Store) ListPersonaCapsuleAttachments(ctx context.Context, targetChatSessionID string) ([]PersonaCapsuleAttachment, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, capsule_id, target_chat_session_id, injection_mode, enabled, created_at, updated_at
		FROM persona_capsule_attachments
		WHERE target_chat_session_id = ?
		ORDER BY updated_at DESC, id DESC
	`, targetChatSessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []PersonaCapsuleAttachment{}
	for rows.Next() {
		item, err := scanD1PersonaCapsuleAttachment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ListAttachedPersonaMemoryEntries is the prepare-turn read: the support-only
// recollections a target session may receive, most important first.
//
// A non-positive limit means "no limit", which is how prepare-turn calls it (it
// applies its own candidate budget afterwards). A positive limit is a row bound
// applied AFTER the ordering, so the entries that survive are the most important
// ones rather than an arbitrary prefix.
func (s *d1Store) ListAttachedPersonaMemoryEntries(ctx context.Context, targetChatSessionID string, limit int) ([]PersonaMemoryEntry, error) {
	query := `SELECT ` + d1PersonaCapsuleEntryColumns + `
		FROM persona_memory_entries e
		INNER JOIN persona_capsule_attachments a ON a.capsule_id = e.capsule_id
		LEFT JOIN protagonist_entity_memories p
			ON e.source_memory_type = 'subjective_entity_memory'
			AND e.source_memory_id = p.id
		WHERE a.target_chat_session_id = ? AND a.enabled = TRUE
		ORDER BY COALESCE(p.importance_10, e.importance_10) DESC, e.id ASC
	`
	args := []any{targetChatSessionID}
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []PersonaMemoryEntry{}
	for rows.Next() {
		item, err := scanD1PersonaMemoryEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// d1PersonaCapsuleListEntriesByCapsule returns one capsule's entries in event
// order with the same live overlay ListAttachedPersonaMemoryEntries applies.
//
// It is a separate statement from the attached read rather than one statement
// with an optional join, because the two order by different keys: the browser
// wants chronology, prepare-turn wants importance. Sharing the projection keeps
// the overlay identical between them, which is what stops the two surfaces from
// showing two different versions of the same recollection.
func (s *d1Store) d1PersonaCapsuleListEntriesByCapsule(ctx context.Context, capsuleID int64) ([]PersonaMemoryEntry, error) {
	rows, err := s.conn.Query(ctx, `SELECT `+d1PersonaCapsuleEntryColumns+`
		FROM persona_memory_entries e
		LEFT JOIN protagonist_entity_memories p
			ON e.source_memory_type = 'subjective_entity_memory'
			AND e.source_memory_id = p.id
		WHERE e.capsule_id = ?
		ORDER BY COALESCE(p.source_turn_index, e.source_turn_index) ASC, e.id ASC
	`, capsuleID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []PersonaMemoryEntry{}
	for rows.Next() {
		item, err := scanD1PersonaMemoryEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
