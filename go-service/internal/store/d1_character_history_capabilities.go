package store

import (
	"context"
)

// D1 character state snapshot history capability.
//
// CharacterStateHistoryStore is the append-only read behind the character
// snapshot history route and behind the admin field-provenance repair preview.
// It is deliberately separate from ListCharacterStates and GetCharacterState:
// those answer "what is the character now", while this one answers "what did the
// character look like at every turn it was written". Reading history through the
// current-state methods would either hide the earlier snapshots or apply today's
// view to yesterday's rows.
//
// The statement is translated from mariaListCharacterStateHistory rather than
// re-derived, because three details of that query are load-bearing and easy to
// lose in a rewrite:
//
//   - Argument handling. A non-positive limit and any limit above the 200 ceiling
//     both fall back to 50, and a negative offset falls back to 0. The admin
//     repair preview walks the history in pages of exactly 200 and stops as soon
//     as a page comes back short, so the page ceiling and the caller's page size
//     are the same number on purpose and must stay equal.
//   - Ordering. turn_index DESC, id DESC puts the newest snapshot first and breaks
//     a same-turn tie by insertion order. BuildCharacterFieldProvenanceFromHistory
//     re-sorts on the same key to decide which same-turn row represents a turn and
//     which discarded row is not evidence of an earlier origin.
//   - Session and character scope. Both predicates are exact equalities, so the
//     read can neither cross a session boundary nor interleave another
//     character's snapshots into one page.
//
// What is deliberately not applied
//
// The current-state readers overlay the durable operator edits that live in
// character_events (d1ApplyCharacterManualEdits). MariaDB does not apply that
// overlay to this history read, and neither does this method:
// BuildCharacterFieldProvenanceFromHistory reconstructs provenance from the
// snapshots as they were actually written, and an overlay would rewrite
// historical values and invent a later origin for a field that never changed.
// An admin repair preview is exactly the place where a rewritten past would be
// most damaging, so the omission is a contract, not an oversight.
//
// SQLite notes
//
//   - turn_index is nullable in the canonical schema. SQLite sorts NULL as the
//     smallest value, the same as MariaDB, so a snapshot with no turn lands last
//     under DESC and the id DESC tie-break orders the rest. That is why the plain
//     column is used here rather than COALESCE(turn_index, 0), which would
//     promote those rows above turn 0.
//   - The character_name comparison is a binary TEXT equality in SQLite, where
//     the MariaDB column is utf8mb4_unicode_ci and therefore case-insensitive.
//     Every other D1 character read (ListCharacterStates, GetCharacterState, and
//     the manual-edit overlay) already uses the same binary equality, so matching
//     the stored name exactly is what keeps one provider internally consistent
//     rather than making this one route quietly lenient.
//
// An empty result is a nil slice, exactly as on the MariaDB path. The history
// route normalises the slice to an empty array before encoding it, so a character
// with no snapshots answers identically on both providers.

var _ CharacterStateHistoryStore = (*d1Store)(nil)

// d1CharacterHistoryPageLimit is the largest page the history read serves. It is
// the admin repair preview's page size as well, so a full page means "there may
// be more" and a short page means "this is the tail".
const d1CharacterHistoryPageLimit = 200

// d1CharacterHistoryDefaultLimit is the page size a non-positive or oversized
// request falls back to.
const d1CharacterHistoryDefaultLimit = 50

// d1ClampCharacterHistoryLimit mirrors the MariaDB bounds. Note that an oversized
// limit falls back to the default rather than to the ceiling, so a caller asking
// for more than 200 gets a 50-row page instead of a truncated 200-row one.
func d1ClampCharacterHistoryLimit(limit int) int {
	if limit <= 0 || limit > d1CharacterHistoryPageLimit {
		return d1CharacterHistoryDefaultLimit
	}
	return limit
}

// d1ClampCharacterHistoryOffset mirrors the MariaDB guard. A negative offset is
// treated as the first page rather than rejected, so a bad query parameter cannot
// turn the history route into an error response.
func d1ClampCharacterHistoryOffset(offset int) int {
	if offset < 0 {
		return 0
	}
	return offset
}

// ListCharacterStateHistory returns one character's append-only snapshot history
// for one chat session, newest snapshot first.
func (s *d1Store) ListCharacterStateHistory(ctx context.Context, chatSessionID, characterName string, limit, offset int) ([]CharacterState, error) {
	rows, err := s.conn.Query(ctx, d1CharacterStateSelect+`
		WHERE chat_session_id = ? AND character_name = ?
		ORDER BY turn_index DESC, id DESC
		LIMIT ? OFFSET ?
	`, chatSessionID, characterName, d1ClampCharacterHistoryLimit(limit), d1ClampCharacterHistoryOffset(offset))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CharacterState
	for rows.Next() {
		item, err := d1ScanCharacterState(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
