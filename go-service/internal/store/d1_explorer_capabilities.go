package store

import (
	"context"
	"strings"
)

// D1 Explorer manual-edit parity.
//
// Explorer edits are the human-facing correction path: an operator fixes a
// wrong memory summary, a wrong KG edge, or a mis-captured fact. Losing one is
// worse than losing a derived artefact, so this path is a faithful port of the
// MariaDB statements rather than a redesign, and every behavioural detail of the
// canonical path is reproduced deliberately:
//
//  1. PATCH validation. The SET clause is assembled only from fields the patch
//     actually carries, and each optional integer is distinguished from an
//     omitted one. A patch that carries nothing is not a statement that clears
//     the row: it is rejected with ErrNotEnabled before any SQL is sent, which
//     is the signal the HTTP layer turns into the shadow-write guard.
//
//  2. No-op handling. Only the columns named by the patch are written. A memory
//     patch that sets only importance must not blank summary_json, and a KG
//     patch that sets only valid_to must not rewrite subject. This is the
//     difference between a bounded manual edit and a destructive one.
//
//  3. Not-found handling. The canonical statements do not inspect the affected
//     row count: a session-scoped UPDATE against an absent row simply matches
//     nothing and reports success. The 404 belongs to the HTTP layer, which
//     resolves the target first and returns "Not Found" before it calls the
//     store. Copying a rows-affected check in here would change observable
//     behaviour, because the HTTP layer maps every error other than
//     ErrNotEnabled to 500 — a row deleted between its pre-read and this write
//     would become an internal error instead of a successful no-op. It would
//     also be wrong on its own terms, since SQLite counts a row as affected when
//     the WHERE clause matches even if the new value equals the old one, while
//     MySQL counts only genuinely changed rows: an idempotent re-patch would be
//     reported as "not found" on one engine and as a success on the other.
//
//  4. Session scoping. Every statement is scoped by chat_session_id in the same
//     WHERE clause as the row id, so a caller can never edit a row of another
//     session by guessing an id. The session is a bound parameter, never
//     interpolated.
//
//  5. Values. Booleans are stored as the 0/1 integers the canonical SQLite
//     schema declares, and an OptionalIntPatch that was explicitly cleared is
//     stored as SQL NULL rather than 0, so "superseded by nothing" stays
//     distinguishable from "superseded by row 0". Text patch values are written
//     verbatim, including an empty string: the canonical path writes them
//     verbatim too, so an operator clearing a nullable text column produces the
//     same NULL-to-empty transition on both engines.
//
//  6. Timestamps. None of memories, kg_triples, or direct_evidence_records
//     carries an updated_at column on either engine, and the MariaDB statements
//     do not touch created_at. An Explorer edit is therefore an in-place
//     correction of a row, not a new version, and created_at must survive it
//     unchanged. Adding a stamp here would invent a column the schema does not
//     have and would make the two engines disagree about row age.
//
// Column names are literals from this file, never caller input; only values are
// bound, so building the clause dynamically introduces no injection surface.
//
// The hard deletes below preserve the same session scope. DeleteCharacterByName
// spans three tables and therefore uses D1Conn.Batch, whose Worker bridge runs
// the statements inside D1's single SQLite transaction boundary.

// d1ExplorerSetter accumulates the SET fragments of one Explorer patch in the
// order the fields were offered, keeping each fragment next to its own argument
// so the two cannot drift apart.
type d1ExplorerSetter struct {
	columns []string
	args    []any
}

// add appends one `column = ?` fragment with its bound value.
func (v *d1ExplorerSetter) add(column string, value any) {
	v.columns = append(v.columns, column+" = ?")
	v.args = append(v.args, value)
}

// empty reports whether the patch carried no field at all, which is the
// no-op case the canonical path rejects.
func (v *d1ExplorerSetter) empty() bool { return len(v.columns) == 0 }

// d1ApplyExplorerPatch runs one session-scoped Explorer UPDATE.
//
// table and the collected column names are literals from this file, so the only
// untrusted input is bound. The row id and the session are appended in the
// order the WHERE clause reads them.
func (s *d1Store) d1ApplyExplorerPatch(ctx context.Context, table string, set *d1ExplorerSetter, rowID int64, chatSessionID string) error {
	// An empty patch is rejected before any statement is sent, exactly as the
	// canonical path does, so a malformed request can never degrade into a
	// statement that rewrites every column.
	if set.empty() {
		return ErrNotEnabled
	}
	args := make([]any, 0, len(set.args)+2)
	args = append(args, set.args...)
	args = append(args, rowID, chatSessionID)

	_, err := s.conn.Exec(ctx, `UPDATE `+table+`
		SET `+strings.Join(set.columns, ", ")+`
		WHERE id = ? AND chat_session_id = ?
	`, args...)
	return err
}

// ---------------------------------------------------------------------------
// memory
// ---------------------------------------------------------------------------

// UpdateMemoryExplorerFields applies a bounded manual edit to one memory row of
// one session.
//
// SummaryJSON, Importance, PlaceWing, and PlaceRoom are each written only when
// the patch carries them, so the operator's correction cannot silently clear an
// unrelated column. The canonical path leaves created_at alone and does not
// inspect the affected row count; both are preserved here, with the reasons
// given in the file comment.
func (s *d1Store) UpdateMemoryExplorerFields(ctx context.Context, chatSessionID string, memoryID int64, patch MemoryExplorerPatch) error {
	set := &d1ExplorerSetter{}
	if patch.SummaryJSON != nil {
		set.add("summary_json", *patch.SummaryJSON)
	}
	if patch.Importance != nil {
		set.add("importance", *patch.Importance)
	}
	if patch.PlaceWing != nil {
		set.add("place_wing", *patch.PlaceWing)
	}
	if patch.PlaceRoom != nil {
		set.add("place_room", *patch.PlaceRoom)
	}
	return s.d1ApplyExplorerPatch(ctx, "memories", set, memoryID, chatSessionID)
}

// ---------------------------------------------------------------------------
// knowledge graph triple
// ---------------------------------------------------------------------------

// UpdateKGTripleExplorerFields applies a bounded manual edit to one KG triple of
// one session.
//
// ValidFrom and ValidTo use the tri-state OptionalIntPatch: an absent field is
// not written at all, a set field writes its value, and a set field with no
// value writes SQL NULL, which is how a triple is returned to "valid now" after
// an operator retracts a stale interval.
func (s *d1Store) UpdateKGTripleExplorerFields(ctx context.Context, chatSessionID string, tripleID int64, patch KGTripleExplorerPatch) error {
	set := &d1ExplorerSetter{}
	if patch.Subject != nil {
		set.add("subject", *patch.Subject)
	}
	if patch.Predicate != nil {
		set.add("predicate", *patch.Predicate)
	}
	if patch.Object != nil {
		set.add("object", *patch.Object)
	}
	if patch.ValidFrom.Set {
		set.add("valid_from", optionalIntArg(patch.ValidFrom))
	}
	if patch.ValidTo.Set {
		set.add("valid_to", optionalIntArg(patch.ValidTo))
	}
	return s.d1ApplyExplorerPatch(ctx, "kg_triples", set, tripleID, chatSessionID)
}

// ---------------------------------------------------------------------------
// direct evidence
// ---------------------------------------------------------------------------

// UpdateDirectEvidenceExplorerFields applies a bounded manual edit to one direct
// evidence record of one session.
//
// This is the record an operator corrects when a fact was captured wrongly, so
// the review-state fields matter as much as the text: archive_state,
// capture_verification, committed_gate, repair_needed, and tombstoned are each
// written only when the patch carries them, and the two booleans are stored as
// the 0/1 integers the canonical schema declares. SupersededByID is tri-state,
// so an operator can point one record at its replacement or explicitly clear
// the link back to NULL.
func (s *d1Store) UpdateDirectEvidenceExplorerFields(ctx context.Context, chatSessionID string, recordID int64, patch DirectEvidenceExplorerPatch) error {
	set := &d1ExplorerSetter{}
	if patch.EvidenceText != nil {
		set.add("evidence_text", *patch.EvidenceText)
	}
	if patch.ArchiveState != nil {
		set.add("archive_state", *patch.ArchiveState)
	}
	if patch.CaptureVerification != nil {
		set.add("capture_verification", *patch.CaptureVerification)
	}
	if patch.CommittedGate != nil {
		set.add("committed_gate", *patch.CommittedGate)
	}
	if patch.RepairNeeded != nil {
		set.add("repair_needed", d1BoolValue(*patch.RepairNeeded))
	}
	if patch.Tombstoned != nil {
		set.add("tombstoned", d1BoolValue(*patch.Tombstoned))
	}
	if patch.SupersededByID.Set {
		set.add("superseded_by_id", optionalIntArg(patch.SupersededByID))
	}
	return s.d1ApplyExplorerPatch(ctx, "direct_evidence_records", set, recordID, chatSessionID)
}

var _ ExplorerMutationStore = (*d1Store)(nil)

func (s *d1Store) DeleteMemoryByID(ctx context.Context, chatSessionID string, memoryID int64) error {
	_, err := s.conn.Exec(ctx, `DELETE FROM memories WHERE id = ? AND chat_session_id = ?`, memoryID, chatSessionID)
	return err
}

func (s *d1Store) DeleteDirectEvidenceByID(ctx context.Context, chatSessionID string, recordID int64) error {
	_, err := s.conn.Exec(ctx, `DELETE FROM direct_evidence_records WHERE id = ? AND chat_session_id = ?`, recordID, chatSessionID)
	return err
}

func (s *d1Store) DeleteKGTripleByID(ctx context.Context, chatSessionID string, tripleID int64) error {
	_, err := s.conn.Exec(ctx, `DELETE FROM kg_triples WHERE id = ? AND chat_session_id = ?`, tripleID, chatSessionID)
	return err
}

func (s *d1Store) DeleteCharacterByName(ctx context.Context, chatSessionID string, characterName string) error {
	name := strings.TrimSpace(characterName)
	if name == "" {
		return ErrNotFound
	}
	return s.conn.Batch(ctx,
		D1Statement{SQL: `DELETE FROM character_states WHERE chat_session_id = ? AND character_name = ?`, Args: []any{chatSessionID, name}},
		D1Statement{SQL: `DELETE FROM character_events WHERE chat_session_id = ? AND character_name = ?`, Args: []any{chatSessionID, name}},
		D1Statement{SQL: `DELETE FROM entities WHERE chat_session_id = ? AND name = ? AND (entity_type IS NULL OR entity_type = '' OR LOWER(entity_type) IN ('character', 'person', 'npc', 'protagonist', 'persona', 'player'))`, Args: []any{chatSessionID, name}},
	)
}
