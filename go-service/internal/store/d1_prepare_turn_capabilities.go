package store

import (
	"context"
	"strings"
)

// D1 prepare-turn range capability.
//
// The prepare-turn projection reads bounded turn windows rather than whole
// tables, and several of these reads have "in range OR still current" semantics
// that matter for prompt assembly. Each is translated from the MariaDB query
// rather than re-derived, because a subtly different window would change what the
// model is shown.
//
// MariaDB's GREATEST(a, b, c) becomes SQLite's scalar MAX(a, b, c). The two agree
// here because every argument is non-NULL: source_turn_start and source_turn_end
// are NOT NULL columns and turn_anchor is wrapped in COALESCE.

var _ PrepareTurnRangeStore = (*d1Store)(nil)

// d1CharacterStateSelectAliased is d1CharacterStateSelect with the "state" alias
// the correlated NOT EXISTS subquery needs.
const d1CharacterStateSelectAliased = `
		SELECT state.id, state.chat_session_id, state.character_name, state.appearance_json,
		       state.personality_json, state.status_json, state.relationships_json,
		       state.speech_style_json, state.field_provenance_json, state.turn_index,
		       state.created_at, state.updated_at
		FROM character_states AS state`

func (s *d1Store) LatestSessionTurnIndex(ctx context.Context, chatSessionID string) (int, error) {
	var latest *int64
	if err := s.conn.QueryRow(ctx, `
		SELECT MAX(turn_index) FROM chat_logs WHERE chat_session_id = ?
	`, chatSessionID).Scan(&latest); err != nil {
		return 0, err
	}
	if latest == nil || *latest < 0 {
		return 0, nil
	}
	return int(*latest), nil
}

// d1IDClause builds the optional "OR id IN (...)" fragment and its arguments,
// skipping non-positive ids exactly as the MariaDB path does.
func d1IDClause(includeIDs []int64) (string, []any) {
	clause := ""
	args := make([]any, 0, len(includeIDs))
	placeholders := make([]string, 0, len(includeIDs))
	for _, id := range includeIDs {
		if id <= 0 {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}
	if len(placeholders) > 0 {
		clause = " OR id IN (" + strings.Join(placeholders, ",") + ")"
	}
	return clause, args
}

func (s *d1Store) ListMemoriesRange(ctx context.Context, chatSessionID string, fromTurn, toTurn int, includeIDs []int64) ([]Memory, error) {
	idClause, idArgs := d1IDClause(includeIDs)
	args := append([]any{chatSessionID, fromTurn, fromTurn, toTurn, toTurn}, idArgs...)

	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, turn_index, summary_json, embedding, embedding_model,
		       importance, emotional_boost, evidence, emotional_intensity,
		       narrative_significance, place_wing, place_room, created_at
		FROM memories
		WHERE chat_session_id = ?
			AND (turn_index < 0 OR ((? <= 0 OR turn_index >= ?) AND (? <= 0 OR turn_index <= ?))`+idClause+`)
		ORDER BY turn_index ASC, id ASC
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Memory
	for rows.Next() {
		var item Memory
		var summaryJSON, embedding, embeddingModel, evidence, placeWing, placeRoom *string
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.TurnIndex, &summaryJSON, &embedding,
			&embeddingModel, &item.Importance, &item.EmotionalBoost, &evidence,
			&item.EmotionalIntensity, &item.NarrativeSignificance, &placeWing,
			&placeRoom, &item.CreatedAt,
		); err != nil {
			return nil, err
		}
		item.SummaryJSON = d1DerefString(summaryJSON)
		item.Embedding = d1DerefString(embedding)
		item.EmbeddingModel = d1DerefString(embeddingModel)
		item.Evidence = d1DerefString(evidence)
		item.PlaceWing = d1DerefString(placeWing)
		item.PlaceRoom = d1DerefString(placeRoom)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) ListEvidenceRange(ctx context.Context, chatSessionID string, fromTurn, toTurn int, includeIDs []int64) ([]DirectEvidence, error) {
	idClause, idArgs := d1IDClause(includeIDs)
	args := append([]any{chatSessionID, fromTurn, fromTurn, toTurn, toTurn}, idArgs...)

	// Tombstoned and superseded records are excluded, and the effective turn is
	// the newest of start, end, and the optional anchor.
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, evidence_kind, evidence_text, source_turn_start, source_turn_end,
		       turn_anchor, source_message_ids_json, source_hash, archive_state, capture_stage,
		       capture_verification, committed_gate, lineage_json, repair_needed, tombstoned,
		       superseded_by_id, created_at
		FROM direct_evidence_records
		WHERE chat_session_id = ?
			AND tombstoned = 0
			AND COALESCE(superseded_by_id, 0) = 0
			AND (
				source_turn_start < 0 OR ((? <= 0 OR MAX(source_turn_start, source_turn_end, COALESCE(turn_anchor, 0)) >= ?)
			 AND (? <= 0 OR MAX(source_turn_start, source_turn_end, COALESCE(turn_anchor, 0)) <= ?))`+idClause+`
			)
		ORDER BY source_turn_start ASC, id ASC
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DirectEvidence
	for rows.Next() {
		var item DirectEvidence
		var turnAnchor, supersededByID *int64
		var sourceMessageIDsJSON, sourceHash, committedGate, lineageJSON *string
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.EvidenceKind, &item.EvidenceText,
			&item.SourceTurnStart, &item.SourceTurnEnd, &turnAnchor,
			&sourceMessageIDsJSON, &sourceHash, &item.ArchiveState, &item.CaptureStage,
			&item.CaptureVerification, &committedGate, &lineageJSON, &item.RepairNeeded,
			&item.Tombstoned, &supersededByID, &item.CreatedAt,
		); err != nil {
			return nil, err
		}
		item.TurnAnchor = int(d1DerefInt64(turnAnchor))
		item.SourceMessageIDsJSON = d1DerefString(sourceMessageIDsJSON)
		item.SourceHash = d1DerefString(sourceHash)
		item.CommittedGate = d1DerefString(committedGate)
		item.LineageJSON = d1DerefString(lineageJSON)
		item.SupersededByID = d1DerefInt64(supersededByID)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) ListKGTriplesRange(ctx context.Context, chatSessionID string, fromTurn, toTurn int) ([]KGTriple, error) {
	// A triple stays relevant while it has no validity end, and an open triple
	// (valid_to NULL or 0) is always included.
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, subject, predicate, object, valid_from, valid_to, source_turn, created_at
		FROM kg_triples
		WHERE chat_session_id = ?
			AND (
				source_turn < 0 OR valid_to IS NULL OR valid_to = 0
				OR ((? <= 0 OR source_turn >= ?) AND (? <= 0 OR source_turn <= ?))
			)
		ORDER BY id ASC
	`, chatSessionID, fromTurn, fromTurn, toTurn, toTurn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []KGTriple
	for rows.Next() {
		var item KGTriple
		var validFrom, validTo, sourceTurn *int64
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.Subject, &item.Predicate,
			&item.Object, &validFrom, &validTo, &sourceTurn, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.ValidFrom = int(d1DerefInt64(validFrom))
		item.ValidTo = int(d1DerefInt64(validTo))
		item.SourceTurn = int(d1DerefInt64(sourceTurn))
		out = append(out, item)
	}
	return out, rows.Err()
}

// ListCharacterStatesCurrentBefore returns each character's latest snapshot
// strictly before beforeTurn, or the latest across the whole session when
// beforeTurn is non-positive.
func (s *d1Store) ListCharacterStatesCurrentBefore(ctx context.Context, chatSessionID string, beforeTurn int) ([]CharacterState, error) {
	rows, err := s.conn.Query(ctx, d1CharacterStateSelectAliased+`
		WHERE state.chat_session_id = ?
		  AND (? <= 0 OR COALESCE(state.turn_index, 0) < ?)
		  AND NOT EXISTS (
			SELECT 1
			FROM character_states newer
			WHERE newer.chat_session_id = state.chat_session_id
			  AND newer.character_name = state.character_name
			  AND (? <= 0 OR COALESCE(newer.turn_index, 0) < ?)
			  AND (COALESCE(newer.turn_index, 0) > COALESCE(state.turn_index, 0)
			       OR (COALESCE(newer.turn_index, 0) = COALESCE(state.turn_index, 0) AND newer.id > state.id))
		  )
		ORDER BY state.turn_index DESC, state.id DESC
	`, chatSessionID, beforeTurn, beforeTurn, beforeTurn, beforeTurn)
	if err != nil {
		return nil, err
	}

	var out []CharacterState
	for rows.Next() {
		item, err := d1ScanCharacterState(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	// Durable operator edits still apply to a historical read.
	return d1ApplyCharacterManualEdits(ctx, s.conn, chatSessionID, "", out)
}

func (s *d1Store) ListActiveStatesRange(ctx context.Context, chatSessionID string, fromTurn, toTurn int) ([]ActiveState, error) {
	// A state is included when it falls in the window, or when nothing newer
	// exists for its type, so the newest state per type is always present.
	rows, err := s.conn.Query(ctx, `
		SELECT state.id, state.chat_session_id, state.state_type, state.content,
		       state.turn_index, state.created_at
		FROM active_states state
		WHERE state.chat_session_id = ?
		  AND (
			((? <= 0 OR state.turn_index >= ?) AND (? <= 0 OR state.turn_index <= ?))
			OR NOT EXISTS (
				SELECT 1
				FROM active_states newer
				WHERE newer.chat_session_id = state.chat_session_id
				  AND newer.state_type = state.state_type
				  AND (COALESCE(newer.turn_index, 0) > COALESCE(state.turn_index, 0)
				       OR (COALESCE(newer.turn_index, 0) = COALESCE(state.turn_index, 0) AND newer.id > state.id))
			)
		  )
		ORDER BY state.turn_index DESC, state.id DESC
	`, chatSessionID, fromTurn, fromTurn, toTurn, toTurn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ActiveState
	for rows.Next() {
		var item ActiveState
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.StateType, &item.Content,
			&item.TurnIndex, &item.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) ListCanonicalStateLayersRange(ctx context.Context, chatSessionID string, fromTurn, toTurn int) ([]CanonicalStateLayer, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT layer.id, layer.chat_session_id, layer.layer_type, layer.content,
		       layer.source_state_type, layer.turn_index, layer.source_turn,
		       layer.source_record, layer.last_verified_turn, layer.confidence,
		       layer.created_at
		FROM canonical_state_layers layer
		WHERE layer.chat_session_id = ?
		  AND (
			((? <= 0 OR layer.turn_index >= ?) AND (? <= 0 OR layer.turn_index <= ?))
			OR NOT EXISTS (
				SELECT 1
				FROM canonical_state_layers newer
				WHERE newer.chat_session_id = layer.chat_session_id
				  AND newer.layer_type = layer.layer_type
				  AND (COALESCE(newer.turn_index, 0) > COALESCE(layer.turn_index, 0)
				       OR (COALESCE(newer.turn_index, 0) = COALESCE(layer.turn_index, 0) AND newer.id > layer.id))
			)
		  )
		ORDER BY layer.turn_index DESC, layer.id DESC
	`, chatSessionID, fromTurn, fromTurn, toTurn, toTurn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CanonicalStateLayer
	for rows.Next() {
		var item CanonicalStateLayer
		var sourceStateType *string
		var sourceTurn, sourceRecord, lastVerifiedTurn *int64
		var confidence *float64
		if err := rows.Scan(
			&item.ID, &item.ChatSessionID, &item.LayerType, &item.Content,
			&sourceStateType, &item.TurnIndex, &sourceTurn, &sourceRecord,
			&lastVerifiedTurn, &confidence, &item.CreatedAt,
		); err != nil {
			return nil, err
		}
		item.SourceStateType = d1DerefString(sourceStateType)
		item.SourceTurn = int(d1DerefInt64(sourceTurn))
		item.SourceRecord = d1DerefInt64(sourceRecord)
		item.LastVerifiedTurn = int(d1DerefInt64(lastVerifiedTurn))
		item.Confidence = d1DerefFloat64(confidence)
		out = append(out, item)
	}
	return out, rows.Err()
}
