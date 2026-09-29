package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// d1Store is the Cloudflare D1 canonical provider.
//
// It implements the canonical Store contract over a D1Conn, so the same
// statements run against Cloudflare D1 in production and against a local SQLite
// engine in verification. MariaDB-only idioms are deliberately absent: no
// FOR UPDATE, no LAST_INSERT_ID, and no MySQL upsert. Idempotent re-inserts use
// SQLite's ON CONFLICT ... DO NOTHING followed by an explicit conflict check,
// which reproduces the MariaDB behaviour of rejecting a same-key write with a
// different payload instead of silently overwriting it.
//
// The canonical Store contract is implemented in full. Optional capability
// interfaces are advertised through Go type assertions: the D1 store implements
// the ones it supports (currently ActiveScopeStore) and is simply not asserted
// for the rest, so a missing optional capability can never be mistaken for a
// silent no-op.

// d1Store implements Store over a D1 transport.
type d1Store struct {
	conn D1Conn

	// Serializes source-revision invalidation with its known-vector discovery.
	// D1 Batch owns the commit boundary; this protects the necessary read-before-
	// batch preparation while one Go service instance is handling the session.
	memoryDerivationWriteMu sync.Mutex
}

var _ Store = (*d1Store)(nil)

// d1Store also satisfies the optional active-scope contract, which
// ListInheritedWorldRules and the world-graph routes depend on.
var _ ActiveScopeStore = (*d1Store)(nil)

// NewD1Store returns the D1 canonical store over the given transport.
func NewD1Store(conn D1Conn) (Store, error) {
	if conn == nil {
		return nil, fmt.Errorf("store: d1 connection is required")
	}
	return &d1Store{conn: conn}, nil
}

// ---------------------------------------------------------------------------
// chat logs
// ---------------------------------------------------------------------------

func (s *d1Store) SaveChatLog(ctx context.Context, log *ChatLog) error {
	if log == nil {
		return errors.New("chat log is required")
	}
	role := strings.ToLower(strings.TrimSpace(log.Role))
	log.Role = role

	// ON CONFLICT DO NOTHING reproduces MariaDB's ON DUPLICATE KEY UPDATE
	// id = LAST_INSERT_ID(id): a replay of the same (session, turn, role) key
	// must not create a second row.
	if _, err := s.conn.Exec(ctx, `
		INSERT INTO chat_logs (chat_session_id, turn_index, role, content, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (chat_session_id, turn_index, role) DO NOTHING
	`, log.ChatSessionID, log.TurnIndex, role, log.Content, d1TimeValue(log.CreatedAt)); err != nil {
		return err
	}

	// Read back the canonical row and reject a conflicting payload. The database
	// cannot enforce this: a same-key upsert with different content is accepted
	// by SQLite, so the conflict gate lives here rather than in a trigger.
	var existingContent string
	if err := s.conn.QueryRow(ctx, `
		SELECT id, content
		FROM chat_logs
		WHERE chat_session_id = ? AND turn_index = ? AND role = ?
		LIMIT 1
	`, log.ChatSessionID, log.TurnIndex, role).Scan(&log.ID, &existingContent); err != nil {
		return err
	}
	if strings.TrimSpace(existingContent) != strings.TrimSpace(log.Content) {
		return fmt.Errorf("chat log role conflict for session %s turn %d role %s", log.ChatSessionID, log.TurnIndex, role)
	}
	return nil
}

func (s *d1Store) ListChatLogs(ctx context.Context, chatSessionID string, fromTurn, toTurn int) ([]ChatLog, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, turn_index, role, content, created_at
		FROM chat_logs
		WHERE chat_session_id = ? AND (? <= 0 OR turn_index >= ?) AND (? <= 0 OR turn_index <= ?)
		ORDER BY turn_index ASC, id ASC
	`, chatSessionID, fromTurn, fromTurn, toTurn, toTurn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ChatLog
	for rows.Next() {
		var item ChatLog
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.TurnIndex, &item.Role, &item.Content, &item.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// effective inputs
// ---------------------------------------------------------------------------

func (s *d1Store) SaveEffectiveInput(ctx context.Context, in *EffectiveInput) error {
	if in == nil {
		return errors.New("effective input is required")
	}
	_, err := s.conn.Exec(ctx, `
		INSERT INTO effective_input_logs (chat_session_id, turn_index, effective_input, created_at)
		VALUES (?, ?, ?, ?)
	`, in.ChatSessionID, in.TurnIndex, in.EffectiveInput, d1TimeValue(in.CreatedAt))
	return err
}

func (s *d1Store) GetEffectiveInput(ctx context.Context, chatSessionID string, turnIndex int) (*EffectiveInput, error) {
	var item EffectiveInput
	err := s.conn.QueryRow(ctx, `
		SELECT id, chat_session_id, turn_index, effective_input, created_at
		FROM effective_input_logs
		WHERE chat_session_id = ? AND turn_index = ?
		ORDER BY id DESC
		LIMIT 1
	`, chatSessionID, turnIndex).Scan(&item.ID, &item.ChatSessionID, &item.TurnIndex, &item.EffectiveInput, &item.CreatedAt)
	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &item, nil
}

// ---------------------------------------------------------------------------
// memories
// ---------------------------------------------------------------------------

func (s *d1Store) SaveMemory(ctx context.Context, mem *Memory) error {
	if mem == nil {
		return errors.New("memory is required")
	}
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO memories (
			chat_session_id, turn_index, summary_json, embedding, embedding_model,
			importance, emotional_boost, evidence, emotional_intensity,
			narrative_significance, place_wing, place_room, created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`, mem.ChatSessionID, mem.TurnIndex, d1NullableString(mem.SummaryJSON), d1NullableString(mem.Embedding),
		d1NullableString(mem.EmbeddingModel), mem.Importance, mem.EmotionalBoost, d1NullableString(mem.Evidence),
		mem.EmotionalIntensity, mem.NarrativeSignificance, d1NullableString(mem.PlaceWing),
		d1NullableString(mem.PlaceRoom), d1TimeValue(mem.CreatedAt)).Scan(&id); err != nil {
		return err
	}
	mem.ID = id
	return nil
}

func (s *d1Store) ListMemories(ctx context.Context, chatSessionID string, fromTurn, toTurn int) ([]Memory, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, turn_index, summary_json, embedding, embedding_model,
		       importance, emotional_boost, evidence, emotional_intensity,
		       narrative_significance, place_wing, place_room, created_at
		FROM memories
		WHERE chat_session_id = ? AND (? <= 0 OR turn_index >= ?) AND (? <= 0 OR turn_index <= ?)
		ORDER BY turn_index ASC, id ASC
	`, chatSessionID, fromTurn, fromTurn, toTurn, toTurn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Memory
	for rows.Next() {
		var item Memory
		var summary, embedding, model, evidence, wing, room *string
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.TurnIndex, &summary, &embedding, &model,
			&item.Importance, &item.EmotionalBoost, &evidence, &item.EmotionalIntensity,
			&item.NarrativeSignificance, &wing, &room, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.SummaryJSON = d1DerefString(summary)
		item.Embedding = d1DerefString(embedding)
		item.EmbeddingModel = d1DerefString(model)
		item.Evidence = d1DerefString(evidence)
		item.PlaceWing = d1DerefString(wing)
		item.PlaceRoom = d1DerefString(room)
		out = append(out, item)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// direct evidence
// ---------------------------------------------------------------------------

func (s *d1Store) SaveEvidence(ctx context.Context, e *DirectEvidence) error {
	if e == nil {
		return errors.New("evidence is required")
	}
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO direct_evidence_records (
			chat_session_id, evidence_kind, evidence_text, source_turn_start, source_turn_end,
			turn_anchor, source_message_ids_json, source_hash, archive_state, capture_stage,
			capture_verification, committed_gate, lineage_json, repair_needed, tombstoned,
			superseded_by_id, created_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`, e.ChatSessionID, e.EvidenceKind, e.EvidenceText, e.SourceTurnStart, e.SourceTurnEnd,
		e.TurnAnchor, d1NullableString(e.SourceMessageIDsJSON), d1NullableString(e.SourceHash),
		e.ArchiveState, e.CaptureStage, e.CaptureVerification, d1NullableString(e.CommittedGate),
		d1NullableString(e.LineageJSON), d1BoolValue(e.RepairNeeded), d1BoolValue(e.Tombstoned),
		e.SupersededByID, d1TimeValue(e.CreatedAt)).Scan(&id); err != nil {
		return err
	}
	e.ID = id
	return nil
}

func (s *d1Store) ListEvidence(ctx context.Context, chatSessionID string) ([]DirectEvidence, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, evidence_kind, evidence_text, source_turn_start, source_turn_end,
			turn_anchor, source_message_ids_json, source_hash, archive_state, capture_stage,
			capture_verification, committed_gate, lineage_json, repair_needed, tombstoned,
			superseded_by_id, created_at
		FROM direct_evidence_records
		WHERE chat_session_id = ?
		ORDER BY source_turn_start ASC, id ASC
	`, chatSessionID)
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

// ---------------------------------------------------------------------------
// knowledge graph triples
// ---------------------------------------------------------------------------

func (s *d1Store) SaveKGTriple(ctx context.Context, t *KGTriple) error {
	if t == nil {
		return errors.New("kg triple is required")
	}
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO kg_triples (chat_session_id, subject, predicate, object, valid_from, valid_to, source_turn, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`, t.ChatSessionID, t.Subject, t.Predicate, t.Object, t.ValidFrom, t.ValidTo, t.SourceTurn,
		d1TimeValue(t.CreatedAt)).Scan(&id); err != nil {
		return err
	}
	t.ID = id
	return nil
}

func (s *d1Store) ListKGTriples(ctx context.Context, chatSessionID string) ([]KGTriple, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, subject, predicate, object, valid_from, valid_to, source_turn, created_at
		FROM kg_triples
		WHERE chat_session_id = ?
		ORDER BY id ASC
	`, chatSessionID)
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

// ---------------------------------------------------------------------------
// audit logs
// ---------------------------------------------------------------------------

func (s *d1Store) SaveAuditLog(ctx context.Context, a *AuditLog) error {
	if a == nil {
		return errors.New("audit log is required")
	}
	// An unresolved turn uses -1 in request diagnostics, not a database row ID.
	// The absent target is stored as NULL, matching the MariaDB path.
	var targetID any = a.TargetID
	if a.TargetID < 0 {
		targetID = nil
	}
	_, err := s.conn.Exec(ctx, `
		INSERT INTO audit_logs (created_at, event_type, chat_session_id, target_type, target_id, summary, details_json, source)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, d1TimeValue(a.CreatedAt), a.EventType, d1NullableString(a.ChatSessionID), d1NullableString(a.TargetType),
		targetID, d1NullableString(a.Summary), d1NullableString(a.DetailsJSON), d1NullableString(a.Source))
	return err
}

func (s *d1Store) ListAuditLogs(ctx context.Context, chatSessionID string, eventType string, limit int) ([]AuditLog, error) {
	query := `
		SELECT id, created_at, event_type, chat_session_id, target_type, target_id, summary, details_json, source
		FROM audit_logs
		WHERE (? = '' OR chat_session_id = ?) AND (? = '' OR event_type = ?)
		ORDER BY created_at DESC, id DESC
	`
	args := []any{strings.TrimSpace(chatSessionID), chatSessionID, strings.TrimSpace(eventType), eventType}
	if limit > 0 {
		query += "\nLIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AuditLog
	for rows.Next() {
		var item AuditLog
		var sid, targetType, summary, detailsJSON, source *string
		var targetID *int64
		if err := rows.Scan(&item.ID, &item.CreatedAt, &item.EventType, &sid, &targetType,
			&targetID, &summary, &detailsJSON, &source); err != nil {
			return nil, err
		}
		item.ChatSessionID = d1DerefString(sid)
		item.TargetType = d1DerefString(targetType)
		item.TargetID = d1DerefInt64(targetID)
		item.Summary = d1DerefString(summary)
		item.DetailsJSON = d1DerefString(detailsJSON)
		item.Source = d1DerefString(source)
		out = append(out, item)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// critic feedback
// ---------------------------------------------------------------------------

func (s *d1Store) SaveCriticFeedback(ctx context.Context, f *CriticFeedback) error {
	if f == nil {
		return errors.New("critic feedback is required")
	}
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO critic_feedback (created_at, chat_session_id, target_type, target_id, feedback_value, feedback_note, source)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`, d1TimeValue(f.CreatedAt), f.ChatSessionID, f.TargetType, f.TargetID, f.FeedbackValue,
		d1NullableString(f.FeedbackNote), d1NullableString(f.Source)).Scan(&id); err != nil {
		return err
	}
	f.ID = id
	return nil
}

func (s *d1Store) ListCriticFeedback(ctx context.Context, chatSessionID string, targetType string, targetID int64) ([]CriticFeedback, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, created_at, chat_session_id, target_type, target_id, feedback_value, feedback_note, source
		FROM critic_feedback
		WHERE chat_session_id = ? AND (? = '' OR target_type = ?) AND (? <= 0 OR target_id = ?)
		ORDER BY created_at DESC, id DESC
	`, chatSessionID, strings.TrimSpace(targetType), targetType, targetID, targetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CriticFeedback
	for rows.Next() {
		var item CriticFeedback
		var note, source *string
		if err := rows.Scan(&item.ID, &item.CreatedAt, &item.ChatSessionID, &item.TargetType,
			&item.TargetID, &item.FeedbackValue, &note, &source); err != nil {
			return nil, err
		}
		item.FeedbackNote = d1DerefString(note)
		item.Source = d1DerefString(source)
		out = append(out, item)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// character events
// ---------------------------------------------------------------------------

func (s *d1Store) SaveCharacterEvent(ctx context.Context, e *CharacterEvent) error {
	if e == nil {
		return errors.New("character event is required")
	}
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO character_events (chat_session_id, character_name, turn_index, event_type, details_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		RETURNING id
	`, e.ChatSessionID, e.CharacterName, e.TurnIndex, e.EventType, d1NullableString(e.DetailsJSON),
		d1TimeValue(e.CreatedAt)).Scan(&id); err != nil {
		return err
	}
	e.ID = id
	return nil
}

func (s *d1Store) ListCharacterEvents(ctx context.Context, chatSessionID string, characterName string) ([]CharacterEvent, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, character_name, turn_index, event_type, details_json, created_at
		FROM character_events
		WHERE chat_session_id = ? AND (? = '' OR character_name = ?)
		ORDER BY turn_index ASC, id ASC
	`, chatSessionID, strings.TrimSpace(characterName), characterName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CharacterEvent
	for rows.Next() {
		var item CharacterEvent
		var turnIndex *int64
		var detailsJSON *string
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.CharacterName, &turnIndex,
			&item.EventType, &detailsJSON, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.TurnIndex = int(d1DerefInt64(turnIndex))
		item.DetailsJSON = d1DerefString(detailsJSON)
		out = append(out, item)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// aggregations
// ---------------------------------------------------------------------------

func (s *d1Store) Stats(ctx context.Context) (StatsResult, error) {
	var out StatsResult
	if err := s.conn.QueryRow(ctx, `SELECT COUNT(*) FROM chat_logs`).Scan(&out.ChatLogs); err != nil {
		return StatsResult{}, err
	}
	if err := s.conn.QueryRow(ctx, `SELECT COUNT(*) FROM memories`).Scan(&out.Memories); err != nil {
		return StatsResult{}, err
	}
	if err := s.conn.QueryRow(ctx, `SELECT COUNT(*) FROM kg_triples`).Scan(&out.KgTriples); err != nil {
		return StatsResult{}, err
	}
	return out, nil
}

func (s *d1Store) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	// SQLite has no GREATEST(); the scalar MAX(a, b, c) form is equivalent. The
	// session id set is the union of the three ledgers, and last_activity is the
	// newest timestamp across them. RFC3339 UTC text sorts chronologically, so a
	// text MAX is a correct recency comparison.
	rows, err := s.conn.Query(ctx, `
		SELECT listed_sessions.chat_session_id,
		       listed_sessions.chat_logs_count,
		       listed_sessions.memories_count,
		       listed_sessions.kg_triples_count,
		       listed_sessions.last_activity
		FROM (
			SELECT sid.chat_session_id,
			       COALESCE(cl.chat_logs_count, 0) AS chat_logs_count,
			       COALESCE(mem.memories_count, 0) AS memories_count,
			       COALESCE(kg.kg_triples_count, 0) AS kg_triples_count,
			       NULLIF(MAX(
			           COALESCE(cl.last_activity, ''),
			           COALESCE(mem.last_activity, ''),
			           COALESCE(kg.last_activity, '')
			       ), '') AS last_activity
			FROM (
				SELECT chat_session_id FROM chat_logs
				UNION
				SELECT chat_session_id FROM memories
				UNION
				SELECT chat_session_id FROM kg_triples
			) sid
			LEFT JOIN (
				SELECT chat_session_id, COUNT(*) AS chat_logs_count, MAX(created_at) AS last_activity
				FROM chat_logs GROUP BY chat_session_id
			) cl ON cl.chat_session_id = sid.chat_session_id
			LEFT JOIN (
				SELECT chat_session_id, COUNT(*) AS memories_count, MAX(created_at) AS last_activity
				FROM memories GROUP BY chat_session_id
			) mem ON mem.chat_session_id = sid.chat_session_id
			LEFT JOIN (
				SELECT chat_session_id, COUNT(*) AS kg_triples_count, MAX(created_at) AS last_activity
				FROM kg_triples GROUP BY chat_session_id
			) kg ON kg.chat_session_id = sid.chat_session_id
		) listed_sessions
		ORDER BY listed_sessions.last_activity IS NULL ASC, listed_sessions.last_activity DESC, listed_sessions.chat_session_id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SessionSummary
	for rows.Next() {
		var item SessionSummary
		var lastActivity *string
		if err := rows.Scan(&item.ChatSessionID, &item.ChatLogsCount, &item.MemoriesCount,
			&item.KGTriplesCount, &lastActivity); err != nil {
			return nil, err
		}
		if lastActivity != nil {
			parsed, err := parseD1Time(*lastActivity)
			if err != nil {
				return nil, err
			}
			item.LastActivity = parsed
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// pending threads
// ---------------------------------------------------------------------------

func (s *d1Store) ListPendingThreads(ctx context.Context, chatSessionID, status string) ([]PendingThread, error) {
	// Status filtering mirrors the MariaDB path: an explicit status selects it,
	// "all" selects everything, and an empty status defaults to the open lanes.
	query := `
		SELECT id, chat_session_id, thread_key, description, status, created_turn, resolved_turn,
			   source_turn, priority, hook_type, hook_metadata_json, pinned, suppressed, user_corrected,
			   created_at, updated_at
		FROM pending_threads
		WHERE chat_session_id = ?
	`
	args := []any{chatSessionID}
	switch {
	case strings.TrimSpace(status) != "" && status != "all":
		query += ` AND status = ?`
		args = append(args, status)
	case status == "":
		query += ` AND status IN ('open', 'paused')`
	}
	query += ` ORDER BY pinned DESC, source_turn DESC, id DESC`

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PendingThread
	for rows.Next() {
		var item PendingThread
		var description, hookType, hookMetadataJSON *string
		var createdTurn, resolvedTurn, sourceTurn, priority *int64
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.ThreadKey, &description, &item.Status,
			&createdTurn, &resolvedTurn, &sourceTurn, &priority, &hookType, &hookMetadataJSON,
			&item.Pinned, &item.Suppressed, &item.UserCorrected, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.Description = d1DerefString(description)
		item.CreatedTurn = int(d1DerefInt64(createdTurn))
		item.ResolvedTurn = int(d1DerefInt64(resolvedTurn))
		item.SourceTurn = int(d1DerefInt64(sourceTurn))
		item.Priority = int(d1DerefInt64(priority))
		item.HookType = d1DerefString(hookType)
		item.HookMetadataJSON = d1DerefString(hookMetadataJSON)
		hydratePendingThreadDerivedFields(&item)
		out = append(out, item)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// active states and canonical state layers
// ---------------------------------------------------------------------------

func (s *d1Store) ListActiveStates(ctx context.Context, chatSessionID, stateType string) ([]ActiveState, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, state_type, content, turn_index, created_at
		FROM active_states
		WHERE chat_session_id = ? AND (? = '' OR state_type = ?)
		ORDER BY turn_index DESC, id DESC
	`, chatSessionID, strings.TrimSpace(stateType), stateType)
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

func (s *d1Store) ListCanonicalStateLayers(ctx context.Context, chatSessionID, layerType string) ([]CanonicalStateLayer, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, layer_type, content, source_state_type, turn_index, source_turn,
			   source_record, last_verified_turn, confidence, created_at
		FROM canonical_state_layers
		WHERE chat_session_id = ? AND (? = '' OR layer_type = ?)
		ORDER BY turn_index DESC, id DESC
	`, chatSessionID, strings.TrimSpace(layerType), layerType)
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
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.LayerType, &item.Content,
			&sourceStateType, &item.TurnIndex, &sourceTurn, &sourceRecord, &lastVerifiedTurn,
			&confidence, &item.CreatedAt); err != nil {
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

// ---------------------------------------------------------------------------
// episode summaries
// ---------------------------------------------------------------------------

func (s *d1Store) ListEpisodeSummaries(ctx context.Context, chatSessionID string, limit, fromTurn, toTurn int) ([]EpisodeSummary, error) {
	query := `
		SELECT id, chat_session_id, from_turn, to_turn, summary_text, key_entities, key_events,
			   open_loops_json, relationship_changes_json, embedding_vector, embedding_model, created_at
		FROM episode_summaries
		WHERE chat_session_id = ? AND (? <= 0 OR from_turn >= ?) AND (? <= 0 OR to_turn <= ?)
		ORDER BY to_turn DESC, id DESC
	`
	args := []any{chatSessionID, fromTurn, fromTurn, toTurn, toTurn}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EpisodeSummary
	for rows.Next() {
		item, err := d1ScanEpisodeSummary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) GetEpisodeSummary(ctx context.Context, episodeID int64) (*EpisodeSummary, error) {
	var item EpisodeSummary
	var keyEntities, keyEvents, openLoopsJSON, relationshipChangesJSON, embeddingVector, embeddingModel *string
	err := s.conn.QueryRow(ctx, `
		SELECT id, chat_session_id, from_turn, to_turn, summary_text, key_entities, key_events,
			   open_loops_json, relationship_changes_json, embedding_vector, embedding_model, created_at
		FROM episode_summaries
		WHERE id = ?
	`, episodeID).Scan(&item.ID, &item.ChatSessionID, &item.FromTurn, &item.ToTurn, &item.SummaryText,
		&keyEntities, &keyEvents, &openLoopsJSON, &relationshipChangesJSON, &embeddingVector, &embeddingModel,
		&item.CreatedAt)
	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	d1ApplyEpisodeSummaryNullables(&item, keyEntities, keyEvents, openLoopsJSON, relationshipChangesJSON, embeddingVector, embeddingModel)
	return &item, nil
}

// d1ScanEpisodeSummary reads one episode_summaries row from a cursor.
func d1ScanEpisodeSummary(rows D1Rows) (EpisodeSummary, error) {
	var item EpisodeSummary
	var keyEntities, keyEvents, openLoopsJSON, relationshipChangesJSON, embeddingVector, embeddingModel *string
	if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.FromTurn, &item.ToTurn, &item.SummaryText,
		&keyEntities, &keyEvents, &openLoopsJSON, &relationshipChangesJSON, &embeddingVector, &embeddingModel,
		&item.CreatedAt); err != nil {
		return EpisodeSummary{}, err
	}
	d1ApplyEpisodeSummaryNullables(&item, keyEntities, keyEvents, openLoopsJSON, relationshipChangesJSON, embeddingVector, embeddingModel)
	return item, nil
}

func d1ApplyEpisodeSummaryNullables(item *EpisodeSummary, keyEntities, keyEvents, openLoopsJSON, relationshipChangesJSON, embeddingVector, embeddingModel *string) {
	item.KeyEntities = d1DerefString(keyEntities)
	item.KeyEvents = d1DerefString(keyEvents)
	item.OpenLoopsJSON = d1DerefString(openLoopsJSON)
	item.RelationshipChangesJSON = d1DerefString(relationshipChangesJSON)
	item.EmbeddingVector = d1DerefString(embeddingVector)
	item.EmbeddingModel = d1DerefString(embeddingModel)
}

// ---------------------------------------------------------------------------
// storylines and world rules
// ---------------------------------------------------------------------------

func (s *d1Store) ListStorylines(ctx context.Context, chatSessionID string) ([]Storyline, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, chat_session_id, name, status, entities_json, current_context, key_points_json,
			   ongoing_tensions_json, confidence, evidence_count, last_evidence_turn, first_turn, last_turn,
			   pinned, suppressed, user_corrected, created_at, updated_at
		FROM storylines
		WHERE chat_session_id = ?
		ORDER BY last_turn DESC, id DESC
	`, chatSessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Storyline
	for rows.Next() {
		var item Storyline
		var entitiesJSON, currentContext, keyPointsJSON, ongoingTensionsJSON *string
		var confidence *float64
		var evidenceCount, lastEvidenceTurn, firstTurn, lastTurn *int64
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.Name, &item.Status,
			&entitiesJSON, &currentContext, &keyPointsJSON, &ongoingTensionsJSON,
			&confidence, &evidenceCount, &lastEvidenceTurn, &firstTurn, &lastTurn,
			&item.Pinned, &item.Suppressed, &item.UserCorrected, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.EntitiesJSON = d1DerefString(entitiesJSON)
		item.CurrentContext = d1DerefString(currentContext)
		item.KeyPointsJSON = d1DerefString(keyPointsJSON)
		item.OngoingTensionsJSON = d1DerefString(ongoingTensionsJSON)
		item.Confidence = d1DerefFloat64(confidence)
		item.EvidenceCount = int(d1DerefInt64(evidenceCount))
		item.LastEvidenceTurn = int(d1DerefInt64(lastEvidenceTurn))
		item.FirstTurn = int(d1DerefInt64(firstTurn))
		item.LastTurn = int(d1DerefInt64(lastTurn))
		out = append(out, item)
	}
	return out, rows.Err()
}

// d1LatestWorldRulePredicate mirrors the MariaDB "newest row per
// (scope, key, scope_name)" correlated subquery. MariaDB's <=> null-safe equality
// becomes SQLite's IS operator, which is also null-safe: two NULLs compare equal
// and a NULL never equals a non-NULL.
const d1LatestWorldRulePredicate = `
		current_rule.id = (
			SELECT candidate.id
			FROM world_rules AS candidate
			WHERE candidate.chat_session_id = current_rule.chat_session_id
			  AND candidate.scope = current_rule.scope
			  AND candidate."key" = current_rule."key"
			  AND candidate.scope_name IS current_rule.scope_name
			ORDER BY COALESCE(candidate.source_turn, 0) DESC, candidate.id DESC
			LIMIT 1
		)`

const d1WorldRuleSelect = `
		SELECT id, chat_session_id, scope, scope_name, category, "key", value_json, genre, source_turn,
			   pinned, suppressed, user_corrected, created_at, updated_at
		FROM world_rules AS current_rule`

func (s *d1Store) ListWorldRules(ctx context.Context, chatSessionID string) ([]WorldRule, error) {
	rows, err := s.conn.Query(ctx, d1WorldRuleSelect+`
		WHERE current_rule.chat_session_id = ?
		  AND `+d1LatestWorldRulePredicate+`
		ORDER BY current_rule.scope, current_rule.category, current_rule."key"
	`, chatSessionID)
	if err != nil {
		return nil, err
	}
	return d1ScanWorldRules(rows)
}

func (s *d1Store) ListInheritedWorldRules(ctx context.Context, chatSessionID string, activeScope, scopeName string) ([]WorldRule, error) {
	activeScope = strings.TrimSpace(activeScope)
	scopeName = strings.TrimSpace(scopeName)
	if activeScope == "" {
		// Fall back to the persisted active scope, exactly as the MariaDB path
		// does, and keep an explicit scope name when one was supplied.
		saved, err := s.GetActiveScope(ctx, chatSessionID)
		switch {
		case err == nil && saved != nil:
			activeScope = strings.TrimSpace(saved.ActiveScope)
			if scopeName == "" {
				scopeName = strings.TrimSpace(saved.ScopeName)
			}
		case err != nil && !errors.Is(err, ErrNotFound):
			return nil, err
		}
	}
	if activeScope == "" {
		activeScope = "root"
	}

	// MariaDB relies on the driver's boolean handling; SQLite stores the flags as
	// INTEGER, so the suppressed filter compares against 0.
	rows, err := s.conn.Query(ctx, d1WorldRuleSelect+`
		WHERE current_rule.chat_session_id = ?
		  AND `+d1LatestWorldRulePredicate+`
		  AND current_rule.suppressed = 0
		ORDER BY current_rule.scope, current_rule.category, current_rule."key"
	`, chatSessionID)
	if err != nil {
		return nil, err
	}
	candidates, err := d1ScanWorldRules(rows)
	if err != nil {
		return nil, err
	}

	// Reduce to the active scope chain, then apply the same precedence the
	// MariaDB path uses: proximity in the chain, pinned rules first, then
	// category, key, and id.
	chain := WorldRuleScopeChain(activeScope)
	chainOrder := make(map[string]int, len(chain))
	for i, scope := range chain {
		chainOrder[scope] = i
	}
	normalizedActive := NormalizeWorldRuleScope(activeScope)
	out := make([]WorldRule, 0, len(candidates))
	for _, item := range candidates {
		itemScope := NormalizeWorldRuleScope(item.Scope)
		if _, ok := chainOrder[itemScope]; !ok {
			continue
		}
		if itemScope == normalizedActive {
			// Only the active scope is narrowed by scope name: an explicit name
			// selects that name exactly, and no name selects the unnamed rules.
			if scopeName != "" && strings.TrimSpace(item.ScopeName) != scopeName {
				continue
			}
			if scopeName == "" && strings.TrimSpace(item.ScopeName) != "" {
				continue
			}
		}
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool {
		left := chainOrder[NormalizeWorldRuleScope(out[i].Scope)]
		right := chainOrder[NormalizeWorldRuleScope(out[j].Scope)]
		if left != right {
			return left < right
		}
		if out[i].Pinned != out[j].Pinned {
			return out[i].Pinned
		}
		if out[i].Category != out[j].Category {
			return out[i].Category < out[j].Category
		}
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func d1ScanWorldRules(rows D1Rows) ([]WorldRule, error) {
	defer rows.Close()
	var out []WorldRule
	for rows.Next() {
		var item WorldRule
		var scopeNameNull, genre, valueJSON *string
		var sourceTurn *int64
		if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.Scope, &scopeNameNull, &item.Category, &item.Key,
			&valueJSON, &genre, &sourceTurn, &item.Pinned, &item.Suppressed, &item.UserCorrected,
			&item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.ScopeName = d1DerefString(scopeNameNull)
		item.ValueJSON = d1DerefString(valueJSON)
		item.Genre = d1DerefString(genre)
		item.SourceTurn = int(d1DerefInt64(sourceTurn))
		out = append(out, item)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// active scope (ActiveScopeStore)
// ---------------------------------------------------------------------------

func (s *d1Store) GetActiveScope(ctx context.Context, chatSessionID string) (*SessionActiveScope, error) {
	var item SessionActiveScope
	var scopeName *string
	err := s.conn.QueryRow(ctx, `
		SELECT id, chat_session_id, active_scope, scope_name, updated_at
		FROM session_active_scopes
		WHERE chat_session_id = ?
		LIMIT 1
	`, chatSessionID).Scan(&item.ID, &item.ChatSessionID, &item.ActiveScope, &scopeName, &item.UpdatedAt)
	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	item.ScopeName = d1DerefString(scopeName)
	return &item, nil
}

func (s *d1Store) UpsertActiveScope(ctx context.Context, item *SessionActiveScope) error {
	if item == nil || strings.TrimSpace(item.ChatSessionID) == "" {
		return ErrNotFound
	}
	activeScope := strings.TrimSpace(item.ActiveScope)
	if activeScope == "" {
		activeScope = "root"
	}
	updatedAt := d1TimeValue(item.UpdatedAt)
	// MariaDB's ON DUPLICATE KEY UPDATE becomes SQLite's ON CONFLICT upsert on the
	// session_active_scopes unique key.
	_, err := s.conn.Exec(ctx, `
		INSERT INTO session_active_scopes (chat_session_id, active_scope, scope_name, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (chat_session_id) DO UPDATE SET
			active_scope = excluded.active_scope,
			scope_name = excluded.scope_name,
			updated_at = excluded.updated_at
	`, item.ChatSessionID, activeScope, d1NullableString(item.ScopeName), updatedAt)
	return err
}

// ---------------------------------------------------------------------------
// character states
// ---------------------------------------------------------------------------

const d1CharacterStateSelect = `
		SELECT id, chat_session_id, character_name, appearance_json, personality_json, status_json,
			   relationships_json, speech_style_json, field_provenance_json, turn_index, created_at, updated_at
		FROM character_states`

func d1ScanCharacterState(rows D1Rows) (CharacterState, error) {
	var item CharacterState
	var appearanceJSON, personalityJSON, statusJSON, relationshipsJSON, speechStyleJSON, fieldProvenanceJSON *string
	var turnIndex *int64
	if err := rows.Scan(&item.ID, &item.ChatSessionID, &item.CharacterName,
		&appearanceJSON, &personalityJSON, &statusJSON, &relationshipsJSON, &speechStyleJSON, &fieldProvenanceJSON,
		&turnIndex, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return CharacterState{}, err
	}
	item.AppearanceJSON = d1DerefString(appearanceJSON)
	item.PersonalityJSON = d1DerefString(personalityJSON)
	item.StatusJSON = d1DerefString(statusJSON)
	item.RelationshipsJSON = d1DerefString(relationshipsJSON)
	item.SpeechStyleJSON = d1DerefString(speechStyleJSON)
	item.FieldProvenanceJSON = d1DerefString(fieldProvenanceJSON)
	item.TurnIndex = int(d1DerefInt64(turnIndex))
	return item, nil
}

func (s *d1Store) ListCharacterStates(ctx context.Context, chatSessionID string) ([]CharacterState, error) {
	// Ordered newest first so the first row seen for a character is its current
	// snapshot; later rows for the same character are skipped.
	rows, err := s.conn.Query(ctx, d1CharacterStateSelect+`
		WHERE chat_session_id = ?
		ORDER BY turn_index DESC, id DESC
	`, chatSessionID)
	if err != nil {
		return nil, err
	}

	var out []CharacterState
	seen := map[string]bool{}
	for rows.Next() {
		item, err := d1ScanCharacterState(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		key := strings.ToLower(strings.TrimSpace(item.CharacterName))
		if key == "" {
			key = strconv.FormatInt(item.ID, 10)
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	return d1ApplyCharacterManualEdits(ctx, s.conn, chatSessionID, "", out)
}

func (s *d1Store) GetCharacterState(ctx context.Context, chatSessionID, characterName string) (*CharacterState, error) {
	rows, err := s.conn.Query(ctx, d1CharacterStateSelect+`
		WHERE chat_session_id = ? AND character_name = ?
		ORDER BY turn_index DESC, id DESC
		LIMIT 1
	`, chatSessionID, characterName)
	if err != nil {
		return nil, err
	}
	var states []CharacterState
	if rows.Next() {
		item, err := d1ScanCharacterState(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		states = append(states, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	states, err = d1ApplyCharacterManualEdits(ctx, s.conn, chatSessionID, characterName, states)
	if err != nil {
		return nil, err
	}
	if len(states) == 0 {
		return nil, ErrNotFound
	}
	return &states[0], nil
}

// d1ApplyCharacterManualEdits overlays the latest cumulative operator override
// per character on the supplied snapshots.
//
// It mirrors mariaApplyCharacterManualEdits and reuses the same pure applier, so
// both providers produce an identical overlay. The durable edits live in
// character_events rather than in the derived turn snapshots, so the overlay is
// applied on read. A NULL details_json carries no edits and is treated as such.
func d1ApplyCharacterManualEdits(ctx context.Context, conn D1Conn, sid, name string, states []CharacterState) ([]CharacterState, error) {
	rows, err := conn.Query(ctx, `
		SELECT e.character_name, e.details_json FROM character_events e
		WHERE e.chat_session_id = ? AND (? = '' OR e.character_name = ?)
		AND e.event_type = 'manual_character_override'
		AND e.id = (SELECT MAX(latest.id) FROM character_events latest
			WHERE latest.chat_session_id = e.chat_session_id AND latest.character_name = e.character_name
			AND latest.event_type = 'manual_character_override') ORDER BY e.id`, sid, name, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var character string
		var raw *string
		if err := rows.Scan(&character, &raw); err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		var record struct {
			Edits []CharacterManualFieldEdit `json:"edits"`
		}
		if err := json.Unmarshal([]byte(*raw), &record); err != nil {
			return nil, err
		}
		if len(record.Edits) == 0 {
			continue
		}
		index := -1
		for i := range states {
			if strings.EqualFold(states[i].CharacterName, character) {
				index = i
				break
			}
		}
		if index < 0 {
			states = append(states, CharacterState{ChatSessionID: sid, CharacterName: character})
			index = len(states) - 1
		}
		applyCharacterManualEdits(&states[index], record.Edits)
	}
	return states, rows.Err()
}

// ---------------------------------------------------------------------------
// resume pack
// ---------------------------------------------------------------------------

func (s *d1Store) GetResumePack(ctx context.Context, chatSessionID string, trigger string) (*ResumePack, error) {
	chapter, err := s.d1LatestChapterSummary(ctx, chatSessionID)
	if err != nil {
		return nil, err
	}
	arc, err := s.d1LatestArcSummary(ctx, chatSessionID)
	if err != nil {
		return nil, err
	}
	saga, err := s.d1LatestSagaDigest(ctx, chatSessionID)
	if err != nil {
		return nil, err
	}

	// Assembly order and text prefixes mirror the MariaDB path exactly, so a
	// resumed session reads identically on either provider.
	sources := []string{}
	parts := []string{}
	if saga != nil {
		sources = append(sources, "saga_digests")
		if saga.ResumePackText != "" {
			parts = append(parts, "Saga: "+saga.ResumePackText)
		} else if saga.SagaSummary != "" {
			parts = append(parts, "Saga: "+saga.SagaSummary)
		}
	}
	if arc != nil {
		sources = append(sources, "arc_summaries")
		if arc.ArcResumeText != "" {
			parts = append(parts, "Arc: "+arc.ArcResumeText)
		} else if arc.CoreConflict != "" {
			parts = append(parts, "Arc: "+arc.CoreConflict)
		}
	}
	if chapter != nil {
		sources = append(sources, "chapter_summaries")
		if chapter.ChapterTitle != "" {
			parts = append(parts, "Chapter: "+chapter.ChapterTitle)
		}
		if chapter.ResumeText != "" {
			parts = append(parts, "Resume: "+chapter.ResumeText)
		}
		if chapter.SummaryText != "" {
			parts = append(parts, "Summary: "+chapter.SummaryText)
		}
	}
	if len(sources) == 0 {
		return &ResumePack{
			PackStatus:    "empty",
			Trigger:       trigger,
			SourcesUsed:   []string{},
			LayerCount:    0,
			AssembledText: "",
			AssemblyNote:  "no hierarchy rows found",
		}, nil
	}
	return &ResumePack{
		PackStatus:    "ready",
		Trigger:       trigger,
		SourcesUsed:   sources,
		LayerCount:    len(sources),
		AssembledText: strings.Join(parts, "\n"),
		Saga:          saga,
		Arc:           arc,
		Chapter:       chapter,
		AssemblyNote:  "assembled from latest saga/arc/chapter rows",
	}, nil
}

// d1LatestChapterSummary reads the newest chapter summary, or nil when the
// session has none. Absence is not an error: an empty pack is a valid state.
func (s *d1Store) d1LatestChapterSummary(ctx context.Context, chatSessionID string) (*ChapterSummary, error) {
	var item ChapterSummary
	var chapterIndex *int64
	var chapterTitle, summaryText, openLoopsJSON, relationshipChangesJSON, worldChangesJSON *string
	var callbackCandidatesJSON, resumeText, embeddingVector, embeddingModel *string
	err := s.conn.QueryRow(ctx, `
		SELECT id, chat_session_id, from_turn, to_turn, chapter_index, chapter_title, summary_text,
		       open_loops_json, relationship_changes_json, world_changes_json, callback_candidates_json,
		       resume_text, embedding_vector, embedding_model, created_at
		FROM chapter_summaries
		WHERE chat_session_id = ?
		ORDER BY chapter_index DESC, id DESC
		LIMIT 1
	`, chatSessionID).Scan(
		&item.ID, &item.ChatSessionID, &item.FromTurn, &item.ToTurn, &chapterIndex, &chapterTitle, &summaryText,
		&openLoopsJSON, &relationshipChangesJSON, &worldChangesJSON, &callbackCandidatesJSON,
		&resumeText, &embeddingVector, &embeddingModel, &item.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, errD1NoRows) || errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	item.ChapterIndex = int(d1DerefInt64(chapterIndex))
	item.ChapterTitle = d1DerefString(chapterTitle)
	item.SummaryText = d1DerefString(summaryText)
	item.OpenLoopsJSON = d1DerefString(openLoopsJSON)
	item.RelationshipChangesJSON = d1DerefString(relationshipChangesJSON)
	item.WorldChangesJSON = d1DerefString(worldChangesJSON)
	item.CallbackCandidatesJSON = d1DerefString(callbackCandidatesJSON)
	item.ResumeText = d1DerefString(resumeText)
	item.EmbeddingVector = d1DerefString(embeddingVector)
	item.EmbeddingModel = d1DerefString(embeddingModel)
	return &item, nil
}

func (s *d1Store) d1LatestArcSummary(ctx context.Context, chatSessionID string) (*ArcSummary, error) {
	var item ArcSummary
	var arcIndex *int64
	var arcName, arcStatus, coreConflict, keyTurningPointsJSON, activePromisesJSON *string
	var unresolvedDebtsJSON, resolvedPayoffsJSON, callbackCandidatesJSON, futurePayoffCandidatesJSON *string
	var irreversibleTurnsJSON, callbackDebtsJSON, relationshipPivotsJSON, arcResumeText *string
	var embeddingVector, embeddingModel *string
	err := s.conn.QueryRow(ctx, `
		SELECT id, chat_session_id, from_turn, to_turn, arc_index, arc_name, arc_status, core_conflict,
		       key_turning_points_json, active_promises_json, unresolved_debts_json, resolved_payoffs_json,
		       callback_candidates_json, future_payoff_candidates_json, irreversible_turns_json, callback_debts_json,
		       relationship_pivots_json, arc_resume_text, embedding_vector, embedding_model, created_at
		FROM arc_summaries
		WHERE chat_session_id = ?
		ORDER BY arc_index DESC, id DESC
		LIMIT 1
	`, chatSessionID).Scan(
		&item.ID, &item.ChatSessionID, &item.FromTurn, &item.ToTurn, &arcIndex, &arcName, &arcStatus, &coreConflict,
		&keyTurningPointsJSON, &activePromisesJSON, &unresolvedDebtsJSON, &resolvedPayoffsJSON,
		&callbackCandidatesJSON, &futurePayoffCandidatesJSON, &irreversibleTurnsJSON, &callbackDebtsJSON,
		&relationshipPivotsJSON, &arcResumeText, &embeddingVector, &embeddingModel, &item.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, errD1NoRows) || errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	item.ArcIndex = int(d1DerefInt64(arcIndex))
	item.ArcName = d1DerefString(arcName)
	item.ArcStatus = d1DerefString(arcStatus)
	item.CoreConflict = d1DerefString(coreConflict)
	item.KeyTurningPointsJSON = d1DerefString(keyTurningPointsJSON)
	item.ActivePromisesJSON = d1DerefString(activePromisesJSON)
	item.UnresolvedDebtsJSON = d1DerefString(unresolvedDebtsJSON)
	item.ResolvedPayoffsJSON = d1DerefString(resolvedPayoffsJSON)
	item.CallbackCandidatesJSON = d1DerefString(callbackCandidatesJSON)
	item.FuturePayoffCandidatesJSON = d1DerefString(futurePayoffCandidatesJSON)
	item.IrreversibleTurnsJSON = d1DerefString(irreversibleTurnsJSON)
	item.CallbackDebtsJSON = d1DerefString(callbackDebtsJSON)
	item.RelationshipPivotsJSON = d1DerefString(relationshipPivotsJSON)
	item.ArcResumeText = d1DerefString(arcResumeText)
	item.EmbeddingVector = d1DerefString(embeddingVector)
	item.EmbeddingModel = d1DerefString(embeddingModel)
	return &item, nil
}

func (s *d1Store) d1LatestSagaDigest(ctx context.Context, chatSessionID string) (*SagaDigest, error) {
	var item SagaDigest
	var eraLabel, sagaSummary, persistentFactsJSON, neverDropCandidatesJSON, resumePackText *string
	var embeddingVector, embeddingModel *string
	err := s.conn.QueryRow(ctx, `
		SELECT id, chat_session_id, from_turn, to_turn, era_label, saga_summary,
		       persistent_facts_json, never_drop_candidates_json, resume_pack_text,
		       embedding_vector, embedding_model, created_at
		FROM saga_digests
		WHERE chat_session_id = ?
		ORDER BY to_turn DESC, id DESC
		LIMIT 1
	`, chatSessionID).Scan(
		&item.ID, &item.ChatSessionID, &item.FromTurn, &item.ToTurn, &eraLabel, &sagaSummary,
		&persistentFactsJSON, &neverDropCandidatesJSON, &resumePackText,
		&embeddingVector, &embeddingModel, &item.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, errD1NoRows) || errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	item.EraLabel = d1DerefString(eraLabel)
	item.SagaSummary = d1DerefString(sagaSummary)
	item.PersistentFactsJSON = d1DerefString(persistentFactsJSON)
	item.NeverDropCandidatesJSON = d1DerefString(neverDropCandidatesJSON)
	item.ResumePackText = d1DerefString(resumePackText)
	item.EmbeddingVector = d1DerefString(embeddingVector)
	item.EmbeddingModel = d1DerefString(embeddingModel)
	return &item, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// d1NullableString maps an empty string to NULL so optional text columns keep
// the same representation the MariaDB path uses.
func d1NullableString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

// d1DerefString renders a NULL text column as the empty string, matching the
// MariaDB scan behaviour for nullable text.
func d1DerefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

// d1BoolValue renders a Go bool as the SQLite integer the canonical schema uses
// for boolean columns.
func d1BoolValue(value bool) int {
	if value {
		return 1
	}
	return 0
}

// d1DerefInt64 renders a NULL integer column as 0, matching how the MariaDB path
// reads nullable integer columns into plain int fields.
func d1DerefInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// d1DerefFloat64 renders a NULL real column as 0, matching how the MariaDB path
// reads nullable float columns into plain float64 fields.
func d1DerefFloat64(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}
