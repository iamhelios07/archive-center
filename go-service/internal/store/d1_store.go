package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
// The append-only ledger (chat logs, effective inputs, memories) is implemented
// and tested here. Every remaining canonical method returns
// errD1Unimplemented rather than silently succeeding, so an unfinished path
// fails loudly instead of pretending to persist.

// errD1Unimplemented marks canonical methods that this layer does not implement
// yet. It must never be treated as a successful write.
var errD1Unimplemented = errors.New("store: d1 method not implemented in this layer")

// d1Store implements Store over a D1 transport.
type d1Store struct {
	conn D1Conn
}

var _ Store = (*d1Store)(nil)

// NewD1Store returns the D1 canonical store over the given transport. The
// returned value satisfies Store; unimplemented methods return a loud error.
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
// not yet implemented
// ---------------------------------------------------------------------------

func (s *d1Store) SaveEvidence(context.Context, *DirectEvidence) error {
	return errD1Unimplemented
}

func (s *d1Store) ListEvidence(context.Context, string) ([]DirectEvidence, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) SaveKGTriple(context.Context, *KGTriple) error {
	return errD1Unimplemented
}

func (s *d1Store) ListKGTriples(context.Context, string) ([]KGTriple, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) SaveAuditLog(context.Context, *AuditLog) error {
	return errD1Unimplemented
}

func (s *d1Store) ListAuditLogs(context.Context, string, string, int) ([]AuditLog, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) SaveCriticFeedback(context.Context, *CriticFeedback) error {
	return errD1Unimplemented
}

func (s *d1Store) ListCriticFeedback(context.Context, string, string, int64) ([]CriticFeedback, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) SaveCharacterEvent(context.Context, *CharacterEvent) error {
	return errD1Unimplemented
}

func (s *d1Store) ListCharacterEvents(context.Context, string, string) ([]CharacterEvent, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) Stats(context.Context) (StatsResult, error) {
	return StatsResult{}, errD1Unimplemented
}

func (s *d1Store) ListSessions(context.Context) ([]SessionSummary, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) GetResumePack(context.Context, string, string) (*ResumePack, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) ListStorylines(context.Context, string) ([]Storyline, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) ListWorldRules(context.Context, string) ([]WorldRule, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) ListInheritedWorldRules(context.Context, string, string, string) ([]WorldRule, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) ListCharacterStates(context.Context, string) ([]CharacterState, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) GetCharacterState(context.Context, string, string) (*CharacterState, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) ListPendingThreads(context.Context, string, string) ([]PendingThread, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) ListActiveStates(context.Context, string, string) ([]ActiveState, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) ListCanonicalStateLayers(context.Context, string, string) ([]CanonicalStateLayer, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) ListEpisodeSummaries(context.Context, string, int, int, int) ([]EpisodeSummary, error) {
	return nil, errD1Unimplemented
}

func (s *d1Store) GetEpisodeSummary(context.Context, int64) (*EpisodeSummary, error) {
	return nil, errD1Unimplemented
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
