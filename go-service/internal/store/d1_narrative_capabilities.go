package store

import (
	"context"
	"fmt"
	"strings"
)

// D1 narrative hierarchy capabilities.
//
// The HTTP layer asserts separate chapter, arc, saga, and episode capabilities,
// so each is declared explicitly rather than inferred from the base Store.
//
// Search uses LIKE, as the MariaDB path does. Note one deliberate, documented
// difference: MariaDB compares under a case-insensitive Unicode collation, while
// SQLite's LIKE folds case for ASCII only. Matching behaviour for non-ASCII text
// therefore requires an exact case; the shared test suite pins the ASCII paths.

var _ ChapterSummaryStore = (*d1Store)(nil)
var _ ArcSummaryStore = (*d1Store)(nil)
var _ SagaDigestStore = (*d1Store)(nil)
var _ EpisodeSummaryStore = (*d1Store)(nil)

// ---------------------------------------------------------------------------
// chapter summaries
// ---------------------------------------------------------------------------

const d1ChapterSummarySelect = `
		SELECT id, chat_session_id, from_turn, to_turn, chapter_index, chapter_title, summary_text,
		       open_loops_json, relationship_changes_json, world_changes_json, callback_candidates_json,
		       resume_text, embedding_vector, embedding_model, created_at
		FROM chapter_summaries`

func d1ScanChapterSummary(rows D1Rows) (ChapterSummary, error) {
	var item ChapterSummary
	var chapterIndex *int64
	var chapterTitle, summaryText, openLoopsJSON, relationshipChangesJSON, worldChangesJSON *string
	var callbackCandidatesJSON, resumeText, embeddingVector, embeddingModel *string
	if err := rows.Scan(
		&item.ID, &item.ChatSessionID, &item.FromTurn, &item.ToTurn, &chapterIndex, &chapterTitle, &summaryText,
		&openLoopsJSON, &relationshipChangesJSON, &worldChangesJSON, &callbackCandidatesJSON,
		&resumeText, &embeddingVector, &embeddingModel, &item.CreatedAt,
	); err != nil {
		return ChapterSummary{}, err
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
	return item, nil
}

func (s *d1Store) SaveChapterSummary(ctx context.Context, item *ChapterSummary) error {
	if item == nil {
		return fmt.Errorf("store: chapter summary is required")
	}
	// created_at is deliberately omitted, matching the MariaDB path: the column
	// default supplies it, so a caller-provided timestamp is not persisted for
	// narrative summaries. The D1 schema default emits the same RFC3339 UTC
	// millisecond form the store writes explicitly elsewhere.
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO chapter_summaries (
			chat_session_id, from_turn, to_turn, chapter_index, chapter_title, summary_text,
			open_loops_json, relationship_changes_json, world_changes_json, callback_candidates_json,
			resume_text, embedding_vector, embedding_model
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		item.ChatSessionID, item.FromTurn, item.ToTurn, item.ChapterIndex, d1NullableString(item.ChapterTitle),
		item.SummaryText, d1NullableString(item.OpenLoopsJSON), d1NullableString(item.RelationshipChangesJSON),
		d1NullableString(item.WorldChangesJSON), d1NullableString(item.CallbackCandidatesJSON),
		d1NullableString(item.ResumeText), d1NullableString(item.EmbeddingVector),
		d1NullableString(item.EmbeddingModel)).Scan(&id); err != nil {
		return err
	}
	item.ID = id
	return nil
}

func (s *d1Store) SearchChapterSummaries(ctx context.Context, chatSessionID, query string, fromTurn, toTurn, limit int) ([]ChapterSummary, error) {
	trimmed := strings.TrimSpace(query)
	like := "%" + trimmed + "%"
	sql := d1ChapterSummarySelect + `
		WHERE chat_session_id = ?
		  AND (? = '' OR chapter_title LIKE ? OR summary_text LIKE ? OR resume_text LIKE ?
		       OR open_loops_json LIKE ? OR relationship_changes_json LIKE ? OR world_changes_json LIKE ?
		       OR callback_candidates_json LIKE ?)
		  AND (? = 0 OR to_turn >= ?)
		  AND (? = 0 OR from_turn <= ?)
		ORDER BY chapter_index DESC, id DESC`
	args := []any{chatSessionID, trimmed, like, like, like, like, like, like, like, fromTurn, fromTurn, toTurn, toTurn}
	if limit > 0 {
		sql += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ChapterSummary{}
	for rows.Next() {
		item, err := d1ScanChapterSummary(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ---------------------------------------------------------------------------
// arc summaries
// ---------------------------------------------------------------------------

const d1ArcSummarySelect = `
		SELECT id, chat_session_id, from_turn, to_turn, arc_index, arc_name, arc_status, core_conflict,
		       key_turning_points_json, active_promises_json, unresolved_debts_json, resolved_payoffs_json,
		       callback_candidates_json, future_payoff_candidates_json, irreversible_turns_json, callback_debts_json,
		       relationship_pivots_json, arc_resume_text, embedding_vector, embedding_model, created_at
		FROM arc_summaries`

func d1ScanArcSummary(rows D1Rows) (ArcSummary, error) {
	var item ArcSummary
	var arcIndex *int64
	var arcName, arcStatus, coreConflict, keyTurningPointsJSON, activePromisesJSON *string
	var unresolvedDebtsJSON, resolvedPayoffsJSON, callbackCandidatesJSON, futurePayoffCandidatesJSON *string
	var irreversibleTurnsJSON, callbackDebtsJSON, relationshipPivotsJSON, arcResumeText *string
	var embeddingVector, embeddingModel *string
	if err := rows.Scan(
		&item.ID, &item.ChatSessionID, &item.FromTurn, &item.ToTurn, &arcIndex, &arcName, &arcStatus, &coreConflict,
		&keyTurningPointsJSON, &activePromisesJSON, &unresolvedDebtsJSON, &resolvedPayoffsJSON,
		&callbackCandidatesJSON, &futurePayoffCandidatesJSON, &irreversibleTurnsJSON, &callbackDebtsJSON,
		&relationshipPivotsJSON, &arcResumeText, &embeddingVector, &embeddingModel, &item.CreatedAt,
	); err != nil {
		return ArcSummary{}, err
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
	return item, nil
}

func (s *d1Store) SaveArcSummary(ctx context.Context, chatSessionID string, item *ArcSummary) error {
	if item == nil {
		return fmt.Errorf("store: arc summary is required")
	}
	if item.ChatSessionID == "" {
		item.ChatSessionID = chatSessionID
	}
	arcStatus := strings.TrimSpace(item.ArcStatus)
	if arcStatus == "" {
		arcStatus = "active"
	}
	// created_at comes from the column default, matching the MariaDB path.
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO arc_summaries (
			chat_session_id, from_turn, to_turn, arc_index, arc_name, arc_status, core_conflict,
			key_turning_points_json, active_promises_json, unresolved_debts_json, resolved_payoffs_json,
			callback_candidates_json, future_payoff_candidates_json, irreversible_turns_json, callback_debts_json,
			relationship_pivots_json, arc_resume_text, embedding_vector, embedding_model
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		item.ChatSessionID, item.FromTurn, item.ToTurn, item.ArcIndex, d1NullableString(item.ArcName),
		arcStatus, d1NullableString(item.CoreConflict), d1NullableString(item.KeyTurningPointsJSON),
		d1NullableString(item.ActivePromisesJSON), d1NullableString(item.UnresolvedDebtsJSON),
		d1NullableString(item.ResolvedPayoffsJSON), d1NullableString(item.CallbackCandidatesJSON),
		d1NullableString(item.FuturePayoffCandidatesJSON), d1NullableString(item.IrreversibleTurnsJSON),
		d1NullableString(item.CallbackDebtsJSON), d1NullableString(item.RelationshipPivotsJSON),
		d1NullableString(item.ArcResumeText), d1NullableString(item.EmbeddingVector),
		d1NullableString(item.EmbeddingModel)).Scan(&id); err != nil {
		return err
	}
	item.ID = id
	return nil
}

func (s *d1Store) GetLatestArcSummary(ctx context.Context, chatSessionID string) (*ArcSummary, error) {
	return s.d1LatestArcSummary(ctx, chatSessionID)
}

func (s *d1Store) ListArcSummaries(ctx context.Context, chatSessionID string, status string, limit int) ([]ArcSummary, error) {
	items, err := s.SearchArcSummaries(ctx, chatSessionID, "", 0, 0, limit)
	if err != nil || strings.TrimSpace(status) == "" {
		return items, err
	}
	// The status filter is applied after retrieval, matching the MariaDB path.
	filtered := make([]ArcSummary, 0, len(items))
	for _, item := range items {
		if item.ArcStatus == status {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

func (s *d1Store) SearchArcSummaries(ctx context.Context, chatSessionID, query string, fromTurn, toTurn, limit int) ([]ArcSummary, error) {
	trimmed := strings.TrimSpace(query)
	like := "%" + trimmed + "%"
	sql := d1ArcSummarySelect + `
		WHERE chat_session_id = ?
		  AND (? = '' OR arc_name LIKE ? OR core_conflict LIKE ? OR arc_resume_text LIKE ?
		       OR key_turning_points_json LIKE ? OR active_promises_json LIKE ? OR unresolved_debts_json LIKE ?
		       OR callback_candidates_json LIKE ? OR future_payoff_candidates_json LIKE ?
		       OR irreversible_turns_json LIKE ? OR callback_debts_json LIKE ? OR relationship_pivots_json LIKE ?)
		  AND (? = 0 OR to_turn >= ?)
		  AND (? = 0 OR from_turn <= ?)
		ORDER BY arc_index DESC, id DESC`
	args := []any{chatSessionID, trimmed, like, like, like, like, like, like, like, like, like, like, like,
		fromTurn, fromTurn, toTurn, toTurn}
	if limit > 0 {
		sql += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ArcSummary{}
	for rows.Next() {
		item, err := d1ScanArcSummary(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ---------------------------------------------------------------------------
// saga digests
// ---------------------------------------------------------------------------

const d1SagaDigestSelect = `
		SELECT id, chat_session_id, from_turn, to_turn, era_label, saga_summary,
		       persistent_facts_json, never_drop_candidates_json, resume_pack_text,
		       embedding_vector, embedding_model, created_at
		FROM saga_digests`

func d1ScanSagaDigest(rows D1Rows) (SagaDigest, error) {
	var item SagaDigest
	var eraLabel, sagaSummary, persistentFactsJSON, neverDropCandidatesJSON, resumePackText *string
	var embeddingVector, embeddingModel *string
	if err := rows.Scan(
		&item.ID, &item.ChatSessionID, &item.FromTurn, &item.ToTurn, &eraLabel, &sagaSummary,
		&persistentFactsJSON, &neverDropCandidatesJSON, &resumePackText,
		&embeddingVector, &embeddingModel, &item.CreatedAt,
	); err != nil {
		return SagaDigest{}, err
	}
	item.EraLabel = d1DerefString(eraLabel)
	item.SagaSummary = d1DerefString(sagaSummary)
	item.PersistentFactsJSON = d1DerefString(persistentFactsJSON)
	item.NeverDropCandidatesJSON = d1DerefString(neverDropCandidatesJSON)
	item.ResumePackText = d1DerefString(resumePackText)
	item.EmbeddingVector = d1DerefString(embeddingVector)
	item.EmbeddingModel = d1DerefString(embeddingModel)
	return item, nil
}

func (s *d1Store) SaveSagaDigest(ctx context.Context, chatSessionID string, item *SagaDigest) error {
	if item == nil {
		return fmt.Errorf("store: saga digest is required")
	}
	if item.ChatSessionID == "" {
		item.ChatSessionID = chatSessionID
	}
	// created_at comes from the column default, matching the MariaDB path.
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO saga_digests (
			chat_session_id, from_turn, to_turn, era_label, saga_summary,
			persistent_facts_json, never_drop_candidates_json, resume_pack_text,
			embedding_vector, embedding_model
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		item.ChatSessionID, item.FromTurn, item.ToTurn, d1NullableString(item.EraLabel), item.SagaSummary,
		d1NullableString(item.PersistentFactsJSON), d1NullableString(item.NeverDropCandidatesJSON),
		d1NullableString(item.ResumePackText), d1NullableString(item.EmbeddingVector),
		d1NullableString(item.EmbeddingModel)).Scan(&id); err != nil {
		return err
	}
	item.ID = id
	return nil
}

func (s *d1Store) GetLatestSagaDigest(ctx context.Context, chatSessionID string) (*SagaDigest, error) {
	return s.d1LatestSagaDigest(ctx, chatSessionID)
}

func (s *d1Store) ListSagaDigests(ctx context.Context, chatSessionID string, limit int) ([]SagaDigest, error) {
	return s.SearchSagaDigests(ctx, chatSessionID, "", 0, 0, limit)
}

func (s *d1Store) SearchSagaDigests(ctx context.Context, chatSessionID, query string, fromTurn, toTurn, limit int) ([]SagaDigest, error) {
	trimmed := strings.TrimSpace(query)
	like := "%" + trimmed + "%"
	sql := d1SagaDigestSelect + `
		WHERE chat_session_id = ?
		  AND (? = '' OR era_label LIKE ? OR saga_summary LIKE ? OR resume_pack_text LIKE ?)
		  AND (? = 0 OR to_turn >= ?)
		  AND (? = 0 OR from_turn <= ?)
		ORDER BY to_turn DESC, id DESC`
	args := []any{chatSessionID, trimmed, like, like, like, fromTurn, fromTurn, toTurn, toTurn}
	if limit > 0 {
		sql += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []SagaDigest{}
	for rows.Next() {
		item, err := d1ScanSagaDigest(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// ---------------------------------------------------------------------------
// episode summaries
// ---------------------------------------------------------------------------

func (s *d1Store) SaveEpisodeSummary(ctx context.Context, item *EpisodeSummary) error {
	if item == nil {
		return fmt.Errorf("store: episode summary is required")
	}
	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO episode_summaries (
			chat_session_id, from_turn, to_turn, summary_text, key_entities, key_events,
			open_loops_json, relationship_changes_json, embedding_vector, embedding_model, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		item.ChatSessionID, item.FromTurn, item.ToTurn, item.SummaryText, d1NullableString(item.KeyEntities),
		d1NullableString(item.KeyEvents), d1NullableString(item.OpenLoopsJSON),
		d1NullableString(item.RelationshipChangesJSON), d1NullableString(item.EmbeddingVector),
		d1NullableString(item.EmbeddingModel), d1TimeValue(item.CreatedAt)).Scan(&id); err != nil {
		return err
	}
	item.ID = id
	return nil
}
