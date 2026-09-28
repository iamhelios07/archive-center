package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var _ SourceRevisionStore = (*d1Store)(nil)
var _ CriticInputSnapshotStore = (*d1Store)(nil)
var _ MemoryDerivationLifecycleAvailability = (*d1Store)(nil)
var _ ActiveSourceRevisionLister = (*d1Store)(nil)
var _ SourceRevisionHistoryLister = (*d1Store)(nil)

func (s *d1Store) MemoryDerivationLifecycleEnabled() bool { return s != nil && s.conn != nil }

func (s *d1Store) RegisterAcceptedSourceRevision(ctx context.Context, source *MemorySourceRevision) (SourceRevisionRegistration, error) {
	var result SourceRevisionRegistration
	if err := validateMemorySourceRevision(source); err != nil {
		return result, err
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()
	var tail int
	if err := s.conn.QueryRow(ctx, `SELECT turn_index FROM chat_logs WHERE chat_session_id = ? ORDER BY turn_index DESC, id DESC LIMIT 1`, source.ChatSessionID).Scan(&tail); err != nil {
		return result, err
	}
	if source.TurnIndex != tail {
		return result, ErrSourceRevisionConflict
	}
	rows, err := s.conn.Query(ctx, `SELECT source_revision, combined_content_hash, raw_user_content, raw_assistant_content FROM memory_source_revisions WHERE chat_session_id = ? AND lifecycle_state = 'active' AND (logical_turn_id = ? OR turn_index = ?) ORDER BY host_observed_at_ms DESC, id DESC LIMIT 2`, source.ChatSessionID, source.LogicalTurnID, source.TurnIndex)
	if err != nil {
		return result, err
	}
	type active struct{ revision, hash, user, assistant string }
	var activeRows []active
	for rows.Next() {
		var x active
		if err := rows.Scan(&x.revision, &x.hash, &x.user, &x.assistant); err != nil {
			_ = rows.Close()
			return result, err
		}
		activeRows = append(activeRows, x)
	}
	if err := rows.Close(); err != nil {
		return result, err
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	if len(activeRows) > 1 {
		return result, ErrSourceRevisionConflict
	}
	canonical, err := s.conn.Query(ctx, `SELECT role, content FROM chat_logs WHERE chat_session_id = ? AND turn_index = ? ORDER BY id`, source.ChatSessionID, source.TurnIndex)
	if err != nil {
		return result, err
	}
	var user, assistant string
	var users, assistants int
	for canonical.Next() {
		var role, content string
		if err := canonical.Scan(&role, &content); err != nil {
			_ = canonical.Close()
			return result, err
		}
		switch strings.ToLower(strings.TrimSpace(role)) {
		case "user":
			users++
			user = content
		case "assistant":
			assistants++
			assistant = content
		}
	}
	if err := canonical.Close(); err != nil {
		return result, err
	}
	if err := canonical.Err(); err != nil {
		return result, err
	}
	userOK := users == 1 && user == source.UserContent
	if strings.TrimSpace(source.UserContent) == "" {
		userOK = users == 0
	}
	if !userOK || assistants != 1 || assistant != source.AssistantContent {
		return result, ErrSourceRevisionConflict
	}
	if len(activeRows) == 1 {
		a := activeRows[0]
		if a.revision != source.SourceRevision || a.hash != source.CombinedContentHash || a.user != source.UserContent || a.assistant != source.AssistantContent {
			return result, ErrSourceRevisionConflict
		}
		return SourceRevisionRegistration{Idempotent: true}, nil
	}
	branchState := source.BranchState
	if branchState == "" {
		branchState = "not_exposed"
	}
	contract := source.ContractVersion
	if contract == "" {
		contract = MemorySourceRevisionContract
	}
	lifecycle := source.LifecycleState
	if lifecycle == "" {
		lifecycle = "active"
	}
	_, err = s.conn.Exec(ctx, `INSERT INTO memory_source_revisions (contract_version,source_revision,chat_session_id,logical_turn_id,turn_index,source_message_id,source_generation_id,branch_id,branch_state,raw_user_content,raw_assistant_content,combined_content_hash,user_observed_content_hash,assistant_observed_content_hash,hash_algorithm,host_observed_at_ms,lifecycle_state) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, contract, source.SourceRevision, source.ChatSessionID, source.LogicalTurnID, source.TurnIndex, d1NullableString(source.SourceMessageID), d1NullableString(source.SourceGenerationID), d1NullableString(source.BranchID), branchState, source.UserContent, source.AssistantContent, source.CombinedContentHash, d1NullableString(source.UserObservedContentHash), d1NullableString(source.AssistantObservedContentHash), source.HashAlgorithm, source.HostObservedAtMS, lifecycle)
	if err != nil {
		return result, err
	}
	return SourceRevisionRegistration{Inserted: true}, nil
}

func (s *d1Store) GetSourceRevision(ctx context.Context, sid, revision string) (*MemorySourceRevision, error) {
	row := s.conn.QueryRow(ctx, sourceRevisionSelect+` WHERE chat_session_id = ? AND source_revision = ?`, strings.TrimSpace(sid), strings.TrimSpace(revision))
	source, err := d1ScanSourceRevision(row)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	return source, err
}
func (s *d1Store) IsSourceRevisionActive(ctx context.Context, sid, revision string) (bool, error) {
	var one int
	err := s.conn.QueryRow(ctx, `SELECT 1 FROM memory_source_revisions WHERE chat_session_id=? AND source_revision=? AND lifecycle_state='active'`, sid, revision).Scan(&one)
	if errors.Is(err, errD1NoRows) {
		return false, nil
	}
	return err == nil, err
}
func (s *d1Store) SaveCriticInputSnapshot(ctx context.Context, sid, revision, snapshot, snapshotHash string, updated time.Time) error {
	sid = strings.TrimSpace(sid)
	revision = strings.TrimSpace(revision)
	snapshot = strings.TrimSpace(snapshot)
	snapshotHash = strings.ToLower(strings.TrimSpace(snapshotHash))
	if sid == "" || revision == "" || snapshot == "" || len(snapshotHash) != sha256.Size*2 {
		return fmt.Errorf("invalid critic input snapshot")
	}
	var decoded any
	if json.Unmarshal([]byte(snapshot), &decoded) != nil {
		return fmt.Errorf("invalid critic input snapshot json")
	}
	actual := sha256.Sum256([]byte(snapshot))
	if hex.EncodeToString(actual[:]) != snapshotHash {
		return fmt.Errorf("critic input snapshot hash mismatch")
	}
	_, err := s.conn.Exec(ctx, `UPDATE memory_source_revisions SET critic_input_snapshot_json=?,critic_input_snapshot_hash=?,updated_at=? WHERE chat_session_id=? AND source_revision=? AND lifecycle_state='active'`, snapshot, snapshotHash, d1TimeValue(nonZeroTime(updated)), sid, revision)
	return err
}
func (s *d1Store) ListActiveSourceRevisions(ctx context.Context, sid string, from, to int) ([]MemorySourceRevision, error) {
	return s.d1ListSourceRevisions(ctx, sid, from, to, true)
}
func (s *d1Store) ListSourceRevisions(ctx context.Context, sid string, from, to int) ([]MemorySourceRevision, error) {
	return s.d1ListSourceRevisions(ctx, sid, from, to, false)
}
func (s *d1Store) d1ListSourceRevisions(ctx context.Context, sid string, from, to int, activeOnly bool) ([]MemorySourceRevision, error) {
	q := sourceRevisionSelect + ` WHERE chat_session_id = ?`
	args := []any{sid}
	if activeOnly {
		q += ` AND lifecycle_state = 'active'`
	}
	if from > 0 {
		q += ` AND turn_index >= ?`
		args = append(args, from)
	}
	if to > 0 {
		q += ` AND turn_index <= ?`
		args = append(args, to)
	}
	q += ` ORDER BY turn_index,id`
	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MemorySourceRevision{}
	for rows.Next() {
		item, err := d1ScanSourceRevision(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *item)
	}
	return out, rows.Err()
}
func (s *d1Store) InvalidateSourceRevisions(ctx context.Context, sid string, from int, lifecycle, reason string, at time.Time) error {
	if strings.TrimSpace(sid) == "" || from <= 0 {
		return fmt.Errorf("invalid source invalidation")
	}
	if lifecycle != "invalidated" && lifecycle != "deleted" && lifecycle != "superseded" {
		return fmt.Errorf("invalid source lifecycle %q", lifecycle)
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()
	statePredicate := `lifecycle_state = 'active'`
	if lifecycle == "deleted" {
		statePredicate = `lifecycle_state <> 'deleted'`
	}
	rows, err := s.conn.Query(ctx, `SELECT source_revision FROM memory_source_revisions WHERE chat_session_id=? AND turn_index>=? AND `+statePredicate+` ORDER BY turn_index,id`, sid, from)
	if err != nil {
		return err
	}
	var revisions []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			_ = rows.Close()
			return err
		}
		revisions = append(revisions, r)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	deletes, err := s.d1KnownVectorDeletes(ctx, sid, revisions, from)
	if err != nil {
		return err
	}
	now := d1TimeValue(nonZeroTime(at))
	stmts := make([]D1Statement, 0, len(revisions)*6+len(deletes))
	for _, d := range deletes {
		stmts = append(stmts, D1Statement{SQL: `INSERT INTO memory_vector_outbox (contract_version,operation_key,operation,chat_session_id,source_revision,document_id,document_json,embedding_ready,required_source_state,status,attempts,created_at,updated_at) VALUES (?,?,'delete',?,?,?,?,1,'inactive','pending',0,?,?) ON CONFLICT(operation_key) DO NOTHING`, Args: []any{MemoryVectorOutboxContract, memoryVectorOperationKey("delete:inactive", sid, d.sourceRevision, d.documentID), sid, d.sourceRevision, d.documentID, memoryVectorDeleteAuditJSON(reason), now, now}})
	}
	for _, r := range revisions {
		stmts = append(stmts, D1Statement{SQL: `UPDATE memory_derivation_dependencies SET lifecycle_state='invalidated',invalidated_at=?,updated_at=? WHERE chat_session_id=? AND source_revision=? AND lifecycle_state='active'`, Args: []any{now, now, sid, r}}, D1Statement{SQL: `UPDATE precise_memory_units SET lifecycle_state='invalidated',updated_at=? WHERE chat_session_id=? AND source_revision=? AND lifecycle_state='active'`, Args: []any{now, sid, r}}, D1Statement{SQL: `UPDATE memory_reprocessing_jobs SET status='stale_rejected',lease_owner=NULL,lease_until=NULL,last_error=?,updated_at=? WHERE chat_session_id=? AND source_revision=? AND status IN ('pending','leased','retryable')`, Args: []any{reason, now, sid, r}}, D1Statement{SQL: `UPDATE memory_vector_outbox SET status='stale_rejected',lease_owner=NULL,lease_until=NULL,last_error=?,updated_at=? WHERE chat_session_id=? AND source_revision=? AND operation='upsert' AND status IN ('pending','leased','retryable','needs_embedding')`, Args: []any{reason, now, sid, r}}, D1Statement{SQL: `UPDATE memory_source_revisions SET lifecycle_state=?,invalidation_reason=?,invalidated_at=?,updated_at=? WHERE chat_session_id=? AND source_revision=? AND ` + statePredicate, Args: []any{lifecycle, reason, now, now, sid, r}})
	}
	return s.conn.Batch(ctx, stmts...)
}

const sourceRevisionSelect = `SELECT id,contract_version,source_revision,chat_session_id,logical_turn_id,turn_index,COALESCE(source_message_id,''),COALESCE(source_generation_id,''),COALESCE(branch_id,''),branch_state,raw_user_content,raw_assistant_content,combined_content_hash,COALESCE(user_observed_content_hash,''),COALESCE(assistant_observed_content_hash,''),hash_algorithm,host_observed_at_ms,lifecycle_state,COALESCE(superseded_by_revision,''),COALESCE(invalidation_reason,''),derived_admission_state,derived_admission_version,derived_extractor_version,derived_index_version,COALESCE(derived_result_hash,''),COALESCE(derived_result_json,''),COALESCE(derived_admitted_at,''),COALESCE(critic_input_snapshot_json,''),COALESCE(critic_input_snapshot_hash,''),created_at,updated_at FROM memory_source_revisions`

type d1SourceScanner interface{ Scan(...any) error }

func d1ScanSourceRevision(row d1SourceScanner) (*MemorySourceRevision, error) {
	var x MemorySourceRevision
	var admitted, created, updated string
	err := row.Scan(&x.ID, &x.ContractVersion, &x.SourceRevision, &x.ChatSessionID, &x.LogicalTurnID, &x.TurnIndex, &x.SourceMessageID, &x.SourceGenerationID, &x.BranchID, &x.BranchState, &x.UserContent, &x.AssistantContent, &x.CombinedContentHash, &x.UserObservedContentHash, &x.AssistantObservedContentHash, &x.HashAlgorithm, &x.HostObservedAtMS, &x.LifecycleState, &x.SupersededByRevision, &x.InvalidationReason, &x.DerivedAdmissionState, &x.DerivedAdmissionVersion, &x.DerivedExtractorVersion, &x.DerivedIndexVersion, &x.DerivedResultHash, &x.DerivedResultJSON, &admitted, &x.CriticInputSnapshotJSON, &x.CriticInputSnapshotHash, &created, &updated)
	if err != nil {
		return nil, err
	}
	var e error
	if admitted != "" {
		x.DerivedAdmittedAt, e = parseD1Time(admitted)
		if e != nil {
			return nil, e
		}
	}
	x.CreatedAt, e = parseD1Time(created)
	if e != nil {
		return nil, e
	}
	x.UpdatedAt, e = parseD1Time(updated)
	if e != nil {
		return nil, e
	}
	return &x, nil
}
