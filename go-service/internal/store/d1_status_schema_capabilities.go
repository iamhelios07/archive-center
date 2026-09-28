package store

import (
	"context"
	"errors"
	"strings"
	"time"
)

// D1 status schema capabilities: the reviewable proposal ledger and the accepted
// registry it feeds.
//
// These are the operator-facing half of the status subsystem: a proposal is
// reviewed, and accepted definitions become the registry rows that current-value
// writes reference through a foreign key. Their validation rules are therefore
// part of the contract, not incidental.

var _ StatusSchemaProposalStore = (*d1Store)(nil)
var _ StatusSchemaRegistryStore = (*d1Store)(nil)

const d1StatusSchemaProposalSelect = `
		SELECT id, chat_session_id, input_channel, proposal_state, schema_name, ruleset_label,
		       schema_json, provenance_json, review_note, reviewer, reviewed_at, created_at, updated_at
		FROM status_schema_proposals`

func d1ScanStatusSchemaProposal(rows D1Rows) (StatusSchemaProposal, error) {
	var item StatusSchemaProposal
	var rulesetLabel, provenanceJSON, reviewNote, reviewer *string
	var reviewedAt *time.Time
	if err := rows.Scan(
		&item.ID, &item.ChatSessionID, &item.InputChannel, &item.ProposalState, &item.SchemaName, &rulesetLabel,
		&item.SchemaJSON, &provenanceJSON, &reviewNote, &reviewer, &reviewedAt, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return StatusSchemaProposal{}, err
	}
	item.RulesetLabel = d1DerefString(rulesetLabel)
	item.ProvenanceJSON = d1DerefString(provenanceJSON)
	item.ReviewNote = d1DerefString(reviewNote)
	item.Reviewer = d1DerefString(reviewer)
	if reviewedAt != nil {
		item.ReviewedAt = *reviewedAt
	}
	return item, nil
}

// ListStatusSchemaProposals returns the newest proposals first. The limit is
// passed through unclamped, matching the MariaDB path for this list.
func (s *d1Store) ListStatusSchemaProposals(ctx context.Context, chatSessionID, proposalState string, limit int) ([]StatusSchemaProposal, error) {
	sql := d1StatusSchemaProposalSelect + ` WHERE chat_session_id = ?`
	args := []any{chatSessionID}
	if trimmed := strings.TrimSpace(proposalState); trimmed != "" {
		sql += ` AND proposal_state = ?`
		args = append(args, trimmed)
	}
	sql += ` ORDER BY updated_at DESC, id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []StatusSchemaProposal
	for rows.Next() {
		item, err := d1ScanStatusSchemaProposal(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *d1Store) GetStatusSchemaProposal(ctx context.Context, id int64) (StatusSchemaProposal, error) {
	rows, err := s.conn.Query(ctx, d1StatusSchemaProposalSelect+` WHERE id = ?`, id)
	if err != nil {
		return StatusSchemaProposal{}, err
	}
	var found bool
	var item StatusSchemaProposal
	if rows.Next() {
		item, err = d1ScanStatusSchemaProposal(rows)
		if err != nil {
			rows.Close()
			return StatusSchemaProposal{}, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return StatusSchemaProposal{}, err
	}
	rows.Close()
	if !found {
		return StatusSchemaProposal{}, ErrNotFound
	}
	return item, nil
}

func (s *d1Store) SaveStatusSchemaProposal(ctx context.Context, proposal StatusSchemaProposal) (StatusSchemaProposal, error) {
	now := d1TimeValue(proposal.CreatedAt)
	inputChannel := strings.TrimSpace(proposal.InputChannel)
	if inputChannel == "" {
		inputChannel = "bootstrap"
	}
	proposalState := strings.TrimSpace(proposal.ProposalState)
	if proposalState == "" {
		proposalState = "pending_review"
	}
	schemaName := strings.TrimSpace(proposal.SchemaName)
	if schemaName == "" {
		schemaName = "status_schema"
	}

	var id int64
	if err := s.conn.QueryRow(ctx, `
		INSERT INTO status_schema_proposals (
			chat_session_id, input_channel, proposal_state, schema_name, ruleset_label,
			schema_json, provenance_json, review_note, reviewer, reviewed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id`,
		proposal.ChatSessionID, inputChannel, proposalState, schemaName,
		d1NullableString(proposal.RulesetLabel), proposal.SchemaJSON,
		d1NullableString(proposal.ProvenanceJSON), d1NullableString(proposal.ReviewNote),
		d1NullableString(proposal.Reviewer), d1NullableTime(proposal.ReviewedAt), now, now).Scan(&id); err != nil {
		return proposal, err
	}
	proposal.ID = id
	proposal.InputChannel = inputChannel
	proposal.ProposalState = proposalState
	proposal.SchemaName = schemaName
	proposal.CreatedAt = parseD1TimeOrZero(now)
	proposal.UpdatedAt = proposal.CreatedAt
	return proposal, nil
}

func (s *d1Store) UpdateStatusSchemaProposalReview(ctx context.Context, id int64, proposalState, reviewNote, reviewer string) error {
	trimmed := strings.TrimSpace(proposalState)
	if trimmed == "" {
		return errors.New("proposal_state is required")
	}
	// A review always stamps reviewed_at, and an absent note or reviewer is
	// stored as NULL rather than as an empty string.
	affected, err := s.conn.Exec(ctx, `
		UPDATE status_schema_proposals
		SET proposal_state = ?, review_note = NULLIF(?, ''), reviewer = NULLIF(?, ''),
		    reviewed_at = `+d1NowExpression+`, updated_at = `+d1NowExpression+`
		WHERE id = ?`,
		trimmed, strings.TrimSpace(reviewNote), strings.TrimSpace(reviewer), id)
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// ---------------------------------------------------------------------------
// status schema registry
// ---------------------------------------------------------------------------

const d1StatusSchemaDefinitionSelect = `
		SELECT id, chat_session_id, source_proposal_id, schema_name, ruleset_label,
		       status_key, label, owner_scope, value_kind, bounds_json, options_json,
		       default_value_json, registry_state, created_at, updated_at
		FROM status_schema_registry`

func d1ScanStatusSchemaDefinition(rows D1Rows) (StatusSchemaDefinition, error) {
	var item StatusSchemaDefinition
	var proposalID *int64
	var rulesetLabel, boundsJSON, optionsJSON, defaultValueJSON *string
	if err := rows.Scan(
		&item.ID, &item.ChatSessionID, &proposalID, &item.SchemaName, &rulesetLabel,
		&item.StatusKey, &item.Label, &item.OwnerScope, &item.ValueKind, &boundsJSON, &optionsJSON,
		&defaultValueJSON, &item.RegistryState, &item.CreatedAt, &item.UpdatedAt,
	); err != nil {
		return StatusSchemaDefinition{}, err
	}
	item.SourceProposalID = d1DerefInt64(proposalID)
	item.RulesetLabel = d1DerefString(rulesetLabel)
	item.BoundsJSON = d1DerefString(boundsJSON)
	item.OptionsJSON = d1DerefString(optionsJSON)
	item.DefaultValueJSON = d1DerefString(defaultValueJSON)
	return item, nil
}

func (s *d1Store) ListStatusSchemaDefinitions(ctx context.Context, chatSessionID, registryState string, limit int) ([]StatusSchemaDefinition, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	sql := d1StatusSchemaDefinitionSelect + ` WHERE chat_session_id = ?`
	args := []any{chatSessionID}
	if trimmed := strings.TrimSpace(registryState); trimmed != "" {
		sql += ` AND registry_state = ?`
		args = append(args, trimmed)
	}
	sql += ` ORDER BY schema_name ASC, status_key ASC, id ASC LIMIT ?`
	args = append(args, limit)

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []StatusSchemaDefinition
	for rows.Next() {
		item, err := d1ScanStatusSchemaDefinition(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// GetStatusSchemaDefinitionByKey resolves the newest active definition for a
// status key and owner scope.
func (s *d1Store) GetStatusSchemaDefinitionByKey(ctx context.Context, chatSessionID, statusKey, ownerScope string) (StatusSchemaDefinition, error) {
	rows, err := s.conn.Query(ctx, d1StatusSchemaDefinitionSelect+`
		WHERE chat_session_id = ? AND status_key = ? AND owner_scope = ? AND registry_state = 'active'
		ORDER BY id DESC
		LIMIT 1`, chatSessionID, statusKey, ownerScope)
	if err != nil {
		return StatusSchemaDefinition{}, err
	}
	var found bool
	var item StatusSchemaDefinition
	if rows.Next() {
		item, err = d1ScanStatusSchemaDefinition(rows)
		if err != nil {
			rows.Close()
			return StatusSchemaDefinition{}, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return StatusSchemaDefinition{}, err
	}
	rows.Close()
	if !found {
		return StatusSchemaDefinition{}, ErrNotFound
	}
	return item, nil
}

// SaveStatusSchemaDefinitions inserts definitions in order and returns the ones
// that succeeded. Like the MariaDB path it reports partial progress on failure
// rather than discarding the rows it already accepted.
func (s *d1Store) SaveStatusSchemaDefinitions(ctx context.Context, definitions []StatusSchemaDefinition) ([]StatusSchemaDefinition, error) {
	out := make([]StatusSchemaDefinition, 0, len(definitions))
	for _, def := range definitions {
		now := d1TimeValue(def.CreatedAt)
		state := strings.TrimSpace(def.RegistryState)
		if state == "" {
			state = "active"
		}
		schemaName := strings.TrimSpace(def.SchemaName)
		if schemaName == "" {
			schemaName = "status_schema"
		}
		label := strings.TrimSpace(def.Label)
		if label == "" {
			label = def.StatusKey
		}

		var id int64
		if err := s.conn.QueryRow(ctx, `
			INSERT INTO status_schema_registry (
				chat_session_id, source_proposal_id, schema_name, ruleset_label,
				status_key, label, owner_scope, value_kind, bounds_json, options_json,
				default_value_json, registry_state, created_at, updated_at
			) VALUES (?, NULLIF(?, 0), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			RETURNING id`,
			def.ChatSessionID, def.SourceProposalID, schemaName, d1NullableString(def.RulesetLabel),
			def.StatusKey, label, def.OwnerScope, def.ValueKind, d1NullableString(def.BoundsJSON),
			d1NullableString(def.OptionsJSON), d1NullableString(def.DefaultValueJSON), state, now, now).Scan(&id); err != nil {
			return out, err
		}
		def.ID = id
		def.SchemaName = schemaName
		def.Label = label
		def.RegistryState = state
		def.CreatedAt = parseD1TimeOrZero(now)
		def.UpdatedAt = def.CreatedAt
		out = append(out, def)
	}
	return out, nil
}
