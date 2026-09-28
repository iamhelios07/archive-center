package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// D1 precise memory write capability.
//
// The precise memory lane already has its three readers ported
// (CharacterPerspectiveMemoryReader, ActiveInteractionMemoryReader,
// GeneralVectorPreciseMemoryReader). Without this write, those readers are
// permanently empty on D1: a Cloudflare deployment would read a memory store
// that nothing can populate. This file closes the lane by porting
// SavePreciseMemoryUnit from mariadb_precise_memory.go.
//
// What one unit commits, and why it is one batch
//
// A precise unit is not one row. It is:
//
//   - the unit row itself,
//   - one memory_derivation_dependencies row per parent (its source revision,
//     plus each distinct direct evidence it cites), and
//   - optionally one memory_vector_outbox row, when the unit is general-vector
//     eligible.
//
// Those three are committed together or not at all. A unit that exists without
// its dependency edges is a unit that no reprocess or rollback can invalidate;
// a unit that exists without its outbox row is a unit that never reaches the
// vector index and so never appears in semantic recall. Both are silent, and
// both are what a partially applied write looks like. D1Conn.Batch is therefore
// the transaction boundary here, exactly as a MariaDB transaction was.
//
// Where the duplicate check moved, and why that is safe
//
// The MariaDB path INSERTS and then catches error 1062, resolving the collision
// against the existing row. D1 cannot reproduce that shape inside a batch:
// Batch returns only an error, so a statement that silently does nothing is
// indistinguishable from one that wrote a row. The resolution query therefore
// runs BEFORE the batch instead of after the failed insert.
//
// The observable contract is identical. The query is the same statement, and
// its two outcomes are the same two outcomes: an exact replay returns
// (false, nil) having written nothing, and a mismatch is an idempotency
// conflict. The ordering change is safe because s.memoryDerivationWriteMu is
// held across the check and the batch, so within this process nothing can
// insert the same unit_id in between. That mutex is process-local; two Cloudflare
// Containers racing on one unit_id is the same unresolved cross-container
// locking boundary the other D1 write capabilities document, and it fails as a
// unique-constraint error from the batch rather than as a silent overwrite.
//
// The source fence
//
// The unit is rejected unless its source revision is currently active, checked
// inside the same critical section as the write. That is what stops a rollback
// from being followed by a straggling complete-turn that resurrects a unit for a
// revision the user has already discarded. The error is ErrSourceRevisionStale
// and it is returned before any statement is sent.
//
// The vector outbox
//
// The outbox payload is built by the same shared helpers the MariaDB path uses
// — PreciseMemorySemanticText, memoryVectorDocumentMetadata,
// preciseMemoryVectorOperationKey, memoryVectorOperationKey — so the document
// text, the metadata, and the operation key are byte-identical on both
// providers. That matters because the operation key is the outbox's unique
// idempotency key: a key computed differently here would let a replay enqueue a
// SECOND operation for a document the worker has already applied, and the
// duplicate would look like new work.
//
// The replay-refresh path is not ported. enqueueAdmissionVectorOperation has a
// lease-aware branch for administrative vector replay
// (WithMemoryAdmissionVectorReplay), which depends on the outbox lease
// capabilities this provider does not yet expose. Rather than quietly take the
// non-refresh branch — which would skip the lease check and could re-enqueue a
// document another worker is holding — the replay request is refused with an
// explicit error. No production caller of SavePreciseMemoryUnit sets that
// context today; it is set only by the administrative reindex route, which
// drives CommitMemoryAdmission instead.

var _ PreciseMemoryWriter = (*d1Store)(nil)
var _ PreciseMemoryWriteAvailability = (*d1Store)(nil)

// PreciseMemoryWritesEnabled reports whether the D1 provider can persist a
// precise unit. It is the same "does this store really write" question the
// MariaDB provider answers with a live handle, and it exists so a composite
// store can refuse the optional projection instead of dropping it.
func (s *d1Store) PreciseMemoryWritesEnabled() bool { return s != nil && s.conn != nil }

// d1PreciseMemoryUnitInsert is the MariaDB INSERT with no dialect change. The
// 41 columns and their order are load-bearing: they are the unit's canonical
// shape, and reordering them would silently transpose two adjacent fields.
const d1PreciseMemoryUnitInsert = `
	INSERT INTO precise_memory_units (
		unit_id, contract_version, chat_session_id, source_turn_start,
		source_turn_end, source_contract, source_revision,
		source_logical_turn_id, source_message_id, source_generation_id,
		source_content_hash, source_role, source_span_start, source_span_end,
		evidence_excerpt, evidence_hash, root_evidence_id,
		direct_evidence_ids_json, memory_kind, memory_subtype, payload_json,
		actor_entity_id, subject_entity_id, affected_entity_id,
		location_entity_id, object_entity_id, relationship_key, truth_scope,
		epistemic_mode, authority_class, admission_state, review_state,
		visibility, knowledge_holder_entity_id, reveal_condition, confidence,
		idempotency_key, lifecycle_state, created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
	          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// SavePreciseMemoryUnit commits one precise unit, its derivation dependencies,
// and its vector outbox operation as a single atomic D1 batch.
//
// It reports whether a NEW unit row was written. A replay of an identical unit
// reports false and writes nothing; a replay that collides on unit_id or
// idempotency_key but carries different content is an error, because silently
// keeping the stored row would mean the caller's payload was never recorded.
func (s *d1Store) SavePreciseMemoryUnit(ctx context.Context, item *PreciseMemoryUnit) (bool, error) {
	if item == nil || strings.TrimSpace(item.SourceRevision) == "" {
		return false, errors.New("precise memory source revision is required")
	}
	if memoryAdmissionVectorReplayFromContext(ctx).Refresh {
		// Failing loudly is the point: the alternative is a silent downgrade of
		// a lease-aware replay to a plain enqueue.
		return false, errors.New("d1: vector replay refresh is not supported by this provider")
	}
	// The fence and the write share one critical section so a revision that goes
	// inactive between the check and the insert cannot admit a stale unit.
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	if err := s.d1RequireActiveSourceRevision(ctx, item.ChatSessionID, item.SourceRevision); err != nil {
		return false, err
	}

	// The MariaDB duplicate resolution, run before the batch. ORDER BY the
	// boolean is a MySQL idiom SQLite has no equivalent for, so the preference
	// for an exact unit_id match is expressed as the same two-key ordering with
	// a CASE: the row that collided on unit_id sorts first.
	var existingUnitID, existingRevision, existingKey string
	err := s.conn.QueryRow(ctx, `
		SELECT unit_id, source_revision, idempotency_key
		FROM precise_memory_units
		WHERE unit_id = ? OR idempotency_key = ?
		ORDER BY CASE WHEN unit_id = ? THEN 0 ELSE 1 END
		LIMIT 1
	`, item.UnitID, item.IdempotencyKey, item.UnitID).Scan(
		&existingUnitID, &existingRevision, &existingKey)
	if err == nil {
		if existingUnitID != item.UnitID ||
			existingRevision != item.SourceRevision ||
			existingKey != item.IdempotencyKey {
			return false, errors.New("precise memory idempotency conflict")
		}
		return false, nil
	}
	if !errors.Is(err, errD1NoRows) {
		return false, err
	}

	statements := make([]D1Statement, 0, 5)
	statements = append(statements, D1Statement{SQL: d1PreciseMemoryUnitInsert, Args: d1PreciseMemoryUnitArgs(item)})

	dependencyStatements, err := d1PreciseMemoryDependencyStatements(item)
	if err != nil {
		return false, err
	}
	statements = append(statements, dependencyStatements...)

	// The outbox statement is omitted when the unit is not general-vector
	// eligible or has no searchable text, which is the MariaDB early return
	// expressed as "add no statement" rather than as a separate code path.
	if outboxStatement, include, err := d1PreciseMemoryVectorStatement(ctx, s, item); err != nil {
		return false, err
	} else if include {
		statements = append(statements, outboxStatement)
	}

	if err := s.conn.Batch(ctx, statements...); err != nil {
		return false, err
	}
	// The unit row is committed. Its id is read back rather than returned by the
	// insert, because a D1 batch does not surface per-statement rows. A failed
	// readback leaves ID zero, which is the same outcome the MariaDB path has
	// when LastInsertId itself fails: the write succeeded, the handle did not.
	var id int64
	if err := s.conn.QueryRow(ctx,
		`SELECT id FROM precise_memory_units WHERE unit_id = ?`, item.UnitID).Scan(&id); err == nil {
		item.ID = id
	}
	return true, nil
}

// d1RequireActiveSourceRevision reports ErrSourceRevisionStale unless the named
// revision of the session is currently active.
func (s *d1Store) d1RequireActiveSourceRevision(ctx context.Context, chatSessionID, sourceRevision string) error {
	var lifecycle string
	err := s.conn.QueryRow(ctx, `SELECT lifecycle_state FROM memory_source_revisions
		WHERE chat_session_id = ? AND source_revision = ?`,
		strings.TrimSpace(chatSessionID), strings.TrimSpace(sourceRevision)).Scan(&lifecycle)
	if errors.Is(err, errD1NoRows) {
		return ErrSourceRevisionStale
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(lifecycle) != "active" {
		return ErrSourceRevisionStale
	}
	return nil
}

// d1PreciseMemoryUnitArgs binds the 41 insert columns in statement order.
//
// The nullable entity, message, subtype, reveal, and root-evidence columns go
// through d1NullableString / d1NullablePositiveInt so an absent value is stored
// as NULL rather than ''. That matters here beyond tidiness: the entity columns
// are foreign keys, and an empty string is not an entity id. created_at and
// updated_at go through d1TimeValue, which supplies the current time for a zero
// value exactly as nonZeroTime does on the reference path.
func d1PreciseMemoryUnitArgs(item *PreciseMemoryUnit) []any {
	return []any{
		item.UnitID, item.ContractVersion, item.ChatSessionID,
		item.SourceTurnStart, item.SourceTurnEnd, item.SourceContract,
		item.SourceRevision, d1NullableString(item.SourceLogicalTurnID),
		d1NullableString(item.SourceMessageID), d1NullableString(item.SourceGenerationID),
		item.SourceContentHash, item.SourceRole, item.SourceSpanStart,
		item.SourceSpanEnd, item.EvidenceExcerpt, item.EvidenceHash,
		d1NullablePositiveInt64(item.RootEvidenceID), item.DirectEvidenceIDsJSON, item.Kind,
		d1NullableString(item.Subtype), item.PayloadJSON,
		d1NullableString(item.ActorEntityID), d1NullableString(item.SubjectEntityID),
		d1NullableString(item.AffectedEntityID), d1NullableString(item.LocationEntityID),
		d1NullableString(item.ObjectEntityID), d1NullableString(item.RelationshipKey),
		item.TruthScope, item.EpistemicMode, item.AuthorityClass,
		item.AdmissionState, item.ReviewState, item.Visibility,
		d1NullableString(item.KnowledgeHolderEntityID),
		d1NullableString(item.RevealCondition), item.Confidence,
		item.IdempotencyKey, item.LifecycleState, d1TimeValue(item.CreatedAt),
		d1TimeValue(item.UpdatedAt),
	}
}

// d1PreciseMemoryDependencyStatements builds one dependency row per parent.
//
// The parent set is the source revision, plus each distinct positive direct
// evidence id that is not already the root evidence. The root is excluded
// because it appears in direct_evidence_ids_json as well, and a duplicated edge
// would make the unit look twice-derived from the same evidence. The version
// triple defaults are the reference defaults and are part of the dependency
// identity: two runs that disagree on any version are different derivations and
// must not share a row.
//
// Each statement is ON CONFLICT DO NOTHING. The canonical unique key spans
// exactly the eight identity columns the MariaDB path verified with a COUNT(*)
// after catching a duplicate, so a suppressed insert means the identical
// dependency is already present — the same condition, without a second
// statement inside the batch.
func d1PreciseMemoryDependencyStatements(item *PreciseMemoryUnit) ([]D1Statement, error) {
	derivationVersion := firstNonEmptyString(item.DerivationVersion, PreciseMemoryUnitContract)
	extractorVersion := firstNonEmptyString(item.ExtractorVersion, "complete_turn.configured_critic_extract")
	indexVersion := firstNonEmptyString(item.IndexVersion, "not_materialized")

	type parent struct{ kind, id string }
	parents := []parent{{kind: "source_revision", id: item.SourceRevision}}
	if item.RootEvidenceID > 0 {
		parents = append(parents, parent{kind: "direct_evidence", id: strconv.FormatInt(item.RootEvidenceID, 10)})
	}
	var evidenceIDs []int64
	_ = json.Unmarshal([]byte(item.DirectEvidenceIDsJSON), &evidenceIDs)
	for _, evidenceID := range evidenceIDs {
		if evidenceID <= 0 || evidenceID == item.RootEvidenceID {
			continue
		}
		parents = append(parents, parent{kind: "direct_evidence", id: strconv.FormatInt(evidenceID, 10)})
	}

	statements := make([]D1Statement, 0, len(parents))
	for _, p := range parents {
		statements = append(statements, D1Statement{
			SQL: `
			INSERT INTO memory_derivation_dependencies (
				contract_version, chat_session_id, source_revision,
				root_source_pointer, child_artifact_type, child_artifact_id,
				parent_artifact_type, parent_artifact_id, derivation_version,
				extractor_version, index_version, lifecycle_state, created_at,
				updated_at
			) VALUES (?, ?, ?, ?, 'precise_memory_unit', ?, ?, ?, ?, ?, ?, 'active', ?, ?)
			ON CONFLICT DO NOTHING`,
			Args: []any{
				MemoryDerivationDependencyContract, item.ChatSessionID,
				item.SourceRevision, "source_revision:" + item.SourceRevision, item.UnitID,
				p.kind, p.id, derivationVersion, extractorVersion,
				indexVersion, d1TimeValue(item.CreatedAt), d1TimeValue(item.UpdatedAt),
			},
		})
	}
	return statements, nil
}

// d1PreciseMemoryVectorStatement builds the outbox enqueue for a unit.
//
// The second result reports whether a statement is needed. A unit that is not
// general-vector eligible, or whose semantic text is empty, contributes nothing
// to the vector index and must not occupy an outbox row — an outbox row with no
// document would be a task the worker can never complete.
//
// When an outbox row for the same operation key already exists, its identity is
// verified and the statement is omitted. That is the reference path's
// post-duplicate check moved ahead of the write for the same reason the unit's
// own duplicate check moved: a batch cannot report which statement was
// suppressed.
func d1PreciseMemoryVectorStatement(ctx context.Context, s *d1Store, item *PreciseMemoryUnit) (D1Statement, bool, error) {
	if !preciseMemoryGeneralVectorEligible(item) {
		return D1Statement{}, false, nil
	}
	documentID := "precise_memory:" + item.ChatSessionID + ":" + item.UnitID
	documentText := PreciseMemorySemanticText(item)
	if documentText == "" {
		return D1Statement{}, false, nil
	}
	documentJSON, err := json.Marshal(map[string]any{
		"ID":            documentID,
		"ChatSessionID": item.ChatSessionID,
		"SourceTable":   "precise_memory_units",
		"SourceRowID":   item.UnitID,
		"SchemaVersion": PreciseMemoryUnitContract,
		"DocumentText":  documentText,
		"Embedding":     item.VectorEmbedding,
		"Metadata": memoryVectorDocumentMetadata(
			item.SourceRevision, item.SourceContract, item.IndexVersion, documentText,
			item.VectorEmbeddingModel, item.VectorContextChunks, item.VectorContextChunkIndex,
		),
	})
	if err != nil {
		return D1Statement{}, false, err
	}
	outbox := &MemoryVectorOutboxItem{
		ContractVersion: MemoryVectorOutboxContract,
		OperationKey: preciseMemoryVectorOperationKey(
			item.ChatSessionID, item.SourceRevision, item.DerivationVersion,
			item.ExtractorVersion, item.IndexVersion, documentID,
		),
		Operation:           "upsert",
		ChatSessionID:       item.ChatSessionID,
		SourceRevision:      item.SourceRevision,
		DocumentID:          documentID,
		DocumentJSON:        string(documentJSON),
		EmbeddingReady:      len(item.VectorEmbedding) > 0,
		RequiredSourceState: "active",
		Status:              preciseMemoryVectorOutboxStatus(item),
		CreatedAt:           item.CreatedAt,
		UpdatedAt:           item.UpdatedAt,
	}
	statement, err := d1EnqueueMemoryVectorStatement(outbox)
	if err != nil {
		return D1Statement{}, false, err
	}
	// An outbox row for this key is a leftover from an interrupted earlier
	// write. Same key and same identity means the work is already queued; a
	// different document under the same key means the key is lying about what it
	// identifies, and that is a conflict rather than a replay.
	var operation, sid, revision, existingDocumentID, existingJSON, requiredSourceState string
	var embeddingReady bool
	err = s.conn.QueryRow(ctx, `
		SELECT operation, chat_session_id, source_revision, document_id,
		       COALESCE(document_json, ''), embedding_ready, required_source_state
		FROM memory_vector_outbox
		WHERE operation_key = ?`, outbox.OperationKey).Scan(
		&operation, &sid, &revision, &existingDocumentID, &existingJSON,
		&embeddingReady, &requiredSourceState)
	if err == nil {
		if operation != outbox.Operation || sid != outbox.ChatSessionID ||
			revision != outbox.SourceRevision || existingDocumentID != outbox.DocumentID ||
			strings.TrimSpace(existingJSON) != strings.TrimSpace(outbox.DocumentJSON) ||
			embeddingReady != outbox.EmbeddingReady ||
			requiredSourceState != outbox.RequiredSourceState {
			return D1Statement{}, false, errors.New("memory vector operation idempotency conflict")
		}
		return D1Statement{}, false, nil
	}
	if !errors.Is(err, errD1NoRows) {
		return D1Statement{}, false, err
	}
	return statement, true, nil
}

// d1EnqueueMemoryVectorStatement validates one outbox item and binds its insert.
//
// The validation is the reference enqueue's, unchanged, and it is not
// defensive decoration: the canonical schema enforces operation, required source
// state, and status with CHECK constraints, so an item that reached the
// statement unvalidated would fail the whole batch rather than one row.
func d1EnqueueMemoryVectorStatement(item *MemoryVectorOutboxItem) (D1Statement, error) {
	if item == nil || strings.TrimSpace(item.OperationKey) == "" ||
		strings.TrimSpace(item.ChatSessionID) == "" ||
		strings.TrimSpace(item.SourceRevision) == "" ||
		strings.TrimSpace(item.DocumentID) == "" {
		return D1Statement{}, errors.New("invalid memory vector outbox item")
	}
	contractVersion := firstNonEmptyString(item.ContractVersion, MemoryVectorOutboxContract)
	if item.Operation != "upsert" && item.Operation != "delete" {
		return D1Statement{}, fmt.Errorf("invalid vector outbox operation %q", item.Operation)
	}
	requiredSourceState := strings.TrimSpace(item.RequiredSourceState)
	if requiredSourceState == "" {
		if item.Operation == "upsert" {
			requiredSourceState = "active"
		} else {
			requiredSourceState = "inactive"
		}
	}
	status := firstNonEmptyString(item.Status, "pending")
	documentJSON := strings.TrimSpace(item.DocumentJSON)
	if documentJSON != "" && !json.Valid([]byte(documentJSON)) {
		return D1Statement{}, errors.New("invalid memory vector document JSON")
	}
	if item.Operation == "upsert" && documentJSON == "" {
		return D1Statement{}, errors.New("memory vector upsert document is required")
	}
	return D1Statement{
		SQL: `
		INSERT INTO memory_vector_outbox (
			contract_version, operation_key, operation, chat_session_id,
			source_revision, document_id, document_json, embedding_ready,
			required_source_state, status, attempts, retry_after, lease_owner,
			lease_until, last_error, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		Args: []any{
			contractVersion, item.OperationKey, item.Operation,
			item.ChatSessionID, item.SourceRevision, item.DocumentID,
			d1NullableString(documentJSON), d1BoolValue(item.EmbeddingReady),
			requiredSourceState, status, item.Attempts,
			d1NullableTime(item.RetryAfter), d1NullableString(item.LeaseOwner),
			d1NullableTime(item.LeaseUntil), d1NullableString(item.LastError),
			d1TimeValue(item.CreatedAt), d1TimeValue(item.UpdatedAt),
		},
	}, nil
}

// d1NullablePositiveInt64 maps a non-positive id to NULL so an absent foreign
// key is stored as NULL rather than as 0, which is not a row id.
func d1NullablePositiveInt64(value int64) any {
	if value <= 0 {
		return nil
	}
	return value
}
