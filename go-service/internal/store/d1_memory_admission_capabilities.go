package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// D1 canonical memory admission — the 3.6 write boundary.
//
// SavePreciseMemoryUnit writes one projection. CommitMemoryAdmission is the
// other thing entirely: it is the single place where one turn's Critic result
// becomes canonical rows, and every downstream reader trusts that either all of
// those rows exist or none of them do.
//
// What one admission commits
//
//   - the turn's compatibility aggregate Memory row (insert or update),
//   - the turn's Direct Evidence set, reconciled against what is already stored
//     (update, insert, tombstone of what the new extraction no longer cites,
//     and tombstone of pre-existing duplicates),
//   - the turn's precise memory units, with their derivation dependency edges
//     (insert, reactivate, or invalidate),
//   - the vector outbox operations for every artifact the admission decided on,
//     including the deletes for artifacts it retired, and
//   - the source revision marker that flips derived_admission_state to
//     'committed'.
//
// Why this is two D1 batches and not one
//
// The MariaDB reference does all of it in one transaction and learns each
// generated id as it goes: LastInsertId after INSERT INTO memories or
// direct_evidence_records, then that id becomes the document id
// "evidence:<session>:<id>" and then the input to
// memoryVectorOperationKey, which is a SHA-256 over (operation, session,
// revision, document id).
//
// D1 cannot reproduce that shape. D1Conn.Batch is the atomic boundary and it
// returns only an error, with no per-statement rows and no way to read between
// two statements of the same transaction. And the id cannot be recovered with a
// SQL expression either, because the operation key is a SHA-256 and SQLite has
// no SHA-256 function: a subquery can produce the document id, but nothing in
// SQL can hash it. (Registering a hash function in the local SQLite test harness
// would make the tests pass while production failed, which is worse than not
// having it.)
//
// Predicting the id from MAX(id) + 1 is not acceptable either: a second Cloudflare
// Container inserting concurrently would make the document id point at a row
// that does not exist, and the vector index would then hold a document whose
// source row is missing.
//
// So the commit is split, and the split is placed where it is safe:
//
//	batch 1 - canonical rows (memory, evidence, precise units, dependencies)
//	          generated ids are read back from the committed rows
//	batch 2 - vector outbox operations AND the derived_admission_state marker
//
// The marker is the commit point, and it sits in batch 2 on purpose. A crash
// between the batches therefore leaves the source revision still 'pending':
// the partial write is indistinguishable from no write, so the durable
// reprocessing lane re-runs the same stored Critic result, the reconcile finds
// every canonical row already present by its natural key, and the outbox
// operations are enqueued then. Self-healing, with no separate repair pass and
// no window in which a committed source marker is missing its vectors.
//
// The opposite ordering would not be safe. Committing the marker in batch 1
// would leave, on a crash between the batches, a source revision that claims to
// be committed with no outbox rows at all — and nothing in the current design
// would ever notice.
//
// Units linked to evidence inside batch 1
//
// A precise unit names the evidence it was derived from, and that evidence's id
// is assigned by the engine during the same batch. Those two columns are
// therefore written as scalar subqueries rather than as values. D1 executes a
// batch's statements in order against one database, so by the time the unit
// insert runs, the evidence insert before it has taken effect and the subquery
// resolves the real id. This is why root_evidence_id and
// direct_evidence_ids_json are the only two columns the admission overrides in
// the shared unit INSERT; everything else binds a value.
//
// Locking
//
// MariaDB takes SELECT ... FOR UPDATE on the source marker, the memory row, the
// evidence set, and the precise set. D1 has no row locks and the mutex that
// serialises here is process-local, so the same known limitation applies as
// everywhere else in this provider: two Containers committing the same turn
// concurrently rely on the batch being atomic and on the natural keys
// (chat_session_id + turn_index, evidence_text within a turn, unit_id) making
// the second writer's statements conflict rather than duplicate. Cross-container
// locking remains unresolved and is recorded rather than papered over.
//
// Retry
//
// The reference retries three times on MySQL error 1213, a deadlock. A SQLite
// transaction is a single writer and cannot deadlock, and matching D1's error
// strings to guess at a transient would be inventing a contract this provider
// cannot honour. So there is one attempt, and a failure is reported as one. The
// staging path below preserves the Critic result either way, so a retry is
// always available without re-running the extraction.

var _ MemoryAdmissionWriter = (*d1Store)(nil)
var _ MemoryAdmissionWriteAvailability = (*d1Store)(nil)

// MemoryAdmissionWritesEnabled reports whether this provider can commit an
// admission at all.
func (s *d1Store) MemoryAdmissionWritesEnabled() bool { return s != nil && s.conn != nil }

// CommitMemoryAdmission commits one turn's canonical memory projection.
func (s *d1Store) CommitMemoryAdmission(ctx context.Context, admission *MemoryAdmission) (MemoryAdmissionResult, error) {
	var result MemoryAdmissionResult
	if err := validateMemoryAdmission(admission); err != nil {
		return result, err
	}
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	result, err := s.d1CommitMemoryAdmissionOnce(ctx, admission)
	if err != nil {
		// The Critic result is the expensive part and it already succeeded. A
		// failed projection must not also lose it, so the result is staged for a
		// later worker to re-project without calling the Critic provider again.
		// A stale source is excluded: there is nothing left to project onto.
		if !errors.Is(err, ErrSourceRevisionStale) && ctx.Err() == nil {
			if stageErr := s.d1StageFailedAdmissionResult(ctx, admission); stageErr != nil {
				return result, fmt.Errorf("%w; stage successful critic result: %v", err, stageErr)
			}
		}
		return result, err
	}
	return result, nil
}

// d1StageFailedAdmissionResult records an already successful Critic result whose
// projection failed, so a later worker can retry the projection alone.
//
// The WHERE clause is a guard, not a filter: it matches only an active source
// that is not already committed, and the affected-row count is what proves the
// stage happened. A zero count is not automatically a conflict — it may mean the
// result is already staged — so the row is read back and the reference's two
// acceptable outcomes are reproduced exactly.
func (s *d1Store) d1StageFailedAdmissionResult(ctx context.Context, admission *MemoryAdmission) error {
	updatedAt := d1TimeValue(admission.CreatedAt)
	affected, err := s.conn.Exec(ctx, `
		UPDATE memory_source_revisions
		SET derived_admission_state = 'pending',
		    derived_admission_version = ?,
		    derived_extractor_version = ?,
		    derived_index_version = ?,
		    derived_result_hash = ?,
		    derived_result_json = ?,
		    derived_admitted_at = NULL,
		    updated_at = ?
		WHERE chat_session_id = ? AND source_revision = ? AND turn_index = ?
		  AND lifecycle_state = 'active'
		  AND derived_admission_state <> 'committed'
	`, admission.DerivationVersion, admission.ExtractorVersion,
		admission.IndexVersion, admission.ResultHash, admission.ResultJSON,
		updatedAt, admission.ChatSessionID, admission.SourceRevision, admission.TurnIndex)
	if err != nil {
		return err
	}
	if affected == 1 {
		return nil
	}

	var lifecycleState, admissionState, derivationVersion, extractorVersion, indexVersion string
	var resultHash *string
	err = s.conn.QueryRow(ctx, `
		SELECT lifecycle_state, derived_admission_state,
		       derived_admission_version, derived_extractor_version,
		       derived_index_version, derived_result_hash
		FROM memory_source_revisions
		WHERE chat_session_id = ? AND source_revision = ? AND turn_index = ?
	`, admission.ChatSessionID, admission.SourceRevision, admission.TurnIndex).Scan(
		&lifecycleState, &admissionState, &derivationVersion,
		&extractorVersion, &indexVersion, &resultHash)
	if errors.Is(err, errD1NoRows) {
		return ErrSourceRevisionStale
	}
	if err != nil {
		return err
	}
	if lifecycleState != "active" {
		return ErrSourceRevisionStale
	}
	if (admissionState == "pending" || admissionState == "committed") &&
		derivationVersion == admission.DerivationVersion &&
		extractorVersion == admission.ExtractorVersion &&
		indexVersion == admission.IndexVersion &&
		d1DerefString(resultHash) == admission.ResultHash {
		return nil
	}
	return errors.New("memory admission result staging conflict")
}

// ---------------------------------------------------------------------------
// pre-read state
// ---------------------------------------------------------------------------

// d1AdmissionPlaceholders renders n comma-separated bind markers with no
// trailing comma. A trailing comma inside "IN (...)" is a parse error in SQLite,
// not an empty set, so this must never be assembled by appending "?," n times.
func d1AdmissionPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// d1AdmissionMarker is the source revision's admission marker. It is the
// fence and the idempotency record in one row: a marker that is not active
// refuses the admission, and a committed marker with matching versions is what
// makes a replay a replay.
type d1AdmissionMarker struct {
	lifecycle         string
	admissionState    string
	derivationVersion string
	extractorVersion  string
	indexVersion      string
	resultHash        string
	resultJSON        string
}

func (s *d1Store) d1ReadAdmissionMarker(ctx context.Context, admission *MemoryAdmission) (d1AdmissionMarker, error) {
	var marker d1AdmissionMarker
	var resultHash, resultJSON *string
	err := s.conn.QueryRow(ctx, `
		SELECT lifecycle_state, derived_admission_state,
		       derived_admission_version, derived_extractor_version,
		       derived_index_version, derived_result_hash, derived_result_json
		FROM memory_source_revisions
		WHERE chat_session_id = ? AND source_revision = ? AND turn_index = ?
	`, admission.ChatSessionID, admission.SourceRevision, admission.TurnIndex).Scan(
		&marker.lifecycle, &marker.admissionState, &marker.derivationVersion,
		&marker.extractorVersion, &marker.indexVersion, &resultHash, &resultJSON)
	if errors.Is(err, errD1NoRows) {
		// A source revision that does not exist is indistinguishable from one
		// that was never accepted, and both mean the same thing here: there is
		// no active source to project onto.
		return marker, ErrSourceRevisionStale
	}
	if err != nil {
		return marker, err
	}
	marker.resultHash = d1DerefString(resultHash)
	marker.resultJSON = d1DerefString(resultJSON)
	return marker, nil
}

// d1AdmissionEvidenceRow is one stored evidence row for the turn being admitted.
type d1AdmissionEvidenceRow struct {
	id         int64
	text       string
	tombstoned bool
}

func (s *d1Store) d1ReadAdmissionEvidence(ctx context.Context, admission *MemoryAdmission) ([]d1AdmissionEvidenceRow, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, evidence_text, tombstoned
		FROM direct_evidence_records
		WHERE chat_session_id = ?
		  AND source_turn_start = ? AND source_turn_end = ?
		  AND capture_stage = 'critic_extract'
		ORDER BY id
	`, admission.ChatSessionID, admission.TurnIndex, admission.TurnIndex)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []d1AdmissionEvidenceRow
	for rows.Next() {
		var row d1AdmissionEvidenceRow
		if err := rows.Scan(&row.id, &row.text, &row.tombstoned); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// d1AdmissionPreciseRow is one stored precise unit for the source revision.
type d1AdmissionPreciseRow struct {
	id             int64
	unitID         string
	idempotencyKey string
	lifecycle      string
}

func (s *d1Store) d1ReadAdmissionPrecise(ctx context.Context, admission *MemoryAdmission) ([]d1AdmissionPreciseRow, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT id, unit_id, idempotency_key, lifecycle_state
		FROM precise_memory_units
		WHERE chat_session_id = ? AND source_revision = ?
		ORDER BY id
	`, admission.ChatSessionID, admission.SourceRevision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []d1AdmissionPreciseRow
	for rows.Next() {
		var row d1AdmissionPreciseRow
		if err := rows.Scan(&row.id, &row.unitID, &row.idempotencyKey, &row.lifecycle); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// d1AdmissionOutboxRow is the state the outbox duplicate and lease checks need.
type d1AdmissionOutboxRow struct {
	id             int64
	operationKey   string
	operation      string
	chatSessionID  string
	sourceRevision string
	documentID     string
	documentJSON   string
	embeddingReady bool
	requiredSource string
	status         string
	leaseUntil     *string
}

// d1AdmissionReadOutbox reads exactly the outbox rows the batch-2 decisions
// depend on: the rows keyed by the operation keys about to be used, and the rows
// sharing a document id with them.
//
// The second set is what the replay branch's supersession check needs — a newer
// operation for the same document has a DIFFERENT operation key, so scoping the
// read by key alone would miss exactly the rows that can refuse the write.
// Reading the whole session's outbox instead would be correct but unbounded, so
// the read is scoped by an explicit IN list of document ids.
func (s *d1Store) d1ReadOutboxForAdmissions(
	ctx context.Context,
	admission *MemoryAdmission,
	operationKeys []string,
	documentIDs []string,
) (map[string]d1AdmissionOutboxRow, map[string][]d1AdmissionOutboxRow, error) {
	byKey := map[string]d1AdmissionOutboxRow{}
	byDocument := map[string][]d1AdmissionOutboxRow{}
	if len(operationKeys) == 0 && len(documentIDs) == 0 {
		return byKey, byDocument, nil
	}
	// The two IN lists are built separately. Reusing one placeholder string for
	// both would put a trailing comma in whichever list is shorter, and
	// "IN (?,?,)" is a parse error rather than an empty match.
	// The two IN lists are read in chunks. A turn that admits many memories
	// produces one operation key and one document id per projection, so the
	// combined list grows with the turn: it stays under D1's bound-parameter
	// limit for ordinary turns and crosses it for a long one. That produced
	// "too many SQL variables" only in deployment, because the local harness
	// runs SQLite and allows far more bindings than D1 does.
	const chunkReserved = 1 // chat_session_id
	statement := `
		SELECT id, operation_key, operation, chat_session_id, source_revision,
		       document_id, COALESCE(document_json, ''), embedding_ready,
		       required_source_state, status, lease_until
		FROM memory_vector_outbox
		WHERE chat_session_id = ? AND (%s)`
	collect := func(query string, args ...any) error {
		rows, err := s.conn.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row d1AdmissionOutboxRow
			if err := rows.Scan(&row.id, &row.operationKey, &row.operation, &row.chatSessionID,
				&row.sourceRevision, &row.documentID, &row.documentJSON, &row.embeddingReady,
				&row.requiredSource, &row.status, &row.leaseUntil); err != nil {
				return err
			}
			byKey[row.operationKey] = row
			byDocument[row.documentID] = append(byDocument[row.documentID], row)
		}
		return rows.Err()
	}
	for _, keys := range d1BindChunks(operationKeys, chunkReserved) {
		args := make([]any, 0, len(keys)+chunkReserved)
		args = append(args, admission.ChatSessionID)
		for _, key := range keys {
			args = append(args, key)
		}
		query := fmt.Sprintf(statement, "operation_key IN ("+d1AdmissionPlaceholders(len(keys))+")")
		if err := collect(query, args...); err != nil {
			return nil, nil, err
		}
	}
	for _, ids := range d1BindChunks(documentIDs, chunkReserved) {
		args := make([]any, 0, len(ids)+chunkReserved)
		args = append(args, admission.ChatSessionID)
		for _, id := range ids {
			args = append(args, id)
		}
		query := fmt.Sprintf(statement, "document_id IN ("+d1AdmissionPlaceholders(len(ids))+")")
		if err := collect(query, args...); err != nil {
			return nil, nil, err
		}
	}
	return byKey, byDocument, nil
}

// ---------------------------------------------------------------------------
// the commit
// ---------------------------------------------------------------------------

// d1AdmissionPlan carries the decisions batch 1 makes and batch 2 executes, so
// the two phases cannot disagree about what the admission decided.
type d1AdmissionPlan struct {
	result MemoryAdmissionResult

	// Vector operations decided in batch 1 and emitted in batch 2.
	retiredEvidence      []d1AdmissionVectorDecision
	duplicateEvidence    []d1AdmissionVectorDecision
	privatePrecise       []d1AdmissionVectorDecision
	retiredPrecise       []d1AdmissionVectorDecision
	eligibilityReconcile []d1AdmissionVectorDecision
	preciseVectorUnits   []*PreciseMemoryUnit
	excludePublicMemory  bool
	admissionVectorItems []MemoryAdmissionVector
}

// d1AdmissionVectorDecision is one document whose vector projection this
// admission retired, together with the reason recorded on the delete.
type d1AdmissionVectorDecision struct {
	documentID string
	reason     string
	// clearRetryAfter distinguishes the two cancel shapes the reference uses.
	// The retired-artifact sweeps clear retry_after; the private and replaced
	// precise-unit sweeps deliberately do not, so a document that keeps being
	// retired retains its backoff history. The difference is reproduced rather
	// than unified.
	clearRetryAfter bool
}

func (s *d1Store) d1CommitMemoryAdmissionOnce(ctx context.Context, admission *MemoryAdmission) (MemoryAdmissionResult, error) {
	var result MemoryAdmissionResult
	vectorReplay := memoryAdmissionVectorReplayFromContext(ctx)

	marker, err := s.d1ReadAdmissionMarker(ctx, admission)
	if err != nil {
		return result, err
	}
	// The fence. A source revision that is not active has been rolled back or
	// superseded, and projecting onto it would resurrect what the user discarded.
	if strings.TrimSpace(marker.lifecycle) != "active" {
		return result, ErrSourceRevisionStale
	}
	if marker.admissionState == "committed" &&
		marker.derivationVersion == admission.DerivationVersion &&
		marker.extractorVersion == admission.ExtractorVersion &&
		marker.indexVersion == admission.IndexVersion {
		if vectorReplay.Refresh {
			// A replay must not rewrite a committed extraction. It may rebuild
			// its projections, but only from the extraction that was committed.
			if marker.resultHash != admission.ResultHash || marker.resultJSON != admission.ResultJSON {
				return result, errors.New("memory admission committed result conflict")
			}
		} else {
			// The idempotent replay. Nothing is written at all, and the stored
			// result is handed back so the caller can report which extraction
			// actually won.
			result.Idempotent = true
			result.ExistingResultHash = marker.resultHash
			result.ExistingResultJSON = marker.resultJSON
			result.CommittedResultHash = marker.resultHash
			return result, nil
		}
	}

	existingEvidence, err := s.d1ReadAdmissionEvidence(ctx, admission)
	if err != nil {
		return result, err
	}
	existingPrecise, err := s.d1ReadAdmissionPrecise(ctx, admission)
	if err != nil {
		return result, err
	}
	existingMemoryID, err := s.d1ReadAdmissionMemoryID(ctx, admission)
	if err != nil {
		return result, err
	}

	plan := &d1AdmissionPlan{
		result:               result,
		excludePublicMemory:  admission.MemoryPublicProjectionExcluded,
		admissionVectorItems: admission.Vectors,
	}

	// ---- batch 1: canonical rows ----
	statements, err := s.d1AdmissionCanonicalStatements(ctx, admission, plan, existingMemoryID, existingEvidence, existingPrecise)
	if err != nil {
		return result, err
	}
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return result, err
	}

	// ---- read back the ids the engine assigned ----
	memoryID, evidenceByText, err := s.d1ReadAdmissionIDs(ctx, admission)
	if err != nil {
		return result, err
	}

	// ---- batch 2: vector outbox operations and the commit marker ----
	vectorStatements, err := s.d1AdmissionVectorStatements(ctx, admission, plan, memoryID, evidenceByText, vectorReplay)
	if err != nil {
		return result, err
	}
	vectorStatements = append(vectorStatements, D1Statement{
		SQL: `
		UPDATE memory_source_revisions
		SET derived_admission_state = 'committed',
		    derived_admission_version = ?,
		    derived_extractor_version = ?,
		    derived_index_version = ?,
		    derived_result_hash = ?,
		    derived_result_json = ?,
		    derived_admitted_at = ?,
		    updated_at = ?
		WHERE chat_session_id = ? AND source_revision = ?
		  AND lifecycle_state = 'active'
		`,
		Args: []any{admission.DerivationVersion, admission.ExtractorVersion,
			admission.IndexVersion, admission.ResultHash, admission.ResultJSON,
			d1TimeValue(admission.CreatedAt), d1TimeValue(admission.CreatedAt),
			admission.ChatSessionID, admission.SourceRevision},
	})
	if err := s.conn.Batch(ctx, vectorStatements...); err != nil {
		return result, err
	}

	// The caller's evidence slice is the returned result of an admitted turn, so
	// the assigned ids are written back into it. They were read from committed
	// rows rather than predicted, which is the same post-write readback the
	// precise unit writer uses.
	if admission.Memory != nil {
		admission.Memory.ID = memoryID
	}
	for _, evidence := range admission.Evidence {
		if evidence == nil {
			continue
		}
		if id, ok := evidenceByText[strings.TrimSpace(evidence.EvidenceText)]; ok {
			evidence.ID = id
		}
	}

	admittedAt := d1TimeValue(admission.CreatedAt)
	plan.result.CommittedResultHash = admission.ResultHash
	if parsed, parseErr := parseD1Time(admittedAt); parseErr == nil {
		plan.result.CommittedAt = parsed
	}
	return plan.result, nil
}

func (s *d1Store) d1ReadAdmissionMemoryID(ctx context.Context, admission *MemoryAdmission) (int64, error) {
	var id int64
	err := s.conn.QueryRow(ctx, `
		SELECT id FROM memories
		WHERE chat_session_id = ? AND turn_index = ?
		ORDER BY id
		LIMIT 1
	`, admission.ChatSessionID, admission.TurnIndex).Scan(&id)
	if errors.Is(err, errD1NoRows) {
		return 0, nil
	}
	return id, err
}

// d1ReadAdmissionIDs reads the ids batch 1 assigned, keyed the way the admission
// keys them: the memory by (session, turn), and each evidence row by its text
// within the turn.
//
// Keying evidence by text is not a convenience. The reference gets the id from
// LastInsertId at insert time and puts it straight into the precise unit's
// root_evidence_id; this provider has to find the row the insert produced, and
// evidence_text is the key the reconcile itself deduplicates on, so the readback
// resolves exactly the row the admission meant.
func (s *d1Store) d1ReadAdmissionIDs(ctx context.Context, admission *MemoryAdmission) (int64, map[string]int64, error) {
	memoryID, err := s.d1ReadAdmissionMemoryID(ctx, admission)
	if err != nil {
		return 0, nil, err
	}
	rows, err := s.conn.Query(ctx, `
		SELECT id, evidence_text
		FROM direct_evidence_records
		WHERE chat_session_id = ?
		  AND source_turn_start = ? AND source_turn_end = ?
		  AND capture_stage = 'critic_extract' AND tombstoned = 0
		ORDER BY id
	`, admission.ChatSessionID, admission.TurnIndex, admission.TurnIndex)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()

	evidenceByText := map[string]int64{}
	for rows.Next() {
		var id int64
		var text string
		if err := rows.Scan(&id, &text); err != nil {
			return 0, nil, err
		}
		evidenceByText[strings.TrimSpace(text)] = id
	}
	return memoryID, evidenceByText, rows.Err()
}

// d1AdmissionRootEvidenceSQL resolves the evidence a precise unit was derived
// from, evaluated inside the batch after the evidence statements have run.
//
// tombstoned = 0 is part of the lookup, not a convenience: a unit must not be
// linked to evidence the same admission just retired, or it would carry a
// derivation edge to a row that is no longer part of the turn.
const d1AdmissionRootEvidenceSQL = `(SELECT r.id FROM direct_evidence_records r
	WHERE r.chat_session_id = ? AND r.evidence_text = ?
	  AND r.source_turn_start = ? AND r.source_turn_end = ?
	  AND r.capture_stage = 'critic_extract' AND r.tombstoned = 0
	ORDER BY r.id LIMIT 1)`

// d1AdmissionDirectEvidenceIDsSQL renders that same id as the one-element JSON
// array the reference stores. The reference builds it in Go from a real id; here
// the id is only knowable to the engine, so the array is composed in SQL.
const d1AdmissionDirectEvidenceIDsSQL = `('[' || ` + d1AdmissionRootEvidenceSQL + ` || ']')`

// d1AdmissionCanonicalStatements builds batch 1: the compatibility aggregate,
// the reconciled evidence set, the precise units, and their dependency edges.
//
// The statement order is the reference's execution order. It is not arbitrary:
// a unit's root evidence subquery must run after the evidence statements, and
// the dependency invalidation for a version change must run before the dependency
// rows for the new version are written.
func (s *d1Store) d1AdmissionCanonicalStatements(
	ctx context.Context,
	admission *MemoryAdmission,
	plan *d1AdmissionPlan,
	existingMemoryID int64,
	existingEvidence []d1AdmissionEvidenceRow,
	existingPrecise []d1AdmissionPreciseRow,
) ([]D1Statement, error) {
	statements := make([]D1Statement, 0, 8+len(admission.Evidence)*2+len(admission.PreciseUnits)*3)

	if mem := admission.Memory; mem != nil {
		if existingMemoryID > 0 {
			// The aggregate is keyed by (session, turn) and there may be more
			// than one row for that key, so the update names the id the read
			// chose. Updating every matching row instead would rewrite rows a
			// previous admission had already retired.
			statements = append(statements, D1Statement{
				SQL: `
				UPDATE memories
				SET summary_json = ?, embedding = ?, embedding_model = ?,
				    importance = ?, emotional_boost = ?, evidence = ?,
				    emotional_intensity = ?, narrative_significance = ?,
				    place_wing = ?, place_room = ?
				WHERE id = ? AND chat_session_id = ?
				`, Args: []any{d1NullableString(mem.SummaryJSON), d1NullableString(mem.Embedding),
					d1NullableString(mem.EmbeddingModel), mem.Importance, mem.EmotionalBoost,
					d1NullableString(mem.Evidence), mem.EmotionalIntensity,
					mem.NarrativeSignificance, d1NullableString(mem.PlaceWing),
					d1NullableString(mem.PlaceRoom), existingMemoryID, mem.ChatSessionID},
			})
			plan.result.MemoryUpdated = true
		} else {
			statements = append(statements, D1Statement{
				SQL: `
				INSERT INTO memories (
					chat_session_id, turn_index, summary_json, embedding, embedding_model,
					importance, emotional_boost, evidence, emotional_intensity,
					narrative_significance, place_wing, place_room, created_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				`, Args: []any{mem.ChatSessionID, mem.TurnIndex, d1NullableString(mem.SummaryJSON),
					d1NullableString(mem.Embedding), d1NullableString(mem.EmbeddingModel),
					mem.Importance, mem.EmotionalBoost, d1NullableString(mem.Evidence),
					mem.EmotionalIntensity, mem.NarrativeSignificance,
					d1NullableString(mem.PlaceWing), d1NullableString(mem.PlaceRoom),
					d1TimeValue(mem.CreatedAt)},
			})
			plan.result.MemoryInserted = true
		}
	}

	desiredEvidence := map[string]bool{}
	existingByText := map[string]d1AdmissionEvidenceRow{}
	var duplicates []d1AdmissionEvidenceRow
	for _, row := range existingEvidence {
		key := strings.TrimSpace(row.text)
		if _, seen := existingByText[key]; seen {
			// Two stored rows carry the same text. The first survives and the
			// rest are tombstoned, so one fact cannot be read back twice.
			duplicates = append(duplicates, row)
			continue
		}
		existingByText[key] = row
	}

	for _, evidence := range admission.Evidence {
		if evidence == nil {
			continue
		}
		key := strings.TrimSpace(evidence.EvidenceText)
		if key == "" || desiredEvidence[key] {
			continue
		}
		desiredEvidence[key] = true
		if prior, ok := existingByText[key]; ok {
			statements = append(statements, D1Statement{
				SQL: `
				UPDATE direct_evidence_records
				SET evidence_kind = ?, source_message_ids_json = ?, source_hash = ?,
				    archive_state = ?, capture_verification = ?, committed_gate = ?,
				    lineage_json = ?, repair_needed = FALSE, tombstoned = FALSE,
				    superseded_by_id = NULL
				WHERE id = ? AND chat_session_id = ?
				`, Args: []any{evidence.EvidenceKind, d1NullableString(evidence.SourceMessageIDsJSON),
					d1NullableString(evidence.SourceHash), evidence.ArchiveState,
					evidence.CaptureVerification, d1NullableString(evidence.CommittedGate),
					d1NullableString(evidence.LineageJSON), prior.id, admission.ChatSessionID},
			})
			if prior.tombstoned {
				plan.result.EvidenceReactivated++
			}
			continue
		}
		statements = append(statements, D1Statement{
			SQL: `
			INSERT INTO direct_evidence_records (
				chat_session_id, evidence_kind, evidence_text, source_turn_start,
				source_turn_end, turn_anchor, source_message_ids_json, source_hash,
				archive_state, capture_stage, capture_verification, committed_gate,
				lineage_json, repair_needed, tombstoned, superseded_by_id, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, Args: []any{evidence.ChatSessionID, evidence.EvidenceKind, evidence.EvidenceText,
				evidence.SourceTurnStart, evidence.SourceTurnEnd, evidence.TurnAnchor,
				d1NullableString(evidence.SourceMessageIDsJSON), d1NullableString(evidence.SourceHash),
				evidence.ArchiveState, evidence.CaptureStage, evidence.CaptureVerification,
				d1NullableString(evidence.CommittedGate), d1NullableString(evidence.LineageJSON),
				d1BoolValue(evidence.RepairNeeded), d1BoolValue(evidence.Tombstoned),
				d1NullablePositiveInt64(evidence.SupersededByID), d1TimeValue(evidence.CreatedAt)},
		})
		plan.result.EvidenceInserted++
	}

	// Evidence the new extraction no longer cites is tombstoned rather than
	// deleted, so the vector delete can retire its document and a later
	// extraction that cites it again finds the row and reactivates it.
	for key, prior := range existingByText {
		if desiredEvidence[key] || prior.tombstoned {
			continue
		}
		statements = append(statements, d1AdmissionTombstoneEvidence(prior.id, admission.ChatSessionID))
		plan.retiredEvidence = append(plan.retiredEvidence, d1AdmissionVectorDecision{
			documentID:      d1AdmissionEvidenceDocumentID(admission.ChatSessionID, prior.id),
			reason:          "retired_evidence",
			clearRetryAfter: true,
		})
		plan.result.EvidenceRetired++
	}
	for _, duplicate := range duplicates {
		if duplicate.tombstoned {
			continue
		}
		statements = append(statements, d1AdmissionTombstoneEvidence(duplicate.id, admission.ChatSessionID))
		plan.duplicateEvidence = append(plan.duplicateEvidence, d1AdmissionVectorDecision{
			documentID:      d1AdmissionEvidenceDocumentID(admission.ChatSessionID, duplicate.id),
			reason:          "duplicate_evidence",
			clearRetryAfter: true,
		})
		plan.result.EvidenceRetired++
	}

	preciseStatements, err := s.d1AdmissionPreciseStatements(ctx, admission, plan, existingPrecise, desiredEvidence)
	if err != nil {
		return nil, err
	}
	return append(statements, preciseStatements...), nil
}

func d1AdmissionTombstoneEvidence(id int64, chatSessionID string) D1Statement {
	return D1Statement{
		SQL: `
		UPDATE direct_evidence_records
		SET tombstoned = TRUE, repair_needed = FALSE
		WHERE id = ? AND chat_session_id = ?
		`, Args: []any{id, chatSessionID},
	}
}

func d1AdmissionEvidenceDocumentID(chatSessionID string, id int64) string {
	return "evidence:" + chatSessionID + ":" + strconv.FormatInt(id, 10)
}

func d1AdmissionPreciseDocumentID(chatSessionID, unitID string) string {
	return "precise_memory:" + chatSessionID + ":" + unitID
}

// d1AdmissionPreciseStatements builds the precise unit statements and their
// dependency edges.
//
// desiredEvidence is the set of evidence texts that will be LIVE after batch 1.
// It is exactly the set the evidence reconcile writes or refreshes, because
// every other stored row for the turn is tombstoned by that same reconcile. That
// makes it the precise check for "this unit's evidence was committed": a unit
// whose excerpt is not in the set is refused before any statement runs, which
// is the reference's guard against projecting a memory onto evidence that does
// not exist.
func (s *d1Store) d1AdmissionPreciseStatements(
	ctx context.Context,
	admission *MemoryAdmission,
	plan *d1AdmissionPlan,
	existingPrecise []d1AdmissionPreciseRow,
	desiredEvidence map[string]bool,
) ([]D1Statement, error) {
	statements := make([]D1Statement, 0, 4*len(admission.PreciseUnits))
	existingByUnit := make(map[string]d1AdmissionPreciseRow, len(existingPrecise))
	for _, row := range existingPrecise {
		existingByUnit[row.unitID] = row
	}

	desiredUnits := map[string]bool{}
	for _, unit := range admission.PreciseUnits {
		if unit == nil || desiredUnits[unit.UnitID] {
			continue
		}
		desiredUnits[unit.UnitID] = true
		if !desiredEvidence[strings.TrimSpace(unit.EvidenceExcerpt)] {
			return nil, fmt.Errorf("precise memory evidence was not committed: %s", unit.EvidenceHash)
		}
		documentID := d1AdmissionPreciseDocumentID(admission.ChatSessionID, unit.UnitID)

		if prior, ok := existingByUnit[unit.UnitID]; ok {
			// A unit_id that already exists with a different idempotency key is
			// not the same derivation. Keeping the stored row would silently
			// discard this one.
			if prior.idempotencyKey != unit.IdempotencyKey {
				return nil, errors.New("precise memory idempotency conflict")
			}
			reactivationArgs := append(d1AdmissionRootEvidenceArgPairs(admission, unit),
				d1TimeValue(unit.UpdatedAt), prior.id, unit.SourceRevision, unit.IdempotencyKey)
			statements = append(statements, D1Statement{
				SQL: d1AdmissionPreciseReactivateSQL(), Args: reactivationArgs,
			})
			// A derivation that changed version is a different derivation, so the
			// old edges are invalidated before the new ones are written.
			statements = append(statements, D1Statement{
				SQL: `
				UPDATE memory_derivation_dependencies
				SET lifecycle_state = 'invalidated', invalidated_at = ?, updated_at = ?
				WHERE source_revision = ?
				  AND child_artifact_type = 'precise_memory_unit'
				  AND child_artifact_id = ?
				  AND lifecycle_state = 'active'
				  AND (
				    derivation_version <> ? OR extractor_version <> ?
				    OR index_version <> ?
				  )
				`, Args: []any{d1TimeValue(admission.CreatedAt), d1TimeValue(admission.CreatedAt),
					unit.SourceRevision, unit.UnitID, unit.DerivationVersion,
					unit.ExtractorVersion, unit.IndexVersion},
			})
			dependencyStatements, err := d1AdmissionDependencyStatements(admission, unit)
			if err != nil {
				return nil, err
			}
			statements = append(statements, dependencyStatements...)
			if !preciseMemoryGeneralVectorEligible(unit) {
				plan.privatePrecise = append(plan.privatePrecise, d1AdmissionVectorDecision{
					documentID:      documentID,
					reason:          "private_precise_memory",
					clearRetryAfter: false,
				})
			}
			plan.preciseVectorUnits = append(plan.preciseVectorUnits, unit)
			if prior.lifecycle != "active" {
				plan.result.PreciseReactivated++
			}
			continue
		}

		insertArgs := d1AdmissionPreciseUnitArgs(admission, unit)
		if insertArgs == nil {
			return nil, errors.New("precise memory unit column order changed; the admission statement must be rebuilt")
		}
		statements = append(statements, D1Statement{
			SQL: d1PreciseMemoryUnitInsertSQL(map[string]string{
				"root_evidence_id":         d1AdmissionRootEvidenceSQL,
				"direct_evidence_ids_json": d1AdmissionDirectEvidenceIDsSQL,
			}),
			Args: insertArgs,
		})
		dependencyStatements, err := d1AdmissionDependencyStatements(admission, unit)
		if err != nil {
			return nil, err
		}
		statements = append(statements, dependencyStatements...)
		plan.result.PreciseInserted++
		plan.preciseVectorUnits = append(plan.preciseVectorUnits, unit)
	}

	for unitID, prior := range existingByUnit {
		if desiredUnits[unitID] || prior.lifecycle != "active" {
			continue
		}
		statements = append(statements, D1Statement{
			SQL: `
			UPDATE precise_memory_units
			SET lifecycle_state = 'invalidated', updated_at = ?
			WHERE id = ? AND source_revision = ?
			`, Args: []any{d1TimeValue(admission.CreatedAt), prior.id, admission.SourceRevision},
		})
		statements = append(statements, D1Statement{
			SQL: `
			UPDATE memory_derivation_dependencies
			SET lifecycle_state = 'invalidated', invalidated_at = ?, updated_at = ?
			WHERE source_revision = ? AND child_artifact_type = 'precise_memory_unit'
			  AND child_artifact_id = ? AND lifecycle_state = 'active'
			`, Args: []any{d1TimeValue(admission.CreatedAt), d1TimeValue(admission.CreatedAt),
				admission.SourceRevision, unitID},
		})
		plan.retiredPrecise = append(plan.retiredPrecise, d1AdmissionVectorDecision{
			documentID:      d1AdmissionPreciseDocumentID(admission.ChatSessionID, unitID),
			reason:          "retired_precise_memory",
			clearRetryAfter: false,
		})
		plan.result.PreciseRetired++
	}
	return statements, nil
}

// d1AdmissionDependencyStatements builds one unit's derivation edges.
//
// It exists separately from d1PreciseMemoryDependencyStatements because that
// builder reads the evidence edge from unit.RootEvidenceID, a value the
// standalone writer has in hand. Here the id only exists inside SQL, so the edge
// is written as CAST(<subquery> AS TEXT).
//
// The NOT NULL column is the backstop: if the subquery resolved to NULL the
// whole batch would fail, which is a safer outcome than writing a dependency edge
// pointing at nothing. It should not be reachable, because the caller has
// already refused the admission when the unit's evidence is not among the
// evidence the reconcile will keep.
func d1AdmissionDependencyStatements(admission *MemoryAdmission, unit *PreciseMemoryUnit) ([]D1Statement, error) {
	statements, err := d1PreciseMemoryDependencyStatements(unit)
	if err != nil {
		return nil, err
	}
	args := []any{
		MemoryDerivationDependencyContract, admission.ChatSessionID,
		admission.SourceRevision, "source_revision:" + admission.SourceRevision, unit.UnitID,
	}
	args = append(args, d1AdmissionRootEvidenceArgs(admission, unit)...)
	return append(statements, D1Statement{
		SQL: `
		INSERT INTO memory_derivation_dependencies (
			contract_version, chat_session_id, source_revision,
			root_source_pointer, child_artifact_type, child_artifact_id,
			parent_artifact_type, parent_artifact_id, derivation_version,
			extractor_version, index_version, lifecycle_state, created_at,
			updated_at
		) VALUES (?, ?, ?, ?, 'precise_memory_unit', ?, 'direct_evidence',
		          CAST(` + d1AdmissionRootEvidenceSQL + ` AS TEXT), ?, ?, ?, 'active', ?, ?)
		ON CONFLICT DO NOTHING`,
		Args: append(args,
			firstNonEmptyString(unit.DerivationVersion, PreciseMemoryUnitContract),
			firstNonEmptyString(unit.ExtractorVersion, "complete_turn.configured_critic_extract"),
			firstNonEmptyString(unit.IndexVersion, "not_materialized"),
			d1TimeValue(unit.CreatedAt), d1TimeValue(unit.UpdatedAt)),
	}), nil
}

// d1AdmissionPreciseReactivateSQL is the reactivation update. The two evidence
// columns are subqueries for the same reason they are on the insert path: the
// evidence id is assigned by the engine during this batch.
func d1AdmissionPreciseReactivateSQL() string {
	return `
		UPDATE precise_memory_units
		SET root_evidence_id = ` + d1AdmissionRootEvidenceSQL + `,
		    direct_evidence_ids_json = ` + d1AdmissionDirectEvidenceIDsSQL + `,
		    lifecycle_state = 'active', updated_at = ?
		WHERE id = ? AND source_revision = ? AND idempotency_key = ?`
}

// d1AdmissionRootEvidenceArgs binds the four placeholders ONE occurrence of the
// root evidence subquery uses, in the order they appear in the statement.
func d1AdmissionRootEvidenceArgs(admission *MemoryAdmission, unit *PreciseMemoryUnit) []any {
	return []any{
		admission.ChatSessionID, strings.TrimSpace(unit.EvidenceExcerpt),
		admission.TurnIndex, admission.TurnIndex,
	}
}

// d1AdmissionRootEvidenceArgPairs binds BOTH occurrences.
//
// The two evidence columns are independent copies of the same subquery, so the
// statement contains it twice and needs eight placeholders, not four. Binding
// one set for two occurrences would shift every later argument by four
// positions and corrupt the tail of the row.
func d1AdmissionRootEvidenceArgPairs(admission *MemoryAdmission, unit *PreciseMemoryUnit) []any {
	single := d1AdmissionRootEvidenceArgs(admission, unit)
	out := make([]any, 0, 2*len(single))
	out = append(out, single...)
	return append(out, single...)
}

// d1AdmissionOverriddenColumnIndex returns the position of root_evidence_id in
// the shared column list, and checks that direct_evidence_ids_json is the column
// immediately after it.
//
// That adjacency is what lets the argument list be split as leading / subquery /
// subquery / trailing without hard-coding a number. If a future column is
// inserted between them, this returns -1 and the caller falls back to building
// the argument list from the column order directly.
func d1AdmissionOverriddenColumnIndex() int {
	order := d1PreciseMemoryUnitArgsOrder()
	for i, column := range order {
		if column != "root_evidence_id" {
			continue
		}
		if i+1 < len(order) && order[i+1] == "direct_evidence_ids_json" {
			return i
		}
		return -1
	}
	return -1
}

// d1AdmissionPreciseUnitArgs binds the unit columns the admission INSERT does
// NOT override, placed around the two subquery argument pairs exactly as the
// generated statement places them.
//
// It is derived from the shared argument list positionally rather than restated,
// so a column added to the unit cannot be silently forgotten here and no
// argument can drift into the wrong column.
func d1AdmissionPreciseUnitArgs(admission *MemoryAdmission, unit *PreciseMemoryUnit) []any {
	full := d1PreciseMemoryUnitArgs(unit)
	pairs := d1AdmissionRootEvidenceArgPairs(admission, unit)
	split := d1AdmissionOverriddenColumnIndex()
	out := make([]any, 0, len(full)-2+len(pairs))
	if split < 0 {
		// The columns are no longer adjacent, so the statement's placeholder
		// order is not the shared order. Refusing is better than binding the
		// arguments to the wrong positions.
		return nil
	}
	out = append(out, full[:split]...)
	out = append(out, pairs...)
	return append(out, full[split+2:]...)
}

// ---------------------------------------------------------------------------
// batch 2: vector outbox operations
// ---------------------------------------------------------------------------

// d1AdmissionVectorStatements builds batch 2: every vector operation the
// admission decided on, followed by nothing else — the commit marker is appended
// by the caller so it is always the last statement of the batch.
//
// The outbox rows this batch depends on are read once, scoped to the operation
// keys and document ids about to be touched. That read is what lets the
// duplicate decision be made before the batch: a D1 batch cannot report which of
// its statements was suppressed, so a statement that must not be emitted has to
// be decided here.
func (s *d1Store) d1AdmissionVectorStatements(
	ctx context.Context,
	admission *MemoryAdmission,
	plan *d1AdmissionPlan,
	memoryID int64,
	evidenceByText map[string]int64,
	vectorReplay memoryAdmissionVectorReplayOptions,
) ([]D1Statement, error) {
	// The eligibility reconcile is a replay-only pass that retires vector
	// projections the canonical admission deliberately omitted. It is decided
	// here, before the identities are collected, because its candidate documents
	// are built from the ids the engine assigned during batch 1.
	if vectorReplay.ReconcileEligibility {
		plan.eligibilityReconcile = d1AdmissionEligibilityDecisions(admission, plan, memoryID, evidenceByText)
	}

	documents, operationKeys, err := s.d1AdmissionVectorIdentities(ctx, admission, plan, memoryID, evidenceByText)
	if err != nil {
		return nil, err
	}
	byKey, byDocument, err := s.d1ReadOutboxForAdmissions(ctx, admission, operationKeys, documents)
	if err != nil {
		return nil, err
	}

	statements := make([]D1Statement, 0, len(documents)*2+len(plan.preciseVectorUnits))
	// The admission fenced on the source revision before any of this ran, so its
	// live state is the one the reference's JOIN would have read.
	sourceState := "active"
	enqueue := func(item *MemoryVectorOutboxItem) error {
		statement, queued, err := s.d1AdmissionEnqueueVector(byKey, byDocument, sourceState, item, vectorReplay)
		if err != nil {
			return err
		}
		if statement.SQL != "" {
			statements = append(statements, statement)
		}
		if queued {
			plan.result.VectorOperations++
		}
		return nil
	}
	sweep := func(decisions []d1AdmissionVectorDecision) error {
		for _, decision := range decisions {
			statements = append(statements, D1Statement{
				SQL:  d1AdmissionCancelPendingUpsertsSQL(decision.clearRetryAfter),
				Args: []any{decision.reason, d1TimeValue(admission.CreatedAt), decision.documentID},
			})
			if err := enqueue(d1AdmissionVectorDelete(admission, decision.documentID, decision.reason)); err != nil {
				return err
			}
		}
		return nil
	}

	if plan.excludePublicMemory && memoryID > 0 {
		if err := enqueue(d1AdmissionVectorDelete(admission,
			"memory:"+admission.ChatSessionID+":"+strconv.FormatInt(memoryID, 10),
			"no_public_memory_projection")); err != nil {
			return nil, err
		}
	}
	if err := sweep(plan.retiredEvidence); err != nil {
		return nil, err
	}
	if err := sweep(plan.duplicateEvidence); err != nil {
		return nil, err
	}
	if err := sweep(plan.privatePrecise); err != nil {
		return nil, err
	}
	if err := sweep(plan.retiredPrecise); err != nil {
		return nil, err
	}
	if err := sweep(plan.eligibilityReconcile); err != nil {
		return nil, err
	}

	// Every unit this admission wrote or reactivated gets its document
	// enqueued, including the ineligible ones: the delete above has just claimed
	// their document, and the upsert here is the same ordering the reference uses
	// when a unit that was private becomes general-vector eligible again.
	for _, unit := range plan.preciseVectorUnits {
		item, err := d1AdmissionPreciseVectorItem(admission, unit)
		if err != nil {
			return nil, err
		}
		if item == nil {
			continue
		}
		if err := enqueue(item); err != nil {
			return nil, err
		}
	}

	for _, item := range plan.admissionVectorItems {
		outboxItem, err := d1AdmissionVectorItem(admission, item, memoryID, evidenceByText)
		if err != nil {
			return nil, err
		}
		if err := enqueue(outboxItem); err != nil {
			return nil, err
		}
	}
	return statements, nil
}

// d1AdmissionEligibilityDecisions retires the vector projections this admission
// deliberately omitted, and nothing else.
//
// It is a replay-only pass. Its whole purpose is to distinguish "this artifact
// has no public vector projection" from "the vector backend was unconfigured
// when this was admitted" — without it, a temporarily missing backend would leave
// every artifact looking like a deliberate private projection and the
// reconciliation would never converge. Canonical rows and the committed
// extraction are untouched; only the vector outbox is.
func d1AdmissionEligibilityDecisions(
	admission *MemoryAdmission,
	plan *d1AdmissionPlan,
	memoryID int64,
	evidenceByText map[string]int64,
) []d1AdmissionVectorDecision {
	desired := map[string]bool{}
	for _, item := range plan.admissionVectorItems {
		rowID, err := d1AdmissionVectorSourceRowID(item, memoryID, evidenceByText)
		if err != nil || rowID <= 0 {
			continue
		}
		desired[item.ArtifactType+":"+admission.ChatSessionID+":"+strconv.FormatInt(rowID, 10)] = true
	}

	var decisions []d1AdmissionVectorDecision
	if memoryID > 0 {
		documentID := "memory:" + admission.ChatSessionID + ":" + strconv.FormatInt(memoryID, 10)
		if !desired[documentID] {
			decisions = append(decisions, d1AdmissionVectorDecision{
				documentID: documentID, reason: "no_public_memory_projection", clearRetryAfter: true,
			})
		}
	}
	for _, evidenceID := range evidenceByText {
		if evidenceID <= 0 {
			continue
		}
		documentID := d1AdmissionEvidenceDocumentID(admission.ChatSessionID, evidenceID)
		if desired[documentID] {
			continue
		}
		decisions = append(decisions, d1AdmissionVectorDecision{
			documentID: documentID, reason: "non_public_evidence_projection", clearRetryAfter: true,
		})
	}
	return decisions
}

// d1AdmissionVectorIdentities resolves every document id and operation key batch
// 2 will touch, so the outbox read can be scoped to them.
//
// The aggregate artifacts are resolved here rather than earlier because they are
// the only documents whose ids come from the batch-1 readback. An artifact with
// no source row is an error rather than a skip: silently dropping a vector would
// leave an admitted memory that recall can never find.
func (s *d1Store) d1AdmissionVectorIdentities(
	ctx context.Context,
	admission *MemoryAdmission,
	plan *d1AdmissionPlan,
	memoryID int64,
	evidenceByText map[string]int64,
) ([]string, []string, error) {
	var documents []string
	var operationKeys []string
	add := func(documentID string) {
		documents = append(documents, documentID)
	}
	addDelete := func(documentID string) {
		add(documentID)
		operationKeys = append(operationKeys,
			memoryAdmissionVectorOperationKey("delete:active", admission, documentID))
	}

	if plan.excludePublicMemory && memoryID > 0 {
		addDelete("memory:" + admission.ChatSessionID + ":" + strconv.FormatInt(memoryID, 10))
	}
	for _, group := range [][]d1AdmissionVectorDecision{
		plan.retiredEvidence, plan.duplicateEvidence, plan.privatePrecise, plan.retiredPrecise,
		plan.eligibilityReconcile,
	} {
		for _, decision := range group {
			addDelete(decision.documentID)
		}
	}
	for _, unit := range plan.preciseVectorUnits {
		documentID := d1AdmissionPreciseDocumentID(admission.ChatSessionID, unit.UnitID)
		if !preciseMemoryGeneralVectorEligible(unit) {
			// An ineligible unit's own document is not enqueued; the delete
			// above already claims it.
			continue
		}
		add(documentID)
		operationKeys = append(operationKeys,
			preciseMemoryVectorOperationKey(admission.ChatSessionID, unit.SourceRevision,
				unit.DerivationVersion, unit.ExtractorVersion, unit.IndexVersion, documentID))
	}
	for _, item := range plan.admissionVectorItems {
		rowID, err := d1AdmissionVectorSourceRowID(item, memoryID, evidenceByText)
		if err != nil {
			return nil, nil, err
		}
		if rowID <= 0 {
			return nil, nil, errors.New("admission vector source row is missing")
		}
		documentID := item.ArtifactType + ":" + admission.ChatSessionID + ":" + strconv.FormatInt(rowID, 10)
		add(documentID)
		operationKeys = append(operationKeys,
			memoryAdmissionVectorOperationKey("upsert", admission, documentID))
	}
	return documents, operationKeys, nil
}

func d1AdmissionVectorSourceRowID(item MemoryAdmissionVector, memoryID int64, evidenceByText map[string]int64) (int64, error) {
	switch item.ArtifactType {
	case "memory":
		return memoryID, nil
	case "evidence":
		return evidenceByText[strings.TrimSpace(item.EvidenceText)], nil
	default:
		return 0, fmt.Errorf("unsupported admission vector artifact %q", item.ArtifactType)
	}
}

// d1AdmissionCancelPendingUpsertsSQL is the sweep that marks an unapplied upsert
// for a document stale before its delete is enqueued.
//
// The two shapes differ in exactly one column and the difference is preserved.
// The retired-artifact sweeps clear retry_after, because the document is gone
// and its backoff history describes an attempt to index something that no longer
// exists. The private and replaced precise-unit sweeps keep retry_after, so a
// document that is repeatedly re-evaluated retains its backoff instead of being
// retried immediately every time.
func d1AdmissionCancelPendingUpsertsSQL(clearRetryAfter bool) string {
	clear := ""
	if clearRetryAfter {
		clear = ",\n\t\t    retry_after = NULL"
	}
	return `
		UPDATE memory_vector_outbox
		SET status = 'stale_rejected', lease_owner = NULL, lease_until = NULL` + clear + `,
		    last_error = ?, updated_at = ?
		WHERE document_id = ? AND operation = 'upsert'
		  AND status IN ('pending', 'retryable', 'needs_embedding')
		`
}

// d1AdmissionVectorDelete builds the delete operation for a retired document.
func d1AdmissionVectorDelete(admission *MemoryAdmission, documentID, reason string) *MemoryVectorOutboxItem {
	return &MemoryVectorOutboxItem{
		ContractVersion: MemoryVectorOutboxContract,
		OperationKey:    memoryAdmissionVectorOperationKey("delete:active", admission, documentID),
		Operation:       "delete",
		ChatSessionID:   admission.ChatSessionID,
		SourceRevision:  admission.SourceRevision,
		DocumentID:      documentID,
		DocumentJSON:    memoryVectorDeleteAuditJSON(reason),
		// A delete needs no vector of its own, so it is always ready to run.
		EmbeddingReady:      true,
		RequiredSourceState: "active",
		Status:              "pending",
		CreatedAt:           admission.CreatedAt,
		UpdatedAt:           admission.CreatedAt,
	}
}

// d1AdmissionPreciseVectorItem builds a precise unit's upsert, or reports that
// there is nothing to enqueue.
func d1AdmissionPreciseVectorItem(admission *MemoryAdmission, unit *PreciseMemoryUnit) (*MemoryVectorOutboxItem, error) {
	if !preciseMemoryGeneralVectorEligible(unit) {
		return nil, nil
	}
	documentID := d1AdmissionPreciseDocumentID(admission.ChatSessionID, unit.UnitID)
	documentText := PreciseMemorySemanticText(unit)
	if documentText == "" {
		return nil, nil
	}
	documentJSON, err := json.Marshal(map[string]any{
		"ID":            documentID,
		"ChatSessionID": unit.ChatSessionID,
		"SourceTable":   "precise_memory_units",
		"SourceRowID":   unit.UnitID,
		"SchemaVersion": PreciseMemoryUnitContract,
		"DocumentText":  documentText,
		"Embedding":     unit.VectorEmbedding,
		"Metadata": memoryVectorDocumentMetadata(
			unit.SourceRevision, unit.SourceContract, unit.IndexVersion, documentText,
			unit.VectorEmbeddingModel, unit.VectorContextChunks, unit.VectorContextChunkIndex,
		),
	})
	if err != nil {
		return nil, err
	}
	return &MemoryVectorOutboxItem{
		ContractVersion: MemoryVectorOutboxContract,
		OperationKey: preciseMemoryVectorOperationKey(
			unit.ChatSessionID, unit.SourceRevision, unit.DerivationVersion,
			unit.ExtractorVersion, unit.IndexVersion, documentID),
		Operation:           "upsert",
		ChatSessionID:       unit.ChatSessionID,
		SourceRevision:      unit.SourceRevision,
		DocumentID:          documentID,
		DocumentJSON:        string(documentJSON),
		EmbeddingReady:      len(unit.VectorEmbedding) > 0,
		RequiredSourceState: "active",
		Status:              preciseMemoryVectorOutboxStatus(unit),
		CreatedAt:           unit.CreatedAt,
		UpdatedAt:           unit.UpdatedAt,
	}, nil
}

// d1AdmissionVectorItem builds an aggregate artifact's upsert.
func d1AdmissionVectorItem(
	admission *MemoryAdmission,
	item MemoryAdmissionVector,
	memoryID int64,
	evidenceByText map[string]int64,
) (*MemoryVectorOutboxItem, error) {
	rowID, err := d1AdmissionVectorSourceRowID(item, memoryID, evidenceByText)
	if err != nil {
		return nil, err
	}
	if rowID <= 0 {
		return nil, errors.New("admission vector source row is missing")
	}
	rowIDText := strconv.FormatInt(rowID, 10)
	documentID := item.ArtifactType + ":" + admission.ChatSessionID + ":" + rowIDText
	documentText := strings.TrimSpace(item.DocumentText)
	documentJSON, err := json.Marshal(map[string]any{
		"ID":                    documentID,
		"Embedding":             item.Embedding,
		"Tier":                  item.Tier,
		"ChatSessionID":         admission.ChatSessionID,
		"SourceTable":           item.SourceTable,
		"SourceRowID":           rowIDText,
		"SchemaVersion":         item.SchemaVersion,
		"DocumentText":          documentText,
		"SearchTextPolicy":      item.SearchTextPolicy,
		"RawLanguage":           item.RawLanguage,
		"SummaryLanguage":       item.SummaryLanguage,
		"SessionOutputLanguage": item.SessionOutputLanguage,
		"AliasCount":            item.AliasCount,
		"Metadata": memoryVectorDocumentMetadata(
			admission.SourceRevision, MemorySourceRevisionContract,
			admission.IndexVersion, documentText, item.EmbeddingModel,
			item.ContextChunks, item.ContextChunkIndex,
		),
	})
	if err != nil {
		return nil, err
	}
	embeddingReady := len(item.Embedding) > 0
	status := "needs_embedding"
	if embeddingReady {
		status = "pending"
	}
	return &MemoryVectorOutboxItem{
		ContractVersion:     MemoryVectorOutboxContract,
		OperationKey:        memoryAdmissionVectorOperationKey("upsert", admission, documentID),
		Operation:           "upsert",
		ChatSessionID:       admission.ChatSessionID,
		SourceRevision:      admission.SourceRevision,
		DocumentID:          documentID,
		DocumentJSON:        string(documentJSON),
		EmbeddingReady:      embeddingReady,
		RequiredSourceState: "active",
		Status:              status,
		CreatedAt:           admission.CreatedAt,
		UpdatedAt:           admission.CreatedAt,
	}, nil
}

// d1AdmissionEnqueueVector decides what a single outbox operation becomes: a
// plain insert, a lease-aware refresh update, or nothing at all because an
// identical operation is already queued.
//
// The decision is made here rather than after the batch because a D1 batch
// cannot report which of its statements was suppressed. The two duplicate
// outcomes are the reference's: an identical operation is a replay and queues
// nothing, and an operation whose key is already used for different work is a
// conflict rather than a silent overwrite.
func (s *d1Store) d1AdmissionEnqueueVector(
	byKey map[string]d1AdmissionOutboxRow,
	byDocument map[string][]d1AdmissionOutboxRow,
	sourceState string,
	item *MemoryVectorOutboxItem,
	vectorReplay memoryAdmissionVectorReplayOptions,
) (D1Statement, bool, error) {
	insert, err := d1EnqueueMemoryVectorStatement(item)
	if err != nil {
		return D1Statement{}, false, err
	}
	existing, exists := byKey[item.OperationKey]
	if !exists {
		if vectorReplay.Refresh {
			// A replay rebuilds the deterministic projections, so the outbox row
			// is created if it is missing rather than refreshed.
			return insert, true, nil
		}
		return insert, true, nil
	}

	if !vectorReplay.Refresh {
		if existing.operation != item.Operation ||
			existing.chatSessionID != item.ChatSessionID ||
			existing.sourceRevision != item.SourceRevision ||
			existing.documentID != item.DocumentID ||
			strings.TrimSpace(existing.documentJSON) != strings.TrimSpace(item.DocumentJSON) ||
			existing.embeddingReady != item.EmbeddingReady ||
			existing.requiredSource != item.RequiredSourceState {
			return D1Statement{}, false, errors.New("memory vector operation idempotency conflict")
		}
		return D1Statement{}, false, nil
	}

	// The refresh branch. The source state is passed in rather than read from the
	// outbox row: the contract is between the OPERATION's required source state
	// and the LIVE source revision, and conflating the two would let a row for a
	// superseded revision look like a row for an active one.
	// The admission fenced on the source before any of this ran, so the only
	// reachable half of the check is a delete that requires an inactive source
	// while the source is active.
	if item.RequiredSourceState == "inactive" && sourceState == "active" {
		return D1Statement{}, false, ErrSourceRevisionStale
	}
	if item.RequiredSourceState == "active" && sourceState != "active" {
		return D1Statement{}, false, ErrSourceRevisionStale
	}
	now := d1TimeValue(time.Now().UTC())
	// lease_until is TEXT in the canonical schema and d1TimeValue writes
	// zero-padded RFC3339 UTC, so the comparison is a string comparison that
	// orders chronologically. It is not a numeric comparison and must not become
	// one.
	if existing.status == "leased" && existing.leaseUntil != nil && *existing.leaseUntil > now {
		return D1Statement{}, false, ErrMemoryReprocessingLeased
	}
	for _, row := range byDocument[item.DocumentID] {
		if row.id > existing.id {
			return D1Statement{}, false, errors.New("memory vector refresh is superseded by a newer operation")
		}
	}
	status := firstNonEmptyString(item.Status, "pending")
	return D1Statement{
		SQL: `
		UPDATE memory_vector_outbox
		SET document_json = ?, embedding_ready = ?, status = ?, attempts = 0,
		    retry_after = NULL, lease_owner = NULL, lease_until = NULL,
		    last_error = NULL, updated_at = ?
		WHERE operation_key = ?
		  AND (status <> 'leased' OR lease_until IS NULL OR lease_until <= ?)
		`, Args: []any{d1NullableString(strings.TrimSpace(item.DocumentJSON)),
			d1BoolValue(item.EmbeddingReady), status, d1TimeValue(item.UpdatedAt),
			item.OperationKey, now},
	}, true, nil
}
