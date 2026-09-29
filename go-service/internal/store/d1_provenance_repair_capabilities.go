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

// D1 provenance-repair capabilities.
//
// Four unrelated capabilities live in this file, grouped only because they are
// all small and mostly read-mostly. Nothing here shares an abstraction with
// anything else: the operator repair, the body-delete artifact preview, the
// vector recovery cache, and the source-discovery job ledger have no common
// table, no common fence, and no common caller. They are separated by section
// comments below so a reader looking for one of them never has to reason about
// the other three.
//
//	1. CharacterProvenanceRepairStore  operator metadata correction / undo
//	2. StateRepairArtifactReader        body-delete artifact preview
//	3. VectorRecoveryCacheReader        saved embedding materializations
//	4. SourceDiscoveryStore             source-discovery job ledger
//
// ---------------------------------------------------------------------------
// 1. CharacterProvenanceRepairStore
// ---------------------------------------------------------------------------
//
// What one repair commits
//
// A repair is two rows written together or not at all:
//
//   - a new character_states snapshot carrying the corrected field provenance,
//   - a character_events row of type 'field_provenance_repair' holding the
//     before/after pair and the operation id.
//
// The event is the only undo record; the snapshot alone would be an unexplained
// new revision of the character. D1Conn.Batch is the transaction boundary here,
// exactly as the MariaDB transaction was, so an operator can never end up with
// a corrected character whose audit trail is missing.
//
// Idempotency is keyed on the operation id, not on the payload
//
// The reference looks the event up by
// JSON_UNQUOTE(JSON_EXTRACT(details_json, '$.operation_id')) and returns the
// stored event when it finds one, writing nothing. That is the contract the
// admin route depends on: the operation id is derived from (session, character,
// provenance) or from the undo event's own id, so re-submitting the same
// request — a retried POST, a double-clicked button, a container that committed
// but lost the response — must not append a second snapshot and a second event.
//
// The lookup must run BEFORE the batch, not after a failed insert. A D1 batch
// reports only an error, never which statement failed, so resolving a
// collision by catching a duplicate is not expressible inside one. The
// observable behaviour is identical: the same statement, the same two outcomes
// (replay returns the stored event having written nothing; a different
// operation id proceeds to write).
//
// s.memoryDerivationWriteMu is held across the lookup and the batch so that,
// within this process, nothing can insert the same operation id in between. The
// mutex is process-local; two Cloudflare Containers racing on one operation id
// is the same cross-container boundary the other D1 write capabilities
// document, and it resolves as a unique/duplicate failure from the batch rather
// than as a silent double repair.
//
// The insert id comes from a read-back, not from the insert
//
// MariaDB reads the new id from LastInsertId on the same connection that
// inserted the row. A D1 batch cannot return rows at all — it returns only
// per-statement change counts — so the committed event is located with the very
// same statement that performs the replay lookup. That is exact rather than
// approximate: the operation id is the identity of the event, the lookup is
// ORDER BY id DESC LIMIT 1 on it, and the batch that just committed is the only
// writer of that row. If the read-back itself fails, the repair is still
// committed and the event is returned with a zero id — the same outcome the
// reference has when LastInsertId fails, where the write succeeded and the
// handle did not.
//
// Why the snapshot is appended rather than updated
//
// This is an append-only history table: ListCharacterStateHistory and the
// prepare-turn "current snapshot" read both rely on the newest row winning, and
// an operator correction must be undoable by re-appending the previous value.
// An UPDATE would destroy both properties, so the statement is the reference's
// INSERT unchanged.
//
// nullableJSONText is reproduced, not replaced by d1NullableString
//
// The reference stores an empty OR the literal JSON text "null" as SQL NULL in
// every character_states JSON column. d1NullableString only maps blank text to
// NULL, so reusing it would persist the four-character string "null". That
// string is valid JSON, round-trips to the caller as the text "null" instead of
// empty, and would be re-parsed by the manual-edit overlay as a null document
// rather than as an absent one. The dedicated helper keeps the reference's
// three-way mapping.
//
// SQLite notes
//
//   - json_extract already dequotes a JSON string, so it is the direct analogue
//     of JSON_UNQUOTE(JSON_EXTRACT(...)) and is the same form the D1 status
//     capabilities already use. The MariaDB column is a real JSON type, so
//     malformed text cannot be stored there; the D1 column is TEXT, and a
//     malformed value would make json_extract fail the lookup rather than match
//     — it fails closed, and neither provider's writers can produce one.
//   - created_at / updated_at are TEXT. d1TimeValue renders zero-padded RFC3339
//     UTC, so the repair's own timestamps stay comparable with schema defaults.

var _ CharacterProvenanceRepairStore = (*d1Store)(nil)

// d1ProvenanceRepairEventLookup is the reference replay lookup with the MariaDB
// JSON accessor swapped for its SQLite equivalent. The predicate is unchanged:
// session, character, event type, and the operation id carried inside the event
// details. The trailing ORDER BY id DESC LIMIT 1 is what makes a replay return
// the newest repair when an id has been reused.
const d1ProvenanceRepairEventLookup = `
		SELECT id, chat_session_id, character_name, turn_index, event_type, details_json, created_at
		FROM character_events
		WHERE chat_session_id = ? AND character_name = ? AND event_type = 'field_provenance_repair'
		  AND json_extract(details_json, '$."operation_id"') = ?
		ORDER BY id DESC LIMIT 1`

// d1ProvenanceRepairCharacterStateInsert is the reference snapshot insert with
// no dialect change. The eleven columns and their order are the snapshot's
// canonical shape; reordering two adjacent fields would silently transpose a
// character attribute into a provenance record.
const d1ProvenanceRepairCharacterStateInsert = `
	INSERT INTO character_states (
		chat_session_id, character_name, appearance_json, personality_json,
		status_json, relationships_json, speech_style_json, field_provenance_json, turn_index,
		created_at, updated_at
	)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// d1ProvenanceRepairEventInsert is the reference audit insert. The id is not
// requested here because a D1 batch cannot return rows; it is read back with
// d1ProvenanceRepairEventLookup.
const d1ProvenanceRepairEventInsert = `
	INSERT INTO character_events (chat_session_id, character_name, turn_index, event_type, details_json, created_at)
	VALUES (?, ?, ?, ?, ?, ?)`

// d1ProvenanceRepairNullableJSONText mirrors the reference's nullableJSONText:
// blank text and the literal JSON document "null" are both stored as SQL NULL,
// so an absent provenance field and a null provenance field are indistinguishable
// to every reader, exactly as they are on MariaDB.
func d1ProvenanceRepairNullableJSONText(value string) any {
	text := strings.TrimSpace(value)
	if text == "" || text == "null" {
		return nil
	}
	return text
}

// d1ProvenanceRepairEvent reads the newest repair event for an operation id.
// errD1NoRows is returned unchanged so the caller can tell "no prior repair"
// apart from a transport failure, which is how the reference distinguishes
// sql.ErrNoRows.
func (s *d1Store) d1ProvenanceRepairEvent(ctx context.Context, chatSessionID, characterName, operationID string) (CharacterEvent, error) {
	var event CharacterEvent
	var turnIndex *int64
	var detailsJSON *string
	err := s.conn.QueryRow(ctx, d1ProvenanceRepairEventLookup, chatSessionID, characterName, operationID).Scan(
		&event.ID, &event.ChatSessionID, &event.CharacterName, &turnIndex,
		&event.EventType, &detailsJSON, &event.CreatedAt)
	if err != nil {
		return CharacterEvent{}, err
	}
	// turn_index is nullable on both providers: an operator repair that does not
	// name a turn stores NULL, which the reference scan renders as 0.
	event.TurnIndex = int(d1DerefInt64(turnIndex))
	event.DetailsJSON = d1DerefString(detailsJSON)
	return event, nil
}

// ApplyCharacterProvenanceRepair appends the corrected metadata snapshot and its
// audit event atomically, or returns the event an identical earlier request
// already wrote.
func (s *d1Store) ApplyCharacterProvenanceRepair(ctx context.Context, before, after CharacterState, operationID string) (CharacterEvent, error) {
	// The lookup and the batch share one critical section so a concurrent repair
	// of the same character cannot interleave between them.
	s.memoryDerivationWriteMu.Lock()
	defer s.memoryDerivationWriteMu.Unlock()

	existing, err := s.d1ProvenanceRepairEvent(ctx, after.ChatSessionID, after.CharacterName, operationID)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, errD1NoRows) {
		return CharacterEvent{}, err
	}

	// The repair instant is truncated to the precision the schema stores BEFORE
	// the audit payload is built, so the recorded "after" is byte-identical to
	// the row that is actually written, and to the event's own CreatedAt. A
	// caller comparing any two of the three must not see them differ — and at
	// nanosecond precision against a millisecond column they would.
	now := d1TimeInstant(time.Now().UTC())
	after.CreatedAt, after.UpdatedAt = now, now
	details, err := json.Marshal(map[string]any{"operation_id": operationID, "before": before, "after": after})
	if err != nil {
		return CharacterEvent{}, err
	}
	event := CharacterEvent{
		ChatSessionID: after.ChatSessionID,
		CharacterName: after.CharacterName,
		TurnIndex:     after.TurnIndex,
		EventType:     "field_provenance_repair",
		DetailsJSON:   string(details),
		CreatedAt:     now,
	}

	if err := s.conn.Batch(ctx,
		D1Statement{SQL: d1ProvenanceRepairCharacterStateInsert, Args: []any{
			after.ChatSessionID, after.CharacterName,
			d1ProvenanceRepairNullableJSONText(after.AppearanceJSON),
			d1ProvenanceRepairNullableJSONText(after.PersonalityJSON),
			d1ProvenanceRepairNullableJSONText(after.StatusJSON),
			d1ProvenanceRepairNullableJSONText(after.RelationshipsJSON),
			d1ProvenanceRepairNullableJSONText(after.SpeechStyleJSON),
			d1ProvenanceRepairNullableJSONText(after.FieldProvenanceJSON),
			after.TurnIndex,
			d1TimeValue(after.CreatedAt), d1TimeValue(after.UpdatedAt),
		}},
		D1Statement{SQL: d1ProvenanceRepairEventInsert, Args: []any{
			event.ChatSessionID, event.CharacterName, event.TurnIndex,
			event.EventType, d1NullableString(event.DetailsJSON), d1TimeValue(now),
		}},
	); err != nil {
		return CharacterEvent{}, err
	}

	// The batch is committed, so the repair happened. The lookup is re-run only
	// to learn the row id the batch could not return; a failure here is not a
	// failed repair and must not be reported as one.
	if stored, readErr := s.d1ProvenanceRepairEvent(ctx, after.ChatSessionID, after.CharacterName, operationID); readErr == nil {
		return stored, nil
	}
	return event, nil
}

// ---------------------------------------------------------------------------
// 2. StateRepairArtifactReader
// ---------------------------------------------------------------------------
//
// This is the delete-preview half of the body-tracking delete flow. It answers a
// narrow question — "which rows in this session carry this character's body
// data?" — and returns each one as a before/after column pair that the route
// later feeds to the transactional applier. It never writes, so it has no
// idempotency contract of its own.
//
// Why almost none of the logic is in SQL
//
// The reference builds each row's text from its own columns, runs a topic
// regex and an entity/name mention test over it, and then prunes body-related
// fields out of shared JSON containers while leaving identity, clothing, and
// unrelated status intact. None of that is expressible as a predicate without
// re-deriving it, so the statements here are the reference's per-table SELECT
// and its source-link query, unchanged, and the pure pruning helpers are reused
// from the MariaDB file rather than reimplemented — a second copy of the pruner
// is exactly how the two providers would drift.
//
// Why the source-link query is the active-revision fence
//
// A body_tracking event is linked to memories and evidence through the turn and
// the evidence ids recorded in its evidence payload, and that payload names a
// source revision. The join carries AND s.lifecycle_state = 'active', so a
// rolled-back revision contributes no links. Dropping it would make the preview
// offer to delete memories and evidence belonging to a revision the user has
// already discarded — reporting state that no longer exists as current.
//
// The status tables are excluded from their own preview
//
// status_current_values and status_change_events carry the body_tracking status
// itself. Listing them would make the preview offer to clear the record of the
// deletion it is planning, so both are filtered with
// status_key NOT IN ('body_tracking','story_clock'). story_clock is excluded for
// the same reason: it is the clock the repair records its own turn against.
//
// Why the scan coerces instead of using a nullable string destination
//
// The reference scans every selected column into sql.NullString, and
// database/sql silently renders an INTEGER or REAL column as its decimal text.
// The cleared column sets include three such columns — direct_evidence_records.
// tombstoned, pending_threads.suppressed, and pending_threads.user_corrected —
// and the topic test, the "already cleared" test, and the turn / evidence-id
// parse all run over that text. A D1 **string destination rejects an integer
// outright, so the conversion is performed explicitly here. The mapping is the
// one database/sql applies, and NULL stays distinct from empty text throughout,
// which the reference relies on: `values[i].Valid && values[i].String == v` is
// what skips an already-cleared column, and a NULL must not be mistaken for the
// empty string that a few clear values actually are.
//
// Vector tiers
//
// Four table families have a legacy vector materialization that may predate the
// source outbox. When the outbox holds no upsert for the derived document id,
// the artifact is reported with its vector ids and an empty "after" snapshot so
// the route captures what is in the vector store before the row is cleared. The
// existence probe is the reference COUNT(*) statement, and the derived id pair
// is the same "<tier>:<session>:<key>" / "<tier>:<key>" pair the MariaDB writer
// uses, with precise_memory_units addressed by unit_id rather than row id.

var _ StateRepairArtifactReader = (*d1Store)(nil)

// d1StateRepairText renders one scanned column as the text the reference's
// sql.NullString scan would have produced, and reports whether the column was
// NULL. The numeric formats are database/sql's own, so a float column does not
// gain or lose precision relative to the reference read.
func d1StateRepairText(value any) (string, bool) {
	switch typed := value.(type) {
	case nil:
		return "", false
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	case int64:
		return strconv.FormatInt(typed, 10), true
	case int:
		return strconv.Itoa(typed), true
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64), true
	case bool:
		if typed {
			return "1", true
		}
		return "0", true
	default:
		return fmt.Sprint(typed), true
	}
}

// d1StateRepairSourceLinks returns the turns and direct-evidence ids that the
// character's body_tracking events actually committed under, restricted to an
// active source revision.
//
// The json_extract replaces MariaDB's JSON_UNQUOTE(JSON_EXTRACT(...)): both
// return the dequoted string for a JSON string value, and both return SQL NULL
// for an absent path, so an event with no recorded revision simply joins to
// nothing instead of matching an arbitrary one.
func (s *d1Store) d1StateRepairSourceLinks(ctx context.Context, sid, entityID string) (map[int]bool, map[int64]bool, error) {
	turns, ids := map[int]bool{}, map[int64]bool{}
	rows, err := s.conn.Query(ctx, `SELECT e.source_turn, e.evidence_json FROM status_change_events e
		JOIN memory_source_revisions s ON s.chat_session_id = e.chat_session_id
		 AND s.source_revision = json_extract(e.evidence_json, '$."source_revision"')
		WHERE e.chat_session_id = ? AND e.owner_id = ? AND e.status_key = 'body_tracking'
		  AND s.lifecycle_state = 'active'`, sid, entityID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var turn *int64
		var raw string
		if err := rows.Scan(&turn, &raw); err != nil {
			return nil, nil, err
		}
		if turn != nil && *turn > 0 {
			turns[int(*turn)] = true
		}
		var evidence struct {
			IDs []int64 `json:"direct_evidence_ids"`
		}
		if err := json.Unmarshal([]byte(raw), &evidence); err != nil {
			return nil, nil, err
		}
		for _, id := range evidence.IDs {
			ids[id] = true
		}
	}
	return turns, ids, rows.Err()
}

// d1StateRepairVectorIDs returns the vector document ids an artifact may be
// materialized under. precise_memory_units is addressed by unit_id, because the
// vector document for a unit is keyed on the unit's stable id, not on the row
// id that a migration or a re-import can renumber.
func (s *d1Store) d1StateRepairVectorIDs(ctx context.Context, sid string, spec bodyRepairTable, id int64) ([]string, error) {
	key := strconv.FormatInt(id, 10)
	if spec.table == "precise_memory_units" {
		if err := s.conn.QueryRow(ctx,
			"SELECT unit_id FROM precise_memory_units WHERE chat_session_id = ? AND id = ?", sid, id).Scan(&key); err != nil {
			return nil, err
		}
	}
	return []string{spec.vectorTier + ":" + sid + ":" + key, spec.vectorTier + ":" + key}, nil
}

// ListBodyRepairArtifacts returns the before/after column plan for every row in
// the session that carries this character's body data.
//
// The result is a non-nil empty slice when nothing matches, so a character with
// no body content encodes identically on both providers.
func (s *d1Store) ListBodyRepairArtifacts(ctx context.Context, sid, entityID, name string) ([]StateRepairArtifactChange, error) {
	turns, evidenceIDs, err := s.d1StateRepairSourceLinks(ctx, sid, entityID)
	if err != nil {
		return nil, err
	}
	out := []StateRepairArtifactChange{}
	for _, spec := range bodyRepairTables {
		columns := bodyRepairColumns(spec)
		selectColumns := strings.Join(columns, ",")
		if spec.identityColumns != "" {
			selectColumns += "," + spec.identityColumns
		}
		where := ""
		if spec.table == "status_current_values" || spec.table == "status_change_events" {
			where = " AND status_key NOT IN ('body_tracking','story_clock')"
		}
		rows, err := s.conn.Query(ctx,
			"SELECT id,"+selectColumns+" FROM "+spec.table+" WHERE "+spec.sessionColumn+" = ?"+where+" ORDER BY id", sid)
		if err != nil {
			return nil, err
		}
		values := make([]any, len(strings.Split(selectColumns, ",")))
		for rows.Next() {
			var id int64
			destinations := make([]any, 0, len(values)+1)
			destinations = append(destinations, &id)
			for i := range values {
				destinations = append(destinations, &values[i])
			}
			if err := rows.Scan(destinations...); err != nil {
				rows.Close()
				return nil, err
			}
			texts := make([]string, 0, len(values))
			for _, raw := range values {
				text, _ := d1StateRepairText(raw)
				texts = append(texts, text)
			}
			text := strings.Join(texts, "\n")
			// This selects reviewable memory units, not a new inference about the
			// character. Mixed-content units are shown in the delete preview.
			linked := spec.table == "direct_evidence_records" && evidenceIDs[id]
			if spec.table == "memories" {
				turn, _ := strconv.Atoi(texts[len(texts)-1])
				linked = turns[turn]
			}
			if spec.table == "precise_memory_units" {
				evidenceID, _ := strconv.ParseInt(texts[len(texts)-1], 10, 64)
				linked = evidenceIDs[evidenceID]
			}
			if !linked && (!bodyRepairTopic.MatchString(text) || !(entityID != "" && strings.Contains(text, entityID) || name != "" && strings.Contains(strings.ToLower(text), strings.ToLower(name)))) {
				continue
			}
			change := StateRepairArtifactChange{Table: spec.table, ID: id, Before: map[string]*string{}, After: map[string]*string{}}
			bound := false
			for _, raw := range values[len(columns):] {
				value, _ := d1StateRepairText(raw)
				if value == entityID || value == name {
					bound = true
				}
			}
			for i, key := range columns {
				current, currentValid := d1StateRepairText(values[i])
				v := spec.clear[key]
				if bodyRepairPartialTable(spec.table) {
					// A NULL column carries nothing to prune. Replacing it with
					// "{}" would invent a document that was never there, and the
					// undo would then have no "before" to restore.
					if !currentValid {
						continue
					}
					v = bodyRepairPruneJSON(current, entityID, name, bound)
				}
				if currentValid && current == v {
					continue
				}
				if currentValid {
					before := current
					change.Before[key] = &before
				} else {
					change.Before[key] = nil
				}
				after := v
				change.After[key] = &after
			}
			if len(change.After) > 0 {
				out = append(out, change)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	// Old and hierarchy vectors can predate the source outbox. Their explicit
	// repair uses the existing vector API and keeps its snapshot in this backup.
	for i := range out {
		spec, _ := bodyRepairSpec(out[i].Table)
		if spec.vectorTier == "" {
			continue
		}
		ids, err := s.d1StateRepairVectorIDs(ctx, sid, spec, out[i].ID)
		if err != nil {
			return nil, err
		}
		var count int
		if err := s.conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM memory_vector_outbox WHERE chat_session_id = ? AND document_id = ? AND operation = 'upsert'",
			sid, ids[0]).Scan(&count); err != nil {
			return nil, err
		}
		if count == 0 {
			out[i].VectorIDs, out[i].VectorAfterJSON = ids, "[]"
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 3. VectorRecoveryCacheReader
// ---------------------------------------------------------------------------
//
// Startup recovery re-embeds documents whose embedding was lost, and the cache
// is the only way to avoid paying for it twice: the outbox still holds the
// materialization that was applied to the vector store. The reader must not
// re-admit a memory, change an outbox status, or touch a lease — the caller
// re-checks document text and embedding model before reusing anything, so this
// is a pure read of what was saved.
//
// Two details are load-bearing:
//
//   - The operation filter is 'upsert'. A 'delete' row's document_json is an
//     audit tombstone, not a recoverable embedding, and feeding one back would
//     resurrect a document the operator deleted.
//   - The order is updated_at then id, ascending. Recovery restores the
//     earliest materialization first so a document that was upserted repeatedly
//     ends up holding the same embedding both providers already agree on.
//     updated_at is TEXT but zero-padded RFC3339 UTC, so it orders
//     chronologically without a CAST.
//
// The 200-id chunking is kept verbatim, including its consequence: the result is
// concatenated chunk by chunk, so a caller passing more than 200 ids receives
// per-chunk order rather than one globally ordered stream. D1 caps bound
// parameters per statement exactly as MariaDB does, so the chunk size is a
// transport requirement rather than an optimisation, and changing it would
// change the observable ordering.

var _ VectorRecoveryCacheReader = (*d1Store)(nil)

// ReadVectorRecoveryCache returns the saved document payloads for the requested
// document ids, oldest materialization first.
//
// An empty id list is a non-nil empty slice: startup recovery treats "no cached
// documents" and "cache unavailable" very differently, and the first must not
// look like a transport failure.
func (s *d1Store) ReadVectorRecoveryCache(ctx context.Context, ids []string) ([]string, error) {
	result := []string{}
	// Startup recovery can hand this every cached document id in the account, and
	// D1 binds at most 100 parameters per statement. The previous chunk of 200
	// cleared the local SQLite harness and failed against D1, which is how a
	// recovery read that only runs on restart could be wrong exactly when it is
	// needed.
	for _, chunk := range d1BindChunks(ids, 0) {
		args := make([]any, 0, len(chunk))
		for _, id := range chunk {
			args = append(args, id)
		}
		rows, err := s.conn.Query(ctx, `SELECT document_json FROM memory_vector_outbox
			WHERE operation = 'upsert' AND document_json IS NOT NULL AND document_id IN (`+
			strings.TrimSuffix(strings.Repeat("?,", len(args)), ",")+`) ORDER BY updated_at, id`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var data string
			if err := rows.Scan(&data); err != nil {
				rows.Close()
				return nil, err
			}
			result = append(result, data)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// 4. SourceDiscoveryStore
// ---------------------------------------------------------------------------
//
// The source-discovery job ledger is a small, self-describing pipeline record:
// a generated job id, a fixed contract, the request as submitted, and the
// result and coverage reports as the pipeline last wrote them. The route drives
// it through many states, so the two properties that matter are that an unknown
// job is ErrNotFound (the route answers 404 rather than inventing a job) and
// that the JSON documents round-trip byte-for-byte, since the pipeline re-reads
// its own request payload to resume.
//
// No source-revision fence applies here, and that is deliberate rather than an
// omission. A discovery job is operator-initiated reference research about a
// work, not a projection derived from a chat turn: it names a work_id and a
// continuity_id and never carries a source_revision. The fence rule exists to
// stop a rolled-back revision from resurrecting derived state, and there is no
// revision in this record to roll back. Inventing one — refusing a save because
// the session has no active revision — would make the whole capability
// unreachable.
//
// Two dialect notes:
//
//   - The contract version is written as the statement literal
//     'source-discovery-pipeline.v1', exactly as the reference does, so the
//     column default is never the thing that decides the contract.
//   - created_at and updated_at are omitted from the insert and filled by the
//     column DEFAULT, matching the reference. The D1 default renders
//     strftime('%Y-%m-%dT%H:%M:%fZ','now'), which parseD1Time reads back as
//     RFC3339 UTC, so a defaulted row and an application-written row stay
//     interchangeable.
//
// The reference wraps the insert error in referenceStoreError, which maps
// MySQL error 1062 to ErrReferenceConflict. That mapping is driver-specific and
// unreachable here: the job id is a freshly generated random UUID, so the
// primary key cannot collide and there is no MySQL error number to inspect.
// Wrapping a SQLite constraint failure as a reference conflict would be a
// guess, so the raw error is returned instead.

var _ SourceDiscoveryStore = (*d1Store)(nil)

// d1SourceDiscoveryJobInsert is the reference insert with no dialect change.
// The contract version stays a statement literal: making it a bound parameter
// would let a caller write a job under a contract the reader does not
// understand, and the whole point of the column is to name the reader.
const d1SourceDiscoveryJobInsert = `
	INSERT INTO source_discovery_jobs
		(job_id, contract_version, work_query, original_title, language_code,
		 edition_hint, job_state, request_json, result_json, coverage_report_json)
	VALUES (?, 'source-discovery-pipeline.v1', ?, ?, ?, ?, ?, ?, ?, ?)`

// SaveSourceDiscoveryJob stores a new pipeline job and returns it as stored.
//
// The arguments are trimmed exactly as the reference trims them, and a blank
// work query or a state outside the pipeline vocabulary is rejected with
// ErrInvalidReference before any statement is sent. That vocabulary check is the
// same one the schema's CHECK constraint enforces, and it is kept application
// side so a bad state is a clean sentinel error rather than a constraint
// violation naming an internal table.
func (s *d1Store) SaveSourceDiscoveryJob(ctx context.Context, input SourceDiscoveryInput, state string, result, coverage map[string]any) (*SourceDiscoveryJob, error) {
	if strings.TrimSpace(input.WorkQuery) == "" || !sourceDiscoveryState(state) {
		return nil, ErrInvalidReference
	}
	jobID, err := canonRandomID()
	if err != nil {
		return nil, err
	}
	inputJSON, _ := json.Marshal(input)
	resultJSON, _ := json.Marshal(result)
	coverageJSON, _ := json.Marshal(coverage)
	if _, err := s.conn.Exec(ctx, d1SourceDiscoveryJobInsert,
		jobID, strings.TrimSpace(input.WorkQuery), strings.TrimSpace(input.OriginalTitle),
		strings.TrimSpace(input.Language), strings.TrimSpace(input.EditionHint), state,
		string(inputJSON), string(resultJSON), string(coverageJSON)); err != nil {
		return nil, err
	}
	// The stored row is re-read rather than reconstructed from the request, so
	// the caller sees the contract, revision, and timestamps the database
	// actually holds instead of the ones the caller hoped for.
	return s.GetSourceDiscoveryJob(ctx, jobID)
}

// GetSourceDiscoveryJob returns one job, or ErrNotFound when the id is unknown.
//
// The three JSON columns are scanned as text and unmarshalled into maps exactly
// as the reference does, with unmarshal errors ignored: a legacy row whose
// payload is not an object must still be visible to the operator with its
// identity intact, not be turned into a 500.
func (s *d1Store) GetSourceDiscoveryJob(ctx context.Context, jobID string) (*SourceDiscoveryJob, error) {
	item := &SourceDiscoveryJob{Contract: SourceDiscoveryContract}
	var contract string
	var inputRaw, resultRaw, coverageRaw string
	// revision is BIGINT UNSIGNED on MariaDB and INTEGER here. It is scanned into
	// an int64 and re-widened, because a revision is a monotonically increasing
	// counter that no write can push negative, and the D1 scan layer has no
	// unsigned destination.
	var revision int64
	err := s.conn.QueryRow(ctx, `
		SELECT job_id, contract_version, job_state, request_json, result_json,
		       coverage_report_json, revision, created_at, updated_at
		FROM source_discovery_jobs WHERE job_id = ?
	`, strings.TrimSpace(jobID)).Scan(&item.JobID, &contract, &item.State, &inputRaw,
		&resultRaw, &coverageRaw, &revision, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	item.Contract = contract
	item.Revision = uint64(revision)
	_ = json.Unmarshal([]byte(inputRaw), &item.Input)
	_ = json.Unmarshal([]byte(resultRaw), &item.Result)
	_ = json.Unmarshal([]byte(coverageRaw), &item.CoverageReport)
	return item, nil
}
