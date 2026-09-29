package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
)

// D1 lorebook reference capabilities.
//
// The lorebook reference lane is a read-only mirror of what the RisuAI host
// reported through getCurrentLorebookEntries. It is deliberately NOT canonical
// memory: the Host's lore is stored apart from memories, evidence, entities, and
// world rules so that no code path can promote a lorebook line into a fact
// about the world. Two capabilities live on these four tables and are ported
// together, because splitting them is the failure this file exists to prevent:
//
//   - LorebookReferenceStore         ApplyLorebookReferenceSnapshot (write)
//                                   + GetLorebookReferenceCurrent (prepare-turn)
//   - LorebookReferenceExplorerStore GetLorebookReferenceCurrentPage
//                                   + GetLorebookReferenceLatestSessionPage
//
// The prepare-turn reader is reachable only when the writer already exists, and
// the Explorer page is reachable only when the same scope rows exist. Porting
// either half alone would leave a Cloudflare deployment answering
// "lorebook_reference_store_unavailable" (503) or a permanently empty page.
//
// Scope identity is a JSON comparison, not a column comparison
//
// lorebook_reference_scopes has no UNIQUE key over
// (chat_session_id, character_index, chat_index). The same session can own
// several scope rows for the same host indices — one per observed enabled-module
// set — and the MariaDB lookup therefore selects every candidate and compares
// the DECODED identity in Go (findLorebookReferenceScope). That is why the
// statement is reproduced as a candidate read plus a canonical-JSON comparison
// rather than "simplified" into a single-row lookup: collapsing it would pick
// an arbitrary module set and return the wrong lore, or return none.
//
// The Go-side comparison is the shared canonicalLorebookModuleIDs and
// lorebookScopeJSON, so a scope written by the MariaDB provider is recognised by
// the D1 provider byte for byte, including the "blanks dropped, deduplicated,
// sorted" module canonicalisation. Reusing the helper rather than restating it
// is what keeps a D1 deployment from growing a second, subtly different scope
// identity rule that no longer matches rows migrated from MariaDB.
//
// The MariaDB-only constructs, and what replaces each
//
//  1. `character_index <=> ?` — MySQL's NULL-safe equality. SQLite's `IS` is the
//     same operator: it is true when both sides are NULL, true when both are
//     equal, and false otherwise. Writing `character_index = ?` instead would
//     make every unobserved (NULL) index look like a different scope, and every
//     prepare-turn read for a scope the Host did not expose would miss.
//
//  2. `INSERT ... ON DUPLICATE KEY UPDATE chat_session_id = VALUES(...)` — the
//     session lock row. SQLite spells the same intent
//     `ON CONFLICT(chat_session_id) DO UPDATE SET ... = excluded....`. The
//     statement is kept rather than dropped even though the assigned value is
//     the one already stored: the row's existence is the signal, and the table
//     is also what the session-stitch and rollback paths enumerate. One visible
//     difference: MariaDB's ON UPDATE CURRENT_TIMESTAMP would bump updated_at,
//     while the SQLite schema carries no such trigger, so updated_at keeps its
//     value. Nothing in the store reads that column, and the MariaDB statement
//     does not name it either.
//
//  3. `SELECT ... FOR UPDATE` — no SQLite equivalent, and none is needed at the
//     statement level: the batch below IS the transaction, and D1 serialises
//     writes to the whole database. The FOR UPDATE existed to hold the scope
//     row stable between the lookup and the insert; here that stability comes
//     from d1LorebookRefWriteMu, which is held across every read that feeds the
//     batch. This is the same process-local fence d1_precise_memory_write_
//     capabilities.go documents, and it carries the same documented limitation:
//     it does not serialise two Cloudflare Containers. D1's single-writer
//     guarantee makes the interleaving harmless for correctness here — the
//     statements in one batch commit atomically and in order — but it does mean
//     the read that picks the scope id is not serialised against a foreign
//     process.
//
//  4. `LastInsertId()` — D1 has no connection-scoped insert cursor, so the
//     scope id comes back with the row via RETURNING. See the note below on why
//     that statement cannot join the batch.
//
//  5. The multi-statement transaction — `s.conn.Batch` is the D1 transaction
//     boundary, and everything that the MariaDB transaction covered atomically
//     is in that one batch.
//
// Why the counts are read, not counted by the batch
//
// A batch cannot report which statement was suppressed. D1Conn.Batch returns
// only an error, so a statement that matched nothing is indistinguishable from
// one that wrote rows. Two counts in the reference come from exactly that kind
// of suppressed statement, and both are recovered here without weakening the
// atomic unit:
//
//   - PreviousCurrentCount is MariaDB's RowsAffected on the demote UPDATE. The
//     demote's WHERE clause is `scope_id = ? AND is_current = TRUE`, which is
//     character-for-character the predicate of the pre-batch count below, and
//     the demote moves every row it matches out of that set. The pre-batch count
//     is therefore the demote's matched-row count, read while the write mutex
//     is held, and the demote itself stays inside the batch. Reading it from the
//     batch would have meant either dropping the batch (losing atomicity) or
//     splitting the write in two (losing atomicity differently).
//
//   - CurrentEntryCount, in the branches that do not promote the new entries to
//     current, is read after the batch commits. The alternative — running the
//     demote outside the batch to learn its count — was rejected: a batch
//     failure would then leave the previous current projection demoted with no
//     snapshot to explain it, permanently, which is a worse state than a read
//     that a transport error can still fail after an honest commit.
//
// Scope creation is the one statement outside the batch
//
// The snapshot and entry rows both need the new scope id, and a batch cannot
// return a row, so the scope INSERT runs first on its own with RETURNING. If a
// later statement in the batch then fails, the freshly created scope row
// survives: MariaDB would have rolled it back. The residue is an empty scope,
// which a subsequent read reports as an existing scope with no snapshot and no
// entries instead of ErrNotFound. It is reachable only when a batch statement
// genuinely fails, and it is the price of replacing LastInsertId; the
// alternative — pre-computing MAX(scope_id)+1 and binding it — would convert a
// rare, self-correcting residue into a cross-container UNIQUE-constraint
// failure on every concurrent create.
//
// Validation happens before anything is sent
//
// The reference validates each entry's extensions_json inside the transaction,
// so a malformed entry rolls the whole observation back and the caller sees an
// error with nothing stored. The D1 path builds the batch in memory first and
// validates while it does, so the same error is returned with nothing stored
// AND without having opened a transaction. The observable contract is identical
// and strictly cheaper.
//
// SQLite notes
//
//   - Timestamps are TEXT in zero-padded RFC3339 UTC, which orders
//     lexicographically in chronological order, so "ORDER BY observed_at DESC,
//     created_at DESC" means recency first on both providers, with the same two
//     tiebreaks. No CAST and no numeric ordering is added.
//   - is_current and complete_snapshot are INTEGER 0/1, written through
//     d1BoolValue and read through the same conversion, so a stored TRUE is a 1
//     exactly as MariaDB's TINYINT scan yields.
//   - Optional entry columns keep NULL distinct from their zero value. The Host
//     not exposing alwaysActive must not become the same observation as the Host
//     exposing alwaysActive:false, because the caller renders the first as
//     "unobserved" and the second as a real flag. d1AssignRow has no **bool
//     destination, so the three tri-state flags are scanned as *any and
//     converted by d1LorebookRefNullableBool, which accepts both the integer a
//     SQLite driver yields and the JSON boolean/number the Worker bridge yields.
//   - Text comparison is SQLite's binary equality where the MariaDB columns are
//     utf8mb4_unicode_ci, so D1 separates "Mira" from "mira" where MariaDB
//     merges them. The states and lifecycles compared here are lowercase
//     constants, so the divergence cannot reach this lane in practice; it is
//     the same deliberate, repo-wide divergence every other D1 reader has.
//   - The limit contract is the reference's: a non-positive limit means "no
//     limit" and the total then equals the number of rows returned, while a
//     positive limit adds a separate COUNT(*) over the SAME current predicate
//     so `total` is the unpaged size of the set being paged.

var _ LorebookReferenceStore = (*d1Store)(nil)
var _ LorebookReferenceExplorerStore = (*d1Store)(nil)

// d1LorebookRefWriteMu serialises the read-then-batch sequence of one
// ApplyLorebookReferenceSnapshot across the whole process. It stands in for the
// MariaDB transaction's SELECT ... FOR UPDATE, which is the only thing that
// could have made the scope resolution and the write a single critical section.
// It is process-local, exactly as the precise-memory write fence is.
var d1LorebookRefWriteMu sync.Mutex

// d1LorebookRefLockUpsert is the MariaDB session-lock statement. It creates the
// per-session row on first use and leaves it untouched afterwards, because the
// assigned value is the value the row already holds.
const d1LorebookRefLockUpsert = `
	INSERT INTO lorebook_reference_session_locks (chat_session_id)
	VALUES (?)
	ON CONFLICT(chat_session_id) DO UPDATE SET chat_session_id = excluded.chat_session_id`

// d1LorebookRefScopeInsert creates the exact observed Host scope. RETURNING is
// the D1 replacement for MariaDB's LastInsertId.
const d1LorebookRefScopeInsert = `
	INSERT INTO lorebook_reference_scopes
		(chat_session_id, character_index, chat_index, enabled_modules_json, scope_identity_json)
	VALUES (?, ?, ?, ?, ?)
	RETURNING scope_id`

// d1LorebookRefScopeSelect is the candidate read. It is NOT restricted to one
// row: the module set is not a column, so the caller must compare every
// candidate's decoded identity before deciding which scope is meant.
const d1LorebookRefScopeSelect = `
	SELECT scope_id, enabled_modules_json, scope_identity_json, created_at, updated_at
	FROM lorebook_reference_scopes
	WHERE chat_session_id = ? AND character_index IS ? AND chat_index IS ?
	ORDER BY scope_id ASC`

// d1LorebookRefLatestAuthority reads the newest observation that may legally
// decide the current projection. A partial or unavailable observation is
// deliberately excluded, because those never replace the projection and must
// not make a later, genuinely newer observation look out of order.
const d1LorebookRefLatestAuthority = `
	SELECT observed_at
	FROM lorebook_reference_snapshots
	WHERE scope_id = ?
	  AND (consent_state = ? OR
	       (consent_state = ? AND observation_state = ? AND complete_snapshot = TRUE))
	ORDER BY observed_at DESC, created_at DESC, snapshot_id DESC
	LIMIT 1`

const d1LorebookRefSnapshotInsert = `
	INSERT INTO lorebook_reference_snapshots
		(snapshot_id, scope_id, contract_version, consent_state, observation_state,
		 complete_snapshot, entry_count, provenance_json, observed_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

// d1LorebookRefDemoteEntries retires the whole current projection of one scope.
// It is scoped by scope_id, never by snapshot: consent revocation and a complete
// re-observation both invalidate the entries of the SCOPE, which is what keeps
// one module set's projection from retiring another's.
const d1LorebookRefDemoteEntries = `
	UPDATE lorebook_reference_entries
	SET is_current = FALSE, lifecycle_state = ?, last_seen_at = ?
	WHERE scope_id = ? AND is_current = TRUE`

// d1LorebookRefCurrentCount is the count the demote's WHERE clause matches. The
// two statements are kept adjacent in this file because they are the same set:
// the pre-read supplies PreviousCurrentCount and the UPDATE retires it.
const d1LorebookRefCurrentCount = `
	SELECT COUNT(*) FROM lorebook_reference_entries WHERE scope_id = ? AND is_current = TRUE`

// d1LorebookRefEntryInsert is the MariaDB entry insert with no dialect change.
// The 25 columns and their order are load-bearing: a reordered pair silently
// transposes two adjacent Host fields, and Host lore is rendered verbatim.
const d1LorebookRefEntryInsert = `
	INSERT INTO lorebook_reference_entries
		(scope_id, snapshot_id, host_entry_id, entry_ordinal, source_kind, source_identity,
		 entry_key, second_key, entry_comment, content, normalized_search_text, entry_mode,
		 always_active, selective, use_regex, insert_order, activation_percent, book_version,
		 folder, extensions_json, content_hash, lifecycle_state, is_current, first_seen_at, last_seen_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// d1LorebookRefLatestSnapshot picks the snapshot that describes the current
// view. It is a plain newest-first read, not the authority predicate above: the
// page shows what was last OBSERVED, including a partial or unavailable report,
// because "the host could not read the lorebook" is information the UI needs.
const d1LorebookRefLatestSnapshot = `
	SELECT snapshot_id, contract_version, consent_state, observation_state, complete_snapshot,
	       provenance_json, observed_at
	FROM lorebook_reference_snapshots
	WHERE scope_id = ? ORDER BY observed_at DESC, created_at DESC, snapshot_id DESC LIMIT 1`

// d1LorebookRefEntryColumns is the MariaDB entry projection, unchanged and in
// the same order, so the scan and the statement cannot drift apart.
const d1LorebookRefEntryColumns = `host_entry_id, entry_ordinal, source_kind, source_identity, entry_key, second_key,
		       entry_comment, content, normalized_search_text, entry_mode, always_active, selective,
		       use_regex, insert_order, activation_percent, book_version, folder, extensions_json, content_hash`

// d1LorebookRefLatestSessionScope resolves the newest scope that actually has an
// observation. The scope_id tiebreak is what makes the answer stable when two
// scopes of one session share an observation instant.
const d1LorebookRefLatestSessionScope = `
	SELECT scope.scope_identity_json
	FROM lorebook_reference_scopes AS scope
	JOIN lorebook_reference_snapshots AS snapshot ON snapshot.scope_id = scope.scope_id
	WHERE scope.chat_session_id = ?
	ORDER BY snapshot.observed_at DESC, snapshot.created_at DESC, snapshot.snapshot_id DESC, scope.scope_id DESC
	LIMIT 1`

// d1LorebookRefNullableBool converts a scanned tri-state flag column into the
// caller's pointer. A NULL means the Host never exposed the field and must stay
// nil; 0/1 and false/true both mean the field was observed. Collapsing the two
// would let "unobserved" render as "observed false" in the lorebook panel.
func d1LorebookRefNullableBool(value any) *bool {
	switch v := value.(type) {
	case nil:
		return nil
	case bool:
		out := v
		return &out
	case int64:
		out := v != 0
		return &out
	case int:
		out := v != 0
		return &out
	case float64:
		out := v != 0
		return &out
	default:
		return nil
	}
}

// d1LorebookRefNullableFlag renders a tri-state Go flag as the SQLite integer
// the schema declares, or SQL NULL when the Host did not expose it. The MariaDB
// path binds a Go bool through database/sql; binding a Go bool through the D1
// transport would leave its encoding to the driver on one path and to JSON on
// the other, so the conversion is made explicit here instead.
func d1LorebookRefNullableFlag(value *bool) any {
	if value == nil {
		return nil
	}
	return d1BoolValue(*value)
}

// d1LorebookRefFindScope resolves the exact observed scope, or ErrNotFound.
//
// The MariaDB signature takes a forUpdate flag because the write path needed a
// locked read and the read path did not. D1 has one mechanism for both — the
// write mutex plus the batch — so the flag disappears rather than becoming a
// second code path.
func (s *d1Store) d1LorebookRefFindScope(ctx context.Context, scope LorebookReferenceScope) (*lorebookReferenceScopeRow, error) {
	modulesJSON, identityJSON, err := lorebookScopeJSON(scope)
	if err != nil {
		return nil, err
	}
	rows, err := s.conn.Query(ctx, d1LorebookRefScopeSelect,
		strings.TrimSpace(scope.ChatSessionID),
		nullableInt64Pointer(scope.CharacterIndex),
		nullableInt64Pointer(scope.ChatIndex))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item lorebookReferenceScopeRow
		if err := rows.Scan(&item.ScopeID, &item.EnabledModulesJSON, &item.ScopeIdentityJSON, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		var storedModules []string
		if err := json.Unmarshal([]byte(item.EnabledModulesJSON), &storedModules); err != nil {
			return nil, err
		}
		storedCanonical, _ := json.Marshal(canonicalLorebookModuleIDs(storedModules))
		var storedScope LorebookReferenceScope
		if err := json.Unmarshal([]byte(item.ScopeIdentityJSON), &storedScope); err != nil {
			return nil, err
		}
		_, storedIdentityJSON, identityErr := lorebookScopeJSON(storedScope)
		if string(storedCanonical) == modulesJSON && identityErr == nil && storedIdentityJSON == identityJSON {
			return &item, nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return nil, ErrNotFound
}

// d1LorebookRefLatestAuthorityObservation reports the newest observation that is
// allowed to decide the current projection, and whether one exists. The
// MariaDB signature's sql.ErrNoRows branch becomes errD1NoRows here, and the
// "no authority yet" case is still (zero time, false, nil) rather than an
// error: a first observation is in order by definition.
func (s *d1Store) d1LorebookRefLatestAuthorityObservation(ctx context.Context, scopeID int64) (time.Time, bool, error) {
	var observedAt time.Time
	err := s.conn.QueryRow(ctx, d1LorebookRefLatestAuthority, scopeID,
		LorebookConsentRevoked, LorebookConsentActive, LorebookObservationObserved).Scan(&observedAt)
	if err == errD1NoRows {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return observedAt.UTC(), true, nil
}

// d1LorebookRefCountCurrent counts the current entries of one scope, which is
// both the demote's matched-row count and the paged total.
func (s *d1Store) d1LorebookRefCountCurrent(ctx context.Context, scopeID int64) (int, error) {
	var total int
	if err := s.conn.QueryRow(ctx, d1LorebookRefCurrentCount, scopeID).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

// ApplyLorebookReferenceSnapshot records one Host lorebook observation and, when
// the observation is authoritative, replaces the current entry projection of
// this exact scope.
//
// A replay is not idempotent by design on either provider: snapshot_id is the
// primary key of the append-only observation ledger, and re-submitting the same
// observation must not create a second ledger row that would then compete with
// the first for "latest observation". The collision therefore surfaces as the
// database's own duplicate-key failure out of the batch, with the batch rolled
// back, instead of being resolved to a silent overwrite.
//
// Entry normalisation (search text, content hash, ordinal, source-kind default,
// extensions default) uses the same shared helpers the MariaDB path uses, so a
// snapshot recorded on one provider yields the same diagnostic columns on the
// other. The content hash in particular is diagnostic only and is never an
// identity or admission gate, exactly as the schema comment says.
func (s *d1Store) ApplyLorebookReferenceSnapshot(ctx context.Context, item *LorebookReferenceSnapshot) (*LorebookReferenceSnapshotResult, error) {
	if err := ValidateLorebookReferenceSnapshot(item); err != nil {
		return nil, err
	}
	modulesJSON, identityJSON, err := lorebookScopeJSON(item.Scope)
	if err != nil {
		return nil, err
	}
	// A blank provenance is stored as the empty JSON object rather than NULL:
	// provenance_json is NOT NULL, and "{}" keeps the column valid JSON so a
	// later reader can decode it unconditionally.
	provenanceJSON := strings.TrimSpace(item.ProvenanceJSON)
	if provenanceJSON == "" {
		provenanceJSON = "{}"
	}
	if !json.Valid([]byte(provenanceJSON)) {
		return nil, ErrInvalidLorebookReference
	}
	// The zero substitution happens on the Go value, not only on the rendered
	// string, because the same value decides the out-of-order comparison below.
	// Falling back to the zero time there would make every caller that omits
	// observed_at look like a stale observation and silently refuse to replace
	// the current projection.
	observedAt := item.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	observedAtValue := d1TimeValue(observedAt)

	// The reference holds one transaction across the scope resolution, the
	// authority check, and the writes. D1 cannot hold a transaction across a
	// read that feeds a batch, so the reads are serialised against the batch
	// by this mutex instead.
	d1LorebookRefWriteMu.Lock()
	defer d1LorebookRefWriteMu.Unlock()

	scopeRow, err := s.d1LorebookRefFindScope(ctx, item.Scope)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	// A scope that does not exist yet is created rather than reused: reusing a
	// neighbour scope's row would attach this module set's lore to another
	// module set's projection.
	scopeCreated := errors.Is(err, ErrNotFound)
	if scopeCreated {
		var scopeID int64
		if err := s.conn.QueryRow(ctx, d1LorebookRefScopeInsert,
			strings.TrimSpace(item.Scope.ChatSessionID),
			nullableInt64Pointer(item.Scope.CharacterIndex),
			nullableInt64Pointer(item.Scope.ChatIndex),
			modulesJSON, identityJSON).Scan(&scopeID); err != nil {
			return nil, err
		}
		scopeRow = &lorebookReferenceScopeRow{ScopeID: scopeID, EnabledModulesJSON: modulesJSON, ScopeIdentityJSON: identityJSON}
	}

	// An authoritative observation replaces the projection, so it is also the
	// only kind that can be out of order: a partial or unavailable report
	// changes nothing and therefore cannot be stale with respect to anything.
	authoritativeObservation := item.ConsentState == LorebookConsentRevoked ||
		(item.ConsentState == LorebookConsentActive && item.ObservationState == LorebookObservationObserved && item.CompleteSnapshot)
	outOfOrder := false
	if authoritativeObservation && !scopeCreated {
		latestObservedAt, found, latestErr := s.d1LorebookRefLatestAuthorityObservation(ctx, scopeRow.ScopeID)
		if latestErr != nil {
			return nil, latestErr
		}
		outOfOrder = found && observedAt.Before(latestObservedAt)
	}

	result := &LorebookReferenceSnapshotResult{
		ScopeID: scopeRow.ScopeID, SnapshotID: strings.TrimSpace(item.SnapshotID),
		ObservationState: item.ObservationState, ObservedEntryCount: len(item.Entries),
		LifecycleAction: "observation_recorded",
	}
	// A non-authoritative observation is stored as observed_partial and never
	// as current, so a partial read can never be rendered as the catalog.
	entryLifecycle := LorebookLifecyclePartial
	entryCurrent := false
	// demoteLifecycle is empty when this observation leaves the current
	// projection alone, which is the case for out-of-order, partial, and
	// unavailable observations.
	demoteLifecycle := ""
	if outOfOrder {
		entryLifecycle = LorebookLifecycleStale
		result.LifecycleAction = "out_of_order_observation_recorded"
	} else if item.ConsentState == LorebookConsentRevoked {
		demoteLifecycle = LorebookLifecycleConsentRevoked
		result.LifecycleAction = "consent_revoked"
	} else if item.ObservationState == LorebookObservationObserved && item.CompleteSnapshot {
		demoteLifecycle = LorebookLifecycleStale
		result.LifecycleAction = "current_projection_replaced"
		entryLifecycle = LorebookLifecycleCurrent
		entryCurrent = true
	}

	// The demote retires exactly the rows this count selects, so the count read
	// here is the demote's affected-row count. See the file comment: a batch
	// cannot report a suppressed statement, and taking the demote out of the
	// batch to learn its count would cost the whole write its atomicity.
	if demoteLifecycle != "" {
		previous, err := s.d1LorebookRefCountCurrent(ctx, scopeRow.ScopeID)
		if err != nil {
			return nil, err
		}
		result.PreviousCurrentCount = int64(previous)
	}

	statements := make([]D1Statement, 0, len(item.Entries)+3)
	statements = append(statements, D1Statement{
		SQL:  d1LorebookRefLockUpsert,
		Args: []any{strings.TrimSpace(item.Scope.ChatSessionID)},
	})
	statements = append(statements, D1Statement{
		SQL: d1LorebookRefSnapshotInsert,
		// The snapshot ledger row stores the TRIMMED snapshot id while the
		// entry rows below store the RAW one. That inconsistency is the
		// reference's, and it is reproduced rather than fixed: a caller that
		// padded its snapshot id gets a trimmed ledger key and padded entry
		// keys on both providers, and "cleaning" only the D1 side would make
		// the two providers disagree about which snapshot an entry belongs to.
		Args: []any{strings.TrimSpace(item.SnapshotID), scopeRow.ScopeID, item.ContractVersion,
			item.ConsentState, item.ObservationState, d1BoolValue(item.CompleteSnapshot),
			len(item.Entries), provenanceJSON, observedAtValue},
	})
	if demoteLifecycle != "" {
		statements = append(statements, D1Statement{
			SQL:  d1LorebookRefDemoteEntries,
			Args: []any{demoteLifecycle, observedAtValue, scopeRow.ScopeID},
		})
	}
	for ordinal, observed := range item.Entries {
		// The ordinal is assigned by position in the slice, not taken from the
		// observation: the Host's ordering is what the page is ordered by, and
		// a Host-supplied ordinal would let a reordered payload renumber the
		// catalog.
		observed.EntryOrdinal = ordinal
		observed.NormalizedSearch = normalizeLorebookSearchText(observed)
		observed.ContentHash = lorebookDiagnosticContentHash(observed.Content)
		// An unlabelled source is the current host aggregate, which is the only
		// kind this adapter can produce.
		if strings.TrimSpace(observed.SourceKind) == "" {
			observed.SourceKind = "current_host_aggregate"
		}
		extensionsJSON := strings.TrimSpace(observed.ExtensionsJSON)
		if extensionsJSON == "" {
			extensionsJSON = "{}"
		}
		// Validated while the batch is assembled, so a malformed entry costs
		// nothing and leaves nothing stored.
		if !json.Valid([]byte(extensionsJSON)) {
			return nil, ErrInvalidLorebookReference
		}
		statements = append(statements, D1Statement{
			SQL: d1LorebookRefEntryInsert,
			Args: []any{scopeRow.ScopeID, item.SnapshotID,
				referenceNullable(observed.HostEntryID), ordinal,
				observed.SourceKind, referenceNullable(observed.SourceIdentity),
				observed.Key, observed.SecondKey, observed.Comment, observed.Content,
				observed.NormalizedSearch, referenceNullable(observed.Mode),
				d1LorebookRefNullableFlag(observed.AlwaysActive),
				d1LorebookRefNullableFlag(observed.Selective),
				d1LorebookRefNullableFlag(observed.UseRegex),
				nullableIntPointer(observed.InsertOrder), nullableFloatPointer(observed.ActivationPct),
				nullableInt64Pointer(observed.BookVersion), referenceNullable(observed.Folder),
				extensionsJSON, observed.ContentHash, entryLifecycle, d1BoolValue(entryCurrent),
				observedAtValue, observedAtValue},
		})
	}
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return nil, err
	}

	if entryCurrent {
		// The demote retired every current row of this scope and this snapshot's
		// entries are the only current ones left, so the count is known without
		// reading it.
		result.CurrentEntryCount = len(item.Entries)
	} else {
		// Read after the commit, because in the consent-revoked branch the count
		// is only zero once the demote has been applied. Every other branch this
		// covers writes its entries as non-current, so the committed state is
		// the same state the in-transaction count would have reported.
		total, err := s.d1LorebookRefCountCurrent(ctx, scopeRow.ScopeID)
		if err != nil {
			return nil, err
		}
		result.CurrentEntryCount = total
	}
	return result, nil
}

// d1LorebookRefGetCurrent is the shared body of the unbounded prepare-turn read
// and the bounded Explorer page. A non-positive limit means "no limit", and the
// returned total then equals the number of rows returned.
func (s *d1Store) d1LorebookRefGetCurrent(ctx context.Context, scope LorebookReferenceScope, limit, offset int) (*LorebookReferenceCurrent, int, error) {
	// A blank session is rejected before the database is touched: the
	// scope lookup would match nothing and report ErrNotFound, and the route
	// would answer 404 for what is really a malformed request.
	if strings.TrimSpace(scope.ChatSessionID) == "" {
		return nil, 0, ErrInvalidLorebookReference
	}
	scopeRow, err := s.d1LorebookRefFindScope(ctx, scope)
	if err != nil {
		return nil, 0, err
	}
	// The requested scope is echoed, not the stored one: they are equal by
	// construction, and the page must render what the caller asked for even
	// after the stored row is re-ordered.
	// The entry slice is allocated up front so a scope with no current entry
	// encodes as [] rather than null on both providers.
	result := &LorebookReferenceCurrent{
		ScopeID: scopeRow.ScopeID, Scope: scope,
		Entries: []LorebookReferenceEntryObservation{},
	}

	var snapshot LorebookReferenceSnapshot
	var provenance string
	err = s.conn.QueryRow(ctx, d1LorebookRefLatestSnapshot, scopeRow.ScopeID).Scan(
		&snapshot.SnapshotID, &snapshot.ContractVersion, &snapshot.ConsentState,
		&snapshot.ObservationState, &snapshot.CompleteSnapshot, &provenance, &snapshot.ObservedAt)
	if err != nil && !errors.Is(err, errD1NoRows) {
		return nil, 0, err
	}
	if err == nil {
		snapshot.Scope = scope
		snapshot.ProvenanceJSON = provenance
		result.LatestSnapshot = &snapshot
	}

	total := 0
	// The total is only read when the result is paged. An unbounded read
	// reports the rows it returned, which avoids a second statement for a
	// caller that already has every row in hand.
	if limit > 0 {
		if err := s.conn.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM lorebook_reference_entries
			WHERE scope_id = ? AND is_current = TRUE AND lifecycle_state = ?`,
			scopeRow.ScopeID, LorebookLifecycleCurrent).Scan(&total); err != nil {
			return nil, 0, err
		}
	}

	entryQuery := `
		SELECT ` + d1LorebookRefEntryColumns + `
		FROM lorebook_reference_entries
		WHERE scope_id = ? AND is_current = TRUE AND lifecycle_state = ?
		ORDER BY entry_ordinal ASC, entry_record_id ASC`
	entryArgs := []any{scopeRow.ScopeID, LorebookLifecycleCurrent}
	if limit > 0 {
		entryQuery += " LIMIT ? OFFSET ?"
		entryArgs = append(entryArgs, limit, offset)
	}
	rows, err := s.conn.Query(ctx, entryQuery, entryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var entry LorebookReferenceEntryObservation
		var hostID, sourceIdentity, mode, folder *string
		// d1AssignRow has no **bool destination, so the three tri-state flags
		// are read raw through the *any destination and converted; a NULL
		// stays nil.
		var alwaysActive, selective, useRegex any
		var insertOrder, bookVersion *int64
		var activationPct *float64
		if err := rows.Scan(&hostID, &entry.EntryOrdinal, &entry.SourceKind, &sourceIdentity,
			&entry.Key, &entry.SecondKey, &entry.Comment, &entry.Content, &entry.NormalizedSearch,
			&mode, &alwaysActive, &selective, &useRegex, &insertOrder, &activationPct,
			&bookVersion, &folder, &entry.ExtensionsJSON, &entry.ContentHash); err != nil {
			return nil, 0, err
		}
		// A NULL optional column reads as the empty string, never as a leftover
		// NULL marker the caller would have to special-case.
		entry.HostEntryID = d1DerefString(hostID)
		entry.SourceIdentity = d1DerefString(sourceIdentity)
		entry.Mode = d1DerefString(mode)
		entry.Folder = d1DerefString(folder)
		if value := d1LorebookRefNullableBool(alwaysActive); value != nil {
			entry.AlwaysActive = value
		}
		if value := d1LorebookRefNullableBool(selective); value != nil {
			entry.Selective = value
		}
		if value := d1LorebookRefNullableBool(useRegex); value != nil {
			entry.UseRegex = value
		}
		if insertOrder != nil {
			value := int(*insertOrder)
			entry.InsertOrder = &value
		}
		if activationPct != nil {
			value := *activationPct
			entry.ActivationPct = &value
		}
		if bookVersion != nil {
			value := *bookVersion
			entry.BookVersion = &value
		}
		result.Entries = append(result.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	if limit <= 0 {
		total = len(result.Entries)
	}
	return result, total, nil
}

// GetLorebookReferenceCurrent returns the whole current projection of one exact
// scope. It is the prepare-turn read, and it is deliberately unbounded: the
// caller applies its own context-overlap ranking and character budget to every
// candidate, so truncating here would silently drop lore the ranking never saw.
func (s *d1Store) GetLorebookReferenceCurrent(ctx context.Context, scope LorebookReferenceScope) (*LorebookReferenceCurrent, error) {
	result, _, err := s.d1LorebookRefGetCurrent(ctx, scope, 0, 0)
	return result, err
}

// GetLorebookReferenceCurrentPage returns one bounded window of the same
// projection, for the Explorer surface.
//
// The page is rejected for a non-positive or oversized limit and a negative
// offset BEFORE any statement is sent. That guard is not politeness: an
// unbounded page would answer with the whole catalog, and the route cannot
// distinguish "the user asked for everything" from "the store ignored the
// limit". A request that carries no usable window is a caller error, so it
// cannot be answered as a successful write.
func (s *d1Store) GetLorebookReferenceCurrentPage(ctx context.Context, scope LorebookReferenceScope, limit, offset int) (*LorebookReferenceCurrentPage, error) {
	if limit <= 0 || limit > 100 || offset < 0 {
		return nil, ErrInvalidLorebookReference
	}
	current, total, err := s.d1LorebookRefGetCurrent(ctx, scope, limit, offset)
	if err != nil {
		return nil, err
	}
	return &LorebookReferenceCurrentPage{
		ScopeID: current.ScopeID, Scope: current.Scope, LatestSnapshot: current.LatestSnapshot,
		Entries: current.Entries, Total: total, Limit: limit, Offset: offset,
	}, nil
}

// GetLorebookReferenceLatestSessionPage resolves the newest observed scope of a
// session and returns one bounded window of it, for a caller that knows only
// the session.
//
// The inner scope identity is decoded and re-validated rather than trusted: the
// join returns whatever JSON the scope row holds, and a stored identity that
// names a different session would otherwise page another session's lore under
// this session's request.
func (s *d1Store) GetLorebookReferenceLatestSessionPage(ctx context.Context, chatSessionID string, limit, offset int) (*LorebookReferenceCurrentPage, error) {
	chatSessionID = strings.TrimSpace(chatSessionID)
	if chatSessionID == "" || limit <= 0 || limit > 100 || offset < 0 {
		return nil, ErrInvalidLorebookReference
	}
	var scopeIdentityJSON string
	err := s.conn.QueryRow(ctx, d1LorebookRefLatestSessionScope, chatSessionID).Scan(&scopeIdentityJSON)
	// A session with no observation is not an error the caller can fix by
	// retrying, but the route renders it as an empty page; the sentinel is
	// what tells the route to do that instead of answering 404.
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var scope LorebookReferenceScope
	if err := json.Unmarshal([]byte(scopeIdentityJSON), &scope); err != nil {
		return nil, err
	}
	if strings.TrimSpace(scope.ChatSessionID) != chatSessionID {
		return nil, ErrInvalidLorebookReference
	}
	return s.GetLorebookReferenceCurrentPage(ctx, scope, limit, offset)
}
