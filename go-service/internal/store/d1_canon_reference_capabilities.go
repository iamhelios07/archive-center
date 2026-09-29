package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// D1 Canon Pack and reference-library capabilities.
//
// Four interfaces live here, and they are four views of ONE storage graph:
//
//   - CanonRegistryStore   the catalog search, the install diagnostics, and the
//                          local overlay policy that suppresses canon material,
//   - CanonPackStore       upload, register, and retire a packed edition,
//   - ReferenceLibraryStore the hand-built reference material, plus the
//                          session-scoped bindings and runtime anchor,
//   - ReferenceCoverageStore the field-level coverage projection and the alias
//                          index the coverage pass reads.
//
// They are ported together because splitting them reproduces the failure this
// file exists to prevent: reference rows that exist but have no owning edition,
// a pack whose bytes are stored but never registered, or an approved projection
// whose canon-pack origin has been retired while the reader still trusts it.
//
// The MariaDB-only constructs, and what replaces each
//
//  1. `ON DUPLICATE KEY UPDATE` and `INSERT IGNORE`. SQLite has no key-agnostic
//     "any unique index" spelling in the statement text, but its upsert DOES
//     accept an omitted conflict target, which matches MySQL semantics exactly:
//     a collision on the primary key OR on any secondary unique index takes the
//     DO UPDATE / DO NOTHING branch. A named target would be narrower than
//     MariaDB and would turn a legitimate MySQL upsert into a constraint error,
//     so the target is deliberately left off every upsert below.
//     `VALUES(col)` becomes `excluded.col`.
//
//  2. The guarded upserts. The reference writes
//     `col = IF(review_status = 'pending', VALUES(col), col)`, which is how a
//     machine-extracted candidate is editable while it is still pending and
//     frozen the moment a human approves it. SQLite evaluates every SET
//     expression against the row as it was before the statement, exactly as
//     MySQL does when the review_status assignment comes last, so the guard is
//     reproduced as `CASE WHEN review_status = 'pending' THEN excluded.col ELSE
//     col END` without reordering.
//
//  3. `SELECT ... FOR UPDATE`. D1 has no row locks. Every read that feeds a
//     batch in this file is therefore taken under one of the two process-local
//     mutexes below, which is the same fence d1_precise_memory_write_ and
//     d1_lorebook_reference_ use and carries the same documented limitation: it
//     does not serialise two Cloudflare Containers. What replaces the lock is
//     the batch itself, which D1 commits atomically and in order, plus the
//     natural keys and CHECK constraints the canonical schema already carries.
//
//  4. `LastInsertId`. Two places need it, and neither is an AUTOINCREMENT
//     surrogate a peer statement must reference:
//     - reference_entity_aliases.alias_id is returned by the upsert itself with
//       RETURNING, which SQLite supports on the DO UPDATE branch, so the caller
//       still learns the id it would have learned from the driver.
//     - canon_pack_installs.install_generation is not a surrogate at all. The
//       reference computes it as MAX(install_generation) + 1 under FOR UPDATE,
//       so the faithful translation is the SAME expression as a scalar subquery
//       evaluated INSIDE the batch, after the demote that precedes it. Predicting
//       it in Go from a pre-read would let a second Container allocate the same
//       generation; evaluated in the batch, the UNIQUE (pack_id, pack_version,
//       install_generation) constraint turns that race into a conflict error
//       instead of a silently duplicated generation.
//
//  5. The multi-statement transaction. Every MariaDB transaction in this file
//     becomes exactly one D1Conn.Batch. That is not a convenience: InstallCanonPack
//     writes the pack content, the registry row, and the evidence edges as one
//     user-visible operation, and a pack whose bytes are stored but not
//     registered is a download that will never resolve.
//
// What had to be pre-read, and why that is safe
//
// A batch cannot return a row, so three MariaDB SELECTs had to move in front of
// the batch. Each is a pure existence/identity lookup whose result decides
// WHICH statements are emitted, and each is emitted into a plan the batch then
// executes:
//
//   - the work edition, deciding whether the pack creates a new work/edition pair;
//   - reference_documents, deciding the document_id an observation binds to;
//   - reference_fact_identities, deciding whether a claim is new or already the
//     same exact fact.
//
// The last one is the interesting case. Inside the MariaDB transaction, a second
// manifest record carrying an identical exact fingerprint finds the row the
// first record inserted a moment earlier. A pre-read cannot see that, so the
// plan tracks the fingerprints it has already allocated: a repeat takes the
// already-exists branch exactly as the locked SELECT would have. Without that,
// two identical records in one pack would emit two inserts of the same
// fingerprint and fail the whole install on the unique index.
//
// JSON notes
//
//   - reference_claims.metadata_json is written with
//     `json_set(COALESCE(metadata_json, '{}'), '$.evidence_grounded',
//     json('false'))`. The json() wrapper is load-bearing: SQLite binds the bare
//     literal FALSE as the integer 0 and would store `{"evidence_grounded":0}`,
//     which is not what MariaDB writes.
//   - The diagnostic source projection returns retrieved_at as a time.Time, not
//     as the stored TEXT. MariaDB scans DATETIME(3) into a time.Time and the
//     route marshals it as RFC3339; handing the raw TEXT through would render
//     the same instant with a different shape in the same JSON field.
//
// updated_at, which the SQLite schema cannot carry on its own
//
// The MariaDB columns are `ON UPDATE CURRENT_TIMESTAMP(3)`, so every UPDATE the
// reference issues also moves updated_at. The canonical schema has no trigger,
// and the D1 statements here therefore name updated_at explicitly. Dropping it
// would not merely lose a diagnostic column: ListReferenceWorks, ListCanonPack-
// Insts and ListCanonOverlays all order by updated_at DESC, so a bumped revision
// that never moves the timestamp would make the newest write sort as the oldest.
//
// Accepted divergences (see the slice brief; none of them is fixed here)
//   - Text comparison is SQLite BINARY equality where the MariaDB columns are
//     utf8mb4_unicode_ci. Every value compared in this file is a lowercase
//     state or a canonNormalize()-lowercased lookup key, so it cannot reach the
//     canon lanes in practice.
//   - A no-op UPDATE reports 1 affected row on SQLite and 0 on MariaDB.

var _ CanonRegistryStore = (*d1Store)(nil)
var _ CanonPackStore = (*d1Store)(nil)
var _ ReferenceLibraryStore = (*d1Store)(nil)
var _ ReferenceCoverageStore = (*d1Store)(nil)

// d1CanonWriteMu serialises the pre-reads that feed the Canon Pack and registry
// batches. It stands in for the FOR UPDATE that the reference takes on the
// install row, the edition pair, and the competing active install.
var d1CanonWriteMu sync.Mutex

// d1ReferenceWriteMu serialises the pre-reads that feed the reference-library
// and coverage batches: the in-use binding probe before a work delete, the
// timeline ordering sets, and the coverage snapshot revision.
var d1ReferenceWriteMu sync.Mutex

// d1ReferenceStoreError maps a SQLite constraint failure onto the two domain
// errors the reference returns from its MySQL error numbers.
//
// The dialect is different and the mapping is by message rather than by number,
// because the bridge transports the engine error as text. What must not drift is
// the two error values: 1062 (duplicate key) is ErrReferenceConflict, and
// 1451/1452 (parent or child row missing) is ErrInvalidReference. Everything
// else passes through, including NOT NULL and CHECK violations, exactly as the
// reference passes its unmapped MySQL numbers through.
func d1ReferenceStoreError(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "UNIQUE constraint failed"),
		strings.Contains(text, "PRIMARY KEY constraint failed"):
		return fmt.Errorf("%w: %v", ErrReferenceConflict, err)
	case strings.Contains(text, "FOREIGN KEY constraint failed"):
		return fmt.Errorf("%w: %v", ErrInvalidReference, err)
	}
	return err
}

// d1ReferenceRowsChanged is the D1 form of referenceRowsChanged: a delete that
// matched nothing is a missing row, not a silent success.
func d1ReferenceRowsChanged(affected int64) error {
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// d1ReferenceRevisionChanged is the D1 form of referenceRevisionChanged: a
// guarded update that matched nothing lost the optimistic-concurrency race.
func d1ReferenceRevisionChanged(affected int64) error {
	if affected == 0 {
		return ErrReferenceConflict
	}
	return nil
}

// ---------------------------------------------------------------------------
// CanonRegistryStore — catalog search
// ---------------------------------------------------------------------------

// SearchCanonRegistry is the Explorer catalog lookup: a substring match over
// the normalised title index, collapsed to one item per install, each carrying
// every title that matched.
//
// The inner LIMIT is the reference's over-fetch: one install can own several
// titles that all contain the needle, so the SQL is bounded at limit*4 rows and
// the loop stops once limit DISTINCT installs have been collected. Truncating
// that to LIMIT would drop installs whose first matching title happened to sort
// late.
//
// The exact-title preference stays in SQL. `ORDER BY (t.normalized_lookup_key = ?)
// DESC` is what makes a work whose title IS the query rank above a work whose
// title merely contains it, and moving that comparison into Go would mean
// fetching every match and sorting in memory.
func (s *d1Store) SearchCanonRegistry(ctx context.Context, query string, limit int) ([]CanonRegistryItem, error) {
	lookup := canonNormalize(query)
	if lookup == "" {
		return nil, ErrInvalidReference
	}
	querySQL := `
		SELECT i.install_id, i.pack_id, i.pack_version, i.lifecycle_status,
		       i.work_id, i.edition_row_id, e.stable_work_id, e.edition_id, w.title,
		       t.title_text, i.review_status, i.trust_status, i.manifest_json,
		       i.coverage_report_json,
		       (SELECT COUNT(*) FROM reference_source_observations s WHERE s.install_id = i.install_id)
		FROM canon_pack_installs i
		JOIN reference_work_editions e ON e.edition_row_id = i.edition_row_id
		JOIN reference_works w ON w.work_id = i.work_id
		JOIN reference_work_titles t ON t.edition_row_id = i.edition_row_id
		WHERE i.lifecycle_status <> 'removed' AND t.normalized_lookup_key LIKE ?
		ORDER BY (t.normalized_lookup_key = ?) DESC, i.updated_at DESC, t.title_kind, t.title_text
	`
	args := []any{"%" + lookup + "%", lookup}
	if limit > 0 {
		querySQL += " LIMIT ?"
		args = append(args, limit*4)
	}
	rows, err := s.conn.Query(ctx, querySQL, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	// The slice is allocated up front so an empty catalog encodes as [] and not
	// as null on both providers.
	items := []CanonRegistryItem{}
	byInstall := map[string]int{}
	for rows.Next() {
		var item CanonRegistryItem
		var matchedTitle string
		var manifestRaw, coverageRaw *string
		if err := rows.Scan(&item.InstallID, &item.PackID, &item.PackVersion, &item.LifecycleStatus,
			&item.WorkID, &item.EditionRowID, &item.StableWorkID, &item.EditionID, &item.Title,
			&matchedTitle, &item.ReviewStatus, &item.TrustStatus, &manifestRaw, &coverageRaw,
			&item.SourceCount); err != nil {
			return nil, err
		}
		if index, exists := byInstall[item.InstallID]; exists {
			items[index].MatchedTitles = appendUniqueString(items[index].MatchedTitles, matchedTitle)
			continue
		}
		_ = json.Unmarshal([]byte(d1DerefString(coverageRaw)), &item.CoverageReport)
		var manifest map[string]any
		_ = json.Unmarshal([]byte(d1DerefString(manifestRaw)), &manifest)
		item.ConflictCount = len(canonArray(manifest["conflicts"]))
		item.UncertainCount = len(canonArray(manifest["uncertainties"]))
		item.MatchedTitles = []string{matchedTitle}
		byInstall[item.InstallID] = len(items)
		items = append(items, item)
		if limit > 0 && len(items) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	// The continuity list is per work, not per install, and two installs of the
	// same work share it. Caching by work id keeps that a single read per work
	// and makes both installs report the identical list.
	continuitiesByWork := map[string][]string{}
	for i := range items {
		if cached, ok := continuitiesByWork[items[i].WorkID]; ok {
			items[i].Continuities = cached
			continue
		}
		continuityRows, err := s.conn.Query(ctx, `
			SELECT continuity_key FROM reference_continuities
			WHERE work_id = ? AND status = 'active' ORDER BY continuity_key`, items[i].WorkID)
		if err != nil {
			return nil, err
		}
		values := []string{}
		for continuityRows.Next() {
			var value string
			if err := continuityRows.Scan(&value); err != nil {
				continuityRows.Close()
				return nil, err
			}
			values = append(values, value)
		}
		if err := continuityRows.Close(); err != nil {
			return nil, err
		}
		continuitiesByWork[items[i].WorkID] = values
		items[i].Continuities = values
	}
	return items, nil
}

// ---------------------------------------------------------------------------
// CanonRegistryStore — install diagnostics
// ---------------------------------------------------------------------------

// GetCanonPackDiagnostics renders the install report: its source observations
// and six quality verdicts.
//
// Every quality number is a correlated count or a grouped subquery and stays in
// SQL. In particular the two "grounded" counts are COUNT over a GROUP BY of the
// origin rows joined to their evidence, which is what makes them per-ITEM counts
// rather than per-EDGE counts: an item with four evidence edges is one grounded
// item, and an item with edges from two different observations is one
// corroborated item. Computing either in Go from a flattened edge list would
// count the same item four times and report traceability as unsatisfied for a
// fully evidenced pack.
func (s *d1Store) GetCanonPackDiagnostics(ctx context.Context, installID string) (*CanonPackDiagnostics, error) {
	installID = strings.TrimSpace(installID)
	var lifecycle string
	var manifestRaw, coverageRaw *string
	err := s.conn.QueryRow(ctx, `
		SELECT lifecycle_status, manifest_json, coverage_report_json
		FROM canon_pack_installs WHERE install_id = ?
	`, installID).Scan(&lifecycle, &manifestRaw, &coverageRaw)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	result := &CanonPackDiagnostics{
		Contract: CanonPackDiagnosticsContract, InstallID: installID,
		LifecycleStatus: lifecycle, Sources: []map[string]any{}, Quality: map[string]any{},
	}
	var manifest map[string]any
	_ = json.Unmarshal([]byte(d1DerefString(manifestRaw)), &manifest)
	_ = json.Unmarshal([]byte(d1DerefString(coverageRaw)), &result.CoverageReport)
	result.Conflicts = canonArray(manifest["conflicts"])
	result.Uncertainties = canonArray(manifest["uncertainties"])

	rows, err := s.conn.Query(ctx, `
		SELECT source_key, source_type, COALESCE(source_uri, ''), access_class,
		       retrieved_at, document_sha256, COALESCE(document_id, '')
		FROM reference_source_observations WHERE install_id = ?
		ORDER BY source_type, source_key
	`, installID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var key, sourceType, uri, accessClass, hash, documentID, retrievedAt string
		if err := rows.Scan(&key, &sourceType, &uri, &accessClass, &retrievedAt, &hash, &documentID); err != nil {
			rows.Close()
			return nil, err
		}
		result.Sources = append(result.Sources, map[string]any{
			"source_key": key, "source_type": sourceType, "uri": uri,
			"access_class": accessClass, "retrieved_at": d1CanonRetrievedAt(retrievedAt),
			"document_sha256": hash, "document_id": documentID,
		})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	var itemCount, evidenceBound, multiSource, claimCount, connectedClaims, logicalFacts int
	if err := s.conn.QueryRow(ctx,
		`SELECT COUNT(*) FROM reference_item_origins WHERE install_id = ?`, installID).Scan(&itemCount); err != nil {
		return nil, err
	}
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT o.item_kind, o.source_item_id
			FROM reference_item_origins o
			JOIN reference_item_evidence ev ON
				(o.item_kind='entity' AND ev.entity_id=o.entity_id) OR
				(o.item_kind='timeline' AND ev.node_id=o.node_id) OR
				(o.item_kind='claim' AND ev.claim_id=o.claim_id)
			WHERE o.install_id = ? GROUP BY o.item_kind, o.source_item_id
		) grounded
	`, installID).Scan(&evidenceBound); err != nil {
		return nil, err
	}
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT o.item_kind, o.source_item_id
			FROM reference_item_origins o
			JOIN reference_item_evidence ev ON
				(o.item_kind='entity' AND ev.entity_id=o.entity_id) OR
				(o.item_kind='timeline' AND ev.node_id=o.node_id) OR
				(o.item_kind='claim' AND ev.claim_id=o.claim_id)
			WHERE o.install_id = ? GROUP BY o.item_kind, o.source_item_id
			HAVING COUNT(DISTINCT ev.source_observation_id) > 1
		) corroborated
	`, installID).Scan(&multiSource); err != nil {
		return nil, err
	}
	// The SUM keeps the "claims linked to an entity" count on the same statement
	// as the claim count. Splitting it would let the two disagree when a claim
	// is deleted between two separate reads.
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN c.subject_entity_id IS NOT NULL THEN 1 ELSE 0 END), 0)
		FROM reference_item_origins o JOIN reference_claims c ON c.claim_id = o.claim_id
		WHERE o.install_id = ? AND o.item_kind = 'claim'
	`, installID).Scan(&claimCount, &connectedClaims); err != nil {
		return nil, err
	}
	if err := s.conn.QueryRow(ctx, `
		SELECT COUNT(DISTINCT fi.logical_fact_id)
		FROM reference_item_origins o JOIN reference_fact_identities fi ON fi.claim_id = o.claim_id
		WHERE o.install_id = ? AND o.item_kind = 'claim'
	`, installID).Scan(&logicalFacts); err != nil {
		return nil, err
	}
	result.Quality = map[string]any{
		"traceability":                 map[string]any{"status": diagnosticStatus(itemCount == evidenceBound), "items": itemCount, "evidence_bound_items": evidenceBound},
		"evidence":                     map[string]any{"status": diagnosticStatus(evidenceBound > 0 || itemCount == 0), "source_observations": len(result.Sources)},
		"independent_source_agreement": map[string]any{"status": "unassessed", "multi_source_items": multiSource, "single_source_or_unassessed_items": evidenceBound - multiSource, "note": "Multiple observations are not treated as independent until mirror and repost relationships are classified."},
		"duplicate_and_conflict":       map[string]any{"logical_facts": logicalFacts, "conflicts": len(result.Conflicts), "uncertainties": len(result.Uncertainties)},
		"entity_connectivity":          map[string]any{"claims": claimCount, "claims_linked_to_entity": connectedClaims},
		"coverage":                     result.CoverageReport,
	}
	return result, nil
}

// d1CanonRetrievedAt renders a stored observation timestamp the way the MariaDB
// scan renders it. MariaDB scans DATETIME(3) into a time.Time and the route
// marshals that as RFC3339; handing the stored TEXT straight through would put
// a millisecond-suffixed string in the same field. An unparseable value is kept
// verbatim rather than dropped, because losing it would silently shrink the
// source list.
func d1CanonRetrievedAt(value string) any {
	if parsed, err := parseD1Time(value); err == nil {
		return parsed
	}
	return value
}

// ---------------------------------------------------------------------------
// CanonRegistryStore — local overlay policy
// ---------------------------------------------------------------------------

// CreateCanonOverlay records one local override or suppression over canon or
// library material.
//
// The target is resolved to its owning work and edition BEFORE the row is
// written, and a mismatch is a conflict rather than a silently cross-work
// overlay. The same holds for a replacement: it must belong to the same work
// and be approved, because a rule that suppresses a canon claim in favour of an
// unapproved user claim would let an unvetted statement win.
func (s *d1Store) CreateCanonOverlay(ctx context.Context, input CanonOverlayInput) (*CanonOverlayRule, error) {
	input.WorkID, input.EditionRowID = strings.TrimSpace(input.WorkID), strings.TrimSpace(input.EditionRowID)
	input.TargetKind, input.TargetID, input.Action = strings.TrimSpace(input.TargetKind), strings.TrimSpace(input.TargetID), strings.TrimSpace(input.Action)
	if input.WorkID == "" || input.EditionRowID == "" || input.TargetID == "" ||
		(input.Action != "supplement" && input.Action != "override" && input.Action != "suppress_for_retrieval" && input.Action != "conflict") {
		return nil, ErrInvalidReference
	}
	var editionWork string
	err := s.conn.QueryRow(ctx,
		`SELECT work_id FROM reference_work_editions WHERE edition_row_id = ?`, input.EditionRowID).Scan(&editionWork)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if editionWork != input.WorkID {
		return nil, ErrReferenceConflict
	}

	rule := &CanonOverlayRule{
		OverlayRuleID: canonStableID("canon-overlay", input.WorkID, input.EditionRowID, input.TargetKind, input.TargetID, input.Action, input.ReplacementID),
		WorkID:        input.WorkID, EditionRowID: input.EditionRowID, Action: input.Action,
		Status: "active", Reason: strings.TrimSpace(input.Reason),
	}
	switch input.TargetKind {
	case "claim":
		// A claim target resolves through its fact identity, so the overlay is
		// written against the LOGICAL fact. Two claims with the same exact
		// fingerprint share the fact, and an overlay on one of them would
		// otherwise leave the other readable.
		err = s.conn.QueryRow(ctx, `
			SELECT fi.logical_fact_id FROM reference_fact_identities fi
			JOIN reference_claims c ON c.claim_id = fi.claim_id
			WHERE fi.claim_id = ? AND c.work_id = ? AND fi.edition_row_id = ?
		`, input.TargetID, input.WorkID, input.EditionRowID).Scan(&rule.TargetLogicalFactID)
		if errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
	case "logical_fact":
		var targetWork, targetEdition string
		err = s.conn.QueryRow(ctx,
			`SELECT work_id, edition_row_id FROM reference_logical_facts WHERE logical_fact_id = ?`,
			input.TargetID).Scan(&targetWork, &targetEdition)
		if errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		if targetWork != input.WorkID || targetEdition != input.EditionRowID {
			return nil, ErrReferenceConflict
		}
		rule.TargetLogicalFactID = input.TargetID
	case "entity":
		var targetWork string
		err = s.conn.QueryRow(ctx, `SELECT work_id FROM reference_entities WHERE entity_id = ?`,
			input.TargetID).Scan(&targetWork)
		if errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		if targetWork != input.WorkID {
			return nil, ErrReferenceConflict
		}
		rule.TargetEntityID = input.TargetID
	case "timeline":
		var targetWork string
		err = s.conn.QueryRow(ctx, `SELECT work_id FROM reference_timeline_nodes WHERE node_id = ?`,
			input.TargetID).Scan(&targetWork)
		if errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		if targetWork != input.WorkID {
			return nil, ErrReferenceConflict
		}
		rule.TargetNodeID = input.TargetID
	default:
		return nil, ErrInvalidReference
	}
	if input.Action == "override" && strings.TrimSpace(input.ReplacementID) == "" {
		return nil, ErrInvalidReference
	}
	switch strings.TrimSpace(input.ReplacementKind) {
	case "":
	case "claim":
		rule.ReplacementClaimID = strings.TrimSpace(input.ReplacementID)
	case "entity":
		rule.ReplacementEntityID = strings.TrimSpace(input.ReplacementID)
	case "timeline":
		rule.ReplacementNodeID = strings.TrimSpace(input.ReplacementID)
	default:
		return nil, ErrInvalidReference
	}
	for _, replacement := range []struct {
		id     string
		table  string
		column string
	}{
		{rule.ReplacementClaimID, "reference_claims", "claim_id"},
		{rule.ReplacementEntityID, "reference_entities", "entity_id"},
		{rule.ReplacementNodeID, "reference_timeline_nodes", "node_id"},
	} {
		if replacement.id == "" {
			continue
		}
		var replacementWork, reviewStatus string
		err = s.conn.QueryRow(ctx,
			"SELECT work_id, review_status FROM "+replacement.table+" WHERE "+replacement.column+" = ?",
			replacement.id).Scan(&replacementWork, &reviewStatus)
		if errors.Is(err, errD1NoRows) {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		if replacementWork != input.WorkID || reviewStatus != "approved" {
			return nil, ErrReferenceConflict
		}
	}
	if _, err := s.conn.Exec(ctx, `
		INSERT INTO reference_overlay_rules
			(overlay_rule_id, work_id, edition_row_id, target_logical_fact_id, target_entity_id,
			 target_node_id, overlay_action, replacement_claim_id, replacement_entity_id,
			 replacement_node_id, rule_status, reason_text, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, `+d1NowExpression+`)
		ON CONFLICT DO UPDATE SET
			rule_status = 'active',
			reason_text = excluded.reason_text,
			revision = reference_overlay_rules.revision + 1,
			updated_at = `+d1NowExpression,
		rule.OverlayRuleID, rule.WorkID, rule.EditionRowID, canonNullable(rule.TargetLogicalFactID),
		canonNullable(rule.TargetEntityID), canonNullable(rule.TargetNodeID), rule.Action,
		canonNullable(rule.ReplacementClaimID), canonNullable(rule.ReplacementEntityID),
		canonNullable(rule.ReplacementNodeID), canonNullable(rule.Reason)); err != nil {
		return nil, d1ReferenceStoreError(err)
	}
	// The rule is read back through the same list the caller will use, so a
	// stored created_at/updated_at and any normalisation the schema applied are
	// reported instead of guessed.
	items, err := s.ListCanonOverlays(ctx, input.WorkID, input.EditionRowID)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].OverlayRuleID == rule.OverlayRuleID {
			return &items[i], nil
		}
	}
	return nil, ErrNotFound
}

// ListCanonOverlays returns the local overlay policy of one work, optionally
// narrowed to one edition.
func (s *d1Store) ListCanonOverlays(ctx context.Context, workID, editionRowID string) ([]CanonOverlayRule, error) {
	query := `SELECT overlay_rule_id, work_id, edition_row_id, target_logical_fact_id,
		target_entity_id, target_node_id, overlay_action, replacement_claim_id,
		replacement_entity_id, replacement_node_id, rule_status, reason_text, created_at, updated_at
		FROM reference_overlay_rules WHERE work_id = ?`
	args := []any{strings.TrimSpace(workID)}
	if strings.TrimSpace(editionRowID) != "" {
		query += " AND edition_row_id = ?"
		args = append(args, strings.TrimSpace(editionRowID))
	}
	query += " ORDER BY updated_at DESC, overlay_rule_id"
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []CanonOverlayRule{}
	for rows.Next() {
		var item CanonOverlayRule
		var targetFact, targetEntity, targetNode, replacementClaim, replacementEntity, replacementNode, reason *string
		if err := rows.Scan(&item.OverlayRuleID, &item.WorkID, &item.EditionRowID, &targetFact,
			&targetEntity, &targetNode, &item.Action, &replacementClaim, &replacementEntity,
			&replacementNode, &item.Status, &reason, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.TargetLogicalFactID, item.TargetEntityID, item.TargetNodeID = d1DerefString(targetFact), d1DerefString(targetEntity), d1DerefString(targetNode)
		item.ReplacementClaimID, item.ReplacementEntityID, item.ReplacementNodeID = d1DerefString(replacementClaim), d1DerefString(replacementEntity), d1DerefString(replacementNode)
		item.Reason = d1DerefString(reason)
		items = append(items, item)
	}
	return items, rows.Err()
}

// ---------------------------------------------------------------------------
// CanonPackStore — statements
// ---------------------------------------------------------------------------

// d1CanonWorkInsert creates the work root of a pack install. It is the MariaDB
// INSERT IGNORE: the work id is a deterministic hash of the stable work id, so
// a second install of the same pack resolves the existing work instead of
// failing on the primary key.
const d1CanonWorkInsert = `
	INSERT INTO reference_works
		(work_id, title, work_type, default_language, status, metadata_json)
	VALUES (?, ?, 'canon', ?, 'active', ?)
	ON CONFLICT DO NOTHING`

// d1CanonEditionInsert creates the edition row. Unlike the work insert this one
// is NOT silently suppressed: a concurrent install of the same edition must
// surface as a conflict rather than two rows claiming the same (stable_work_id,
// edition_id) pair with different owners.
const d1CanonEditionInsert = `
	INSERT INTO reference_work_editions
		(edition_row_id, work_id, stable_work_id, edition_id, original_language,
		 edition_language, edition_label, edition_status, metadata_json)
	VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)`

const d1CanonEditionLookup = `
	SELECT edition_row_id, work_id FROM reference_work_editions
	WHERE stable_work_id = ? AND edition_id = ?`

const d1CanonContinuityInsert = `
	INSERT INTO reference_continuities
		(continuity_id, work_id, continuity_key, label, status, metadata_json)
	VALUES (?, ?, ?, ?, 'active', ?)
	ON CONFLICT DO NOTHING`

// d1CanonDemoteActivePack retires the currently active install of this pack for
// this edition. It must precede the install insert: the schema carries a
// generated active_generation_marker with a unique (pack_id,
// edition_row_id, active_generation_marker) index, so a second active install
// would be rejected outright if the demote ran afterwards.
const d1CanonDemoteActivePack = `
	UPDATE canon_pack_installs SET lifecycle_status = 'inactive'
	WHERE pack_id = ? AND edition_row_id = ? AND lifecycle_status = 'active'`

// d1CanonInstallInsert registers the pack. The generation is the reference's
// own COALESCE(MAX(install_generation), 0) + 1 evaluated INSIDE the batch, so a
// second Container that installs concurrently is stopped by the unique index
// rather than being handed a generation Go predicted before the demote ran.
//
// installed_at, activated_at and updated_at all default from the schema when
// omitted; only activated_at is named here, because the reference sets it
// explicitly to the install instant.
const d1CanonInstallInsert = `
	INSERT INTO canon_pack_installs
		(install_id, pack_id, pack_version, install_generation, manifest_contract,
		 manifest_sha256, manifest_json, work_id, edition_row_id, pack_status,
		 review_status, trust_status, lifecycle_status, validation_report_json,
		 coverage_report_json, activated_at)
	VALUES (?, ?, ?,
	    (SELECT COALESCE(MAX(canon_pack_installs.install_generation), 0) + 1
	     FROM canon_pack_installs
	     WHERE canon_pack_installs.pack_id = ? AND canon_pack_installs.pack_version = ?),
	    'canon-pack-manifest.v1', ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, ` + d1NowExpression + `)`

const d1CanonTitleInsert = `
	INSERT INTO reference_work_titles
		(title_row_id, work_id, edition_row_id, title_kind, title_text, language_code,
		 normalization_contract, normalized_lookup_key, normalized_lookup_digest, edition_scope_key)
	VALUES (?, ?, ?, ?, ?, ?, 'canon_title_normalization.v1', ?, ?, ?)
	ON CONFLICT DO NOTHING`

// d1CanonDocumentInsert stores a metadata-only document for one source. The
// reference follows this with a SELECT that resolves whichever document_id
// ended up owning the (work, continuity, content hash) triple; the resolution
// is pre-read here instead, so this statement only runs when the triple is new.
const d1CanonDocumentInsert = `
	INSERT INTO reference_documents
		(document_id, work_id, continuity_id, source_type, source_uri, content_hash,
		 raw_retention, raw_text, import_status, provenance_json)
	VALUES (?, ?, ?, ?, ?, ?, 'none', NULL, 'metadata_only', ?)
	ON CONFLICT DO NOTHING`

const d1CanonObservationInsert = `
	INSERT INTO reference_source_observations
		(observation_id, work_id, edition_row_id, continuity_id, origin_kind,
		 install_id, source_key, source_type, source_uri, license_json, access_class,
		 retrieved_at, document_sha256, document_id, provenance_json)
	VALUES (?, ?, ?, ?, 'canon_pack', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

const d1CanonEntityInsert = `
	INSERT INTO reference_entities
		(entity_id, work_id, continuity_id, entity_type, canonical_name,
		 description_text, metadata_json, review_status, review_source,
		 review_reason, reviewed_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'canon_pack', ?, ` + d1NowExpression + `)`

const d1CanonAliasInsert = `
	INSERT INTO reference_entity_aliases
		(work_id, continuity_id, entity_id, alias_text, normalized_alias, language_code)
	VALUES (?, ?, ?, ?, ?, ?)
	ON CONFLICT DO NOTHING`

const d1CanonNodeInsert = `
	INSERT INTO reference_timeline_nodes
		(node_id, work_id, continuity_id, node_key, label, ordinal_value,
		 branch_key, node_kind, metadata_json, review_status, review_source,
		 review_reason, reviewed_at)
	VALUES (?, ?, ?, ?, ?, ?, 'main', 'event', ?, ?, 'canon_pack', ?, ` + d1NowExpression + `)`

const d1CanonOriginInsert = `
	INSERT INTO reference_item_origins
		(origin_membership_id, work_id, edition_row_id, item_kind, node_id,
		 entity_id, claim_id, origin_kind, origin_owner_id, install_id,
		 source_item_id, review_state)
	VALUES (?, ?, ?, ?, ?, ?, ?, 'canon_pack', ?, ?, ?, ?)`

const d1CanonEvidenceInsert = `
	INSERT INTO reference_item_evidence
		(evidence_edge_id, item_kind, node_id, entity_id, claim_id,
		 source_observation_id, document_hash_contract, document_sha256,
		 locator_json, locator_digest, evidence_state)
	VALUES (?, ?, ?, ?, ?, ?, 'source_bytes_sha256.v1', ?, ?, ?, 'active')`

const d1CanonClaimInsert = `
	INSERT INTO reference_claims
		(claim_id, work_id, continuity_id, document_id, claim_type,
		 subject_entity_id, claim_text, temporal_scope, branch_key,
		 knowledge_scope, confidence, review_status, review_source,
		 review_reason, reviewed_at, metadata_json)
	VALUES (?, ?, ?, ?, ?, ?, ?, 'bounded', 'main', 'public_world', 1,
		 ?, 'canon_pack', ?, ` + d1NowExpression + `, ?)`

const d1CanonClaimApprove = `
	UPDATE reference_claims SET review_status = 'approved', review_source = 'canon_pack',
		review_reason = ?, reviewed_at = ` + d1NowExpression + `, updated_at = ` + d1NowExpression + `
	WHERE claim_id = ? AND review_status <> 'approved'`

const d1CanonLogicalFactInsert = `
	INSERT INTO reference_logical_facts
		(logical_fact_id, work_id, edition_row_id, continuity_id, applicability_scope_digest, fact_status)
	VALUES (?, ?, ?, ?, ?, 'active')`

const d1CanonFactIdentityInsert = `
	INSERT INTO reference_fact_identities
		(claim_id, fingerprint_contract, exact_fingerprint, logical_fact_id,
		 equivalence_status, equivalence_basis, edition_row_id, continuity_id,
		 applicability_scope_digest)
	VALUES (?, 'canon_fact_exact.v1', ?, ?, 'exact', 'normalized_exact', ?, ?, ?)`

const d1CanonFactIdentityLookup = `
	SELECT continuity_id, exact_fingerprint, claim_id, logical_fact_id
	FROM reference_fact_identities
	WHERE fingerprint_contract = 'canon_fact_exact.v1' AND edition_row_id = ?
	  AND exact_fingerprint IN (` + d1CanonPlaceholderList + `)`

// d1CanonPlaceholderList is the token the fact-identity lookup splices its
// placeholder run into. Building the run from an explicit count is required: a
// trailing comma in an IN list is a parse error in SQLite, not an empty set.
const d1CanonPlaceholderList = "{{PLACEHOLDERS}}"

// d1CanonLookupChunk bounds one IN list. A pack can name far more documents and
// facts than one statement may bind, and a trailing comma in an IN list is a
// parse error rather than an empty set, so the placeholder run is always built
// from an explicit count. A pack that exceeded the cap would fail the whole
// install for a reason that has nothing to do with the pack.
//
// The bound is d1BoundParameterCeiling rather than a number chosen here: the
// earlier value of 200 was sized against SQLite, which the local harness runs
// and which allows far more bindings than D1 does, so it cleared the local suite
// and still failed in deployment. A pack install is the worst place to discover
// that.
const d1CanonLookupChunk = d1BoundParameterCeiling

// ---------------------------------------------------------------------------
// CanonPackStore — install
// ---------------------------------------------------------------------------

// d1CanonSourceRef is one resolved source observation and the document it binds
// to. The reference threads the same shape through the content pass so an
// evidence edge can name both without re-reading.
type d1CanonSourceRef struct {
	ObservationID string
	DocumentID    string
	Hash          string
}

// d1CanonFactIdentity is one pre-read exact fact.
type d1CanonFactIdentity struct {
	claimID       string
	logicalFactID string
}

// d1CanonDocumentKey identifies the (continuity, content hash) triple that owns
// a reference document.
type d1CanonDocumentKey struct {
	continuityID string
	contentHash  string
}

func d1CanonDocumentKeyString(key d1CanonDocumentKey) string {
	return key.continuityID + "\x00" + key.contentHash
}

// InstallCanonPack registers one uploaded Canon Pack and every row it projects.
//
// The whole install is ONE D1Conn.Batch. The reference wraps it in a
// transaction for the same reason: a pack whose titles, sources and entities are
// stored but whose registry entry is not would appear in SearchCanonRegistry as
// nothing and could never be downloaded again, and a registry entry without its
// content would report an empty pack as installed. Splitting the write would
// make that state reachable.
//
// The three lookups that decided statement order in the reference (the edition
// pair, the document identity, the exact fact identity) run first, under
// d1CanonWriteMu, and only decide WHICH statements are emitted. Every value
// they return is either deterministic from the manifest or already present, so
// nothing is copied out of the batch and into Go.
func (s *d1Store) InstallCanonPack(ctx context.Context, input CanonPackInstallInput) (*CanonPackInstall, error) {
	var manifest map[string]any
	if len(input.ManifestJSON) == 0 || json.Unmarshal(input.ManifestJSON, &manifest) != nil {
		return nil, ErrInvalidReference
	}
	pack, work := canonObject(manifest["pack"]), canonObject(manifest["work"])
	edition := canonObject(work["edition"])
	review, trust := canonObject(manifest["review"]), canonObject(manifest["trust"])
	packID, packVersion := canonString(pack["id"]), canonString(pack["version"])
	stableWorkID, editionID := canonString(work["stable_id"]), canonString(edition["edition_id"])
	title := canonString(canonObject(work["original_title"])["text"])
	// The two SHA-256 checks are the same checks, and they matter: the manifest
	// digest and the archive digest are what make a re-uploaded pack the SAME
	// pack. Without them an install is only identified by its install_id, which
	// is random, and a byte-identical pack would install as a new generation
	// that the registry then treats as a different work.
	if canonString(manifest["contract"]) != "canon-pack-manifest.v1" ||
		packID == "" || packVersion == "" || stableWorkID == "" || editionID == "" || title == "" ||
		canonString(review["status"]) != "approved" || !canonSHA256(input.ManifestSHA256) ||
		!canonSHA256(input.ArchiveSHA256) {
		return nil, ErrInvalidReference
	}
	installID, err := canonRandomID()
	if err != nil {
		return nil, err
	}
	continuityKeys := canonStrings(work["continuity_ids"])
	if len(continuityKeys) == 0 {
		return nil, ErrInvalidReference
	}
	// The source timestamps are checked here rather than inside the transaction
	// the reference uses. The observable result is identical — the domain error,
	// with nothing stored — and no transaction is spent discovering it.
	if err := d1CanonValidateSources(manifest); err != nil {
		return nil, err
	}

	d1CanonWriteMu.Lock()
	defer d1CanonWriteMu.Unlock()

	workID, editionRowID, editionStatements, err := s.d1CanonResolveWorkEdition(ctx, stableWorkID, editionID, title, work, edition)
	if err != nil {
		return nil, d1ReferenceStoreError(err)
	}

	statements := make([]D1Statement, 0, 64)
	statements = append(statements, editionStatements...)

	continuityRows := make(map[string]string, len(continuityKeys))
	for _, continuityKey := range continuityKeys {
		continuityID := canonStableID("canon-continuity", stableWorkID, continuityKey)
		continuityRows[continuityKey] = continuityID
		statements = append(statements, D1Statement{
			SQL: d1CanonContinuityInsert,
			Args: []any{continuityID, workID, continuityKey, continuityKey, canonJSON(map[string]any{
				"stable_continuity_id": continuityKey, "origin": "canon_pack",
			})},
		})
	}

	validationJSON := canonValidationJSON(input.ValidationReportJSON, input.ArchiveSHA256)
	coverageJSON := canonJSON(canonObject(manifest["coverage_report"]))
	statements = append(statements,
		D1Statement{SQL: d1CanonDemoteActivePack, Args: []any{packID, editionRowID}},
		D1Statement{SQL: d1CanonInstallInsert, Args: []any{
			installID, packID, packVersion, packID, packVersion,
			input.ManifestSHA256, string(input.ManifestJSON), workID, editionRowID,
			canonString(pack["status"]), canonString(review["status"]), canonString(trust["status"]),
			validationJSON, coverageJSON,
		}},
	)
	statements = append(statements, d1CanonTitleStatements(workID, editionRowID, work)...)

	// The content pass needs two resolutions before it can emit a single
	// statement list, because both its inserts depend on rows a PREVIOUS pack
	// install may already have created.
	factIdentities, err := s.d1CanonReadFactIdentities(ctx, editionRowID, manifest, continuityRows)
	if err != nil {
		return nil, err
	}
	documents, err := s.d1CanonReadDocuments(ctx, workID, manifest, continuityRows)
	if err != nil {
		return nil, err
	}

	sources, sourceStatements, err := d1CanonSourceStatements(installID, workID, editionRowID, manifest, continuityRows, documents)
	if err != nil {
		return nil, err
	}
	statements = append(statements, sourceStatements...)

	contentStatements, err := d1CanonContentStatements(installID, packID, packVersion, workID, editionRowID, manifest, continuityRows, sources, factIdentities)
	if err != nil {
		return nil, err
	}
	statements = append(statements, contentStatements...)

	if err := s.conn.Batch(ctx, statements...); err != nil {
		return nil, d1ReferenceStoreError(err)
	}
	return s.GetCanonPackInstall(ctx, installID)
}

// d1CanonResolveWorkEdition returns the work and edition row ids this install
// belongs to, plus the statements that create them when they do not exist yet.
//
// The lookup is the reference's SELECT ... FOR UPDATE moved in front of the
// batch. Nothing is lost by that: both row ids are deterministic hashes of the
// manifest's stable ids, so the batch inserts exactly the rows the locked read
// would have found missing.
func (s *d1Store) d1CanonResolveWorkEdition(
	ctx context.Context, stableWorkID, editionID, title string,
	work, edition map[string]any,
) (string, string, []D1Statement, error) {
	var editionRowID, workID string
	err := s.conn.QueryRow(ctx, d1CanonEditionLookup, stableWorkID, editionID).Scan(&editionRowID, &workID)
	if err == nil {
		return workID, editionRowID, nil, nil
	}
	if !errors.Is(err, errD1NoRows) {
		return "", "", nil, err
	}
	workID = canonStableID("canon-work", stableWorkID)
	editionRowID = canonStableID("canon-edition", stableWorkID, editionID)
	metadata := canonJSON(map[string]any{"stable_work_id": stableWorkID, "origin": "canon_pack"})
	return workID, editionRowID, []D1Statement{
		{SQL: d1CanonWorkInsert, Args: []any{workID, title, canonString(work["original_language"]), metadata}},
		{SQL: d1CanonEditionInsert, Args: []any{editionRowID, workID, stableWorkID, editionID,
			canonString(work["original_language"]), canonString(edition["language"]),
			canonString(edition["label"]), canonJSON(edition)}},
	}, nil
}

// d1CanonTitleStatements builds the original, translated and alias title rows.
//
// The normalised key and its digest are produced by the shared canonNormalize
// and canonHash helpers rather than by anything local. They are load-bearing:
// the registry search matches on normalized_lookup_key and the schema constrains
// the digest to 64 lowercase hex characters, and a different normalisation rule
// on this provider would make titles written by MariaDB unsearchable here.
func d1CanonTitleStatements(workID, editionRowID string, work map[string]any) []D1Statement {
	type titleEntry struct{ Kind, Text, Language string }
	titles := []titleEntry{}
	original := canonObject(work["original_title"])
	titles = append(titles, titleEntry{"original", canonString(original["text"]), canonString(original["language"])})
	for _, raw := range canonArray(work["translated_titles"]) {
		v := canonObject(raw)
		titles = append(titles, titleEntry{"translated", canonString(v["text"]), canonString(v["language"])})
	}
	for _, raw := range canonArray(work["aliases"]) {
		v := canonObject(raw)
		titles = append(titles, titleEntry{"alias", canonString(v["text"]), canonString(v["language"])})
	}
	statements := make([]D1Statement, 0, len(titles))
	for _, item := range titles {
		lookup := canonNormalize(item.Text)
		if lookup == "" {
			continue
		}
		digest := canonHash(lookup)
		statements = append(statements, D1Statement{
			SQL: d1CanonTitleInsert,
			Args: []any{canonStableID("canon-title", workID, editionRowID, item.Kind, item.Language, digest),
				workID, editionRowID, item.Kind, item.Text, item.Language, lookup, digest, editionRowID},
		})
	}
	return statements
}

// d1CanonValidateSources refuses a manifest whose source cannot be attributed to
// an instant.
//
// The reference parses retrieved_at inside its transaction, so a bad timestamp
// rolls the whole install back. Here it is checked before the batch is opened:
// the observable result is the same error with nothing stored, and no
// transaction is spent discovering it. Validating here is also what keeps a bad
// timestamp from reaching the statement builder, where a source list that failed
// to parse would otherwise look like an empty source list.
func d1CanonValidateSources(manifest map[string]any) error {
	for _, raw := range canonArray(manifest["sources"]) {
		source := canonObject(raw)
		if _, err := time.Parse(time.RFC3339, canonString(source["retrieved_at"])); err != nil {
			return ErrInvalidReference
		}
	}
	return nil
}

// d1CanonSourcePair is one (continuity, manifest source) pair of an install.
type d1CanonSourcePair struct {
	continuityKey string
	continuityID  string
	sourceID      string
	documentHash  string
}

// d1CanonContinuityOrder returns the install's continuity keys in a stable
// order.
//
// The reference iterates its continuity map directly, so its statement order is
// whatever Go's map iteration produces; the resulting rows are the same either
// way, because nothing in the source pass depends on the order. Sorting makes
// this provider's batch deterministic, which is what lets a test assert a
// statement count.
func d1CanonContinuityOrder(continuityRows map[string]string) []string {
	keys := make([]string, 0, len(continuityRows))
	for key := range continuityRows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// d1CanonManifestSources returns every (continuity, source) pair the install
// projects.
//
// Every source is bound to EVERY continuity of the edition, and that is the
// reference's rule, not an accident of its loop: a source document is evidence
// for the whole edition, and an evidence edge in a continuity the source was
// not repeated into would leave that continuity unable to ground anything. A
// per-source continuity list would silently drop those edges.
func d1CanonManifestSources(manifest map[string]any, continuityRows map[string]string) []d1CanonSourcePair {
	var pairs []d1CanonSourcePair
	for _, raw := range canonArray(manifest["sources"]) {
		source := canonObject(raw)
		sourceID, documentHash := canonString(source["id"]), canonString(source["document_sha256"])
		for _, continuityKey := range d1CanonContinuityOrder(continuityRows) {
			pairs = append(pairs, d1CanonSourcePair{
				continuityKey: continuityKey,
				continuityID:  continuityRows[continuityKey],
				sourceID:      sourceID,
				documentHash:  documentHash,
			})
		}
	}
	return pairs
}

// d1CanonReadDocuments resolves the document_id every (continuity, content hash)
// triple already owns, across this install AND earlier ones.
//
// The reference resolves it by inserting and then re-selecting, which is safe
// inside one transaction and impossible inside a batch. Pre-reading is
// equivalent because the answer is a pure function of the triple: either a row
// exists, and the pre-read returns the id that row owns, or it does not, and the
// deterministic hash is the id the row is about to get. Entries this install
// allocates are written into the returned map as the plan is built, so two
// sources sharing a content hash resolve to the same document exactly as they
// would have inside the transaction.
func (s *d1Store) d1CanonReadDocuments(
	ctx context.Context, workID string, manifest map[string]any, continuityRows map[string]string,
) (map[string]string, error) {
	documents := map[string]string{}
	pairs := d1CanonManifestSources(manifest, continuityRows)
	if len(pairs) == 0 {
		return documents, nil
	}
	var hashes []string
	seen := map[string]bool{}
	for _, pair := range pairs {
		if pair.documentHash == "" || seen[pair.documentHash] {
			continue
		}
		seen[pair.documentHash] = true
		hashes = append(hashes, pair.documentHash)
	}
	for start := 0; start < len(hashes); start += d1CanonLookupChunk {
		end := start + d1CanonLookupChunk
		if end > len(hashes) {
			end = len(hashes)
		}
		args := make([]any, 0, len(hashes[start:end])+1)
		args = append(args, workID)
		for _, hash := range hashes[start:end] {
			args = append(args, hash)
		}
		rows, err := s.conn.Query(ctx, `
			SELECT continuity_id, content_hash, document_id
			FROM reference_documents
			WHERE work_id = ? AND content_hash IN (`+
			strings.TrimSuffix(strings.Repeat("?,", end-start), ",")+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var continuityID, contentHash, documentID string
			if err := rows.Scan(&continuityID, &contentHash, &documentID); err != nil {
				rows.Close()
				return nil, err
			}
			documents[d1CanonDocumentKeyString(d1CanonDocumentKey{continuityID, contentHash})] = documentID
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return documents, nil
}

// d1CanonReadFactIdentities pre-resolves the exact fact identities the content
// pass may reuse. Keyed by continuity and exact fingerprint, because that pair
// is what canon_fact_exact.v1 defines as the same fact.
//
// Every claim and relation record in the manifest contributes its fingerprint
// before any statement is emitted, so the content builder never has to ask
// whether a fact already exists.
func (s *d1Store) d1CanonReadFactIdentities(
	ctx context.Context, editionRowID string, manifest map[string]any, continuityRows map[string]string,
) (map[string]d1CanonFactIdentity, error) {
	identities := map[string]d1CanonFactIdentity{}
	var fingerprints []string
	seen := map[string]bool{}
	for _, pair := range d1CanonClaimFingerprints(editionRowID, manifest, continuityRows) {
		if seen[pair.fingerprint] {
			continue
		}
		seen[pair.fingerprint] = true
		fingerprints = append(fingerprints, pair.fingerprint)
	}
	if len(fingerprints) == 0 {
		return identities, nil
	}
	for start := 0; start < len(fingerprints); start += d1CanonLookupChunk {
		end := start + d1CanonLookupChunk
		if end > len(fingerprints) {
			end = len(fingerprints)
		}
		args := make([]any, 0, len(fingerprints[start:end])+1)
		args = append(args, editionRowID)
		for _, fingerprint := range fingerprints[start:end] {
			args = append(args, fingerprint)
		}
		rows, err := s.conn.Query(ctx, strings.ReplaceAll(d1CanonFactIdentityLookup, d1CanonPlaceholderList,
			strings.TrimSuffix(strings.Repeat("?,", end-start), ",")), args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var continuityID, fingerprint, claimID, logicalFactID string
			if err := rows.Scan(&continuityID, &fingerprint, &claimID, &logicalFactID); err != nil {
				rows.Close()
				return nil, err
			}
			identities[continuityID+"\x00"+fingerprint] = d1CanonFactIdentity{claimID: claimID, logicalFactID: logicalFactID}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return identities, nil
}

// d1CanonFingerprint pairs a continuity with the exact fingerprint a claim or
// relation record produces inside it.
type d1CanonFingerprint struct {
	continuityID string
	fingerprint  string
}

// d1CanonClaimFingerprints enumerates every exact fingerprint this install's
// claim and relation records produce, one per (record, continuity).
//
// It has to be per continuity because canon_fact_exact.v1 folds the continuity
// into the digest. A fingerprint computed without one would never match a stored
// row, the pre-read would always come back empty, and every re-installed claim
// would be written as a NEW fact — which is exactly the duplicate-and-conflict
// count the install diagnostics report.
//
// A relation is the harder half. Its rendered statement names the DISPLAY names
// the entity and event passes collected, not the record's own subject and object
// ids, so the name map has to be rebuilt here with the same walk order the
// content pass uses. Computing the fingerprint from the raw ids would produce a
// digest that matches nothing ever, and a re-installed pack would duplicate
// every relation.
func d1CanonClaimFingerprints(editionRowID string, manifest map[string]any, continuityRows map[string]string) []d1CanonFingerprint {
	content := canonObject(manifest["content"])
	entityNames := d1CanonDisplayNames(content)
	var out []d1CanonFingerprint
	for _, kind := range []string{"relations", "claims"} {
		for _, raw := range canonArray(content[kind]) {
			record := canonObject(raw)
			if canonString(record["review_state"]) == "rejected" {
				continue
			}
			claimType, statement := canonString(record["claim_type"]), canonString(record["statement"])
			if kind == "relations" {
				claimType = "relation:" + canonString(record["predicate"])
				statement = fmt.Sprintf("%s %s %s", canonDisplayName(entityNames, canonString(record["subject_id"])),
					canonString(record["predicate"]), canonDisplayName(entityNames, canonString(record["object_id"])))
			}
			for _, continuityKey := range canonStrings(record["continuity_ids"]) {
				continuityID := continuityRows[continuityKey]
				if continuityID == "" {
					continue
				}
				_, fingerprint := canonFactIdentity(editionRowID, continuityID, claimType, statement)
				out = append(out, d1CanonFingerprint{continuityID: continuityID, fingerprint: fingerprint})
			}
		}
	}
	return out
}

// d1CanonDisplayNames collects the local-id to display-name map the relation
// pass renders statements from.
//
// It is keyed by local id alone and last write wins, exactly as the content
// pass builds it, so the two passes cannot disagree about what a relation reads
// like. An id the map does not know falls back to the id itself through
// canonDisplayName, which is the reference's own fallback.
func d1CanonDisplayNames(content map[string]any) map[string]string {
	entityNames := map[string]string{}
	for _, kind := range []string{"entities", "locations", "factions", "settings"} {
		for _, raw := range canonArray(content[kind]) {
			record := canonObject(raw)
			if canonString(record["review_state"]) == "rejected" {
				continue
			}
			for _, continuityKey := range canonStrings(record["continuity_ids"]) {
				if continuityKey == "" {
					continue
				}
				entityNames[canonString(record["id"])] = canonString(canonObject(record["name"])["text"])
			}
		}
	}
	for _, raw := range canonArray(content["events"]) {
		record := canonObject(raw)
		if canonString(record["review_state"]) == "rejected" {
			continue
		}
		for _, continuityKey := range canonStrings(record["continuity_ids"]) {
			if continuityKey == "" {
				continue
			}
			entityNames[canonString(record["id"])] = canonString(canonObject(record["name"])["text"])
		}
	}
	return entityNames
}

// d1CanonSourceStatements builds the document and source-observation rows, and
// returns the source index the content pass resolves evidence edges against.
func d1CanonSourceStatements(
	installID, workID, editionRowID string, manifest map[string]any,
	continuityRows map[string]string, documents map[string]string,
) (map[string]d1CanonSourceRef, []D1Statement, error) {
	result := map[string]d1CanonSourceRef{}
	sources := canonObject(map[string]any{})[""]
	_ = sources
	byID := map[string]map[string]any{}
	for _, raw := range canonArray(manifest["sources"]) {
		source := canonObject(raw)
		byID[canonString(source["id"])] = source
	}
	pairs := d1CanonManifestSources(manifest, continuityRows)
	statements := make([]D1Statement, 0, len(pairs)*2)
	for _, pair := range pairs {
		source := byID[pair.sourceID]
		if source == nil {
			return nil, nil, ErrInvalidReference
		}
		// d1CanonValidateSources already parsed this; the error is unreachable and
		// returning the domain error rather than a parse string keeps the caller
		// from ever seeing a different error value for the same manifest.
		retrievedAt, err := time.Parse(time.RFC3339, canonString(source["retrieved_at"]))
		if err != nil {
			return nil, nil, ErrInvalidReference
		}
		documentKey := d1CanonDocumentKey{pair.continuityID, pair.documentHash}
		documentID := documents[d1CanonDocumentKeyString(documentKey)]
		provenance := canonJSON(map[string]any{
			"origin": "canon_pack", "source_id": pair.sourceID, "title": canonString(source["title"]),
		})
		if documentID == "" {
			documentID = canonStableID("canon-document", workID, pair.continuityID, pair.documentHash)
			documents[d1CanonDocumentKeyString(documentKey)] = documentID
			statements = append(statements, D1Statement{
				SQL: d1CanonDocumentInsert,
				Args: []any{documentID, workID, pair.continuityID, canonString(source["source_type"]),
					canonNullable(canonString(source["uri"])), pair.documentHash, provenance},
			})
		}
		observationID := canonStableID("canon-observation", installID, pair.continuityKey, pair.sourceID)
		sourceKey := pair.sourceID + ":" + canonHash(pair.continuityKey)[:16]
		statements = append(statements, D1Statement{
			SQL: d1CanonObservationInsert,
			Args: []any{observationID, workID, editionRowID, pair.continuityID, installID, sourceKey,
				canonString(source["source_type"]), canonNullable(canonString(source["uri"])),
				canonJSON(canonObject(source["license"])), canonString(source["access_class"]),
				d1TimeValue(retrievedAt), pair.documentHash, documentID, provenance},
		})
		result[pair.continuityKey+"\x00"+pair.sourceID] = d1CanonSourceRef{
			ObservationID: observationID, DocumentID: documentID, Hash: pair.documentHash,
		}
	}
	return result, statements, nil
}

// d1CanonContentStatements builds every entity, alias, node, claim, logical fact
// and evidence edge the manifest projects.
//
// The local maps are the same ones the reference keeps in Go while it walks the
// manifest, and they are what make the pass order-sensitive in the same way:
// a relation rendered as "A knows B" needs the display names of A and B, so the
// entity and event passes must already have run.
func d1CanonContentStatements(
	installID, packID, packVersion, workID, editionRowID string, manifest map[string]any,
	continuityRows map[string]string, sources map[string]d1CanonSourceRef,
	factIdentities map[string]d1CanonFactIdentity,
) ([]D1Statement, error) {
	content := canonObject(manifest["content"])
	statements := make([]D1Statement, 0, 64)
	entityIDs, entityNames := map[string]string{}, map[string]string{}
	// claimedFacts is the map the reference gets for free from its transaction:
	// the locked SELECT it repeats for every claim also sees the row the
	// previous record in the SAME pack just inserted. Here the plan records the
	// fingerprints it has already allocated so a duplicate record in one manifest
	// takes the already-exists branch instead of colliding with its own batch.
	claimedFacts := map[string]d1CanonFactIdentity{}

	for _, kind := range []string{"entities", "locations", "factions", "settings"} {
		for _, raw := range canonArray(content[kind]) {
			record := canonObject(raw)
			if canonString(record["review_state"]) == "rejected" {
				continue
			}
			for _, continuityKey := range canonStrings(record["continuity_ids"]) {
				continuityID := continuityRows[continuityKey]
				if continuityID == "" {
					return nil, ErrInvalidReference
				}
				localID := canonString(record["id"])
				entityID := canonStableID("canon-item", installID, continuityKey, kind, localID)
				name := canonString(canonObject(record["name"])["text"])
				entityType := strings.TrimSuffix(kind, "s")
				if kind == "entities" {
					entityType = canonString(record["kind"])
				}
				statements = append(statements, D1Statement{
					SQL: d1CanonEntityInsert,
					Args: []any{entityID, workID, continuityID, entityType, name,
						canonNullable(canonString(record["summary"])), canonJSON(record),
						canonReviewState(record), "installed from " + packID + "@" + packVersion},
				})
				originStatements, err := d1CanonOriginStatements(installID, workID, editionRowID, "entity", entityID, localID, canonReviewState(record))
				if err != nil {
					return nil, err
				}
				statements = append(statements, originStatements...)
				evidenceStatements, err := d1CanonEvidenceStatements("entity", entityID, continuityKey, record, sources)
				if err != nil {
					return nil, err
				}
				statements = append(statements, evidenceStatements...)
				for _, aliasRaw := range canonArray(record["aliases"]) {
					alias := canonObject(aliasRaw)
					text := canonString(alias["text"])
					if text == "" {
						continue
					}
					statements = append(statements, D1Statement{
						SQL:  d1CanonAliasInsert,
						Args: []any{workID, continuityID, entityID, text, canonNormalize(text), canonString(alias["language"])},
					})
				}
				entityIDs[continuityKey+"\x00"+localID] = entityID
				entityNames[localID] = name
			}
		}
	}

	for index, raw := range canonArray(content["events"]) {
		record := canonObject(raw)
		if canonString(record["review_state"]) == "rejected" {
			continue
		}
		for _, continuityKey := range canonStrings(record["continuity_ids"]) {
			continuityID, localID := continuityRows[continuityKey], canonString(record["id"])
			if continuityID == "" {
				return nil, ErrInvalidReference
			}
			nodeID := canonStableID("canon-item", installID, continuityKey, "events", localID)
			name := canonString(canonObject(record["name"])["text"])
			statements = append(statements, D1Statement{
				SQL: d1CanonNodeInsert,
				Args: []any{nodeID, workID, continuityID, installID + ":" + localID, name, int64((index + 1) * 10),
					canonJSON(record), canonReviewState(record), "installed from " + packID + "@" + packVersion},
			})
			originStatements, err := d1CanonOriginStatements(installID, workID, editionRowID, "timeline", nodeID, localID, canonReviewState(record))
			if err != nil {
				return nil, err
			}
			statements = append(statements, originStatements...)
			evidenceStatements, err := d1CanonEvidenceStatements("timeline", nodeID, continuityKey, record, sources)
			if err != nil {
				return nil, err
			}
			statements = append(statements, evidenceStatements...)
			entityNames[localID] = name
		}
	}

	for _, kind := range []string{"relations", "claims"} {
		for _, raw := range canonArray(content[kind]) {
			record := canonObject(raw)
			if canonString(record["review_state"]) == "rejected" {
				continue
			}
			for _, continuityKey := range canonStrings(record["continuity_ids"]) {
				continuityID, localID := continuityRows[continuityKey], canonString(record["id"])
				if continuityID == "" {
					return nil, ErrInvalidReference
				}
				claimType, statement := canonString(record["claim_type"]), canonString(record["statement"])
				subjectIDs := canonStrings(record["subject_ids"])
				if kind == "relations" {
					claimType = "relation:" + canonString(record["predicate"])
					subject, object := canonString(record["subject_id"]), canonString(record["object_id"])
					statement = fmt.Sprintf("%s %s %s", canonDisplayName(entityNames, subject),
						canonString(record["predicate"]), canonDisplayName(entityNames, object))
					subjectIDs = []string{subject, object}
				}
				firstSource, err := d1CanonFirstEvidenceSource(continuityKey, record, sources)
				if err != nil {
					return nil, err
				}
				subjectEntityID := ""
				for _, subject := range subjectIDs {
					if id := entityIDs[continuityKey+"\x00"+subject]; id != "" {
						subjectEntityID = id
						break
					}
				}
				applicabilityDigest, exactFingerprint := canonFactIdentity(editionRowID, continuityID, claimType, statement)
				factKey := continuityID + "\x00" + exactFingerprint
				identity, exists := factIdentities[factKey]
				if !exists {
					identity, exists = claimedFacts[factKey]
				}
				if exists {
					// Corroboration, not a duplicate: the same exact fact seen in
					// this pack or in an earlier one keeps its original claim row
					// and only upgrades it to approved.
					if canonReviewState(record) == "approved" {
						statements = append(statements, D1Statement{
							SQL:  d1CanonClaimApprove,
							Args: []any{"corroborated by " + packID + "@" + packVersion, identity.claimID},
						})
					}
				} else {
					claimID := canonStableID("canon-item", installID, continuityID, kind, localID)
					logicalFactID := canonStableID("canon-logical-fact", editionRowID, continuityID, exactFingerprint)
					metadata := map[string]any{
						"manifest_record": record, "source_item_id": localID, "install_id": installID,
						"exact_fingerprint": exactFingerprint, "logical_fact_id": logicalFactID,
					}
					statements = append(statements,
						D1Statement{SQL: d1CanonClaimInsert, Args: []any{
							claimID, workID, continuityID, firstSource.DocumentID, claimType,
							canonNullable(subjectEntityID), statement, canonReviewState(record),
							"installed from " + packID + "@" + packVersion, canonJSON(metadata),
						}},
						D1Statement{SQL: d1CanonLogicalFactInsert, Args: []any{
							logicalFactID, workID, editionRowID, continuityID, applicabilityDigest,
						}},
						D1Statement{SQL: d1CanonFactIdentityInsert, Args: []any{
							claimID, exactFingerprint, logicalFactID, editionRowID, continuityID, applicabilityDigest,
						}},
					)
					// The freshly created claim must become the identity the
					// origin and evidence rows below are written against. Leaving
					// the zero value here would bind an empty claim_id, and the
					// origin would then point at a claim that does not exist.
					identity = d1CanonFactIdentity{claimID: claimID, logicalFactID: logicalFactID}
					claimedFacts[factKey] = identity
				}
				originStatements, err := d1CanonOriginStatements(installID, workID, editionRowID, "claim", identity.claimID, localID, canonReviewState(record))
				if err != nil {
					return nil, err
				}
				statements = append(statements, originStatements...)
				evidenceStatements, err := d1CanonEvidenceStatements("claim", identity.claimID, continuityKey, record, sources)
				if err != nil {
					return nil, err
				}
				statements = append(statements, evidenceStatements...)
			}
		}
	}
	return statements, nil
}

// d1CanonOriginStatements builds the origin membership row. Exactly one of the
// three target columns is non-NULL, and the schema CHECK enforces that, so the
// same three-way binding is kept rather than folded into a single column.
func d1CanonOriginStatements(installID, workID, editionRowID, kind, targetID, sourceItemID, reviewState string) ([]D1Statement, error) {
	var node, entity, claim any
	switch kind {
	case "timeline":
		node = targetID
	case "entity":
		entity = targetID
	case "claim":
		claim = targetID
	default:
		return nil, ErrInvalidReference
	}
	return []D1Statement{{
		SQL: d1CanonOriginInsert,
		Args: []any{canonStableID("canon-origin", installID, kind, sourceItemID, targetID), workID, editionRowID,
			kind, node, entity, claim, installID, installID, sourceItemID, reviewState},
	}}, nil
}

// d1CanonEvidenceStatements builds one record's evidence edges. The locator
// digest is what makes the edge identity stable, so re-uploading the same
// locator for the same observation is the same edge on both providers.
func d1CanonEvidenceStatements(kind, targetID, continuityKey string, record map[string]any, sources map[string]d1CanonSourceRef) ([]D1Statement, error) {
	var node, entity, claim any
	switch kind {
	case "timeline":
		node = targetID
	case "entity":
		entity = targetID
	case "claim":
		claim = targetID
	}
	statements := make([]D1Statement, 0, len(canonArray(record["evidence"])))
	for _, raw := range canonArray(record["evidence"]) {
		evidence := canonObject(raw)
		source := sources[continuityKey+"\x00"+canonString(evidence["source_id"])]
		if source.ObservationID == "" {
			return nil, ErrInvalidReference
		}
		locatorJSON := canonJSON(canonObject(evidence["locator"]))
		statements = append(statements, D1Statement{
			SQL: d1CanonEvidenceInsert,
			Args: []any{canonStableID("canon-evidence", targetID, source.ObservationID, canonHash(locatorJSON)),
				kind, node, entity, claim, source.ObservationID, source.Hash, locatorJSON, canonHash(locatorJSON)},
		})
	}
	return statements, nil
}

// d1CanonFirstEvidenceSource picks the observation a claim is attributed to.
//
// A claim with no evidence is refused rather than stored without provenance: an
// unevidenced canon fact is exactly what the traceability diagnostic counts, and
// admitting one would let a pack report items it cannot ground.
func d1CanonFirstEvidenceSource(continuityKey string, record map[string]any, sources map[string]d1CanonSourceRef) (d1CanonSourceRef, error) {
	evidence := canonArray(record["evidence"])
	if len(evidence) == 0 {
		return d1CanonSourceRef{}, ErrInvalidReference
	}
	source := sources[continuityKey+"\x00"+canonString(canonObject(evidence[0])["source_id"])]
	if source.DocumentID == "" {
		return d1CanonSourceRef{}, ErrInvalidReference
	}
	return source, nil
}

// ---------------------------------------------------------------------------
// CanonPackStore — reads and lifecycle
// ---------------------------------------------------------------------------

// canonPackSelectSQL is the shared install projection. It is dialect neutral
// and is reused verbatim: its three correlated origin counts are the
// RecordCounts the install detail renders, and they must be counted per install
// or a work with two installed packs would report the union of both.
func (s *d1Store) ListCanonPackInstalls(ctx context.Context, lifecycle string) ([]CanonPackInstall, error) {
	query := canonPackSelectSQL()
	args := []any{}
	if strings.TrimSpace(lifecycle) != "" {
		query += " WHERE i.lifecycle_status = ?"
		args = append(args, strings.TrimSpace(lifecycle))
	}
	query += " ORDER BY i.updated_at DESC, i.pack_id, i.install_generation DESC"
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []CanonPackInstall{}
	for rows.Next() {
		item, err := d1CanonScanPackInstall(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, *item)
	}
	return items, rows.Err()
}

// GetCanonPackInstall returns one install with its work, edition and counts.
func (s *d1Store) GetCanonPackInstall(ctx context.Context, installID string) (*CanonPackInstall, error) {
	item, err := d1CanonScanPackInstall(s.conn.QueryRow(ctx,
		canonPackSelectSQL()+" WHERE i.install_id = ?", strings.TrimSpace(installID)))
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	return item, err
}

// d1CanonScanPackInstall is the D1 form of scanCanonPackInstall.
//
// It exists rather than reusing the MariaDB scanner because that one stores
// ActivatedAt and RemovedAt in sql.NullTime and ValidationReportJSON in []byte,
// and the D1 transport has no destination for either: a nullable instant is a
// **time.Time and a nullable text is a **string. The mapping performed after the
// scan is the reference's, including the archive digest read back out of the
// validation report.
func d1CanonScanPackInstall(row canonScanner) (*CanonPackInstall, error) {
	item := &CanonPackInstall{Contract: CanonPackLifecycleContract, RecordCounts: map[string]int{}}
	var generation int64
	var validationRaw, coverageRaw *string
	var activated, removed *time.Time
	var entityCount, timelineCount, claimCount int
	if err := row.Scan(&item.InstallID, &item.PackID, &item.PackVersion, &generation,
		&item.WorkID, &item.EditionRowID, &item.StableWorkID, &item.EditionID, &item.Title,
		&item.PackStatus, &item.ReviewStatus, &item.TrustStatus, &item.LifecycleStatus,
		&item.ManifestSHA256, &validationRaw, &coverageRaw, &item.InstalledAt, &activated,
		&removed, &item.UpdatedAt, &entityCount, &timelineCount, &claimCount); err != nil {
		return nil, err
	}
	item.InstallGeneration = uint64(generation)
	item.ActivatedAt = activated
	item.RemovedAt = removed
	var validation map[string]any
	_ = json.Unmarshal([]byte(d1DerefString(validationRaw)), &validation)
	item.ArchiveSHA256 = canonString(validation["archive_sha256"])
	_ = json.Unmarshal([]byte(d1DerefString(coverageRaw)), &item.CoverageReport)
	item.RecordCounts["entities"] = entityCount
	item.RecordCounts["timeline"] = timelineCount
	item.RecordCounts["claims"] = claimCount
	return item, nil
}

// SetCanonPackLifecycle activates, deactivates, or removes an installed pack.
//
// The two refs state the whole guarantee: a removed or failed install can never
// be reactivated, and only one install of a pack may be active for an edition
// at a time. The demote and the activate therefore travel in ONE batch, demote
// first, which is also the order the generated active_generation_marker unique
// index requires.
func (s *d1Store) SetCanonPackLifecycle(ctx context.Context, installID, action string) (*CanonPackLifecycleResult, error) {
	action = strings.TrimSpace(action)
	if action != "activate" && action != "deactivate" && action != "remove" && action != "rollback" {
		return nil, ErrInvalidReference
	}
	installID = strings.TrimSpace(installID)

	d1CanonWriteMu.Lock()
	defer d1CanonWriteMu.Unlock()

	var packID, editionRowID, status string
	err := s.conn.QueryRow(ctx, `
		SELECT pack_id, edition_row_id, lifecycle_status FROM canon_pack_installs
		WHERE install_id = ?
	`, installID).Scan(&packID, &editionRowID, &status)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	result := &CanonPackLifecycleResult{Contract: CanonPackLifecycleContract, Action: action, InstallID: installID}
	var statements []D1Statement
	switch action {
	case "activate", "rollback":
		if status == "removed" || status == "failed" {
			return nil, ErrReferenceConflict
		}
		err = s.conn.QueryRow(ctx, `
			SELECT install_id FROM canon_pack_installs
			WHERE pack_id = ? AND edition_row_id = ? AND lifecycle_status = 'active' AND install_id <> ?
			LIMIT 1
		`, packID, editionRowID, installID).Scan(&result.PreviousInstallID)
		if err != nil && !errors.Is(err, errD1NoRows) {
			return nil, err
		}
		statements = []D1Statement{
			{SQL: `
				UPDATE canon_pack_installs SET lifecycle_status = 'inactive', updated_at = ` + d1NowExpression + `
				WHERE pack_id = ? AND edition_row_id = ? AND lifecycle_status = 'active' AND install_id <> ?
			`, Args: []any{packID, editionRowID, installID}},
			{SQL: `
				UPDATE canon_pack_installs
				SET lifecycle_status = 'active', activated_at = ` + d1NowExpression + `,
				    removed_at = NULL, updated_at = ` + d1NowExpression + `
				WHERE install_id = ?
			`, Args: []any{installID}},
		}
		result.LifecycleStatus = "active"
	case "deactivate":
		if status == "removed" {
			return nil, ErrReferenceConflict
		}
		statements = []D1Statement{{
			SQL: `UPDATE canon_pack_installs
				SET lifecycle_status = 'inactive', updated_at = ` + d1NowExpression + `
				WHERE install_id = ?`,
			Args: []any{installID},
		}}
		result.LifecycleStatus = "inactive"
	case "remove":
		statements = []D1Statement{{
			SQL: `UPDATE canon_pack_installs
				SET lifecycle_status = 'removed', removed_at = ` + d1NowExpression + `,
				    updated_at = ` + d1NowExpression + `
				WHERE install_id = ?`,
			Args: []any{installID},
		}}
		result.LifecycleStatus = "removed"
	}
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return nil, d1ReferenceStoreError(err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// ReferenceLibraryStore — works
// ---------------------------------------------------------------------------

// CreateReferenceWork registers a hand-built work.
func (s *d1Store) CreateReferenceWork(ctx context.Context, item *ReferenceWork) error {
	if item == nil || referenceRequired(item.WorkID, item.Title) != nil {
		return ErrInvalidReference
	}
	_, err := s.conn.Exec(ctx, `
		INSERT INTO reference_works
			(work_id, title, work_type, default_language, status, metadata_json)
		VALUES (?, ?, ?, ?, ?, ?)
	`, strings.TrimSpace(item.WorkID), strings.TrimSpace(item.Title), defaultString(item.WorkType, "custom"),
		strings.TrimSpace(item.DefaultLanguage), defaultString(item.Status, "draft"), referenceJSON(item.MetadataJSON))
	return d1ReferenceStoreError(err)
}

const d1ReferenceWorkSelect = `SELECT work_id, title, work_type, default_language, status, metadata_json,
		               revision, created_at, updated_at FROM reference_works`

// GetReferenceWork returns one work, or ErrNotFound.
func (s *d1Store) GetReferenceWork(ctx context.Context, workID string) (*ReferenceWork, error) {
	if referenceRequired(workID) != nil {
		return nil, ErrInvalidReference
	}
	var item ReferenceWork
	var metadata *string
	err := s.conn.QueryRow(ctx, d1ReferenceWorkSelect+" WHERE work_id = ?",
		strings.TrimSpace(workID)).Scan(&item.WorkID, &item.Title, &item.WorkType, &item.DefaultLanguage,
		&item.Status, &metadata, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	item.MetadataJSON = d1DerefString(metadata)
	return &item, nil
}

// ListReferenceWorks returns works newest-updated first. A non-positive limit
// means no limit, which is the reference's contract and the one the route
// relies on when it asks for the whole library.
func (s *d1Store) ListReferenceWorks(ctx context.Context, status string, limit int) ([]ReferenceWork, error) {
	query := d1ReferenceWorkSelect
	args := []any{}
	if strings.TrimSpace(status) != "" {
		query += " WHERE status = ?"
		args = append(args, strings.TrimSpace(status))
	}
	query += " ORDER BY updated_at DESC, title ASC"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ReferenceWork{}
	for rows.Next() {
		var item ReferenceWork
		var metadata *string
		if err := rows.Scan(&item.WorkID, &item.Title, &item.WorkType, &item.DefaultLanguage,
			&item.Status, &metadata, &item.Revision, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.MetadataJSON = d1DerefString(metadata)
		items = append(items, item)
	}
	return items, rows.Err()
}

// UpdateReferenceWork rewrites a work under an optimistic revision guard.
//
// updated_at is named explicitly because the MariaDB column carries
// ON UPDATE CURRENT_TIMESTAMP(3) and the canonical schema has no trigger. The
// recency ordering ListReferenceWorks exposes is only correct if the bump
// happens here, so this is not a cosmetic addition.
func (s *d1Store) UpdateReferenceWork(ctx context.Context, item *ReferenceWork, expectedRevision int64) error {
	if item == nil || expectedRevision < 1 || referenceRequired(item.WorkID, item.Title) != nil {
		return ErrInvalidReference
	}
	affected, err := s.conn.Exec(ctx, `
		UPDATE reference_works
		SET title = ?, work_type = ?, default_language = ?, status = ?, metadata_json = ?,
		    revision = revision + 1, updated_at = `+d1NowExpression+`
		WHERE work_id = ? AND revision = ?
	`, strings.TrimSpace(item.Title), defaultString(item.WorkType, "custom"), strings.TrimSpace(item.DefaultLanguage),
		defaultString(item.Status, "draft"), referenceJSON(item.MetadataJSON), strings.TrimSpace(item.WorkID), expectedRevision)
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	return d1ReferenceRevisionChanged(affected)
}

// DeleteReferenceWork removes a work that nothing references.
//
// The in-use probe is the reference's `SELECT binding_id ... FOR UPDATE` moved in
// front of the delete, under d1ReferenceWriteMu. It is also repeated as a guard
// inside the delete itself so a binding created between the probe and the delete
// still refuses the delete rather than orphaning the binding, which the schema
// would in any case reject with a foreign key error. The two paths are
// distinguishable: the guard suppresses the row, and the follow-up read says
// whether a binding was the reason.
func (s *d1Store) DeleteReferenceWork(ctx context.Context, workID string) error {
	if referenceRequired(workID) != nil {
		return ErrInvalidReference
	}
	workID = strings.TrimSpace(workID)

	d1ReferenceWriteMu.Lock()
	defer d1ReferenceWriteMu.Unlock()

	var bindingID string
	err := s.conn.QueryRow(ctx, `
		SELECT binding_id FROM session_reference_bindings
		WHERE work_id = ? LIMIT 1
	`, workID).Scan(&bindingID)
	if err == nil {
		return ErrReferenceWorkInUse
	}
	if !errors.Is(err, errD1NoRows) {
		return err
	}

	affected, err := s.conn.Exec(ctx, `
		DELETE FROM reference_works
		WHERE work_id = ?
		  AND NOT EXISTS (
			SELECT 1 FROM session_reference_bindings b WHERE b.work_id = reference_works.work_id)
	`, workID)
	if err != nil {
		mapped := d1ReferenceStoreError(err)
		if errors.Is(mapped, ErrInvalidReference) {
			return fmt.Errorf("%w: remove session links, local overlays, and installed Canon Packs first", ErrReferenceConflict)
		}
		return mapped
	}
	if affected > 0 {
		return nil
	}
	// Nothing was deleted. The guard, not a missing row, is the more likely
	// reason, and reporting ErrNotFound for a work that a session still links
	// would send the operator to the wrong fix.
	var raced string
	if raceErr := s.conn.QueryRow(ctx,
		`SELECT binding_id FROM session_reference_bindings WHERE work_id = ? LIMIT 1`, workID).Scan(&raced); raceErr == nil {
		return ErrReferenceWorkInUse
	} else if !errors.Is(raceErr, errD1NoRows) {
		return raceErr
	}
	return ErrNotFound
}

// ---------------------------------------------------------------------------
// ReferenceLibraryStore — continuities
// ---------------------------------------------------------------------------

// UpsertReferenceContinuity creates or refreshes one continuity of a work.
func (s *d1Store) UpsertReferenceContinuity(ctx context.Context, item *ReferenceContinuity) error {
	if item == nil || referenceRequired(item.ContinuityID, item.WorkID, item.ContinuityKey, item.Label) != nil {
		return ErrInvalidReference
	}
	_, err := s.conn.Exec(ctx, `
		INSERT INTO reference_continuities
			(continuity_id, work_id, continuity_key, label, parent_continuity_id, status, metadata_json)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT DO UPDATE SET
			label = excluded.label, parent_continuity_id = excluded.parent_continuity_id,
			status = excluded.status, metadata_json = excluded.metadata_json,
			revision = reference_continuities.revision + 1,
			updated_at = `+d1NowExpression,
		strings.TrimSpace(item.ContinuityID), strings.TrimSpace(item.WorkID), strings.TrimSpace(item.ContinuityKey),
		strings.TrimSpace(item.Label), referenceNullable(item.ParentContinuityID), defaultString(item.Status, "active"),
		referenceJSON(item.MetadataJSON))
	return d1ReferenceStoreError(err)
}

// ListReferenceContinuities returns the continuities of a work, labelled order
// then key order, which is the order the picker renders them in.
func (s *d1Store) ListReferenceContinuities(ctx context.Context, workID string) ([]ReferenceContinuity, error) {
	if referenceRequired(workID) != nil {
		return nil, ErrInvalidReference
	}
	rows, err := s.conn.Query(ctx, `
		SELECT continuity_id, work_id, continuity_key, label, parent_continuity_id,
		       status, metadata_json, revision, created_at, updated_at
		FROM reference_continuities WHERE work_id = ? ORDER BY label, continuity_key
	`, strings.TrimSpace(workID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ReferenceContinuity{}
	for rows.Next() {
		var item ReferenceContinuity
		var parent, metadata *string
		if err := rows.Scan(&item.ContinuityID, &item.WorkID, &item.ContinuityKey, &item.Label,
			&parent, &item.Status, &metadata, &item.Revision, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.ParentContinuityID = d1DerefString(parent)
		item.MetadataJSON = d1DerefString(metadata)
		items = append(items, item)
	}
	return items, rows.Err()
}

// DeleteReferenceContinuity removes one continuity.
func (s *d1Store) DeleteReferenceContinuity(ctx context.Context, continuityID string) error {
	return s.d1ReferenceDeleteRow(ctx, "reference_continuities", "continuity_id", continuityID)
}

// ---------------------------------------------------------------------------
// ReferenceLibraryStore — documents
// ---------------------------------------------------------------------------

// SaveReferenceDocument stores one imported reference source.
func (s *d1Store) SaveReferenceDocument(ctx context.Context, item *ReferenceDocument) error {
	if item == nil || referenceRequired(item.DocumentID, item.WorkID, item.ContinuityID, item.ContentHash) != nil {
		return ErrInvalidReference
	}
	_, err := s.conn.Exec(ctx, `
		INSERT INTO reference_documents
			(document_id, work_id, continuity_id, source_type, source_uri, content_hash,
			 raw_retention, raw_text, import_status, provenance_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, strings.TrimSpace(item.DocumentID), strings.TrimSpace(item.WorkID), strings.TrimSpace(item.ContinuityID),
		defaultString(item.SourceType, "manual_text"), referenceNullable(item.SourceURI), strings.TrimSpace(item.ContentHash),
		defaultString(item.RawRetention, "full"), referenceNullable(item.RawText), defaultString(item.ImportStatus, "pending"),
		referenceJSON(item.ProvenanceJSON))
	return d1ReferenceStoreError(err)
}

// UpdateReferenceDocumentSource attaches the full text of a document.
//
// raw_retention is forced to 'full' rather than taken from the caller: the
// statement only makes sense for a document whose body is now being retained,
// and a caller that left the field at its metadata-only value would otherwise
// store text the retention policy says must not be kept.
func (s *d1Store) UpdateReferenceDocumentSource(ctx context.Context, item *ReferenceDocument) error {
	if item == nil || referenceRequired(item.DocumentID, item.WorkID, item.ContinuityID, item.ContentHash, item.RawText) != nil {
		return ErrInvalidReference
	}
	affected, err := s.conn.Exec(ctx, `
		UPDATE reference_documents
		SET source_type = ?, source_uri = ?, raw_retention = 'full', raw_text = ?, provenance_json = ?,
		    updated_at = `+d1NowExpression+`
		WHERE document_id = ? AND work_id = ? AND continuity_id = ? AND content_hash = ?
	`, defaultString(item.SourceType, "community_wiki"), referenceNullable(item.SourceURI), item.RawText,
		referenceJSON(item.ProvenanceJSON), strings.TrimSpace(item.DocumentID), strings.TrimSpace(item.WorkID),
		strings.TrimSpace(item.ContinuityID), strings.TrimSpace(item.ContentHash))
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	return d1ReferenceRowsChanged(affected)
}

const d1ReferenceDocumentSelect = `SELECT document_id, work_id, continuity_id, source_type, source_uri, content_hash,
	                 raw_retention, raw_text, import_status, provenance_json, created_at, updated_at
	          FROM reference_documents`

// GetReferenceDocument returns one document, or ErrNotFound.
func (s *d1Store) GetReferenceDocument(ctx context.Context, documentID string) (*ReferenceDocument, error) {
	if referenceRequired(documentID) != nil {
		return nil, ErrInvalidReference
	}
	var item ReferenceDocument
	var uri, raw, provenance *string
	err := s.conn.QueryRow(ctx, d1ReferenceDocumentSelect+" WHERE document_id = ?",
		strings.TrimSpace(documentID)).Scan(&item.DocumentID, &item.WorkID, &item.ContinuityID,
		&item.SourceType, &uri, &item.ContentHash, &item.RawRetention, &raw, &item.ImportStatus,
		&provenance, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	item.SourceURI = d1DerefString(uri)
	item.RawText = d1DerefString(raw)
	item.ProvenanceJSON = d1DerefString(provenance)
	return &item, nil
}

// UpdateReferenceDocumentStatus moves a document through its import lifecycle.
func (s *d1Store) UpdateReferenceDocumentStatus(ctx context.Context, documentID, status string) error {
	if referenceRequired(documentID, status) != nil {
		return ErrInvalidReference
	}
	affected, err := s.conn.Exec(ctx, `
		UPDATE reference_documents SET import_status = ?, updated_at = `+d1NowExpression+`
		WHERE document_id = ?
	`, strings.TrimSpace(status), strings.TrimSpace(documentID))
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	return d1ReferenceRowsChanged(affected)
}

// ListReferenceDocuments returns the documents of a work, optionally narrowed to
// one continuity and one import status.
func (s *d1Store) ListReferenceDocuments(ctx context.Context, workID, continuityID, status string) ([]ReferenceDocument, error) {
	if referenceRequired(workID) != nil {
		return nil, ErrInvalidReference
	}
	query := d1ReferenceDocumentSelect + " WHERE work_id = ?"
	args := []any{strings.TrimSpace(workID)}
	if strings.TrimSpace(continuityID) != "" {
		query += " AND continuity_id = ?"
		args = append(args, strings.TrimSpace(continuityID))
	}
	if strings.TrimSpace(status) != "" {
		query += " AND import_status = ?"
		args = append(args, strings.TrimSpace(status))
	}
	query += " ORDER BY updated_at DESC, document_id"
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ReferenceDocument{}
	for rows.Next() {
		var item ReferenceDocument
		var uri, raw, provenance *string
		if err := rows.Scan(&item.DocumentID, &item.WorkID, &item.ContinuityID, &item.SourceType, &uri,
			&item.ContentHash, &item.RawRetention, &raw, &item.ImportStatus, &provenance,
			&item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.SourceURI = d1DerefString(uri)
		item.RawText = d1DerefString(raw)
		item.ProvenanceJSON = d1DerefString(provenance)
		items = append(items, item)
	}
	return items, rows.Err()
}

// DeleteReferenceDocument removes one document.
func (s *d1Store) DeleteReferenceDocument(ctx context.Context, documentID string) error {
	return s.d1ReferenceDeleteRow(ctx, "reference_documents", "document_id", documentID)
}

// d1ReferenceDeleteRow is the reference's deleteReferenceRow. The table and key
// names are composed into SQL, so the allowlist is what keeps a request from
// choosing them: without it, a caller could name any table in the database.
func (s *d1Store) d1ReferenceDeleteRow(ctx context.Context, table, key, value string) error {
	if referenceRequired(value) != nil {
		return ErrInvalidReference
	}
	allowed := map[string]string{
		"reference_continuities":   "continuity_id",
		"reference_documents":      "document_id",
		"reference_timeline_nodes": "node_id",
		"reference_entities":       "entity_id",
		"reference_claims":         "claim_id",
	}
	if allowed[table] != key {
		return ErrInvalidReference
	}
	affected, err := s.conn.Exec(ctx, "DELETE FROM "+table+" WHERE "+key+" = ?", strings.TrimSpace(value))
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	return d1ReferenceRowsChanged(affected)
}

// ---------------------------------------------------------------------------
// ReferenceLibraryStore — timeline nodes
// ---------------------------------------------------------------------------

// UpsertReferenceTimelineNode creates or refreshes one timeline node.
//
// The IF(review_status = 'pending', ...) guard is the whole point of this
// upsert: an approved node is a human decision, and a re-extraction that
// produces a different label for the same key must not silently rewrite it. The
// guard is reproduced as a CASE over the pre-update review_status, and
// updated_at moves on the same condition so an approved node that nothing
// changed also does not appear freshly edited.
func (s *d1Store) UpsertReferenceTimelineNode(ctx context.Context, item *ReferenceTimelineNode) error {
	if item == nil || referenceRequired(item.NodeID, item.WorkID, item.ContinuityID, item.NodeKey, item.Label) != nil {
		return ErrInvalidReference
	}
	_, err := s.conn.Exec(ctx, `
		INSERT INTO reference_timeline_nodes
			(node_id, work_id, continuity_id, node_key, label, ordinal_value,
			 parent_node_id, branch_key, node_kind, metadata_json, review_status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT DO UPDATE SET
			label = CASE WHEN reference_timeline_nodes.review_status = 'pending' THEN excluded.label ELSE reference_timeline_nodes.label END,
			ordinal_value = CASE WHEN reference_timeline_nodes.review_status = 'pending' THEN excluded.ordinal_value ELSE reference_timeline_nodes.ordinal_value END,
			parent_node_id = CASE WHEN reference_timeline_nodes.review_status = 'pending' THEN excluded.parent_node_id ELSE reference_timeline_nodes.parent_node_id END,
			branch_key = CASE WHEN reference_timeline_nodes.review_status = 'pending' THEN excluded.branch_key ELSE reference_timeline_nodes.branch_key END,
			node_kind = CASE WHEN reference_timeline_nodes.review_status = 'pending' THEN excluded.node_kind ELSE reference_timeline_nodes.node_kind END,
			metadata_json = CASE WHEN reference_timeline_nodes.review_status = 'pending' THEN excluded.metadata_json ELSE reference_timeline_nodes.metadata_json END,
			review_status = CASE WHEN reference_timeline_nodes.review_status = 'pending' THEN excluded.review_status ELSE reference_timeline_nodes.review_status END,
			updated_at = CASE WHEN reference_timeline_nodes.review_status = 'pending' THEN `+d1NowExpression+` ELSE reference_timeline_nodes.updated_at END
	`, strings.TrimSpace(item.NodeID), strings.TrimSpace(item.WorkID), strings.TrimSpace(item.ContinuityID),
		strings.TrimSpace(item.NodeKey), strings.TrimSpace(item.Label), item.Ordinal,
		referenceNullable(item.ParentNodeID), defaultString(item.BranchKey, "main"),
		defaultString(item.NodeKind, "event"), referenceJSON(item.MetadataJSON), defaultString(item.ReviewStatus, "pending"))
	return d1ReferenceStoreError(err)
}

// ListReferenceTimelineNodes returns the chronology of a work.
//
// The review_status = 'approved' case appends two extra restrictions and they
// are the point of the read: an item is only retrievable when its origin is
// still live, and a local overlay may suppress it. Dropping either clause would
// serve canon material from a retired pack, or material the user explicitly
// overrode, as if it were approved.
func (s *d1Store) ListReferenceTimelineNodes(ctx context.Context, workID, continuityID, reviewStatus string) ([]ReferenceTimelineNode, error) {
	if referenceRequired(workID) != nil {
		return nil, ErrInvalidReference
	}
	query := `SELECT node_id, work_id, continuity_id, node_key, label, ordinal_value,
		                 parent_node_id, branch_key, node_kind, metadata_json, review_status,
		                 review_source, review_reason, reviewed_at, created_at, updated_at
	          FROM reference_timeline_nodes item WHERE work_id = ?`
	args := []any{strings.TrimSpace(workID)}
	if strings.TrimSpace(continuityID) != "" {
		query += " AND continuity_id = ?"
		args = append(args, strings.TrimSpace(continuityID))
	}
	if strings.TrimSpace(reviewStatus) != "" {
		query += " AND review_status = ?"
		args = append(args, strings.TrimSpace(reviewStatus))
	}
	if strings.TrimSpace(reviewStatus) == "approved" {
		query += canonPackActiveOriginClause("timeline", "node_id", "item.node_id")
		query += ` AND NOT EXISTS (
			SELECT 1 FROM reference_overlay_rules overlay_rule
			WHERE overlay_rule.target_node_id = item.node_id AND overlay_rule.rule_status = 'active'
			  AND overlay_rule.overlay_action IN ('suppress_for_retrieval', 'override')
			  AND (overlay_rule.overlay_action <> 'override' OR overlay_rule.replacement_node_id <> item.node_id)
		)`
	}
	query += " ORDER BY continuity_id, branch_key, ordinal_value, node_key"
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ReferenceTimelineNode{}
	for rows.Next() {
		var item ReferenceTimelineNode
		var parent, metadata, reviewReason *string
		var reviewedAt *time.Time
		if err := rows.Scan(&item.NodeID, &item.WorkID, &item.ContinuityID, &item.NodeKey, &item.Label,
			&item.Ordinal, &parent, &item.BranchKey, &item.NodeKind, &metadata, &item.ReviewStatus,
			&item.ReviewSource, &reviewReason, &reviewedAt,
			&item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.ParentNodeID = d1DerefString(parent)
		item.MetadataJSON = d1DerefString(metadata)
		item.ReviewReason = d1DerefString(reviewReason)
		item.ReviewedAt = reviewedAt
		items = append(items, item)
	}
	return items, rows.Err()
}

// DeleteReferenceTimelineNode removes one timeline node.
func (s *d1Store) DeleteReferenceTimelineNode(ctx context.Context, nodeID string) error {
	return s.d1ReferenceDeleteRow(ctx, "reference_timeline_nodes", "node_id", nodeID)
}

// d1ReferenceApprovedNodeOrder is the exact order NormalizeReferenceTimelineOrder
// normalises in: the stored ordinal first, then branch, key, and id. The
// node_id tiebreak is what makes the result deterministic, and therefore makes a
// repeated normalise a genuine no-op instead of a reshuffle of the nodes that
// share an ordinal.
const d1ReferenceApprovedNodeOrder = `
		ORDER BY ordinal_value, branch_key, node_key, node_id`

// d1ReferenceApprovedNodeIDs returns the approved node ids of one continuity in
// normalisation order, or as an unordered set when ordered is false.
//
// The statement is the reference's, unchanged. ApplyReferenceTimelineOrder does
// not need the order, only the set, and reading it in the same order costs
// nothing while keeping one statement for both callers.
func (s *d1Store) d1ReferenceApprovedNodeIDs(ctx context.Context, workID, continuityID string, ordered bool) ([]string, error) {
	query := `
		SELECT node_id FROM reference_timeline_nodes
		WHERE work_id = ? AND continuity_id = ? AND review_status = 'approved'`
	if ordered {
		query += d1ReferenceApprovedNodeOrder
	}
	rows, err := s.conn.Query(ctx, query, workID, continuityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// NormalizeReferenceTimelineOrder renumbers the approved chronology to 10, 20, 30
// and returns how many nodes it touched. The read is the reference's own ORDER
// BY, not a Go-side sort, so the numbering is the same sequence the caller
// would have seen.
func (s *d1Store) NormalizeReferenceTimelineOrder(ctx context.Context, workID, continuityID string) (int, error) {
	if referenceRequired(workID, continuityID) != nil {
		return 0, ErrInvalidReference
	}
	workID, continuityID = strings.TrimSpace(workID), strings.TrimSpace(continuityID)

	d1ReferenceWriteMu.Lock()
	defer d1ReferenceWriteMu.Unlock()

	ids, err := s.d1ReferenceApprovedNodeIDs(ctx, workID, continuityID, true)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	statements := make([]D1Statement, 0, len(ids))
	for i, id := range ids {
		statements = append(statements, D1Statement{
			SQL: `UPDATE reference_timeline_nodes SET ordinal_value = ?, updated_at = ` + d1NowExpression + `
				WHERE work_id = ? AND continuity_id = ? AND node_id = ?`,
			Args: []any{int64((i + 1) * 10), workID, continuityID, id},
		})
	}
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return 0, d1ReferenceStoreError(err)
	}
	return len(ids), nil
}

// ApplyReferenceTimelineOrder writes a caller-supplied ordering.
//
// The order must be a permutation of the approved set: the reference checks both
// that the two sets have the same size and that no id is unknown or repeated, and
// it must keep doing so. Accepting a partial order would leave the omitted nodes
// carrying stale ordinals that the next normalise pass would renumber out from
// under the caller.
func (s *d1Store) ApplyReferenceTimelineOrder(ctx context.Context, workID, continuityID string, orderedIDs []string) (int, error) {
	if referenceRequired(workID, continuityID) != nil || len(orderedIDs) == 0 {
		return 0, ErrInvalidReference
	}
	workID, continuityID = strings.TrimSpace(workID), strings.TrimSpace(continuityID)

	d1ReferenceWriteMu.Lock()
	defer d1ReferenceWriteMu.Unlock()

	approved, err := s.d1ReferenceApprovedNodeIDs(ctx, workID, continuityID, false)
	if err != nil {
		return 0, err
	}
	allowed := make(map[string]struct{}, len(approved))
	for _, id := range approved {
		allowed[id] = struct{}{}
	}
	if len(allowed) != len(orderedIDs) {
		return 0, ErrReferenceConflict
	}
	seen := map[string]struct{}{}
	for _, id := range orderedIDs {
		id = strings.TrimSpace(id)
		if _, ok := allowed[id]; !ok {
			return 0, ErrReferenceConflict
		}
		if _, duplicate := seen[id]; duplicate {
			return 0, ErrReferenceConflict
		}
		seen[id] = struct{}{}
	}
	statements := make([]D1Statement, 0, len(orderedIDs))
	for i, id := range orderedIDs {
		statements = append(statements, D1Statement{
			SQL: `UPDATE reference_timeline_nodes SET ordinal_value = ?, updated_at = ` + d1NowExpression + `
				WHERE work_id = ? AND continuity_id = ? AND node_id = ? AND review_status = 'approved'`,
			Args: []any{int64((i + 1) * 10), workID, continuityID, strings.TrimSpace(id)},
		})
	}
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return 0, d1ReferenceStoreError(err)
	}
	return len(orderedIDs), nil
}

// ---------------------------------------------------------------------------
// ReferenceLibraryStore — entities and aliases
// ---------------------------------------------------------------------------

// UpsertReferenceEntity creates or refreshes one reference entity under the same
// approved-is-frozen guard as the timeline upsert.
func (s *d1Store) UpsertReferenceEntity(ctx context.Context, item *ReferenceEntity) error {
	if item == nil || referenceRequired(item.EntityID, item.WorkID, item.ContinuityID, item.EntityType, item.CanonicalName) != nil {
		return ErrInvalidReference
	}
	_, err := s.conn.Exec(ctx, `
		INSERT INTO reference_entities
			(entity_id, work_id, continuity_id, entity_type, canonical_name,
			 description_text, metadata_json, review_status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT DO UPDATE SET
			entity_type = CASE WHEN reference_entities.review_status = 'pending' THEN excluded.entity_type ELSE reference_entities.entity_type END,
			canonical_name = CASE WHEN reference_entities.review_status = 'pending' THEN excluded.canonical_name ELSE reference_entities.canonical_name END,
			description_text = CASE WHEN reference_entities.review_status = 'pending' THEN excluded.description_text ELSE reference_entities.description_text END,
			metadata_json = CASE WHEN reference_entities.review_status = 'pending' THEN excluded.metadata_json ELSE reference_entities.metadata_json END,
			review_status = CASE WHEN reference_entities.review_status = 'pending' THEN excluded.review_status ELSE reference_entities.review_status END,
			updated_at = CASE WHEN reference_entities.review_status = 'pending' THEN `+d1NowExpression+` ELSE reference_entities.updated_at END
	`, strings.TrimSpace(item.EntityID), strings.TrimSpace(item.WorkID), strings.TrimSpace(item.ContinuityID),
		strings.TrimSpace(item.EntityType), strings.TrimSpace(item.CanonicalName), referenceNullable(item.DescriptionText),
		referenceJSON(item.MetadataJSON), defaultString(item.ReviewStatus, "pending"))
	return d1ReferenceStoreError(err)
}

// ListReferenceEntities returns the entities of a work. The approved case is
// narrowed by live origin and by the local overlay policy, exactly as the
// timeline read is.
func (s *d1Store) ListReferenceEntities(ctx context.Context, workID, continuityID, reviewStatus string) ([]ReferenceEntity, error) {
	if referenceRequired(workID) != nil {
		return nil, ErrInvalidReference
	}
	query := `SELECT entity_id, work_id, continuity_id, entity_type, canonical_name,
	                 description_text, metadata_json, review_status, review_source,
	                 review_reason, reviewed_at, created_at, updated_at
	          FROM reference_entities item WHERE work_id = ?`
	args := []any{strings.TrimSpace(workID)}
	if strings.TrimSpace(continuityID) != "" {
		query += " AND continuity_id = ?"
		args = append(args, strings.TrimSpace(continuityID))
	}
	if strings.TrimSpace(reviewStatus) != "" {
		query += " AND review_status = ?"
		args = append(args, strings.TrimSpace(reviewStatus))
	}
	if strings.TrimSpace(reviewStatus) == "approved" {
		query += canonPackActiveOriginClause("entity", "entity_id", "item.entity_id")
		query += ` AND NOT EXISTS (
			SELECT 1 FROM reference_overlay_rules overlay_rule
			WHERE overlay_rule.target_entity_id = item.entity_id AND overlay_rule.rule_status = 'active'
			  AND overlay_rule.overlay_action IN ('suppress_for_retrieval', 'override')
			  AND (overlay_rule.overlay_action <> 'override' OR overlay_rule.replacement_entity_id <> item.entity_id)
		)`
	}
	query += " ORDER BY canonical_name, entity_id"
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ReferenceEntity{}
	for rows.Next() {
		var item ReferenceEntity
		var description, metadata, reviewReason *string
		var reviewedAt *time.Time
		if err := rows.Scan(&item.EntityID, &item.WorkID, &item.ContinuityID, &item.EntityType,
			&item.CanonicalName, &description, &metadata, &item.ReviewStatus, &item.ReviewSource,
			&reviewReason, &reviewedAt, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.DescriptionText = d1DerefString(description)
		item.MetadataJSON = d1DerefString(metadata)
		item.ReviewReason = d1DerefString(reviewReason)
		item.ReviewedAt = reviewedAt
		items = append(items, item)
	}
	return items, rows.Err()
}

// UpsertReferenceEntityAlias records one surface alias and reports the row it
// resolved to.
//
// RETURNING is the D1 replacement for LastInsertId, and it is used on the
// upsert's DO UPDATE branch as well as on a fresh insert, so a caller that asked
// for the alias id learns which row actually owns the alias even when a previous
// install already created it. A caller that supplied its own alias id keeps it,
// which is the reference's rule.
func (s *d1Store) UpsertReferenceEntityAlias(ctx context.Context, item *ReferenceEntityAlias) error {
	if item == nil || referenceRequired(item.WorkID, item.ContinuityID, item.EntityID, item.AliasText, item.NormalizedAlias) != nil {
		return ErrInvalidReference
	}
	var aliasID int64
	err := s.conn.QueryRow(ctx, `
		INSERT INTO reference_entity_aliases
			(work_id, continuity_id, entity_id, alias_text, normalized_alias, language_code)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT DO UPDATE SET
			alias_text = excluded.alias_text,
			language_code = excluded.language_code
		RETURNING alias_id
	`, strings.TrimSpace(item.WorkID), strings.TrimSpace(item.ContinuityID), strings.TrimSpace(item.EntityID),
		strings.TrimSpace(item.AliasText), strings.TrimSpace(item.NormalizedAlias), strings.TrimSpace(item.LanguageCode)).Scan(&aliasID)
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	if item.AliasID == 0 {
		item.AliasID = aliasID
	}
	return nil
}

const d1ReferenceAliasSelect = `SELECT alias_id, work_id, continuity_id, entity_id, alias_text,
		               normalized_alias, language_code, created_at
		FROM reference_entity_aliases`

// ListReferenceEntityAliases returns the aliases of one entity.
func (s *d1Store) ListReferenceEntityAliases(ctx context.Context, entityID string) ([]ReferenceEntityAlias, error) {
	if referenceRequired(entityID) != nil {
		return nil, ErrInvalidReference
	}
	rows, err := s.conn.Query(ctx, d1ReferenceAliasSelect+" WHERE entity_id = ? ORDER BY alias_text, alias_id",
		strings.TrimSpace(entityID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ReferenceEntityAlias{}
	for rows.Next() {
		var item ReferenceEntityAlias
		if err := rows.Scan(&item.AliasID, &item.WorkID, &item.ContinuityID, &item.EntityID,
			&item.AliasText, &item.NormalizedAlias, &item.LanguageCode, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// DeleteReferenceEntity removes one reference entity.
func (s *d1Store) DeleteReferenceEntity(ctx context.Context, entityID string) error {
	return s.d1ReferenceDeleteRow(ctx, "reference_entities", "entity_id", entityID)
}

// ---------------------------------------------------------------------------
// ReferenceLibraryStore — claims
// ---------------------------------------------------------------------------

// UpsertReferenceClaim creates or refreshes one factual claim under the same
// approved-is-frozen guard.
func (s *d1Store) UpsertReferenceClaim(ctx context.Context, item *ReferenceClaim) error {
	if item == nil || referenceRequired(item.ClaimID, item.WorkID, item.ContinuityID, item.DocumentID, item.ClaimType, item.ClaimText) != nil {
		return ErrInvalidReference
	}
	_, err := s.conn.Exec(ctx, `
		INSERT INTO reference_claims
			(claim_id, work_id, continuity_id, document_id, claim_type, subject_entity_id,
			 claim_text, evidence_excerpt, temporal_scope, valid_from_node_id, valid_to_node_id,
			 reveal_from_node_id, branch_key, knowledge_scope, confidence, review_status, metadata_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT DO UPDATE SET
			claim_type = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.claim_type ELSE reference_claims.claim_type END,
			subject_entity_id = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.subject_entity_id ELSE reference_claims.subject_entity_id END,
			claim_text = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.claim_text ELSE reference_claims.claim_text END,
			evidence_excerpt = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.evidence_excerpt ELSE reference_claims.evidence_excerpt END,
			temporal_scope = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.temporal_scope ELSE reference_claims.temporal_scope END,
			valid_from_node_id = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.valid_from_node_id ELSE reference_claims.valid_from_node_id END,
			valid_to_node_id = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.valid_to_node_id ELSE reference_claims.valid_to_node_id END,
			reveal_from_node_id = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.reveal_from_node_id ELSE reference_claims.reveal_from_node_id END,
			branch_key = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.branch_key ELSE reference_claims.branch_key END,
			knowledge_scope = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.knowledge_scope ELSE reference_claims.knowledge_scope END,
			confidence = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.confidence ELSE reference_claims.confidence END,
			metadata_json = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.metadata_json ELSE reference_claims.metadata_json END,
			review_status = CASE WHEN reference_claims.review_status = 'pending' THEN excluded.review_status ELSE reference_claims.review_status END,
			updated_at = CASE WHEN reference_claims.review_status = 'pending' THEN `+d1NowExpression+` ELSE reference_claims.updated_at END
	`, strings.TrimSpace(item.ClaimID), strings.TrimSpace(item.WorkID), strings.TrimSpace(item.ContinuityID),
		strings.TrimSpace(item.DocumentID), strings.TrimSpace(item.ClaimType), referenceNullable(item.SubjectEntityID),
		strings.TrimSpace(item.ClaimText), referenceNullable(item.EvidenceExcerpt), defaultString(item.TemporalScope, "bounded"),
		referenceNullable(item.ValidFromNodeID), referenceNullable(item.ValidToNodeID), referenceNullable(item.RevealFromNodeID),
		defaultString(item.BranchKey, "main"), defaultString(item.KnowledgeScope, "public_world"), item.Confidence,
		defaultString(item.ReviewStatus, "pending"), referenceJSON(item.MetadataJSON))
	return d1ReferenceStoreError(err)
}

// ListReferenceClaims returns the claims of a work and attaches each claim's
// knower set.
//
// The knowers are a second projection, not a column, and they are private
// knowledge: a claim that only one character knows must not be recalled for
// another. Reading them per claim keeps the ORDER BY entity_id the reference
// relies on, so a caller diffing two reads sees the same order.
func (s *d1Store) ListReferenceClaims(ctx context.Context, workID, continuityID, reviewStatus, branchKey string) ([]ReferenceClaim, error) {
	if referenceRequired(workID) != nil {
		return nil, ErrInvalidReference
	}
	query := `SELECT claim_id, work_id, continuity_id, document_id, claim_type, subject_entity_id,
	                 claim_text, evidence_excerpt, temporal_scope, valid_from_node_id, valid_to_node_id,
	                 reveal_from_node_id, branch_key, knowledge_scope, confidence, review_status,
	                 review_source, review_reason, reviewed_at, metadata_json, created_at, updated_at
	          FROM reference_claims item WHERE work_id = ?`
	args := []any{strings.TrimSpace(workID)}
	if strings.TrimSpace(continuityID) != "" {
		query += " AND continuity_id = ?"
		args = append(args, strings.TrimSpace(continuityID))
	}
	if strings.TrimSpace(reviewStatus) != "" {
		query += " AND review_status = ?"
		args = append(args, strings.TrimSpace(reviewStatus))
	}
	if strings.TrimSpace(reviewStatus) == "approved" {
		query += canonPackActiveOriginClause("claim", "claim_id", "item.claim_id")
		// The claim variant of the overlay clause goes through the fact identity,
		// because an overlay targets a LOGICAL fact. Matching on the claim id
		// alone would let a second claim carrying the same fact stay retrievable
		// while the first was suppressed.
		query += ` AND NOT EXISTS (
			SELECT 1 FROM reference_fact_identities fact_identity
			JOIN reference_overlay_rules overlay_rule
				ON overlay_rule.target_logical_fact_id = fact_identity.logical_fact_id
			WHERE fact_identity.claim_id = item.claim_id AND overlay_rule.rule_status = 'active'
			  AND overlay_rule.overlay_action IN ('suppress_for_retrieval', 'override')
			  AND (overlay_rule.overlay_action <> 'override' OR overlay_rule.replacement_claim_id <> item.claim_id)
		)`
	}
	if strings.TrimSpace(branchKey) != "" {
		query += " AND branch_key = ?"
		args = append(args, strings.TrimSpace(branchKey))
	}
	query += " ORDER BY updated_at DESC, claim_id"
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ReferenceClaim{}
	for rows.Next() {
		var item ReferenceClaim
		var subject, evidence, validFrom, validTo, revealFrom, metadata, reviewReason *string
		var reviewedAt *time.Time
		if err := rows.Scan(&item.ClaimID, &item.WorkID, &item.ContinuityID, &item.DocumentID,
			&item.ClaimType, &subject, &item.ClaimText, &evidence, &item.TemporalScope,
			&validFrom, &validTo, &revealFrom, &item.BranchKey, &item.KnowledgeScope,
			&item.Confidence, &item.ReviewStatus, &item.ReviewSource, &reviewReason, &reviewedAt,
			&metadata, &item.CreatedAt, &item.UpdatedAt); err != nil {
			rows.Close()
			return nil, err
		}
		item.SubjectEntityID = d1DerefString(subject)
		item.EvidenceExcerpt = d1DerefString(evidence)
		item.ValidFromNodeID = d1DerefString(validFrom)
		item.ValidToNodeID = d1DerefString(validTo)
		item.RevealFromNodeID = d1DerefString(revealFrom)
		item.MetadataJSON = d1DerefString(metadata)
		item.ReviewReason = d1DerefString(reviewReason)
		item.ReviewedAt = reviewedAt
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for i := range items {
		knowers, err := s.d1ReferenceClaimKnowers(ctx, items[i].ClaimID)
		if err != nil {
			return nil, err
		}
		items[i].KnowerEntityIDs = knowers
	}
	return items, nil
}

// ReplaceReferenceClaimKnowers replaces the private-knowledge set of a claim.
//
// The delete and every insert are one batch, so a reader never observes a claim
// whose knower set is half replaced. Duplicates are collapsed and a blank id is
// refused, both before the batch, so a malformed request costs nothing.
func (s *d1Store) ReplaceReferenceClaimKnowers(ctx context.Context, claimID string, entityIDs []string) error {
	if referenceRequired(claimID) != nil {
		return ErrInvalidReference
	}
	claimID = strings.TrimSpace(claimID)
	statements := []D1Statement{{
		SQL:  `DELETE FROM reference_claim_knowers WHERE claim_id = ?`,
		Args: []any{claimID},
	}}
	seen := map[string]struct{}{}
	for _, entityID := range entityIDs {
		entityID = strings.TrimSpace(entityID)
		if entityID == "" {
			return ErrInvalidReference
		}
		if _, ok := seen[entityID]; ok {
			continue
		}
		seen[entityID] = struct{}{}
		statements = append(statements, D1Statement{
			SQL:  `INSERT INTO reference_claim_knowers (claim_id, entity_id) VALUES (?, ?)`,
			Args: []any{claimID, entityID},
		})
	}
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return d1ReferenceStoreError(err)
	}
	return nil
}

func (s *d1Store) d1ReferenceClaimKnowers(ctx context.Context, claimID string) ([]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT entity_id FROM reference_claim_knowers WHERE claim_id = ? ORDER BY entity_id
	`, strings.TrimSpace(claimID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []string{}
	for rows.Next() {
		var entityID string
		if err := rows.Scan(&entityID); err != nil {
			return nil, err
		}
		items = append(items, entityID)
	}
	return items, rows.Err()
}

// DeleteReferenceClaim removes one claim.
func (s *d1Store) DeleteReferenceClaim(ctx context.Context, claimID string) error {
	return s.d1ReferenceDeleteRow(ctx, "reference_claims", "claim_id", claimID)
}

// d1ReferenceReviewTarget maps an extracted-candidate kind to the one table and
// key column its review status lives in.
//
// The table name is composed into the UPDATE, so it may only ever come from this
// allowlist. A name taken from the request would let a review transition write
// to any table in the database.
func d1ReferenceReviewTarget(kind string) (string, string, bool) {
	switch strings.TrimSpace(kind) {
	case "timeline":
		return "reference_timeline_nodes", "node_id", true
	case "entity":
		return "reference_entities", "entity_id", true
	case "claim":
		return "reference_claims", "claim_id", true
	}
	return "", "", false
}

// UpdateReferenceCandidateReview approves, rejects, or resets one extracted
// candidate.
//
// The table and key are chosen from the kind, never from the request, and
// reviewed_at is stamped in the same statement as the status so a row can never
// carry a new status with a stale review instant.
func (s *d1Store) UpdateReferenceCandidateReview(ctx context.Context, workID, kind, id, status, source, reason string) error {
	if referenceRequired(workID, kind, id, status, source) != nil {
		return ErrInvalidReference
	}
	if status != "approved" && status != "rejected" && status != "pending" {
		return ErrInvalidReference
	}
	table, column, ok := d1ReferenceReviewTarget(kind)
	if !ok {
		return ErrInvalidReference
	}
	affected, err := s.conn.Exec(ctx,
		"UPDATE "+table+" SET review_status = ?, review_source = ?, review_reason = ?, reviewed_at = "+
			d1NowExpression+", updated_at = "+d1NowExpression+" WHERE work_id = ? AND "+column+" = ?",
		status, strings.TrimSpace(source), referenceNullable(reason), strings.TrimSpace(workID), strings.TrimSpace(id))
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	return d1ReferenceRowsChanged(affected)
}

// UpdateReferenceLibraryItem applies a user correction to one library item and
// approves it in the same statement.
//
// This is the escape hatch from the frozen-on-approval rule, and it is the only
// path that may rewrite an approved row. The claim variant also clears
// evidence_grounded: a human-edited claim is by definition no longer the text an
// evidence locator describes, and leaving the flag set would let recall treat an
// edited claim as source-backed.
//
// json('false') is load-bearing. SQLite binds the bare literal FALSE as the
// integer 0, so json_set(..., false) would store {"evidence_grounded":0}. The
// json() wrapper marks the value as JSON, which is the boolean MariaDB writes.
func (s *d1Store) UpdateReferenceLibraryItem(ctx context.Context, item *ReferenceLibraryItemUpdate) error {
	if item == nil || referenceRequired(item.WorkID, item.Kind, item.ID) != nil {
		return ErrInvalidReference
	}
	var affected int64
	var err error
	switch strings.TrimSpace(item.Kind) {
	case "timeline":
		if referenceRequired(item.NodeKey, item.Label, item.NodeKind, item.BranchKey) != nil {
			return ErrInvalidReference
		}
		affected, err = s.conn.Exec(ctx, `
			UPDATE reference_timeline_nodes
			SET node_key = ?, label = ?, ordinal_value = ?, node_kind = ?, branch_key = ?,
				review_status = 'approved', review_source = 'user_edit',
				review_reason = 'user corrected reference data', reviewed_at = `+d1NowExpression+`,
				updated_at = `+d1NowExpression+`
			WHERE work_id = ? AND node_id = ?
		`, strings.TrimSpace(item.NodeKey), strings.TrimSpace(item.Label), item.Ordinal,
			strings.TrimSpace(item.NodeKind), strings.TrimSpace(item.BranchKey),
			strings.TrimSpace(item.WorkID), strings.TrimSpace(item.ID))
	case "entity":
		if referenceRequired(item.EntityType, item.CanonicalName) != nil {
			return ErrInvalidReference
		}
		affected, err = s.conn.Exec(ctx, `
			UPDATE reference_entities
			SET entity_type = ?, canonical_name = ?, description_text = ?,
				review_status = 'approved', review_source = 'user_edit',
				review_reason = 'user corrected reference data', reviewed_at = `+d1NowExpression+`,
				updated_at = `+d1NowExpression+`
			WHERE work_id = ? AND entity_id = ?
		`, strings.TrimSpace(item.EntityType), strings.TrimSpace(item.CanonicalName),
			referenceNullable(item.DescriptionText), strings.TrimSpace(item.WorkID), strings.TrimSpace(item.ID))
	case "claim":
		if referenceRequired(item.ClaimType, item.ClaimText, item.TemporalScope, item.KnowledgeScope) != nil {
			return ErrInvalidReference
		}
		affected, err = s.conn.Exec(ctx, `
			UPDATE reference_claims
			SET claim_type = ?, claim_text = ?, evidence_excerpt = ?, temporal_scope = ?,
				knowledge_scope = ?, confidence = ?, review_status = 'approved',
				metadata_json = json_set(COALESCE(metadata_json, '{}'), '$.evidence_grounded', json('false')),
				review_source = 'user_edit', review_reason = 'user corrected reference data',
				reviewed_at = `+d1NowExpression+`, updated_at = `+d1NowExpression+`
			WHERE work_id = ? AND claim_id = ?
		`, strings.TrimSpace(item.ClaimType), strings.TrimSpace(item.ClaimText),
			referenceNullable(item.EvidenceExcerpt), strings.TrimSpace(item.TemporalScope),
			strings.TrimSpace(item.KnowledgeScope), item.Confidence,
			strings.TrimSpace(item.WorkID), strings.TrimSpace(item.ID))
	default:
		return ErrInvalidReference
	}
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	return d1ReferenceRowsChanged(affected)
}

// ---------------------------------------------------------------------------
// ReferenceLibraryStore — session bindings and runtime
// ---------------------------------------------------------------------------

// UpsertSessionReferenceBinding creates or advances one session-to-work link.
func (s *d1Store) UpsertSessionReferenceBinding(ctx context.Context, item *SessionReferenceBinding, expectedRevision int64) error {
	if item == nil || expectedRevision < 0 || referenceRequired(item.BindingID, item.ChatSessionID, item.WorkID, item.ContinuityID) != nil {
		return ErrInvalidReference
	}
	if expectedRevision == 0 {
		_, err := s.conn.Exec(ctx, `
			INSERT INTO session_reference_bindings
				(binding_id, chat_session_id, work_id, continuity_id, binding_role, reference_mode, enabled, injection_enabled,
				 anchor_mode, current_node_id, reveal_ceiling_node_id, divergence_node_id,
				 future_policy, priority)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, strings.TrimSpace(item.BindingID), strings.TrimSpace(item.ChatSessionID), strings.TrimSpace(item.WorkID),
			strings.TrimSpace(item.ContinuityID), defaultString(item.BindingRole, "primary"), defaultString(item.ReferenceMode, "supplement"),
			d1BoolValue(item.Enabled), d1BoolValue(item.InjectionEnabled),
			defaultString(item.AnchorMode, "manual"), referenceNullable(item.CurrentNodeID),
			referenceNullable(item.RevealCeilingNodeID), referenceNullable(item.DivergenceNodeID),
			defaultString(item.FuturePolicy, "block"), item.Priority)
		return d1ReferenceStoreError(err)
	}
	affected, err := s.conn.Exec(ctx, `
		UPDATE session_reference_bindings
		SET binding_role = ?, reference_mode = ?, enabled = ?, injection_enabled = ?, anchor_mode = ?, current_node_id = ?,
			reveal_ceiling_node_id = ?, divergence_node_id = ?, future_policy = ?,
			priority = ?, revision = revision + 1, updated_at = `+d1NowExpression+`
		WHERE binding_id = ? AND chat_session_id = ? AND revision = ?
	`, defaultString(item.BindingRole, "primary"), defaultString(item.ReferenceMode, "supplement"),
		d1BoolValue(item.Enabled), d1BoolValue(item.InjectionEnabled), defaultString(item.AnchorMode, "manual"),
		referenceNullable(item.CurrentNodeID), referenceNullable(item.RevealCeilingNodeID),
		referenceNullable(item.DivergenceNodeID), defaultString(item.FuturePolicy, "block"), item.Priority,
		strings.TrimSpace(item.BindingID), strings.TrimSpace(item.ChatSessionID), expectedRevision)
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	return d1ReferenceRevisionChanged(affected)
}

// ListSessionReferenceBindings returns the work links of one session, most
// important first.
func (s *d1Store) ListSessionReferenceBindings(ctx context.Context, chatSessionID string, enabledOnly bool) ([]SessionReferenceBinding, error) {
	if referenceRequired(chatSessionID) != nil {
		return nil, ErrInvalidReference
	}
	query := `SELECT binding_id, chat_session_id, work_id, continuity_id, binding_role, reference_mode,
	                 enabled, injection_enabled, anchor_mode, current_node_id, reveal_ceiling_node_id,
	                 divergence_node_id, future_policy, priority, revision, created_at, updated_at
	          FROM session_reference_bindings WHERE chat_session_id = ?`
	args := []any{strings.TrimSpace(chatSessionID)}
	if enabledOnly {
		// The column is an INTEGER 0/1, so the predicate compares against 1
		// rather than binding a Go bool whose encoding would depend on the
		// transport.
		query += " AND enabled = 1"
	}
	query += " ORDER BY priority DESC, created_at, binding_id"
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []SessionReferenceBinding{}
	for rows.Next() {
		var item SessionReferenceBinding
		var current, reveal, divergence *string
		if err := rows.Scan(&item.BindingID, &item.ChatSessionID, &item.WorkID, &item.ContinuityID,
			&item.BindingRole, &item.ReferenceMode, &item.Enabled, &item.InjectionEnabled, &item.AnchorMode,
			&current, &reveal, &divergence, &item.FuturePolicy, &item.Priority, &item.Revision,
			&item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, err
		}
		item.CurrentNodeID = d1DerefString(current)
		item.RevealCeilingNodeID = d1DerefString(reveal)
		item.DivergenceNodeID = d1DerefString(divergence)
		items = append(items, item)
	}
	return items, rows.Err()
}

// DeleteSessionReferenceBinding removes one session-to-work link.
func (s *d1Store) DeleteSessionReferenceBinding(ctx context.Context, chatSessionID, bindingID string) error {
	if referenceRequired(chatSessionID, bindingID) != nil {
		return ErrInvalidReference
	}
	affected, err := s.conn.Exec(ctx, `
		DELETE FROM session_reference_bindings WHERE chat_session_id = ? AND binding_id = ?
	`, strings.TrimSpace(chatSessionID), strings.TrimSpace(bindingID))
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	return d1ReferenceRowsChanged(affected)
}

// UpsertSessionReferenceRuntime creates or advances the assisted-anchor
// candidate of one binding.
func (s *d1Store) UpsertSessionReferenceRuntime(ctx context.Context, item *SessionReferenceRuntime, expectedRevision int64) error {
	if item == nil || expectedRevision < 0 || referenceRequired(item.BindingID) != nil {
		return ErrInvalidReference
	}
	if expectedRevision == 0 {
		_, err := s.conn.Exec(ctx, `
			INSERT INTO session_reference_runtime
				(binding_id, candidate_node_id, candidate_source_turn, candidate_evidence_json,
				 candidate_confirmed, last_claim_ids_json, diagnostics_json)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		`, strings.TrimSpace(item.BindingID), referenceNullable(item.CandidateNodeID), d1NullablePositiveInt(item.CandidateSourceTurn),
			referenceJSON(item.CandidateEvidenceJSON), d1BoolValue(item.CandidateConfirmed),
			referenceJSON(item.LastClaimIDsJSON), referenceJSON(item.DiagnosticsJSON))
		return d1ReferenceStoreError(err)
	}
	affected, err := s.conn.Exec(ctx, `
		UPDATE session_reference_runtime
		SET candidate_node_id = ?, candidate_source_turn = ?, candidate_evidence_json = ?,
			candidate_confirmed = ?, last_claim_ids_json = ?, diagnostics_json = ?,
			revision = revision + 1, updated_at = `+d1NowExpression+`
		WHERE binding_id = ? AND revision = ?
	`, referenceNullable(item.CandidateNodeID), d1NullablePositiveInt(item.CandidateSourceTurn),
		referenceJSON(item.CandidateEvidenceJSON), d1BoolValue(item.CandidateConfirmed),
		referenceJSON(item.LastClaimIDsJSON), referenceJSON(item.DiagnosticsJSON),
		strings.TrimSpace(item.BindingID), expectedRevision)
	if err != nil {
		return d1ReferenceStoreError(err)
	}
	return d1ReferenceRevisionChanged(affected)
}

// GetSessionReferenceRuntime returns the assisted-anchor candidate of one
// binding, or ErrNotFound.
func (s *d1Store) GetSessionReferenceRuntime(ctx context.Context, bindingID string) (*SessionReferenceRuntime, error) {
	if referenceRequired(bindingID) != nil {
		return nil, ErrInvalidReference
	}
	var item SessionReferenceRuntime
	var candidateNode, evidence, claims, diagnostics *string
	var sourceTurn *int64
	err := s.conn.QueryRow(ctx, `
		SELECT binding_id, candidate_node_id, candidate_source_turn, candidate_evidence_json,
		       candidate_confirmed, last_claim_ids_json, diagnostics_json, revision, created_at, updated_at
		FROM session_reference_runtime WHERE binding_id = ?
	`, strings.TrimSpace(bindingID)).Scan(&item.BindingID, &candidateNode, &sourceTurn, &evidence,
		&item.CandidateConfirmed, &claims, &diagnostics, &item.Revision, &item.CreatedAt, &item.UpdatedAt)
	if errors.Is(err, errD1NoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	item.CandidateNodeID = d1DerefString(candidateNode)
	if sourceTurn != nil {
		item.CandidateSourceTurn = int(*sourceTurn)
	}
	item.CandidateEvidenceJSON = d1DerefString(evidence)
	item.LastClaimIDsJSON = d1DerefString(claims)
	item.DiagnosticsJSON = d1DerefString(diagnostics)
	return &item, nil
}

// ---------------------------------------------------------------------------
// ReferenceCoverageStore
// ---------------------------------------------------------------------------

// ListReferenceEntityAliasesByScope returns every alias of one work/continuity
// scope, which is the alias index the coverage pass compares the active request
// context against. It is ordered by entity then alias id so a caller can diff
// two passes and see the same order.
func (s *d1Store) ListReferenceEntityAliasesByScope(ctx context.Context, workID, continuityID string) ([]ReferenceEntityAlias, error) {
	if referenceRequired(workID, continuityID) != nil {
		return nil, ErrInvalidReference
	}
	rows, err := s.conn.Query(ctx, d1ReferenceAliasSelect+`
		WHERE work_id = ? AND continuity_id = ?
		ORDER BY entity_id, alias_id
	`, strings.TrimSpace(workID), strings.TrimSpace(continuityID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ReferenceEntityAlias{}
	for rows.Next() {
		var item ReferenceEntityAlias
		if err := rows.Scan(&item.AliasID, &item.WorkID, &item.ContinuityID, &item.EntityID,
			&item.AliasText, &item.NormalizedAlias, &item.LanguageCode, &item.CreatedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

const d1ReferenceCoverageSnapshotInsert = `
	INSERT INTO session_reference_coverage_snapshots
		(binding_id, contract_version, context_hash, inventory_hash, snapshot_hash,
		 source_message_count, field_count, covered_field_count, stats_json)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

const d1ReferenceCoverageSnapshotUpdate = `
	UPDATE session_reference_coverage_snapshots
	SET contract_version = ?, context_hash = ?, inventory_hash = ?, snapshot_hash = ?,
	    source_message_count = ?, field_count = ?, covered_field_count = ?,
	    stats_json = ?, revision = revision + 1, updated_at = ` + d1NowExpression + `
	WHERE binding_id = ? AND revision = ?`

const d1ReferenceCoverageFieldDelete = `
	DELETE FROM session_reference_coverage_fields WHERE binding_id = ?`

const d1ReferenceCoverageFieldInsert = `
	INSERT INTO session_reference_coverage_fields
		(binding_id, field_key, work_id, continuity_id, reference_kind, source_id,
		 field_name, field_value, normalized_value, match_values_json,
		 present_in_context, matched_locations_json, eligible, eligibility_reason)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// ReplaceSessionReferenceCoverageSnapshot replaces the field coverage of one
// binding and reports whether anything changed.
//
// The whole replacement is ONE batch: the snapshot header, the deletion of the
// previous field rows, and every new field row. A partial write here would leave
// the header's covered_field_count describing a field set that does not exist,
// and the coverage panel would then report fields that are not there as missing.
//
// The identical-snapshot case returns (false, nil) and writes nothing, including
// the field rows. Rewriting identical fields would look identical in the output
// but would churn every row's created_at, and the reference deliberately treats
// a re-derived identical snapshot as a no-op.
//
// Field validation happens while the batch is assembled, so a malformed field is
// rejected with nothing stored and without a transaction ever being opened.
func (s *d1Store) ReplaceSessionReferenceCoverageSnapshot(ctx context.Context, snapshot *SessionReferenceCoverageSnapshot, fields []SessionReferenceCoverageField) (bool, error) {
	if snapshot == nil || referenceRequired(snapshot.BindingID, snapshot.ContractVersion, snapshot.ContextHash, snapshot.InventoryHash, snapshot.SnapshotHash) != nil {
		return false, ErrInvalidReference
	}
	bindingID := strings.TrimSpace(snapshot.BindingID)
	fieldStatements := make([]D1Statement, 0, len(fields))
	for _, field := range fields {
		if referenceRequired(field.FieldKey, field.WorkID, field.ContinuityID, field.ReferenceKind, field.SourceID, field.FieldName) != nil {
			return false, ErrInvalidReference
		}
		fieldStatements = append(fieldStatements, D1Statement{
			SQL: d1ReferenceCoverageFieldInsert,
			Args: []any{bindingID, strings.TrimSpace(field.FieldKey), strings.TrimSpace(field.WorkID),
				strings.TrimSpace(field.ContinuityID), strings.TrimSpace(field.ReferenceKind),
				strings.TrimSpace(field.SourceID), strings.TrimSpace(field.FieldName), field.FieldValue,
				field.NormalizedValue, referenceJSON(field.MatchValuesJSON), d1BoolValue(field.PresentInContext),
				referenceJSON(field.MatchedLocationsJSON), d1BoolValue(field.Eligible),
				defaultString(field.EligibilityReason, "eligible")},
		})
	}

	d1ReferenceWriteMu.Lock()
	defer d1ReferenceWriteMu.Unlock()

	var existingHash string
	var existingRevision int64
	err := s.conn.QueryRow(ctx, `
		SELECT snapshot_hash, revision
		FROM session_reference_coverage_snapshots
		WHERE binding_id = ?
	`, bindingID).Scan(&existingHash, &existingRevision)
	statements := make([]D1Statement, 0, len(fieldStatements)+2)
	switch {
	case errors.Is(err, errD1NoRows):
		statements = append(statements, D1Statement{
			SQL: d1ReferenceCoverageSnapshotInsert,
			Args: []any{bindingID, strings.TrimSpace(snapshot.ContractVersion), strings.TrimSpace(snapshot.ContextHash),
				strings.TrimSpace(snapshot.InventoryHash), strings.TrimSpace(snapshot.SnapshotHash),
				snapshot.SourceMessageCount, snapshot.FieldCount, snapshot.CoveredFieldCount,
				referenceJSON(snapshot.StatsJSON)},
		})
	case err != nil:
		return false, err
	case strings.TrimSpace(existingHash) == strings.TrimSpace(snapshot.SnapshotHash):
		return false, nil
	default:
		// The revision guard is kept. It is the optimistic lock the reference
		// takes, and dropping it would let two concurrent passes silently
		// interleave their field sets.
		statements = append(statements, D1Statement{
			SQL: d1ReferenceCoverageSnapshotUpdate,
			Args: []any{strings.TrimSpace(snapshot.ContractVersion), strings.TrimSpace(snapshot.ContextHash),
				strings.TrimSpace(snapshot.InventoryHash), strings.TrimSpace(snapshot.SnapshotHash),
				snapshot.SourceMessageCount, snapshot.FieldCount, snapshot.CoveredFieldCount,
				referenceJSON(snapshot.StatsJSON), bindingID, existingRevision},
		})
	}
	statements = append(statements, D1Statement{SQL: d1ReferenceCoverageFieldDelete, Args: []any{bindingID}})
	statements = append(statements, fieldStatements...)
	if err := s.conn.Batch(ctx, statements...); err != nil {
		return false, d1ReferenceStoreError(err)
	}
	return true, nil
}
