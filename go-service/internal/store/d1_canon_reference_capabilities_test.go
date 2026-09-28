package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// D1 Canon Pack and reference-library tests.
//
// Every test here runs against the real SQLite engine through the same D1
// transport the Worker uses, so these assert the statements D1 actually
// executes rather than a mock's idea of them.
//
// The failure modes worth pinning in this lane are structural, not cosmetic:
//
//   - An install that half landed. A pack whose content rows are stored but
//     whose registry entry is not is a download that never resolves, so the
//     install must be one batch and a rejected manifest must store nothing.
//   - An identical pack treated as a new work. The exact-fact identity is what
//     makes a re-installed claim the SAME claim; if the pre-read that resolves
//     it misses, the registry grows a duplicate and the conflict count the
//     diagnostics report climbs for no reason.
//   - An approved projection served from a retired pack. The approved reads
//     narrow by live origin, and dropping that clause is invisible until a user
//     removes a pack and still sees its lore.
//   - A local overlay that does not suppress. The overlay rules are the user's
//     last word over canon material; if a read ignores them, an explicitly
//     suppressed fact keeps being injected.
//   - Coverage that under-reports. The snapshot header, the field rows and the
//     counted totals are one projection; a partial write describes fields that
//     do not exist.

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

// d1CanonSHA is the 64-character lowercase hex digest the canon tables'
// CHECK constraints require. canonHash is the same function the writer uses, so
// a fixture digest and a produced digest are indistinguishable.
func d1CanonSHA(label string) string { return canonHash(label) }

// d1CanonSource is one manifest source entry.
func d1CanonSource(id, hash, title string) map[string]any {
	return map[string]any{
		"id": id, "document_sha256": hash, "retrieved_at": "2026-03-04T05:06:07Z",
		"source_type": "community_wiki", "uri": "https://example.invalid/" + id,
		"title": title, "license": map[string]any{"kind": "cc-by-sa"},
		"access_class": "public",
	}
}

// d1CanonEvidence cites a source with a locator, which is what makes the
// evidence edge's digest stable.
func d1CanonEvidence(sourceID string) []any {
	return []any{map[string]any{"source_id": sourceID, "locator": map[string]any{"page": 7}}}
}

// d1CanonContent is the manifest content block. The record kinds and field
// names are the ones insertCanonContent reads, so a fixture that renames one
// would silently produce an empty pack rather than a failing test.
func d1CanonContent(sourceID string) map[string]any {
	return map[string]any{
		"entities": []any{map[string]any{
			"id": "hero", "review_state": "approved", "continuity_ids": []any{"main"},
			"kind": "person", "name": map[string]any{"text": "Mira"},
			"summary": "the protagonist", "aliases": []any{map[string]any{"text": "Mira K.", "language": "en"}},
			"evidence": d1CanonEvidence(sourceID),
		}},
		"locations": []any{map[string]any{
			"id": "keep", "review_state": "approved", "continuity_ids": []any{"main"},
			"name": map[string]any{"text": "North Keep"}, "evidence": d1CanonEvidence(sourceID),
		}},
		"factions": []any{}, "settings": []any{},
		"events": []any{map[string]any{
			"id": "arrival", "review_state": "approved", "continuity_ids": []any{"main"},
			"name": map[string]any{"text": "Arrival"}, "evidence": d1CanonEvidence(sourceID),
		}},
		"relations": []any{map[string]any{
			"id": "keeps", "review_state": "approved", "continuity_ids": []any{"main"},
			"predicate": "guards", "subject_id": "hero", "object_id": "keep",
			"evidence": d1CanonEvidence(sourceID),
		}},
		"claims": []any{map[string]any{
			"id": "born", "review_state": "approved", "continuity_ids": []any{"main"},
			"claim_type": "origin", "statement": "Mira was born in the North Keep.",
			"subject_ids": []any{"hero"}, "evidence": d1CanonEvidence(sourceID),
		}},
	}
}

// d1CanonManifest builds a complete, valid canon-pack manifest.
func d1CanonManifest(sourceHash string) map[string]any {
	const sourceID = "wiki-1"
	return map[string]any{
		"contract": "canon-pack-manifest.v1",
		"pack":     map[string]any{"id": "pack-alpha", "version": "1.0.0", "status": "complete"},
		"work": map[string]any{
			"stable_id": "stable-work-1", "original_language": "en",
			"continuity_ids":    []any{"main"},
			"original_title":    map[string]any{"text": "The North Keep", "language": "en"},
			"translated_titles": []any{map[string]any{"text": "Le Nord", "language": "fr"}},
			"aliases":           []any{map[string]any{"text": "Keep of the North", "language": "en"}},
			"edition":           map[string]any{"edition_id": "ed-1", "language": "en", "label": "First"},
		},
		"review":          map[string]any{"status": "approved"},
		"trust":           map[string]any{"status": "verified"},
		"coverage_report": map[string]any{"events": 1},
		"conflicts":       []any{},
		"uncertainties":   []any{},
		"sources":         []any{d1CanonSource(sourceID, sourceHash, "North Keep wiki")},
		"content":         d1CanonContent(sourceID),
	}
}

// d1CanonInput encodes a manifest into an install input.
func d1CanonInput(t *testing.T, manifest map[string]any) CanonPackInstallInput {
	t.Helper()
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	return CanonPackInstallInput{
		ManifestJSON:         raw,
		ManifestSHA256:       d1CanonSHA("manifest"),
		ArchiveSHA256:        d1CanonSHA("archive"),
		ValidationReportJSON: []byte(`{"warnings":[]}`),
	}
}

// d1ReferenceSeedWork creates a work and its default continuity directly, so a
// library test does not have to build a pack to get a usable scope.
func d1ReferenceSeedWork(t *testing.T, conn *sqliteD1Conn, workID, title string) (string, string) {
	t.Helper()
	ctx := context.Background()
	if _, err := conn.Exec(ctx, `
		INSERT INTO reference_works (work_id, title, work_type, default_language, status)
		VALUES (?, ?, 'custom', 'en', 'active')`, workID, title); err != nil {
		t.Fatalf("seed work %s: %v", workID, err)
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO reference_continuities (continuity_id, work_id, continuity_key, label, status)
		VALUES (?, ?, 'main', 'Main', 'active')`, workID+"-c1", workID); err != nil {
		t.Fatalf("seed continuity: %v", err)
	}
	return workID, workID + "-c1"
}

// d1ReferenceSeedDocument stores the document a claim must reference, because
// the schema makes a claim's document a real foreign key.
func d1ReferenceSeedDocument(t *testing.T, conn *sqliteD1Conn, workID, continuityID, documentID string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `
		INSERT INTO reference_documents (document_id, work_id, continuity_id, source_type, content_hash, import_status)
		VALUES (?, ?, ?, 'manual_text', ?, 'ready')`, documentID, workID, continuityID, d1CanonSHA(documentID)); err != nil {
		t.Fatalf("seed document %s: %v", documentID, err)
	}
}

// ---------------------------------------------------------------------------
// CanonPackStore
// ---------------------------------------------------------------------------

// TestD1CanonPackInstallCommitsEverythingAsOneOperation pins the whole install:
// the work, the edition, the registry entry, the titles, the source
// observations and every projected item must all exist after one call, because
// a pack that stored content without registering itself would be invisible to
// the catalog and could never be downloaded again.
func TestD1CanonPackInstallCommitsEverythingAsOneOperation(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	install, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("InstallCanonPack: %v", err)
	}
	if install.Contract != CanonPackLifecycleContract || install.LifecycleStatus != "active" {
		t.Errorf("install = %+v, want an active install carrying the lifecycle contract", install)
	}
	if install.PackID != "pack-alpha" || install.PackVersion != "1.0.0" {
		t.Errorf("pack identity = %s@%s", install.PackID, install.PackVersion)
	}
	if install.InstallGeneration != 1 {
		t.Errorf("install generation = %d, want 1 for the first install of a pack version", install.InstallGeneration)
	}
	if install.ActivatedAt == nil {
		t.Error("an install is activated at creation; activated_at must not be NULL")
	}
	if install.ArchiveSHA256 != d1CanonSHA("archive") {
		t.Errorf("archive digest = %q, want the digest read back out of the validation report", install.ArchiveSHA256)
	}
	if install.RecordCounts["entities"] != 2 || install.RecordCounts["timeline"] != 1 || install.RecordCounts["claims"] != 2 {
		t.Errorf("record counts = %+v, want 2 entities, 1 timeline, 2 claims (the relation is a claim too)", install.RecordCounts)
	}

	// The work and edition the pack belongs to must exist, not merely be named.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_works WHERE work_id = ?`, install.WorkID); got != 1 {
		t.Errorf("reference_works rows = %d, want 1", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_work_editions WHERE edition_row_id = ?`, install.EditionRowID); got != 1 {
		t.Errorf("reference_work_editions rows = %d, want 1", got)
	}
	// One source bound to one continuity, plus the document it describes.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_source_observations WHERE install_id = ?`, install.InstallID); got != 1 {
		t.Errorf("source observations = %d, want 1", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_documents`); got != 1 {
		t.Errorf("reference documents = %d, want 1", got)
	}
	// Every projected item carries an origin membership, which is what the
	// approved reads and the traceability diagnostic both count.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_item_origins WHERE install_id = ?`, install.InstallID); got != 5 {
		t.Errorf("item origins = %d, want 5 (2 entities, 1 node, 2 claims)", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_item_evidence`); got != 5 {
		t.Errorf("evidence edges = %d, want one per projected item", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_fact_identities`); got != 2 {
		t.Errorf("fact identities = %d, want 2", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_logical_facts`); got != 2 {
		t.Errorf("logical facts = %d, want 2", got)
	}
	// The entity alias is stored with the same normalisation the search index
	// uses, so a pack and its alias resolve the same way on both providers.
	var normalized string
	if err := conn.QueryRow(ctx, `SELECT normalized_alias FROM reference_entity_aliases`).Scan(&normalized); err != nil {
		t.Fatalf("read alias: %v", err)
	}
	if normalized != canonNormalize("Mira K.") {
		t.Errorf("normalized alias = %q, want %q", normalized, canonNormalize("Mira K."))
	}
}

// TestD1CanonPackInstallRejectsMalformedManifestAndStoresNothing pins the other
// half: a manifest the reference refuses must leave no work, no edition and no
// registry row. A rejected install that still created its work would leave an
// empty work in the library picker forever.
func TestD1CanonPackInstallRejectsMalformedManifestAndStoresNothing(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(map[string]any)
		input   func(CanonPackInstallInput) CanonPackInstallInput
		wantErr error
	}{
		{
			name:    "wrong contract",
			mutate:  func(m map[string]any) { m["contract"] = "canon-pack-manifest.v0" },
			wantErr: ErrInvalidReference,
		},
		{
			name:    "review not approved",
			mutate:  func(m map[string]any) { m["review"] = map[string]any{"status": "pending"} },
			wantErr: ErrInvalidReference,
		},
		{
			name:    "no continuity",
			mutate:  func(m map[string]any) { m["work"].(map[string]any)["continuity_ids"] = []any{} },
			wantErr: ErrInvalidReference,
		},
		{
			name:    "unparseable retrieved_at",
			mutate:  func(m map[string]any) { m["sources"].([]any)[0].(map[string]any)["retrieved_at"] = "yesterday" },
			wantErr: ErrInvalidReference,
		},
		{
			name: "uppercase manifest digest",
			input: func(in CanonPackInstallInput) CanonPackInstallInput {
				in.ManifestSHA256 = strings.ToUpper(in.ManifestSHA256)
				return in
			},
			wantErr: ErrInvalidReference,
		},
		{
			name:    "short archive digest",
			input:   func(in CanonPackInstallInput) CanonPackInstallInput { in.ArchiveSHA256 = "abc"; return in },
			wantErr: ErrInvalidReference,
		},
		{
			// A claim that cites a source the manifest does not declare cannot be
			// grounded, and admitting it would let the pack report items the
			// traceability diagnostic cannot account for.
			name: "claim cites an unknown source",
			mutate: func(m map[string]any) {
				claim := m["content"].(map[string]any)["claims"].([]any)[0].(map[string]any)
				claim["evidence"] = d1CanonEvidence("not-a-source")
			},
			wantErr: ErrInvalidReference,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			ctx := context.Background()
			manifest := d1CanonManifest(d1CanonSHA("doc-1"))
			if tc.mutate != nil {
				tc.mutate(manifest)
			}
			input := d1CanonInput(t, manifest)
			if tc.input != nil {
				input = tc.input(input)
			}
			if _, err := st.InstallCanonPack(ctx, input); !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			for table, want := range map[string]int{
				"reference_works": 0, "reference_work_editions": 0, "canon_pack_installs": 0,
				"reference_continuities": 0, "reference_entities": 0, "reference_claims": 0,
				"reference_item_origins": 0, "reference_item_evidence": 0,
				"reference_source_observations": 0, "reference_work_titles": 0,
			} {
				if got := d1Count(t, conn, `SELECT COUNT(*) FROM `+table); got != want {
					t.Errorf("%s rows = %d, want %d: a refused install must store nothing", table, got, want)
				}
			}
		})
	}
}

// TestD1CanonPackSecondInstallAdvancesGenerationAndDemotesTheFirst pins the
// one-active-install rule and the generation counter. The generation is
// computed inside the batch, so a second install must see generation 2 and the
// first must be demoted ? never two active installs, and never a reused
// generation that the unique index would reject.
func TestD1CanonPackSecondInstallAdvancesGenerationAndDemotesTheFirst(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	first, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("first install: %v", err)
	}
	second, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("second install: %v", err)
	}
	if second.InstallID == first.InstallID {
		t.Fatal("a second install must get its own install id")
	}
	if second.InstallGeneration != 2 {
		t.Errorf("second generation = %d, want 2", second.InstallGeneration)
	}
	if second.WorkID != first.WorkID || second.EditionRowID != first.EditionRowID {
		t.Errorf("the same pack must resolve to the same work/edition: %s/%s then %s/%s",
			first.WorkID, first.EditionRowID, second.WorkID, second.EditionRowID)
	}
	// Exactly one active install may exist for a pack/edition pair. The
	// generated active_generation_marker unique index enforces it, so this is
	// also the assertion that the demote runs before the insert.
	if got := d1Count(t, conn, `
		SELECT COUNT(*) FROM canon_pack_installs WHERE lifecycle_status = 'active'`); got != 1 {
		t.Errorf("active installs = %d, want 1 after a re-install", got)
	}
	var demoted string
	if err := conn.QueryRow(ctx, `SELECT lifecycle_status FROM canon_pack_installs WHERE install_id = ?`,
		first.InstallID).Scan(&demoted); err != nil {
		t.Fatalf("read first lifecycle: %v", err)
	}
	if demoted != "inactive" {
		t.Errorf("first install lifecycle = %q, want inactive", demoted)
	}
	// The work and edition are created once, not per install.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_works`); got != 1 {
		t.Errorf("works = %d, want 1 across two installs of one pack", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_continuities`); got != 1 {
		t.Errorf("continuities = %d, want 1", got)
	}
	// The document is keyed by content hash, so the second install reuses it
	// rather than storing the same bytes twice.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_documents`); got != 1 {
		t.Errorf("documents = %d, want 1: the same content hash must resolve to one document", got)
	}
}

// TestD1CanonPackReinstallCorroboratesTheSameExactFact pins the identity rule
// that keeps the registry stable. The second install carries the same claim, so
// it must resolve to the SAME claim row through its exact fingerprint and only
// upgrade it to approved. Writing a second claim would make the same fact
// readable twice and would show up as a duplicate in the diagnostics.
func TestD1CanonPackReinstallCorroboratesTheSameExactFact(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if _, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1")))); err != nil {
		t.Fatalf("first install: %v", err)
	}
	var firstClaim, firstFact string
	if err := conn.QueryRow(ctx, `SELECT claim_id, logical_fact_id FROM reference_fact_identities
		ORDER BY claim_id LIMIT 1`).Scan(&firstClaim, &firstFact); err != nil {
		t.Fatalf("read first identity: %v", err)
	}
	if _, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1")))); err != nil {
		t.Fatalf("second install: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_claims`); got != 2 {
		t.Errorf("claims = %d, want 2: an identical claim must not become a second row", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_fact_identities`); got != 2 {
		t.Errorf("fact identities = %d, want 2", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_logical_facts`); got != 2 {
		t.Errorf("logical facts = %d, want 2", got)
	}
	var secondClaim, secondFact string
	if err := conn.QueryRow(ctx, `SELECT claim_id, logical_fact_id FROM reference_fact_identities
		ORDER BY claim_id LIMIT 1`).Scan(&secondClaim, &secondFact); err != nil {
		t.Fatalf("read second identity: %v", err)
	}
	if secondClaim != firstClaim || secondFact != firstFact {
		t.Errorf("the exact fingerprint must resolve to the same claim and fact across installs: %s/%s then %s/%s",
			firstClaim, firstFact, secondClaim, secondFact)
	}
	// The second install still records its own origin and evidence for that
	// claim: corroboration is additional provenance, not a replacement.
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_item_origins`); got != 10 {
		t.Errorf("item origins = %d, want 10 across two installs of 5 items", got)
	}
}

// TestD1CanonPackListAndGetInstall pins the install projection: newest first,
// the lifecycle filter, the record counts, and ErrNotFound.
func TestD1CanonPackListAndGetInstall(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	all, err := st.ListCanonPackInstalls(ctx, "")
	if err != nil {
		t.Fatalf("ListCanonPackInstalls: %v", err)
	}
	if all == nil || len(all) != 0 {
		t.Errorf("empty install list = %#v, want a non-nil empty slice", all)
	}
	if _, err := st.GetCanonPackInstall(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing install error = %v, want ErrNotFound", err)
	}

	if _, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1")))); err != nil {
		t.Fatalf("first install: %v", err)
	}
	if _, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1")))); err != nil {
		t.Fatalf("second install: %v", err)
	}

	active, err := st.ListCanonPackInstalls(ctx, "active")
	if err != nil {
		t.Fatalf("ListCanonPackInstalls active: %v", err)
	}
	if len(active) != 1 || active[0].LifecycleStatus != "active" {
		t.Errorf("active installs = %+v, want exactly the newest one", active)
	}
	all, err = st.ListCanonPackInstalls(ctx, "")
	if err != nil {
		t.Fatalf("ListCanonPackInstalls all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("all installs = %d, want 2", len(all))
	}
	// The generation is the final tiebreak, so the newest install leads even
	// when both rows share an updated_at.
	if all[0].InstallGeneration != 2 {
		t.Errorf("first listed generation = %d, want 2", all[0].InstallGeneration)
	}
	removed, err := st.ListCanonPackInstalls(ctx, "removed")
	if err != nil {
		t.Fatalf("ListCanonPackInstalls removed: %v", err)
	}
	if len(removed) != 0 {
		t.Errorf("removed installs = %+v, want none", removed)
	}
}

// TestD1CanonPackLifecycleTransitions pins the four actions and the two
// refusals. A removed install must never be reactivated, and activating one
// install must retire its predecessor in the same batch.
func TestD1CanonPackLifecycleTransitions(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if _, err := st.SetCanonPackLifecycle(ctx, "missing", "activate"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing install error = %v, want ErrNotFound", err)
	}
	if _, err := st.SetCanonPackLifecycle(ctx, "x", "explode"); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("unknown action error = %v, want ErrInvalidReference", err)
	}

	first, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("first install: %v", err)
	}
	second, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("second install: %v", err)
	}

	deactivated, err := st.SetCanonPackLifecycle(ctx, second.InstallID, "deactivate")
	if err != nil {
		t.Fatalf("deactivate: %v", err)
	}
	if deactivated.Contract != CanonPackLifecycleContract || deactivated.LifecycleStatus != "inactive" {
		t.Errorf("deactivate result = %+v", deactivated)
	}
	reactivated, err := st.SetCanonPackLifecycle(ctx, second.InstallID, "activate")
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if reactivated.LifecycleStatus != "active" {
		t.Errorf("activate result = %+v", reactivated)
	}
	if reactivated.PreviousInstallID != "" {
		t.Errorf("no other install is active at this point, so previous_install_id = %q, want empty", reactivated.PreviousInstallID)
	}

	// Activating the demoted predecessor must report and retire the current one.
	rollback, err := st.SetCanonPackLifecycle(ctx, first.InstallID, "rollback")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rollback.PreviousInstallID != second.InstallID {
		t.Errorf("previous install = %q, want %q", rollback.PreviousInstallID, second.InstallID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM canon_pack_installs WHERE lifecycle_status = 'active'`); got != 1 {
		t.Errorf("active installs = %d, want 1 after a rollback", got)
	}

	removed, err := st.SetCanonPackLifecycle(ctx, first.InstallID, "remove")
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if removed.LifecycleStatus != "removed" {
		t.Errorf("remove result = %+v", removed)
	}
	var removedAt *string
	if err := conn.QueryRow(ctx, `SELECT removed_at FROM canon_pack_installs WHERE install_id = ?`,
		first.InstallID).Scan(&removedAt); err != nil {
		t.Fatalf("read removed_at: %v", err)
	}
	if removedAt == nil || *removedAt == "" {
		t.Error("removing an install must stamp removed_at")
	}
	if _, err := st.SetCanonPackLifecycle(ctx, first.InstallID, "activate"); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("reactivating a removed install error = %v, want ErrReferenceConflict", err)
	}
	if _, err := st.SetCanonPackLifecycle(ctx, first.InstallID, "deactivate"); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("deactivating a removed install error = %v, want ErrReferenceConflict", err)
	}
}

// ---------------------------------------------------------------------------
// CanonRegistryStore
// ---------------------------------------------------------------------------

// TestD1CanonRegistrySearchMatchesTitlesAndRanksExactFirst pins the catalog
// search: the LIKE predicate and the exact-title preference both live in the
// statement, because moving either into Go would change which install leads.
func TestD1CanonRegistrySearchMatchesTitlesAndRanksExactFirst(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	install, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("InstallCanonPack: %v", err)
	}
	none, err := st.SearchCanonRegistry(ctx, "   ", 10)
	if !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank query error = %v, want ErrInvalidReference", err)
	}
	if none != nil {
		t.Errorf("blank query result = %#v, want nil", none)
	}

	// The pack registers an original, a translated and an alias title, so one
	// install yields one item carrying every title that matched.
	byPartial, err := st.SearchCanonRegistry(ctx, "keep", 10)
	if err != nil {
		t.Fatalf("search by partial title: %v", err)
	}
	if len(byPartial) != 1 {
		t.Fatalf("partial search = %+v, want one install", byPartial)
	}
	item := byPartial[0]
	if item.InstallID != install.InstallID {
		t.Errorf("search returned install %q, want %q", item.InstallID, install.InstallID)
	}
	if len(item.MatchedTitles) < 2 {
		t.Errorf("matched titles = %v, want the original and the alias", item.MatchedTitles)
	}
	if item.SourceCount != 1 {
		t.Errorf("source count = %d, want 1", item.SourceCount)
	}
	if item.ConflictCount != 0 || item.UncertainCount != 0 {
		t.Errorf("conflict/uncertain counts = %d/%d, want 0/0 for a manifest with none", item.ConflictCount, item.UncertainCount)
	}
	if item.Title != "The North Keep" || item.StableWorkID != "stable-work-1" || item.EditionID != "ed-1" {
		t.Errorf("catalog item identity = %+v", item)
	}
	if !equalStringSlices(item.Continuities, []string{"main"}) {
		t.Errorf("continuities = %v, want [main]", item.Continuities)
	}

	// The exact normalized title outranks a work that merely contains it.
	exact, err := st.SearchCanonRegistry(ctx, "the north keep", 10)
	if err != nil {
		t.Fatalf("search by exact title: %v", err)
	}
	if len(exact) != 1 || exact[0].InstallID != install.InstallID {
		t.Errorf("exact search = %+v, want the installed pack", exact)
	}
	if exact[0].LifecycleStatus != "active" {
		t.Errorf("lifecycle = %q, want active", exact[0].LifecycleStatus)
	}

	// A removed install must not appear in the catalog at all.
	if _, err := st.SetCanonPackLifecycle(ctx, install.InstallID, "remove"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	afterRemove, err := st.SearchCanonRegistry(ctx, "keep", 10)
	if err != nil {
		t.Fatalf("search after remove: %v", err)
	}
	if len(afterRemove) != 0 {
		t.Errorf("a removed install must leave the catalog: %+v", afterRemove)
	}
}

// TestD1CanonPackDiagnosticsCountsItemsNotEdges pins the two grouped counts.
// Each of the five projected items here carries exactly one evidence edge, so
// counting edges and counting items agree; the second install adds a second
// edge per item from a different observation, and that is where an edge-counting
// implementation would report twice as many corroborated items as there are.
func TestD1CanonPackDiagnosticsCountsItemsNotEdges(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	install, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("InstallCanonPack: %v", err)
	}
	diagnostics, err := st.GetCanonPackDiagnostics(ctx, install.InstallID)
	if err != nil {
		t.Fatalf("GetCanonPackDiagnostics: %v", err)
	}
	if diagnostics.Contract != CanonPackDiagnosticsContract {
		t.Errorf("diagnostics contract = %q", diagnostics.Contract)
	}
	if diagnostics.Sources == nil || len(diagnostics.Sources) != 1 {
		t.Fatalf("diagnostic sources = %#v, want one entry", diagnostics.Sources)
	}
	source := diagnostics.Sources[0]
	if source["source_type"] != "community_wiki" || source["access_class"] != "public" {
		t.Errorf("source projection = %+v", source)
	}
	// retrieved_at is a time, not the stored text, so the JSON field has the
	// same shape it has on MariaDB.
	retrieved, ok := source["retrieved_at"].(interface{ Format(string) string })
	if !ok {
		t.Fatalf("retrieved_at = %#v, want a time value rather than raw text", source["retrieved_at"])
	}
	if got := retrieved.Format("2006-01-02T15:04:05Z"); got != "2026-03-04T05:06:07Z" {
		t.Errorf("retrieved_at = %s, want the manifest instant", got)
	}

	traceability, _ := diagnostics.Quality["traceability"].(map[string]any)
	if traceability == nil {
		t.Fatalf("traceability verdict missing: %+v", diagnostics.Quality)
	}
	if traceability["items"].(int) != 5 || traceability["evidence_bound_items"].(int) != 5 {
		t.Errorf("traceability = %+v, want 5 items and 5 evidence-bound items", traceability)
	}
	if traceability["status"] != "satisfied" {
		t.Errorf("a fully evidenced pack must be satisfied, got %v", traceability["status"])
	}
	agreement, _ := diagnostics.Quality["independent_source_agreement"].(map[string]any)
	if agreement["multi_source_items"].(int) != 0 {
		t.Errorf("one observation per item must report 0 multi-source items, got %v", agreement["multi_source_items"])
	}
	connectivity, _ := diagnostics.Quality["entity_connectivity"].(map[string]any)
	if connectivity["claims"].(int) != 2 {
		t.Errorf("claim count = %v, want 2", connectivity["claims"])
	}
	duplicates, _ := diagnostics.Quality["duplicate_and_conflict"].(map[string]any)
	if duplicates["logical_facts"].(int) != 2 {
		t.Errorf("logical fact count = %v, want 2", duplicates["logical_facts"])
	}
	if _, ok := diagnostics.Quality["coverage"]; !ok {
		t.Error("the coverage report must be projected into the quality block")
	}

	// Corroboration is counted per ITEM, and only the claims are corroborated
	// here. An entity or a timeline node gets a fresh per-install id, so its
	// evidence edges stay on one observation; a claim resolves to the SAME claim
	// row through its exact fingerprint, which is what gives it a second
	// observation. Counting edges instead of items would report four here.
	second, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("second install: %v", err)
	}
	corroborated, err := st.GetCanonPackDiagnostics(ctx, second.InstallID)
	if err != nil {
		t.Fatalf("diagnostics for second install: %v", err)
	}
	after, _ := corroborated.Quality["independent_source_agreement"].(map[string]any)
	if after["multi_source_items"].(int) != 2 {
		t.Errorf("multi-source items = %v, want 2 (the two claims), not the four joined evidence edges",
			after["multi_source_items"])
	}
	if after["single_source_or_unassessed_items"].(int) != 3 {
		t.Errorf("single-source items = %v, want 3", after["single_source_or_unassessed_items"])
	}
	edges := d1Count(t, conn, `SELECT COUNT(*) FROM reference_item_evidence`)
	if edges != 10 {
		t.Errorf("evidence edges = %d, want 10 (5 items x 2 observations)", edges)
	}
	if _, err := st.GetCanonPackDiagnostics(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing install diagnostics error = %v, want ErrNotFound", err)
	}
}

// TestD1CanonOverlaySuppressesApprovedProjection pins the overlay contract end
// to end: an approved entity is retrievable, a suppress rule removes it, and a
// conflicting target is refused rather than written across works.
func TestD1CanonOverlaySuppressesApprovedProjection(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	install, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("InstallCanonPack: %v", err)
	}
	before, err := st.ListReferenceEntities(ctx, install.WorkID, "", "approved")
	if err != nil {
		t.Fatalf("ListReferenceEntities: %v", err)
	}
	if len(before) != 2 {
		t.Fatalf("approved entities = %d, want 2", len(before))
	}
	var target string
	for _, entity := range before {
		if entity.CanonicalName == "Mira" {
			target = entity.EntityID
		}
	}
	if target == "" {
		t.Fatal("the pack must register the Mira entity")
	}

	if _, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{WorkID: " ", EditionRowID: " ", TargetKind: "entity"}); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank overlay error = %v, want ErrInvalidReference", err)
	}
	if _, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{
		WorkID: install.WorkID, EditionRowID: install.EditionRowID,
		TargetKind: "entity", TargetID: target, Action: "delete",
	}); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("unknown action error = %v, want ErrInvalidReference", err)
	}
	if _, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{
		WorkID: install.WorkID, EditionRowID: "no-such-edition",
		TargetKind: "entity", TargetID: target, Action: "suppress_for_retrieval",
	}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown edition error = %v, want ErrNotFound", err)
	}

	rule, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{
		WorkID: install.WorkID, EditionRowID: install.EditionRowID,
		TargetKind: "entity", TargetID: target, Action: "suppress_for_retrieval",
		Reason: "user says this is a mistranslation",
	})
	if err != nil {
		t.Fatalf("CreateCanonOverlay: %v", err)
	}
	if rule.Status != "active" || rule.TargetEntityID != target || rule.Action != "suppress_for_retrieval" {
		t.Errorf("overlay rule = %+v", rule)
	}
	if rule.Reason != "user says this is a mistranslation" {
		t.Errorf("overlay reason = %q", rule.Reason)
	}
	if rule.OverlayRuleID == "" {
		t.Error("an overlay must report the id it was stored under")
	}

	// The suppression is what the approved read is for: without it the
	// explicitly retired material keeps being injected.
	after, err := st.ListReferenceEntities(ctx, install.WorkID, "", "approved")
	if err != nil {
		t.Fatalf("ListReferenceEntities after suppression: %v", err)
	}
	if len(after) != 1 {
		t.Errorf("approved entities after suppression = %d, want 1: %+v", len(after), after)
	}
	for _, entity := range after {
		if entity.EntityID == target {
			t.Error("the suppressed entity must not be returned by the approved read")
		}
	}
	// The unfiltered read is unchanged: suppression is a retrieval policy, not
	// a deletion.
	all, err := st.ListReferenceEntities(ctx, install.WorkID, "", "")
	if err != nil {
		t.Fatalf("ListReferenceEntities unfiltered: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("unfiltered entities = %d, want 2", len(all))
	}

	rules, err := st.ListCanonOverlays(ctx, install.WorkID, install.EditionRowID)
	if err != nil {
		t.Fatalf("ListCanonOverlays: %v", err)
	}
	if len(rules) != 1 || rules[0].OverlayRuleID != rule.OverlayRuleID {
		t.Errorf("overlay list = %+v, want the stored rule", rules)
	}
	otherEdition, err := st.ListCanonOverlays(ctx, install.WorkID, "other")
	if err != nil {
		t.Fatalf("ListCanonOverlays other edition: %v", err)
	}
	if otherEdition == nil || len(otherEdition) != 0 {
		t.Errorf("narrowed overlay list = %#v, want a non-nil empty slice", otherEdition)
	}

	// Re-creating the same rule is an upsert, not a duplicate row.
	again, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{
		WorkID: install.WorkID, EditionRowID: install.EditionRowID,
		TargetKind: "entity", TargetID: target, Action: "suppress_for_retrieval",
		Reason: "second opinion",
	})
	if err != nil {
		t.Fatalf("repeat CreateCanonOverlay: %v", err)
	}
	if again.OverlayRuleID != rule.OverlayRuleID {
		t.Errorf("a repeated rule must keep its id: %q then %q", rule.OverlayRuleID, again.OverlayRuleID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_overlay_rules`); got != 1 {
		t.Errorf("overlay rules = %d, want 1 after a repeat", got)
	}
}

// TestD1CanonOverlayRefusesForeignTargets pins the cross-work refusal. An
// overlay is scoped to one work and edition; writing one across works would
// suppress another work's material from a rule the user never saw.
func TestD1CanonOverlayRefusesForeignTargets(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	install, err := st.InstallCanonPack(ctx, d1CanonInput(t, d1CanonManifest(d1CanonSHA("doc-1"))))
	if err != nil {
		t.Fatalf("InstallCanonPack: %v", err)
	}
	otherWork, _ := d1ReferenceSeedWork(t, conn, "work-other", "Other")
	// The overlay resolves its edition FIRST, so the foreign-work case needs a
	// real second work/edition pair to get past that step and fail on the one
	// under test.
	otherEdition := "ed-other"
	if _, err := conn.Exec(ctx, `
		INSERT INTO reference_work_editions
			(edition_row_id, work_id, stable_work_id, edition_id, edition_label, edition_status)
		VALUES (?, ?, 'stable-other', 'ed-other', 'Other', 'active')`,
		otherEdition, otherWork); err != nil {
		t.Fatalf("seed other edition: %v", err)
	}
	entities, err := st.ListReferenceEntities(ctx, install.WorkID, "", "")
	if err != nil {
		t.Fatalf("ListReferenceEntities: %v", err)
	}
	var target string
	for _, entity := range entities {
		target = entity.EntityID
		break
	}
	if target == "" {
		t.Fatalf("the installed pack must register an entity: %+v", entities)
	}

	if _, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{
		WorkID: otherWork, EditionRowID: otherEdition,
		TargetKind: "entity", TargetID: target, Action: "suppress_for_retrieval",
	}); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("cross-work overlay error = %v, want ErrReferenceConflict", err)
	}
	if _, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{
		WorkID: install.WorkID, EditionRowID: otherEdition,
		TargetKind: "entity", TargetID: target, Action: "suppress_for_retrieval",
	}); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("cross-edition overlay error = %v, want ErrReferenceConflict", err)
	}
	if _, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{
		WorkID: install.WorkID, EditionRowID: install.EditionRowID,
		TargetKind: "entity", TargetID: "no-such-entity", Action: "suppress_for_retrieval",
	}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown target error = %v, want ErrNotFound", err)
	}
	// A replacement must be approved, or an unvetted statement would displace
	// canon material.
	if _, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{
		WorkID: install.WorkID, EditionRowID: install.EditionRowID,
		TargetKind: "entity", TargetID: target, Action: "override",
		ReplacementKind: "entity", ReplacementID: "no-such-entity",
	}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown replacement error = %v, want ErrNotFound", err)
	}
	if _, err := st.CreateCanonOverlay(ctx, CanonOverlayInput{
		WorkID: install.WorkID, EditionRowID: install.EditionRowID,
		TargetKind: "entity", TargetID: target, Action: "override",
	}); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("override without a replacement error = %v, want ErrInvalidReference", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_overlay_rules`); got != 0 {
		t.Errorf("a refused overlay must write nothing, rules = %d", got)
	}
}

// ---------------------------------------------------------------------------
// ReferenceLibraryStore
// ---------------------------------------------------------------------------

// TestD1ReferenceWorkLifecycle pins create, read-back, ordering, the revision
// guard and the in-use refusal.
func TestD1ReferenceWorkLifecycle(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()

	if err := st.CreateReferenceWork(ctx, nil); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("nil work error = %v, want ErrInvalidReference", err)
	}
	if err := st.CreateReferenceWork(ctx, &ReferenceWork{WorkID: "w1"}); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank title error = %v, want ErrInvalidReference", err)
	}
	// The defaults are applied, not stored as blanks: an unnamed work_type or
	// status would be indistinguishable from a caller that meant one.
	if err := st.CreateReferenceWork(ctx, &ReferenceWork{WorkID: "w1", Title: "First Work"}); err != nil {
		t.Fatalf("CreateReferenceWork: %v", err)
	}
	// A second work with an explicitly older updated_at, so the recency ordering
	// the list exposes is observable without depending on the clock advancing
	// between two statements.
	if _, err := conn.Exec(ctx, `
		INSERT INTO reference_works (work_id, title, work_type, default_language, status, updated_at)
		VALUES ('w0', 'Older Work', 'custom', 'en', 'active', '2000-01-01T00:00:00.000Z')`); err != nil {
		t.Fatalf("seed older work: %v", err)
	}
	// w1 is pinned to a known past instant for the same reason: asserting that
	// an update MOVES the timestamp cannot be done by comparing two readings
	// taken inside the same millisecond.
	if _, err := conn.Exec(ctx, `
		UPDATE reference_works SET updated_at = '2001-01-01T00:00:00.000Z' WHERE work_id = 'w1'`); err != nil {
		t.Fatalf("pin work timestamp: %v", err)
	}
	got, err := st.GetReferenceWork(ctx, "w1")
	if err != nil {
		t.Fatalf("GetReferenceWork: %v", err)
	}
	if got.WorkType != "custom" || got.Status != "draft" {
		t.Errorf("work defaults = %+v, want custom/draft", got)
	}
	if got.MetadataJSON != "" {
		t.Errorf("a blank metadata must read back as the empty string, got %q", got.MetadataJSON)
	}
	if got.Revision != 1 {
		t.Errorf("revision = %d, want 1", got.Revision)
	}
	if _, err := st.GetReferenceWork(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing work error = %v, want ErrNotFound", err)
	}
	if _, err := st.GetReferenceWork(ctx, " "); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank id error = %v, want ErrInvalidReference", err)
	}

	// updated_at must move on an update, because the list is ordered by it. The
	// seeded w0 carries an explicitly older timestamp, so the assertion is about
	// the ordering the update produced rather than about the clock.
	updated := &ReferenceWork{WorkID: "w1", Title: "First Work Renamed", WorkType: "novel", Status: "active", MetadataJSON: `{"a":1}`}
	if err := st.UpdateReferenceWork(ctx, updated, got.Revision); err != nil {
		t.Fatalf("UpdateReferenceWork: %v", err)
	}
	after, err := st.GetReferenceWork(ctx, "w1")
	if err != nil {
		t.Fatalf("GetReferenceWork after update: %v", err)
	}
	if after.Title != "First Work Renamed" || after.WorkType != "novel" || after.Status != "active" {
		t.Errorf("updated work = %+v", after)
	}
	if after.MetadataJSON != `{"a":1}` {
		t.Errorf("metadata = %q", after.MetadataJSON)
	}
	if after.Revision != got.Revision+1 {
		t.Errorf("revision = %d, want %d", after.Revision, got.Revision+1)
	}
	if !after.UpdatedAt.After(got.UpdatedAt) {
		t.Errorf("updated_at = %s, want it to move past the pre-update %s: the library list is ordered by it",
			after.UpdatedAt, got.UpdatedAt)
	}
	// A stale expected revision is a conflict, and nothing is written.
	if err := st.UpdateReferenceWork(ctx, updated, got.Revision); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("stale revision error = %v, want ErrReferenceConflict", err)
	}
	if err := st.UpdateReferenceWork(ctx, updated, 0); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("zero expected revision error = %v, want ErrInvalidReference", err)
	}

	if err := st.CreateReferenceWork(ctx, &ReferenceWork{WorkID: "w2", Title: "Second Work"}); err != nil {
		t.Fatalf("CreateReferenceWork w2: %v", err)
	}
	all, err := st.ListReferenceWorks(ctx, "", 0)
	if err != nil {
		t.Fatalf("ListReferenceWorks: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("works = %d, want 3", len(all))
	}
	if all[len(all)-1].WorkID != "w0" {
		t.Errorf("the work with the oldest updated_at must sort last: %+v", all)
	}
	active, err := st.ListReferenceWorks(ctx, "active", 1)
	if err != nil {
		t.Fatalf("ListReferenceWorks active: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("status filtered works = %d, want 1", len(active))
	}
	if empty, err := st.ListReferenceWorks(ctx, "no-such-status", 0); err != nil || empty == nil || len(empty) != 0 {
		t.Errorf("filtered empty list = %#v, %v, want a non-nil empty slice", empty, err)
	}

	// A linked work may not be deleted, and the refusal must name the problem.
	if err := st.UpsertReferenceContinuity(ctx, &ReferenceContinuity{
		ContinuityID: "w1-c1", WorkID: "w1", ContinuityKey: "main", Label: "Main",
	}); err != nil {
		t.Fatalf("UpsertReferenceContinuity: %v", err)
	}
	if err := st.UpsertSessionReferenceBinding(ctx, &SessionReferenceBinding{
		BindingID: "b1", ChatSessionID: "s1", WorkID: "w1", ContinuityID: "w1-c1",
		Enabled: true, AnchorMode: "manual", FuturePolicy: "block",
	}, 0); err != nil {
		t.Fatalf("UpsertSessionReferenceBinding: %v", err)
	}
	if err := st.DeleteReferenceWork(ctx, "w1"); !errors.Is(err, ErrReferenceWorkInUse) {
		t.Errorf("delete of a linked work = %v, want ErrReferenceWorkInUse", err)
	}
	if err := st.DeleteReferenceWork(ctx, "w2"); err != nil {
		t.Fatalf("DeleteReferenceWork: %v", err)
	}
	if err := st.DeleteReferenceWork(ctx, "w2"); !errors.Is(err, ErrNotFound) {
		t.Errorf("repeated delete error = %v, want ErrNotFound", err)
	}
}

// TestD1ReferenceLibraryContinuitiesAndDocuments pins the continuity and
// document lanes, including the upsert revision bump and the document status
// transition.
func TestD1ReferenceLibraryContinuitiesAndDocuments(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1ReferenceSeedWork(t, conn, "w1", "Work")

	if err := st.UpsertReferenceContinuity(ctx, &ReferenceContinuity{
		ContinuityID: "c-main", WorkID: "w1", ContinuityKey: "mainline", Label: "Main",
	}); err != nil {
		t.Fatalf("UpsertReferenceContinuity: %v", err)
	}
	if err := st.UpsertReferenceContinuity(ctx, &ReferenceContinuity{
		ContinuityID: "c-alt", WorkID: "w1", ContinuityKey: "alt", Label: "Alternate", Status: "active",
	}); err != nil {
		t.Fatalf("UpsertReferenceContinuity alt: %v", err)
	}
	// Re-upserting the same key is an update of the existing row, not a second
	// one. The (work_id, continuity_key) unique index is what decides which row
	// is updated, so the fixture uses a key the store did not already seed.
	if err := st.UpsertReferenceContinuity(ctx, &ReferenceContinuity{
		ContinuityID: "c-main", WorkID: "w1", ContinuityKey: "mainline", Label: "Main Line",
	}); err != nil {
		t.Fatalf("repeat UpsertReferenceContinuity: %v", err)
	}
	continuities, err := st.ListReferenceContinuities(ctx, "w1")
	if err != nil {
		t.Fatalf("ListReferenceContinuities: %v", err)
	}
	if len(continuities) != 3 {
		t.Fatalf("continuities = %d, want 3 (the seeded one plus two upserts)", len(continuities))
	}
	// Ordered by label, so "Alternate" leads.
	if continuities[0].Label != "Alternate" {
		t.Errorf("continuity order = %+v, want label order", continuities)
	}
	var label string
	var revision int64
	if err := conn.QueryRow(ctx, `SELECT label, revision FROM reference_continuities WHERE continuity_id = 'c-main'`).
		Scan(&label, &revision); err != nil {
		t.Fatalf("read continuity: %v", err)
	}
	if label != "Main Line" || revision != 2 {
		t.Errorf("continuity = %q revision %d, want Main Line revision 2", label, revision)
	}

	doc := &ReferenceDocument{
		DocumentID: "d1", WorkID: "w1", ContinuityID: "w1-c1",
		SourceType: "manual_text", ContentHash: d1CanonSHA("doc-1"),
	}
	if err := st.SaveReferenceDocument(ctx, doc); err != nil {
		t.Fatalf("SaveReferenceDocument: %v", err)
	}
	// Defaults: raw retention full, import pending.
	fetched, err := st.GetReferenceDocument(ctx, "d1")
	if err != nil {
		t.Fatalf("GetReferenceDocument: %v", err)
	}
	if fetched.RawRetention != "full" || fetched.ImportStatus != "pending" {
		t.Errorf("document defaults = %+v", fetched)
	}
	if _, err := st.GetReferenceDocument(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing document error = %v, want ErrNotFound", err)
	}

	// The raw text is attached under a forced full-retention update.
	doc.SourceType = "community_wiki"
	doc.SourceURI = "https://example.invalid/d1"
	doc.RawText = "the body of the document"
	doc.ProvenanceJSON = `{"via":"test"}`
	if err := st.UpdateReferenceDocumentSource(ctx, doc); err != nil {
		t.Fatalf("UpdateReferenceDocumentSource: %v", err)
	}
	after, err := st.GetReferenceDocument(ctx, "d1")
	if err != nil {
		t.Fatalf("GetReferenceDocument after source: %v", err)
	}
	if after.RawText != "the body of the document" || after.SourceType != "community_wiki" {
		t.Errorf("document after source update = %+v", after)
	}
	if after.RawRetention != "full" {
		t.Errorf("raw retention = %q, want full", after.RawRetention)
	}
	if after.ProvenanceJSON != `{"via":"test"}` {
		t.Errorf("provenance = %q", after.ProvenanceJSON)
	}
	// A mismatched content hash must not match the row.
	stale := *doc
	stale.ContentHash = d1CanonSHA("other")
	if err := st.UpdateReferenceDocumentSource(ctx, &stale); !errors.Is(err, ErrNotFound) {
		t.Errorf("content hash mismatch error = %v, want ErrNotFound", err)
	}
	if err := st.UpdateReferenceDocumentStatus(ctx, "d1", "ready"); err != nil {
		t.Fatalf("UpdateReferenceDocumentStatus: %v", err)
	}
	if err := st.UpdateReferenceDocumentStatus(ctx, "missing", "ready"); !errors.Is(err, ErrNotFound) {
		t.Errorf("status of a missing document = %v, want ErrNotFound", err)
	}
	docs, err := st.ListReferenceDocuments(ctx, "w1", "w1-c1", "ready")
	if err != nil {
		t.Fatalf("ListReferenceDocuments: %v", err)
	}
	if len(docs) != 1 || docs[0].DocumentID != "d1" {
		t.Errorf("filtered documents = %+v", docs)
	}
	none, err := st.ListReferenceDocuments(ctx, "w1", "w1-c1", "pending")
	if err != nil {
		t.Fatalf("ListReferenceDocuments pending: %v", err)
	}
	if none == nil || len(none) != 0 {
		t.Errorf("filtered empty documents = %#v, want a non-nil empty slice", none)
	}
	if err := st.DeleteReferenceDocument(ctx, "d1"); err != nil {
		t.Fatalf("DeleteReferenceDocument: %v", err)
	}
	if err := st.DeleteReferenceDocument(ctx, "d1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("repeated document delete = %v, want ErrNotFound", err)
	}
}

// TestD1ReferenceTimelineFrozenAfterApprovalAndOrdering pins the guard that
// makes an approved node immutable, the normalisation pass, and the refusal of
// a partial order.
func TestD1ReferenceTimelineFrozenAfterApprovalAndOrdering(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1ReferenceSeedWork(t, conn, "w1", "Work")

	seeds := []struct {
		id       string
		key      string
		label    string
		ordinal  int64
		approved bool
	}{
		{"n3", "k3", "Third", 5, true},
		{"n1", "k1", "First", 30, true},
		{"n2", "k2", "Second", 20, false},
	}
	for _, seed := range seeds {
		status := "pending"
		if seed.approved {
			status = "approved"
		}
		if err := st.UpsertReferenceTimelineNode(ctx, &ReferenceTimelineNode{
			NodeID: seed.id, WorkID: "w1", ContinuityID: "w1-c1", NodeKey: seed.key,
			Label: seed.label, Ordinal: seed.ordinal, ReviewStatus: status,
		}); err != nil {
			t.Fatalf("seed node %s: %v", seed.id, err)
		}
	}

	// A pending node is editable by the same key.
	if err := st.UpsertReferenceTimelineNode(ctx, &ReferenceTimelineNode{
		NodeID: "n2", WorkID: "w1", ContinuityID: "w1-c1", NodeKey: "k2",
		Label: "Second, corrected", Ordinal: 25, ReviewStatus: "pending",
	}); err != nil {
		t.Fatalf("update pending node: %v", err)
	}
	// An approved node is frozen: a re-extraction must not rewrite it.
	if err := st.UpsertReferenceTimelineNode(ctx, &ReferenceTimelineNode{
		NodeID: "n1", WorkID: "w1", ContinuityID: "w1-c1", NodeKey: "k1",
		Label: "Machine rewrite", Ordinal: 1, ReviewStatus: "pending",
	}); err != nil {
		t.Fatalf("rewrite approved node: %v", err)
	}
	nodes, err := st.ListReferenceTimelineNodes(ctx, "w1", "w1-c1", "")
	if err != nil {
		t.Fatalf("ListReferenceTimelineNodes: %v", err)
	}
	byID := map[string]ReferenceTimelineNode{}
	for _, node := range nodes {
		byID[node.NodeID] = node
	}
	if byID["n1"].Label != "First" {
		t.Errorf("an approved node must not be rewritten by a re-extraction: %+v", byID["n1"])
	}
	if byID["n1"].Ordinal != 30 {
		t.Errorf("an approved node must keep its ordinal: %d", byID["n1"].Ordinal)
	}
	if byID["n2"].Label != "Second, corrected" || byID["n2"].Ordinal != 25 {
		t.Errorf("a pending node must accept the re-extraction: %+v", byID["n2"])
	}

	// Normalisation numbers the approved set 10, 20 in stored-ordinal order.
	normalized, err := st.NormalizeReferenceTimelineOrder(ctx, "w1", "w1-c1")
	if err != nil {
		t.Fatalf("NormalizeReferenceTimelineOrder: %v", err)
	}
	if normalized != 2 {
		t.Errorf("normalized count = %d, want 2 (the approved nodes only)", normalized)
	}
	// n3 (ordinal 5) precedes n1 (ordinal 30), so n3 becomes 10 and n1 becomes 20.
	normalizedOrdinals := d1ReferenceOrdinalsByNode(t, conn)
	if normalizedOrdinals["n3"] != 10 || normalizedOrdinals["n1"] != 20 {
		t.Errorf("normalised ordinals = %v, want n3=10 n1=20 in stored order", normalizedOrdinals)
	}
	// A second normalise is a no-op: the order is already the numbering.
	again, err := st.NormalizeReferenceTimelineOrder(ctx, "w1", "w1-c1")
	if err != nil {
		t.Fatalf("repeat NormalizeReferenceTimelineOrder: %v", err)
	}
	if again != 2 {
		t.Errorf("repeat normalised count = %d, want 2", again)
	}
	if repeat := d1ReferenceOrdinalsByNode(t, conn); repeat["n3"] != 10 || repeat["n1"] != 20 {
		t.Errorf("a repeated normalise must not reshuffle: %v", repeat)
	}
	if empty, err := st.NormalizeReferenceTimelineOrder(ctx, "w1", "no-such"); err != nil || empty != 0 {
		t.Errorf("normalising an empty continuity = %d, %v, want 0, nil", empty, err)
	}

	// A partial order is refused: it would leave the omitted node stale.
	if _, err := st.ApplyReferenceTimelineOrder(ctx, "w1", "w1-c1", []string{"n1"}); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("partial order error = %v, want ErrReferenceConflict", err)
	}
	if _, err := st.ApplyReferenceTimelineOrder(ctx, "w1", "w1-c1", []string{"n1", "n1"}); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("repeated id order error = %v, want ErrReferenceConflict", err)
	}
	if _, err := st.ApplyReferenceTimelineOrder(ctx, "w1", "w1-c1", []string{"n1", "n2"}); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("unapproved id order error = %v, want ErrReferenceConflict", err)
	}
	if _, err := st.ApplyReferenceTimelineOrder(ctx, "w1", "w1-c1", nil); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("empty order error = %v, want ErrInvalidReference", err)
	}

	applied, err := st.ApplyReferenceTimelineOrder(ctx, "w1", "w1-c1", []string{"n1", "n3"})
	if err != nil {
		t.Fatalf("ApplyReferenceTimelineOrder: %v", err)
	}
	if applied != 2 {
		t.Errorf("applied count = %d, want 2", applied)
	}
	appliedOrdinals := d1ReferenceOrdinalsByNode(t, conn)
	if appliedOrdinals["n1"] != 10 || appliedOrdinals["n3"] != 20 {
		t.Errorf("applied ordinals = %v, want n1=10 n3=20", appliedOrdinals)
	}
}

// d1ReferenceOrdinalsByNode reads the approved ordinal of every timeline node,
// keyed by node id, so an ordering assertion is about the numbering rather than
// about the order the assertion happens to read in.
func d1ReferenceOrdinalsByNode(t *testing.T, conn *sqliteD1Conn) map[string]int64 {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT node_id, ordinal_value FROM reference_timeline_nodes WHERE review_status = 'approved'`)
	if err != nil {
		t.Fatalf("read ordinals: %v", err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var id string
		var value int64
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("scan ordinal: %v", err)
		}
		out[id] = value
	}
	return out
}

// TestD1ReferenceEntitiesAliasesAndClaims pins the entity alias index, the
// claim knower projection, and the user-edit path that clears
// evidence_grounded.
func TestD1ReferenceEntitiesAliasesAndClaims(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1ReferenceSeedWork(t, conn, "w1", "Work")
	d1ReferenceSeedDocument(t, conn, "w1", "w1-c1", "d1")
	if _, err := conn.Exec(ctx, `
		INSERT INTO reference_entities (entity_id, work_id, continuity_id, entity_type, canonical_name, review_status)
		VALUES ('e1', 'w1', 'w1-c1', 'person', 'Mira', 'approved')`); err != nil {
		t.Fatalf("seed entity: %v", err)
	}

	alias := &ReferenceEntityAlias{
		WorkID: "w1", ContinuityID: "w1-c1", EntityID: "e1",
		AliasText: "Mira K.", NormalizedAlias: "mira k.", LanguageCode: "en",
	}
	if err := st.UpsertReferenceEntityAlias(ctx, alias); err != nil {
		t.Fatalf("UpsertReferenceEntityAlias: %v", err)
	}
	if alias.AliasID == 0 {
		t.Error("an alias must report the row id it was stored under")
	}
	// The same normalized alias resolves to the same row, not a duplicate.
	repeat := &ReferenceEntityAlias{
		WorkID: "w1", ContinuityID: "w1-c1", EntityID: "e1",
		AliasText: "Mira Kestral", NormalizedAlias: "mira k.", LanguageCode: "en",
	}
	if err := st.UpsertReferenceEntityAlias(ctx, repeat); err != nil {
		t.Fatalf("repeat UpsertReferenceEntityAlias: %v", err)
	}
	if repeat.AliasID != alias.AliasID {
		t.Errorf("a repeated alias must resolve to the same row: %d then %d", alias.AliasID, repeat.AliasID)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_entity_aliases`); got != 1 {
		t.Errorf("aliases = %d, want 1", got)
	}
	aliases, err := st.ListReferenceEntityAliases(ctx, "e1")
	if err != nil {
		t.Fatalf("ListReferenceEntityAliases: %v", err)
	}
	if len(aliases) != 1 || aliases[0].AliasText != "Mira Kestral" {
		t.Errorf("aliases = %+v, want the refreshed text", aliases)
	}
	byScope, err := st.ListReferenceEntityAliasesByScope(ctx, "w1", "w1-c1")
	if err != nil {
		t.Fatalf("ListReferenceEntityAliasesByScope: %v", err)
	}
	if len(byScope) != 1 || byScope[0].EntityID != "e1" {
		t.Errorf("scoped aliases = %+v", byScope)
	}
	if _, err := st.ListReferenceEntityAliasesByScope(ctx, "w1", ""); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank continuity error = %v, want ErrInvalidReference", err)
	}
	none, err := st.ListReferenceEntityAliasesByScope(ctx, "w1", "no-such")
	if err != nil {
		t.Fatalf("ListReferenceEntityAliasesByScope empty: %v", err)
	}
	if none == nil || len(none) != 0 {
		t.Errorf("empty scope aliases = %#v, want a non-nil empty slice", none)
	}

	claim := &ReferenceClaim{
		ClaimID: "cl1", WorkID: "w1", ContinuityID: "w1-c1", DocumentID: "d1",
		ClaimType: "origin", ClaimText: "Mira was born in the North Keep.",
		SubjectEntityID: "e1", Confidence: 0.9, ReviewStatus: "approved",
		MetadataJSON: `{"evidence_grounded":true}`,
	}
	if err := st.UpsertReferenceClaim(ctx, claim); err != nil {
		t.Fatalf("UpsertReferenceClaim: %v", err)
	}
	if err := st.ReplaceReferenceClaimKnowers(ctx, "cl1", []string{"e1", "e1", "  "}); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank knower error = %v, want ErrInvalidReference", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_claim_knowers`); got != 0 {
		t.Errorf("a refused knower replacement must write nothing, rows = %d", got)
	}
	if err := st.ReplaceReferenceClaimKnowers(ctx, "cl1", []string{"e1", "e1"}); err != nil {
		t.Fatalf("ReplaceReferenceClaimKnowers: %v", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM reference_claim_knowers`); got != 1 {
		t.Errorf("knowers = %d, want 1 after duplicate collapsing", got)
	}
	claims, err := st.ListReferenceClaims(ctx, "w1", "w1-c1", "approved", "")
	if err != nil {
		t.Fatalf("ListReferenceClaims: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("approved claims = %d, want 1", len(claims))
	}
	if !equalStringSlices(claims[0].KnowerEntityIDs, []string{"e1"}) {
		t.Errorf("knowers = %v, want [e1]", claims[0].KnowerEntityIDs)
	}
	if claims[0].TemporalScope != "bounded" || claims[0].BranchKey != "main" || claims[0].KnowledgeScope != "public_world" {
		t.Errorf("claim defaults = %+v", claims[0])
	}

	// A user edit rewrites the claim AND clears the evidence-grounded flag. A
	// stored 0 instead of a JSON false would be a different value entirely.
	if err := st.UpdateReferenceLibraryItem(ctx, &ReferenceLibraryItemUpdate{
		WorkID: "w1", Kind: "claim", ID: "cl1", ClaimType: "origin",
		ClaimText: "Mira was born in the South Keep.", TemporalScope: "bounded",
		KnowledgeScope: "public_world", Confidence: 0.5,
	}); err != nil {
		t.Fatalf("UpdateReferenceLibraryItem claim: %v", err)
	}
	var metadata string
	if err := conn.QueryRow(ctx, `SELECT metadata_json FROM reference_claims WHERE claim_id = 'cl1'`).
		Scan(&metadata); err != nil {
		t.Fatalf("read claim metadata: %v", err)
	}
	if !strings.Contains(metadata, `"evidence_grounded":false`) {
		t.Errorf("claim metadata = %s, want a JSON false for evidence_grounded", metadata)
	}
	if strings.Contains(metadata, `:0`) {
		t.Errorf("claim metadata = %s, must not store the integer 0 in place of a JSON boolean", metadata)
	}
	if err := st.UpdateReferenceLibraryItem(ctx, &ReferenceLibraryItemUpdate{
		WorkID: "w1", Kind: "claim", ID: "missing", ClaimType: "origin",
		ClaimText: "x", TemporalScope: "bounded", KnowledgeScope: "public_world",
	}); !errors.Is(err, ErrNotFound) {
		t.Errorf("edit of a missing claim = %v, want ErrNotFound", err)
	}
	if err := st.UpdateReferenceLibraryItem(ctx, &ReferenceLibraryItemUpdate{
		WorkID: "w1", Kind: "nonsense", ID: "cl1",
	}); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("unknown kind error = %v, want ErrInvalidReference", err)
	}

	// The review transition and its stamp.
	if err := st.UpdateReferenceCandidateReview(ctx, "w1", "claim", "cl1", "rejected", "user", "contradicted"); err != nil {
		t.Fatalf("UpdateReferenceCandidateReview: %v", err)
	}
	if err := st.UpdateReferenceCandidateReview(ctx, "w1", "claim", "cl1", "maybe", "user", ""); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("unknown status error = %v, want ErrInvalidReference", err)
	}
	if err := st.UpdateReferenceCandidateReview(ctx, "w1", "nonsense", "cl1", "approved", "user", ""); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("unknown kind error = %v, want ErrInvalidReference", err)
	}
	if err := st.UpdateReferenceCandidateReview(ctx, "w1", "claim", "missing", "approved", "user", ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("review of a missing claim = %v, want ErrNotFound", err)
	}
	var reviewedAt *string
	if err := conn.QueryRow(ctx, `SELECT reviewed_at FROM reference_claims WHERE claim_id = 'cl1'`).
		Scan(&reviewedAt); err != nil {
		t.Fatalf("read reviewed_at: %v", err)
	}
	if reviewedAt == nil || *reviewedAt == "" {
		t.Error("a review transition must stamp reviewed_at")
	}
}

// TestD1ReferenceBindingAndRuntime pins the optimistic-revision path for the
// session-scoped rows, the enabled-only filter and the ordering.
func TestD1ReferenceBindingAndRuntime(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1ReferenceSeedWork(t, conn, "w1", "Work")

	if err := st.UpsertSessionReferenceBinding(ctx, nil, 0); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("nil binding error = %v, want ErrInvalidReference", err)
	}
	if err := st.UpsertSessionReferenceBinding(ctx, &SessionReferenceBinding{
		BindingID: "b1", ChatSessionID: "s1", WorkID: "w1", ContinuityID: "w1-c1",
		Enabled: true, InjectionEnabled: true, AnchorMode: "manual", FuturePolicy: "block", Priority: 5,
	}, 0); err != nil {
		t.Fatalf("UpsertSessionReferenceBinding: %v", err)
	}
	if err := st.UpsertSessionReferenceBinding(ctx, &SessionReferenceBinding{
		BindingID: "b2", ChatSessionID: "s1", WorkID: "w1", ContinuityID: "w1-c1",
		Enabled: false, AnchorMode: "manual", FuturePolicy: "block", Priority: 9,
	}, 0); !errors.Is(err, ErrReferenceConflict) {
		// The same (session, work, continuity) triple is unique, so a second
		// binding for it is a duplicate, not a conflict on the PK.
		t.Logf("second binding error = %v", err)
	}
	bindings, err := st.ListSessionReferenceBindings(ctx, "s1", false)
	if err != nil {
		t.Fatalf("ListSessionReferenceBindings: %v", err)
	}
	if len(bindings) != 1 {
		t.Fatalf("bindings = %d, want 1", len(bindings))
	}
	if !bindings[0].Enabled || !bindings[0].InjectionEnabled {
		t.Errorf("boolean columns did not round-trip: %+v", bindings[0])
	}
	if bindings[0].BindingRole != "primary" || bindings[0].ReferenceMode != "supplement" {
		t.Errorf("binding defaults = %+v", bindings[0])
	}
	enabled, err := st.ListSessionReferenceBindings(ctx, "s1", true)
	if err != nil {
		t.Fatalf("ListSessionReferenceBindings enabled: %v", err)
	}
	if len(enabled) != 1 {
		t.Errorf("enabled bindings = %d, want 1", len(enabled))
	}
	empty, err := st.ListSessionReferenceBindings(ctx, "no-such-session", true)
	if err != nil {
		t.Fatalf("ListSessionReferenceBindings empty: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Errorf("empty bindings = %#v, want a non-nil empty slice", empty)
	}

	// The update path is guarded by expectedRevision.
	updated := bindings[0]
	updated.AnchorMode = "auto"
	if err := st.UpsertSessionReferenceBinding(ctx, &updated, bindings[0].Revision); err != nil {
		t.Fatalf("UpdateSessionReferenceBinding: %v", err)
	}
	if err := st.UpsertSessionReferenceBinding(ctx, &updated, bindings[0].Revision); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("stale binding revision error = %v, want ErrReferenceConflict", err)
	}
	reloaded, err := st.ListSessionReferenceBindings(ctx, "s1", false)
	if err != nil {
		t.Fatalf("ListSessionReferenceBindings after update: %v", err)
	}
	if reloaded[0].AnchorMode != "auto" {
		t.Errorf("anchor mode = %q, want auto", reloaded[0].AnchorMode)
	}

	if _, err := st.GetSessionReferenceRuntime(ctx, "b1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing runtime error = %v, want ErrNotFound", err)
	}
	// The candidate node is a real foreign key, so the runtime cannot point at
	// a timeline node that does not exist.
	runtime := &SessionReferenceRuntime{
		BindingID: "b1", CandidateNodeID: "n1", CandidateSourceTurn: 4,
		CandidateEvidenceJSON: `{"turn":4}`, CandidateConfirmed: true,
		LastClaimIDsJSON: `["cl1"]`, DiagnosticsJSON: `{"note":"ok"}`,
	}
	if err := st.UpsertSessionReferenceRuntime(ctx, runtime, 0); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("runtime naming an unknown node = %v, want a domain error", err)
	}
	if err := st.UpsertReferenceTimelineNode(ctx, &ReferenceTimelineNode{
		NodeID: "n1", WorkID: "w1", ContinuityID: "w1-c1", NodeKey: "k1", Label: "Anchor",
		ReviewStatus: "pending",
	}); err != nil {
		t.Fatalf("seed timeline node: %v", err)
	}
	if err := st.UpsertSessionReferenceRuntime(ctx, runtime, 0); err != nil {
		t.Fatalf("UpsertSessionReferenceRuntime: %v", err)
	}
	got, err := st.GetSessionReferenceRuntime(ctx, "b1")
	if err != nil {
		t.Fatalf("GetSessionReferenceRuntime: %v", err)
	}
	if got.CandidateNodeID != "n1" || got.CandidateSourceTurn != 4 || !got.CandidateConfirmed {
		t.Errorf("runtime = %+v", got)
	}
	if got.LastClaimIDsJSON != `["cl1"]` {
		t.Errorf("last claim ids = %q", got.LastClaimIDsJSON)
	}
	// A non-positive source turn is stored as NULL, not as 0, so "no turn" and
	// "turn zero" stay distinguishable.
	runtime.CandidateSourceTurn = 0
	runtime.CandidateNodeID = ""
	if err := st.UpsertSessionReferenceRuntime(ctx, runtime, got.Revision); err != nil {
		t.Fatalf("UpdateSessionReferenceRuntime: %v", err)
	}
	cleared, err := st.GetSessionReferenceRuntime(ctx, "b1")
	if err != nil {
		t.Fatalf("GetSessionReferenceRuntime after update: %v", err)
	}
	if cleared.CandidateSourceTurn != 0 || cleared.CandidateNodeID != "" {
		t.Errorf("cleared runtime = %+v", cleared)
	}
	if err := st.UpsertSessionReferenceRuntime(ctx, runtime, got.Revision); !errors.Is(err, ErrReferenceConflict) {
		t.Errorf("stale runtime revision error = %v, want ErrReferenceConflict", err)
	}

	if err := st.DeleteSessionReferenceBinding(ctx, "s1", "b1"); err != nil {
		t.Fatalf("DeleteSessionReferenceBinding: %v", err)
	}
	if err := st.DeleteSessionReferenceBinding(ctx, "s1", "b1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("repeated binding delete = %v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// ReferenceCoverageStore
// ---------------------------------------------------------------------------

// TestD1ReferenceCoverageSnapshotReplacesAsOneProjection pins the coverage
// write. The header, the field delete and the field inserts are one projection:
// a partial write would leave covered_field_count describing a field set that
// does not exist, which is the under-report this read exists to prevent.
func TestD1ReferenceCoverageSnapshotReplacesAsOneProjection(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1ReferenceSeedWork(t, conn, "w1", "Work")
	if _, err := conn.Exec(ctx, `
		INSERT INTO session_reference_bindings (binding_id, chat_session_id, work_id, continuity_id, enabled)
		VALUES ('b1', 's1', 'w1', 'w1-c1', 1)`); err != nil {
		t.Fatalf("seed binding: %v", err)
	}

	snapshot := &SessionReferenceCoverageSnapshot{
		BindingID: "b1", ContractVersion: "reference_coverage.v1",
		ContextHash: d1CanonSHA("ctx-1"), InventoryHash: d1CanonSHA("inv-1"),
		SnapshotHash: d1CanonSHA("snap-1"), SourceMessageCount: 6, FieldCount: 2,
		CoveredFieldCount: 1, StatsJSON: `{"matched":1}`,
	}
	fields := []SessionReferenceCoverageField{
		{BindingID: "b1", FieldKey: "f1", WorkID: "w1", ContinuityID: "w1-c1",
			ReferenceKind: "entity", SourceID: "e1", FieldName: "canonical_name",
			FieldValue: "Mira", NormalizedValue: "mira", MatchValuesJSON: `["mira"]`,
			PresentInContext: true, MatchedLocationsJSON: `[{"turn":3}]`, Eligible: true},
		{BindingID: "b1", FieldKey: "f2", WorkID: "w1", ContinuityID: "w1-c1",
			ReferenceKind: "entity", SourceID: "e2", FieldName: "canonical_name",
			FieldValue: "North Keep", NormalizedValue: "north keep", Eligible: true,
			EligibilityReason: "not in context"},
	}
	changed, err := st.ReplaceSessionReferenceCoverageSnapshot(ctx, snapshot, fields)
	if err != nil {
		t.Fatalf("ReplaceSessionReferenceCoverageSnapshot: %v", err)
	}
	if !changed {
		t.Error("the first snapshot must report that it changed the projection")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_reference_coverage_fields WHERE binding_id = 'b1'`); got != 2 {
		t.Errorf("coverage fields = %d, want 2", got)
	}
	var eligibilityReason string
	if err := conn.QueryRow(ctx, `SELECT eligibility_reason FROM session_reference_coverage_fields WHERE field_key = 'f1'`).
		Scan(&eligibilityReason); err != nil {
		t.Fatalf("read eligibility reason: %v", err)
	}
	if eligibilityReason != "eligible" {
		t.Errorf("eligibility reason = %q, want the eligible default", eligibilityReason)
	}

	// The identical snapshot is a no-op: nothing is rewritten.
	repeat, err := st.ReplaceSessionReferenceCoverageSnapshot(ctx, snapshot, fields)
	if err != nil {
		t.Fatalf("identical snapshot: %v", err)
	}
	if repeat {
		t.Error("an identical snapshot must report no change")
	}

	// A different snapshot replaces the field set rather than appending to it.
	next := *snapshot
	next.SnapshotHash = d1CanonSHA("snap-2")
	next.InventoryHash = d1CanonSHA("inv-2")
	next.CoveredFieldCount = 2
	changed, err = st.ReplaceSessionReferenceCoverageSnapshot(ctx, &next, fields[:1])
	if err != nil {
		t.Fatalf("changed snapshot: %v", err)
	}
	if !changed {
		t.Error("a different snapshot hash must report that it changed the projection")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_reference_coverage_fields WHERE binding_id = 'b1'`); got != 1 {
		t.Errorf("coverage fields after replacement = %d, want 1: the previous set must be replaced, not added to", got)
	}
	var revision int64
	var covered int
	if err := conn.QueryRow(ctx, `SELECT revision, covered_field_count FROM session_reference_coverage_snapshots
		WHERE binding_id = 'b1'`).Scan(&revision, &covered); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if revision != 2 {
		t.Errorf("snapshot revision = %d, want 2", revision)
	}
	if covered != 2 {
		t.Errorf("covered field count = %d, want 2", covered)
	}
}

// TestD1ReferenceCoverageSnapshotRefusesMalformedInput pins that a rejected
// field stores nothing at all, including the snapshot header.
func TestD1ReferenceCoverageSnapshotRefusesMalformedInput(t *testing.T) {
	st, conn := newD1TestStore(t)
	ctx := context.Background()
	d1ReferenceSeedWork(t, conn, "w1", "Work")
	if _, err := conn.Exec(ctx, `
		INSERT INTO session_reference_bindings (binding_id, chat_session_id, work_id, continuity_id, enabled)
		VALUES ('b1', 's1', 'w1', 'w1-c1', 1)`); err != nil {
		t.Fatalf("seed binding: %v", err)
	}

	if _, err := st.ReplaceSessionReferenceCoverageSnapshot(ctx, nil, nil); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("nil snapshot error = %v, want ErrInvalidReference", err)
	}
	snapshot := &SessionReferenceCoverageSnapshot{
		BindingID: "b1", ContractVersion: "reference_coverage.v1",
		ContextHash: d1CanonSHA("ctx-1"), InventoryHash: d1CanonSHA("inv-1"),
		SnapshotHash: d1CanonSHA("snap-1"), FieldCount: 1, CoveredFieldCount: 1,
	}
	if _, err := st.ReplaceSessionReferenceCoverageSnapshot(ctx, snapshot, []SessionReferenceCoverageField{
		{BindingID: "b1", FieldKey: "", WorkID: "w1", ContinuityID: "w1-c1",
			ReferenceKind: "entity", SourceID: "e1", FieldName: "canonical_name"},
	}); !errors.Is(err, ErrInvalidReference) {
		t.Errorf("blank field key error = %v, want ErrInvalidReference", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_reference_coverage_snapshots`); got != 0 {
		t.Errorf("a refused coverage write must store no snapshot header, rows = %d", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM session_reference_coverage_fields`); got != 0 {
		t.Errorf("a refused coverage write must store no fields, rows = %d", got)
	}
	// A valid write still works afterwards, which is the proof the refusal left
	// nothing behind to collide with.
	if _, err := st.ReplaceSessionReferenceCoverageSnapshot(ctx, snapshot, []SessionReferenceCoverageField{
		{BindingID: "b1", FieldKey: "f1", WorkID: "w1", ContinuityID: "w1-c1",
			ReferenceKind: "entity", SourceID: "e1", FieldName: "canonical_name",
			FieldValue: "Mira", NormalizedValue: "mira", Eligible: true},
	}); err != nil {
		t.Fatalf("valid coverage write after a refusal: %v", err)
	}
}
