package store

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

// D1 precise memory write tests.
//
// Every test runs against the real SQLite engine through the same D1 transport
// the Worker uses, with foreign keys enabled exactly as D1 enforces them. That
// matters for this file specifically: the write is one atomic batch across three
// tables, and a test that only checked the returned error would not notice a
// partially applied commit.
//
// The seeds are chosen around the two failure modes that are silent. A write
// that is admitted against a superseded source revision resurrects a memory the
// user already rolled back, and a write that commits the unit without its
// dependency edges or its vector outbox row produces a memory that can never be
// recalled or invalidated. Both are asserted by counting rows, not by reading
// the returned error.

// d1WriteScene is a source revision the write path will admit, plus a superseded
// and an invalidated one it must refuse.
type d1WriteScene struct {
	SessionID  string
	LiveRev    string
	Superseded string
	Invalid    string
	Unknown    string
}

// d1SeedPreciseWriteScene registers three source revisions in one session. The
// live one is what the fence admits; the other two exist so the fence has
// something to reject without the test having to fabricate a state.
func d1SeedPreciseWriteScene(t *testing.T, conn *sqliteD1Conn, sessionID string) d1WriteScene {
	t.Helper()
	scene := d1WriteScene{
		SessionID:  sessionID,
		LiveRev:    sessionID + "-rev-live",
		Superseded: sessionID + "-rev-superseded",
		Invalid:    sessionID + "-rev-invalidated",
		Unknown:    sessionID + "-rev-unknown",
	}
	d1SeedIdentityRevision(t, conn, sessionID, scene.LiveRev, "turn-1", "active")
	d1SeedIdentityRevision(t, conn, sessionID, scene.Superseded, "turn-2", "superseded")
	d1SeedIdentityRevision(t, conn, sessionID, scene.Invalid, "turn-3", "invalidated")
	return scene
}

// d1WriteUnit returns a unit that satisfies every canonical CHECK constraint, so
// a test states only the field it is actually about.
func d1WriteUnit(scene d1WriteScene, unitID string) *PreciseMemoryUnit {
	return &PreciseMemoryUnit{
		UnitID:                unitID,
		ContractVersion:       PreciseMemoryUnitContract,
		ChatSessionID:         scene.SessionID,
		SourceTurnStart:       1,
		SourceTurnEnd:         1,
		SourceContract:        "source_acceptance_observation.v1",
		SourceRevision:        scene.LiveRev,
		SourceContentHash:     "content-hash-" + unitID,
		SourceRole:            "combined_turn_pair",
		SourceSpanStart:       0,
		SourceSpanEnd:         1,
		EvidenceExcerpt:       "the lantern was still lit",
		EvidenceHash:          "evidence-hash-" + unitID,
		DirectEvidenceIDsJSON: "[]",
		Kind:                  "event",
		PayloadJSON:           `{"summary":"the lantern was still lit"}`,
		TruthScope:            "objective",
		EpistemicMode:         "direct",
		AuthorityClass:        "objective_world_state",
		AdmissionState:        "committed",
		ReviewState:           "source_observed",
		Visibility:            "public",
		Confidence:            0.8,
		IdempotencyKey:        "ik-" + unitID,
		LifecycleState:        "active",
	}
}

// d1CountDependencies returns the dependency rows recorded for one child unit.
func d1CountDependencies(t *testing.T, conn *sqliteD1Conn, unitID string) int {
	t.Helper()
	return d1Count(t, conn, `SELECT COUNT(*) FROM memory_derivation_dependencies
		WHERE child_artifact_type = 'precise_memory_unit' AND child_artifact_id = ?`, unitID)
}

// d1OutboxDocument returns the stored outbox document for an operation key.
func d1OutboxDocument(t *testing.T, conn *sqliteD1Conn, operationKey string) (string, string, bool) {
	t.Helper()
	var documentID, status string
	if err := conn.QueryRow(context.Background(), `
		SELECT document_id, status FROM memory_vector_outbox WHERE operation_key = ?`,
		operationKey).Scan(&documentID, &status); err != nil {
		return "", "", false
	}
	return documentID, status, true
}

// TestD1PreciseMemoryWriteCapabilitiesAreAdvertised pins the delivery state
// through the manifest, and pins the availability probe separately: a provider
// that implements the writer but reports writes as disabled would make the
// foreground complete-turn path skip the projection entirely.
func TestD1PreciseMemoryWriteCapabilitiesAreAdvertised(t *testing.T) {
	st, _ := newD1TestStore(t)

	var store Store = st
	if _, ok := store.(PreciseMemoryWriter); !ok {
		t.Fatal("the D1 provider must expose PreciseMemoryWriter")
	}
	for _, capability := range []string{"PreciseMemoryWriter", "PreciseMemoryWriteAvailability"} {
		reported := false
		for _, status := range CapabilityReport(st) {
			if status.Name != capability {
				continue
			}
			reported = true
			if !status.Implemented {
				t.Errorf("the capability manifest reports %s as missing", capability)
			}
		}
		if !reported {
			t.Errorf("%s is not probed by the capability manifest", capability)
		}
	}

	if !st.PreciseMemoryWritesEnabled() {
		t.Error("a D1 store with a live connection must report precise memory writes as enabled")
	}
	var nilStore *d1Store
	if nilStore.PreciseMemoryWritesEnabled() {
		t.Error("a D1 store with no connection must not claim it can write precise memory")
	}
}

// TestD1SavePreciseMemoryUnitRejectsInactiveSource pins the fence, and pins that
// the rejection writes nothing at all.
//
// A fence that returned the error but still enqueued a vector operation would
// leave a document in the index for a memory the store refuses to admit — the
// recall path would surface content that no durable row explains.
func TestD1SavePreciseMemoryUnitRejectsInactiveSource(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	cases := []struct {
		name     string
		revision string
	}{
		{"superseded", scene.Superseded},
		{"invalidated", scene.Invalid},
		{"never accepted", scene.Unknown},
	}
	for _, tc := range cases {
		unit := d1WriteUnit(scene, "u-"+strings.ReplaceAll(tc.name, " ", "-"))
		unit.SourceRevision = tc.revision
		inserted, err := st.SavePreciseMemoryUnit(ctx, unit)
		if inserted {
			t.Errorf("%s revision: inserted = true, want false", tc.name)
		}
		if !errors.Is(err, ErrSourceRevisionStale) {
			t.Errorf("%s revision: error = %v, want ErrSourceRevisionStale", tc.name, err)
		}
		if got := d1Count(t, conn, `SELECT COUNT(*) FROM precise_memory_units WHERE unit_id = ?`, unit.UnitID); got != 0 {
			t.Errorf("%s revision: %d unit rows were written", tc.name, got)
		}
		if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != 0 {
			t.Errorf("%s revision: %d outbox rows were written", tc.name, got)
		}
		if got := d1CountDependencies(t, conn, unit.UnitID); got != 0 {
			t.Errorf("%s revision: %d dependency rows were written", tc.name, got)
		}
	}
}

// TestD1SavePreciseMemoryUnitCommitsOneBatch pins the atomic commit: the unit,
// its dependency edges, and its vector outbox operation all exist afterwards,
// and none of them is missing.
//
// The dependency count is the load-bearing assertion. A unit with no edges
// cannot be invalidated by a rollback or a reprocess, and nothing about reading
// the unit would reveal that.
func TestD1SavePreciseMemoryUnitCommitsOneBatch(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	unit := d1WriteUnit(scene, "u-commit")
	unit.ActorEntityID = "s1-actor"
	unit.SubjectEntityID = "s1-subject"
	d1SeedIdentity(t, conn, unit.ActorEntityID, "s1", "session_npc", "character", "Alex",
		"active", EntityIdentityReviewStateSourceObserved, scene.LiveRev, 1, 1)
	d1SeedIdentity(t, conn, unit.SubjectEntityID, "s1", "session_npc", "character", "Bell",
		"active", EntityIdentityReviewStateSourceObserved, scene.LiveRev, 1, 1)

	inserted, err := st.SavePreciseMemoryUnit(ctx, unit)
	if err != nil {
		t.Fatalf("SavePreciseMemoryUnit: %v", err)
	}
	if !inserted {
		t.Fatal("a first write must report inserted")
	}
	if unit.ID == 0 {
		t.Error("the committed unit must report its stored id")
	}

	if got := d1Count(t, conn, `SELECT COUNT(*) FROM precise_memory_units WHERE unit_id = ?`, unit.UnitID); got != 1 {
		t.Fatalf("unit rows = %d, want 1", got)
	}
	// Only the source revision is a parent: the unit cites no evidence.
	if got := d1CountDependencies(t, conn, unit.UnitID); got != 1 {
		t.Errorf("dependency rows = %d, want 1 (the source revision)", got)
	}
	var parentType, parentID string
	if err := conn.QueryRow(ctx, `
		SELECT parent_artifact_type, parent_artifact_id FROM memory_derivation_dependencies
		WHERE child_artifact_type = 'precise_memory_unit' AND child_artifact_id = ?`,
		unit.UnitID).Scan(&parentType, &parentID); err != nil {
		t.Fatalf("read the unit's dependency edge: %v", err)
	}
	if parentType != "source_revision" || parentID != scene.LiveRev {
		t.Errorf("dependency edge = %s:%s, want source_revision:%s", parentType, parentID, scene.LiveRev)
	}
	// A general-vector eligible unit with searchable text must also queue its
	// document, or it never reaches semantic recall.
	operationKey := preciseMemoryVectorOperationKey(
		scene.SessionID, scene.LiveRev, unit.DerivationVersion, unit.ExtractorVersion,
		unit.IndexVersion, "precise_memory:s1:"+unit.UnitID)
	documentID, status, ok := d1OutboxDocument(t, conn, operationKey)
	if !ok {
		t.Fatal("an eligible unit must enqueue its vector document")
	}
	if documentID != "precise_memory:s1:"+unit.UnitID {
		t.Errorf("outbox document = %q, want the unit's own document", documentID)
	}
	// No embedding was supplied, so the operation is waiting for one rather than
	// ready: claiming otherwise would make a worker ask for a vector it does not
	// have.
	if status != "needs_embedding" {
		t.Errorf("outbox status = %q, want %q", status, "needs_embedding")
	}
}

// TestD1SavePreciseMemoryUnitDependencyParents pins the parent set for a unit
// that cites evidence.
//
// The root evidence is excluded because it is also in the citation list, and a
// repeated edge would make one piece of evidence look like two independent
// derivations. A zero or negative id in the citation list is skipped for the
// same reason it is on the reference path: it is not an evidence row.
func TestD1SavePreciseMemoryUnitDependencyParents(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	for _, evidenceID := range []int64{11, 12, 13} {
		if _, err := conn.Exec(ctx, `INSERT INTO direct_evidence_records (
			id, chat_session_id, evidence_text, source_turn_start, source_turn_end, source_hash
		) VALUES (?, 's1', ?, 1, 1, ?)`,
			evidenceID, "evidence "+strconv.FormatInt(evidenceID, 10),
			"eh-"+strconv.FormatInt(evidenceID, 10)); err != nil {
			t.Fatalf("seed evidence %d: %v", evidenceID, err)
		}
	}

	unit := d1WriteUnit(scene, "u-evidence")
	unit.RootEvidenceID = 12
	// 11 and 12 are distinct; 12 is the root and must not be repeated; 0 and a
	// negative id are not evidence rows.
	unit.DirectEvidenceIDsJSON = `[11, 12, 0, -3]`
	if _, err := st.SavePreciseMemoryUnit(ctx, unit); err != nil {
		t.Fatalf("SavePreciseMemoryUnit: %v", err)
	}

	// One edge for the source revision, one for the root, one for evidence 11.
	if got := d1CountDependencies(t, conn, unit.UnitID); got != 3 {
		t.Errorf("dependency rows = %d, want 3 (source revision, root, and the one distinct citation)", got)
	}
	evidenceEdges := d1Count(t, conn, `
		SELECT COUNT(*) FROM memory_derivation_dependencies
		WHERE child_artifact_type = 'precise_memory_unit' AND child_artifact_id = ?
		  AND parent_artifact_type = 'direct_evidence'`, unit.UnitID)
	if evidenceEdges != 2 {
		t.Errorf("evidence edges = %d, want 2 (the root and evidence 11)", evidenceEdges)
	}
	duplicated := d1Count(t, conn, `
		SELECT COUNT(*) FROM memory_derivation_dependencies
		WHERE child_artifact_type = 'precise_memory_unit' AND child_artifact_id = ?
		  AND parent_artifact_type = 'direct_evidence' AND parent_artifact_id = '12'`, unit.UnitID)
	if duplicated != 1 {
		t.Errorf("root evidence edges = %d, want exactly 1; a repeated edge reads as two derivations", duplicated)
	}
}

// TestD1SavePreciseMemoryUnitReplaysIdempotently pins the replay contract.
//
// A retried turn must not produce a second unit, a second set of edges, or a
// second vector operation. It reports inserted=false and changes nothing, so the
// caller can tell a retry from a first write without re-reading the store.
func TestD1SavePreciseMemoryUnitReplaysIdempotently(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	first := d1WriteUnit(scene, "u-replay")
	if _, err := st.SavePreciseMemoryUnit(ctx, first); err != nil {
		t.Fatalf("first SavePreciseMemoryUnit: %v", err)
	}
	firstID := first.ID
	unitRows := d1Count(t, conn, `SELECT COUNT(*) FROM precise_memory_units`)
	depRows := d1Count(t, conn, `SELECT COUNT(*) FROM memory_derivation_dependencies`)
	outboxRows := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`)

	// The same unit, rebuilt from scratch as a retry would.
	replay := d1WriteUnit(scene, "u-replay")
	inserted, err := st.SavePreciseMemoryUnit(ctx, replay)
	if err != nil {
		t.Fatalf("replayed SavePreciseMemoryUnit: %v", err)
	}
	if inserted {
		t.Error("a replay must report inserted=false")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM precise_memory_units`); got != unitRows {
		t.Errorf("unit rows after replay = %d, want %d", got, unitRows)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_derivation_dependencies`); got != depRows {
		t.Errorf("dependency rows after replay = %d, want %d", got, depRows)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != outboxRows {
		t.Errorf("outbox rows after replay = %d, want %d", got, outboxRows)
	}
	// The stored row is untouched: a replay must not move the id the first write
	// reported, or a caller holding that id would now point at a different row.
	var storedID int64
	if err := conn.QueryRow(ctx, `SELECT id FROM precise_memory_units WHERE unit_id = ?`,
		replay.UnitID).Scan(&storedID); err != nil {
		t.Fatalf("read the replayed unit: %v", err)
	}
	if storedID != firstID {
		t.Errorf("stored id after replay = %d, want the original %d", storedID, firstID)
	}
}

// TestD1SavePreciseMemoryUnitRejectsIdempotencyConflict pins the other half.
//
// Two units that share a unit_id but disagree on the source revision are not a
// replay: one of them is a different derivation that would be lost. Silently
// keeping the stored row would make the caller's payload disappear, so this is
// an error and the stored row is left as it was.
func TestD1SavePreciseMemoryUnitRejectsIdempotencyConflict(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	// A second live revision is needed, and the schema allows only one active
	// revision per logical turn, so it is registered on its own turn.
	secondLive := "s1-rev-live-2"
	d1SeedIdentityRevision(t, conn, "s1", secondLive, "turn-4", "active")

	first := d1WriteUnit(scene, "u-conflict")
	if _, err := st.SavePreciseMemoryUnit(ctx, first); err != nil {
		t.Fatalf("first SavePreciseMemoryUnit: %v", err)
	}

	// Same unit_id and idempotency key, different source revision.
	conflicting := d1WriteUnit(scene, "u-conflict")
	conflicting.SourceRevision = secondLive
	inserted, err := st.SavePreciseMemoryUnit(ctx, conflicting)
	if inserted {
		t.Error("a conflicting replay must not report inserted")
	}
	if err == nil || !strings.Contains(err.Error(), "idempotency conflict") {
		t.Errorf("conflicting replay error = %v, want an idempotency conflict", err)
	}

	// A different unit_id that reuses an existing idempotency key collides too:
	// the per-session idempotency key is what makes a retry safe, so two units
	// cannot share one.
	reusedKey := d1WriteUnit(scene, "u-other")
	reusedKey.IdempotencyKey = first.IdempotencyKey
	if _, err := st.SavePreciseMemoryUnit(ctx, reusedKey); err == nil ||
		!strings.Contains(err.Error(), "idempotency conflict") {
		t.Errorf("reused idempotency key error = %v, want an idempotency conflict", err)
	}

	// The original unit is intact.
	var storedRevision string
	if err := conn.QueryRow(ctx, `SELECT source_revision FROM precise_memory_units WHERE unit_id = ?`,
		first.UnitID).Scan(&storedRevision); err != nil {
		t.Fatalf("read the stored unit: %v", err)
	}
	if storedRevision != scene.LiveRev {
		t.Errorf("stored source revision = %q, want the original %q", storedRevision, scene.LiveRev)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM precise_memory_units`); got != 1 {
		t.Errorf("unit rows after the conflicts = %d, want 1", got)
	}
}

// TestD1SavePreciseMemoryUnitSkipsIneligibleVectorWork pins which units queue a
// vector document.
//
// Each of these is a unit that must NOT reach the general vector index: a
// holder-scoped unit belongs to one character's knowledge, and a private one is
// not the user's to have retrieved from a semantic search. Queueing either
// would put private or character-scoped content into the general index, which is
// exactly the leak the perspective lane exists to prevent.
func TestD1SavePreciseMemoryUnitSkipsIneligibleVectorWork(t *testing.T) {
	cases := []struct {
		name  string
		apply func(*PreciseMemoryUnit)
	}{
		{"holder scoped", func(u *PreciseMemoryUnit) { u.KnowledgeHolderEntityID = "s1-holder" }},
		{"owner private", func(u *PreciseMemoryUnit) { u.Visibility = "owner_private" }},
		{"reveal required", func(u *PreciseMemoryUnit) { u.Visibility = "reveal_required" }},
		{"hidden epistemic mode", func(u *PreciseMemoryUnit) { u.EpistemicMode = "hidden" }},
		{"suspected epistemic mode", func(u *PreciseMemoryUnit) { u.EpistemicMode = "suspected" }},
		{"unknown epistemic mode", func(u *PreciseMemoryUnit) { u.EpistemicMode = "unknown" }},
		{"user private", func(u *PreciseMemoryUnit) { u.Visibility = "user_private" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, conn := newD1TestStore(t)
			scene := d1SeedPreciseWriteScene(t, conn, "s1")
			ctx := context.Background()

			unit := d1WriteUnit(scene, "u-ineligible")
			tc.apply(unit)
			if unit.KnowledgeHolderEntityID != "" {
				// The holder is a foreign key, so the identity has to exist.
				d1SeedIdentity(t, conn, unit.KnowledgeHolderEntityID, "s1", "session_npc",
					"character", "Rowan", "active", EntityIdentityReviewStateSourceObserved,
					scene.LiveRev, 1, 1)
			}
			if _, err := st.SavePreciseMemoryUnit(ctx, unit); err != nil {
				t.Fatalf("SavePreciseMemoryUnit: %v", err)
			}
			if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != 0 {
				t.Errorf("outbox rows = %d, want 0 for an ineligible unit", got)
			}
			// The unit itself is still committed: ineligibility for the general
			// vector index is not a reason to drop the memory.
			if got := d1Count(t, conn, `SELECT COUNT(*) FROM precise_memory_units WHERE unit_id = ?`, unit.UnitID); got != 1 {
				t.Errorf("unit rows = %d, want 1", got)
			}
		})
	}
}

// TestD1SavePreciseMemoryUnitQueuesReadyEmbedding pins the other status.
//
// With an embedding present the operation is pending and a worker may take it
// immediately. Reporting needs_embedding here would leave an already-materialised
// vector unmaterialised in the index until something noticed.
func TestD1SavePreciseMemoryUnitQueuesReadyEmbedding(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	unit := d1WriteUnit(scene, "u-embedded")
	unit.VectorEmbedding = []float32{0.1, 0.2, 0.3}
	unit.VectorEmbeddingModel = "text-embedding-3-small"
	if _, err := st.SavePreciseMemoryUnit(ctx, unit); err != nil {
		t.Fatalf("SavePreciseMemoryUnit: %v", err)
	}

	operationKey := preciseMemoryVectorOperationKey(
		scene.SessionID, scene.LiveRev, unit.DerivationVersion, unit.ExtractorVersion,
		unit.IndexVersion, "precise_memory:s1:"+unit.UnitID)
	_, status, ok := d1OutboxDocument(t, conn, operationKey)
	if !ok {
		t.Fatal("an eligible unit with an embedding must enqueue its document")
	}
	if status != "pending" {
		t.Errorf("outbox status = %q, want %q", status, "pending")
	}
	if got := d1Count(t, conn,
		`SELECT COUNT(*) FROM memory_vector_outbox WHERE operation_key = ? AND embedding_ready = 1`,
		operationKey); got != 1 {
		t.Error("the queued operation must record that its embedding is ready")
	}
}

// TestD1SavePreciseMemoryUnitVectorDocumentMatchesTheIndex pins the document
// payload, because it is what the vector worker will actually index.
//
// The document id, the source table, and the semantic text are the contract
// between the write and the index. A document id computed differently here would
// let a later delete miss it, and a semantic text that differs would make the
// same memory findable by one provider and not the other.
func TestD1SavePreciseMemoryUnitVectorDocumentMatchesTheIndex(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	unit := d1WriteUnit(scene, "u-doc")
	unit.PayloadJSON = `{"subject":"the lantern","summary":"the lantern was still lit"}`
	unit.EvidenceExcerpt = "still burning"
	if _, err := st.SavePreciseMemoryUnit(ctx, unit); err != nil {
		t.Fatalf("SavePreciseMemoryUnit: %v", err)
	}

	var raw string
	if err := conn.QueryRow(ctx,
		`SELECT document_json FROM memory_vector_outbox WHERE document_id = ?`,
		"precise_memory:s1:"+unit.UnitID).Scan(&raw); err != nil {
		t.Fatalf("read the queued document: %v", err)
	}
	var document struct {
		ID            string         `json:"ID"`
		ChatSessionID string         `json:"ChatSessionID"`
		SourceTable   string         `json:"SourceTable"`
		SourceRowID   string         `json:"SourceRowID"`
		SchemaVersion string         `json:"SchemaVersion"`
		DocumentText  string         `json:"DocumentText"`
		Metadata      map[string]any `json:"Metadata"`
	}
	if err := json.Unmarshal([]byte(raw), &document); err != nil {
		t.Fatalf("the queued document is not valid JSON: %v", err)
	}
	if document.SourceTable != "precise_memory_units" {
		t.Errorf("SourceTable = %q, want %q", document.SourceTable, "precise_memory_units")
	}
	if document.SourceRowID != unit.UnitID {
		t.Errorf("SourceRowID = %q, want %q", document.SourceRowID, unit.UnitID)
	}
	if document.SchemaVersion != PreciseMemoryUnitContract {
		t.Errorf("SchemaVersion = %q, want %q", document.SchemaVersion, PreciseMemoryUnitContract)
	}
	if want := PreciseMemorySemanticText(unit); document.DocumentText != want {
		t.Errorf("DocumentText = %q, want the shared projection %q", document.DocumentText, want)
	}
	// The shared projection always leads with the kind, so a schema-valid unit
	// can never produce an empty document. That is what keeps the
	// "no searchable text" early return on the enqueue unreachable for any row
	// the schema admits, and therefore keeps an outbox row from being queued for
	// a document the worker could not index.
	if document.DocumentText == "" {
		t.Error("a schema-valid unit must always produce a non-empty document text")
	}
	if !strings.Contains(document.DocumentText, "the lantern") {
		t.Errorf("DocumentText = %q, want it to lead with the semantic subject", document.DocumentText)
	}
	if document.Metadata == nil {
		t.Error("the queued document must carry the shared metadata map")
	}
}

// TestD1SavePreciseMemoryUnitBatchIsAtomic pins the transaction boundary.
//
// The unit names an entity that does not exist, so its foreign key fails. If the
// unit, its edges, and its outbox row were three separate statements, the unit
// and its edges would survive and the store would be left claiming a memory it
// never fully committed. D1Conn.Batch is the atomic unit and this proves the
// provider actually uses it.
func TestD1SavePreciseMemoryUnitBatchIsAtomic(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	unit := d1WriteUnit(scene, "u-atomic")
	// A stable_entity_id that was never registered. Foreign keys are enforced in
	// this harness exactly as D1 enforces them, so the insert fails.
	unit.SubjectEntityID = "s1-does-not-exist"

	inserted, err := st.SavePreciseMemoryUnit(ctx, unit)
	if inserted {
		t.Error("a failed batch must not report inserted")
	}
	if err == nil {
		t.Fatal("a foreign key violation must surface as an error")
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM precise_memory_units`); got != 0 {
		t.Errorf("unit rows = %d, want 0; the batch must roll back the whole write", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_derivation_dependencies`); got != 0 {
		t.Errorf("dependency rows = %d, want 0; the batch must roll back the whole write", got)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM memory_vector_outbox`); got != 0 {
		t.Errorf("outbox rows = %d, want 0; the batch must roll back the whole write", got)
	}
}

// TestD1SavePreciseMemoryUnitFeedsTheTypedLaneReaders closes the loop with the
// three already-ported readers.
//
// A write that is correct in isolation but invisible to the read path would
// leave a Cloudflare deployment with a memory store that accepts turns and
// recalls nothing. Each reader's own filter is exercised here, so this also
// proves the write stores the values those filters select on.
func TestD1SavePreciseMemoryUnitFeedsTheTypedLaneReaders(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	holder := "s1-holder"
	d1SeedIdentity(t, conn, holder, "s1", "session_npc", "character", "Rowan",
		"active", EntityIdentityReviewStateSourceObserved, scene.LiveRev, 1, 1)

	// A perspective observation: one character's knowledge, readable only by
	// that character.
	observation := d1WriteUnit(scene, "u-observation")
	observation.Kind = "observation"
	observation.EpistemicMode = "known"
	observation.KnowledgeHolderEntityID = holder
	observation.PayloadJSON = `{"summary":"Rowan still fears the bridge"}`
	// A boundary: an interaction edge with no knowledge holder, so it belongs to
	// the active-interaction lane.
	boundary := d1WriteUnit(scene, "u-boundary")
	boundary.Kind = "boundary"
	boundary.PayloadJSON = `{"relationship_key":"mira-ilse","summary":"they met at the gate"}`

	for _, unit := range []*PreciseMemoryUnit{observation, boundary} {
		if _, err := st.SavePreciseMemoryUnit(ctx, unit); err != nil {
			t.Fatalf("SavePreciseMemoryUnit %s: %v", unit.UnitID, err)
		}
	}

	perspective, err := st.ListCharacterPerspectiveMemoryUnits(ctx, "s1", holder)
	if err != nil {
		t.Fatalf("ListCharacterPerspectiveMemoryUnits: %v", err)
	}
	if got := d1PreciseUnitIDs(perspective); len(got) != 1 || got[0] != "u-observation" {
		t.Errorf("perspective lane = %v, want only the observation", got)
	}
	// A different holder sees nothing: the write must not have widened the lane.
	other, err := st.ListCharacterPerspectiveMemoryUnits(ctx, "s1", "s1-someone-else")
	if err != nil {
		t.Fatalf("ListCharacterPerspectiveMemoryUnits for another holder: %v", err)
	}
	if len(other) != 0 {
		t.Errorf("another holder read %v, want nothing", d1PreciseUnitIDs(other))
	}

	// The active-interaction lane admits BOTH observation and boundary kinds.
	// The observation appears here as well as in the perspective lane, because
	// an interaction observation is a real occurrence in the scene even when it
	// is also scoped to one character's knowledge. The boundary is the kind the
	// perspective lane does NOT admit, so its presence here is what proves the
	// two lanes are not the same read.
	interactions, err := st.ListActiveInteractionMemoryUnits(ctx, "s1")
	if err != nil {
		t.Fatalf("ListActiveInteractionMemoryUnits: %v", err)
	}
	got := d1PreciseUnitIDs(interactions)
	if len(got) != 2 {
		t.Fatalf("active interaction lane = %v, want the observation and the boundary", got)
	}
	if !equalStringSlices(got, []string{"u-boundary", "u-observation"}) {
		t.Errorf("active interaction lane = %v, want [u-boundary u-observation] in turn order", got)
	}
}

// TestD1SavePreciseMemoryUnitTimestamps pins the timestamp contract.
//
// created_at and updated_at are TEXT. A unit written with a zero time must
// still receive a real one, or the source_turn ordering and the review indexes
// would sort against an empty string.
func TestD1SavePreciseMemoryUnitTimestamps(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")
	ctx := context.Background()

	written := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	unit := d1WriteUnit(scene, "u-time")
	unit.CreatedAt = written
	unit.UpdatedAt = written
	if _, err := st.SavePreciseMemoryUnit(ctx, unit); err != nil {
		t.Fatalf("SavePreciseMemoryUnit: %v", err)
	}

	var created, updated string
	if err := conn.QueryRow(ctx,
		`SELECT created_at, updated_at FROM precise_memory_units WHERE unit_id = ?`,
		unit.UnitID).Scan(&created, &updated); err != nil {
		t.Fatalf("read the unit's timestamps: %v", err)
	}
	if !strings.HasPrefix(created, "2026-04-05T06:07:08") {
		t.Errorf("created_at = %q, want the supplied instant", created)
	}
	if !strings.HasPrefix(updated, "2026-04-05T06:07:08") {
		t.Errorf("updated_at = %q, want the supplied instant", updated)
	}

	// A zero time is filled in, not written as the zero instant.
	zeroed := d1WriteUnit(scene, "u-zero-time")
	zeroed.CreatedAt = time.Time{}
	zeroed.UpdatedAt = time.Time{}
	if _, err := st.SavePreciseMemoryUnit(ctx, zeroed); err != nil {
		t.Fatalf("SavePreciseMemoryUnit with zero times: %v", err)
	}
	if err := conn.QueryRow(ctx,
		`SELECT created_at FROM precise_memory_units WHERE unit_id = ?`,
		zeroed.UnitID).Scan(&created); err != nil {
		t.Fatalf("read the zero-time unit: %v", err)
	}
	if strings.HasPrefix(created, "0001-01-01") || created == "" {
		t.Errorf("created_at = %q, want the current instant", created)
	}
}

// TestD1SavePreciseMemoryUnitRejectsIncompleteInput pins the argument contract.
func TestD1SavePreciseMemoryUnitRejectsIncompleteInput(t *testing.T) {
	st, _ := newD1TestStore(t)
	ctx := context.Background()

	if inserted, err := st.SavePreciseMemoryUnit(ctx, nil); inserted || err == nil {
		t.Error("a nil unit must be rejected")
	}
	scene := d1WriteScene{SessionID: "s1", LiveRev: "s1-rev-live"}
	noRevision := d1WriteUnit(scene, "u-no-revision")
	noRevision.SourceRevision = "   "
	if inserted, err := st.SavePreciseMemoryUnit(ctx, noRevision); inserted || err == nil {
		t.Error("a unit with no source revision must be rejected")
	} else if !strings.Contains(err.Error(), "source revision is required") {
		t.Errorf("error = %v, want it to name the missing source revision", err)
	}
}

// TestD1SavePreciseMemoryUnitRefusesVectorReplay pins the loud failure.
//
// The administrative vector replay marks its context so the enqueue takes a
// lease-aware branch. Silently taking the plain branch here would skip the lease
// check and could re-enqueue a document another worker holds, so the request is
// refused instead.
func TestD1SavePreciseMemoryUnitRefusesVectorReplay(t *testing.T) {
	st, conn := newD1TestStore(t)
	scene := d1SeedPreciseWriteScene(t, conn, "s1")

	replayCtx := WithMemoryAdmissionVectorReplay(context.Background(), true, false)
	unit := d1WriteUnit(scene, "u-replay-refused")
	inserted, err := st.SavePreciseMemoryUnit(replayCtx, unit)
	if inserted {
		t.Error("a refused replay must not report inserted")
	}
	if err == nil || !strings.Contains(err.Error(), "replay refresh is not supported") {
		t.Errorf("error = %v, want an explicit refusal", err)
	}
	if got := d1Count(t, conn, `SELECT COUNT(*) FROM precise_memory_units`); got != 0 {
		t.Errorf("unit rows = %d, want 0; a refused replay must write nothing", got)
	}
}
