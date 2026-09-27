package store

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

// d1TestDB opens a single-connection in-memory SQLite database with foreign
// keys enabled, matching Cloudflare D1's default enforcement. modernc.org/sqlite
// is the same SQLite engine family D1 runs, so these tests exercise the real
// dialect rather than a mock.
func d1TestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// A single connection keeps the in-memory database shared across queries.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	return db
}

func d1SeedJSON(t *testing.T, db *sql.DB, rows map[string]string) {
	t.Helper()
	if _, err := db.Exec("CREATE TABLE probe (k TEXT PRIMARY KEY, evidence_json TEXT, source_turn INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for k, v := range rows {
		if _, err := db.Exec("INSERT INTO probe (k, evidence_json) VALUES (?, ?)", k, v); err != nil {
			t.Fatalf("insert %s: %v", k, err)
		}
	}
}

func d1SelectKeys(t *testing.T, db *sql.DB, predicate string) map[string]bool {
	t.Helper()
	rows, err := db.Query("SELECT k FROM probe WHERE " + predicate + " ORDER BY k")
	if err != nil {
		t.Fatalf("query with predicate %s: %v", predicate, err)
	}
	defer rows.Close()
	got := map[string]bool{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[k] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return got
}

// TestD1CurrentProjectionPredicateMatchesMariaDBSemantics pins the equivalence
// with MariaDB's JSON_UNQUOTE(JSON_EXTRACT(...)) = 'true': the normal JSON
// boolean true and the historic JSON string "true" are both eligible, while
// false, absent, and explicit null are not.
func TestD1CurrentProjectionPredicateMatchesMariaDBSemantics(t *testing.T) {
	db := d1TestDB(t)
	d1SeedJSON(t, db, map[string]string{
		"bool_true":  `{"current_projection":true}`,
		"str_true":   `{"current_projection":"true"}`,
		"bool_false": `{"current_projection":false}`,
		"str_false":  `{"current_projection":"false"}`,
		"absent":     `{"other":1}`,
		"json_null":  `{"current_projection":null}`,
		"number_one": `{"current_projection":1}`,
	})

	got := d1SelectKeys(t, db, d1CurrentProjectionPredicate("evidence_json"))

	for _, want := range []string{"bool_true", "str_true"} {
		if !got[want] {
			t.Errorf("predicate must match %s; matched=%v", want, got)
		}
	}
	for _, unwanted := range []string{"bool_false", "str_false", "absent", "json_null", "number_one"} {
		if got[unwanted] {
			t.Errorf("predicate must not match %s; matched=%v", unwanted, got)
		}
	}
	if len(got) != 2 {
		t.Errorf("matched %d rows, want exactly 2: %v", len(got), got)
	}
}

// TestD1NaiveJSONExtractDropsBooleanTrue documents why the helper exists: the
// obvious json_extract(...) = 'true' test loses every JSON-boolean row, which is
// the form the product code actually writes.
func TestD1NaiveJSONExtractDropsBooleanTrue(t *testing.T) {
	db := d1TestDB(t)
	d1SeedJSON(t, db, map[string]string{
		"bool_true": `{"current_projection":true}`,
		"str_true":  `{"current_projection":"true"}`,
	})

	naive := d1SelectKeys(t, db, "json_extract(evidence_json, '$.current_projection') = 'true'")
	if naive["bool_true"] {
		t.Fatal("test premise broken: naive predicate unexpectedly matched the boolean form")
	}
	if !naive["str_true"] {
		t.Fatal("test premise broken: naive predicate should match the string form")
	}

	portable := d1SelectKeys(t, db, d1CurrentProjectionPredicate("evidence_json"))
	if !portable["bool_true"] || !portable["str_true"] {
		t.Fatalf("portable predicate must match both forms: %v", portable)
	}
}

// TestD1JSONPathPresentDetectsExplicitNull pins the pending_thread requirement:
// an explicit JSON null is present, and json_extract cannot tell it apart from
// an absent path.
func TestD1JSONPathPresentDetectsExplicitNull(t *testing.T) {
	db := d1TestDB(t)
	d1SeedJSON(t, db, map[string]string{
		"null_value": `{"pending_thread":null}`,
		"object":     `{"pending_thread":{"thread_id":"a"}}`,
		"empty_str":  `{"pending_thread":""}`,
		"absent":     `{"other":1}`,
	})

	present := d1SelectKeys(t, db, d1JSONPathPresent("evidence_json", d1JSONPath("pending_thread")))
	for _, want := range []string{"null_value", "object", "empty_str"} {
		if !present[want] {
			t.Errorf("json_type path-presence must include %s: %v", want, present)
		}
	}
	if present["absent"] {
		t.Errorf("json_type path-presence must exclude the absent path: %v", present)
	}

	naive := d1SelectKeys(t, db, "json_extract(evidence_json, '$.pending_thread') IS NOT NULL")
	if naive["null_value"] {
		t.Error("test premise broken: naive predicate unexpectedly saw the explicit null as present")
	}
	if !present["null_value"] {
		t.Error("portable predicate must see the explicit null as present")
	}
}

// TestD1StatusObservationTurnSQL pins the repair-recorded-turn ordering against
// statusObservationTurn/SQL: a positive repair_recorded_turn wins, otherwise
// source_turn, otherwise 0.
func TestD1StatusObservationTurnSQL(t *testing.T) {
	db := d1TestDB(t)
	if _, err := db.Exec("CREATE TABLE e (id INTEGER PRIMARY KEY AUTOINCREMENT, evidence_json TEXT, source_turn INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	cases := []struct {
		name     string
		evidence string
		turn     int
		want     int
	}{
		{"repair_positive_wins", `{"repair_recorded_turn":5}`, 2, 5},
		{"repair_zero_falls_back", `{"repair_recorded_turn":0}`, 7, 7},
		{"repair_negative_falls_back", `{"repair_recorded_turn":-3}`, 4, 4},
		{"repair_string_rejected", `{"repair_recorded_turn":"9"}`, 4, 4},
		{"repair_null_falls_back", `{"repair_recorded_turn":null}`, 8, 8},
		{"repair_absent_falls_back", `{"other":1}`, 3, 3},
		{"no_repair_no_turn", `{}`, 0, 0},
	}
	for _, tc := range cases {
		if _, err := db.Exec("INSERT INTO e (evidence_json, source_turn) VALUES (?, ?)", tc.evidence, tc.turn); err != nil {
			t.Fatalf("%s: insert: %v", tc.name, err)
		}
	}
	expr := d1StatusObservationTurnSQL("e")
	for i, tc := range cases {
		var got int
		if err := db.QueryRow("SELECT "+expr+" FROM e WHERE id = ?", i+1).Scan(&got); err != nil {
			t.Fatalf("%s: query: %v", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: observation turn = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestD1StatusProjectionSourceSQL pins the source eligibility rule: an active
// revision is eligible, and so is a state_repair.v1 correction with a blank or
// absent source revision.
func TestD1StatusProjectionSourceSQL(t *testing.T) {
	db := d1TestDB(t)
	if _, err := db.Exec("CREATE TABLE r (id INTEGER PRIMARY KEY AUTOINCREMENT, lifecycle_state TEXT)"); err != nil {
		t.Fatalf("create revision table: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE e (id INTEGER PRIMARY KEY AUTOINCREMENT, evidence_json TEXT, revision_id INTEGER)"); err != nil {
		t.Fatalf("create evidence table: %v", err)
	}
	revStates := []string{"active", "superseded", "superseded", "superseded", "superseded", "superseded"}
	for _, state := range revStates {
		if _, err := db.Exec("INSERT INTO r (lifecycle_state) VALUES (?)", state); err != nil {
			t.Fatalf("insert revision: %v", err)
		}
	}
	evidence := []string{
		`{}`,
		`{"source_revision":"","source_contract":"state_repair.v1"}`,
		`{"source_contract":"state_repair.v1"}`,
		`{"source_revision":null,"source_contract":"state_repair.v1"}`,
		`{"source_revision":"rev-1","source_contract":"state_repair.v1"}`,
		`{"source_revision":123,"source_contract":"state_repair.v1"}`,
	}
	want := []bool{true, true, true, true, false, false}
	for i, ev := range evidence {
		if _, err := db.Exec("INSERT INTO e (evidence_json, revision_id) VALUES (?, ?)", ev, i+1); err != nil {
			t.Fatalf("insert evidence %d: %v", i, err)
		}
	}
	pred := d1StatusProjectionSourceSQL("e", "r")
	for i := range evidence {
		var eligible int
		if err := db.QueryRow(
			"SELECT CASE WHEN "+pred+" THEN 1 ELSE 0 END FROM e JOIN r ON r.id = e.revision_id WHERE e.id = ?", i+1,
		).Scan(&eligible); err != nil {
			t.Fatalf("case %d: query: %v", i, err)
		}
		if (eligible == 1) != want[i] {
			t.Errorf("case %d (%s): eligible = %t, want %t", i, evidence[i], eligible == 1, want[i])
		}
	}
}

func TestD1QuoteHelpers(t *testing.T) {
	if got := d1QuoteIdent(`weird"name`); got != `"weird""name"` {
		t.Errorf("d1QuoteIdent = %s", got)
	}
	if got := d1QuoteIdentList([]string{"a", "b"}); got != `"a", "b"` {
		t.Errorf("d1QuoteIdentList = %s", got)
	}
	if got := d1StringLiteral("it's"); got != "'it''s'" {
		t.Errorf("d1StringLiteral = %s", got)
	}
	if got := d1JSONPath("current_projection"); got != "'$.current_projection'" {
		t.Errorf("d1JSONPath = %s", got)
	}
}

func TestD1PositiveJSONIntegerAndTextHelpers(t *testing.T) {
	db := d1TestDB(t)
	d1SeedJSON(t, db, map[string]string{
		"int_pos":    `{"n":4,"s":"x"}`,
		"int_zero":   `{"n":0,"s":"y"}`,
		"int_neg":    `{"n":-2,"s":"z"}`,
		"str_num":    `{"n":"4","s":"w"}`,
		"float":      `{"n":4.5,"s":"v"}`,
		"null_value": `{"n":null,"s":null}`,
		"num_text":   `{"n":1,"s":7}`,
	})

	// The positive-integer expression yields the value only for positive JSON integers.
	rows, err := db.Query("SELECT k, " + d1PositiveJSONInteger("evidence_json", d1JSONPath("n")) + " FROM probe ORDER BY k")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got := map[string]int{}
	for rows.Next() {
		var k string
		var v int
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[k] = v
	}
	rows.Close()
	if got["int_pos"] != 4 {
		t.Errorf("int_pos = %d, want 4", got["int_pos"])
	}
	// "num_text" carries n=1, a valid positive integer, even though its "s"
	// field is numeric; the "n" expression is independent of "s".
	if got["num_text"] != 1 {
		t.Errorf("num_text = %d, want 1 (its n field is a positive integer)", got["num_text"])
	}
	for _, k := range []string{"int_zero", "int_neg", "str_num", "float", "null_value"} {
		if got[k] != 0 {
			t.Errorf("%s = %d, want 0 (non-positive-integer must be rejected)", k, got[k])
		}
	}

	// The text helper yields the string only for JSON strings.
	textRows, err := db.Query("SELECT k, " + d1JSONTextOrEmpty("evidence_json", d1JSONPath("s")) + " FROM probe ORDER BY k")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	texts := map[string]string{}
	for textRows.Next() {
		var k, v string
		if err := textRows.Scan(&k, &v); err != nil {
			t.Fatalf("scan: %v", err)
		}
		texts[k] = v
	}
	textRows.Close()
	if texts["int_pos"] != "x" {
		t.Errorf("int_pos text = %q, want x", texts["int_pos"])
	}
	if texts["num_text"] != "" {
		t.Errorf("num_text = %q, want empty (numeric identity must not be coerced)", texts["num_text"])
	}
	if texts["null_value"] != "" {
		t.Errorf("null_value = %q, want empty", texts["null_value"])
	}
}

// TestD1JSONStringBlankPredicateRejectsNonStringValues pins the MariaDB
// equivalence for COALESCE(JSON_UNQUOTE(JSON_EXTRACT(...)), ”) = ”: absent and
// explicit null are blank, an empty string is blank, and a non-string value is
// NOT blank. Treating a JSON number as blank (which the text-or-empty primitive
// would do) silently admitted a malformed source revision as a valid
// state_repair correction.
func TestD1JSONStringBlankPredicateRejectsNonStringValues(t *testing.T) {
	db := d1TestDB(t)
	d1SeedJSON(t, db, map[string]string{
		"absent":     `{"other":1}`,
		"json_null":  `{"source_revision":null}`,
		"empty":      `{"source_revision":""}`,
		"text_value": `{"source_revision":"rev-1"}`,
		"number":     `{"source_revision":123}`,
		"boolean":    `{"source_revision":false}`,
		"object":     `{"source_revision":{"a":1}}`,
	})

	blank := d1SelectKeys(t, db, d1JSONStringBlankPredicate("evidence_json", d1JSONPath("source_revision")))

	for _, want := range []string{"absent", "json_null", "empty"} {
		if !blank[want] {
			t.Errorf("blank predicate must include %s; got %v", want, blank)
		}
	}
	for _, unwanted := range []string{"text_value", "number", "boolean", "object"} {
		if blank[unwanted] {
			t.Errorf("blank predicate must exclude %s; got %v", unwanted, blank)
		}
	}

	// The text-or-empty primitive must not be used as a blank test: it maps a
	// JSON number to the empty string.
	naive := d1SelectKeys(t, db, d1JSONTextOrEmpty("evidence_json", d1JSONPath("source_revision"))+" = ''")
	if !naive["number"] {
		t.Error("test premise broken: text-or-empty should collapse the number to an empty string")
	}
	if blank["number"] {
		t.Error("blank predicate must reject the numeric source revision")
	}
}
