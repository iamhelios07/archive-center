package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// trackedSchemaPath is the generated schema committed to the repository.
// The test working directory is this package, so the repository root is three
// levels up.
const (
	testMigrationsDir = "../../../migrations"
	trackedSchemaPath = "../../../deploy/cloudflare/migrations/001_canonical_schema.sql"
)

// TestGeneratedSchemaMatchesTrackedFile is the drift guard: if migrations/ changes
// without re-running the generator, the committed D1 schema would silently
// disagree with the MariaDB schema. Regenerate with
//
//	go run ./cmd/d1-schema-gen
//
// from go-service/ whenever this test fails.
func TestGeneratedSchemaMatchesTrackedFile(t *testing.T) {
	rendered, _, err := generate(testMigrationsDir)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	tracked, err := os.ReadFile(filepath.Clean(trackedSchemaPath))
	if err != nil {
		t.Fatalf("read tracked schema: %v", err)
	}
	// Normalise line endings before comparing: the generator always emits LF, and
	// a checkout with core.autocrlf enabled may materialise CRLF in the working
	// tree. .gitattributes pins this path to eol=lf, but the comparison stays
	// robust so a contributor's local git settings cannot produce a false stale
	// report.
	trackedLF := bytes.ReplaceAll(tracked, []byte("\r\n"), []byte("\n"))
	if !bytes.Equal(rendered, trackedLF) {
		t.Fatalf("tracked D1 schema is stale: %s differs from generator output (%d vs %d bytes). Re-run: cd go-service; go run ./cmd/d1-schema-gen",
			trackedSchemaPath, len(trackedLF), len(rendered))
	}
}

// TestGenerateIsDeterministic guards the property the drift test depends on: two
// runs over the same input must be byte-identical.
func TestGenerateIsDeterministic(t *testing.T) {
	first, statsA, err := generate(testMigrationsDir)
	if err != nil {
		t.Fatalf("first generate: %v", err)
	}
	second, statsB, err := generate(testMigrationsDir)
	if err != nil {
		t.Fatalf("second generate: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("generate is not deterministic: two runs differ")
	}
	if statsA != statsB {
		t.Errorf("stats differ between runs: %+v vs %+v", statsA, statsB)
	}
}

// TestGeneratedSchemaHasNoMariaDBOnlyConstructs asserts the generated DDL is
// valid SQLite: no ALTER statements, no ENGINE/CHARSET clauses, no UNSIGNED, no
// MariaDB REGEXP, and no SQLite internal objects.
func TestGeneratedSchemaHasNoMariaDBOnlyConstructs(t *testing.T) {
	rendered, stats, err := generate(testMigrationsDir)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	sql := string(rendered)

	// Only statement-level occurrences matter; the header comment explains the
	// conversions and legitimately names MariaDB keywords.
	var statements []string
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		statements = append(statements, line)
	}
	body := strings.Join(statements, "\n")

	for _, forbidden := range []string{
		"ALTER TABLE",
		"ENGINE=",
		"UNSIGNED",
		"COLLATE",
		"CHARSET",
		"REGEXP",
		"AUTO_INCREMENT",
		"PERSISTENT",
		"ON UPDATE CURRENT_TIMESTAMP",
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("generated schema still contains MariaDB-only construct %q", forbidden)
		}
	}
	for _, forbidden := range []string{`"sqlite_sequence"`, `"sqlite_schema"`} {
		if strings.Contains(body, forbidden) {
			t.Errorf("generated schema must not reference SQLite internal object %s", forbidden)
		}
	}
	if stats.tables == 0 {
		t.Fatal("generated schema has no tables")
	}
	if stats.autoIncrement == 0 {
		t.Fatal("generated schema has no AUTOINCREMENT table; MariaDB id parity would be lost")
	}
	if stats.autoIncrement != 50 {
		t.Errorf("AUTOINCREMENT table count = %d, want 50 (MariaDB defines 50 tables with AUTO_INCREMENT in their first declaration); if migrations changed, confirm the new count and update this expectation", stats.autoIncrement)
	}
	if stats.tables != 81 {
		t.Errorf("table count = %d, want 81 unique tables in migrations/; if migrations changed, confirm the new count and update this expectation", stats.tables)
	}
	if stats.tablesWithFK == 0 {
		t.Fatal("generated schema declares no foreign keys")
	}
}

// TestTranslateCheckRegexp pins the REGEXP translation used for digest CHECK
// constraints against the equivalent SQLite expression.
func TestTranslateCheckRegexp(t *testing.T) {
	got, err := translateCheckRegexp("CONSTRAINT chk_x CHECK (manifest_sha256 REGEXP '^[0-9a-f]{64}$')")
	if err != nil {
		t.Fatalf("translateCheckRegexp: %v", err)
	}
	want := "CONSTRAINT chk_x CHECK ((length(manifest_sha256) = 64 AND manifest_sha256 NOT GLOB '*[^0-9a-f]*'))"
	if got != want {
		t.Errorf("translation = %q, want %q", got, want)
	}

	// A pattern the translation cannot express must fail loudly rather than be
	// dropped, so validation cannot silently disappear.
	if _, err := translateCheckRegexp("CHECK (name REGEXP '^x+$')"); err == nil {
		t.Error("unsupported REGEXP must return an error instead of passing through")
	}
}

// TestHasKeywordBoundaries guards the classifier against column names that share
// a keyword prefix.
func TestHasKeywordBoundaries(t *testing.T) {
	cases := []struct {
		input string
		kw    string
		want  bool
	}{
		{"KEY idx_a (a)", "KEY", true},
		{"KEY_POINTS_JSON TEXT", "KEY", false},
		{"INDEX idx_a (a)", "INDEX", true},
		{"INDEXED_AT TEXT", "INDEX", false},
		{"UNIQUE (a)", "UNIQUE", true},
		{"UNIQUE_ID TEXT", "UNIQUE", false},
		{"CHECK (a > 0)", "CHECK", true},
		{"CHECKSUM TEXT", "CHECK", false},
		{"PRIMARY KEY (a)", "PRIMARY KEY", true},
	}
	for _, tc := range cases {
		if got := hasKeyword(tc.input, tc.kw); got != tc.want {
			t.Errorf("hasKeyword(%q, %q) = %t, want %t", tc.input, tc.kw, got, tc.want)
		}
	}
}

// TestParseColumnListDropsPrefixLengths pins the MariaDB prefix-index handling:
// col(191) must reduce to col or SQLite rejects the constraint with
// "expressions prohibited in PRIMARY KEY and UNIQUE constraints".
func TestParseColumnListDropsPrefixLengths(t *testing.T) {
	got := parseColumnList("content(191), created_at DESC, plain")
	want := []string{"content", "created_at", "plain"}
	if len(got) != len(want) {
		t.Fatalf("parseColumnList = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("parseColumnList[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
