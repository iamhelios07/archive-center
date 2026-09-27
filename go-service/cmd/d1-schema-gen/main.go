// Command d1-schema-gen converts the MariaDB canonical DDL under migrations/
// into a consolidated SQLite/D1 schema.
//
// Cloudflare D1 runs SQLite, and SQLite cannot add constraints after the fact:
// its ALTER TABLE supports only ADD COLUMN, RENAME COLUMN, RENAME TO, and DROP
// COLUMN. The MariaDB migrations therefore cannot be replayed statement by
// statement. This generator applies the first declaration of every table plus
// every later ALTER TABLE to a table model, then emits each table's FINAL shape
// as one CREATE TABLE.
//
// The generated file is a build artifact of this command. Do not edit it by
// hand; re-run the generator instead so the D1 schema cannot drift away from
// migrations/.
//
// Usage:
//
//	go run ./cmd/d1-schema-gen -migrations ../migrations -out ../deploy/cloudflare/migrations/001_canonical_schema.sql
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func main() {
	migrationsDir := flag.String("migrations", "../migrations", "directory holding the MariaDB migration files")
	outPath := flag.String("out", "../deploy/cloudflare/migrations/001_canonical_schema.sql", "output path for the D1 schema")
	flag.Parse()

	rendered, stats, err := generate(*migrationsDir)
	if err != nil {
		fail("%v", err)
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fail("create output directory: %v", err)
	}
	if err := os.WriteFile(*outPath, rendered, 0o644); err != nil {
		fail("write %s: %v", *outPath, err)
	}
	fmt.Printf("wrote %s: %d tables, %d indexes, %d autoincrement tables\n",
		*outPath, stats.tables, stats.indexes, stats.autoIncrement)
}

// schemaStats summarises a generated schema.
type schemaStats struct {
	tables        int
	indexes       int
	autoIncrement int
	tablesWithFK  int
}

// generate reads the MariaDB migrations and renders the consolidated D1 schema.
// It is deterministic: the same input always produces byte-identical output.
func generate(migrationsDir string) ([]byte, schemaStats, error) {
	paths, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	if err != nil || len(paths) == 0 {
		return nil, schemaStats{}, fmt.Errorf("no migration files found under %s: %v", migrationsDir, err)
	}
	sort.Strings(paths)

	schema := newSchema()
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, schemaStats{}, fmt.Errorf("read %s: %w", path, err)
		}
		// Normalise line endings before parsing. Some MariaDB migrations are
		// declared eol=crlf in .gitattributes, so a checkout can hand us CRLF
		// while another hands us LF. Without this the generated schema would
		// depend on the checkout's line endings and stop being reproducible.
		normalised := strings.ReplaceAll(string(content), "\r\n", "\n")
		for _, stmt := range splitStatements(normalised) {
			if err := schema.apply(stmt, filepath.Base(path)); err != nil {
				return nil, schemaStats{}, fmt.Errorf("%s: %w", filepath.Base(path), err)
			}
		}
	}
	if err := schema.validate(); err != nil {
		return nil, schemaStats{}, fmt.Errorf("validation: %w", err)
	}
	stats := schemaStats{
		tables:        len(schema.tables),
		indexes:       schema.indexCount(),
		autoIncrement: schema.autoIncrementCount(),
	}
	for _, t := range schema.tables {
		if len(t.fks) > 0 {
			stats.tablesWithFK++
		}
	}
	return []byte(schema.render(paths)), stats, nil
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "d1-schema-gen: "+format+"\n", args...)
	os.Exit(1)
}

// ---------------------------------------------------------------------------
// statement splitting
// ---------------------------------------------------------------------------

// splitStatements splits a SQL script on top-level semicolons. It removes line
// comments and respects single-quoted strings and parenthesis depth.
func splitStatements(script string) []string {
	var out []string
	var b strings.Builder
	inQuote := false
	depth := 0
	for _, line := range strings.Split(script, "\n") {
		if !inQuote && depth == 0 {
			if idx := strings.Index(line, "--"); idx >= 0 {
				line = line[:idx]
			}
		}
		for i := 0; i < len(line); i++ {
			ch := line[i]
			switch {
			case ch == '\'':
				if inQuote && i+1 < len(line) && line[i+1] == '\'' {
					b.WriteByte(ch)
					b.WriteByte(line[i+1])
					i++
					continue
				}
				inQuote = !inQuote
			case !inQuote && ch == '(':
				depth++
			case !inQuote && ch == ')':
				if depth > 0 {
					depth--
				}
			case !inQuote && depth == 0 && ch == ';':
				if s := strings.TrimSpace(b.String()); s != "" {
					out = append(out, s)
				}
				b.Reset()
				continue
			}
			b.WriteByte(ch)
		}
		b.WriteByte('\n')
	}
	if s := strings.TrimSpace(b.String()); s != "" {
		out = append(out, s)
	}
	return out
}

// splitTopLevel splits a parenthesised body on commas at depth zero.
func splitTopLevel(body string) []string {
	var out []string
	var b strings.Builder
	inQuote := false
	depth := 0
	for i := 0; i < len(body); i++ {
		ch := body[i]
		switch {
		case ch == '\'':
			if inQuote && i+1 < len(body) && body[i+1] == '\'' {
				b.WriteByte(ch)
				b.WriteByte(body[i+1])
				i++
				continue
			}
			inQuote = !inQuote
		case !inQuote && ch == '(':
			depth++
		case !inQuote && ch == ')':
			if depth > 0 {
				depth--
			}
		case !inQuote && depth == 0 && ch == ',':
			if s := strings.TrimSpace(b.String()); s != "" {
				out = append(out, s)
			}
			b.Reset()
			continue
		}
		b.WriteByte(ch)
	}
	if s := strings.TrimSpace(b.String()); s != "" {
		out = append(out, s)
	}
	return out
}

// ---------------------------------------------------------------------------
// schema model
// ---------------------------------------------------------------------------

type column struct {
	name string
	def  string // SQLite column definition without the name, already normalised
}

type foreignKey struct {
	name     string
	cols     []string
	refTable string
	refCols  []string
	actions  string
}

type indexDef struct {
	name string
	cols []string
}

type table struct {
	name       string
	columns    []column
	primaryKey string // rendered column list, empty when the PK is inline
	uniques    []string
	fks        []foreignKey
	checks     []string
	indexes    []indexDef
	declaredIn string
}

type schema struct {
	order  []string
	tables map[string]*table
}

func newSchema() *schema {
	return &schema{tables: map[string]*table{}}
}

func (s *schema) indexCount() int {
	n := 0
	for _, t := range s.tables {
		n += len(t.indexes)
	}
	return n
}

func (s *schema) autoIncrementCount() int {
	n := 0
	for _, t := range s.tables {
		for _, c := range t.columns {
			if strings.Contains(c.def, "AUTOINCREMENT") {
				n++
				break
			}
		}
	}
	return n
}

func (t *table) columnIndex(name string) int {
	for i, c := range t.columns {
		if c.name == name {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// statement application
// ---------------------------------------------------------------------------

var (
	createTableRe = regexp.MustCompile(`(?is)^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + "`?" + `"?([A-Za-z0-9_]+)"?` + "`?" + `\s*\((.*)\)\s*(?:ENGINE.*|COMMENT.*)?$`)
	createIndexRe = regexp.MustCompile(`(?is)^CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?` + "`?" + `"?([A-Za-z0-9_]+)"?` + "`?" + `\s+ON\s+` + "`?" + `"?([A-Za-z0-9_]+)"?` + "`?" + `\s*\(([^)]*)\)`)
	alterTableRe  = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+` + "`?" + `"?([A-Za-z0-9_]+)"?` + "`?" + `\s+(.*)$`)
	identListRe   = regexp.MustCompile(`[A-Za-z0-9_]+`)
)

func (s *schema) apply(stmt, file string) error {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return nil
	}
	upper := strings.ToUpper(stmt)
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		return s.applyCreateTable(stmt, file)
	case strings.HasPrefix(upper, "CREATE INDEX"), strings.HasPrefix(upper, "CREATE UNIQUE INDEX"):
		return s.applyCreateIndex(stmt)
	case strings.HasPrefix(upper, "ALTER TABLE"):
		return s.applyAlterTable(stmt)
	default:
		// SET NAMES, INSERT, DROP, and other non-DDL statements do not shape the
		// canonical table model.
		return nil
	}
}

func (s *schema) applyCreateTable(stmt, file string) error {
	m := createTableRe.FindStringSubmatch(stmt)
	if m == nil {
		return fmt.Errorf("unrecognised CREATE TABLE statement: %s", firstLine(stmt))
	}
	name := unquote(m[1])

	// A later CREATE TABLE IF NOT EXISTS is a runtime no-op in MariaDB, so the
	// first declaration stays authoritative and only ALTER TABLE mutates it.
	if _, exists := s.tables[name]; exists {
		return nil
	}
	t := &table{name: name, declaredIn: file}
	for _, part := range splitTopLevel(m[2]) {
		if err := t.applyDefinition(part); err != nil {
			return fmt.Errorf("table %s: %w", name, err)
		}
	}
	s.tables[name] = t
	s.order = append(s.order, name)
	return nil
}

func (t *table) applyDefinition(part string) error {
	part = strings.TrimSpace(part)
	upper := strings.ToUpper(part)
	switch {
	case hasKeyword(upper, "PRIMARY KEY"):
		cols := parseColumnList(parenList(part))
		if len(cols) == 0 {
			return fmt.Errorf("unparsable primary key: %s", part)
		}
		t.primaryKey = strings.Join(quoteAll(cols), ", ")
	case hasKeyword(upper, "UNIQUE KEY"), hasKeyword(upper, "UNIQUE INDEX"), hasKeyword(upper, "UNIQUE"):
		cols := parseColumnList(parenList(part))
		if len(cols) == 0 {
			return fmt.Errorf("unparsable unique constraint: %s", part)
		}
		t.uniques = append(t.uniques, strings.Join(quoteAll(cols), ", "))
	case hasKeyword(upper, "INDEX"), hasKeyword(upper, "KEY"):
		head := indexHeadRe.FindStringSubmatch(part)
		if head == nil {
			return fmt.Errorf("unparsable index: %s", part)
		}
		cols := parseColumnList(parenList(part))
		if len(cols) == 0 {
			return fmt.Errorf("unparsable index columns: %s", part)
		}
		t.indexes = append(t.indexes, indexDef{name: unquote(head[1]), cols: cols})
	case hasKeyword(upper, "CONSTRAINT"):
		// CONSTRAINT introduces either a named foreign key or a named check.
		switch {
		case strings.Contains(upper, "FOREIGN KEY"):
			fk, err := parseForeignKey(part)
			if err != nil {
				return err
			}
			t.fks = append(t.fks, fk)
		case strings.Contains(upper, "CHECK"):
			translated, err := translateCheckRegexp(part)
			if err != nil {
				return err
			}
			t.checks = append(t.checks, translated)
		default:
			return fmt.Errorf("unsupported CONSTRAINT clause: %s", part)
		}
	case hasKeyword(upper, "FOREIGN KEY"):
		fk, err := parseForeignKey(part)
		if err != nil {
			return err
		}
		t.fks = append(t.fks, fk)
	case hasKeyword(upper, "CHECK"):
		translated, err := translateCheckRegexp(part)
		if err != nil {
			return err
		}
		t.checks = append(t.checks, translated)
	default:
		col, err := parseColumn(part)
		if err != nil {
			return err
		}
		if idx := t.columnIndex(col.name); idx >= 0 {
			t.columns[idx] = col
			return nil
		}
		t.columns = append(t.columns, col)
	}
	return nil
}

// checkRegexpRe matches the only REGEXP form the canonical schema uses: an
// anchored, fixed-length character-class test such as '^[0-9a-f]{64}$'.
var checkRegexpRe = regexp.MustCompile(`(?i)([A-Za-z_][A-Za-z0-9_.]*)\s+REGEXP\s+'\^\[([^\]]+)\]\{(\d+)\}\$'`)

// translateCheckRegexp rewrites MariaDB's REGEXP operator inside a CHECK
// constraint into an equivalent SQLite expression.
//
// SQLite has no REGEXP operator by default. The canonical schema uses REGEXP
// only for anchored fixed-length character-class validation of digest columns,
// for example manifest_sha256 REGEXP '^[0-9a-f]{64}$'. That maps exactly to a
// length test plus a negated GLOB: a NOT GLOB '*[^0-9a-f]*' is true when the
// value contains no character outside the class, and the length test fixes the
// count, which matches the anchored {64} quantifier. Case sensitivity is
// preserved because GLOB is case-sensitive, as is the MariaDB regex.
//
// Any REGEXP the translation cannot express is an error, not a silent drop, so
// an unsupported pattern cannot slip through unvalidated.
func translateCheckRegexp(check string) (string, error) {
	out := checkRegexpRe.ReplaceAllString(check, "(length($1) = $3 AND $1 NOT GLOB '*[^$2]*')")
	if strings.Contains(strings.ToUpper(out), "REGEXP") {
		return "", fmt.Errorf("unsupported REGEXP in CHECK constraint: %s", firstLine(check))
	}
	return out, nil
}

// hasKeyword reports whether an upper-cased clause starts with the given SQL
// keyword followed by whitespace, an opening parenthesis, or an identifier
// quote. Without the boundary check a column such as key_points_json would be
// mistaken for a KEY clause and a column such as unique_id for a UNIQUE clause.
func hasKeyword(upper, keyword string) bool {
	if !strings.HasPrefix(upper, keyword) {
		return false
	}
	rest := upper[len(keyword):]
	if rest == "" {
		return true
	}
	switch rest[0] {
	case ' ', '\t', '\n', '\r', '(', '`', '"':
		return true
	default:
		return false
	}
}

var (
	fkRe        = regexp.MustCompile(`(?is)FOREIGN\s+KEY\s*\(([^)]*)\)\s*REFERENCES\s+` + "`?" + `"?([A-Za-z0-9_]+)"?` + "`?" + `\s*\(([^)]*)\)\s*((?:ON\s+(?:DELETE|UPDATE)\s+(?:CASCADE|SET\s+NULL|RESTRICT|NO\s+ACTION)\s*)*)`)
	fkNameRe    = regexp.MustCompile(`(?is)^CONSTRAINT\s+` + "`?" + `"?([A-Za-z0-9_]+)"?` + "`?" + `\s+`)
	indexHeadRe = regexp.MustCompile(`(?is)^(?:INDEX|KEY)\s+` + "`?" + `"?([A-Za-z0-9_]+)"?` + "`?" + `\s*\(`)
	autoIncRe   = regexp.MustCompile(`(?i)AUTO_INCREMENT`)
)

// parenList returns the contents of the first balanced parenthesis group.
func parenList(clause string) string {
	start := strings.IndexByte(clause, '(')
	if start < 0 {
		return ""
	}
	depth := 0
	for i := start; i < len(clause); i++ {
		switch clause[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return clause[start+1 : i]
			}
		}
	}
	return ""
}

// parseColumnList extracts column names from a MariaDB index or constraint
// column list.
//
// MariaDB allows prefix lengths and sort directions, for example
// `UNIQUE KEY uq (content(191), created_at DESC)`. Emitting the length as a
// column would make SQLite reject the constraint with "expressions prohibited
// in PRIMARY KEY and UNIQUE constraints", so the prefix length and direction are
// dropped and only the column name is kept.
func parseColumnList(list string) []string {
	var out []string
	for _, raw := range splitTopLevel(list) {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		if idx := strings.IndexByte(item, '('); idx >= 0 {
			item = strings.TrimSpace(item[:idx])
		}
		if fields := strings.Fields(item); len(fields) > 0 {
			item = fields[0] // drops a trailing ASC/DESC
		}
		if name := unquote(item); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func parseForeignKey(part string) (foreignKey, error) {
	m := fkRe.FindStringSubmatch(part)
	if m == nil {
		return foreignKey{}, fmt.Errorf("unparsable foreign key: %s", part)
	}
	fk := foreignKey{
		cols:     parseColumnList(m[1]),
		refTable: unquote(m[2]),
		refCols:  parseColumnList(m[3]),
		actions:  strings.Join(strings.Fields(strings.ToUpper(m[4])), " "),
	}
	if nm := fkNameRe.FindStringSubmatch(part); nm != nil {
		fk.name = unquote(nm[1])
	}
	return fk, nil
}

// ---------------------------------------------------------------------------
// column parsing and type normalisation
// ---------------------------------------------------------------------------

func parseColumn(part string) (column, error) {
	fields := strings.Fields(part)
	if len(fields) < 1 {
		return column{}, fmt.Errorf("empty column definition")
	}
	name := unquote(fields[0])
	def := strings.TrimSpace(strings.TrimPrefix(part, fields[0]))
	if name == "" || def == "" {
		return column{}, fmt.Errorf("unparsable column definition: %s", part)
	}
	return column{name: name, def: normaliseType(def)}, nil
}

var (
	reUnsigned      = regexp.MustCompile(`(?i)\b(UNSIGNED|ZEROFILL)\b`)
	reCharset       = regexp.MustCompile(`(?i)\b(CHARACTER\s+SET|COLLATE)\s+[A-Za-z0-9_]+`)
	reComment       = regexp.MustCompile(`(?is)\bCOMMENT\s+'(?:[^']|'')*'`)
	reIntType       = regexp.MustCompile(`(?i)\b(BIGINT|MEDIUMINT|SMALLINT|TINYINT|INTEGER|INT)\b`)
	reTextType      = regexp.MustCompile(`(?i)\b(VARCHAR|CHAR|LONGTEXT|MEDIUMTEXT|TINYTEXT|TEXT)\b(\s*\(\s*\d+\s*\))?`)
	reJSONType      = regexp.MustCompile(`(?i)\bJSON\b`)
	reTimeType      = regexp.MustCompile(`(?i)\b(DATETIME|TIMESTAMP|DATE|TIME)\b(\s*\(\s*\d+\s*\))?`)
	reRealType      = regexp.MustCompile(`(?i)\b(DOUBLE|FLOAT|DECIMAL|NUMERIC)\b(\s*\([^)]*\))?`)
	reBoolType      = regexp.MustCompile(`(?i)\b(BOOLEAN|BOOL)\b`)
	reBlobType      = regexp.MustCompile(`(?i)\b(VARBINARY|BINARY|BLOB)\b(\s*\(\s*\d+\s*\))?`)
	reCurrentTS     = regexp.MustCompile(`(?i)DEFAULT\s+CURRENT_TIMESTAMP(\s*\(\s*\d+\s*\))?`)
	reOnUpdateTS    = regexp.MustCompile(`(?i)\bON\s+UPDATE\s+CURRENT_TIMESTAMP(\s*\(\s*\d+\s*\))?`)
	reDefaultFalse  = regexp.MustCompile(`(?i)DEFAULT\s+FALSE\b`)
	reDefaultTrue   = regexp.MustCompile(`(?i)DEFAULT\s+TRUE\b`)
	reSpace         = regexp.MustCompile(`\s+`)
	reInlinePrimary = regexp.MustCompile(`(?i)\bPRIMARY\s+KEY\b`)
	// MariaDB's ADD COLUMN ... AFTER col / FIRST has no SQLite equivalent;
	// column order is not observable in SQLite, so the position is dropped.
	reColumnPosition = regexp.MustCompile("(?i)\\s+(AFTER\\s+[A-Za-z0-9_`\"]+|FIRST)$")
	// MariaDB spells stored generated columns PERSISTENT; SQLite spells it STORED.
	rePersistent = regexp.MustCompile(`(?i)\bPERSISTENT\b`)
)

// normaliseType maps a MariaDB column definition to its SQLite equivalent.
//
// AUTO_INCREMENT primary keys become INTEGER PRIMARY KEY AUTOINCREMENT. This is
// mandatory for parity: MariaDB never reuses AUTO_INCREMENT values, while a bare
// SQLite INTEGER PRIMARY KEY reuses them once the highest row is deleted, which
// would break canonical numeric ids and their vector references.
func normaliseType(def string) string {
	def = strings.TrimSpace(def)
	def = reColumnPosition.ReplaceAllString(def, "")
	def = reComment.ReplaceAllString(def, "")
	def = reCharset.ReplaceAllString(def, "")
	def = reUnsigned.ReplaceAllString(def, "")
	def = rePersistent.ReplaceAllString(def, "STORED")

	if autoIncRe.MatchString(def) {
		rest := autoIncRe.ReplaceAllString(def, "")
		rest = reInlinePrimary.ReplaceAllString(rest, "")
		rest = reIntType.ReplaceAllString(rest, "")
		rest = strings.Trim(strings.TrimSpace(reSpace.ReplaceAllString(rest, " ")), ",")
		// Any residual NOT NULL / DEFAULT on an autoincrement key is dropped:
		// SQLite's INTEGER PRIMARY KEY AUTOINCREMENT is always NOT NULL and
		// auto-assigned.
		rest = regexp.MustCompile(`(?i)\bNOT\s+NULL\b`).ReplaceAllString(rest, "")
		rest = regexp.MustCompile(`(?i)\bDEFAULT\s+[A-Za-z0-9_']+`).ReplaceAllString(rest, "")
		rest = strings.TrimSpace(reSpace.ReplaceAllString(rest, " "))
		if rest == "" {
			return "INTEGER PRIMARY KEY AUTOINCREMENT"
		}
		return "INTEGER PRIMARY KEY AUTOINCREMENT " + rest
	}

	def = reTextType.ReplaceAllString(def, "TEXT")
	def = reJSONType.ReplaceAllString(def, "TEXT")
	def = reTimeType.ReplaceAllString(def, "TEXT")
	def = reBlobType.ReplaceAllString(def, "BLOB")
	def = reRealType.ReplaceAllString(def, "REAL")
	def = reBoolType.ReplaceAllString(def, "INTEGER")
	def = reIntType.ReplaceAllString(def, "INTEGER")

	def = reCurrentTS.ReplaceAllString(def, "DEFAULT CURRENT_TIMESTAMP")
	// MariaDB's ON UPDATE CURRENT_TIMESTAMP has no SQLite equivalent; the
	// application owns updated_at values instead, so the attribute is dropped.
	def = reOnUpdateTS.ReplaceAllString(def, "")
	def = reDefaultFalse.ReplaceAllString(def, "DEFAULT 0")
	def = reDefaultTrue.ReplaceAllString(def, "DEFAULT 1")
	def = strings.TrimSpace(reSpace.ReplaceAllString(def, " "))
	return def
}

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

func (s *schema) applyCreateIndex(stmt string) error {
	m := createIndexRe.FindStringSubmatch(stmt)
	if m == nil {
		return fmt.Errorf("unrecognised CREATE INDEX statement: %s", firstLine(stmt))
	}
	unique := strings.TrimSpace(m[1]) != ""
	name := unquote(m[2])
	tbl := unquote(m[3])
	cols := parseColumnList(m[4])
	if len(cols) == 0 {
		return fmt.Errorf("CREATE INDEX %s has no columns", name)
	}
	t := s.tables[tbl]
	if t == nil {
		return fmt.Errorf("CREATE INDEX on unknown table %s", tbl)
	}
	if unique {
		t.uniques = append(t.uniques, strings.Join(quoteAll(cols), ", "))
		return nil
	}
	t.indexes = append(t.indexes, indexDef{name: name, cols: cols})
	return nil
}

func (s *schema) applyAlterTable(stmt string) error {
	m := alterTableRe.FindStringSubmatch(stmt)
	if m == nil {
		return fmt.Errorf("unrecognised ALTER TABLE statement: %s", firstLine(stmt))
	}
	name := unquote(m[1])
	t := s.tables[name]
	if t == nil {
		return fmt.Errorf("ALTER TABLE on unknown table %s", name)
	}
	// One ALTER TABLE may carry several comma-separated actions, for example
	//   ALTER TABLE t
	//     ADD COLUMN a ... AFTER id,
	//     ADD COLUMN b ... AFTER a;
	// so each top-level action is applied in order. splitTopLevel keeps commas
	// inside parentheses (DECIMAL(10,2), CHECK (... IN ('a','b'))) intact.
	for _, action := range splitTopLevel(m[2]) {
		if err := t.applyAlterAction(strings.TrimSpace(action)); err != nil {
			return fmt.Errorf("ALTER TABLE %s: %w", name, err)
		}
	}
	return nil
}

func (t *table) applyAlterAction(action string) error {
	if action == "" {
		return nil
	}
	upper := strings.ToUpper(action)
	switch {
	case strings.HasPrefix(upper, "ADD COLUMN"), strings.HasPrefix(upper, "ADD "):
		rest := action
		if strings.HasPrefix(upper, "ADD COLUMN") {
			rest = action[len("ADD COLUMN"):]
		} else {
			rest = action[len("ADD"):]
		}
		rest = strings.TrimSpace(rest)
		if strings.HasPrefix(strings.ToUpper(rest), "IF NOT EXISTS") {
			rest = strings.TrimSpace(rest[len("IF NOT EXISTS"):])
		}
		// ADD CONSTRAINT / ADD FOREIGN KEY / ADD INDEX / ADD UNIQUE are handled
		// through the shared definition parser.
		return t.applyDefinition(rest)
	case strings.HasPrefix(upper, "MODIFY"), strings.HasPrefix(upper, "CHANGE"):
		rest := action
		if strings.HasPrefix(upper, "MODIFY COLUMN") {
			rest = action[len("MODIFY COLUMN"):]
		} else if strings.HasPrefix(upper, "MODIFY") {
			rest = action[len("MODIFY"):]
		} else if strings.HasPrefix(upper, "CHANGE COLUMN") {
			rest = action[len("CHANGE COLUMN"):]
		} else {
			rest = action[len("CHANGE"):]
		}
		return t.applyDefinition(strings.TrimSpace(rest))
	case strings.HasPrefix(upper, "DROP COLUMN"):
		col := strings.TrimSpace(action[len("DROP COLUMN"):])
		idx := t.columnIndex(unquote(col))
		if idx >= 0 {
			t.columns = append(t.columns[:idx], t.columns[idx+1:]...)
		}
		return nil
	case strings.HasPrefix(upper, "DROP CONSTRAINT"):
		nm := strings.TrimSpace(action[len("DROP CONSTRAINT"):])
		nm = strings.TrimPrefix(nm, "IF EXISTS ")
		want := unquote(nm)
		kept := t.checks[:0]
		for _, c := range t.checks {
			if !strings.Contains(strings.ToLower(c), strings.ToLower(want)) {
				kept = append(kept, c)
			}
		}
		t.checks = kept
		return nil
	case strings.HasPrefix(upper, "DROP FOREIGN KEY"):
		nm := strings.TrimSpace(action[len("DROP FOREIGN KEY"):])
		nm = strings.TrimPrefix(nm, "IF EXISTS ")
		want := unquote(nm)
		kept := t.fks[:0]
		for _, fk := range t.fks {
			if fk.name != want {
				kept = append(kept, fk)
			}
		}
		t.fks = kept
		return nil
	default:
		// Unsupported ALTER actions must be loud rather than silently dropped.
		return fmt.Errorf("unsupported action on %s: %s", t.name, firstLine(action))
	}
}

// ---------------------------------------------------------------------------
// rendering
// ---------------------------------------------------------------------------

func (s *schema) validate() error {
	if len(s.tables) == 0 {
		return fmt.Errorf("no tables parsed")
	}
	for _, name := range s.order {
		t := s.tables[name]
		if len(t.columns) == 0 {
			return fmt.Errorf("table %s has no columns", name)
		}
		for _, fk := range t.fks {
			ref := s.tables[fk.refTable]
			if ref == nil {
				return fmt.Errorf("table %s references unknown table %s", name, fk.refTable)
			}
			// SQLite does not validate that a referenced column exists at CREATE
			// TABLE time (forward references are allowed), so a parse error in a
			// foreign key would otherwise reach the database unnoticed. Column
			// existence is checked here instead.
			for _, col := range fk.cols {
				if t.columnIndex(col) < 0 {
					return fmt.Errorf("table %s foreign key references unknown local column %s", name, col)
				}
			}
			for _, col := range fk.refCols {
				if ref.columnIndex(col) < 0 {
					return fmt.Errorf("table %s foreign key references unknown column %s.%s", name, fk.refTable, col)
				}
			}
		}
		for _, u := range t.uniques {
			for _, col := range parseColumnList(u) {
				if t.columnIndex(col) < 0 {
					return fmt.Errorf("table %s unique constraint references unknown column %s", name, col)
				}
			}
		}
		for _, idx := range t.indexes {
			for _, col := range idx.cols {
				if t.columnIndex(col) < 0 {
					return fmt.Errorf("table %s index %s references unknown column %s", name, idx.name, col)
				}
			}
		}
	}
	return nil
}

func (s *schema) render(paths []string) string {
	var b strings.Builder
	b.WriteString("-- Archive Center Cloudflare D1 - consolidated canonical schema.\n")
	b.WriteString("--\n")
	b.WriteString("-- GENERATED FILE. DO NOT EDIT BY HAND.\n")
	b.WriteString("--\n")
	b.WriteString("-- Produced by:\n")
	b.WriteString("--   go-service/cmd/d1-schema-gen\n")
	b.WriteString("-- from the MariaDB migrations:")
	for _, p := range paths {
		b.WriteString("\n--   migrations/" + filepath.Base(p))
	}
	b.WriteString("\n--\n")
	b.WriteString("-- Why this is a consolidated schema rather than a replay of the source\n")
	b.WriteString("-- migrations: SQLite ALTER TABLE supports only ADD COLUMN, RENAME COLUMN,\n")
	b.WriteString("-- RENAME TO, and DROP COLUMN. It cannot MODIFY a column or add a constraint\n")
	b.WriteString("-- afterwards, so every MariaDB ALTER is folded into each table's final\n")
	b.WriteString("-- shape and declared once, with foreign keys inline.\n")
	b.WriteString("--\n")
	b.WriteString("-- Two type decisions carry parity meaning:\n")
	b.WriteString("--   * BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY becomes\n")
	b.WriteString("--     INTEGER PRIMARY KEY AUTOINCREMENT. A bare INTEGER PRIMARY KEY would\n")
	b.WriteString("--     reuse ids after a full delete, while MariaDB never reuses\n")
	b.WriteString("--     AUTO_INCREMENT values.\n")
	b.WriteString("--   * JSON becomes TEXT. JSON predicates are applied with SQLite json_type()\n")
	b.WriteString("--     and json_extract() through the helpers in internal/store/d1_dialect.go.\n")
	b.WriteString("--\n")
	b.WriteString("-- SQLite internal objects (sqlite_sequence, sqlite_schema, sqlite_autoindex_*)\n")
	b.WriteString("-- are intentionally absent and must never be targeted by the reset allowlist.\n\n")

	for _, name := range s.order {
		t := s.tables[name]
		b.WriteString(fmt.Sprintf("-- %s\n", name))
		b.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", quoteIdent(name)))
		var lines []string
		for _, c := range t.columns {
			lines = append(lines, "    "+quoteIdent(c.name)+" "+c.def)
		}
		if t.primaryKey != "" {
			lines = append(lines, "    PRIMARY KEY ("+t.primaryKey+")")
		}
		for _, u := range t.uniques {
			lines = append(lines, "    UNIQUE ("+u+")")
		}
		for _, fk := range t.fks {
			line := "    FOREIGN KEY (" + strings.Join(quoteAll(fk.cols), ", ") + ") REFERENCES " +
				quoteIdent(fk.refTable) + " (" + strings.Join(quoteAll(fk.refCols), ", ") + ")"
			if fk.actions != "" {
				line += " " + fk.actions
			}
			lines = append(lines, line)
		}
		for _, chk := range t.checks {
			lines = append(lines, "    "+chk)
		}
		b.WriteString(strings.Join(lines, ",\n"))
		b.WriteString("\n);\n")
		if len(t.indexes) > 0 {
			for _, idx := range t.indexes {
				b.WriteString(fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s);\n",
					quoteIdent(idx.name), quoteIdent(name), strings.Join(quoteAll(idx.cols), ", ")))
			}
		}
		b.WriteString("\n")
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteAll(names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, quoteIdent(n))
	}
	return out
}

func unquote(name string) string {
	return strings.Trim(strings.TrimSpace(name), "`\"")
}

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return s[:idx] + " ..."
	}
	if len(s) > 120 {
		return s[:120] + " ..."
	}
	return s
}
