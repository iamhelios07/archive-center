package store

import "strings"

// D1/SQLite SQL dialect helpers.
//
// D1 runs SQLite, and the MariaDB implementation uses JSON, identifier, and
// transaction semantics that do not translate mechanically. These helpers
// centralise the portable equivalents so every D1 statement reuses one
// verified predicate instead of re-deriving it per call site.
//
// The behaviours encoded here were established against SQLite directly:
//
//   - json_type(col, path) reports 'true'/'false'/'null'/'text'/'integer'/'real'
//     and SQL NULL when the path is absent.
//   - json_extract(col, path) returns SQL integer 1/0 for JSON booleans, the
//     dequoted SQL text for JSON strings, and SQL NULL for both an explicit
//     JSON null and an absent path.
//
// Two consequences drive these helpers:
//
//  1. MariaDB's JSON_UNQUOTE(JSON_EXTRACT(col, '$.current_projection')) = 'true'
//     matches both a JSON boolean true and a JSON string "true". SQLite's
//     json_extract(...) = 'true' matches only the string, so testing it alone
//     silently drops every JSON-boolean row. The legacy-compatible predicate
//     tested both forms.
//  2. An explicit JSON null is indistinguishable from an absent path through
//     json_extract, so path EXISTENCE must use json_type(...) IS NOT NULL.

// d1QuoteIdent quotes a SQLite identifier so reserved words and mixed casing
// stay safe. Embedded double quotes are doubled per SQL.
func d1QuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// d1QuoteIdentList quotes each identifier and joins them with ", ".
func d1QuoteIdentList(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, d1QuoteIdent(name))
	}
	return strings.Join(quoted, ", ")
}

// d1StringLiteral quotes a SQL string literal, doubling embedded single quotes.
func d1StringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// d1JSONPath renders a SQLite JSON path literal for a single key.
func d1JSONPath(key string) string {
	return d1StringLiteral("$." + key)
}

// d1CurrentProjectionPredicate returns the SQLite predicate equivalent to
// MariaDB's JSON_UNQUOTE(JSON_EXTRACT(col, '$.current_projection')) = 'true'.
//
// It matches the normal JSON boolean true written by the current code and the
// historic JSON string "true", exactly as MariaDB does. Testing only
// json_extract(col, ...) = 'true' would match just the string form and drop the
// boolean form, which is the majority of real rows.
func d1CurrentProjectionPredicate(col string) string {
	path := d1JSONPath("current_projection")
	return "(json_type(" + col + ", " + path + ") = 'true' OR json_extract(" + col + ", " + path + ") = 'true')"
}

// d1JSONPathPresent returns a predicate that is true when a JSON path exists,
// counting an explicit JSON null as present.
//
// MariaDB's JSON_EXTRACT returns JSON null for an explicit null and SQL NULL
// for an absent path, which the existing code distinguishes through
// JSON_CONTAINS_PATH. In SQLite json_extract collapses both to SQL NULL, so
// json_extract(col, path) IS NOT NULL answers "absent" for {"k": null}. Only
// json_type distinguishes 'null' from SQL NULL.
func d1JSONPathPresent(col, path string) string {
	return "json_type(" + col + ", " + path + ") IS NOT NULL"
}

// d1PositiveJSONInteger returns an expression that evaluates to the JSON
// integer at path when it is a positive integer, and to 0 otherwise.
//
// This mirrors the MariaDB intent of accepting only a real positive
// repair_recorded_turn. MariaDB coerces a JSON string "5" through CAST(... AS
// SIGNED); the D1 form deliberately rejects non-integer JSON types so a
// mis-typed value cannot silently reorder the current projection.
func d1PositiveJSONInteger(col, path string) string {
	extract := "json_extract(" + col + ", " + path + ")"
	return "(CASE WHEN json_type(" + col + ", " + path + ") = 'integer' AND " + extract +
		" > 0 THEN " + extract + " ELSE 0 END)"
}

// d1JSONTextOrEmpty returns an expression that evaluates to the JSON text at
// path when the value is a JSON string, and to the empty string otherwise
// (including an explicit null or an absent path).
//
// A non-string value yields the empty string rather than a coerced scalar, so a
// numeric or boolean field cannot be mistaken for a valid string identity.
func d1JSONTextOrEmpty(col, path string) string {
	extract := "json_extract(" + col + ", " + path + ")"
	return "CASE WHEN json_type(" + col + ", " + path + ") = 'text' THEN " + extract + " ELSE '' END"
}

// d1JSONStringBlankPredicate returns a predicate that is true when the JSON
// value at path is absent, an explicit JSON null, or an empty string.
//
// MariaDB collapses an absent path, an explicit JSON null, and an empty string
// to the same blank value, and keeps every other value's own text:
//
//	COALESCE(JSON_UNQUOTE(JSON_EXTRACT(doc, '$.path')), '') = ''
//
// (The statement is shown in an indented block rather than inline so it is not
// rewritten as a typographic quote: the empty string literal is part of the
// expression, not punctuation around it.)
//
// A non-string value is therefore NOT blank: a malformed numeric or boolean
// revision must not pass as a blank one. This cannot be expressed with the
// d1JSONTextOrEmpty helper compared against an empty string, because that form
// maps a JSON number such as 123 to the empty string and would wrongly accept it,
// while MariaDB compares its text '123' and rejects it.
func d1JSONStringBlankPredicate(col, path string) string {
	jsonType := "json_type(" + col + ", " + path + ")"
	return "(" + jsonType + " IS NULL OR " + jsonType + " = 'null' OR (" + jsonType +
		" = 'text' AND json_extract(" + col + ", " + path + ") = ''))"
}

// d1JSONTextEquals returns a predicate that is true only when the JSON value at
// path is a string equal to want.
func d1JSONTextEquals(col, path, want string) string {
	extract := "json_extract(" + col + ", " + path + ")"
	return "(json_type(" + col + ", " + path + ") = 'text' AND " + extract + " = " + d1StringLiteral(want) + ")"
}

// d1StatusObservationTurnSQL mirrors statusObservationTurnSQL: prefer a positive
// repair_recorded_turn, otherwise fall back to source_turn, otherwise 0.
func d1StatusObservationTurnSQL(alias string) string {
	return "COALESCE(NULLIF(" + d1PositiveJSONInteger(alias+".evidence_json", d1JSONPath("repair_recorded_turn")) +
		", 0), " + alias + ".source_turn, 0)"
}

// d1StatusProjectionSourceSQL mirrors statusProjectionSourceSQL: an accepted
// active source revision, or an explicit state_repair.v1 correction that
// carries no source revision.
func d1StatusProjectionSourceSQL(alias, revisionAlias string) string {
	return "(" + revisionAlias + ".lifecycle_state = 'active' OR (" +
		d1JSONStringBlankPredicate(alias+".evidence_json", d1JSONPath("source_revision")) + " AND " +
		d1JSONTextEquals(alias+".evidence_json", d1JSONPath("source_contract"), StateRepairContract) + "))"
}
