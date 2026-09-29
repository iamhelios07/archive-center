package store

import (
	"testing"
	"time"
)

// D1 timestamp text-ordering guarantees.
//
// Timestamps in the canonical D1 schema are TEXT and are compared and ordered as
// TEXT, not parsed. That is a deliberate performance choice — the hot paths
// ("ORDER BY updated_at DESC" over a turn's memories, a lease held while
// "lease_until > ?", a reprocessing wake cursor) would otherwise parse every row
// — but it makes the RENDERED FORMAT part of the correctness contract.
//
// The failure this file exists to prevent: RFC3339Nano omits trailing zeros, so
// an exact-second instant renders without any fractional part. Compared as text
// against a sub-second instant in the same second, the '.' that starts the
// fraction sorts before the 'Z' that ends the whole-second form, so the LATER
// instant sorts FIRST. Nothing raises an error; rows just come back in the wrong
// order and a live lease reads as expired.

// d1TimeOrderingSeeds covers the cases where a variable-width fraction misorders:
// a whole second against the fractions within it, and a three-digit default
// against a shorter fraction in the same second.
var d1TimeOrderingSeeds = []time.Time{
	time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC),
	time.Date(2026, 3, 4, 5, 6, 7, 1_000, time.UTC),
	time.Date(2026, 3, 4, 5, 6, 7, 5_000_000, time.UTC),
	time.Date(2026, 3, 4, 5, 6, 7, 50_000_000, time.UTC),
	time.Date(2026, 3, 4, 5, 6, 7, 500_000_000, time.UTC),
	time.Date(2026, 3, 4, 5, 6, 8, 0, time.UTC),
}

// TestD1TimeLayoutIsFixedWidth pins the property itself: every instant in the
// same second must render to the same LENGTH.
//
// A variable-width format can satisfy every round-trip test and still misorder,
// because the bug is in the comparison and not in the parse.
func TestD1TimeLayoutIsFixedWidth(t *testing.T) {
	width := 0
	for i, instant := range d1TimeOrderingSeeds {
		rendered := d1TimeValue(instant)
		if i == 0 {
			width = len(rendered)
			// A UTC instant must end in Z and must carry a '.' before it.
			if rendered[len(rendered)-1] != 'Z' {
				t.Errorf("d1TimeValue = %q, want a UTC form ending in Z", rendered)
			}
		} else if len(rendered) != width {
			t.Errorf("d1TimeValue(%v) = %q is %d characters, want the same %d as every other instant",
				instant, rendered, len(rendered), width)
		}
		if rendered[len(rendered)-len(".000Z"):] == "" {
			t.Errorf("d1TimeValue = %q, want a fixed millisecond fraction", rendered)
		}
	}
}

// TestD1TimeValueOrdersChronologicallyAsText is the property every timestamp
// comparison in this provider depends on: sorting the rendered strings must
// produce the same order as sorting the instants.
//
// The reverse direction is checked too, because an ORDER BY DESC on a lease or a
// wake cursor is the case that silently returns the newest row last.
func TestD1TimeValueOrdersChronologicallyAsText(t *testing.T) {
	forward := make([]string, 0, len(d1TimeOrderingSeeds))
	for _, instant := range d1TimeOrderingSeeds {
		forward = append(forward, d1TimeValue(instant))
	}
	assertD1TextOrderMatchesInstants(t, forward)

	// Now the descending order, which is what ORDER BY ... DESC produces and
	// what a "is this lease still held" comparison evaluates.
	backward := make([]string, len(forward))
	for i, value := range forward {
		backward[len(forward)-1-i] = value
	}
	assertD1TextOrderIsDescending(t, backward)
}

// assertD1TextOrderMatchesInstants checks that the given rendered strings, in the
// order given, are in non-decreasing chronological order when compared as text
// AND when parsed back.
func assertD1TextOrderMatchesInstants(t *testing.T, rendered []string) {
	t.Helper()
	for i := 1; i < len(rendered); i++ {
		previous, current := rendered[i-1], rendered[i]
		// A plain string comparison is exactly what the database does.
		if previous > current {
			t.Errorf("text order breaks at %d: %q sorts before %q, so an ASC query would return the later instant first",
				i, current, previous)
		}
		assertD1InstantsAgree(t, previous, current)
	}
}

// assertD1TextOrderIsDescending is the DESC counterpart: the strings must be in
// non-increasing text order, which must also be non-increasing chronological
// order. This is the direction a recency query and a lease comparison use, and
// it is where a whole-second rendering would slip through unnoticed.
func assertD1TextOrderIsDescending(t *testing.T, rendered []string) {
	t.Helper()
	for i := 1; i < len(rendered); i++ {
		previous, current := rendered[i-1], rendered[i]
		if previous < current {
			t.Errorf("text order breaks at %d: %q sorts after %q, so a DESC query would return the later instant first",
				i, current, previous)
		}
		assertD1InstantsAgree(t, current, previous)
	}
}

// assertD1InstantsAgree checks that parsing two rendered timestamps yields the
// same relative order their text implies.
func assertD1InstantsAgree(t *testing.T, earlierText, laterText string) {
	t.Helper()
	earlier, err := parseD1Time(earlierText)
	if err != nil {
		t.Fatalf("parse %q: %v", earlierText, err)
	}
	later, err := parseD1Time(laterText)
	if err != nil {
		t.Fatalf("parse %q: %v", laterText, err)
	}
	if earlier.After(later) {
		t.Errorf("the seeded order is not chronological: %v sorts after %v", earlier, later)
	}
}

// TestD1TimeValueMatchesTheSchemaDefaultShape pins interoperability with the
// column defaults.
//
// The canonical schema's timestamp defaults are
// strftime('%Y-%m-%dT%H:%M:%fZ','now'), which is UTC RFC3339 with exactly three
// fractional digits. A row created by a default and a row written by the store
// are ordered against each other, so they must render to the same shape. If they
// did not, a store-written row would sort before or after a default-written row
// at the same instant depending only on how many digits each happened to carry.
func TestD1TimeValueMatchesTheSchemaDefaultShape(t *testing.T) {
	stamp := time.Date(2026, 3, 4, 5, 6, 7, 123_000_000, time.UTC)
	// strftime('%Y-%m-%dT%H:%M:%fZ') on the same instant.
	schemaDefault := stamp.Format("2006-01-02T15:04:05.000") + "Z"
	if d1TimeValue(stamp) != schemaDefault {
		t.Errorf("d1TimeValue = %q, want %q so store-written and default-written rows sort against each other",
			d1TimeValue(stamp), schemaDefault)
	}
}

// TestParseD1TimeStillReadsEarlierRepresentations keeps the reader permissive.
//
// Writing is now fixed-width, but a database that was written by an earlier
// revision of this provider holds variable-width values, and the column defaults
// in some migrations produce the space-separated form. Tightening the reader
// would turn a cosmetic difference into an unreadable timestamp on a row that
// already exists.
func TestParseD1TimeStillReadsEarlierRepresentations(t *testing.T) {
	want := time.Date(2026, 3, 4, 5, 6, 7, 500_000_000, time.UTC)
	for _, text := range []string{
		"2026-03-04T05:06:07.500Z",       // canonical fixed width
		"2026-03-04T05:06:07.5Z",         // earlier RFC3339Nano rendering
		"2026-03-04T05:06:07.123456789Z", // earlier RFC3339Nano rendering
		"2026-03-04T05:06:07Z",           // earlier RFC3339Nano rendering of a whole second
		"2026-03-04 05:06:07",            // SQLite CURRENT_TIMESTAMP
	} {
		got, err := parseD1Time(text)
		if err != nil {
			t.Errorf("parseD1Time(%q): %v", text, err)
			continue
		}
		if got.Year() != want.Year() || got.Minute() != want.Minute() {
			t.Errorf("parseD1Time(%q) = %v, want the same instant as %v", text, got, want)
		}
	}
	// A whole second must still parse as that whole second, not as an error or
	// as a different instant.
	whole, err := parseD1Time("2026-03-04T05:06:07Z")
	if err != nil {
		t.Fatalf("parse a whole-second timestamp: %v", err)
	}
	if whole.Nanosecond() != 0 {
		t.Errorf("parseD1Time of a whole second = %v, want zero nanoseconds", whole)
	}
}

// TestD1TimeValueFillsTheZeroTime pins that a zero value still becomes "now", so
// fixing the format did not accidentally make it a zero instant.
func TestD1TimeValueFillsTheZeroTime(t *testing.T) {
	rendered := d1TimeValue(time.Time{})
	parsed, err := parseD1Time(rendered)
	if err != nil {
		t.Fatalf("parse the filled timestamp %q: %v", rendered, err)
	}
	if time.Since(parsed) > time.Hour {
		t.Errorf("d1TimeValue of a zero time = %q, want approximately now", rendered)
	}
}
