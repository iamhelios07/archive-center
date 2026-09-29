package store

import (
	"strings"
	"testing"
)

// d1BindChunks is what keeps a long list from becoming a production-only
// failure, so the split itself is the contract under test.
func TestD1BindChunksStaysUnderTheD1Ceiling(t *testing.T) {
	for _, size := range []int{1, 89, 90, 91, 180, 1000} {
		values := make([]string, size)
		for i := range values {
			values[i] = "v"
		}
		for reserved := 0; reserved <= 3; reserved++ {
			chunks := d1BindChunks(values, reserved)
			total := 0
			for _, chunk := range chunks {
				total += len(chunk)
				if len(chunk) > d1BoundParameterCeiling {
					t.Fatalf("size=%d reserved=%d produced a chunk of %d, which exceeds %d",
						size, reserved, len(chunk), d1BoundParameterCeiling)
				}
			}
			if total != size {
				t.Fatalf("size=%d reserved=%d lost values: %d of %d", size, reserved, total, size)
			}
		}
	}
}

func TestD1BindChunksPreservesOrderAndEmptyInput(t *testing.T) {
	if chunks := d1BindChunks([]string{}, 0); chunks != nil {
		t.Errorf("an empty list must produce no chunks, got %v", chunks)
	}
	values := make([]string, 250)
	for i := range values {
		values[i] = string(rune('a' + i%26))
	}
	var seen []string
	for _, chunk := range d1BindChunks(values, 1) {
		seen = append(seen, chunk...)
	}
	if len(seen) != len(values) {
		t.Fatalf("reassembled %d values, want %d", len(seen), len(values))
	}
	for i := range values {
		if seen[i] != values[i] {
			t.Fatalf("value %d moved: got %q want %q", i, seen[i], values[i])
		}
	}
}

// A reserved budget that leaves no room must still terminate.
func TestD1BindChunksSurvivesAFullyReservedStatement(t *testing.T) {
	chunks := d1BindChunks([]string{"a", "b", "c"}, d1BoundParameterCeiling+5)
	total := 0
	for _, chunk := range chunks {
		total += len(chunk)
	}
	if total != 3 {
		t.Fatalf("lost values with an over-reserved statement: %d of 3", total)
	}
}

// The ceiling is a D1 property, not a preference. If a future edit raises it
// past what D1 accepts, every guarantee above becomes a production-only failure
// again.
func TestD1BoundParameterCeilingFitsTheD1Limit(t *testing.T) {
	const d1Limit = 100
	if d1BoundParameterCeiling > d1Limit {
		t.Fatalf("d1BoundParameterCeiling = %d exceeds the D1 limit of %d", d1BoundParameterCeiling, d1Limit)
	}
	if strings.TrimSpace(d1BoundParameterCeilingNote) == "" {
		t.Fatal("the D1 limit the ceiling is derived from must stay written down")
	}
}
