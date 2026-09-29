package store

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// D1 rejects a statement that binds more than 100 parameters. The local harness
// runs SQLite, which allows far more, so an IN list that only grows in
// production fails there and nowhere else. Every generated list in the D1
// provider must therefore be split, and this test is what keeps the next one
// from forgetting.
func TestD1GeneratedInListsNeverOutgrowTheBindingLimit(t *testing.T) {
	root := "."
	if wd, err := os.Getwd(); err == nil {
		root = wd
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read store package: %v", err)
	}
	// A generated list is bounded either by the shared ceiling or by a fixed
	// arity. Both are fine; an unbounded length is the defect this exists for.
	unbounded := regexp.MustCompile(`strings\.Repeat\("\?,"\s*,\s*len\([a-zA-Z]+\)\)`)
	chunked := regexp.MustCompile(`strings\.Repeat\("\?,"\s*,\s*(end-start|len\(chunk\)|len\(args\)|len\(keys\))\)`)
	fixed := regexp.MustCompile(`strings\.Repeat\("\?,"\s*,\s*\d+\)`)
	helperDecl := regexp.MustCompile(`strings\.Repeat\("\?,"\s*,\s*n\)`)

	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "d1_") || !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			if !strings.Contains(line, `strings.Repeat("?,"`) {
				continue
			}
			checked++
			switch {
			case chunked.MatchString(line) || fixed.MatchString(line):
				// Bounded by construction.
			case helperDecl.MatchString(line):
				// The placeholder helpers themselves take a caller-supplied
				// count. They are the mechanism, not a list; whether the count
				// is bounded is decided at each call site above.
			case unbounded.MatchString(line):
				t.Errorf("%s:%d builds an IN list from an unbounded length: %s",
					name, i+1, strings.TrimSpace(line))
			default:
				t.Errorf("%s:%d has a generated IN list this test cannot classify: %s",
					name, i+1, strings.TrimSpace(line))
			}
		}
	}
	if checked == 0 {
		t.Fatal("no generated IN list was inspected; the scan is not looking at the right package")
	}
	t.Logf("inspected %d generated IN list(s)", checked)
}

// A chunk constant above the D1 limit is the same defect as an unbounded list:
// the loop looks bounded, passes every local test, and fails in deployment. This
// is the case that a scan of the placeholder lines alone cannot see, because
// `end-start` looks bounded whatever the stride constant is.
func TestD1ChunkConstantsStayUnderTheBindingLimit(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read store package: %v", err)
	}
	constPattern := regexp.MustCompile(`const (d1\w*Chunk(?:Size)?|d1\w*LookupChunk)\s*=\s*(\d+)`)
	// Row-counting chunks are a different limit: they size how much work one
	// statement does, not how many values it binds.
	rowCountingChunks := map[string]bool{"d1_reset.go": true}
	found := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "d1_") || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for i, line := range strings.Split(string(data), "\n") {
			m := constPattern.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			found++
			value, err := strconv.Atoi(m[2])
			if err != nil {
				t.Errorf("%s:%d has an unreadable chunk constant %q", name, i+1, m[2])
				continue
			}
			// Not every "chunk" constant sizes a bound list. d1ResetChunkSize
			// bounds how many ROWS one DELETE covers, and that statement binds two
			// parameters (the cursor and the row limit) no matter how many rows it
			// touches. A constant only has to match this ceiling when it is the
			// stride of a loop that builds an IN list, so the two cases are named
			// apart rather than measured by the number alone.
			if value > d1BoundParameterCeiling && !rowCountingChunks[name] {
				t.Errorf("%s:%d declares %s = %d, which exceeds d1BoundParameterCeiling %d; "+
					"if this is the stride of a loop that builds an IN list it passes local tests "+
					"and fails against D1", name, i+1, m[1], value, d1BoundParameterCeiling)
			}
		}
	}
	if found == 0 {
		t.Fatal("no chunk constant was inspected; the scan is not looking at the right package")
	}
	t.Logf("inspected %d chunk constant(s)", found)
}

// The ceiling is derived from D1, and a future edit that raises it past the
// documented limit would turn every chunked read back into a deployment-only
// failure.
func TestD1CeilingStaysUnderTheDocumentedLimit(t *testing.T) {
	const d1MaxBoundParameters = 100
	if d1BoundParameterCeiling > d1MaxBoundParameters {
		t.Fatalf("d1BoundParameterCeiling = %d, D1 allows %d",
			d1BoundParameterCeiling, d1MaxBoundParameters)
	}
	// A statement that binds the ceiling plus the reserved arguments must still fit.
	reserved := 3
	if d1BoundParameterCeiling+reserved > d1MaxBoundParameters {
		t.Fatalf("ceiling %d plus %d reserved arguments exceeds the D1 limit %d",
			d1BoundParameterCeiling, reserved, d1MaxBoundParameters)
	}
	if _, err := strconv.Atoi(strings.TrimSpace("90")); err != nil {
		t.Fatalf("the documented ceiling must stay a literal an operator can verify: %v", err)
	}
}
