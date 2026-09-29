package store

// D1 bound-parameter budget.
//
// Cloudflare D1 rejects a statement that binds more than 100 parameters
// (https://developers.cloudflare.com/d1/platform/limits/). The local test
// harness runs SQLite through the same D1Conn interface, and SQLite allows far
// more than that, so a list that only grows in production fails there and
// nowhere else: a canon pack, a rebuild read, or a long turn is exactly the
// shape that trips it, and nothing in the local suite ever saw it.
//
// Every generated IN list therefore has to be split before it reaches the wire.
// The ceiling below D1's own limit leaves room for the arguments a statement
// binds outside the list (a session id, a work id, a limit) so a caller cannot
// pass a full list and still overflow on the fixed part.

// d1BoundParameterCeiling is the largest IN list this provider will put in one
// statement. D1's documented limit is 100 bound parameters per query, and the
// headroom below it covers the arguments a statement binds outside the list.
const d1BoundParameterCeiling = 90

const d1BoundParameterCeilingNote = "D1: maximum bound parameters per query = 100"

// d1BindChunks splits values into consecutive slices that each fit inside
// d1BoundParameterCeiling. reserved is the number of bound parameters the
// statement spends on everything that is not the list.
//
// Splitting the request rather than capping the list is deliberate. Several of
// these reads are documented as uncapped rebuild reads: bounding them would
// truncate a rebuild into an apparently complete but wrong result, which is
// worse than failing. Chunks preserve the exact result set.
func d1BindChunks[T any](values []T, reserved int) [][]T {
	if len(values) == 0 {
		return nil
	}
	size := d1BoundParameterCeiling - reserved
	if size < 1 {
		// A statement that already spends the whole budget has no room for the
		// list. Returning one value per chunk still lets the caller make
		// progress instead of looping forever on a zero-width slice.
		size = 1
	}
	chunks := make([][]T, 0, (len(values)+size-1)/size)
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}
