package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/risulongmemory/archive-center-go/internal/cloudflarebridge"
)

// D1 SQL transport.
//
// Cloudflare D1 speaks SQLite over the Worker bridge, not database/sql. The D1
// canonical store therefore talks to this narrow interface instead of *sql.DB,
// so the same statement text runs through both transports:
//
//   - in production, d1BridgeConn forwards each statement to the Worker, which
//     executes it against the D1 binding;
//   - in tests and local verification, a SQLite-backed connector satisfies the
//     same interface, so the dialect exercised there is the dialect D1 runs.
//
// Keeping the store on this interface is also what prevents the MariaDB
// database/sql idioms (FOR UPDATE, LAST_INSERT_ID, MySQL upserts) from leaking
// into the D1 path.

// D1Statement is one parameterised statement inside a D1 batch. A batch is the
// D1 transaction boundary: D1 commits the sequence atomically and rolls the
// whole batch back on failure.
type D1Statement struct {
	SQL  string
	Args []any
}

// D1Conn executes SQL against a D1 database.
type D1Conn interface {
	// QueryRow returns a single-row handle; the caller detects absence through
	// the scan error.
	QueryRow(ctx context.Context, query string, args ...any) D1Row
	// Query returns a cursor over the result set.
	Query(ctx context.Context, query string, args ...any) (D1Rows, error)
	// Exec runs a single statement and reports the affected row count.
	Exec(ctx context.Context, query string, args ...any) (int64, error)
	// Batch runs statements as one atomic D1 transaction.
	Batch(ctx context.Context, statements ...D1Statement) error
}

// D1Row is a single result row.
type D1Row interface {
	Scan(dest ...any) error
}

// D1Rows is a result cursor.
type D1Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// ---------------------------------------------------------------------------
// bridge transport
// ---------------------------------------------------------------------------

// d1QueryPayload is the d1.query request payload.
type d1QueryPayload struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args,omitempty"`
}

// d1QueryResult is the d1.query response result.
type d1QueryResult struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// d1BatchPayload is the d1.batch request payload.
type d1BatchPayload struct {
	Statements []d1BatchStatement `json:"statements"`
}

type d1BatchStatement struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args,omitempty"`
}

// d1BatchResult is the d1.batch response result.
type d1BatchResult struct {
	Changes []int64 `json:"changes"`
}

// d1BridgeConn forwards D1 statements to the Worker bridge.
type d1BridgeConn struct {
	client *cloudflarebridge.Client
}

// NewD1BridgeConn builds a D1 transport over the Container-to-Worker bridge.
func NewD1BridgeConn(client *cloudflarebridge.Client) (D1Conn, error) {
	if client == nil {
		return nil, fmt.Errorf("store: d1 bridge client is required")
	}
	return &d1BridgeConn{client: client}, nil
}

func (c *d1BridgeConn) QueryRow(ctx context.Context, query string, args ...any) D1Row {
	rows, err := c.Query(ctx, query, args...)
	return &d1BridgeRow{rows: rows, err: err}
}

func (c *d1BridgeConn) Query(ctx context.Context, query string, args ...any) (D1Rows, error) {
	var result d1QueryResult
	if err := c.client.Do(ctx, cloudflarebridge.OpD1Query, d1QueryPayload{SQL: query, Args: args}, &result); err != nil {
		return nil, err
	}
	return &d1BridgeRows{result: result, index: -1}, nil
}

func (c *d1BridgeConn) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	var result d1BatchResult
	if err := c.client.Do(ctx, cloudflarebridge.OpD1Batch, d1BatchPayload{
		Statements: []d1BatchStatement{{SQL: query, Args: args}},
	}, &result); err != nil {
		return 0, err
	}
	if len(result.Changes) == 0 {
		return 0, nil
	}
	return result.Changes[0], nil
}

func (c *d1BridgeConn) Batch(ctx context.Context, statements ...D1Statement) error {
	if len(statements) == 0 {
		return nil
	}
	payload := d1BatchPayload{Statements: make([]d1BatchStatement, 0, len(statements))}
	for _, stmt := range statements {
		payload.Statements = append(payload.Statements, d1BatchStatement{SQL: stmt.SQL, Args: stmt.Args})
	}
	var result d1BatchResult
	return c.client.Do(ctx, cloudflarebridge.OpD1Batch, payload, &result)
}

// d1BridgeRows adapts a decoded bridge result to D1Rows.
type d1BridgeRows struct {
	result d1QueryResult
	index  int
}

func (r *d1BridgeRows) Next() bool {
	r.index++
	return r.index < len(r.result.Rows)
}

func (r *d1BridgeRows) Scan(dest ...any) error {
	if r.index < 0 || r.index >= len(r.result.Rows) {
		return fmt.Errorf("store: d1 scan called outside a valid row")
	}
	return d1AssignRow(dest, r.result.Rows[r.index])
}

func (r *d1BridgeRows) Err() error { return nil }

func (r *d1BridgeRows) Close() error { return nil }

// d1BridgeRow adapts a query to the single-row handle.
type d1BridgeRow struct {
	rows D1Rows
	err  error
}

func (r *d1BridgeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.rows == nil {
		return fmt.Errorf("store: no rows available")
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return errD1NoRows
	}
	return r.rows.Scan(dest...)
}

// errD1NoRows reports an empty single-row result. It mirrors sql.ErrNoRows
// without making the store depend on database/sql.
var errD1NoRows = fmt.Errorf("store: d1 query returned no rows")

// d1AssignRow copies decoded JSON values into caller scan destinations. The
// bridge transports JSON, so numbers arrive as float64; integer destinations are
// converted explicitly rather than relying on driver behaviour.
func d1AssignRow(dest []any, values []any) error {
	if len(dest) != len(values) {
		return fmt.Errorf("store: d1 scan expected %d destinations, row has %d columns", len(dest), len(values))
	}
	for i := range dest {
		if err := d1Assign(dest[i], values[i]); err != nil {
			return err
		}
	}
	return nil
}

func d1Assign(dest, value any) error {
	if dest == nil {
		return fmt.Errorf("store: d1 scan destination is nil")
	}
	switch target := dest.(type) {
	case *any:
		*target = value
		return nil
	case **string:
		// NULL stays distinguishable from the empty string so optional text
		// columns round-trip exactly.
		if value == nil {
			*target = nil
			return nil
		}
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("store: d1 scan cannot store %T into **string", value)
		}
		copied := text
		*target = &copied
		return nil
	case *string:
		if value == nil {
			*target = ""
			return nil
		}
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("store: d1 scan cannot store %T into *string", value)
		}
		*target = text
		return nil
	case **int64:
		// NULL stays distinguishable from 0 for optional numeric columns such as
		// superseded_by_id and turn_anchor.
		if value == nil {
			*target = nil
			return nil
		}
		n, err := d1ToInt64(value)
		if err != nil {
			return err
		}
		copied := n
		*target = &copied
		return nil
	case *int64:
		n, err := d1ToInt64(value)
		if err != nil {
			return err
		}
		*target = n
		return nil
	case *int:
		n, err := d1ToInt64(value)
		if err != nil {
			return err
		}
		*target = int(n)
		return nil
	case **float64:
		// NULL stays distinguishable from 0 for optional numeric columns such as
		// canonical_state_layers.confidence.
		if value == nil {
			*target = nil
			return nil
		}
		f, err := d1ToFloat64(value)
		if err != nil {
			return err
		}
		copied := f
		*target = &copied
		return nil
	case *float64:
		f, err := d1ToFloat64(value)
		if err != nil {
			return err
		}
		*target = f
		return nil
	case *bool:
		switch v := value.(type) {
		case nil:
			*target = false
		case bool:
			*target = v
		case float64:
			*target = v != 0
		case int64:
			*target = v != 0
		default:
			return fmt.Errorf("store: d1 scan cannot store %T into *bool", value)
		}
		return nil
	case **time.Time:
		// Optional timestamps such as narrative summary created_at stay NULL
		// rather than being coerced to the zero time.
		if value == nil {
			*target = nil
			return nil
		}
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("store: d1 scan cannot store %T into **time.Time", value)
		}
		parsed, err := parseD1Time(text)
		if err != nil {
			return err
		}
		*target = &parsed
		return nil
	case *time.Time:
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("store: d1 scan cannot store %T into *time.Time", value)
		}
		parsed, err := parseD1Time(text)
		if err != nil {
			return err
		}
		*target = parsed
		return nil
	default:
		return fmt.Errorf("store: d1 scan does not support destination type %T", dest)
	}
}

func d1ToInt64(value any) (int64, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case json.Number:
		return v.Int64()
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("store: d1 scan cannot store %T into an integer destination", value)
	}
}

func d1ToFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case nil:
		return 0, nil
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case json.Number:
		return v.Float64()
	default:
		return 0, fmt.Errorf("store: d1 scan cannot store %T into a float destination", value)
	}
}

// d1TimeLayout is how the D1 canonical schema stores timestamps: TEXT in UTC
// RFC3339 with nanosecond precision. MariaDB DATETIME(3) has millisecond
// precision, so millisecond values round-trip unchanged.
const d1TimeLayout = time.RFC3339Nano

// parseD1Time accepts the RFC3339 representation the D1 store writes, plus the
// space-separated form SQLite's CURRENT_TIMESTAMP default produces, so rows
// created by a schema default remain readable.
func parseD1Time(text string) (time.Time, error) {
	if parsed, err := time.Parse(d1TimeLayout, text); err == nil {
		return parsed.UTC(), nil
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02 15:04:05.999999999"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("store: d1 cannot parse timestamp %q", text)
}

// d1TimeValue renders a Go time for storage. Zero values become the current UTC
// time, matching nonZeroTime on the MariaDB path.
func d1TimeValue(t time.Time) string {
	if t.IsZero() {
		return time.Now().UTC().Format(d1TimeLayout)
	}
	return t.UTC().Format(d1TimeLayout)
}
