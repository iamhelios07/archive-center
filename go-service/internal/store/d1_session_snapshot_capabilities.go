package store

import (
	"context"
)

// D1 session-state aggregate snapshot capability.
//
// The I-1 /session-state response is assembled from eight component projections.
// The HTTP layer can already assemble them from independent Store calls, but that
// path costs eight separate reads, omits RecentChatLogs entirely, and leaves the
// route unable to say anything about how the parts were obtained. Implementing
// SessionStateSnapshotReader moves the assembly into one bounded store read, and
// is the capability the D1 manifest already probes for under that name.
//
// What is deliberately not claimed
//
// The MariaDB read runs its eight queries inside one database/sql read-only
// transaction, so it pins a single connection and a single MVCC snapshot and sets
// SingleConnection to true. The D1 transport cannot make that promise, so this
// implementation reports false rather than repeating a guarantee its transport
// does not offer:
//
//   - D1Conn exposes no transaction, no session, and no connection handle. A read
//     transaction is not merely unused here, it is inexpressible: the interface
//     the store is given has no method that could open one.
//   - Every Query is its own bridge request and its own Worker round trip, which
//     runs a single env.DB.prepare(sql).all() against the D1 binding. Nothing
//     spans those calls, so a concurrent writer can commit between two components
//     and the snapshot can legitimately mix a pre-write and a post-write state.
//   - Batch is the one D1 transaction boundary, and it is a write boundary: the
//     Worker reports only committed row counts, so a batch cannot return the rows
//     a read aggregate is made of.
//
// The flag is consumed by handleSessionState to decide whether the recent-mention
// signal still needs a separate read. Because this snapshot already carries the
// session's chat logs, the route has what it needs from the aggregate itself, and
// the fallback only runs for a session that has no chat logs at all, where the
// extra bounded read cannot return anything either. So the honest false costs no
// response content while keeping the field a statement about the transport rather
// than a claim the bridge cannot back.
//
// TraceMethods is still filled. It records which store methods produced the
// aggregate, which is exactly what the field exists to prove: one bounded store
// read instead of independent per-component calls.

var _ SessionStateSnapshotReader = (*d1Store)(nil)

// d1SessionStateSnapshotTraceMethods names the component reads in the order the
// snapshot assembles them. That order is the MariaDB order, so the first component
// to fail is the same on both providers.
var d1SessionStateSnapshotTraceMethods = []string{
	"ListActiveStates",
	"ListCanonicalStateLayers",
	"ListStorylines",
	"ListCharacterStates",
	"ListWorldRules",
	"ListPendingThreads",
	"ListCharacterEvents",
	"ListChatLogs",
}

// ReadSessionStateSnapshot returns the whole session-state aggregate for one
// chat session.
//
// Every component delegates to the same public method the per-route handlers call.
// Reusing them is the point: the filters, the ordering, the NULL handling, and the
// character manual-edit overlay are then each defined once, so the aggregate read
// and the route's own fallback assembly cannot drift apart and the D1 statement
// text for a projection is never written a second time. Re-deriving the queries
// here would create a second definition of the same projection, which is the one
// thing a parity port must not do.
//
// The empty filters match the MariaDB aggregate: no state type, layer type,
// character name, thread status, or turn range is applied here, because the route
// filters and re-sorts the aggregate itself once it is assembled.
func (s *d1Store) ReadSessionStateSnapshot(ctx context.Context, chatSessionID string) (*SessionStateSnapshot, error) {
	activeStates, err := s.ListActiveStates(ctx, chatSessionID, "")
	if err != nil {
		return nil, err
	}
	canonicalLayers, err := s.ListCanonicalStateLayers(ctx, chatSessionID, "")
	if err != nil {
		return nil, err
	}
	storylines, err := s.ListStorylines(ctx, chatSessionID)
	if err != nil {
		return nil, err
	}
	characters, err := s.ListCharacterStates(ctx, chatSessionID)
	if err != nil {
		return nil, err
	}
	worldRules, err := s.ListWorldRules(ctx, chatSessionID)
	if err != nil {
		return nil, err
	}
	pendingThreads, err := s.ListPendingThreads(ctx, chatSessionID, "")
	if err != nil {
		return nil, err
	}
	characterEvents, err := s.ListCharacterEvents(ctx, chatSessionID, "")
	if err != nil {
		return nil, err
	}
	recentChatLogs, err := s.ListChatLogs(ctx, chatSessionID, 0, 0)
	if err != nil {
		return nil, err
	}

	return &SessionStateSnapshot{
		ActiveStates:         activeStates,
		CanonicalStateLayers: canonicalLayers,
		Storylines:           storylines,
		CharacterStates:      characters,
		WorldRules:           worldRules,
		PendingThreads:       pendingThreads,
		CharacterEvents:      characterEvents,
		RecentChatLogs:       recentChatLogs,
		// False is the truthful answer, not a missing one: the D1 transport issues
		// one statement per bridge request and offers no read transaction that could
		// span them. See the file comment.
		SingleConnection: false,
		// Copied per call so a caller that reorders or truncates the returned trace
		// cannot alter what the next snapshot reports.
		TraceMethods: append([]string(nil), d1SessionStateSnapshotTraceMethods...),
	}, nil
}
