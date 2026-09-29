package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// D1 worldline topology capability: the bounded read that assembles one
// stable-character session family into the shape the presentation layer draws.
//
// What this read is
//
// It is a graph read, not a row read. Membership comes from the route-binding
// ledger, which canonical session belongs to which stable character. Edges come
// from session_fork_lineage, which session was copied from which parent and at
// which turn. Nodes come from chat_logs, which turns actually exist and are
// complete. The consumer, buildWorldlineTopologyViewModel in
// group_step23_fork_lineage.go, type-switches on WorldlineTopologySnapshotStore
// and draws exactly these three, so a family missing any one of them still
// serialises as a well-formed ViewModel. It just draws the wrong graph: no
// completed turns means no turn nodes at all, and a family cut before the parent
// means the child is marked parent_outside_current_family and its fork edge
// never appears.
//
// The bound is applied before the assembly, so the sort keys decide membership
//
// Both limits are enforced by SQL, and both ORDER BY clauses are what make the
// cut mean something. The family read asks for limit + 1 rows ordered anchor-first
// and then by canonical_session_id, so the surviving set is the anchor plus the
// lexicographically smallest siblings. Reordering that read does not merely
// reshuffle the answer: it changes WHICH sessions end up in the IN list that
// bounds the two reads after it, so a lost CASE key would keep a different
// sibling and drop the anchor, and a ViewModel built from that family would
// render a fork edge to a session the snapshot does not contain.
//
// The turn read is the same story at a larger scale. It orders by turn_index and
// then by chat_session_id, and takes the first 4096 of an ordered 4097, so the
// cap keeps the LOWEST turn indices and the session tiebreak only decides which
// session is credited first when two sessions share a turn. Without the
// turn_index key the cap would keep whatever 4096 groups the group-by happened to
// emit, and a busy family would then report a hole in its turn range. That is not
// a cosmetic reordering: the consumer raises completed_turn_gap on exactly that
// signal, so a dropped sort key shows up as a permanent false alarm on every
// truncated family rather than as a wrong-looking array.
//
// There is no parent walk to translate
//
// The reference does not chase parents with a recursive CTE, and neither does this
// port. The family is never derived from lineage: it is every active binding row
// that shares the anchor stable character, which is a flat set the ledger already
// holds. Lineage is read afterwards and only for sessions the bounded family
// already contains, so there is no recursive walk here to either translate or
// reimplement. The four reads stay four statements.
//
// What is lost by not holding one transaction
//
// The reference wraps all four in a REPEATABLE READ read-only transaction so
// every statement observes the same snapshot. D1 cannot do that on a read path:
// D1Conn.Batch is the only transaction boundary, it returns just an error, and
// nothing can read a row out of it, so the four statements are sequential and
// each one sees the database as of its own execution.
//
// The consequence is bounded rather than open. Both IN lists are built from the
// family as read in statement two, so a binding inserted after that point cannot
// leak lineage or turns for a session the snapshot does not list, and a session
// that leaves the family between the two reads still has its rows read (they are
// simply discarded by the ViewModel, which filters on the family set). What a
// concurrent writer can still change is the CONTENT of a session already in the
// family: a fork record that appears between the family read and the lineage read
// is read, and a chat log that lands late contributes its turn on the next read.
// Both are resolved as unresolved lineage or a short turn range rather than as a
// wrong edge, and the window is a few milliseconds inside one HTTP request.
//
// MariaDB-only idioms
//
//   - CHAR_LENGTH(TRIM(content)) > 0 becomes LENGTH(TRIM(content)) > 0. Both
//     count characters rather than bytes, so the emptiness test is the same
//     predicate; only the function name is MySQL.
//   - The REPEATABLE READ transaction is replaced by the sequential reads
//     described above. There is no FOR UPDATE here to restate: the reference takes
//     no row lock, because this capability only reads.
//   - imported_at is TEXT here and is ordered by its raw text. The schema default
//     and d1TimeValue both render a fixed three-digit millisecond fraction, so
//     lexicographic order is chronological order and no CAST is introduced.

var _ WorldlineTopologySnapshotStore = (*d1Store)(nil)

// d1WorldlineTopologyAnchorCharacterSQL resolves the stable character an anchor
// session is routed to.
//
// DISTINCT collapses the several host chats that may share one canonical session,
// and LIMIT 2 is the ambiguity detector rather than a bound: a second row means
// the ledger claims the anchor belongs to two characters, which the Go side
// refuses. The ORDER BY is what makes that refusal deterministic, because without
// it the two reported ids would depend on the order rows happen to be stored in.
//
// Only active bindings count. A retired binding that still names the anchor must
// not re-open a family the host has deliberately unrouted.
const d1WorldlineTopologyAnchorCharacterSQL = `
	SELECT DISTINCT stable_character_id
	FROM session_route_bindings
	WHERE canonical_session_id = ? AND binding_state = 'active'
	ORDER BY stable_character_id ASC
	LIMIT 2`

// d1WorldlineTopologyFamilySQL is the family membership read.
//
// GROUP BY collapses the several bindings a stable character has for one canonical
// session. The CASE expression is the whole point of the ordering: the anchor is
// pinned to position zero no matter how it sorts lexicographically, so a family
// cut at the limit always keeps the session the request was made about. Without
// it, an anchor that sorts late in the family would be the row dropped by the
// limit, and the returned graph would be centred on a sibling instead.
const d1WorldlineTopologyFamilySQL = `
	SELECT canonical_session_id
	FROM session_route_bindings
	WHERE stable_character_id = ? AND binding_state = 'active'
	GROUP BY canonical_session_id
	ORDER BY CASE WHEN canonical_session_id = ? THEN 0 ELSE 1 END,
	         canonical_session_id ASC
	LIMIT ?`

// d1WorldlineTopologyCompletedTurnSQL finds the turns that genuinely exist.
//
// A turn counts only when it carries BOTH a non-empty user row and a non-empty
// assistant row, and the two MAX(CASE ...) aggregates are how that is expressed:
// the group is kept only when each side saw at least one qualifying row. The
// alternative, selecting turns and filtering in Go, would be a different read
// with a different bound, and a family of 4096 turns with half of them empty would
// exhaust the cap and truncate the turns that did complete.
//
// turn_index > 0 excludes the starter row, which is not a conversation turn. The
// LOWER and TRIM on role are load bearing: a role stored with padding or mixed
// case is still the user or assistant side of the exchange, and dropping either
// would silently delete the turn from the graph.
//
// ORDER BY turn_index ASC, chat_session_id ASC is what makes LIMIT 4097 mean
// "the lowest 4096 turns", and the priority of the two keys is the part that is
// observable: swapping them groups the result per session instead of per turn,
// and the cap would then keep the earliest turns of whichever session sorts
// first rather than the earliest turns of the family. The second key is kept for
// parity and because it states the contract rather than relying on SQLite
// happening to emit groups in session order, which the GROUP BY key prefix
// already implies here.
const d1WorldlineTopologyCompletedTurnSQL = `
	SELECT chat_session_id, turn_index
	FROM chat_logs
	WHERE chat_session_id IN (%s)
	  AND turn_index > 0
	GROUP BY chat_session_id, turn_index
	HAVING MAX(CASE
	           WHEN LOWER(TRIM(role)) = 'user'
	            AND LENGTH(TRIM(content)) > 0 THEN 1 ELSE 0
	       END) = 1
	   AND MAX(CASE
	           WHEN LOWER(TRIM(role)) = 'assistant'
	            AND LENGTH(TRIM(content)) > 0 THEN 1 ELSE 0
	       END) = 1
	ORDER BY turn_index ASC, chat_session_id ASC
	LIMIT ?`

// d1WorldlinePlaceholders renders n comma-separated bind markers with no
// trailing comma. A trailing comma inside "IN (...)" is a parse error in SQLite
// rather than an empty set, so the marker list is never assembled by appending a
// comma n times.
func d1WorldlinePlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// d1WorldlineAnyArgs converts a session list into bind arguments.
func d1WorldlineAnyArgs(sessionIDs []string) []any {
	args := make([]any, 0, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		args = append(args, sessionID)
	}
	return args
}

// d1WorldlineAnchorCharacterID resolves the one stable character the anchor is
// routed to. An anchor with no active binding reports ErrNotFound, which the
// presentation route renders as current_session_not_routed rather than as an
// empty graph, so the error value is part of the contract and not just a Go
// convenience.
func (s *d1Store) d1WorldlineAnchorCharacterID(ctx context.Context, anchorSessionID string) (string, error) {
	rows, err := s.conn.Query(ctx, d1WorldlineTopologyAnchorCharacterSQL, anchorSessionID)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	stableCharacterIDs := make([]string, 0, 2)
	for rows.Next() {
		var stableCharacterID string
		if err := rows.Scan(&stableCharacterID); err != nil {
			return "", err
		}
		if stableCharacterID = strings.TrimSpace(stableCharacterID); stableCharacterID != "" {
			stableCharacterIDs = append(stableCharacterIDs, stableCharacterID)
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(stableCharacterIDs) == 0 {
		return "", ErrNotFound
	}
	if len(stableCharacterIDs) > 1 {
		return "", errors.New("anchor session is bound to multiple stable characters")
	}
	return stableCharacterIDs[0], nil
}

// d1WorldlineFamilySessionIDs returns the bounded family, one entry per canonical
// session, in the snapshot order.
//
// The caller asks for one row more than the limit so truncation can be detected
// from the result instead of from a second count query; the extra row is dropped
// by the caller and sets the truncated flag. A blank canonical session id is
// dropped here rather than being carried into the two IN lists, because a blank
// bind value would silently widen the read to an empty-string session.
func (s *d1Store) d1WorldlineFamilySessionIDs(ctx context.Context, stableCharacterID, anchorSessionID string, limit int) ([]string, error) {
	rows, err := s.conn.Query(ctx, d1WorldlineTopologyFamilySQL,
		stableCharacterID, anchorSessionID, limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sessionIDs := make([]string, 0, limit+1)
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return nil, err
		}
		if sessionID = strings.TrimSpace(sessionID); sessionID != "" {
			sessionIDs = append(sessionIDs, sessionID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sessionIDs, nil
}

// d1WorldlineLineageRecords reads the raw fork lineage of the bounded family.
//
// The projection and the NULL normalisation are the shared fork-lineage read, not
// a second copy: scope ids, fork turn, and the inherited-items map stay NULL in
// the table and are read as the zero values the MariaDB scan produces, so a
// lineage row that has no parent scope and a lineage row whose parent scope is the
// empty string are indistinguishable downstream on both providers.
//
// ORDER BY chat_session_id ASC, imported_at DESC, id DESC is the newest-import
// order the fork-lineage read uses, and the id tiebreak is load bearing: a session
// re-imported twice at the same instant is ordered by the later write rather
// than by whatever order the rows are stored in.
func (s *d1Store) d1WorldlineLineageRecords(ctx context.Context, sessionIDs []string) ([]ForkLineageRecord, error) {
	// The family is capped at 1000 sessions, which is well past what D1 binds in
	// one statement, so a large family failed in deployment while passing every
	// local test. The chunks follow the family's own ordering, so concatenating
	// them in order preserves the ORDER BY the statement already guarantees.
	records := make([]ForkLineageRecord, 0, len(sessionIDs))
	for _, chunk := range d1BindChunks(sessionIDs, 0) {
		rows, err := s.conn.Query(ctx, d1ForkLineageSelect+
			` WHERE chat_session_id IN (`+d1WorldlinePlaceholders(len(chunk))+`)
			  ORDER BY chat_session_id ASC, imported_at DESC, id DESC`,
			d1WorldlineAnyArgs(chunk)...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			record, err := d1ScanForkLineageRecord(rows)
			if err != nil {
				rows.Close()
				return nil, err
			}
			records = append(records, record)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return records, nil
}

// d1WorldlineCompletedTurns reads the completed turns of the bounded family.
//
// One row past the shared cap is requested so truncation is decided by the result
// rather than by a second query, and the caller drops the extra row. The cap is
// the shared worldlineTopologyCompletedTurnLimit constant, not a per-provider
// number, so the two providers truncate the same graph at the same point.
func (s *d1Store) d1WorldlineCompletedTurns(ctx context.Context, sessionIDs []string) ([]WorldlineCompletedTurn, error) {
	// Each chunk asks for one row past the cap so the caller can still decide
	// truncation from the result rather than from a second count query. A chunk
	// cannot contribute more than the whole cap to the global top, so asking for
	// cap+1 per chunk and merging is exact rather than an approximation.
	turns := make([]WorldlineCompletedTurn, 0, len(sessionIDs))
	for _, chunk := range d1BindChunks(sessionIDs, 1) {
		args := append(d1WorldlineAnyArgs(chunk), worldlineTopologyCompletedTurnLimit+1)
		rows, err := s.conn.Query(ctx,
			fmt.Sprintf(d1WorldlineTopologyCompletedTurnSQL, d1WorldlinePlaceholders(len(chunk))),
			args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var turn WorldlineCompletedTurn
			if err := rows.Scan(&turn.ChatSessionID, &turn.TurnIndex); err != nil {
				rows.Close()
				return nil, err
			}
			turn.ChatSessionID = strings.TrimSpace(turn.ChatSessionID)
			if turn.ChatSessionID != "" && turn.TurnIndex > 0 {
				turns = append(turns, turn)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	// The statement orders by (turn_index, chat_session_id) across the whole
	// family. Splitting by session breaks that global order, so the merge
	// restores it before the caller truncates positionally.
	sort.SliceStable(turns, func(i, j int) bool {
		if turns[i].TurnIndex != turns[j].TurnIndex {
			return turns[i].TurnIndex < turns[j].TurnIndex
		}
		return turns[i].ChatSessionID < turns[j].ChatSessionID
	})
	return turns, nil
}

// GetWorldlineTopologySnapshot assembles the bounded read model for one
// stable-character session family.
//
// The four reads run in dependency order: the anchor character decides which
// bindings are in scope, the family decides which sessions the last two reads
// cover, and the last two are independent of each other. Non-positive limit
// means the default family size, matching the reference, so a caller that does
// not care gets the same bounded graph on both providers.
func (s *d1Store) GetWorldlineTopologySnapshot(ctx context.Context, anchorSessionID string, limit int) (WorldlineTopologySnapshot, error) {
	anchorSessionID = strings.TrimSpace(anchorSessionID)
	if anchorSessionID == "" {
		return WorldlineTopologySnapshot{}, errors.New("anchor_session_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	stableCharacterID, err := s.d1WorldlineAnchorCharacterID(ctx, anchorSessionID)
	if err != nil {
		return WorldlineTopologySnapshot{}, err
	}

	snapshot := WorldlineTopologySnapshot{
		StableCharacterID: stableCharacterID,
		AnchorSessionID:   anchorSessionID,
		// All three collections start as non-nil empties. They are handed to the
		// presentation layer as they are, and a nil slice encodes as JSON null
		// where the route contract promises an array.
		SessionIDs:     []string{},
		LineageRecords: []ForkLineageRecord{},
		CompletedTurns: []WorldlineCompletedTurn{},
	}

	sessionIDs, err := s.d1WorldlineFamilySessionIDs(ctx, stableCharacterID, anchorSessionID, limit)
	if err != nil {
		return WorldlineTopologySnapshot{}, err
	}
	if len(sessionIDs) > limit {
		sessionIDs = sessionIDs[:limit]
		snapshot.Truncated = true
	}
	if len(sessionIDs) == 0 {
		return WorldlineTopologySnapshot{}, ErrNotFound
	}
	snapshot.SessionIDs = sessionIDs

	// The truncation above is what bounds the two IN lists, so a family that was
	// cut cannot pull lineage or turns for a session it does not report. That is
	// why the cut is decided before these reads rather than after them.
	lineageRecords, err := s.d1WorldlineLineageRecords(ctx, snapshot.SessionIDs)
	if err != nil {
		return WorldlineTopologySnapshot{}, err
	}
	snapshot.LineageRecords = lineageRecords

	completedTurns, err := s.d1WorldlineCompletedTurns(ctx, snapshot.SessionIDs)
	if err != nil {
		return WorldlineTopologySnapshot{}, err
	}
	if len(completedTurns) > worldlineTopologyCompletedTurnLimit {
		completedTurns = completedTurns[:worldlineTopologyCompletedTurnLimit]
		snapshot.TurnsTruncated = true
	}
	snapshot.CompletedTurns = completedTurns

	return snapshot, nil
}
