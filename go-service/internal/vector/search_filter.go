package vector

import "strings"

// searchFilterValues is the shared, deliberately narrow interpreter for the
// public recall filter language. Both Chroma and Vectorize accept only exact
// equality over these three metadata fields; the durable overlay must apply the
// same interpretation before it can merge a D1 candidate.
func searchFilterValues(sessionID, filter string) (session, tier, sourceTable string) {
	session = strings.TrimSpace(sessionID)
	tier = tierFromFilter(filter)
	sourceTable = metadataStringEqualityFromFilter(filter, "source_table")
	return session, tier, sourceTable
}

func searchFilterMatches(doc VectorDocument, sessionID, filter string) bool {
	session, tier, sourceTable := searchFilterValues(sessionID, filter)
	if session != "" && strings.TrimSpace(doc.ChatSessionID) != session {
		return false
	}
	if tier != "" && strings.TrimSpace(doc.Tier) != tier {
		return false
	}
	if sourceTable != "" && strings.TrimSpace(doc.SourceTable) != sourceTable {
		return false
	}
	return true
}

func metadataStringEqualityFromFilter(filter, field string) string {
	original := strings.TrimSpace(filter)
	lower := strings.ToLower(original)
	field = strings.ToLower(strings.TrimSpace(field))
	if original == "" || field == "" {
		return ""
	}
	index := strings.Index(lower, field)
	if index < 0 {
		return ""
	}
	remainder := strings.TrimSpace(original[index+len(field):])
	if !strings.HasPrefix(remainder, "==") {
		return ""
	}
	remainder = strings.TrimSpace(strings.TrimPrefix(remainder, "=="))
	if len(remainder) < 2 || (remainder[0] != '"' && remainder[0] != '\'') {
		return ""
	}
	quote := remainder[0]
	end := strings.IndexByte(remainder[1:], quote)
	if end < 0 {
		return ""
	}
	return strings.TrimSpace(remainder[1 : end+1])
}

func tierFromFilter(filter string) string {
	lower := strings.ToLower(filter)
	for _, tier := range []string{"memory", "episode", "chapter", "arc", "saga", "evidence"} {
		if strings.Contains(lower, "tier") && strings.Contains(lower, tier) {
			return tier
		}
	}
	return ""
}
