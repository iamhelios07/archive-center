-- Archive Center Cloudflare D1 — turn preparation settings.
--
-- These rows hold settings that the local runtime keeps in files next to the data
-- directory:
--
--   memory-preprocessing.json   multi-agent roles, JEV settings, shared prompt
--   body-tracking.json          per-session body tracking configuration
--
-- They are in D1 because a Cloudflare Container is stateless. There is no managed
-- launcher to hand it a persistent directory, so a file-backed setting is lost on
-- every restart, and a Container scales to zero. A user who turns a role on would
-- find it off again after an idle period, with no error anywhere: the write
-- succeeded, the file was in the layer, and the layer went away. That is worse
-- than a missing feature because it looks like the setting is being ignored.
--
-- The shape is a scoped key/value document rather than a column per setting. The
-- two files are versioned JSON documents with a contract_version, and the local
-- runtime already treats an unknown field as a future version rather than as
-- corruption. Storing the document verbatim keeps that property, and keeps a new
-- setting from needing a migration.
--
-- DELIBERATELY NOT IN THE RESET ALLOWLIST
--
-- TestD1ResetAllowlistMatchesMariaDB requires the D1 allowlist to match
-- mariaAdminResetTables in membership and order, because the two providers must
-- not drift. MariaDB has no row for this data, so no D1-only table can be
-- added to the allowlist at all. The behaviour is also correct on its own terms:
-- the MariaDB reset deletes conversation and memory rows and never touched the
-- settings file, so a user keeps their agent configuration across a reset on both
-- providers. Adding this table to the allowlist would make Cloudflare delete
-- settings that the local runtime keeps, which is exactly the kind of silent
-- divergence the parity work exists to remove.

CREATE TABLE IF NOT EXISTS "d1_turn_preparation_settings" (
    -- 'multi_agent' for the single multi-agent document, 'body_tracking' for
    -- per-session rows. Both are stored the same way because they are both
    -- versioned JSON documents the local runtime writes to disk.
    "scope"         TEXT NOT NULL,
    -- 'default' for a single-document scope; a session id otherwise.
    "document_key"  TEXT NOT NULL,
    "payload_json"  TEXT NOT NULL,
    "updated_at"    TEXT NOT NULL,
    PRIMARY KEY ("scope", "document_key")
);

-- The per-session read path is "every body-tracking row for these sessions", so
-- scope is the leading column and a lookup by scope alone is index-covered.
CREATE INDEX IF NOT EXISTS "ix_d1_turn_preparation_settings_scope"
    ON "d1_turn_preparation_settings" ("scope", "document_key");
