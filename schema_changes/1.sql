CREATE TABLE schema_version (
    version INTEGER
);

INSERT INTO schema_version (version) VALUES (1);

ALTER TABLE scrobble ADD COLUMN fetched_at INTEGER;
