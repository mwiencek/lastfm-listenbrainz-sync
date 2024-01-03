CREATE TABLE schema_version (
    version INTEGER
);

# keep in sync with constants.py
INSERT INTO schema_version (version) VALUES (1);

CREATE TABLE scrobble (
    uts INTEGER,
    artist_name TEXT,
    track_name TEXT,
    album_name TEXT,
    recording_mbid TEXT,
    release_mbid TEXT,
    loved INTEGER,
    fetched_at INTEGER
);

CREATE INDEX scrobble_idx_uts ON scrobble (uts);
CREATE UNIQUE INDEX scrobble_idx_uniq ON scrobble (uts, artist_name, track_name);
