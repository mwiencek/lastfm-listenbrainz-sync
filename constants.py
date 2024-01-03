import base64
import os


_cwd = os.getcwd()

_EGGPLANT = b'ZjZkN2YyMWMwMDQwZjhjYjkxOTljNTQ0ZmFmM2I5ZWE='
BABA_GHANOUJ = base64.b64decode(_EGGPLANT).decode('ascii')
LAST_SUBMITTED_LISTEN_FILE = os.path.join(_cwd, 'last_submitted_listen')
LASTFM_API_ROOT = 'http://ws.audioscrobbler.com/2.0/'
LASTFM_DATA_ROOT = os.path.join(_cwd, 'lastfm_data')
LATEST_SCHEMA_VERSION = 1  # keep in sync with schema.sql
LISTENBRAINZ_API_ROOT = 'https://api.listenbrainz.org/1/'
SCROBBLES_DB_FILE = os.path.join(_cwd, 'scrobbles.db')
USER_AGENT = 'github:mwiencek/lastfm-listenbrainz-sync'
