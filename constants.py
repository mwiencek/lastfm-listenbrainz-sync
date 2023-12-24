import os
from config import lastfm_username


_cwd = os.getcwd()

LAST_SUBMITTED_LISTEN_FILE = os.path.join(_cwd, 'last_submitted_listen')
LASTFM_API_ROOT = 'http://ws.audioscrobbler.com/2.0/'
LASTFM_DATA_ROOT = os.path.join(_cwd, 'lastfm_data', lastfm_username)
LASTFM_MAX_PAGE = 999999
LISTENBRAINZ_API_ROOT = 'https://api.listenbrainz.org/1/'
SCROBBLES_DB_FILE = os.path.join(_cwd, 'scrobbles.db')
USER_AGENT = 'github:mwiencek/lastfm-listenbrainz-sync'
