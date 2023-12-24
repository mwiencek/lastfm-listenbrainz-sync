import configparser

_config = configparser.ConfigParser()
_config.read('config.ini')

_lastfm = _config['lastfm']
lastfm_username = _lastfm['username']
lastfm_api_key = _lastfm['api_key']
lastfm_first_scrobble_date = _lastfm['first_scrobble_date']

_listenbrainz = _config['listenbrainz']
listenbrainz_user_token = _listenbrainz['user_token']
