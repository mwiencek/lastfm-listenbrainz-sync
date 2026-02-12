import configparser
import sys

_config = configparser.ConfigParser()
_config.read('config.ini')

_lastfm = _config['lastfm']
lastfm_username = _lastfm['username']
lastfm_api_key = _lastfm['api_key']
lastfm_first_scrobble_date = _lastfm['first_scrobble_date']

if lastfm_api_key == 'your_lastfm_api_key' or not lastfm_api_key.strip():
    print('Error: You must enter your Last.fm API key in config.ini')
    print('Get it from: https://www.last.fm/api/account/create')
    sys.exit(1)

_listenbrainz = _config['listenbrainz']
listenbrainz_user_token = _listenbrainz['user_token']
