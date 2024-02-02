#!.venv/bin/python

import json
import os
import time

import requests
import tqdm

from config import listenbrainz_user_token
from constants import (
    LAST_SUBMITTED_LISTEN_FILE,
    LISTENBRAINZ_API_ROOT,
    USER_AGENT
)
from utils import (
    epoch_range_for_date,
    get_db_con,
    get_iso_date_from_argv
)


def scrobble_to_listen(scrobble):
    additional_info = {
        'submission_client': USER_AGENT
    }
    track_metadata = {
        'track_name': scrobble['track_name'],
        'artist_name': scrobble['artist_name'],
        'additional_info': additional_info
    }
    if scrobble['album_name']:
        track_metadata['release_name'] = scrobble['album_name']
    if scrobble['recording_mbid']:
        additional_info['lastfm_track_mbid'] = scrobble['recording_mbid']
    if scrobble['release_mbid']:
        additional_info['lastfm_release_mbid'] = scrobble['release_mbid']
    return {
        'track_metadata': track_metadata,
        'listened_at': scrobble['uts']
    }


def submit_listens(db_cur, total_scrobbles,
                   update_last_submitted_listen=False):
    progress_bar = tqdm.tqdm(total=total_scrobbles)
    db_cur.arraysize = 1000
    rows = db_cur.fetchmany()
    while rows:
        url = LISTENBRAINZ_API_ROOT + 'submit-listens'
        res = requests.post(
            url,
            headers={
                'authorization': 'Token ' + listenbrainz_user_token,
                'content-type': 'application/json; charset=UTF-8',
                'user-agent': USER_AGENT
            },
            data=json.dumps({
                'listen_type': 'import',
                'payload': list(map(scrobble_to_listen, rows))
            })
        )
        ratelimit_remaining = int(res.headers['X-RateLimit-Remaining'])
        ratelimit_reset_in = int(res.headers['X-RateLimit-Reset-In'])
        if res.status_code == 429:
            time.sleep(ratelimit_reset_in)
            continue
        else:
            if res.status_code != 200:
                print(res.headers)
                print(res.json())
            res.raise_for_status()
        if update_last_submitted_listen:
            last_row = rows[-1]
            last_submitted_listen = {
                'uts': last_row['uts'],
                'artist_name': last_row['artist_name'],
                'track_name': last_row['track_name']
            }
            with open(LAST_SUBMITTED_LISTEN_FILE, 'w') as fp:
                json.dump(last_submitted_listen, fp)
        progress_bar.update(n=len(rows))
        rows = db_cur.fetchmany()
        if rows and ratelimit_remaining == 0:
            time.sleep(ratelimit_reset_in)
    progress_bar.close()


date_obj = get_iso_date_from_argv()

db_con = get_db_con()
db_cur = db_con.cursor()

if date_obj:
    from_uts, to_uts = epoch_range_for_date(date_obj)
    scrobble_query = '''
          SELECT *
            FROM scrobble
           WHERE uts >= ? AND uts < ?
        ORDER BY uts, artist_name, track_name
    '''
    scrobble_query_params = ((from_uts, to_uts),)
    update_last_submitted_listen = False
else:
    last_submitted_listen = None
    if os.path.exists(LAST_SUBMITTED_LISTEN_FILE):
        with open(LAST_SUBMITTED_LISTEN_FILE, 'r') as fp:
            last_submitted_listen_contents = fp.read().rstrip()
            if last_submitted_listen_contents:
                last_submitted_listen = json.loads(
                    last_submitted_listen_contents
                )
            del last_submitted_listen_contents

    if last_submitted_listen:
        scrobble_query = '''
            SELECT * FROM scrobble
            WHERE (
                (uts, artist_name, track_name) >= (?, ?, ?)
                AND NOT (uts = ? AND artist_name = ? AND track_name = ?)
            )
            ORDER BY uts, artist_name, track_name
        '''
        scrobble_query_params = ((
            last_submitted_listen['uts'],
            last_submitted_listen['artist_name'],
            last_submitted_listen['track_name'],
            last_submitted_listen['uts'],
            last_submitted_listen['artist_name'],
            last_submitted_listen['track_name'],
        ),)
    else:
        scrobble_query = '''
              SELECT *
                FROM scrobble
            ORDER BY uts, artist_name, track_name
        '''
        scrobble_query_params = ()
    update_last_submitted_listen = True


db_cur.execute(
    f'SELECT count(*) total_scrobbles FROM ({scrobble_query}) x',
    *scrobble_query_params
)
total_scrobbles = db_cur.fetchone()['total_scrobbles']

db_cur.execute(scrobble_query, *scrobble_query_params)

submit_listens(
    db_cur,
    total_scrobbles,
    update_last_submitted_listen=update_last_submitted_listen
)
