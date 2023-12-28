#!.venv/bin/python

import json
import os
from constants import LAST_SUBMITTED_LISTEN_FILE
from utils import (
    epoch_range_for_date,
    get_db_con,
    get_iso_date_from_argv,
    submit_listens
)


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
                last_submitted_listen = json.loads(last_submitted_listen_contents)
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
