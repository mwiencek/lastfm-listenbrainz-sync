#!.venv/bin/python

from utils import (
    epoch_range_for_date,
    get_db_con,
    get_iso_date_from_argv,
    submit_listens
)


date_obj = get_iso_date_from_argv()
from_uts, to_uts = epoch_range_for_date(date_obj)

db_con = get_db_con()
db_cur = db_con.cursor()


scrobble_query = '''
      SELECT *
        FROM scrobble
       WHERE uts >= ? AND uts < ?
    ORDER BY uts, artist_name, track_name
'''
scrobble_query_params = (from_uts, to_uts)

db_cur.execute(
    f'SELECT count(*) total_scrobbles FROM ({scrobble_query}) x',
    scrobble_query_params
)
total_scrobbles = db_cur.fetchone()['total_scrobbles']

db_cur.execute(scrobble_query, scrobble_query_params)

submit_listens(db_cur, total_scrobbles)
