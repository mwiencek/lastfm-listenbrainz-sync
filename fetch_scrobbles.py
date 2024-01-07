#!.venv/bin/python

import datetime
import itertools
import os
import re
from constants import LASTFM_DATA_ROOT
from config import lastfm_first_scrobble_date
from utils import (
    get_db_con,
    get_iso_date_from_argv,
    fetch_scrobbles_for_date
)


date_obj = get_iso_date_from_argv()

fetched_at = int(datetime.datetime.now().timestamp())

db_con = get_db_con()

if date_obj:
    fetch_scrobbles_for_date(db_con, date_obj, fetched_at)
else:
    os.makedirs(LASTFM_DATA_ROOT, mode=0o755, exist_ok=True)

    today = datetime.date.today()

    last_fetched_date = max(
        itertools.chain(
            map(
                lambda x: datetime.date.fromisoformat(
                    re.sub(r'\.json$', '', x)
                ),
                os.listdir(LASTFM_DATA_ROOT)
            ),
            [datetime.date.fromisoformat(lastfm_first_scrobble_date)]
        )
    )

    db_cur = db_con.cursor()

    def get_scrobble_count():
        db_cur.execute('SELECT count(*) scrobble_count FROM scrobble')
        row = db_cur.fetchone()
        if row is not None:
            return row['scrobble_count']
        return 0

    scrobble_count_before = get_scrobble_count()

    total_days = (today - last_fetched_date).days + 1

    while last_fetched_date <= today:
        fetch_scrobbles_for_date(db_con, last_fetched_date, fetched_at)
        last_fetched_date += datetime.timedelta(days=1)

    scrobble_count_after = get_scrobble_count()
    fetched_scrobble_count = (scrobble_count_after - scrobble_count_before)

    print(
        'Fetched ' +
        str(fetched_scrobble_count) +
        ' new scrobble' +
        ('' if fetched_scrobble_count == 1 else 's')
    )
