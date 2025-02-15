import datetime
import itertools
import os
import re
import sys
import time

import requests
import sqlite3

from constants import (
    LATEST_SCHEMA_VERSION,
    SCROBBLES_DB_FILE,
    USER_AGENT
)


def epoch_range_for_date(date_obj):
    # in the lastfm api, "from" is inclusive and "to" is exclusive
    dt1 = datetime.datetime.combine(date_obj, datetime.datetime.min.time())
    dt2 = dt1 + datetime.timedelta(days=1)
    return (int(dt1.timestamp()), int(dt2.timestamp()))


def get_iso_date_from_argv():
    if len(sys.argv) > 1:
        return datetime.date.fromisoformat(sys.argv[1])
    return None


def get_db_con():
    create_schema = False
    if not os.path.exists(SCROBBLES_DB_FILE):
        create_schema = True

    con = sqlite3.connect(SCROBBLES_DB_FILE, isolation_level='DEFERRED')

    # https://docs.python.org/3/library/sqlite3.html#how-to-create-and-use-row-factories
    def dict_factory(cursor, row):
        fields = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(fields, row)}

    con.row_factory = dict_factory

    if create_schema:
        with open(os.path.join(os.getcwd(), 'schema.sql'), 'r') as fp:
            cur = con.cursor()
            cur.executescript(fp.read())
            cur.close()

    upgrade_schema(con)
    return con


def get_last_fetched_date(root_dir, start_date):
    return max(
        itertools.chain(
            map(
                lambda x: datetime.date.fromisoformat(
                    re.sub(r'\.json$', '', x)
                ),
                os.listdir(root_dir)
            ),
            [start_date]
        )
    )


def make_json_request(url):
    # retry on 50x errors
    attempts = 0
    while attempts < 3:
        attempts += 1
        time.sleep(1 + attempts)
        res = requests.get(url, headers={'user-agent': USER_AGENT})
        if attempts < 3 and res.status_code in (500, 502, 503, 504):
            continue
        else:
            res.raise_for_status()
        return res.json()


def cmp_strings(a, b):
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def upgrade_schema(db_con):
    db_cur = db_con.cursor()

    db_cur.execute(
        '''
        SELECT 1
          FROM sqlite_schema
         WHERE type = 'table'
         AND name = 'schema_version';
        '''
    )
    schema_version_exists = db_cur.fetchone()
    if schema_version_exists is None:
        current_schema_version = 0
    else:
        db_cur.execute('SELECT version FROM schema_version;')
        current_schema_version = db_cur.fetchone()['version']

    if current_schema_version > LATEST_SCHEMA_VERSION:
        print(
            'Error: The schema version of scrobbles.db is newer than ' +
            'the current codebase supports.',
            file=sys.stderr
        )
        sys.exit(1)

    while current_schema_version < LATEST_SCHEMA_VERSION:
        with open(
            os.path.join(
                os.getcwd(),
                'schema_changes',
                str(current_schema_version + 1) + '.sql'
            ),
            'r'
        ) as fp:
            db_cur.executescript(fp.read())
        current_schema_version += 1

    assert current_schema_version == LATEST_SCHEMA_VERSION

    db_con.commit()
