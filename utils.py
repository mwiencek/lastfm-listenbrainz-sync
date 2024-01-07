import datetime
from functools import cmp_to_key
import json
import math
import os
import requests
import sqlite3
import sys
import time
import tqdm
from config import lastfm_username, listenbrainz_user_token
from constants import (
    BABA_GHANOUJ,
    LAST_SUBMITTED_LISTEN_FILE,
    LASTFM_API_ROOT,
    LASTFM_DATA_ROOT,
    LATEST_SCHEMA_VERSION,
    LISTENBRAINZ_API_ROOT,
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


def get_total_pages_from_json(res_json):
    return int(res_json['recenttracks']['@attr']['totalPages'])


def load_json_into_db(db_cur, recenttracks, fetched_at):
    params = []
    for track in recenttracks:
        uts = int(track['date']['uts'])
        artist_name = track['artist']['name']
        track_name = track['name']
        album_name = track['album']['#text']
        recording_mbid = track['mbid']
        release_mbid = track['album']['mbid']
        loved = int(track['loved'])
        params.append((
            uts,
            artist_name,
            track_name,
            album_name,
            recording_mbid,
            release_mbid,
            loved,
            fetched_at
        ))
    db_cur.executemany(
        '''
        INSERT INTO scrobble (uts, artist_name, track_name, album_name,
                              recording_mbid, release_mbid, loved, fetched_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (uts, artist_name, track_name) DO UPDATE
                SET album_name = excluded.album_name,
                    recording_mbid = excluded.recording_mbid,
                    release_mbid = excluded.release_mbid,
                    loved = excluded.loved,
                    fetched_at = excluded.fetched_at
        ''',
        params
    )


def cmp_strings(a, b):
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def cmp_recenttracks(a, b):
    return (
        (int(a['date']['uts']) - int(b['date']['uts'])) or
        cmp_strings(a['artist']['name'], b['artist']['name']) or
        cmp_strings(a['name'], b['name'])
    )


def is_not_now_playing_track(track):
    return track.get('@attr', {}).get('nowplaying') != 'true'


def fetch_scrobbles_for_date(db_con, date_obj, fetched_at):
    print(f'Fetching scrobbles for {date_obj}')
    recenttracks = []
    from_uts, to_uts = epoch_range_for_date(date_obj)
    base_url = (
        LASTFM_API_ROOT +
        '?method=user.getrecenttracks' +
        '&user=' + lastfm_username +
        '&extended=1' +
        '&from=' + str(from_uts) +
        '&to=' + str(to_uts) +
        '&limit=200' +
        '&api_key=' + BABA_GHANOUJ +
        '&format=json'
    )
    current_page = 1
    total_pages = math.inf
    while current_page <= total_pages:
        page_url = base_url + '&page=' + str(current_page)
        res_json = make_json_request(page_url)
        track = res_json['recenttracks']['track']
        # may be a dict if there is only a single item
        if isinstance(track, dict):
            track = [track]
        recenttracks.extend(filter(is_not_now_playing_track, track))
        total_pages = get_total_pages_from_json(res_json)
        current_page += 1

    recenttracks.sort(key=cmp_to_key(cmp_recenttracks))

    json_file = os.path.join(LASTFM_DATA_ROOT, str(date_obj) + '.json')
    with open(json_file, 'w') as fp:
        json.dump({'recenttracks': recenttracks}, fp)

    # delete existing scrobbles for this date
    db_cur = db_con.cursor()

    def get_scrobbles():
        db_cur.execute(
            '''
            SELECT uts, artist_name, track_name, album_name,
                   recording_mbid, release_mbid, loved
              FROM scrobble
             WHERE uts >= ?
               AND uts < ?
            ''',
            (from_uts, to_uts)
        )
        return set(map(lambda x: tuple(sorted(x.items())), db_cur.fetchall()))

    scrobbles_before = get_scrobbles()

    db_cur.execute(
        'DELETE FROM scrobble WHERE uts >= ? AND uts < ?',
        (from_uts, to_uts)
    )

    load_json_into_db(db_cur, recenttracks, fetched_at)

    db_con.commit()

    scrobbles_after = get_scrobbles()
    added_scrobbles = sorted(
        map(dict, scrobbles_after - scrobbles_before),
        key=lambda x: x['uts']
    )
    removed_scrobbles = sorted(
        map(dict, scrobbles_before - scrobbles_after),
        key=lambda x: x['uts']
    )

    for scrobble in removed_scrobbles:
        print('\tRemoved ' + str(dict(scrobble)))

    for scrobble in added_scrobbles:
        print('\tAdded ' + str(dict(scrobble)))


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
