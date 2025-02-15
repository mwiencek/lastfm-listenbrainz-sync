#!.venv/bin/python

import datetime
from functools import cmp_to_key
import json
import math
import os
from constants import (
    BABA_GHANOUJ,
    LASTFM_API_ROOT,
    LASTFM_DATA_ROOT,
    SCROBBLE_FIXES_ROOT
)
from config import lastfm_first_scrobble_date, lastfm_username
from utils import (
    cmp_strings,
    epoch_range_for_date,
    get_db_con,
    get_iso_date_from_argv,
    get_last_fetched_date,
    make_json_request
)


def cmp_recenttracks(a, b):
    return (
        (int(a['date']['uts']) - int(b['date']['uts'])) or
        cmp_strings(a['artist']['name'], b['artist']['name']) or
        cmp_strings(a['name'], b['name'])
    )


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

    scrobble_fixes = os.path.join(SCROBBLE_FIXES_ROOT,
                                  str(date_obj) + '.sql')
    if os.path.exists(scrobble_fixes):
        with open(scrobble_fixes, 'r') as fp:
            db_cur.executescript(fp.read())

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

    db_con.commit()


def get_total_pages_from_json(res_json):
    return int(res_json['recenttracks']['@attr']['totalPages'])


def is_not_now_playing_track(track):
    return track.get('@attr', {}).get('nowplaying') != 'true'


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


date_obj = get_iso_date_from_argv()

fetched_at = int(datetime.datetime.now().timestamp())

db_con = get_db_con()

if date_obj:
    fetch_scrobbles_for_date(db_con, date_obj, fetched_at)
else:
    os.makedirs(LASTFM_DATA_ROOT, mode=0o755, exist_ok=True)

    today = datetime.date.today()

    last_fetched_date = get_last_fetched_date(
        LASTFM_DATA_ROOT,
        datetime.date.fromisoformat(lastfm_first_scrobble_date)
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
