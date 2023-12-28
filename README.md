# lastfm-listenbrainz-sync

Scripts to sync Last.fm scrobbles to ListenBrainz.

Compared to the [official importer](https://listenbrainz.org/profile/import/), this allows you to clean up your scrobbles locally (to standardize artist, track, or release names to match MusicBrainz, hopefully improving MBID mapping) before importing (assuming you know how to use SQL), and doubles as a local backup of your scrobbling history.

There are also known issues with the official importer that cause it to drop many scrobbles (LB-524, LB-1107, LB-1108, LB-1314). Those issues don't exist here.

## Prerequisites

 1. [Last.fm API key](https://www.last.fm/api/account/create)
 2. [ListenBrainz user token](https://listenbrainz.org/profile/)

## Setup

Make sure you have `sqlite3` installed on your system.

```sh
python3 -m venv .venv
.venv/bin/activate  # or activate.fish, if you use fish
pip install -r requirements.txt
cp config.dist.ini config.ini
```

Edit `config.ini` and fill out all the values.

## Fetching/submitting scrobbles

Scrobbles have to fetched from Last.fm first. If you're running this for the first time, it'll take a while, since it only fetches about one day per second.

```sh
./fetch_scrobbles.py
```

You can run this periodically to keep your local cache of scrobbles up-to-date; it will resume where it left off.

The scrobbles will be downloaded as JSON to `lastfm_data/`. They'll also be stored in a SQLite database named `scrobbles.db`.

Scrobbles are submitted to ListenBrainz from `scrobbles.db`, not the JSON files. If you'd like to make modifications to the metadata before submission, you may do so with SQL:

```
sqlite3 scrobbles.db
sqlite> update scrobble set artist_name = 'The Foo' where artist_name = 'Foo';
```

See [schema.sql](schema.sql) for the complete database schema. Note that scrobbles are unique on `(uts, artist_name, track_name)`, not just `uts`.

To submit new scrobbles as listens to ListenBrainz, do:

```sh
./submit_listens.py
```

The script maintains a `last_submitted_listen` file, and uses that to determine which scrobbles are new. So if you've fetched more scrobbles by running `fetch_scrobbles.py` again, or if the submit script fails or is interrupted, it will resume submission from where it previously left off.

## Fetching/submitting scrobbles for a particular date

If you want to reimport listens for a particular date, say because you fixed some metadata in `scrobbles.db`, then delete the old listens from ListenBrainz first, and run `submit_listens.py` with the date you want:

```sh
./submit_listens.py 2023-12-28
```

If you want to refetch metadata from Last.fm for a particular date, say because you have Last.fm Pro and fixed some metadata on Last.fm itself using their "Edit scrobble" feature, then run `fetch_scrobbles.py` with the date you want:

```sh
./fetch_scrobbles.py 2023-12-28
```

And see above for resubmitting these to ListenBrainz.

# License

[MIT](LICENSE)
