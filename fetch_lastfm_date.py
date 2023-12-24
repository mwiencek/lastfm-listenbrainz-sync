#!.venv/bin/python

from utils import (
    create_lastfm_data_root,
    get_db_con,
    get_iso_date_from_argv,
    fetch_lastfm_date
)


date_obj = get_iso_date_from_argv()

create_lastfm_data_root()

db_con = get_db_con()

fetch_lastfm_date(db_con, date_obj)
