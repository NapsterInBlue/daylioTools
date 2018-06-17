import sys
import os

import sqlite3
from contextlib import contextmanager

sys.path.append(os.path.abspath(__file__+'/..'))
from conf import DROPBOXPATH, DBPATH

@contextmanager
def connect_to_db(database):
    path = DBPATH
    conn = sqlite3.connect(path+database)
    cur = conn.cursor()
    yield cur
    conn.commit()
    conn.close()

def get_raw_data():
    import csv
    from dateutil.parser import parse
    from datetime import datetime

    _build_blank_raw_table()

    csvPath = DROPBOXPATH
    f_in = csv.reader(open(csvPath))
    f_in.__next__()

    with connect_to_db('/rawDaylio.db') as rawCur:
        for row in f_in:
            year, date, day, time, mood, activities, note = row
            record_dt = parse(date + ' ' + year + ' ' + time)
            sql_dt = datetime.strftime(record_dt, '%Y-%m-%d %H:%M:%S')
            sql_dtID = datetime.strftime(record_dt, '%Y%m%d')
            rawCur.execute('''
                INSERT OR IGNORE
                INTO raw (record_dt, record_dateID, day, mood, activities, note)
                VALUES (?, ?, ?, ?, ?, ?)''',
                (sql_dt, sql_dtID, day, mood, activities, note))

def _build_blank_raw_table():
    with connect_to_db('/rawDaylio.db') as rawCur:
        rawCur.execute('''
            CREATE TABLE IF NOT EXISTS raw (
                record_dt TIMESTAMP UNIQUE,
                record_dateID INT,
                day TEXT,
                mood TEXT,
                activities TEXT,
                note INT
            )''')


if __name__ == '__main__':
    get_raw_data()
