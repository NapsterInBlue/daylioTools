import sys
import os

import sqlite3
from contextlib import contextmanager
from datetime import datetime, time
from dateutil.parser import parse
from collections import namedtuple
import re

sys.path.append(os.path.abspath(__file__+'/..'))
from conf import DROPBOXPATH, DBPATH

@contextmanager
def connect_to_db(database, detect_types=sqlite3.PARSE_DECLTYPES):
    path = DBPATH
    # inexplicably, you need the second argument so typing works on reads
    # feels like that should be standard...
    conn = sqlite3.connect(path+database, detect_types=detect_types)
    cur = conn.cursor()
    yield cur
    conn.commit()
    conn.close()


CleanRow = namedtuple('CleanRow', ['entry_date', 'entry_timestampID'
                                  ,'entry_moodID', 'activities', 'checks'])

def load_clean_data():
    # separated into two databases so can iterate through the raw, while
    # using a different cursor to write to the other
    with connect_to_db('/rawDaylio.db') as rawCur:
        with connect_to_db('/daylio.db') as prodCur:

            # for activity "First Appearance" purposes
            _sort_raw_table_values_asc(rawCur)

            for row in rawCur:
                rowAttrs = CleanRow(*clean_raw_row(row))

                calendar_id = _load_calendar_entry(cur=prodCur
                                                  , cleanRow=rowAttrs)

                for activity in clean_activity_row(rowAttrs.activities):
                    activity = activity.strip()
                    _load_activity_entry(cur=prodCur, activity=activity
                                        , entry_date=rowAttrs.entry_date
                                        , calendar_id=calendar_id)





def clean_raw_row(row):

    record_dt, record_dtID, day, mood, activities, checks = row

    entry_date = record_dt.date()
    entry_timestampID = bin_time(record_dt)

    letters = re.compile('[a-zA-Z\.]')
    checks = re.sub(letters, '', str(checks))

    moodMap = {'fuck': 1, 'bleh': 2, 'meh': 3, 'good': 4, 'rad': 5}
    entry_moodID = moodMap[mood]

    return (entry_date, entry_timestampID, entry_moodID, activities, checks)

def clean_activity_row(row):
    for act in row.split('|'):
        yield clean_activity(act)

def clean_activity(string):
    string = string.lower()
    string = re.sub('[\"\']', '', string)
    return string

def bin_time(t):
    timestampMap = {'Morning': 1, 'Lunch': 2, 'Afternoon': 3,
                    'Evening': 4, 'Night': 5}

    if t.time() >= time(22):
        t = 'Night'
    elif t.time() >= time(18, 15):
        t = 'Evening'
    elif t.time() >= time(15):
        t = 'Afternoon'
    elif t.time() >= time(12, 15):
        t = 'Lunch'
    else:
        t = 'Morning'

    return timestampMap[t]



def _sort_raw_table_values_asc(cur):
    cur.execute('''
        select
            record_dt,
            record_dateID,
            day,
            mood,
            activities,
            note

            from            raw
            order by        raw.record_dt asc
    ''')

def _load_calendar_entry(cur, cleanRow):
    # map raw to calendar table then get its calendar_id for later loads
    cur.execute('''
        INSERT OR IGNORE INTO calendar
            (date, timestamp_id, mood_id, checks)
            VALUES
            (?, ?, ?, ?)''',
        (cleanRow.entry_date, cleanRow.entry_timestampID
         , cleanRow.entry_moodID, cleanRow.checks))

    cur.execute('''
        SELECT id FROM calendar
            WHERE date = ? and timestamp_id = ?''',
        (cleanRow.entry_date, cleanRow.entry_timestampID))
    calendar_id = cur.fetchone()[0]

    return calendar_id

def _load_activity_entry(cur, activity, entry_date, calendar_id):
    # check against activityDim for existence, else load w/ first_appearance
    cur.execute('''
        INSERT OR IGNORE INTO activityDim
            (activity, first_appearance)
            VALUES (?, ?)''',
        (activity, entry_date,))

    cur.execute('''
        SELECT id FROM activityDim
            WHERE activity = ?''',
            (activity, ))
    activity_id = cur.fetchone()[0]

    # map raw to to entries table
    cur.execute('''
        INSERT OR IGNORE into entries (calendar_id, activity_id)
            VALUES (?, ?)''',
        (calendar_id, activity_id))


if __name__ == '__main__':
    load_clean_data()
