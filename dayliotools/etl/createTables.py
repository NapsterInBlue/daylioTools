import sys
import os
from contextlib import contextmanager
import sqlite3

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


def build_empty_tables():
    with connect_to_db('/daylio.db') as cur:
        cur.executescript('''
            CREATE TABLE IF NOT EXISTS calendar (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                date DATE,
                timestamp_id INTEGER,
                mood_id INTEGER,
                checks INTEGER,

                CONSTRAINT unique_entry UNIQUE (date, timestamp_id)
                );

            CREATE TABLE IF NOT EXISTS timestampDim (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                timestamp TEXT UNIQUE
                );

            CREATE TABLE IF NOT EXISTS moodDim (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                moodtext TEXT UNIQUE
                );

            CREATE TABLE IF NOT EXISTS activityDim (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                activity TEXT UNIQUE,
                first_appearance DATE
                );

            CREATE TABLE IF NOT EXISTS entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
                calendar_id INT,
                activity_id INT,

                CONSTRAINT unique_entry UNIQUE (calendar_id, activity_id)
                )

            ''')


def build_basic_dim_tables():
    with connect_to_db('/daylio.db') as cur:
        cur.executescript('''
            INSERT OR IGNORE INTO moodDim (moodtext) values ('fuck');
            INSERT OR IGNORE INTO moodDim (moodtext) values ('bleh');
            INSERT OR IGNORE INTO moodDim (moodtext) values ('meh');
            INSERT OR IGNORE INTO moodDim (moodtext) values ('good');
            INSERT OR IGNORE INTO moodDim (moodtext) values ('rad');

            INSERT OR IGNORE INTO timestampDim (timestamp) values ('Morning');
            INSERT OR IGNORE INTO timestampDim (timestamp) values ('Lunch');
            INSERT OR IGNORE INTO timestampDim (timestamp) values ('Afternoon');
            INSERT OR IGNORE INTO timestampDim (timestamp) values ('Evening');
            INSERT OR IGNORE INTO timestampDim (timestamp) values ('Night');
            ''')


if __name__ == "__main__":
    build_empty_tables()
    build_basic_dim_tables()
