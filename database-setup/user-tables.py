import datetime
import sqlite3
import sys

import jsonlines
from tqdm import tqdm

from postingstats.db.utils import Inserter, iter_lines, iso_to_posix

USER_TABLE = 'users'
USER_PUBLIC_METRICS_TABLE = 'user_public_metrics'

input_file = sys.argv[1]
database_name = sys.argv[2]

# Connect to SQLite database
conn = sqlite3.connect(database_name + '.db')

# Create tables
conn.execute(f'''CREATE TABLE IF NOT EXISTS {USER_TABLE} (
                    id INTEGER64 PRIMARY KEY,
                    name TEXT,
                    username TEXT,
                    verified BOOLEAN,
                    created_at DATETIME,
                    created_at_posix INTEGER64
                )''')
conn.execute(f'''CREATE TABLE IF NOT EXISTS {USER_PUBLIC_METRICS_TABLE} (
                    followers_count INTEGER,
                    following_count INTEGER,
                    listed_count INTEGER,
                    tweet_count INTEGER,
                    user_id INTEGER64,
                    FOREIGN KEY (user_id) REFERENCES {USER_TABLE} (id)
                )''')
conn.commit()

with (Inserter(conn, USER_TABLE) as users,
      Inserter(conn, USER_PUBLIC_METRICS_TABLE) as user_public_metrics,
      ):
    reader = jsonlines.Reader(iter_lines(input_file))
    for user in tqdm(reader, desc='Writing to database'):
        user['created_at_posix'] = iso_to_posix(user['created_at'])
        users.insert(user)

        pm = user['public_metrics']
        pm['user_id'] = user['id']
        user_public_metrics.insert(pm)

conn.close()
