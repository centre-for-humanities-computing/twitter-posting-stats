import gzip
import sqlite3
import sys

import jsonlines
from tqdm import tqdm

from postingstats.datamodels import TweetType
from postingstats.db.utils import Inserter, iter_lines, iso_to_posix

TWEET_TABLE = 'tweets'
HASHTAG_TABLE = 'hashtags'
TWEET_PUBLIC_METRICS_TABLE = 'tweet_public_metrics'

input_file = sys.argv[1]
database_name = sys.argv[2] + '.db'

# Connect to SQLite database
conn = sqlite3.connect(database_name)

# Create tables
conn.execute(f'''CREATE TABLE IF NOT EXISTS {TWEET_TABLE} (
                    id INTEGER64 PRIMARY KEY,
                    author_id INTEGER64,
                    text TEXT,
                    created_at DATETIME,
                    created_at_posix INTEGER64,
                    type INTEGER,
                    type_name TEXT
                )''')
conn.execute(f'''CREATE TABLE IF NOT EXISTS {HASHTAG_TABLE} (
                    hashtag TEXT,
                    tweet_id INTEGER64,
                    FOREIGN KEY (tweet_id) REFERENCES {TWEET_TABLE} (id)
                )''')
conn.execute(f'''CREATE TABLE IF NOT EXISTS {TWEET_PUBLIC_METRICS_TABLE} (
                    retweet_count INTEGER,
                    reply_count INTEGER,
                    like_count INTEGER,
                    quote_count INTEGER,
                    impression_count INTEGER,
                    tweet_id INTEGER64,
                    FOREIGN KEY (tweet_id) REFERENCES {TWEET_TABLE} (id)
                )''')
conn.commit()

with (Inserter(conn, TWEET_TABLE) as tweets,
      Inserter(conn, HASHTAG_TABLE) as hashtags,
      Inserter(conn, TWEET_PUBLIC_METRICS_TABLE) as public_metrics,
      ):
    reader = jsonlines.Reader(iter_lines(input_file))
    for tweet in tqdm(reader, desc='Writing to database'):
        pm = tweet['public_metrics']
        pm['tweet_id'] = tweet['id']
        public_metrics.insert(pm)
        del tweet['public_metrics']

        for hashtag in tweet['hashtags']:
            hashtags.insert({'hashtag': hashtag, 'tweet_id': tweet['id']})
        del tweet['hashtags']

        tweet['created_at_posix'] = iso_to_posix(tweet['created_at'])
        tweet_type = TweetType[tweet['type']]
        tweet['type'] = tweet_type.value
        tweet['type_name'] = tweet_type.name
        tweets.insert(tweet)

conn.close()

