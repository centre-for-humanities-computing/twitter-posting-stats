# Twitter posting stats

This project is for gathering by-user posting statistics from Twitter scrapes. Since
those scrapes can get pretty big, `pyspark` is used. The data amounts are probably not
big enough to merit large Spark clusters, but in principle this should scale to bigger
and more machines.

## Input data
The input data should be line-delimited JSON files of Tweets as scraped via the Twitter
API. The Tweet objects should contain these fields as a minimum in this format:

```
{
  "id": str,
  "text": str,
  "created_at": datetime,
  "author_id": str,
  "public_metrics": {
    "retweet_count": int,
    "reply_count": int,
    "like_count": int,
    "quote_count": int
  },
  "includes": {
    "users": [
      {
        "id": str,
        "username": str,
        "verified": bool,
        "description": str,
        "protected": bool,
        "name": str,
        "created_at": datetime,
        "public_metrics": {
          "followers_count": int
          "following_count": int
          "tweet_count": int
          "listed_count": int
        }
      }
    ]
  }
}
```

## How to run
Run the Spark app either with Python, e.g.:

```
python main.py "input/examples_*.ndjson" --output-path output/example
```

Be mindful of quotes if you use a glob pattern.

You can also run with `spark-submit` which can give more control over Spark 
configuration, in which case you should pass the `-n` (`--no-local`) to the Python
script, e.g.:

```
spark-submit --master 'local[32]' --driver-memory 64G main.py -n "input/examples_*.ndjson"
```