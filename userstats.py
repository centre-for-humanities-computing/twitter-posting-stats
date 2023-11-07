import re
from collections import Counter
from datetime import date
from typing import List

from datamodels import User, Tweet


def create_stats(user: User, tweets: List[Tweet], end_date: date):
    # Account creation date (possible?)
    creation_date = user.created_at

    # Date of first tweet
    first_tweet = earliest_tweet(tweets).created_at

    # Date of last tweet
    last_tweet = latest_tweet(tweets).created_at

    # Av. daily tweets
    avg_daily_tweets = average_daily_tweets(len(tweets), creation_date, end_date)

    # # following
    following = user.public_metrics.following_count
    followers = user.public_metrics.followers_count

    # top 10 hashtags
    top10_hashtags = [k for k, v in Counter(get_hashtags(tweets)).most_common(10)]

    # Verified status? (prior to “Twitter blue”)
    verified = user.verified

    # Maybe proxy for “non-anonymous” - does the display name contain proper name,
    # one from given name list one from surname list
    identifiable = is_identifiable(user.name)

    return {
        "userName": user.username,
        "displayName": user.name,
        "creationDate": creation_date.isoformat(),
        "firstTweet": first_tweet.isoformat(),
        "lastTweet": last_tweet.isoformat(),
        "avgDailyTweets": avg_daily_tweets,
        "following": following,
        "followers": followers,
        "top10Hashtags": top10_hashtags,
        "verified": verified,
        "identifiable": identifiable
    }


def earliest_tweet(tweets: List[Tweet]):
    return min(tweets, key=Tweet.get_creation_date)


def latest_tweet(tweets: List[Tweet]):
    return max(tweets, key=Tweet.get_creation_date)


def average_daily_tweets(n_tweets: int, start_date: date, end_date: date):
    time_delta = end_date - start_date
    return n_tweets / (time_delta.days + 1)


hashtag = re.compile(r"#\w+")


def get_hashtags(tweets: List[Tweet]):
    return [match for tweet in tweets for match in re.findall(hashtag, tweet.text)]


proper_name = re.compile("([A-Z][a-z]+ ?){2,}")


def is_identifiable(name: str):
    match = proper_name.match(name)
    return match is not None
