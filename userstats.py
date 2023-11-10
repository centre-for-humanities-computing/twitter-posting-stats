import re
import dacy.datasets
from collections import Counter
from datetime import datetime
from typing import List, Union

from datamodels import User, Tweet, MinimalTweet


def create_stats(user: User, tweets: List[Tweet], end_date: datetime):
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


def earliest_tweet(tweets: List[Union[Tweet, MinimalTweet]]):
    return min(tweets, key=Tweet.get_creation_date)


def latest_tweet(tweets: List[Union[Tweet, MinimalTweet]]):
    return max(tweets, key=Tweet.get_creation_date)


def average_daily_tweets(n_tweets: int, start_date: datetime, end_date: datetime):
    start_date = start_date.date()
    end_date = end_date.date()
    if start_date > end_date:
        raise ValueError(
            f"Start date {start_date} is later than the end date {end_date}!"
        )
    time_delta = end_date - start_date
    return n_tweets / (time_delta.days + 1)


hashtag = re.compile(r"#(?!\d)\w+")


def get_hashtags(tweets: List[Tweet]):
    return [match for tweet in tweets for match in re.findall(hashtag, tweet.text)]


first_names = {n.lower() for n in dacy.datasets.danish_names()["first_name"]}
last_names = {n.lower() for n in dacy.datasets.danish_names()["last_name"]}
# chars in regex is based on what are in the above name lists
proper_name = re.compile(r"[A-ZÆØÅÉÜ'-]?[a-zæøåéü'-]+")


def is_identifiable(name: str):
    """
    Deems whether a name is identifiable based on a predefined list of Danish names.
    :param name:
    :return: True if deemed identifiable, False otherwise
    """
    matches = proper_name.findall(name)
    if matches is None:
        return False
    has_first_name = any(name.strip().lower() in first_names for name in matches)
    has_last_name = any(name.strip().lower() in last_names for name in matches)
    return has_first_name and has_last_name
