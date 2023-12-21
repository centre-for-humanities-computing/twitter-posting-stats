import re
import dacy.datasets
from collections import Counter
from datetime import datetime
from typing import List, Union

from datamodels import User, Tweet, TweetBase, TweetType, MinimalTweet, TweetCollection


class Stats(dict):
    def __init__(self, user: User, tweets: List[MinimalTweet]):
        super().__init__()

        # identity
        self.id = user.id
        self.user_name = user.username
        self.display_name = user.name
        self.verified = user.verified
        self.identifiable = is_identifiable(user.name)

        # dates
        self.created_at = user.created_at.isoformat()
        self.first_tweet = earliest_tweet(tweets).created_at.isoformat()
        self.last_tweet = latest_tweet(tweets).created_at.isoformat()

        # tweet counts
        self.n_tweets = len(tweets)
        self.n_orig_tweets = sum(1 for t in tweets if t.type == TweetType.ORIGINAL)
        self.n_replies = sum(1 for t in tweets if t.type == TweetType.REPLY)
        self.n_quotes = sum(1 for t in tweets if t.type == TweetType.QUOTED)
        self.n_retweets = sum(1 for t in tweets if t.type == TweetType.RETWEET)
        self.hashtags = [(k, v) for k, v in Counter(get_hashtags(tweets)).most_common()]

        self.following = user.public_metrics.following_count
        self.followers = user.public_metrics.followers_count

    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def from_tweet_collection(cls, tweet_collection: TweetCollection):
        return cls(tweet_collection.get_user(), tweet_collection.tweets)


def earliest_tweet(tweets: List[Union[Tweet, TweetBase]]):
    return min(tweets, key=Tweet.get_creation_date)


def latest_tweet(tweets: List[Union[Tweet, TweetBase]]):
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


def get_hashtags(tweets: List[MinimalTweet]):
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
