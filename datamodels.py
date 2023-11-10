import datetime
from datetime import date, datetime
from dataclasses import dataclass

import dataclasses_json
from dataclasses_json import dataclass_json
from typing import List, Optional

# Sets decoding format for JSON deserialization of dates in tweets
date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
dataclasses_json.cfg.global_config.decoders[datetime] = \
    lambda d: datetime.strptime(d, date_format)


@dataclass_json
@dataclass(frozen=True)
class UserPublicMetrics:
    followers_count: int
    following_count: int
    tweet_count: int
    listed_count: int


@dataclass_json
@dataclass(frozen=True)
class User:
    id: str
    username: str
    verified: bool
    public_metrics: UserPublicMetrics
    description: str
    protected: bool
    name: str
    created_at: datetime

    def get_id(self):
        return self.get_id

    def get_creation_date(self):
        return self.created_at


@dataclass(frozen=True)
class UserByDate:
    date: date
    user: User

    def get_user_id(self):
        return self.user.id

    def get_user(self):
        return self.user


@dataclass_json
@dataclass(frozen=True)
class Includes:
    users: List[User]


@dataclass_json
@dataclass(frozen=True)
class TweetPublicMetrics:
    retweet_count: int
    reply_count: int
    like_count: int
    quote_count: int
    impression_count: Optional[int] = -1


@dataclass(frozen=True)
class MinimalTweet:
    id: str
    text: str
    public_metrics: TweetPublicMetrics
    created_at: datetime
    author_id: str

    def get_author_id(self):
        return self.author_id

    def get_creation_date(self):
        return self.created_at


@dataclass_json
@dataclass(frozen=True)
class Tweet(MinimalTweet):
    includes: Includes

    def get_users(self):
        return self.includes.users

    def minimal_tweet(self):
        """
        :return: a :class:`MinimalTweet` from this Tweet object.
        """
        return MinimalTweet(self.id, self.text, self.public_metrics, self.created_at,
                            self.author_id)

    def get_dated_author(self):
        for user in self.get_users():
            if user.id == self.author_id:
                return UserByDate(self.created_at, user)
        raise ValueError(
            f"No included users correspond to author of tweet {self.id}."
        )

    def tweet_collection(self):
        return TweetCollection([self.minimal_tweet()], self.get_dated_author())


@dataclass
class TweetCollection:
    tweets: List[MinimalTweet]
    dated_user: UserByDate

    def get_user(self):
        return self.dated_user.user

    def get_user_id(self):
        return self.dated_user.get_user_id()


def latest_dated_user(user1: UserByDate, user2: UserByDate):
    return max(user1, user2, key=lambda u: u.date)


def merge_tweet_collections(coll1: TweetCollection, coll2: TweetCollection) \
        -> TweetCollection:
    """
    Merges two :class:`TweetCollection`s into one. Favours the latest dated user.

    Raises a ValueError if the users do not have the same ID.
    :param coll1:
    :param coll2:
    :return: a new, merged :class:`TweetCollection`
    """
    if coll1.get_user_id() != coll2.get_user_id():
        raise ValueError("Cannot merge Tweet collections from different users.")

    return TweetCollection(
        coll1.tweets + coll2.tweets,
        latest_dated_user(coll1.dated_user, coll2.dated_user)
    )
