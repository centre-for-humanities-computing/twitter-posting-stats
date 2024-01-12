import datetime
import enum
from datetime import date, datetime
from dataclasses import dataclass

import dataclasses_json
from dataclasses_json import dataclass_json
from typing import List, Optional

# Sets encoding/decoding format for JSON (de-)serialization of dates in tweets and users
date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
dataclasses_json.cfg.global_config.decoders[datetime] = \
    lambda d: datetime.strptime(d, date_format)
dataclasses_json.cfg.global_config.encoders[datetime] = \
    lambda d: d.isoformat()


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


@dataclass(frozen=True)
class UserByDate:
    date: date
    user: User

    def get_user_id(self):
        return self.user.id

    def get_user(self):
        return self.user

    @staticmethod
    def latest_dated_user(user1: 'UserByDate', user2: 'UserByDate'):
        return max(user1, user2, key=lambda u: u.date)


@dataclass_json
@dataclass(frozen=True)
class Includes:
    users: List[User]


@dataclass_json
@dataclass(frozen=True)
class Entities:
    hashtags: Optional[List[dict]] = None


@dataclass_json
@dataclass(frozen=True)
class TweetPublicMetrics:
    retweet_count: int
    reply_count: int
    like_count: int
    quote_count: int
    impression_count: int = -1


class TweetType(enum.Enum):
    ORIGINAL = 0
    RETWEET = 1
    REPLY = 2
    QUOTED = 3

    @staticmethod
    def names():
        return [t.name for t in TweetType]


# use names instead of values for JSON de- and encoding for readability
dataclasses_json.cfg.global_config.decoders[TweetType] = lambda t: TweetType[t]
dataclasses_json.cfg.global_config.encoders[TweetType] = lambda t: t.name


@dataclass(frozen=True)
class TweetBase:
    id: str
    text: str
    public_metrics: TweetPublicMetrics
    created_at: datetime
    author_id: str

    def get_id(self):
        return self.id

    def get_author_id(self):
        return self.author_id

    def get_creation_date(self):
        return self.created_at


@dataclass_json
@dataclass(frozen=True)
class MinimalTweet(TweetBase):
    type: TweetType
    hashtags: List[str]


@dataclass_json
@dataclass(frozen=True)
class Tweet(TweetBase):
    includes: Includes
    referenced_tweets: Optional[List[dict]] = None
    entities: Optional[Entities] = None

    def get_users(self):
        return self.includes.users

    def get_hashtags(self) -> List[str]:
        if not self.entities or not self.entities.hashtags:
            return []
        return [hashtag['tag'].lower() for hashtag in self.entities.hashtags]

    def tweet_type(self):
        if not self.referenced_tweets:
            return TweetType.ORIGINAL
        elif self.referenced_tweets[0]['type'] == 'replied_to':
            return TweetType.REPLY
        elif self.referenced_tweets[0]['type'] == 'quoted':
            return TweetType.QUOTED
        elif self.referenced_tweets[0]['type'] == 'retweeted':
            return TweetType.RETWEET
        else:
            raise ValueError(f"Unrecognized tweet type! {self.referenced_tweets}")

    def minimal_tweet(self) -> MinimalTweet:
        """
        :return: a :class:`MinimalTweet` from this Tweet object.
        """
        return MinimalTweet(self.id, self.text, self.public_metrics, self.created_at,
                            self.author_id, self.tweet_type(), self.get_hashtags())

    def get_dated_author(self):
        for user in self.get_users():
            if user.id == self.author_id:
                return UserByDate(self.created_at, user)
        raise ValueError(
            f"No included users correspond to author of tweet {self.id}."
        )

    def get_dated_users(self):
        return [UserByDate(self.created_at, user) for user in self.get_users()]
