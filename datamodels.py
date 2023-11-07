import datetime
from datetime import date, datetime
from dataclasses import dataclass, field
from functools import partial

import dataclasses_json
from dataclasses_json import dataclass_json
from typing import List, Dict, Union, Optional


date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
dataclasses_json.cfg.global_config.encoders[date] = date.isoformat
dataclasses_json.cfg.global_config.decoders[date] = lambda d: datetime.strptime(d, date_format)


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
    # description: str
    protected: bool
    name: str

    # location: str
    # profile_image_url: str
    created_at: date
    # entities: Dict[str, List[Entity]]
    # pinned_tweet_id: Optional[str] = None

    # url: Optional[str] = None

    def get_id(self):
        return self.get_id

    def get_creation_date(self):
        return self.created_at


@dataclass
class UserByDate:
    date: date
    user: User

    def get_user_id(self):
        return self.user.id

    def get_user(self):
        return self.user


def get_latest_dated_user(user1: UserByDate, user2: UserByDate):
    return max(user1, user2, key=lambda u: u.date)


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
    impression_count: int


@dataclass_json
@dataclass()
class Tweet:
    id: str
    text: str
    # public_metrics: TweetPublicMetrics
    created_at: date
    author_id: str
    includes: Includes

    def get_author_id(self):
        return self.author_id

    def get_creation_date(self):
        return self.created_at

    def get_users(self):
        return self.includes.users

    def dated_user_mentions(self):
        return [UserByDate(self.created_at, user) for user in self.includes.users]


