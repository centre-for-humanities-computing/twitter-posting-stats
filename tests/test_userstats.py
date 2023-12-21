from datetime import datetime
import re
from typing import List

import pytest
import userstats

from datamodels import TweetBase, TweetPublicMetrics
from userstats import is_identifiable, average_daily_tweets, latest_tweet, earliest_tweet


class TestTweetStats:

    january_first = TweetBase(
        "1", "Very nice.", TweetPublicMetrics(1, 1, 1, 1),
        datetime.fromisoformat("2020-01-01"), "User"
    )
    january_second = TweetBase(
        "2", "Still nice.", TweetPublicMetrics(1, 1, 1, 1),
        datetime.fromisoformat("2020-01-02"), "User"
    )
    january_third = TweetBase(
        "3", "Screw 2020.", TweetPublicMetrics(1, 1, 1, 1),
        datetime.fromisoformat("2020-01-03"), "User"
    )

    @pytest.fixture
    def example_tweets(self) -> List[TweetBase]:
        return [
            TestTweetStats.january_first,
            TestTweetStats.january_second,
            TestTweetStats.january_third
        ]

    def test_earliest_tweet(self, example_tweets):
        assert earliest_tweet(example_tweets) == TestTweetStats.january_first

    def test_latest_tweet(self, example_tweets):
        assert latest_tweet(example_tweets) == TestTweetStats.january_third

    def test_average_daily_tweets(self, example_tweets):
        n_tweets = len(example_tweets)
        earliest_date = earliest_tweet(example_tweets).created_at
        latest_date = latest_tweet(example_tweets).created_at
        assert average_daily_tweets(n_tweets, earliest_date, latest_date) == 1

    def test_average_daily_tweets_error(self, example_tweets):
        with pytest.raises(ValueError):
            average_daily_tweets(3,
                                 TestTweetStats.january_third.created_at,
                                 TestTweetStats.january_first.created_at)


class TestGetHashTags:
    @pytest.fixture
    def hashtag_regex(self) -> re.Pattern:
        return userstats.hashtag

    def test_default(self, hashtag_regex):
        matches = hashtag_regex.findall("A good example of #hashtag")
        assert len(matches) == 1

    def test_casing(self, hashtag_regex):
        matches = hashtag_regex.findall("A good example of #HashTag")
        assert len(matches) == 1

    def test_no_spaces(self, hashtag_regex):
        matches = hashtag_regex.findall("A good example of#hashtag")
        assert len(matches) == 1

    def test_in_middle_of_sentence(self, hashtag_regex):
        matches = hashtag_regex.findall("A good #hashtag example")
        assert len(matches) == 1

    def test_valid_with_numbers(self, hashtag_regex):
        matches = hashtag_regex.findall("A good example of #hashtag123")
        assert len(matches) == 1

    def test_invalid_with_numbers(self, hashtag_regex):
        matches = hashtag_regex.findall("A bad example of #123hashtag")
        assert len(matches) == 0


class TestIsIdentifiable:
    def test_regular_danish_names(self):
        assert is_identifiable("Anders Bendtsen")
        assert is_identifiable("Claus Degn Eriksen")
        assert is_identifiable("Søren Bækgaard")  # with special characters

    def test_with_emojis(self):
        assert is_identifiable("Anders Bendtsen 💙💛")
        assert is_identifiable("Claus💛 Degn💛 Eriksen💛")

    def test_lower_case(self):
        assert is_identifiable("anders bendtsen")
        assert is_identifiable("claus degn eriksen")

    def test_camel_case(self):
        assert is_identifiable("AndersBendtsen")
        assert is_identifiable("ClausDegnEriksen")

    def test_foreign_names(self):
        assert is_identifiable("Arthur Nilsson")  # Swedish last name
        assert is_identifiable("Arthur Krüger")  # German last name, special character
        assert not is_identifiable("Arthur Cameron")  # English name

    def test_single_name(self):
        assert not is_identifiable("Anders")
        assert not is_identifiable("Claus")

    def test_single_name_with_identifiers(self):
        assert not is_identifiable("Anders Fra Venstre")

    def test_initials(self):
        assert not is_identifiable("Anders B")
        assert not is_identifiable("Claus D. E")

    def test_1337_writing(self):
        assert not is_identifiable("4nd3rs b3ndt$3n")
        assert not is_identifiable("Cl4us Degn Eriksen")
        assert is_identifiable("Claus |)39/V Eriksen")  # sufficient material

    def test_non_names(self):
        assert not is_identifiable("cool_guy13")
        assert not is_identifiable("nedmedsystemet")
