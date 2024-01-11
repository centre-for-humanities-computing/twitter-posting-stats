import re
from datetime import datetime

import dacy.datasets

first_names = {n.lower() for n in dacy.datasets.danish_names()["first_name"]}
last_names = {n.lower() for n in dacy.datasets.danish_names()["last_name"]}
# chars in regex is based on what are in the above name lists
proper_name = re.compile(r"[A-ZÆØÅÉÜ'-]?[a-zæøåéü'-]+")


def is_identifiable(name: str) -> bool:
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


def average_daily_tweets(n_tweets: int, start_date: datetime, end_date: datetime):
    start_date = start_date.date()
    end_date = end_date.date()
    if start_date > end_date:
        raise ValueError(
            f"Start date {start_date} is later than the end date {end_date}!"
        )
    time_delta = end_date - start_date
    return n_tweets / (time_delta.days + 1)
