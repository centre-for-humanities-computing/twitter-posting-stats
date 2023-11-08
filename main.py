import argparse
import gzip
import json
import logging
import os
import shutil
import time
from datetime import date, datetime

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel

from datamodels import User, Tweet, UserByDate, get_latest_dated_user
from userstats import create_stats


def main(input_path: str, output_path: str, end_date: date, spark_memory: str,
         persist: bool, n_cores: str):
    if not output_path:
        output_path = "output/" + input_path
        logging.warning(f"No output path given. It will be %s", output_path)
    if os.path.exists(output_path):
        logging.warning(f"Removing %s", output_path)
        shutil.rmtree(output_path)

    conf = SparkConf() \
        .set("spark.executor.memory", "4G") \
        .set("spark.driver.memory", spark_memory) \
        .setMaster(f"local[{n_cores}]")
    sc = SparkContext(conf=conf)

    start = time.perf_counter()

    tweets = sc.textFile(input_path) \
        .map(Tweet.from_json)
    if persist:
        tweets = tweets.persist(StorageLevel.MEMORY_AND_DISK)

    if not end_date:
        logging.warning("'end_date' was not given as an argument, so it will be "
                        "calculated which is quite slow!")
        end_date = tweets.map(Tweet.get_creation_date).max()
        logging.warning("'end_date' is %s. You can use this if you need to run "
                        "the app again.", end_date)

    # If we want to be sure that we get the latest possible info on a user, we want to
    # collect the data from all tweets and not just the tweets by the specific user.
    # Therefore, we collect the latest user info below from all tweets.
    latest_user_info = tweets.flatMap(Tweet.dated_user_mentions) \
        .keyBy(UserByDate.get_user_id) \
        .reduceByKey(get_latest_dated_user) \
        .mapValues(UserByDate.get_user)

    tweets_by_user = tweets.map(Tweet.drop_includes).groupBy(Tweet.get_author_id)

    latest_user_info.join(tweets_by_user) \
        .mapValues(lambda user_and_tweets:
                   create_stats(user_and_tweets[0], user_and_tweets[1], end_date)) \
        .values() \
        .map(json.dumps) \
        .saveAsTextFile(output_path)

    print('Finished in ', time.perf_counter() - start)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("input_path",
                            help="Input file. For multiple, use a glob pattern.")
    arg_parser.add_argument("--output-path",
                            required=False,
                            default=None,
                            help="Path for output folder.")
    arg_parser.add_argument("--end-date",
                            required=False,
                            default=None,
                            help="Latest date for tweets in the sample. The app is "
                                 "significantly faster with this argument provided.")
    arg_parser.add_argument("--spark-memory",
                            required=False,
                            default="12G",
                            help="Sets Spark driver memory which stores RDDs in local "
                                 "mode. Will be overruled by other values given if "
                                 "the app is run with 'spark-submit'.")
    arg_parser.add_argument("--no-persistence",
                            action="store_true"
                            )
    arg_parser.add_argument("--n-cores",
                            default='*')
    args = arg_parser.parse_args()

    end_date_arg = None if not args.end_date else datetime.fromisoformat(args.end_date)
    main(args.input_path, args.output_path, end_date_arg, args.spark_memory,
         not args.no_persistence, args.n_cores)
