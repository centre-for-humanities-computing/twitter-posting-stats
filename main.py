import argparse
import json
import logging
import os
import shutil
import time
from functools import partial

from pyspark.conf import SparkConf
from pyspark.context import SparkContext

from datamodels import Tweet, TweetCollection, MinimalTweet
from userstats import Stats


def none_if_error(func):
    def skip_errors_func(func_args):
        try:
            return func(func_args)
        except Exception as e:
            logging.error(f"Encountered error {e} when calling {func} on '{func_args}'")
            return None
    return skip_errors_func


def is_not_none(obj):
    return obj is not None


def main(input_path: str, output_path: str, caching: bool, sc: SparkContext):
    if not output_path:
        output_path = "output/" + os.path.basename(input_path)
        logging.warning("No output path given. It will be %s", output_path)
    if os.path.exists(output_path):
        logging.warning("Removing %s", output_path)
        shutil.rmtree(output_path)

    start = time.perf_counter()

    # deduplication is done based on IDs instead of comparing the whole tweets
    tweets = sc.textFile(input_path) \
        .map(none_if_error(Tweet.from_json)) \
        .filter(is_not_none) \
        .keyBy(Tweet.get_id) \
        .reduceByKey(lambda a, b: a) \
        .values()

    if caching:
        tweets = tweets.cache()

    tweets.map(Tweet.minimal_tweet)\
        .map(partial(MinimalTweet.to_json, ensure_ascii=False))\
        .saveAsTextFile(output_path + "/minimal-tweets")

    tweets.map(none_if_error(Tweet.tweet_collection)) \
        .filter(is_not_none) \
        .keyBy(TweetCollection.get_user_id) \
        .reduceByKey(TweetCollection.merge) \
        .mapValues(Stats.from_tweet_collection) \
        .values() \
        .map(partial(json.dumps, ensure_ascii=False)) \
        .saveAsTextFile(output_path + "/user-stats")

    print('Finished in', round(time.perf_counter() - start, 2))


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("input_path",
                            help="Input file. For multiple, use a glob pattern.")
    arg_parser.add_argument("--output-path",
                            required=False,
                            default=None,
                            help="Path for output folder.")
    arg_parser.add_argument("--spark-driver-memory",
                            required=False,
                            default="12G",
                            help="Sets Spark driver memory which stores RDDs in local "
                                 "mode. Use the '-n' flag to ignore this argument if "
                                 "using spark-submit.")
    arg_parser.add_argument("--n-cores",
                            default='*',
                            help="Sets number of cores that Spark will use in local "
                                 "mode. Use the '-n' flag to ignore this argument if "
                                 "using spark-submit.")
    arg_parser.add_argument("-c", "--no-caching",
                            required=False,
                            action="store_false",
                            help="Flag for marking that RDDs should not be cached. Can "
                                 "increase computation time, but decrease memory "
                                 "pressure.")
    arg_parser.add_argument("-n", "--no-local",
                            required=False,
                            action="store_false",
                            help="Flag for marking that the 'spark' CLI arguments "
                                 "should be ignored if Spark configuration is given "
                                 "via spark-submit or a Spark configuration.")
    args = arg_parser.parse_args()

    # 'store_false' flags are true by default, but false if passed. For readability,
    # assign these to semantically proper variable names.
    caching = args.no_caching
    local = args.no_local

    conf = SparkConf()
    if local:
        conf.set("spark.driver.memory", args.spark_driver_memory) \
            .setMaster(f"local[{args.n_cores}]")
    spark_context = SparkContext(conf=conf)

    main(
        args.input_path,
        args.output_path,
        caching,
        spark_context
    )
