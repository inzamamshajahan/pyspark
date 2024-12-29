import configparser
from pyspark import SparkConf
import sys

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

def load_survey_df(spark, data_file):
    return spark.read \
                 .option("header", "true") \
                 .option("inferSchema", "true") \
                 .csv(sys.argv[1])
    # in the above code read is one action
    # inferSchema is another action
    # so this function will lead to 2 jobs in spark UI, because it is 2 actions.

def count_by_country(survey_df):
    return survey_df \
    .where("Age < 40") \
    .select("Age", "Gender", "Country", "state") \
    .groupBy("Country") \
    .count()