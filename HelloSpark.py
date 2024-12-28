from pyspark.sql import *
from lib import logger as lg

if __name__ == "__main__":

    spark = SparkSession.builder \
    .appName("Hello Spark") \
    .master("local[3]") \
    .getOrCreate()

    logger = lg.Log4j(spark)
    logger.info("Starting HelloSpark")
    # Processing code
    logger.info("Finished HelloSpark")
    # spark.stop()