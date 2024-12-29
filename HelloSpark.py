from pyspark.sql import *
from lib.logger import *
from lib.utils import *
import sys



if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting HelloSpark")

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)
    # Processing code
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    survey_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)

    logger.info(count_df.collect())
    # In the above code we used collect() instead of show(), because
    # The collect() action returnes the result as a python list
    # The show() method is a utiity function to print the dataframe, usually used for debugging purposes

    input("Press Enter")
    # The above input ensures that our program do not finish so that we can analyze the execution plan via spark UI
    # While a pyspark program is running localhost:4040 shows the execution plan

    logger.info("Finished HelloSpark")
    # spark.stop()