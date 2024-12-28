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
    survey_df.show()
    
    logger.info("Finished HelloSpark")
    # spark.stop()