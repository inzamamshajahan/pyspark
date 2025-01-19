import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logger import *


def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"

if __name__ == "__main__":
    spark = SparkSession \
    .builder \
    .appName("UDF Demo") \
    .master("local[2]") \
    .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/survey.csv")

    survey_df.show(10)
    # Register it as a UDF
    # The below line registers our function as a UDF in the spark session
    # The driver will serialize and send this function to the executor
    # The below method do not create a UDF in the catalog
    parse_gender_udf = udf(parse_gender, StringType())
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))

    survey_df2.show(10)

    # Using the UDF in a SQL expression.
    # Here the registration is a bit different.
    # Here we register the function as a SQL function and it goes to the catalog
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)
