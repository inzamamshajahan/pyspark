from pyspark.sql import *
from lib.logger import *

if __name__ == "__main__":
    spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("SparkSQLTableDemo") \
    .enableHiveSupport() \
    .getOrCreate()

    # in teh above the enableHiveSupport is done to create a persistent hive metastore for the spark managed table creation

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
    .format("parquet") \
    .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    # flightTimeParquetDF.write \
    # .mode("overwrite") \
    # .partitionBy("ORGIN", 'OP_CARRIER') \
    # .saveAsTable("AIRLINE_DB.flight_data_tbl") # THIS IS OPTIONAL IF YOU SET HTE SETCURRENT DATABASE
    # # Above approach creates too many partitions due to too many orgins

    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5,"OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("AIRLINE_DB.flight_data_tbl")


    logger.info(spark.catalog.listTables("AIRLINE_DB"))

