# in this example we are going to generate spark avro output.
# Spark avro do not come bundled with apache spark
# so to work with avro data we need to include and additional scala package
# we are working of python ide and it do not add option to add any java or scala dependencies
# added it in the spark-default.conf file

from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logger import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("dataSource/flight*.parquet")

    #below is the code to check the number of partitions
    logger.info("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
    # below code is to find the number of records per partitions
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    # re-partitioning the dataframe
    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.info("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()

    # partitionedDF.write \
    # .format("avro") \
    # .mode("overwrite")  \
    # .option("path", "dataSink/avro/") \
    # .save()
    # the save method in the overwrite method cleans the directory first and writes in it

    # the number of output files which will be written in the output path will depend on the number of partitions
    # the partitions will be parallaly written by the executors
    # The .avro.crc is the checksum file for the data and the .avro is the actual data file
    # partitioning can help in 2 ways, 1. Parallel processing, 2. Partition Elimination

    flightTimeParquetDF.write \
    .format("json") \
    .mode("overwrite") \
    .option("path", "dataSink/json") \
    .partitionBy("OP_CARRIER", "ORIGIN") \
    .save()

