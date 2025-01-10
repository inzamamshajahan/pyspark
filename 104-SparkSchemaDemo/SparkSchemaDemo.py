from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
    .builder \
    .master("local[3]") \
    .appName("SparkSchemaDemo") \
    .getOrCreate()

    logger = Log4j(spark)

    # flightTimeCsvDF = spark.read \
    # .format('csv') \
    # .option("header", "true") \
    # .load("data/flight*.csv")
    # # in the above method of reading the DF all the columns will be given a string data type

    flightTimeCsvDF = spark.read \
        .format('csv') \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/flight*.csv")
    # in the above method the schema will be infered, that is not all the columns will be string.

    flightTimeCsvDF.show(5)
    logger.info("CSV Schema:" + flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = spark.read \
        .format('json') \
        .load("data/flight*.json")
    # json do not come with header so removed the header option
    # based on the following docs, there is no infer schema method available, therefore removed that too from the options we used for csv
    # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.json.html#pyspark.sql.DataFrameReader.json
    # looks like the json will always infer the schema

    flightTimeJsonDF.show(5)
    logger.info("JSON Schema:" + flightTimeJsonDF.schema.simpleString())
    # in the output we were able to see that the columns were sorted in the alphabetical order
    # from the above output the integers were converted to big int but the date column were still string.

    flightTimeParquetDF = spark.read \
        .format('parquet') \
        .load("data/flight*.parquet")

    flightTimeParquetDF.show(5)
    logger.info("Parquet Schema:" + flightTimeParquetDF.schema.simpleString())
    # in this method the order and the data type of the files were same as the source, as the schema details are also available in a parquet file.
    # it is the recommended data type for processing.

