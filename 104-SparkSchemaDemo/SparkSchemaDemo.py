from pyspark.sql import SparkSession
from lib.logger import Log4j
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

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

    # flightTimeCsvDF = spark.read \
    #     .format('csv') \
    #     .option("header", "true") \
    #     .option("inferSchema", "true") \
    #     .load("data/flight*.csv")
    # # in the above method the schema will be infered, that is not all the columns will be string.

    # flightTimeCsvDF.show(5)
    # logger.info("CSV Schema:" + flightTimeCsvDF.schema.simpleString())
    
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])
    
    flightTimeCsvDF = spark.read \
        .format('csv') \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.csv")
    # if the data type specified in the schema object do not match with the data being loaded, pyspark will throw a runtime error.
    # when we specify the schema we should also specify the dateformat
    

    flightTimeCsvDF.show(5)
    logger.info("CSV Schema:" + flightTimeCsvDF.schema.simpleString())
    
    #Below is the DDL method to specify the schema
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""
          
    flightTimeCsvDF = spark.read \
        .format('csv') \
        .option("header", "true") \
        .schema(flightSchemaDDL) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.csv")     
    
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

