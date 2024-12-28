from pyspark.sql import *

if __name__ == "__main__":

    spark = SparkSession.builder \
    .appName("Hello Spark") \
    .master("local[2]") \
    .getOrCreate()

    data_list = [
        ("Ravi", 28),
        ("David", 45),
        ("Abdul", 37)
    ]

    print(data_list)