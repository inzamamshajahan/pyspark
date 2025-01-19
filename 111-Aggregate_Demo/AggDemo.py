from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
    .appName("Agg Demo") \
    .master("local[2]") \
    .getOrCreate()

    logger = Log4j(spark)

    invoice_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/invoices.csv")

    # Summarization/Simple Aggregations, which gives a single row as output.
    # Below is the syntax using the column objects
    invoice_df.select(f.count('*').alias("Count *"),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrice"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")
                      ).show()

    # Below is the syntax for the same Aggregations using the SQL like expressions
    # In spark the count(*) and count(1) will count all the rows even if they have null values in all
    # the columns
    # Counting a field will not count the null values
    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
    ).show()

    # Group Aggregations
    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
    SELECT Country, InvoiceNo,
    sum(Quantity) as TotalQuantity,
    round(sum(Quantity * UnitPrice), 2) as InvoiceValue
    FROM sales
    GROUP BY Country, InvoiceNo
    """)

    summary_sql.show()


