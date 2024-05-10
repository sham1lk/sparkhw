import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col  # Add this line
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import time

spark = (
    SparkSession.builder
        .master("local[*]")
        .appName("SQL Test")
        .getOrCreate()
)

schema = "name string, price float, current_price float, lowest_price float, msrp_price float, image string, item_url string, category_name string, category_url string, available string, in_promotion string, lowest_product_price float, on_sale string, free_delivery string, product_id string, review_count int, review_stars float, short_desc string, brand_name string, item_url_key string, parsed_at string"

# Read CSV file into dataset
dataset = spark.read.csv("hdfs://localhost:9000/user/data/mimodump-dataset.csv", header=True, schema=schema, sep=";")

# Register dataset as a temporary view
dataset.createOrReplaceTempView("ItemPrice")

# Query 1: Select name from ItemPrice where current_price > 500
start_time = time.time()
query_1_result = spark.sql("SELECT name FROM ItemPrice WHERE current_price > 500").show()
end_time = time.time()
print("Query 1 Execution Time:", end_time - start_time, "seconds")

# Query 2: Select COUNT(name) from ItemPrice where current_price > 500
start_time = time.time()
query_2_result = spark.sql("SELECT COUNT(name) FROM ItemPrice WHERE current_price > 500").show()
end_time = time.time()
print("Query 2 Execution Time:", end_time - start_time, "seconds")

# Query 3: SELECT brand_name, AVG(review_stars) AS avg_review_stars FROM ItemPrice GROUP BY brand_name ORDER BY avg_review_stars DESC LIMIT 25
start_time = time.time()
query_3_result = spark.sql("SELECT brand_name, AVG(review_stars) AS avg_review_stars FROM ItemPrice GROUP BY brand_name ORDER BY avg_review_stars DESC LIMIT 25").show()
end_time = time.time()
print("Query 3 Execution Time:", end_time - start_time, "seconds")

# Query 4: WITH category_avg_price AS ( SELECT category_name, AVG(current_price) AS avg_price FROM ItemPrice GROUP BY category_name ) SELECT category_name, avg_price FROM category_avg_price ORDER BY avg_price DESC LIMIT 10
start_time = time.time()
query_4_result = spark.sql("WITH category_avg_price AS (SELECT category_name, AVG(current_price) AS avg_price FROM ItemPrice GROUP BY category_name) SELECT category_name, avg_price FROM category_avg_price ORDER BY avg_price DESC LIMIT 10").show()
end_time = time.time()
print("Query 4 Execution Time:", end_time - start_time, "seconds")

spark.stop()
