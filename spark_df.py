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

df = spark.read.csv("hdfs://localhost:9000/user/data/mimodump-dataset.csv", header=True, inferSchema=True, sep=";")

# Query 1: Select name from ItemPrice where current_price > 500
start_time = time.time()
query_1_result = df.select("name").filter(col("current_price") > 500).show()
end_time = time.time()
print("Query 1 Execution Time:", end_time - start_time, "seconds")

# Query 2: Select COUNT(name) from ItemPrice where current_price > 500
start_time = time.time()
query_2_result = df.filter(col("current_price") > 500).count()
print("Query 2 Execution Time:", time.time() - start_time, "seconds")

# Query 3: SELECT brand_name, AVG(review_stars) AS avg_review_stars FROM ItemPrice GROUP BY brand_name ORDER BY avg_review_stars DESC LIMIT 25
start_time = time.time()
query_3_result = df.groupBy("brand_name").agg({"review_stars": "avg"}).orderBy(col("avg(review_stars)").desc()).limit(25).show()
end_time = time.time()
print("Query 3 Execution Time:", end_time - start_time, "seconds")

# Query 4: WITH category_avg_price AS ( SELECT category_name, AVG(current_price) AS avg_price FROM ItemPrice GROUP BY category_name ) SELECT category_name, avg_price FROM category_avg_price ORDER BY avg_price DESC LIMIT 10
start_time = time.time()
category_avg_price = df.groupBy("category_name").agg({"current_price": "avg"}).withColumnRenamed("avg(current_price)", "avg_price")
query_4_result = category_avg_price.orderBy(col("avg_price").desc()).limit(10).show()
end_time = time.time()
print("Query 4 Execution Time:", end_time - start_time, "seconds")

spark.stop()
