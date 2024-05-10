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
sc = spark.sparkContext
rdd = sc.textFile("hdfs://localhost:9000/user/data/mimodump-dataset.csv")


header = rdd.first()
rdd = rdd.filter(lambda line: line != header).map(lambda line: line.split(';'))

# 1. Select name from ItemPrice where current_price > 500
start_time = time.time()
query_1_result = rdd.filter(lambda x: len(x) > 2 and (float(x[2]) > 500 if x[2].replace('.', '', 1).isdigit() else False)).map(lambda x: x[0]).collect()
end_time = time.time()
print("Query 1 Execution Time:", end_time - start_time, "seconds")
# print("Query 1 Result:", query_1_result)

# 2. Select COUNT(name) from ItemPrice where current_price > 500
start_time = time.time()
query_2_result = rdd.filter(lambda x: len(x) > 2 and (float(x[2]) > 500 if x[2].replace('.', '', 1).isdigit() else False)).map(lambda x: x[0]).count()
end_time = time.time()
print("Query 2 Execution Time:", end_time - start_time, "seconds")
print("Query 2 Result:", query_2_result)

# 3. SELECT brand_name, AVG(review_stars) AS avg_review_stars FROM ItemPrice GROUP BY brand_name ORDER BY avg_review_stars DESC LIMIT 25
start_time = time.time()
brand_review_rdd = rdd.filter(lambda x: len(x) > 18 and x[17] and x[18] and (x[17].replace('.', '', 1).isdigit() if x[17] else False)) \
                      .map(lambda x: (x[18], (float(x[17]), 1))) \
                      .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                      .mapValues(lambda x: x[0] / x[1]) \
                      .sortBy(lambda x: x[1], ascending=False)
query_3_result = brand_review_rdd.take(25)
end_time = time.time()
print("Query 3 Execution Time:", end_time - start_time, "seconds")
print("Query 3 Result:", query_3_result)


# 4. WITH category_avg_price AS (
#    SELECT category_name, AVG(current_price) AS avg_price
#    FROM ItemPrice
#    GROUP BY category_name
#    )
#    SELECT category_name, avg_price
#    FROM category_avg_price
#    ORDER BY avg_price DESC
#    LIMIT 10;

start_time = time.time()
category_avg_price_rdd = rdd.filter(lambda x: len(x) > 7 and x[2] and (float(x[2]) if x[2].replace('.', '', 1).isdigit() else False)) \
                             .map(lambda x: (x[7], (float(x[2]), 1))) \
                             .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                             .mapValues(lambda x: x[0] / x[1]) \
                             .sortBy(lambda x: x[1], ascending=False)
query_4_result = category_avg_price_rdd.take(10)
end_time = time.time()
print("Query 4 Execution Time:", end_time - start_time, "seconds")
print("Query 4 Result:", query_4_result)

sc.stop()
