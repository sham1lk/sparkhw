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
rdd = rdd.filter(lambda line: line != header).map(lambda line: line.replace(';', ''))
char_rdd = rdd.flatMap(lambda line: list(line))

char_rdd = char_rdd.filter(lambda c: c.isalpha())
char_count_rdd = char_rdd.map(lambda c: (c, 1))
char_count_total_rdd = char_count_rdd.reduceByKey(lambda a, b: a + b)
top_5_chars = char_count_total_rdd.sortBy(lambda x: x[1], ascending=False).take(5)

print("Total occurrences of each character:")
print(top_5_chars.collect())

sc.stop()
