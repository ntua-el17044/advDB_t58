
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, desc, row_number, asc, avg, max, month, dayofmonth, hour, first, dayofweek
import os
import sys
import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

start = time.time()

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("spark session created")

df = spark.read.parquet("/data/yellow_tripdata_2022-0*.parquet")

windowC = Window.partitionBy("day").orderBy(col("congestion").desc(),col("day").asc())

w = df.select("tpep_pickup_datetime","Passenger_count")\
.withColumn("day", dayofweek("tpep_pickup_datetime"))\
.withColumn("hour", hour("tpep_pickup_datetime"))\
.groupBy("day","hour")\
.agg(avg("Passenger_count").alias("congestion"))\
.withColumn("row",row_number().over(windowC))\
.filter(col("row") <= 3)\
.drop("row")\
.drop("tpep_pickup_datetime")\
.drop("Passenger_count")

w1 = w.collect()

w.show(21)

end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')
