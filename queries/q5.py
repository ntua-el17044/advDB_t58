
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

windowC = Window.partitionBy("month").orderBy(col("average tip percentage").desc(),col("month").asc())

w = df.select("tpep_pickup_datetime","Fare_amount","Tip_amount")\
.withColumn("day", dayofmonth(df.tpep_pickup_datetime))\
.withColumn("month", month(df.tpep_pickup_datetime))\
.withColumn("tipp",df.tip_amount/df.fare_amount)\
.drop("tpep_pickup_datetime")\
.drop("Fare_amount")\
.drop("Tip_amount")\
.groupBy("month","day")\
.agg(avg("tipp").alias("average tip percentage"))\
.withColumn("row",row_number().over(windowC))\
.filter(col("row") <= 5)\
.drop("row")

w1 = w.collect()

w.show(30)

end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')
