
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour, first
import os
import sys
import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

start = time.time()

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("spark session created")

df = spark.read.parquet("/data/yellow_tripdata_2022-0*.parquet")

windowM =Window.partitionBy("month").orderBy(desc(col("Tolls_amount")))

w = df.withColumn("month", month("tpep_pickup_datetime"))\
.withColumn("row",row_number().over(windowM))\
.filter(col("row")==1).drop("row")\
.orderBy(asc(col("month")))

w1 = w.collect()

w.show(6)

end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')
