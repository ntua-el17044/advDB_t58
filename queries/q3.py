
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, desc, row_number, asc, avg,count, max, month, dayofmonth, hour, first
import os
import sys
import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

start = time.time()

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
print("spark session created")

df = spark.read.parquet("/data/yellow_tripdata_2022-0*.parquet")
lt = spark.read.option("header",True).csv("hdfs://192.168.0.1:9000/data/taxi+_zone_lookup.csv")

w = df.select("tpep_pickup_datetime","Trip_distance","Total_amount","PULocationID","DOLocationID")\
.join(lt.withColumnRenamed("Borough", "SBorough").withColumnRenamed("Zone","SZone"),df.PULocationID == lt.LocationID,"inner").drop("LocationID","service_zone")\
.join(lt.withColumnRenamed("Borough", "DBorough").withColumnRenamed("Zone","DZone"),df.DOLocationID == lt.LocationID,"inner").drop("LocationID","service_zone")\
.where((col("PULocationID")!=col("DOLocationID")) & (col("SZone")!=col("DZone")))\
.withColumn("month", month("tpep_pickup_datetime"))\
.withColumn("half", dayofmonth("tpep_pickup_datetime")>15)\
.groupBy("month","half")\
.agg(avg("Trip_distance"),avg("Total_amount"))\
.orderBy("month","half")

w1 = w.collect()

w.show(12)

end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')
