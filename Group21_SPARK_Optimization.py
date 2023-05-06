import pyspark
from delta import *
import argparse
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import isnan
from pyspark.sql.functions import broadcast

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time

from pyspark.sql import SQLContext
from pyspark.sql import types
from pyspark.sql.functions import isnan, when, count, col
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import  StringIndexer, VectorAssembler, StandardScaler, ChiSqSelector
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import concat, to_timestamp
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as F

### Spark Session optimization
#spark = (SparkSession.builder.master('yarn').appName("PARQUETtoCSV")
 #       .config("spark.driver.memory", "1500m")
  #      .config("spark.executor.instances", "2")
   #      .config("spark.executor.cores" , 2 )
   #     .config("spark.executor.memory", "1500m")
    #    .config("spark.executor.memoryOverhead", "1500m")
     #   .config("spark.driver.memoryOverhead", "1500m")
      #  .getOrCreate())




builder = pyspark.sql.SparkSession.builder.appName("MyApp").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


## Test for the data skewness
spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", False)
# Check the parameters
spark.conf.set("spark.sql.shuffle.partitions", 5)
print(spark.conf.get("spark.sql.adaptive.enabled"))
print(spark.conf.get("spark.sql.shuffle.partitions"))


programStart = time.time()
start = programStart
df=spark.read.format("csv").option("header","false").load("hdfs://namenode:9000/project_data/CSV1/part-00000")

print(f"Time taken to read CSV {time.time() - start} seconds")

#df.write.format("parquet").save("hdfs://namenode:9000/project_data/data_parquet/")

start = time.time()
df = spark.read.parquet("hdfs://namenode:9000/project_data/data_parquet/")
print(f"Time taken to read Parquet file {time.time() - start} seconds")



col_names =[ 'Ven_ID', 'Pick_Date', 'Pick_Time', 'Drop_Date', 'Drop_Time', 'Pass_Count', 'Trip_Dist', 'PULocID', 'DOLocID' ,'Pay_Type' ,'Fare' ,'Extra', 'Mta_Tax', 'Tip' ,'Toll','Improv_Sur' , 'Total' , 'Cong_Sur' ]
df =df.toDF(*col_names)

df = df.withColumn("Ven_ID", col("Ven_ID").cast("integer"))
df = df.withColumn("Pick_Time", to_timestamp(df["Pick_Time"], "HH:mm:ss").cast(TimestampType()))
df = df.withColumn("Drop_Time", to_timestamp(df["Drop_Time"], "HH:mm:ss").cast(TimestampType()))

df = df.withColumn("Pick_Date", col("Pick_Date").cast("date"))
df = df.withColumn("Drop_Date", col("Drop_Date").cast("date"))
df = df.withColumn("Pass_Count", col("Pass_Count").cast("integer"))

df = df.withColumn("Trip_Dist", col("Trip_Dist").cast("double"))
df = df.withColumn("PULocID", col("PULocID").cast("integer"))
df = df.withColumn("DOLocID", col("DOLocID").cast("integer"))
df = df.withColumn("Pay_Type", col("Pay_Type").cast("integer"))
df = df.withColumn("Fare", col("Fare").cast("double"))
df = df.withColumn("Extra", col("Extra").cast("double"))
df = df.withColumn("Mta_Tax", col("Mta_Tax").cast("double"))
df = df.withColumn("Tip", col("Tip").cast("double"))
df = df.withColumn("Toll", col("Toll").cast("double"))
df = df.withColumn("Improv_Sur", col("Improv_Sur").cast("double"))
df = df.withColumn("Total", col("Total").cast("double"))
df = df.withColumn("Cong_Sur", col("Cong_Sur").cast("double"))

## Run some aggregation operatoprs using pay_type
start = time.time()
df.groupby("Pay_Type").count().sort("count", ascending=False).show(5)
df.groupby("Pay_Type").avg('Total').sort("Pay_Type", ascending=False).show(5)
print(f"Time taken for count , fare average using the Pay_Type {time.time() - start} seconds")


df = df.withColumn('salt', F.rand())
df.repartition(5,'salt')

## Run some aggregation operatoprs using pay_type
start = time.time()
df.groupby("Pay_Type").count().sort("count", ascending=False).show(5)
df.groupby("Pay_Type").avg('Total').sort("Pay_Type", ascending=False).show(5)
print(f"Time taken for count , fare average using the Pay_Type {time.time() - start} seconds")



df2 = df.where(col("Pass_Count").isNotNull() & col("Cong_Sur").isNotNull())
#
###### Test processing for some of the functions
##print(df2.select("Trip_Dist").describe().toPandas())
##print(df2.select("Pass_Count").describe().toPandas())
##print(df2.select("Tip").describe().toPandas())
##print(df2.select("Total").describe().toPandas())
##print(df2.select("Pay_Type").describe().toPandas())
#
#
#
#### Filtering out problematic Data
#
## create a new DataFrame based on the query
result_df = df2.filter((col("Total") < 2000) & (col("Total") > 0) & (col("Trip_Dist") < 1000) & (col("Trip_Dist") > 0.01) & (col("Tip") >= 0.00))
print(df2.count())
print(result_df.count())
#
#
## Show the resulting DataFrame
#result_df.show(5)
##result_df.cache()
##result_df.persist()
#
### Load the zone lookup table from HDFS
zone_df =spark.read.format("csv").option("header","true").load("hdfs://namenode:9000/project_data/zone_lookup.csv")
zone_df.show(10)
#
#### Perform Join Operation using broadcast
#
Join_time = time.time()
#result_df = result_df.join(zone_df, result_df.PULocID ==  zone_df.LocationID, "left")
result_df = result_df.join(broadcast(zone_df), result_df["PULocID"] == zone_df["LocationID"])
print(f"Join completed job in {time.time() - Join_time} seconds")

#result_df.show(5)

# Example of Bucketing

#(result_df.write.format('parquet')  
#    .bucketBy(20, "PULocID")
#    .mode("overwrite")
#    .saveAsTable("PULocID_Table"))

#bucket_df = spark.read.table("PULocID_Table") 
bucket_df = spark.read.format("parquet").option("path", "home/ubuntu/spark-warehouse/PULocID_Table").load()

bucket_df.show(5)

### Find some aggregated parameters based on Location ID 
start = time.time()
result_df.groupby("PULocID").count().sort("count" , ascending=False).show(5)
result_df.groupby("PULocID").avg().sort("PULocID", ascending=True).show(5)
result_df.groupby("PULocID").max().sort("PULocID", ascending=True).show(5)
result_df.groupby("PULocID").min().sort("PULocID", ascending=True).show(5)
result_df.groupby("PULocID").sum().sort("PULocID", ascending=True).show(5)
print(f"Aggregared functions without bucketing {time.time() - start} seconds")

## Calculate the same aggregated parameters on bucketed table
start = time.time()
bucket_df.groupby("PULocID").count().sort("count" , ascending=False).show(5)
bucket_df.groupby("PULocID").avg().sort("PULocID", ascending=True).show(5)
bucket_df.groupby("PULocID").max().sort("PULocID", ascending=True).show(5)
bucket_df.groupby("PULocID").min().sort("PULocID", ascending=True).show(5)
bucket_df.groupby("PULocID").sum().sort("PULocID", ascending=True).show(5)
print(f"Aggregared functions with bucketing {time.time() - start} seconds")



#(result_df.groupby("DOLocID").count().sort("count", ascending=False)).show(5)
#
#(result_df.groupby("Fare").avg()).show(10)
#
#(result_df.groupby("PULocID" , "DOLocID").count().sort("count", ascending=False)).show(5)
#
#(result_df.groupby("Fare").avg()).show(5)
#
#
#result_df.show(5)
#
#print(f"... completed job in {time.time() - start} seconds")
#