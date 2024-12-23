# Databricks notebook source



# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.rohitnyctaxidatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.rohitnyctaxidatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.rohitnyctaxidatalake.dfs.core.windows.net", ApplicationID)
spark.conf.set("fs.azure.account.oauth2.client.secret.rohitnyctaxidatalake.dfs.core.windows.net", secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.rohitnyctaxidatalake.dfs.core.windows.net", f"https://login.microsoftonline.com/{DirectoryID}/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@rohitnyctaxidatalake.dfs.core.windows.net/")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark SQL basic example").getOrCreate()


# COMMAND ----------

df_triptype = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@rohitnyctaxidatalake.dfs.core.windows.net/trip_type/')

# COMMAND ----------

df_triptype.display()

# COMMAND ----------

df_tripzone = spark.read.format('csv').option("header",True).option("inferSchema",True).load('abfss://bronze@rohitnyctaxidatalake.dfs.core.windows.net/trip_zone/')

# COMMAND ----------

df_tripzone.display()

# COMMAND ----------

myschema = '''
            VendorID BIGINT,
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            store_and_fwd_flag STRING,
            RatecedelD BIGINT,
            PULocationID BIGINT,
            DOLOCationID BIGINT,
            Passenger_count BIGINT,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type BIGINT, 
            trip_type BIGINT,
            congestion_surcharge DOUBLE
           '''

# COMMAND ----------

df_trip = spark.read.format("parquet").option("header",True).schema(myschema).option('recursiveFileLookup',True).load('abfss://bronze@rohitnyctaxidatalake.dfs.core.windows.net/data2032/')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.count()

# COMMAND ----------

df_triptype.display() 


# COMMAND ----------

df_triptype = df_triptype.withColumnRenamed('description', 'trip_description')

# COMMAND ----------

df_triptype.display() 


# COMMAND ----------

df_triptype.write.format("parquet").mode('append').option("path","abfss://silver@rohitnyctaxidatalake.dfs.core.windows.net/trip_type").save()

# COMMAND ----------

df_tripzone.display()

# COMMAND ----------

df_tripzone = df_tripzone.withColumn('zone1', split(col('Zone'), '/')[0])\
                .withColumn('zone2', split(col('Zone'), '/')[1])


# COMMAND ----------

df_tripzone.display()

# COMMAND ----------

df_tripzone.write.format('parquet').mode('append').option("path","abfss://silver@rohitnyctaxidatalake.dfs.core.windows.net/trip_zonee").save()

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date',to_date(col('lpep_pickup_datetime')))\
            .withColumn('trip_year',year(col('lpep_pickup_datetime')))\
                .withColumn('trip_month',month(col('lpep_pickup_datetime')))\
                    .withColumn('trip_day',dayofmonth(col('lpep_pickup_datetime')))\
                         .withColumn('trip_hour',hour(col('lpep_pickup_datetime')))

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.createOrReplaceTempView("df_trip")

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(total_amount) as total_amount from df_trip

# COMMAND ----------

df_trip.write.format('parquet').mode('append').option("path","abfss://silver@rohitnyctaxidatalake.dfs.core.windows.net/trip_2023data").save()

# COMMAND ----------



# COMMAND ----------

df_trip.printSchema()

# COMMAND ----------

df_trip = df_trip.withColumn("column_name", col("column_name").cast("long"))

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.drop(col('trip_day'))\
            .drop(col('trip_hour')) 

# COMMAND ----------

df_trip.show(10)

# COMMAND ----------

df_trip.write.format('parquet').mode('append').option("path","abfss://silver@rohitnyctaxidatalake.dfs.core.windows.net/trip_2023dataa").save()

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.printSchema()
