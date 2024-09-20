# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Första delen - Data ingestion

# COMMAND ----------

import pandas as pd
import requests
from datetime import date, timedelta
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType, FloatType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC spark session

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %run ./helper_functions/calendar

# COMMAND ----------

database = 'emanuel_db'
create_calendar(database=database)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Skapa catalog (Traditionellt - Database)
# MAGIC ```%sql create catalog if not exists emanuel_db```

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists emanuel_db

# COMMAND ----------

# MAGIC %md
# MAGIC ###Skapa databas (Traditionellt - Schema)
# MAGIC ```%sql create schema if not exists <catalog>.bronze```

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema if not exists emanuel_db.bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Med Autoloader
# MAGIC
# MAGIC Ingest data med Autoloader.
# MAGIC ```.readStream``` används för inkrementell data laddning (streaming) - Spark bestämmer vilken ny data som inte ännu har processats. 

# COMMAND ----------

name = "_".join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user').split("@")[0].split(".")[0:2])
deltaTablesDirectory = '/Users/'+name+'/elpriser/'
dbutils.fs.mkdirs(deltaTablesDirectory)
elpriserRawDataDirectory = 'databricks_academy/raw/elpriser/'

schema = 'bronze'
table = 'elpriser_bronze'

def ingest_folder(folder, data_format, table):
  bronze_products = (spark.readStream
                      .format("cloudFiles")
                      .option("cloudFiles.format", data_format)
                      .option("cloudFiles.inferColumnTypes", "true")
                      .option("cloudFiles.schemaLocation",
                              f"{deltaTablesDirectory}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                      .load(folder))
  return (bronze_products.writeStream
            .option("checkpointLocation",
                    f"{deltaTablesDirectory}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
            .option("overwriteSchema", "true") #merge any new column dynamically
            .trigger(once = True) #Remove for real time streaming
            .table(table)) #Table will be created if we haven't specified the schema first
  
ingest_folder('/'+elpriserRawDataDirectory, 'json',  f'{database}.{schema}.{table}').awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emanuel_db.bronze.elpriser_bronze

# COMMAND ----------


