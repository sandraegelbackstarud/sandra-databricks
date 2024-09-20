# Databricks notebook source
import pandas as pd
import requests
from datetime import date, timedelta
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType, FloatType, DecimalType

# COMMAND ----------

PRISKLASS = ['SE1','SE2','SE3','SE4']
start_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
end_date = date.today().strftime("%Y-%m-%d")
DATUM = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d") # Format: YYYY-MM-DD
DATUM

# COMMAND ----------

elpriserRawDataDirectory = 'databricks_academy/raw/elpriser/'

def cleanup_folder(path):
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)

def fetch_data():

    data = []
    for p in PRISKLASS:
        for d in DATUM:
            api_url = f'https://www.elprisetjustnu.se/api/v1/prices/{d[:4]}/{d[5:7]}-{d[8:]}_{p}.json'
            print(api_url)
            response = requests.get(url=api_url)
            print(response.status_code)
            data_json = response.json()
            data_json = [dict(item, **{'elzon': p}) for item in data_json]
            data += data_json

    elpris = spark.createDataFrame(sc.parallelize(data))
    elpris = elpris.cache()
    elpris.repartition(10).write.format("json").mode("append").option("overwriteSchema", True).save(elpriserRawDataDirectory)
    cleanup_folder(elpriserRawDataDirectory)

    return data

def load_data():
    return spark.table('emanuel_db.bronze.elpriser')
    

# COMMAND ----------

data = fetch_data()

# COMMAND ----------


