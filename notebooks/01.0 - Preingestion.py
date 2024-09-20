# Databricks notebook source
import pandas as pd
import requests
from datetime import date
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, TimestampType, DoubleType, FloatType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC elprisetjustnu.se har ett öppet API för att hämta el-data från de senaste månaderna.
# MAGIC
# MAGIC Hämta eldata via API
# MAGIC GET https://www.elprisetjustnu.se/api/v1/prices/[ÅR]/[MÅNAD]-[DAG]_[PRISKLASS].json

# COMMAND ----------

# MAGIC %md
# MAGIC <table class="table table-sm mb-5">
# MAGIC             <tbody><tr>
# MAGIC               <th>Variabel</th>
# MAGIC               <th>Beskrivning</th>
# MAGIC               <th>Exempel</th>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>ÅR</th>
# MAGIC               <td>Alla fyra siffror</td>
# MAGIC               <td>2023</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>MÅNAD</th>
# MAGIC               <td>Alltid två siffror, med en inledande nolla</td>
# MAGIC               <td>03</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>DAG</th>
# MAGIC               <td>Alltid två siffror, med en inledande nolla</td>
# MAGIC               <td>13</td>
# MAGIC             </tr>
# MAGIC             <tr>
# MAGIC               <th>PRISKLASS</th>
# MAGIC               <td>
# MAGIC                 SE1 = Luleå / Norra Sverige<br>
# MAGIC                 SE2 = Sundsvall / Norra Mellansverige<br>
# MAGIC                 SE3 = Stockholm / Södra Mellansverige<br>
# MAGIC                 SE4 = Malmö / Södra Sverige
# MAGIC               </td>
# MAGIC               <td>SE3</td>
# MAGIC             </tr>
# MAGIC           </tbody></table>

# COMMAND ----------

# MAGIC %md
# MAGIC Använd endpointen för att hämta historisk data. Tidigast tillängliga datum är 2022-10-26. 
# MAGIC
# MAGIC Lämpliga paket i Python att använda:
# MAGIC
# MAGIC - ```requests``` - API-anrop. [docs](https://requests.readthedocs.io/en/latest/)
# MAGIC - ```pandas``` - Pandas DataFrame. [docs](https://pandas.pydata.org/docs/)
# MAGIC - ```datetime``` - Datumhantering [docs](https://docs.python.org/3/library/datetime.html)
# MAGIC - ```pyspark``` - Datahantering i spark. [docs](https://spark.apache.org/docs/3.1.3/api/python/index.html)
# MAGIC
# MAGIC Skapa en funktion som loopar över en array med datum

# COMMAND ----------

PRISKLASS = ['SE1','SE2','SE3','SE4']
end_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d") #Yesterdays date
DATUM = pd.date_range(start='2022-12-01', end=end_date).strftime("%Y-%m-%d") # Format: YYYY-MM-DD
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
    elpris.repartition(10).write.format("json").mode("overwrite").option("overwriteSchema", True).save(elpriserRawDataDirectory)
    cleanup_folder(elpriserRawDataDirectory)

    return data

def load_data():
    return spark.table('emanuel_db.bronze.elpriser')
    


# COMMAND ----------

data = fetch_data()

# COMMAND ----------

# Listing the files under the directory
for fileInfo in dbutils.fs.ls(elpriserRawDataDirectory): print(fileInfo.name)
