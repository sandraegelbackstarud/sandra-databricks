-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Translate ingestion flow from pyspark

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Skapa en ny elpristabell med samma kolumner som i den andra tabellen fast med Spark SQL
-- MAGIC ```
-- MAGIC CREATE TABLE [IF NOT EXITS] catalog.schema.table
-- MAGIC (
-- MAGIC   COL1  DATA_TYPE,
-- MAGIC   COL2  DATA_TYPE,
-- MAGIC   ...
-- MAGIC   COLN  DATA_TYPE
-- MAGIC )
-- MAGIC ```

-- COMMAND ----------

create table if not exists emanuel_db.bronze.elprizer_bronze_sql
(
  ...
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use ```COPY INTO``` för data ingestion till den nyskapade tabellen
-- MAGIC
-- MAGIC - Path ```'/databricks_academy/raw/elpriser/'```
-- MAGIC - Filformat ```json```
-- MAGIC - Format Option ```('mergeSchema' = 'true')```
-- MAGIC - Copy Option ```('mergeSchema' = 'true')```

-- COMMAND ----------

copy into 
...
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Validera att allt ser ok ut med tex ```count(*)``` eller ```select * ... limit 10 ```

-- COMMAND ----------

select * from ...

-- COMMAND ----------

select count(*) ...

-- COMMAND ----------


