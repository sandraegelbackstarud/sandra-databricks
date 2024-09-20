-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Sista delen - Gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Skapa en vy som är redo att konsumeras av verksamheten
-- MAGIC
-- MAGIC Joina elpris data från silver med kalendern från silver in den slutgiltiga vyn i guld

-- COMMAND ----------

CREATE OR REPLACE view <view_name> AS
  SELECT
    ...

-- COMMAND ----------

select * from <view_name>

-- COMMAND ----------


