-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Andra delen - Data transformation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Skapa tabellerna ```elpriser``` och ```calendar``` i schemat ```silver```
-- MAGIC
-- MAGIC ***Tips*** för calendar som har så många kolumner. Skriv ut schemat från ```silver.calendar```. Notera python dekoreringen

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read...

-- COMMAND ----------

create table if not exists <table_name>
(
  ...
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC skapa date_start om omvandla till date

-- COMMAND ----------

create table if not exists <table_name>
(
  ...
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Skapa date_start om omvandla till date.
-- MAGIC
-- MAGIC Tänk på att sätta upp en checker 

-- COMMAND ----------

insert into <table_name>
select 
  ...
from <table_name> src
--no duplicate handler
where not exists 
  (
    select 1 from <table_name> trg
    where trg.date = src.date
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Kolla status på den nya tabellen

-- COMMAND ----------

select count(*) from <table_name>

-- COMMAND ----------

insert into <table_name>
select
  EUR_per_kWh,
  EXR as exchange_rate,
  SEK_per_kWh,
  elzon,           
  to_timestamp(time_end) as time_end,
  to_timestamp(time_start) as time_start,
  to_date(time_start) as date_start
from <table_name> src
where not exists
  (
    select 1 from <table_name> trg
    where ...
  )


-- COMMAND ----------

select count(*) from emanuel_db.silver.elpriser_sql

-- COMMAND ----------


