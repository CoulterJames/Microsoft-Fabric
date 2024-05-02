-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "4af86401-715f-4058-835f-be09b42a335a",
-- META       "default_lakehouse_name": "Silver_Layer",
-- META       "default_lakehouse_workspace_id": "d597b07b-fc71-4e47-8be1-0613db6a94ec"
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC 
-- MAGIC from pyspark.sql import functions as F
-- MAGIC 
-- MAGIC df = spark.createDataFrame([(1,)])
-- MAGIC df = df.withColumn(
-- MAGIC     "timeSeq", 
-- MAGIC     F.explode(F.expr("sequence(0, 1439)"))
-- MAGIC )
-- MAGIC df = df.withColumn("hour", F.lpad(F.floor(df.timeSeq / 60).cast("string"),2,"0"))
-- MAGIC df = df.withColumn("minute", F.lpad((df.timeSeq % 60).cast("string"),2,"0"))
-- MAGIC 
-- MAGIC df = df.withColumn("time",  F.concat(df.hour, F.lit(":"), df.minute))
-- MAGIC 
-- MAGIC df = df.withColumn("amorpm",F.when(df.time < "12:00", 'AM').otherwise('PM'))
-- MAGIC 
-- MAGIC df.createOrReplaceTempView("timedim")


-- CELL ********************

CREATE OR REPLACE TEMPORARY VIEW TimeView
AS

SELECT timeSeq+1 AS TimeKey,
    time AS Time,
    hour AS Hour,
    minute AS Minute,       
    amorpm AS AmOrPm
FROM timedim

-- CELL ********************

MERGE INTO Silver_Layer.Time USING TimeView ON Silver_Layer.Time.TimeKey = TimeView.TimeKey
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
