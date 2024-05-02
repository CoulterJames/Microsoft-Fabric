-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "4af86401-715f-4058-835f-be09b42a335a",
-- META       "default_lakehouse_name": "Silver_Layer",
-- META       "default_lakehouse_workspace_id": "d597b07b-fc71-4e47-8be1-0613db6a94ec",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "4af86401-715f-4058-835f-be09b42a335a"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE OR REPLACE TEMPORARY VIEW DayNameReference AS
SELECT 1 AS DayOfWeek, 'Sunday' AS DayName
UNION 
SELECT 2 AS DayOfWeek, 'Monday' AS DayName
UNION 
SELECT 3 AS DayOfWeek, 'Tuesday' AS DayName
UNION 
SELECT 4 AS DayOfWeek, 'Wednesday' AS DayName
UNION 
SELECT 5 AS DayOfWeek, 'Thursday' AS DayName
UNION 
SELECT 6 AS DayOfWeek, 'Friday' AS DayName
UNION 
SELECT 7 AS DayOfWeek, 'Saturday' AS DayName

-- CELL ********************

CREATE OR REPLACE TEMPORARY VIEW MonthNameReference AS
SELECT 1 AS MonthOfYear, 'January' AS MonthName
UNION 
SELECT 2 AS MonthOfYear, 'February' AS MonthName
UNION 
SELECT 3 AS MonthOfYear, 'March' AS MonthName
UNION 
SELECT 4 AS MonthOfYear, 'April' AS MonthName
UNION 
SELECT 5 AS MonthOfYear, 'May' AS MonthName
UNION 
SELECT 6 AS MonthOfYear, 'June' AS MonthName
UNION 
SELECT 7 AS MonthOfYear, 'July' AS MonthName
UNION
SELECT 8 AS MonthOfYear, 'August' AS MonthName
UNION 
SELECT 9 AS MonthOfYear, 'September' AS MonthName
UNION 
SELECT 10 AS MonthOfYear, 'October' AS MonthName
UNION 
SELECT 11 AS MonthOfYear, 'November' AS MonthName
UNION 
SELECT 12 AS MonthOfYear, 'December' AS MonthName

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql import functions as F
-- MAGIC start = "2024-01-01"
-- MAGIC end = "2024-12-31"
-- MAGIC 
-- MAGIC '''
-- MAGIC a = [(start,end)]
-- MAGIC df = spark.createDataFrame(a, ["minDate", "maxDate"])
-- MAGIC from pyspark.sql import functions as F
-- MAGIC '''
-- MAGIC df = spark.createDataFrame([(1,)])
-- MAGIC 
-- MAGIC df = df.withColumn(
-- MAGIC     "date", 
-- MAGIC     F.explode(F.expr("sequence(to_date('2024-01-01'), to_date('2024-12-31'), interval 1 day)"))
-- MAGIC )
-- MAGIC 
-- MAGIC df = df.withColumn("DateKey",  F.date_format("date", "yyyyMMdd"))
-- MAGIC 
-- MAGIC df = df["DateKey","date"]
-- MAGIC 
-- MAGIC df.createOrReplaceTempView("datedim")
-- MAGIC 
-- MAGIC #df.show()
-- MAGIC #Date;Day;DayInWeekNum;DayName;WeekNum;MonthNum;MonthName;Year

-- CELL ********************

CREATE OR REPLACE TEMPORARY VIEW DateFlags AS
SELECT 
      date_format(Date, "yyyyMMdd") AS DateKey,
      timestamp(Date) AS Date,
      Day(Date) AS Day,
      CASE WHEN DayOfWeek(Date) IN (2, 3, 4, 5, 6, 7) THEN DayOfWeek(Date)-1
           WHEN DayOfWeek(Date) = 1 THEN 7 END AS DayInWeekNum, --(Databricks function indexed with Sunday = 1, Monday = 2, ..., Saturday = 7) 
      dnr.DayName AS DayName,
      weekofyear(Date) as WeekNum,
      Month(Date) AS MonthNum,
      mnr.MonthName AS MonthName,
      concat(mnr.MonthName, " ", Year(Date)) AS MonthAndYearName,
      date_format(Date, "yyyyMM") AS YearMonth,
      CONCAT('Q', Quarter(Date)) AS Quarter,
      CONCAT(Year(Date),'Q',Quarter(Date)) AS YearQuarter,
      Year(Date) AS Year,
      datediff(Date, current_date) AS DaysBetween,
      cast(months_between(date_format(Date, 'yyyy-MM'), date_format(current_date, 'yyyy-MM')) as int) AS MonthsBetween,
      case when current_date = Date then "Y" else "N" end AS TodayFlag,
      case when Year(current_date) = Year(Date) AND WeekOfYear(current_date) = WeekOfYear(Date) then "Y" else "N" end AS ThisWeekFlag,
      case when Year(current_date) = Year(Date) AND Month(current_date) = Month(Date) then "Y" else "N" end AS ThisMonthFlag,
      case when Year(current_date) = Year(Date) AND Month(current_date)>1 AND Month(current_date) = Month(Date)+1 then "Y" 
           when Year(current_date) = Year(Date)+1 AND Month(current_date)=1 AND Month(Date)=12 then "Y" 
           else "N" end AS LastMonthFlag,
      case when Year(current_date) = Year(Date) AND Quarter(current_date) = Quarter(Date) then "Y" 
           else "N" end AS ThisQuarterFlag,
      case when Year(current_date) = Year(Date) then "Y" ELSE "N" end AS ThisYearFlag,
      current_timestamp AS CreatedDateTime
FROM datedim

LEFT JOIN DayNameReference dnr ON dnr.DayOfWeek = DayOfWeek(Date)

LEFT JOIN MonthNameReference mnr ON mnr.MonthOfYear = Month(Date)

-- CELL ********************

CREATE OR REPLACE TEMPORARY VIEW DateView 
AS
SELECT * FROM DateFlags

-- CELL ********************

MERGE INTO Silver_Layer.Date USING DateView ON Silver_Layer.Date.DateKey = DateView.DateKey
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- CELL ********************

MERGE INTO Gold_Layer.DimDate USING DateView ON Gold_Layer.DimDate.DateKey = DateView.DateKey
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
