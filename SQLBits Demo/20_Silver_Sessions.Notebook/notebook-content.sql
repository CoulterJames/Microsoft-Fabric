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

CREATE OR REPLACE TEMPORARY VIEW vw_TitlePresenter AS
SELECT DISTINCT 
        DENSE_RANK() OVER (ORDER BY Title) AS TitleId,
        DENSE_RANK() OVER (ORDER BY Presenter) AS PresenterId
FROM Bronze_Layer.sqlbits
LATERAL VIEW EXPLODE(SPLIT(Presenters,';')) exploded_table AS Presenter


-- CELL ********************

CREATE OR REPLACE TEMPORARY VIEW vw_TitleTopic AS
SELECT DISTINCT 
        DENSE_RANK() OVER (ORDER BY Title) AS TitleId,
        DENSE_RANK() OVER (ORDER BY Topic) AS TopicId
FROM Bronze_Layer.sqlbits
LATERAL VIEW EXPLODE(SPLIT(Topics,';')) exploded_table AS Topic

-- CELL ********************

CREATE OR REPLACE TEMPORARY VIEW vw_Sessions AS
SELECT DISTINCT
    INT(DATE_FORMAT(TO_TIMESTAMP(Date,"dd/MM/yyyy"),"yyyyMMdd")) AS DateKey,
    LEFT(RIGHT(STRING(StartTime),8),5) AS StartTime,
    LEFT(RIGHT(STRING(EndTime),8),5) AS EndTime,
    (BIGINT(EndTime)-BIGINT(StartTime))/60 AS DurationInMinutes,
    DENSE_RANK() OVER (ORDER BY Title) AS TitleId,
    DENSE_RANK() OVER (ORDER BY Theme) AS ThemeId
FROM Bronze_Layer.sqlbits
        

-- CELL ********************

MERGE INTO Silver_Layer.Sessions 
USING vw_Sessions ON Silver_Layer.Sessions.TitleId = vw_Sessions.TitleId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- CELL ********************

MERGE INTO Silver_Layer.TitleTopic 
USING vw_TitleTopic 
ON Silver_Layer.TitleTopic.TitleId = vw_TitleTopic.TitleId
AND Silver_Layer.TitleTopic.TopicId = vw_TitleTopic.TopicId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- CELL ********************

MERGE INTO Silver_Layer.TitlePresenter 
USING vw_TitlePresenter 
ON Silver_Layer.TitlePresenter.TitleId = vw_TitlePresenter.TitleId
AND Silver_Layer.TitlePresenter.PresenterId = vw_TitlePresenter.PresenterId
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
