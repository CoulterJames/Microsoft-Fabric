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

CREATE OR REPLACE TEMPORARY VIEW vw_Theme AS
SELECT DISTINCT 
        DENSE_RANK() OVER (ORDER BY Theme) AS ThemeId,
        Theme
FROM Bronze_Layer.sqlbits


-- CELL ********************

MERGE INTO Silver_Layer.Theme 
USING vw_Theme ON Silver_Layer.Theme.Theme = vw_Theme.Theme
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
