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

CREATE OR REPLACE TABLE Silver_Layer.Theme 
(
    ThemeId int,
    Theme string
) USING DELTA


-- CELL ********************

CREATE OR REPLACE TABLE Silver_Layer.Topic 
(
    TopicId int,
    Topic string
) USING DELTA

-- CELL ********************

CREATE OR REPLACE TABLE Silver_Layer.Presenter 
(
    PresenterId int,
    PresenterName string
) USING DELTA

-- CELL ********************

CREATE TABLE IF NOT EXISTS Silver_Layer.Title 
(
    TitleId int,
    Title string
) USING DELTA

-- CELL ********************

CREATE TABLE IF NOT EXISTS Silver_Layer.Date 
(
    DateKey int,
    Date timestamp,
    Day int,
    DayInWeekNum int,
    DayName string,
    WeekNum int,
    MonthNum int,
    MonthName string,
    MonthAndYearName string,
    YearMonth string,
    Quarter string,
    YearQuarter string,
    Year int,
    DaysBetween string,
    MonthsBetween string,
    TodayFlag string,
    ThisWeekFlag string,
    ThisMonthFlag string,
    LastMonthFlag string,
    ThisQuarterFlag string,
    ThisYearFlag string
) USING DELTA

-- CELL ********************

CREATE TABLE IF NOT EXISTS Silver_Layer.Time 
(
    TimeKey int,
    Time string,
    Hour int,
    Minute int,
    AmOrPm string
) USING DELTA

-- CELL ********************

CREATE OR REPLACE TABLE Silver_Layer.TitleTopic
(
    TitleId int,
    TopicId int
) USING DELTA

-- CELL ********************

CREATE OR REPLACE TABLE Silver_Layer.TitlePresenter
(
    TitleId int,
    PresenterId int
) USING DELTA

-- CELL ********************

CREATE OR REPLACE TABLE Silver_Layer.Sessions
(
    DateKey int,
    StartTime string,
    EndTime string,
    DurationInMinutes int,
    TitleId int,
    ThemeId int
) USING DELTA
