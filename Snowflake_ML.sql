-- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role ACCOUNTADMIN;
use warehouse COMPUTE_WH;
use database DEV;
use schema STOCK;

-- Inspect the first 10 rows of your training data. This is the data we'll use to create your model.
select * from STOCK_PRICE_ANALYSIS limit 10;

-- Prepare your training data. Timestamp_ntz is a required format. Also, only include select columns.
CREATE VIEW STOCK_PRICE_ANALYSIS_v1 AS SELECT
    to_timestamp_ntz(DATE) as DATE_v1,
    CLOSE,
    SYMBOL
FROM STOCK_PRICE_ANALYSIS;

-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE or REPLACE SNOWFLAKE.ML.FORECAST stock_forecast(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'STOCK_PRICE_ANALYSIS_v1'),
    SERIES_COLNAME => 'SYMBOL',
    TIMESTAMP_COLNAME => 'DATE_v1',
    TARGET_COLNAME => 'CLOSE',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Generate predictions and store the results to a table.
BEGIN
    -- This is the step that creates your predictions.
    CALL stock_forecast!FORECAST(
        FORECASTING_PERIODS => 14,
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE TABLE My_stock_forecasts_2024_10_14 AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View your predictions.
SELECT * FROM My_stock_forecasts_2024_10_14;

-- Union your predictions with your historical data, then view the results in a chart.
SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM STOCK_PRICE_ANALYSIS
UNION ALL
SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
    FROM My_stock_forecasts_2024_10_14;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of your model. 
CALL stock_forecast!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of your features, including auto-generated features. 
CALL stock_forecast!EXPLAIN_FEATURE_IMPORTANCE();
