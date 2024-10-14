# Data Warehousing Lab 1
# Gayatri Patil and Shweta Shinde

## Project Overview

This project is part of the Data Warehousing course at San Jose State University. The objective of Lab 1 is to demonstrate the process of data loading, forecasting, and data transformation using Apache Airflow and Snowflake. The lab focuses on establishing a data pipeline that connects a source database, processes the data, and loads it into a Snowflake data warehouse for analysis.

## Problem Statement

In the context of this lab, the primary challenge is to build a robust data pipeline that effectively handles data extraction, transformation, and loading (ETL) from a source API into Snowflake. Additionally, we aim to perform time series forecasting on stock prices and enhance our dataset by creating lag and difference columns to better analyze trends.

## Procedures Followed

1. **Environment Setup**
   - Installed necessary libraries and set up the development environment for Apache Airflow and Snowflake.

2. **Data Extraction**
   - Connected to the source API(https://www.alphavantage.co/documentation/) and extracted the relevant datasets required for analysis.
   - For this Lab 1 we are using Starbucks(SBUX) and New York Times(NYT) stock data.

3. **Data Transformation**
   - Implemented data cleaning and transformation processes to prepare the data.

4. **Data Loading**
   - Loaded the transformed data into the Snowflake main table using Apache Airflow DAGs using incremental load.
   - 1st load data in staging table DEV.STOCK.STOCK_PRICE_STAGE and then in main table DEV.STOCK.STOCK_PRICE_ANALYSIS

5. **Forecasting**
   - Utilized statistical models to forecast stock prices based on historical data.
   - Evaluated the accuracy of the forecasts to ensure reliability.
   - Forcast table DEV.STOCK.STOCK_PRICE_FORECAST
     
6. **Volumn Trend**
   - Created new table DEV.STOCK.STOCK_VOLUMN_POINTS with lag and difference columns to facilitate time series analysis.
   - Calculate Correlation of the stock using LAG windows function.

7. **Results**
   - Summarized the results of the forecasting model and provided insights based on the analysis.

8. **Data Visulization**
   - Create plots for better understanding of data and stock trends.
  
## Python Files Overview

### 1. `stock_pipeline_ETL.py`
- **Purpose**: Extracts stock data from an external API and loads it into Snowflake.
- **Key Functions**:
  - `return_last_90d_price()`: Fetches historical and current stock price data.
  - `create_load_incremental()`: Loads the extracted data into a Snowflake staging table and then main table(incremental load).

### 2. `stock_pipeline_MLandAG.py`
- **Purpose**: Applies ARIMA forecasting on the stock data.
- **Key Functions**:
  - `train_arima_model()`: Trains an ARIMA model on historical stock data.
  - `forecast(steps=10)`: Generates 10-day forecasts for stock prices and stores the output in Snowflake.

### 3. `Stock_DataVisuals.ipynb`
- **Purpose**: To plot graphs using the data loading in the tables.
- **Plots**:
  - **Bar Gragh**: Shows us the Volume-Price Change Correlation by Symbol.
  - **Scatter Plot**: Shows us the Volume Change Scatter Plot (by Symbol and Date).

### 4. `*.csv`
-  Several CSV files serve as table extracts, providing the data needed for this project. These files are used to simulate real-world tables and help in understanding how data flows through the pipeline:
  
## Conclusion

The lab successfully demonstrated the end-to-end process of building a data pipeline, from data extraction to loading into a Snowflake data warehouse. Building a ML forcasting model, new table for undersang volumn difference using LAG. The plots implemention gives a clear picture of correlation of the stocks and how the trent changes.
This experience enhanced our understanding of data warehousing concepts and practical application in real-world scenarios.

## Acknowledgments

- [Apache Airflow](https://airflow.apache.org/) for orchestration.
- [Snowflake](https://www.snowflake.com/) for data warehousing capabilities.
