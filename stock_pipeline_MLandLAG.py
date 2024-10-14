#Step 1 Import all required modules
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import numpy as np


#Connect to snowflake account

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()



@task
# Suppress warnings from ARIMA model
#warnings.filterwarnings("ignore")

def train_arima_model(train_input_table,forecast_table):
    conn = return_snowflake_conn()
    try:
        # Fetch data from Snowflake
        conn.execute(f"SELECT SYMBOL, DATE, CLOSE FROM {train_input_table} ORDER BY SYMBOL, DATE;")
        data = conn.fetchall()

        # Create DataFrame from fetched data
        df = pd.DataFrame(data, columns=['SYMBOL', 'DATE', 'CLOSE'])
        df['DATE'] = pd.to_datetime(df['DATE'])
        df['CLOSE'] = pd.to_numeric(df['CLOSE'], errors='coerce')

        results = []

        # Loop over each symbol and fit a separate ARIMA model
        for symbol, symbol_data in df.groupby('SYMBOL'):
            symbol_data.set_index('DATE', inplace=True)

            # Fit ARIMA model
            model = ARIMA(symbol_data['CLOSE'], order=(5, 1, 0))  # Example ARIMA parameters
            model_fit = model.fit()

            # Forecast the next 10 days
            forecast = model_fit.forecast(steps=10)

            # Create a DataFrame for the forecasted values with SYMBOL and DATE
            forecast_dates = pd.date_range(start=symbol_data.index[-1] + timedelta(1), periods=10)
            forecast_df = pd.DataFrame({
                'SYMBOL': symbol,
                'DATE': forecast_dates,
                'FORECAST_CLOSE': forecast
            })

            # Append result
            results.append(forecast_df)

        # Concatenate all forecast DataFrames
        result_df = pd.concat(results)

        # Create the destination table if it doesn't exist
        create_table_query = f"""
        CREATE OR REPLACE TABLE {forecast_table} (
            SYMBOL VARCHAR,
            DATE DATE,
            FORECAST_CLOSE FLOAT
        );
        """
        conn.execute(create_table_query)

        # Insert the forecast data into the Snowflake table
        for index, row in result_df.iterrows():
            insert_query = f"""
            INSERT INTO {forecast_table} (SYMBOL, DATE, FORECAST_CLOSE)
            VALUES ('{row['SYMBOL']}', '{row['DATE'].date()}', {row['FORECAST_CLOSE']});
            """
            conn.execute(insert_query)

    except Exception as e:
        print(e)
        raise e

@task
def create_volumn_points(volumn_point,main_table):
    conn = return_snowflake_conn()
    try:
        conn.execute(f"""
                       CREATE OR REPLACE TABLE {volumn_point} AS
                        SELECT 
                        DATE, 
                        VOLUME, 
                        LAG(VOLUME, 1) OVER (PARTITION BY SYMBOL ORDER BY DATE) AS PREV_VOLUME,
                        ROUND(((VOLUME - LAG(VOLUME, 1) OVER (PARTITION BY SYMBOL ORDER BY DATE)) / 
                        LAG(VOLUME, 1) OVER (PARTITION BY SYMBOL ORDER BY DATE)) * 100,3) AS VOLUME_CHANGE_PCT,
                        SYMBOL
                        FROM 
                        {main_table}
                        WHERE 
                        VOLUME IS NOT NULL
                         ORDER BY SYMBOL, DATE DESC;
                     """)

        conn.execute("COMMIT;")  
        print(f"Target table create '{volumn_point}', Data loaded successfully in both the tables using full load ")
    except Exception as e:
        conn.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'Pipeline_ML_and_LAG',
    start_date = datetime(2024,10,11),
    catchup=False,
    tags=['ETL'],
    schedule = None
) as dag2:
    
 [train_arima_model("dev.stock.stock_price_analysis","DEV.STOCK.STOCK_PRICE_FORECAST"),create_volumn_points("dev.stock.stock_volumn_points","dev.stock.stock_price_analysis")]
    
    

