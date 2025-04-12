import sys
import os

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2
import pandas as pd
from config.db_config import DB_CONFIG

def calculate_pm25_aqi(pm25):
    try:
        pm25 = float(pm25)
        if pm25 <= 12:
            return round((50 / 12) * pm25)
        elif pm25 <= 35.4:
            return round(((100 - 51) / (35.4 - 12.1)) * (pm25 - 12.1) + 51)
        elif pm25 <= 55.4:
            return round(((150 - 101) / (55.4 - 35.5)) * (pm25 - 35.5) + 101)
        elif pm25 <= 150.4:
            return round(((200 - 151) / (150.4 - 55.5)) * (pm25 - 55.5) + 151)
        elif pm25 <= 250.4:
            return round(((300 - 201) / (250.4 - 150.5)) * (pm25 - 150.5) + 201)
        else:
            return 301
    except:
        return None

def create_final_table():
    query_schema = "CREATE SCHEMA IF NOT EXISTS transformations;"
    query_table = """
    CREATE TABLE IF NOT EXISTS transformations.final_beijing_merged (
        station_id INTEGER,
        datetime TIMESTAMP,
        source TEXT,
        pm25 DOUBLE PRECISION,
        pm10 DOUBLE PRECISION,
        o3 DOUBLE PRECISION,
        no2 DOUBLE PRECISION,
        so2 DOUBLE PRECISION,
        co DOUBLE PRECISION,
        aqi INTEGER
    );
    """

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(query_schema)
    cur.execute(query_table)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Created transformations.final_beijing_merged")

def merge_and_insert():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Load both tables into pandas
    query_live = """
    SELECT station_id, datetime, source,
           pm25, pm10, o3, no2, so2, co
    FROM transformations.merged_observations_pollutants
    WHERE station_id = 3;
    """
    query_hist = "SELECT * FROM csv_data.beijing_air_quality;"

    df_live = pd.read_sql(query_live, conn)
    df_hist = pd.read_sql(query_hist, conn)\
    
    # Clean column names
    df_hist.columns = [col.replace("_", "").strip().lower() for col in df_hist.columns]

    import numpy as np

    # Convert empty strings and non-numeric entries to NaN
    df_hist = df_hist.replace(r'^\s*$', np.nan, regex=True)

# Convert all relevant columns to numeric (force non-numeric to NaN)
    cols_to_convert = ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']
    for col in cols_to_convert:
        df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce')

    # Clean and align historical data
    df_hist.columns = [col.strip().lower().replace(' ', 'NaN') for col in df_hist.columns]
    df_hist['datetime'] = pd.to_datetime(df_hist['date'], format="%d/%m/%Y")
    df_hist['station_id'] = 3
    df_hist['source'] = 'csv'

    df_hist['aqi'] = df_hist['pm25'].apply(calculate_pm25_aqi)

    df_hist = df_hist[['station_id', 'datetime', 'source', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'aqi']]

    df_live['aqi'] = df_live['pm25'].apply(calculate_pm25_aqi)

    df_live = df_live[['station_id', 'datetime', 'source', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'aqi']]

    # Merge the tables
    df_merged = pd.concat([df_live, df_hist], ignore_index=True)

    # Insert into final table
    for _, row in df_merged.iterrows():
        cur.execute("""
            INSERT INTO transformations.final_beijing_merged (
                station_id, datetime, source, pm25, pm10, o3, no2, so2, co, aqi
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Merged and inserted data into transformations.final_beijing_merged")

if __name__ == "__main__":
    create_final_table()
    merge_and_insert()
