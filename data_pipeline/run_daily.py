import sys
import os

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.db_config import DB_CONFIG, API_TOKEN, CITIES
from data_pipeline.fetch_waqi import fetch_waqi_data
from data_pipeline.insert_to_db import insert_data
from data_pipeline.clean_data import clean_observations, clean_pollutants
from data_pipeline.deduplicate import remove_observation_duplicates


import psycopg2
import pandas as pd
from datetime import datetime

def create_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_id SERIAL PRIMARY KEY,
            name TEXT UNIQUE,
            city TEXT,
            country TEXT,
            latitude FLOAT,
            longitude FLOAT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            observation_id SERIAL PRIMARY KEY,
            station_id INTEGER REFERENCES stations(station_id),
            datetime TIMESTAMP,
            aqi INTEGER,
            dominant_pol TEXT,
            source TEXT,
            temperature FLOAT,
            humidity FLOAT,
            pressure FLOAT,
            wind FLOAT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pollutants (
            pollutant_id SERIAL PRIMARY KEY,
            observation_id INTEGER REFERENCES observations(observation_id),
            name TEXT,
            value FLOAT
        );
    """)

    conn.commit()
    cur.close()
    print("✅ Tables checked/created.")

# Create logs folder if not exists
os.makedirs("logs", exist_ok=True)
log_path = f"logs/waqi_log_{datetime.now().strftime('%Y%m%d')}.txt"

with open(log_path, "w") as log_file:
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Create necessary tables if not already present
    create_tables(conn)
    
    for city in CITIES:
        try:
            data = fetch_waqi_data(city, API_TOKEN)
            insert_data(conn, data)
            log_file.write(f"{datetime.now()} - SUCCESS: WAQI - {city}\\n")
        except Exception as e:
            log_file.write(f"{datetime.now()} - ERROR: WAQI - {city} - {e}\\n")
    
    # Deduplicate before exporting
    remove_observation_duplicates()

    # Export for Power BI
    try:
        df_obs = pd.read_sql_query("SELECT * FROM observations", conn)
        df_pol = pd.read_sql_query("SELECT * FROM pollutants", conn)
        df_obs_clean = clean_observations(df_obs)
        df_pol_clean = clean_pollutants(df_pol)
        df = df_obs_clean.merge(df_pol_clean, on='observation_id')
        os.makedirs("powerbi", exist_ok=True)
        df.to_excel("powerbi/waqi_data.xlsx", index=False)
    except Exception as e:
        log_file.write(f"{datetime.now()} - ERROR: Exporting data - {e}\\n")
    
    conn.close()