import sys
import os
from datetime import datetime
import pandas as pd
import psycopg2

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.db_config import DB_CONFIG, API_TOKEN, CITIES
from data_pipeline.ingestion.fetch_waqi import fetch_waqi_data
from data_pipeline.insert_to_db import insert_data
from data_pipeline.output.clean_export_data import clean_observations, clean_pollutants
from data_pipeline.cleaning.remove_duplicates import remove_observation_duplicates

def run_daily():
    os.makedirs("logs", exist_ok=True)
    log_path = f"logs/waqi_log_{datetime.now().strftime('%Y%m%d')}.txt"

    with open(log_path, "w") as log_file:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            create_tables(conn)

            for city in CITIES:
                try:
                    data = fetch_waqi_data(city, API_TOKEN)
                    insert_data(conn, data)
                    log_file.write(f"{datetime.now()} - SUCCESS: WAQI - {city}\n")
                except Exception as e:
                    log_file.write(f"{datetime.now()} - ERROR: WAQI - {city} - {e}\n")

            # Deduplicate
            # Deduplicate
            try:
                remove_observation_duplicates()
            except psycopg2.errors.UndefinedTable:
                log_file.write(f"{datetime.now()} - SKIPPED: Deduplication (table doesn't exist yet)\n")
            except Exception as e:
                log_file.write(f"{datetime.now()} - ERROR: Deduplication - {e}\n")


            conn.close()

        except Exception as conn_err:
            log_file.write(f"{datetime.now()} - ERROR: DB connection failed - {conn_err}\n")

def create_tables(conn):
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS real_time_data;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS real_time_data.stations (
            station_id SERIAL PRIMARY KEY,
            name TEXT UNIQUE,
            city TEXT,
            country TEXT,
            latitude FLOAT,
            longitude FLOAT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS real_time_data.observations (
            observation_id SERIAL PRIMARY KEY,
            station_id INTEGER REFERENCES real_time_data.stations(station_id),
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
        CREATE TABLE IF NOT EXISTS real_time_data.pollutants (
            pollutant_id SERIAL PRIMARY KEY,
            observation_id INTEGER REFERENCES real_time_data.observations(observation_id),
            name TEXT,
            value FLOAT
        );
    """)

    conn.commit()
    cur.close()
    print("✅ Tables checked/created in schema real_time_data.")

if __name__ == "__main__":
    run_daily()
