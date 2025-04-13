import sys
import os

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import psycopg2
import pandas as pd
from config.db_config import DB_CONFIG

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def create_schema_and_table():
    create_schema_query = "CREATE SCHEMA IF NOT EXISTS transformations;"

    create_table_query = """
    CREATE TABLE IF NOT EXISTS transformations.merged_observations_pollutants (
        observation_id INTEGER PRIMARY KEY,
        station_id INTEGER,
        datetime TIMESTAMP,
        source TEXT,
        temperature DOUBLE PRECISION,
        humidity DOUBLE PRECISION,
        pressure DOUBLE PRECISION,
        wind DOUBLE PRECISION,
        pm25 DOUBLE PRECISION,
        pm10 DOUBLE PRECISION,
        o3 DOUBLE PRECISION,
        no2 DOUBLE PRECISION,
        so2 DOUBLE PRECISION,
        co DOUBLE PRECISION
    );
    """

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(create_schema_query)
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Schema and table created (if not exist).")

def merge_public_sources():
    conn = psycopg2.connect(**DB_CONFIG)

    # Query to get merged and pivoted data
    query = """
    SELECT
        o.observation_id,
        o.station_id,
        o.datetime,
        o.source,
        o.temperature,
        o.humidity,
        o.pressure,
        o.wind,
        MAX(CASE WHEN p.name = 'pm25' THEN p.value END) AS pm25,
        MAX(CASE WHEN p.name = 'pm10' THEN p.value END) AS pm10,
        MAX(CASE WHEN p.name = 'o3' THEN p.value END) AS o3,
        MAX(CASE WHEN p.name = 'no2' THEN p.value END) AS no2,
        MAX(CASE WHEN p.name = 'so2' THEN p.value END) AS so2,
        MAX(CASE WHEN p.name = 'co' THEN p.value END) AS co
    FROM
        public.observations o
    JOIN
        public.pollutants p
    ON
        o.observation_id = p.observation_id
    GROUP BY
        o.observation_id, o.station_id, o.datetime, o.source, o.temperature, o.humidity, o.pressure, o.wind
    ORDER BY o.observation_id;
    """

    df = pd.read_sql_query(query, conn)

    # Insert into the new table
    cur = conn.cursor()
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO transformations.merged_observations_pollutants (
                observation_id, station_id, datetime, source, temperature,
                humidity, pressure, wind, pm25, pm10, o3, no2, so2, co
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (observation_id) DO NOTHING;
        """, tuple(row))
    
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data merged and inserted into transformations.merged_observations_pollutants")

if __name__ == "__main__":
    create_schema_and_table()
    merge_public_sources()
