import sys
import os

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import psycopg2
from config.db_config import DB_CONFIG

# AQI Breakpoints (EPA)
AQI_BREAKPOINTS = {
    'pm25': [(0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
             (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 350.4, 301, 400), (350.5, 500.4, 401, 500)],
    'pm10': [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
             (255, 354, 151, 200), (355, 424, 201, 300), (425, 504, 301, 400), (505, 604, 401, 500)],
    'o3': [(0.0, 0.054, 0, 50), (0.055, 0.070, 51, 100), (0.071, 0.085, 101, 150),
           (0.086, 0.105, 151, 200), (0.106, 0.200, 201, 300)],
    'no2': [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150),
            (361, 649, 151, 200), (650, 1249, 201, 300)],
    'so2': [(0, 35, 0, 50), (36, 75, 51, 100), (76, 185, 101, 150),
            (186, 304, 151, 200), (305, 604, 201, 300)],
    'co': [(0.0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150),
           (12.5, 15.4, 151, 200), (15.5, 30.4, 201, 300)]
}

AQI_CATEGORIES = [
    (0, 50, "Good"),
    (51, 100, "Moderate"),
    (101, 150, "Unhealthy for Sensitive Groups"),
    (151, 200, "Unhealthy"),
    (201, 300, "Very Unhealthy"),
    (301, 500, "Hazardous"),
]

def calculate_aqi(pollutant, value):
    try:
        if value is None or pd.isna(value):
            return None
        value = float(value)
        for bp_low, bp_high, aqi_low, aqi_high in AQI_BREAKPOINTS[pollutant]:
            if bp_low <= value <= bp_high:
                return round((aqi_high - aqi_low) / (bp_high - bp_low) * (value - bp_low) + aqi_low)
    except:
        return None
    return None

def get_aqi_category(aqi_value):
    if aqi_value is None:
        return None
    for low, high, label in AQI_CATEGORIES:
        if low <= aqi_value <= high:
            return label
    return "Out of Range"

def create_table_if_needed(cur):
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS transformations;
        DROP TABLE IF EXISTS transformations.final_city_merged;
        CREATE TABLE transformations.final_city_merged (
            station_id INTEGER,
            datetime TIMESTAMP,
            source TEXT,
            pm25 DOUBLE PRECISION,
            pm10 DOUBLE PRECISION,
            o3 DOUBLE PRECISION,
            no2 DOUBLE PRECISION,
            so2 DOUBLE PRECISION,
            co DOUBLE PRECISION,
            aqi INTEGER,
            aqi_category TEXT
        );
    """)


def merge_city_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    create_table_if_needed(cur)
    
    all_final_rows = []

    for city_id, city_name in [(1, 'beijing'), (2, 'delhi'), (3, 'paris')]:
        print(f"📥 Processing {city_name.capitalize()} (station_id={city_id})...")

        # Live data
        df_live = pd.read_sql(f"""
            SELECT station_id, datetime, source, pm25, pm10, o3, no2, so2, co
            FROM transformations.merged_observations_pollutants
            WHERE station_id = {city_id}
        """, conn)

        # Historical data
        df_hist = pd.read_sql(f"SELECT * FROM historical_data.{city_name}_air_quality", conn)
        df_hist.columns = [col.strip().lower().replace("_", "") for col in df_hist.columns]
        df_hist['datetime'] = pd.to_datetime(df_hist['date'], dayfirst=False, errors='coerce')
        df_hist['station_id'] = city_id
        df_hist['source'] = 'csv'

        for pol in ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']:
            df_hist[pol] = pd.to_numeric(df_hist.get(pol), errors='coerce')

        def row_max_aqi(row):
            values = [calculate_aqi(pol, row[pol]) for pol in ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']]
            values = [v for v in values if v is not None]
            return max(values) if values else None

        for df in [df_live, df_hist]:
            df['aqi'] = df.apply(row_max_aqi, axis=1)
            df['aqi_category'] = df['aqi'].apply(get_aqi_category)

        df_live = df_live[['station_id', 'datetime', 'source', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'aqi', 'aqi_category']]
        df_hist = df_hist[['station_id', 'datetime', 'source', 'pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'aqi', 'aqi_category']]

        all_final_rows.append(pd.concat([df_live, df_hist], ignore_index=True))

    df_final = pd.concat(all_final_rows, ignore_index=True)

    # ✅ Sort the data by datetime
    df_final = (
    pd.concat(all_final_rows, ignore_index=True)
    .sort_values(by=['station_id', 'datetime'], ascending=[True, False])
)

    for _, row in df_final.iterrows():
        if pd.isna(row['datetime']):
            continue
        cur.execute("""
            INSERT INTO transformations.final_city_merged (
                station_id, datetime, source, pm25, pm10, o3, no2, so2, co, aqi, aqi_category
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['station_id'],
            row['datetime'],
            row['source'],
            float(row['pm25']) if pd.notna(row['pm25']) else None,
            float(row['pm10']) if pd.notna(row['pm10']) else None,
            float(row['o3']) if pd.notna(row['o3']) else None,
            float(row['no2']) if pd.notna(row['no2']) else None,
            float(row['so2']) if pd.notna(row['so2']) else None,
            float(row['co']) if pd.notna(row['co']) else None,
            int(row['aqi']) if pd.notna(row['aqi']) else None,
            row['aqi_category']
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Final table created: transformations.final_city_merged (sorted by datetime)")

if __name__ == "__main__":
    merge_city_data()