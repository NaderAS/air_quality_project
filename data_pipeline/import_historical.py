import os
import pandas as pd
import psycopg2
from datetime import datetime
from config.db_config import DB_CONFIG

def load_csv_to_db(file_path, city_name):
    df = pd.read_csv(file_path)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Ensure station exists or create it
    cur.execute("""
        INSERT INTO stations (name, city, country, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (name) DO NOTHING
        RETURNING station_id
    """, (city_name, city_name, 'Unknown', 0.0, 0.0))

    station_id = cur.fetchone()
    if not station_id:
        cur.execute("SELECT station_id FROM stations WHERE name = %s", (city_name,))
        station_id = cur.fetchone()
    station_id = station_id[0]

    # Insert each row
    for _, row in df.iterrows():
        try:
            dt = pd.to_datetime(row['date'])

            # Check for duplicates
            cur.execute("""
                SELECT 1 FROM observations
                WHERE station_id = %s AND datetime = %s
            """, (station_id, dt))
            if cur.fetchone():
                continue

            # Insert observation
            cur.execute("""
                INSERT INTO observations (station_id, datetime, source)
                VALUES (%s, %s, %s)
                RETURNING observation_id
            """, (station_id, dt, 'csv'))
            obs_id = cur.fetchone()[0]

            for pol in ['pm25', 'pm10', 'o3', 'no2', 'so2', 'co']:
                if not pd.isna(row.get(pol)):
                    cur.execute("""
                        INSERT INTO pollutants (observation_id, name, value)
                        VALUES (%s, %s, %s)
                    """, (obs_id, pol, row[pol]))

        except Exception as e:
            print(f"⚠️ Failed on {row['date']} in {city_name}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Done importing: {city_name}")

def import_all_historical():
    folder = "data/Air Quality Datasets"
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder, filename)
            city = filename.replace("-air-quality.csv", "").replace(".csv", "").replace("_", " ").strip()
            load_csv_to_db(file_path, city)

if __name__ == "__main__":
    import_all_historical()
