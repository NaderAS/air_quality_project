import sys
import os

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import psycopg2
from config.db_config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# Step 1: Load and aggregate AQI by station_id and year
query_aqi = """
    SELECT station_id, EXTRACT(YEAR FROM datetime)::int AS year, AVG(aqi)::float AS avg_aqi
    FROM transformations.final_city_merged
    GROUP BY station_id, year
"""
aqi_df = pd.read_sql(query_aqi, conn)

# Step 2: Load burden datasets
china_df = pd.read_sql("SELECT * FROM burden_data.china_dataset", conn)
france_df = pd.read_sql("SELECT * FROM burden_data.france_dataset", conn)
india_df = pd.read_sql("SELECT * FROM burden_data.india_dataset", conn)

# Step 3: Add station_id and ensure year is int
china_df["station_id"] = 3
france_df["station_id"] = 5
india_df["station_id"] = 4

for df in [china_df, france_df, india_df]:
    df["year"] = df["year"].astype(int)

# Step 4: Combine burden datasets
burden_df = pd.concat([china_df, france_df, india_df], ignore_index=True)

# Step 5: Merge burden data with AQI
merged_df = pd.merge(
    burden_df,
    aqi_df,
    on=["station_id", "year"],
    how="inner"
)

# Step 6: Drop old table if it exists
cur.execute("DROP TABLE IF EXISTS transformations.final_city_burden_merged")
conn.commit()

# Step 7: Create new merged table
cur.execute("""
    CREATE TABLE transformations.final_city_burden_merged (
        id SERIAL PRIMARY KEY,
        station_id INTEGER,
        year INTEGER,
        country TEXT,
        ghe_cause TEXT,
        mean_value DOUBLE PRECISION,
        mean_lower_value DOUBLE PRECISION,
        mean_upper_value DOUBLE PRECISION,
        age_standardized_rate DOUBLE PRECISION,
        age_standardized_rate_lower DOUBLE PRECISION,
        age_standardized_rate_upper DOUBLE PRECISION,
        avg_aqi DOUBLE PRECISION
    )
""")
conn.commit()

# Step 8: Insert merged data
for _, row in merged_df.iterrows():
    cur.execute("""
        INSERT INTO transformations.final_city_burden_merged (
            station_id, year, country, ghe_cause,
            mean_value, mean_lower_value, mean_upper_value,
            age_standardized_rate, age_standardized_rate_lower,
            age_standardized_rate_upper, avg_aqi
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row["station_id"],
        row["year"],
        row["country__territory__area"],
        row["ghe_cause"],
        float(row["mean_value"]),
        float(row["mean_lower_value"]),
        float(row["mean_upper_value"]),
        float(row["age_standardized_rate"]),
        float(row["age_standardized_rate_lower_value"]),
        float(row["age_standardized_rate_upper_value"]),
        float(row["avg_aqi"])
    ))

conn.commit()
cur.close()
conn.close()

print("✅ Done! Table 'final_city_burden_merged' has been created and populated.")