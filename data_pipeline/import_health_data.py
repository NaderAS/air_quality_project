import os
import sys

# Ensure root path for importing config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import psycopg2
from config.db_config import DB_CONFIG

def sanitize_table_name(filename):
    return filename.lower().replace(".csv", "").replace("-", "_").replace(",", "").replace(" ", "_").strip()

def load_health_csv(file_path, raw_table_name):
    df = pd.read_csv(file_path)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    schema = "health_data"
    table_name = f"{schema}.{raw_table_name}"

    # 🔧 Create schema if not exists
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # Drop old table if exists
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Quote and sanitize column names
    columns = ",\n".join(
        f'"{col.lower().replace(" ", "_")}" TEXT'
        for col in df.columns
    )

    # Create the table
    cur.execute(f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            {columns}
        );
    """)

    # Prepare insert column list
    col_names = ", ".join([f'"{col.lower().replace(" ", "_")}"' for col in df.columns])

    # Insert rows
    for _, row in df.iterrows():
        values = [str(row[col]) if not pd.isna(row[col]) else None for col in df.columns]
        placeholders = ", ".join(["%s"] * len(values))
        cur.execute(
            f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
            values
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Imported to table: {table_name}")

def import_healthcare_files():
    folder = "data/Healthcare Datasets"
    for filename in os.listdir(folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder, filename)
            raw_table_name = sanitize_table_name(filename)
            load_health_csv(file_path, raw_table_name)

if __name__ == "__main__":
    import_healthcare_files()
