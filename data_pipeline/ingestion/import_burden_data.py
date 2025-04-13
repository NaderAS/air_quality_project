import os
import sys
import re

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import psycopg2
from config.db_config import DB_CONFIG

def sanitize_table_name(filename):
    return filename.lower().replace(".xlsx", "").replace(" ", "_").replace("-", "_").replace(",", "").strip()

def sanitize_column_name(col):
    return (
        col.strip()
        .lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("&", "and")
        .replace("[", "")
        .replace("]", "")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "_")
    )

def clean_value(val):
    if pd.isna(val):
        return None
    return re.sub(r"\s+", " ", str(val).replace('\xa0', ' ')).strip()

def load_excel_to_table(file_path, raw_table_name):
    print(f"📄 Loading: {file_path}")

    # Load all values as strings
    df = pd.read_excel(file_path, dtype=str)
    df.dropna(axis=0, how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)

    cleaned_columns = [sanitize_column_name(col) for col in df.columns]
    df.columns = cleaned_columns

    if len(df.columns) == 0 or df.shape[0] == 0:
        print(f"⚠️ Skipped empty file: {file_path}")
        return

    schema = "burden_data"
    table_name = f"{schema}.{raw_table_name}"

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Create schema and drop table if exists
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Create table
    column_definitions = ",\n".join([f'"{col}" TEXT' for col in cleaned_columns])
    cur.execute(f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            {column_definitions}
        );
    """)

    col_names = ", ".join([f'"{col}"' for col in cleaned_columns])
    placeholders = ", ".join(["%s"] * len(cleaned_columns))
    insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

    inserted = 0
    expected_len = len(cleaned_columns)

    for i, row in df.iterrows():
        values = [clean_value(v) for v in row.tolist()]

        if len(values) != expected_len:
            print(f"❌ Skipped row {i}: expected {expected_len} values, got {len(values)}")
            continue

        if all(v is None or v == '' for v in values):
            continue

        try:
            cur.execute(insert_sql, values)
            inserted += 1
        except Exception as e:
            print(f"❌ Insert error at row {i}: {e}")
            print(f"   → {values}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Done: {table_name} ({inserted} rows inserted)")

def import_burden_data():
    folder = "data/Cleaned Burden Datasets"
    for filename in os.listdir(folder):
        if filename.endswith(".xlsx"):
            path = os.path.join(folder, filename)
            table_name = sanitize_table_name(filename)
            load_excel_to_table(path, table_name)

if __name__ == "__main__":
    import_burden_data()
