import psycopg2
from config.db_config import DB_CONFIG

def remove_observation_duplicates():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("🔍 Finding duplicate observations...")

        # Step 1: Find duplicate observation_ids (keep the latest)
        cur.execute("""
            SELECT a.observation_id
            FROM real_time_data.observations a
            JOIN real_time_data.observations b
              ON a.station_id = b.station_id AND a.datetime = b.datetime
            WHERE a.observation_id < b.observation_id
        """)
        duplicates = cur.fetchall()
        duplicate_ids = [row[0] for row in duplicates]

        if not duplicate_ids:
            print("✅ No duplicate observations found.")
        else:
            print(f"🧹 Removing {len(duplicate_ids)} duplicate observations...")

            # Step 2: Delete related pollutants first
            cur.execute("""
                DELETE FROM real_time_data.pollutants
                WHERE observation_id = ANY(%s)
            """, (duplicate_ids,))

            # Step 3: Delete duplicate observations
            cur.execute("""
                DELETE FROM real_time_data.observations
                WHERE observation_id = ANY(%s)
            """, (duplicate_ids,))

            conn.commit()
            print("✅ Duplicates removed successfully.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error during deduplication: {e}")

# Run it
remove_observation_duplicates()
