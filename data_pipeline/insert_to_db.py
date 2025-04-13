import psycopg2
from datetime import datetime

def insert_data(conn, data):
    try:
        cur = conn.cursor()
        city_data = data['city']

        # Insert station or get its ID
        cur.execute("""
            INSERT INTO real_time_data.stations (name, city, country, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (name) DO NOTHING
            RETURNING station_id
        """, (
            city_data['name'],
            city_data.get('name', 'Unknown'),
            city_data.get('country', 'Unknown'),
            city_data['geo'][0],
            city_data['geo'][1]
        ))

        station_id = cur.fetchone()
        if not station_id:
            cur.execute("SELECT station_id FROM real_time_data.stations WHERE name = %s", (city_data['name'],))
            station_id = cur.fetchone()
        station_id = station_id[0]

        # Prepare observation data
        obs_time = datetime.strptime(data['time']['s'], "%Y-%m-%d %H:%M:%S")
        aqi = data.get('aqi', None)
        dominant = data.get('dominentpol', None)
        iaqi = data.get('iaqi', {})

        # 🔍 CHECK FOR EXISTING observation BEFORE INSERTING
        cur.execute("""
            SELECT observation_id FROM real_time_data.observations
            WHERE station_id = %s AND datetime = %s
        """, (station_id, obs_time))
        existing = cur.fetchone()

        if existing:
            print(f"⚠️ Observation already exists for station {station_id} at {obs_time}")
            cur.close()
            return

        # INSERT OBSERVATION if not duplicate
        cur.execute("""
            INSERT INTO real_time_data.observations (station_id, datetime, aqi, dominant_pol, source, temperature, humidity, pressure, wind)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING observation_id
        """, (
            station_id, obs_time, aqi, dominant, 'waqi',
            iaqi.get('t', {}).get('v'),
            iaqi.get('h', {}).get('v'),
            iaqi.get('p', {}).get('v'),
            iaqi.get('w', {}).get('v')
        ))

        observation_id = cur.fetchone()[0]

        for pol in ['pm25', 'pm10', 'o3', 'co', 'no2', 'so2']:
            if pol in iaqi:
                cur.execute("""
                    INSERT INTO real_time_data.pollutants (observation_id, name, value)
                    VALUES (%s, %s, %s)
                """, (observation_id, pol, iaqi[pol]['v']))

        conn.commit()
        cur.close()

    except Exception as e:
        conn.rollback()
        print(f"[WAQI Error] {e}")
        raise e