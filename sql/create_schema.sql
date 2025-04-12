CREATE TABLE IF NOT EXISTS stations (
    station_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE,
    city TEXT,
    country TEXT,
    latitude FLOAT,
    longitude FLOAT
);

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

CREATE TABLE IF NOT EXISTS pollutants (
    pollutant_id SERIAL PRIMARY KEY,
    observation_id INTEGER REFERENCES observations(observation_id),
    name TEXT,
    value FLOAT
);