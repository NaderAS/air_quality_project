import pandas as pd

def clean_observations(df_obs):
    # Optional: sort by time to improve interpolation accuracy
    df_obs = df_obs.sort_values(by=['station_id', 'datetime'])

    # Interpolate AQI per station
    df_obs['aqi'] = df_obs.groupby('station_id')['aqi'].transform(
        lambda x: x.interpolate(method='linear', limit_direction='both')
    )

    # Interpolate weather columns per station
    for col in ['temperature', 'humidity', 'pressure', 'wind']:
        if col in df_obs.columns:
            df_obs[col] = df_obs.groupby('station_id')[col].transform(
                lambda x: x.interpolate(method='linear', limit_direction='both')
            )

    return df_obs

def clean_pollutants(df_pol):
    df_pol = df_pol.dropna(subset=['value'])
    df_pol['value'] = df_pol['value'].clip(lower=0, upper=1000)
    return df_pol
