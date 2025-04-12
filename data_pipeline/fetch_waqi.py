import requests

def fetch_waqi_data(city, API_TOKEN):
    url = f"https://api.waqi.info/feed/{city}/?token={API_TOKEN}"
    res = requests.get(url).json()
    if res['status'] != 'ok':
        raise Exception(f"WAQI fetch failed for {city}")
    return res['data']
