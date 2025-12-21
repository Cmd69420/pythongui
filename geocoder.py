import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

GOOGLE_API_KEY = "AIzaSyCwGkrq4Onpvj9Yu5His9row-fIg5v6N0I"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def geocode_address(address: str):
    params = {
        "address": address,
        "key": GOOGLE_API_KEY
    }
    r = requests.get(GEOCODE_URL, params=params, timeout=10)
    data = r.json()

    if data["status"] == "OK":
        loc = data["results"][0]["geometry"]["location"]
        return loc["lat"], loc["lng"]

    return None, None


def geocode_dataframe(df: pd.DataFrame, address_col="address", max_workers=8):
    df["latitude"] = None
    df["longitude"] = None

    tasks = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, row in df.iterrows():
            addr = str(row[address_col]).strip()
            if not addr or addr.lower() == "nan":
                continue

            tasks[executor.submit(geocode_address, addr)] = idx

        for future in as_completed(tasks):
            idx = tasks[future]
            try:
                lat, lng = future.result()
                df.at[idx, "latitude"] = lat
                df.at[idx, "longitude"] = lng
            except:
                pass

    return df
