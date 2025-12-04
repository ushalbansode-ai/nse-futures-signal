import os
import zipfile
import requests
import datetime
import pandas as pd

RAW_DIR = "data/raw/"

NSE_URL = "https://www.nseindia.com/api/reports?archive=true&date={DATE}&type=derivatives&mode=FO"


def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)


def fetch_bhavcopy(date=None):
    ensure_dirs()

    if date is None:
        date = datetime.datetime.now().strftime("%d-%m-%Y")

    print(f"[INFO] Downloading bhavcopy for {date}")

    url = NSE_URL.replace("{DATE}", date)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com",
    }

    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print("Download failed:", r.status_code)
        return None

    zip_path = RAW_DIR + f"bhav_{date}.zip"
    with open(zip_path, "wb") as f:
        f.write(r.content)

    print("[INFO] ZIP saved:", zip_path)

    # Extract CSV
    with zipfile.ZipFile(zip_path, "r") as z:
        csv_name = z.namelist()[0]
        z.extract(csv_name, RAW_DIR)

    extracted_csv = RAW_DIR + csv_name
    print("[INFO] Extracted CSV:", extracted_csv)

    return extracted_csv
  
