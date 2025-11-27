# src/fetch_bhavcopy.py

import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta

HEADERS = {"User-Agent": "Mozilla/5.0"}

BASE = "https://archives.nseindia.com/content/fo"

def fetch_bhavcopy(max_days_back=10):
    today = datetime.today()
    tried = []

    for i in range(max_days_back):
        d = today - timedelta(days=i)
        if d.weekday() >= 5:
            continue

        date_str = d.strftime("%Y%m%d")   # 20251125
        filename = f"BhavCopy_NSE_FO_0_0_0_{date_str}_F_0000.csv.zip"
        url = f"{BASE}/{filename}"

        tried.append(url)
        print("Trying:", url)

        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200 and len(r.content) > 2000:
                print("Downloaded:", url)

                z = zipfile.ZipFile(io.BytesIO(r.content))
                csv_name = z.namelist()[0]
                print("Extracting:", csv_name)

                df = pd.read_csv(z.open(csv_name))
                print("Rows:", len(df))
                return df

        except Exception as e:
            print("Error:", e)

    print("\nTried URLs:")
    for u in tried:
        print(u)

    raise Exception("No F&O CSV bhavcopy found in last 10 days.")
    
