import os
import requests
import datetime


NSE_URL = "https://www.nseindia.com/api/market-data-pre-open?key=FO"


def download_bhavcopy(output_folder):
    """
    Downloads derivative bhavcopy (FO) from NSE API.
    Saves as a CSV file under data/raw/.
    Returns full file path.
    """

    today = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"bhavcopy_{today}.csv"
    full_path = os.path.join(output_folder, filename)

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Connection": "keep-alive"
    }

    print("📡 Connecting to NSE API...")

    try:
        r = requests.get(NSE_URL, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()

        # FO Bhavcopy records are inside 'data' → 'fno'
        if "data" not in data or "fno" not in data["data"]:
            print("❌ NSE FO data missing")
            return None

        fno = data["data"]["fno"]

        import pandas as pd
        df = pd.DataFrame(fno)
        df.to_csv(full_path, index=False)

        print(f"✅ Bhavcopy saved: {full_path}")
        return full_path

    except Exception as e:
        print("❌ ERROR downloading bhavcopy:", e)
        return None
        
