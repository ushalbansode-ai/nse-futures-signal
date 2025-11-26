import requests
import pandas as pd
from io import BytesIO

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def fetch_bhavcopy():
    print("Fetching NSE Futures DAT bhavcopy...\n")

    url_base = "https://archives.nseindia.com/content/fo/"
    max_days = 6

    from datetime import datetime, timedelta
    today = datetime.today()

    for i in range(max_days):
        d = today - timedelta(days=i)
        if d.weekday() >= 5:
            print(f"Skipping weekend: {d.date()}")
            continue

        dd = d.strftime("%d")
        mm = d.strftime("%m")
        yyyy = d.strftime("%Y")

        dat1 = f"FNO_BC{dd}{mm}{yyyy}.DAT"      # Correct pattern
        dat2 = f"FNOBCT{dd}{mm}{yyyy}.DAT"      # Correct alternative pattern

        urls = [
            url_base + dat1,
            url_base + dat2,
        ]

        for u in urls:
            print(f"Trying DAT: {u}")
            try:
                r = requests.get(u, headers=HEADERS, timeout=10)
                if r.status_code == 200 and len(r.content) > 2000:
                    print(f"\nSUCCESS — Downloaded {u}\n")
                    df = pd.read_csv(BytesIO(r.content), sep="|")
                    return df
            except Exception:
                pass

    raise Exception("No bhavcopy DAT available for last 6 trading days.")
    
