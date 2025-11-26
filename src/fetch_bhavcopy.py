import pandas as pd
import requests
from datetime import datetime, timedelta

BASE = "https://archives.nseindia.com/content/fo"

# NSE valid DAT patterns (confirmed)
PATTERNS = [
    "FNO_{d}.DAT",
    "FNO_BC{d}.DAT",
    "FNOBC{d}.DAT",
    "FNOBCT{d}.DAT"
]


def fetch_bhavcopy():
    """
    Auto-fetch NSE Futures bhavcopy DAT file.
    No directory listing. No chardet. Pure deterministic logic.
    Tries 7 recent trading days.
    """

    today = datetime.now()
    attempts = []

    for i in range(7):
        day = today - timedelta(days=i)

        # Skip Saturday/Sunday
        if day.weekday() >= 5:
            continue

        dstr = day.strftime("%d%m%Y")

        for pattern in PATTERNS:
            filename = pattern.format(d=dstr)
            url = f"{BASE}/{filename}"

            attempts.append(url)
            print(f"Trying: {url}")

            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200 and len(r.content) > 5000:
                    print(f"Downloaded DAT: {filename}")

                    text = r.content.decode("latin1")  # Safe decode
                    lines = [x.strip() for x in text.split("\n") if x.strip()]

                    df = pd.DataFrame([x.split(",") for x in lines])
                    df.columns = df.iloc[0]
                    df = df[1:]

                    return df

            except Exception as e:
                print(f"Error fetching {url}: {e}")

    print("\nTried URLs:")
    for a in attempts:
        print(a)

    raise Exception("No valid F&O DAT bhavcopy found in last 7 days.")
    
