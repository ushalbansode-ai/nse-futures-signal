import os
import requests
import zipfile
import datetime
import pandas as pd

BASE_DIR = "data"
OUT_DIR = "data/signals"

# Ensure folders exist
for p in [BASE_DIR, OUT_DIR]:
    os.makedirs(p, exist_ok=True)


# -----------------------------------------------------------
# 1) Download latest Bhavcopy (NEW NSE FORMAT)
# -----------------------------------------------------------

def fetch_latest_bhavcopy():
    print("Fetching latest bhavcopy...")

    base_url = (
        "https://archives.nseindia.com/content/fo/"
        "BhavCopy_NSE_FO_0_0_0_{date}_F_0000.csv.zip"
    )

    today = datetime.date.today()
    tried = []

    # Try last 4 days
    for i in range(0, 4):
        d = today - datetime.timedelta(days=i)
        date_str = d.strftime("%Y%m%d")

        url = base_url.format(date=date_str)
        tried.append(url)

        print("Trying:", url)

        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})

        if r.status_code == 200:
            zip_path = f"{BASE_DIR}/Bhav_{date_str}.zip"
            open(zip_path, "wb").write(r.content)
            print("Downloaded:", url)

            # Extract ZIP
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(BASE_DIR)
                extracted_file = z.namelist()[0]

            print("Extracted:", extracted_file)

            return f"{BASE_DIR}/{extracted_file}"

        else:
            print("Failed:", r.status_code)

    print("Tried URLs:")
    for u in tried:
        print(" -", u)

    raise Exception("No bhavcopy found for last 4 days")


# -----------------------------------------------------------
# 2) Compute Signals
# -----------------------------------------------------------

def compute_signals(df):

    # Ensure correct columns exist
    required_cols = [
        "TckrSymb",
        "OpnIntrst",
        "ChngInOpnIntrst",
        "LastPric",
        "PrvsClsgPric",
        "TtlTradgVol",
    ]

    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Missing column: {col}")

    # Add momentum
    df["momentum"] = df["LastPric"] - df["PrvsClsgPric"]

    # Add OI Trend
    df["oi_trend"] = df["ChngInOpnIntrst"]

    # Volume spike
    df["vol_spike"] = df["TtlTradgVol"].pct_change().fillna(0)

    # BUY Signal
    df["buy_signal"] = (
        (df["momentum"] > 0)
        & (df["oi_trend"] > 0)
        & (df["vol_spike"] > 0.25)
    ).astype(int)

    return df


# -----------------------------------------------------------
# 3) MAIN
# -----------------------------------------------------------

def main():
    print("Fetching latest bhavcopy...")

    csv_path = fetch_latest_bhavcopy()

    print("Reading:", csv_path)
    df = pd.read_csv(csv_path)

    print("Rows:", len(df))
    print("Columns:", list(df.columns))

    print("Computing signals...")
    df = compute_signals(df)

    out_file = f"{OUT_DIR}/signal_{datetime.date.today()}.csv"
    df.to_csv(out_file, index=False)

    print("Saved:", out_file)


# -----------------------------------------------------------
# Run
# -----------------------------------------------------------
if __name__ == "__main__":
    main()
    
