import os
import requests
import zipfile
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

# =====================================================================
# FOLDER CREATION (Fixes GitHub Actions "directory does not exist" error)
# =====================================================================

BASE_DIR = "data"
OUT_DIR = "data/signals"

os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

OUT_FILE = f"{OUT_DIR}/latest.csv"

# =====================================================================
# BHAVCOPY URL GENERATOR
# =====================================================================

def get_bhavcopy_url(date_obj):
    yyyy = date_obj.strftime("%Y")
    mon = date_obj.strftime("%b").upper()
    dd = date_obj.strftime("%d")
    return f"https://archives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{yyyy}{mon}{dd}_F_0000.csv.zip"

# =====================================================================
# DOWNLOAD BHAVCOPY WITH RETRIES
# =====================================================================

def download_latest_bhavcopy():
    today = datetime.now()
    attempts = 4

    for i in range(attempts):
        dt = today - timedelta(days=i)
        url = get_bhavcopy_url(dt)

        print(f"Trying:\n{url}")
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})

        if resp.status_code == 200:
            print("Downloaded:")
            print(url)
            return resp.content

        print("Failed:", resp.status_code)

    raise Exception("Could not download any bhavcopy file!")

# =====================================================================
# COLUMN MAPPING (Dynamic, Based on NSE Changes)
# =====================================================================

COLUMN_MAP = {
    "symbol": ["TckrSymb", "Symbol"],
    "instr_type": ["FinInstrmTp"],
    "expiry": ["XpryDt"],
    "strike": ["StrkPric"],
    "opt_type": ["OptnTp"],
    "open_int": ["OpnIntrst"],
    "chg_oi": ["ChngInOpnIntrst"],
    "vol": ["TtlTrdQty", "TtlTradgVol"],
    "value": ["TtlTrdVal"],
    "last": ["LastPric"],
    "close": ["ClsPric"],
    "prev_close": ["PrevClsPric"],
    "settle": ["SttlmPric"]
}

def detect_column(df, canonical):
    for actual in COLUMN_MAP[canonical]:
        if actual in df.columns:
            return actual
    return None

# =====================================================================
# SIGNAL COMPUTATION
# =====================================================================

def compute_signals(df):
    # Fix previous close missing issue
    df["prev_close_safe"] = df["prev_close"].fillna(df["close"])

    df["price_change"] = df["close"] - df["prev_close_safe"]

    df["signal"] = df.apply(lambda r:
                            "LONG" if r["price_change"] > 0 and r["chg_oi"] > 0 else
                            "SHORT" if r["price_change"] < 0 and r["chg_oi"] < 0 else
                            "NEUTRAL",
                            axis=1)

    return df

# =====================================================================
# MAIN PROCESS
# =====================================================================

def main():
    print("\nFetching latest NSE F&O bhavcopy...\n")
    zip_bytes = download_latest_bhavcopy()

    # Extract zip file ================================
    z = zipfile.ZipFile(BytesIO(zip_bytes))
    csv_name = z.namelist()[0]
    print(f"Extracting: {csv_name}")
    z.extract(csv_name, BASE_DIR)

    csv_path = f"{BASE_DIR}/{csv_name}"
    print(f"Extracted file: {csv_path}")

    # Load CSV ================================
    df = pd.read_csv(csv_path)
    print("Rows:", len(df))

    print("\nRaw columns (sample):", list(df.columns)[:15])

    # Apply dynamic column mapping =====================
    mapped = {}
    for key in COLUMN_MAP:
        col = detect_column(df, key)
        if col:
            mapped[key] = col

    print("\nDetected column mapping (canonical -> actual):")
    for k, v in mapped.items():
        print(f"{k:12} -> {v}")

    # Rename columns so we work with uniform names
    df = df.rename(columns={mapped[k]: k for k in mapped})

    # Compute signals ===============================
    print("\nComputing signals...")
    df = compute_signals(df)

    # Save output ===============================
    df.to_csv(OUT_FILE, index=False)
    print(f"\nSaved output to: {OUT_FILE}")

# =====================================================================

if __name__ == "__main__":
    main()
    
