import os
import requests
import pandas as pd
import zipfile
from io import BytesIO
from datetime import datetime, timedelta

# ==========================================================
# SAFE FOLDER CREATION (Fixes GitHub “FileExistsError”)
# ==========================================================

BASE_DIR = "data"
OUT_DIR = "data/signals"

def safe_mkdir(path):
    # If a FILE exists but we need a FOLDER → rename file
    if os.path.isfile(path):
        os.rename(path, path + "_old")

    if not os.path.exists(path):
        os.makedirs(path)

safe_mkdir(BASE_DIR)
safe_mkdir(OUT_DIR)

OUT_FILE = f"{OUT_DIR}/latest.csv"

# ==========================================================
# DOWNLOAD LATEST F&O BHAVCOPY  (NSE ZIP PATTERN)
# ==========================================================

def try_url(url):
    print("Trying:", url)
    r = requests.get(url, timeout=10)
    if r.status_code == 200:
        print("Downloaded:", url)
        return r
    print("Failed:", r.status_code)
    return None


def get_latest_bhavcopy():
    today = datetime.today()
    
    # NSE uses ZIP like:
    # https://archives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_20251126_F_0000.csv.zip
    
    for i in range(7):
        d = today - timedelta(days=i)
        dstr = d.strftime("%Y%m%d")

        url = (
            "https://archives.nseindia.com/content/fo/"
            f"BhavCopy_NSE_FO_0_0_0_{dstr}_F_0000.csv.zip"
        )

        r = try_url(url)
        if r:
            return dstr, r.content

    raise Exception("No bhavcopy found for last 7 days!")


# ==========================================================
# EXTRACT ZIP & READ CSV
# ==========================================================

def extract_csv(date_str, zip_bytes):
    zip_path = f"{BASE_DIR}/BhavCopy_{date_str}.zip"

    with open(zip_path, "wb") as f:
        f.write(zip_bytes)

    with zipfile.ZipFile(BytesIO(zip_bytes)) as z:
        csv_name = z.namelist()[0]
        print("Extracting:", csv_name)
        z.extract(csv_name, BASE_DIR)
        csv_path = f"{BASE_DIR}/{csv_name}"
        return csv_path


# ==========================================================
# COLUMN MAPPING (AUTO-DETECT)
# ==========================================================

COLUMN_MAP = {
    'symbol': ['TckrSymb', 'SYMBOL'],
    'instr_type': ['FinInstrmTp'],
    'expiry': ['XpryDt'],
    'strike': ['StrkPric'],
    'opt_type': ['OptnTp'],
    'open_int': ['OpnIntrst'],
    'chg_oi': ['ChngInOpnIntrst'],
    'vol': ['TtlTrdQty'],
    'value': ['TtlTrdVal'],
    'last': ['LastPric'],
    'close': ['ClsPric'],
    'prev_close': ['PrvsClsPric'],
    'settle': ['SttlmPric'],
}


def auto_map_columns(df):
    final = {}
    for k, cand in COLUMN_MAP.items():
        for c in cand:
            if c in df.columns:
                final[k] = c
                break
    return final


# ==========================================================
# SIGNAL COMPUTATION
# ==========================================================

def compute_signals(df):
    mapping = auto_map_columns(df)

    # Convert relevant columns to numeric
    num_cols = ["open_int", "chg_oi", "vol", "value",
                "last", "close", "prev_close", "strike"]

    for c in num_cols:
        if c in mapping:
            df[mapping[c]] = pd.to_numeric(df[mapping[c]], errors="coerce")

    # Example signal: Momentum
    df["momentum"] = df[mapping["last"]] - df[mapping["prev_close"]]

    # Example signal: OI Trend
    df["oi_trend"] = df[mapping["chg_oi"]]

    # Example signal: Volume Spike
    df["vol_spike"] = (df[mapping["vol"]] >
                       df[mapping["vol"]].rolling(20).mean() * 1.5)

    return df


# ==========================================================
# MAIN
# ==========================================================

def main():
    print("Fetching latest bhavcopy...")
    date_str, zip_bytes = get_latest_bhavcopy()

    csv_path = extract_csv(date_str, zip_bytes)
    print("Reading:", csv_path)

    df = pd.read_csv(csv_path)
    print("Rows:", len(df))
    print("Columns:", list(df.columns))

    df = compute_signals(df)

    print("Saving output →", OUT_FILE)
    df.to_csv(OUT_FILE, index=False)
    print("DONE!")


if __name__ == "__main__":
    main()
    
