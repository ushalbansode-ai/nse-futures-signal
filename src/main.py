import os
import json
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta

# =====================================================
# 1) ENSURE REQUIRED DIRECTORIES EXIST
# =====================================================
BASE_DIR = "data"
OUT_DIR = "data/signals"

for path in [BASE_DIR, OUT_DIR]:
    os.makedirs(path, exist_ok=True)


# =====================================================
# 2) DOWNLOAD NSE BHAVCOPY (ZIP OR CSV)
# =====================================================
def try_download(url, save_as):
    try:
        print(f"Trying:\n{url}")
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            with open(save_as, "wb") as f:
                f.write(r.content)
            print(f"Downloaded:\n{url}")
            return True
        print(f"Failed: {r.status_code}")
        return False
    except Exception as e:
        print(f"Error downloading: {e}")
        return False


def download_latest_bhavcopy():
    print("Fetching latest NSE F&O bhavcopy (csv.zip pattern)...")

    today = datetime.now()
    for i in range(3):  # try last 3 days
        d = today - timedelta(days=i)
        dd = d.strftime("%d")
        mm = d.strftime("%m")
        yyyy = d.strftime("%Y")

        fname = f"fo{dd}{mm}{yyyy}bhav.csv.zip"
        url = f"https://archives.nseindia.com/content/historical/DERIVATIVES/{yyyy}/{d.strftime('%b').upper()}/{fname}"
        save_zip = os.path.join(BASE_DIR, fname)

        if try_download(url, save_zip):
            return save_zip

    print("Trying NSE 'BhavCopy NSE FO' pattern...")

    # fallback – NSE's long name format
    for i in range(3):
        d = today - timedelta(days=i)
        fname = f"BhavCopy_NSE_FO_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip"
        url = f"https://archives.nseindia.com/content/fo/{fname}"
        save_zip = os.path.join(BASE_DIR, fname)

        if try_download(url, save_zip):
            return save_zip

    raise Exception("Could not download any bhavcopy file!")


# =====================================================
# 3) AUTO–MAP NSE COLUMNS TO STANDARD NAMES
# =====================================================
COLUMN_MAP = {
    "symbol": ["TckrSymb", "SYMBOL"],
    "instr_type": ["FinInstrmTp"],
    "expiry": ["XpryDt"],
    "strike": ["StrkPric"],
    "opt_type": ["OptnTp"],
    "open_int": ["OpnIntrst"],
    "chg_oi": ["ChngInOpnIntrst"],
    "vol": ["TtlTradgVol"],
    "value": ["TtlTrfVal"],
    "last": ["LastPric"],
    "close": ["ClsPric"],
    "prev_close": ["PrvsClsgPric"],
    "settle": ["SttlmPric"],
}


def normalize_columns(df):
    print("\nRaw columns (sample):", df.columns.tolist()[:15])

    final_map = {}
    for canonical, possible in COLUMN_MAP.items():
        found = None
        for col in possible:
            if col in df.columns:
                found = col
                break
        if found is None:
            raise Exception(f"Missing required column for '{canonical}' – expected any of {possible}")
        final_map[canonical] = found

    print("\nDetected column mapping (canonical -> actual):")
    for k, v in final_map.items():
        print(f"  {k:<10} -> {v}")

    return df.rename(columns=final_map)


# =====================================================
# 4) SIGNAL CALCULATION
# =====================================================
def compute_signals(df):

    df["oi_change_pct"] = (df["chg_oi"] / df["open_int"]).replace([float("inf"), -float("inf")], 0)
    df["vol_ratio"] = df["vol"] / df["vol"].rolling(20).mean().fillna(df["vol"])

    df["signal"] = "NEUTRAL"
    df.loc[(df["vol_ratio"] > 2) & (df["oi_change_pct"] > 0.10), "signal"] = "LONG BUILDUP"
    df.loc[(df["vol_ratio"] > 2) & (df["oi_change_pct"] < -0.10), "signal"] = "SHORT COVERING"

    return df
# =====================================================
# 5) MAIN EXECUTION LOGIC
# =====================================================
def main():
    zip_path = download_latest_bhavcopy()
    print(f"\nExtracting: {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(BASE_DIR)
        extracted = z.namelist()[0]

    csv_path = os.path.join(BASE_DIR, extracted)
    print("Extracted file:", csv_path)

    df = pd.read_csv(csv_path)
    print("Rows:", len(df))
    # normalize
    df = normalize_columns(df)

    # compute signals
    print("Computing signals...")
    df = compute_signals(df)
    # save
    out_file = os.path.join(OUT_DIR, "fo_signals.csv")
    df.to_csv(out_file, index=False)

    print("\nSaved:", out_file)
    print("Job complete.")
# =====================================================
# 6) START
# =====================================================
if __name__ == "__main__":
    main()
                           
