# src/main.py
import os
import json
import zipfile
import io
from datetime import datetime, timedelta

import requests
import pandas as pd

BASE_URL = "https://archives.nseindia.com/content/fo/"
OUT_DIR = "data/signals"
os.makedirs(OUT_DIR, exist_ok=True)


# --- utils: flexible column mapper -----------------------------------------
COL_ALIASES = {
    "symbol": ["TckrSymb", "TrdSymbol", "TrdSymb", "Symbol", "SYMBOL", "TrdSymbol"],
    "instr_type": ["FinInstrmTp", "Instrument", "INSTRUMENT", "Sgmt", "Sgmnt"],
    "expiry": ["XpryDt", "ExprDt", "EXPIRY_DT", "Expiry", "EXPIRY"],
    "strike": ["StrkPric", "StrkPr", "StrikePrice", "FinInstrmActnStrkPric", "STRIKE"],
    "opt_type": ["OptnTp", "OptType", "OptionType", "OPTTYP", "OptnTp"],
    "open_int": ["OpnIntrst", "OPEN_INT", "OpenInterest", "Open_Int"],
    "chg_oi": ["ChngInOpnIntrst", "ChngInOpnIntrst", "CHG_IN_OI", "ChgInOpnIntrst"],
    "vol": ["TtlTradgVol", "TtlTrdQty", "CONTRACTS", "TtlTradgVol", "TtlTrdQty", "TtlTradgVol"],
    "value": ["TtlTrfVal", "TtlTrdVal", "VAL_INLAKH", "TtlTrfVal"],
    "last": ["LastPric", "LastPrice", "Last_Price", "Last"],
    "close": ["ClsPric", "Close", "ClosePrice", "ClsPric", "PrvsClsgPric", "PrvsClsgPric"],
    "prev_close": ["PrvsClsgPric", "PrevClsgPric", "PrevClose", "Prev_Close"],
    # helpful extras user might have:
    "settle": ["SttlmPric", "Setlmt", "SETTLE_PR"]
}


def detect_column(df_columns, aliases):
    """Return first alias present in df_columns, or None."""
    for a in aliases:
        if a in df_columns:
            return a
    return None


def map_columns(df):
    """Map dataframe columns to canonical names and return new df + mapping dict."""
    cols = df.columns.tolist()
    mapping = {}
    for canon, aliases in COL_ALIASES.items():
        found = detect_column(cols, aliases)
        mapping[canon] = found  # may be None
    # Build renamed DataFrame copy for canonical access
    renamed = df.copy()
    rename_map = {v: k for k, v in mapping.items() if v is not None}
    # rename_map maps original_col -> canon_key, but pandas rename expects original->new
    renamed = renamed.rename(columns=rename_map)
    return renamed, mapping


# --- download / extract ----------------------------------------------------
def download_bhavcopy_last_n_days(n_days=5):
    today = datetime.today()
    tried_urls = []
    for i in range(n_days):
        dt = today - timedelta(days=i)
        # skip weekends
        if dt.weekday() >= 5:
            continue
        date_code = dt.strftime("%Y%m%d")
        fname = f"BhavCopy_NSE_FO_0_0_0_{date_code}_F_0000.csv.zip"
        url = BASE_URL + fname
        tried_urls.append(url)
        print("Trying:", url)
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            if r.status_code == 200 and len(r.content) > 2000:
                print("Downloaded:", url)
                return r.content, url
            else:
                print("Failed:", r.status_code, url)
        except Exception as e:
            print("Error fetching", url, e)
    print("Tried URLs:")
    for u in tried_urls:
        print("  ", u)
    return None, None


def extract_csv_from_zip(zip_bytes):
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))
    # prefer first CSV-like file
    csv_name = None
    for nm in z.namelist():
        if nm.lower().endswith(".csv"):
            csv_name = nm
            break
    if csv_name is None:
        csv_name = z.namelist()[0]
    print("Extracting:", csv_name)
    with z.open(csv_name) as fh:
        df = pd.read_csv(fh, low_memory=False)
    return df, csv_name


# --- core signal logic ----------------------------------------------------
def compute_signals_canonical(df):
    """
    Expects df with canonical column names (after map_columns rename).
    Uses:
      - 'symbol', 'instr_type', 'expiry', 'strike', 'opt_type',
      - 'open_int', 'chg_oi', 'vol', 'last', 'close', 'prev_close'
    """
    # ensure numeric conversions
    for col in ["open_int", "chg_oi", "vol", "last", "close", "prev_close", "strike"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # compute fallback previous close
    if "prev_close" not in df.columns and "close" in df.columns:
        df["prev_close"] = pd.NA

    # Price change uses LastPric - PrevClsgPric when available, else Last - Close
    if "last" in df.columns and "prev_close" in df.columns and df["prev_close"].notna().any():
        df["price_change"] = df["last"] - df["prev_close"]
    elif "last" in df.columns and "close" in df.columns:
        df["price_change"] = df["last"] - df["close"]
    else:
        df["price_change"] = pd.NA

    # OI change and volume
    if "chg_oi" not in df.columns:
        df["chg_oi"] = 0
    if "vol" not in df.columns:
        df["vol"] = 0

    # Basic rules
    df["signal"] = "HOLD"
    # Long buildup: OI up & price up
    df.loc[(df["chg_oi"] > 0) & (df["price_change"] > 0), "signal"] = "Long Buildup"
    # Short buildup: OI up & price down
    df.loc[(df["chg_oi"] > 0) & (df["price_change"] < 0), "signal"] = "Short Buildup"
    # Long unwinding: OI down & price down
    df.loc[(df["chg_oi"] < 0) & (df["price_change"] < 0), "signal"] = "Long Unwinding"
    # Short covering: OI down & price up
    df.loc[(df["chg_oi"] < 0) & (df["price_change"] > 0), "signal"] = "Short Covering"

    # Add a small score to rank interesting rows:
    df["score"] = 0
    df.loc[df["chg_oi"] > 0, "score"] += 1
    df.loc[df["vol"] > 0, "score"] += 1
    df.loc[df["price_change"] > 0, "score"] += 1

    return df


# --- main --------------------------------------------------------------
def main():
    print("Fetching latest NSE F&O bhavcopy (csv.zip pattern)...")
    zip_bytes, url = download_bhavcopy_last_n_days(7)
    if zip_bytes is None:
        raise SystemExit("No bhavcopy found in last 7 days; please check NSE archive pattern.")

    df_raw, csv_name = extract_csv_from_zip(zip_bytes)
    print("Raw columns (sample):", df_raw.columns[:20].tolist())

    # map columns
    df_mapped, mapping = map_columns(df_raw)
    print("Detected column mapping (canonical -> actual):")
    for k, v in mapping.items():
        print(f"  {k:12s} -> {v}")

    # verify we have symbol (at minimum)
    if mapping.get("symbol") is None:
        raise SystemExit("Could not detect symbol column in bhavcopy. Columns present: " + ", ".join(df_raw.columns))

    # compute signals on canonical names
    df_signals = compute_signals_canonical(df_mapped)

    # Choose output columns (include canonical metadata if present)
    out_cols = []
    for c in ["symbol", "instr_type", "expiry", "strike", "opt_type", "open_int", "chg_oi", "vol", "last", "close", "price_change", "signal", "score"]:
        if c in df_signals.columns:
            out_cols.append(c)

    out_df = df_signals[out_cols].copy()

    # rename columns back to readable names for output
    readable_map = {
        "symbol": "symbol",
        "instr_type": "instrument_type",
        "expiry": "expiry",
        "strike": "strike",
        "opt_type": "option_type",
        "open_int": "open_interest",
        "chg_oi": "change_in_oi",
        "vol": "volume",
        "last": "last_price",
        "close": "close_price",
        "price_change": "price_change",
        "signal": "signal",
        "score": "score"
    }
    out_df = out_df.rename(columns=readable_map)

    csv_path = os.path.join(OUT_DIR, "latest_signals.csv")
    json_path = os.path.join(OUT_DIR, "latest_signals.json")

    out_df.to_csv(csv_path, index=False)
    out_df.to_json(json_path, orient="records", force_ascii=False, indent=2)

    print(f"Saved outputs: {csv_path}, {json_path}")
    print("Done.")


if __name__ == "__main__":
    main()
        
