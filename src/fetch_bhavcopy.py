import requests
import datetime
import io
import zipfile
import pandas as pd
import csv

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com"
}

BASE = "https://archives.nseindia.com/content/fo/"

# DAT filename patterns used by NSE
DAT_PATTERNS = [
    "FNO_BC{DD}{MM}{YYYY}.DAT",
    "FNO_BC{DD}{MMM}{YYYY}.DAT",
    "FNOBCT{DD}{MM}{YYYY}.DAT",
    "fo{DD}{MMM}{YYYY}bhav.DAT",
]

# ZIP fallback patterns (if needed)
ZIP_PATTERNS = [
    "fo{DD}{MMM}{YYYY}bhav.csv.zip",
]


def _fetch(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200 and len(r.content) > 200:
            return r.content
    except:
        pass
    return None


def _guess_delimiter(text):
    """Guess delimiter using simple heuristics (no chardet)."""
    candidates = ["|", ",", ";", "\t", "~"]
    best = "|"
    best_count = 0

    sample = "\n".join(text.splitlines()[:20])

    for d in candidates:
        c = sample.count(d)
        if c > best_count:
            best_count = c
            best = d

    return best if best_count > 0 else ","


def _parse_dat(content_bytes):
    """Decode DAT bytes and parse into DataFrame."""
    # try utf-8
    try:
        text = content_bytes.decode("utf-8")
    except:
        text = content_bytes.decode("latin-1", errors="replace")

    text = text.replace("\r\n", "\n").strip()

    delim = _guess_delimiter(text)

    try:
        df = pd.read_csv(io.StringIO(text), sep=delim, engine="python", low_memory=False)
        df.columns = df.columns.str.strip()
        return df
    except:
        # fallback: whitespace split
        df = pd.read_csv(io.StringIO(text), delim_whitespace=True, engine="python")
        df.columns = df.columns.str.strip()
        return df


def _parse_zip(content_bytes):
    """Parse zipped CSV file."""
    z = zipfile.ZipFile(io.BytesIO(content_bytes))
    name = [n for n in z.namelist() if n.lower().endswith(".csv")][0]
    return pd.read_csv(z.open(name))


def fetch_bhavcopy(max_days=6):
    """
    Fetch DAT bhavcopy first (priority).
    Skip weekends.
    Fallback to ZIP if DAT not available.
    Returns DataFrame.
    """

    today = datetime.date.today()

    for i in range(max_days):
        d = today - datetime.timedelta(days=i)

        if d.weekday() >= 5:
            print(f"Skipping weekend: {d}")
            continue

        DD = d.strftime("%d")
        MM = d.strftime("%m")
        MMM = d.strftime("%b").upper()
        YYYY = d.strftime("%Y")

        # ---- Try DAT files (priority) ----
        for pat in DAT_PATTERNS:
            fname = pat.format(DD=DD, MM=MM, MMM=MMM, YYYY=YYYY)
            url = BASE + fname
            print("Trying DAT:", url)

            content = _fetch(url)
            if content:
                print("DAT found. Parsing...")
                df = _parse_dat(content)
                print("DAT parsed:", len(df), "rows")
                return df

        # ---- Try ZIP fallback ----
        for pat in ZIP_PATTERNS:
            fname = pat.format(DD=DD, MM=MM, MMM=MMM, YYYY=YYYY)
            url = BASE + fname
            print("Trying ZIP:", url)

            content = _fetch(url)
            if content:
                df = _parse_zip(content)
                print("ZIP parsed:", len(df), "rows")
                return df

    raise Exception("No bhavcopy (DAT/ZIP) found for last 6 days.")
    
