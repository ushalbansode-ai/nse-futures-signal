# src/fetch_bhavcopy.py
import requests
import datetime
import io
import zipfile
import pandas as pd
import chardet
import csv

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com"
}

BASE = "https://archives.nseindia.com/content/fo/"

# Candidate filename patterns (DAT-first). Extend if NSE changes naming again.
DAT_PATTERNS = [
    # older / common pattern
    "fo{DD}{MMM}{YYYY}bhav.DAT",        # fo26NOV2025bhav.DAT
    "fo{DD}{MM}{YYYY}bhav.DAT",         # fo26112025bhav.DAT
    # newer patterns observed in screenshots
    "FNO_BC{DD}{MM}{YYYY}.DAT",         # FNO_BC25112025.DAT
    "FNO_BC{DD}{MMM}{YYYY}.DAT",        # FNO_BC26NOV2025.DAT
    # alternate (if ever present)
    "FNOBCT{DD}{MM}{YYYY}.DAT",
]

ZIP_PATTERNS = [
    "fo{DD}{MMM}{YYYY}bhav.csv.zip",
    "fo{DD}{MM}{YYYY}bhav.csv.zip",
    "BhavCopy_NSE_FO_0_0_0_{YYYY}{MM}{DD}_F_0000.csv.zip",
    # generic fallback (rare)
    "fo{DD}{MMM}{YYYY}bhav.zip"
]

COMMON_DELIMS = ['|', ',', '\t', '^', '~', ';']


def _fetch_url(url, timeout=18):
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code == 200 and r.content and len(r.content) > 100:
            return r.content
    except Exception:
        pass
    return None


def _detect_encoding(b: bytes):
    try:
        guess = chardet.detect(b)
        return guess.get("encoding") or "utf-8"
    except Exception:
        return "utf-8"


def _guess_delim(sample_text: str, max_lines=10):
    lines = sample_text.splitlines()[:max_lines]
    counts = {d: sum(1 for ln in lines if d in ln) for d in COMMON_DELIMS}
    # pick delimiter with highest occurrences
    delim, cnt = max(counts.items(), key=lambda kv: kv[1])
    if cnt == 0:
        return None
    return delim


def _parse_dat_bytes(b: bytes) -> pd.DataFrame:
    """Try parse .DAT content robustly to DataFrame."""
    enc = _detect_encoding(b)
    text = None
    for e in (enc, "utf-8", "latin-1", "cp1252"):
        try:
            text = b.decode(e)
            break
        except Exception:
            text = None
    if text is None:
        text = b.decode("latin-1", errors="replace")

    # normalize line endings
    text = text.replace("\r\n", "\n").strip()
    if not text:
        raise ValueError("Empty DAT content after decoding")

    # try delimiter guess
    delim = _guess_delim(text)
    if delim:
        try:
            df = pd.read_csv(io.StringIO(text), sep=delim, engine="python", low_memory=False)
            df.columns = df.columns.str.strip()
            return df
        except Exception:
            pass

    # try csv.Sniffer
    try:
        sample = "\n".join(text.splitlines()[:50])
        sniff = csv.Sniffer().sniff(sample, delimiters=''.join(COMMON_DELIMS))
        df = pd.read_csv(io.StringIO(text), sep=sniff.delimiter, engine="python", low_memory=False)
        df.columns = df.columns.str.strip()
        return df
    except Exception:
        pass

    # fallback: try fixed width
    try:
        df = pd.read_fwf(io.StringIO(text))
        df.columns = df.columns.str.strip()
        return df
    except Exception:
        pass

    # last-resort: whitespace split
    lines = [ln for ln in text.splitlines() if ln.strip()]
    rows = [ln.split() for ln in lines]
    maxcols = max(len(r) for r in rows)
    cols = [f"col_{i}" for i in range(maxcols)]
    df = pd.DataFrame([r + [""]*(maxcols - len(r)) for r in rows], columns=cols)
    return df


def _parse_zip_bytes(b: bytes) -> pd.DataFrame:
    z = zipfile.ZipFile(io.BytesIO(b))
    # pick first CSV-like member
    for name in z.namelist():
        if name.lower().endswith(".csv"):
            try:
                return pd.read_csv(z.open(name))
            except Exception:
                # try decode bytes and guess delimiter
                raw = z.read(name)
                enc = _detect_encoding(raw)
                try:
                    txt = raw.decode(enc)
                except Exception:
                    txt = raw.decode("latin-1", errors="replace")
                delim = _guess_delim(txt) or ","
                return pd.read_csv(io.StringIO(txt), sep=delim, engine="python", low_memory=False)
    # if no csv found, try first file anyway
    name = z.namelist()[0]
    raw = z.read(name)
    enc = _detect_encoding(raw)
    try:
        txt = raw.decode(enc)
    except Exception:
        txt = raw.decode("latin-1", errors="replace")
    delim = _guess_delim(txt) or ","
    return pd.read_csv(io.StringIO(txt), sep=delim, engine="python", low_memory=False)


def fetch_bhavcopy(max_lookback_days: int = 7) -> pd.DataFrame:
    """
    Try multiple DAT filename patterns first (DAT prioritized), then ZIP patterns.
    Skips weekends. Looks back up to `max_lookback_days`.
    Returns: pandas.DataFrame on success or raises Exception.
    """

    today = datetime.date.today()

    for delta in range(max_lookback_days):
        day = today - datetime.timedelta(days=delta)

        # Skip weekends
        if day.weekday() >= 5:
            # saturday=5 sunday=6
            # continue but still print for debug
            print(f"Skipping weekend: {day.isoformat()}")
            continue

        DD = day.strftime("%d")
        MM = day.strftime("%m")
        MMM = day.strftime("%b").upper()
        YYYY = day.strftime("%Y")

        # Try DAT patterns first (user said DAT-only today)
        for pat in DAT_PATTERNS:
            fname = pat.format(DD=DD, MM=MM, MMM=MMM, YYYY=YYYY)
            url = BASE + fname
            print("Trying DAT:", url)
            content = _fetch_url(url)
            if content:
                try:
                    df = _parse_dat_bytes(content)
                    print(f"Parsed DAT: {fname} → rows={len(df)}")
                    return df
                except Exception as e:
                    print("DAT parse failed:", e)
                    # if parse failed, continue trying other patterns

        # If DATs not found/parsed, try ZIP patterns
        for pat in ZIP_PATTERNS:
            fname = pat.format(DD=DD, MM=MM, MMM=MMM, YYYY=YYYY)
            url = BASE + fname
            print("Trying ZIP:", url)
            content = _fetch_url(url)
            if content:
                try:
                    df = _parse_zip_bytes(content)
                    print(f"Parsed ZIP: {fname} → rows={len(df)}")
                    return df
                except Exception as e:
                    print("ZIP parse failed:", e)

    raise Exception(f"No bhavcopy (DAT/ZIP) found in last {max_lookback_days} days.")
                
