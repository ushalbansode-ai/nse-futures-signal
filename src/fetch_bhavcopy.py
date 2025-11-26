import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# Correct NSE archive domain
ARCHIVE_URL = "https://archives.nseindia.com/content/fo/"


def fetch_bhavcopy():

    print("\nFetching NSE Futures DAT bhavcopy using AUTO-SCAN...\n")

    # Step 1: Fetch directory listing page
    try:
        h = requests.get(ARCHIVE_URL, headers=HEADERS, timeout=10)
        h.raise_for_status()
    except Exception as e:
        raise Exception("Failed to load NSE directory listing") from e

    soup = BeautifulSoup(h.text, "html.parser")

    # Step 2: Extract file links
    all_links = [a.get("href") for a in soup.find_all("a") if a.get("href")]

    # Step 3: Filter only DAT bhavcopies
    dat_files = [
        link for link in all_links
        if link.endswith(".DAT") and ("FNO" in link.upper())
    ]

    if not dat_files:
        raise Exception("No DAT files found in directory listing")

    print(f"Found {len(dat_files)} DAT files on server.")

    # Step 4: Sort by date automatically
    def extract_date(fn):
        # Extract ddmmyyyy from filename if present
        numbers = ''.join(ch for ch in fn if ch.isdigit())
        return numbers[-8:]  # last 8 digits = ddmmyyyy

    dat_files_sorted = sorted(dat_files, key=extract_date, reverse=True)

    # Step 5: Try each file from newest to oldest
    for fname in dat_files_sorted[:10]:  # top 10 recent files only
        url = ARCHIVE_URL + fname
        print(f"Trying: {url}")

        try:
            r = requests.get(url, headers=HEADERS, timeout=10)

            if r.status_code == 200 and len(r.content) > 5000:
                print(f"\nSUCCESS — Downloaded {fname}\n")
                df = pd.read_csv(BytesIO(r.content), sep="|")
                return df

        except Exception:
            continue

    raise Exception("Auto-scan failed — could not download ANY recent DAT file.")
    
