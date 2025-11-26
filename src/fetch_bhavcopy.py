import pandas as pd
import requests
import io
import zipfile
from datetime import datetime, timedelta

# -------------------------------------------------------------
# Fetch NSE FO Bhavcopy and ALWAYS return a Pandas DataFrame
# -------------------------------------------------------------

def fetch_bhavcopy():
    today = datetime.now()
    attempts = 5

    for i in range(attempts):
        dt = today - timedelta(days=i)
        date_str = dt.strftime("%d%m%Y")
        date_csv = dt.strftime("%Y%m%d")

        # --- RAW .DAT file
        url_dat = f"https://archives.nseindia.com/content/fo/FNOBCT{date_str}.DAT"

        # --- ZIP CSV
        url_zip = f"https://archives.nseindia.com/content/fo/BhavCopy_NSE_FO_{date_csv}.csv.zip"

        print(f"Trying: {url_dat}")
        try:
            r = requests.get(url_dat, timeout=20)
            if r.status_code == 200 and len(r.content) > 1000:
                df = pd.read_csv(io.BytesIO(r.content))
                return df
        except:
            pass

        print(f"Trying: {url_zip}")
        try:
            r = requests.get(url_zip, timeout=20)
            if r.status_code == 200:
                z = zipfile.ZipFile(io.BytesIO(r.content))
                csv_name = z.namelist()[0]
                df = pd.read_csv(z.open(csv_name))
                return df
        except:
            pass

    raise Exception("Failed to fetch bhavcopy for all attempts.")
    
