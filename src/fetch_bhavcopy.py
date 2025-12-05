import os
import requests
import datetime
import pandas as pd
import time
from io import BytesIO
import zipfile


def download_bhavcopy(output_folder):
    """Download F&O bhavcopy from NSE website (actual bhavcopy, not pre-open data)."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"bhavcopy_{today.replace('-', '')}.csv"
    full_path = os.path.join(output_folder, filename)
    
    # Format for NSE bhavcopy URL (ddMMMyy format)
    date_str = datetime.datetime.now().strftime("%d%b%Y").upper()
    
    # Try multiple possible URLs for F&O bhavcopy
    urls = [
        f"https://archives.nseindia.com/content/historical/DERIVATIVES/{datetime.datetime.now().strftime('%Y')}/{datetime.datetime.now().strftime('%b').upper()}/fo{date_str}bhav.csv.zip",
        f"https://www1.nseindia.com/content/historical/DERIVATIVES/{datetime.datetime.now().strftime('%Y')}/{datetime.datetime.now().strftime('%b').upper()}/fo{date_str}bhav.csv.zip",
        f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{datetime.datetime.now().strftime('%d%m%Y')}.csv"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": "https://www.nseindia.com/",
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    # First, get the main page to set cookies
    try:
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(1)
    except:
        pass
    
    for url in urls:
        try:
            print(f"Trying URL: {url}")
            response = session.get(url, timeout=30)
            
            if response.status_code == 200:
                # Check if it's a zip file
                if url.endswith('.zip'):
                    with zipfile.ZipFile(BytesIO(response.content)) as z:
                        # Extract CSV file
                        csv_filename = [name for name in z.namelist() if name.endswith('.csv')][0]
                        with z.open(csv_filename) as f:
                            content = f.read()
                        
                        # Save to file
                        with open(full_path, 'wb') as out_file:
                            out_file.write(content)
                else:
                    # Direct CSV
                    with open(full_path, 'wb') as f:
                        f.write(response.content)
                
                print(f"✅ Bhavcopy downloaded → {full_path}")
                return full_path
                
        except Exception as e:
            print(f"❌ Error with URL {url}: {e}")
            continue
    
    print("❌ Could not download bhavcopy from any source")
    return None


# Alternative: Using NSE's API for option chain (if bhavcopy fails)
def download_option_chain(output_folder):
    """Fallback: Download option chain data from NSE API."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"option_chain_{today.replace('-', '')}.csv"
    full_path = os.path.join(output_folder, filename)
    
    # Try to get option chain for NIFTY
    url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
        "X-Requested-With": "XMLHttpRequest",
    }
    
    session = requests.Session()
    session.headers.update(headers)
    
    try:
        # Get cookies first
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(2)
        
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Process option chain data
        records = data['records']['data']
        
        # Convert to DataFrame
        df = pd.json_normalize(records)
        
        # Save to CSV
        df.to_csv(full_path, index=False)
        print(f"✅ Option chain downloaded → {full_path}")
        return full_path
        
    except Exception as e:
        print(f"❌ Error downloading option chain: {e}")
        return None
