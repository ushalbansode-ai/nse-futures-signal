import os
import json
import pandas as pd

SIGNALS_DIR = "data/signals/"


def save_signals(df):
    os.makedirs(SIGNALS_DIR, exist_ok=True)
    out = SIGNALS_DIR + "latest_signals.json"
    df.to_json(out, orient="records", indent=2)
    print("[INFO] Signals saved:", out)
  
