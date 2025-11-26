import pandas as pd
import numpy as np

def add_technical_features(df):
    df["avg_price"] = (df["OPEN"] + df["HIGH"] + df["LOW"] + df["CLOSE"]) / 4
    df["ret"] = df["avg_price"].pct_change().fillna(0)

    df["vwap"] = (df["VALUE"] / df["VOLUME"]).replace([np.inf, -np.inf], 0)

    df["momentum3"] = df["avg_price"].diff(3).fillna(0)
    df["vol_spike"] = df["VOLUME"] / df["VOLUME"].rolling(10).mean()
    df["vol_spike"].replace([np.inf, -np.inf], 0, inplace=True)

    return df
  
