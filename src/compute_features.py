def compute_features(df):
    """
    Compute features for futures contracts.
    Must return a Pandas DataFrame.
    """
    import pandas as pd

    df = df.copy()

    # Convert columns to numeric safely
    df["OPEN"] = pd.to_numeric(df["OPEN"], errors="coerce")
    df["HIGH"] = pd.to_numeric(df["HIGH"], errors="coerce")
    df["LOW"] = pd.to_numeric(df["LOW"], errors="coerce")
    df["CLOSE"] = pd.to_numeric(df["CLOSE"], errors="coerce"])
    df["SETTLE_PR"] = pd.to_numeric(df["SETTLE_PR"], errors="coerce")

    # Basic features
    df["range"] = df["HIGH"] - df["LOW"]
    df["body"] = df["CLOSE"] - df["OPEN"]
    df["upper_wick"] = df["HIGH"] - df[["CLOSE", "OPEN"]].max(axis=1)
    df["lower_wick"] = df[["CLOSE", "OPEN"]].min(axis=1) - df["LOW"]

    # Momentum feature
    df["momentum"] = df["CLOSE"] - df["CLOSE"].shift(1)

    # Only return FUTIDX + FUTSTK rows
    df = df[df["INSTRUMENT"].str.contains("FUT", na=False)]

    return df
  
