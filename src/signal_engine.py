def generate_signals(df):
    """Simple signal logic."""
    if df.empty:
        return df

    df["signal"] = df["change"].apply(lambda x: "BUY" if x > 0 else "SELL")
    return df
    
