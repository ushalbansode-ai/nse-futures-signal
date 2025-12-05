import pandas as pd


def compare_with_previous(latest_df, previous_df):
    """Compare latest data with previous day's data."""
    if latest_df.empty:
        return latest_df
    
    # Clean column names
    latest_df.columns = latest_df.columns.str.strip()
    
    if previous_df.empty:
        # If no previous data, set change to 0
        latest_df["change"] = 0
        return latest_df
    
    # Clean previous df column names
    previous_df.columns = previous_df.columns.str.strip()
    
    # Find common symbol column
    symbol_col = None
    for col in latest_df.columns:
        if 'symbol' in col.lower() or 'SYMBOL' in col:
            symbol_col = col
            break
    
    if symbol_col is None:
        print("⚠️ No symbol column found")
        latest_df["change"] = 0
        return latest_df
    
    # Find price column (close or last price)
    price_col = None
    for col in latest_df.columns:
        if 'close' in col.lower() or 'CLOSE' in col or 'last' in col.lower() or 'LAST' in col:
            price_col = col
            break
    
    if price_col is None:
        print("⚠️ No price column found")
        latest_df["change"] = 0
        return latest_df
    
    # Merge with previous data
    merged = latest_df.merge(
        previous_df[[symbol_col, price_col]],
        on=symbol_col,
        how="left",
        suffixes=("", "_prev")
    )
    
    # Calculate change
    merged["change"] = merged[price_col] - merged[f"{price_col}_prev"].fillna(merged[price_col])
    
    return merged
