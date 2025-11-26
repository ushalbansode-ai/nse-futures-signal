import pandas as pd

def generate_signals(df):
    df = df.copy()

    # BASIC SIGNAL MODEL
    conditions_buy = (
        (df["RET"] > 0.5) &         # price rising
        (df["OI_PCT"] > 2) &        # OI building
        (df["MOMENTUM"] == 1)
    )

    conditions_sell = (
        (df["RET"] < -0.5) &        # price falling
        (df["OI_PCT"] < -2) &
        (df["MOMENTUM"] == 0)
    )

    df["SIGNAL"] = "HOLD"
    df.loc[conditions_buy, "SIGNAL"] = "BUY"
    df.loc[conditions_sell, "SIGNAL"] = "SELL"

    # FINAL OUTPUT COLUMNS
    return df[[
        "INSTRUMENT", "SYMBOL", "EXPIRY_DT",
        "STRIKE_PR", "OPTION_TYP",
        "OPEN", "HIGH", "LOW", "CLOSE",
        "OPEN_INT", "CHG_IN_OI",
        "RET", "OI_PCT", "MOMENTUM",
        "SIGNAL"
    ]]
    
