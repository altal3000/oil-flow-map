"""
extract_prices.py
Fetches 5 years of Brent crude front-month futures (BZ=F) from Yahoo Finance
and saves to data/raw/eia_prices_raw.csv in the same format as the original
EIA extract — ensuring backward compatibility with train_prophet.py.

Replaces extract_eia.py. No API key required.

Run from project root:
    python scripts/extract_prices.py
"""

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path

RAW_DATA_PATH = Path("data/raw")
RAW_DATA_PATH.mkdir(exist_ok=True)
OUTPUT_PATH = RAW_DATA_PATH / "eia_prices_raw.csv"


# Named eia_prices_raw.csv for backward compatibility with train_prophet.py and app.py



def fetch_prices():
    """Fetch 5 years of Brent front-month futures from Yahoo Finance."""
    print("Fetching Brent crude futures (BZ=F) from Yahoo Finance...")

    end_date   = datetime.today()
    start_date = end_date - timedelta(days=365 * 5)

    ticker = yf.Ticker("BZ=F")
    hist   = ticker.history(
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d"),
        interval="1d",
    )

    if hist.empty:
        print("ERROR: No data returned from Yahoo Finance.")
        return None

    hist = hist.reset_index()
    hist["Date"] = pd.to_datetime(hist["Date"]).dt.tz_localize(None)

    # Build dataframe in same column format as original EIA CSV
    # so train_prophet.py requires no changes
    df = pd.DataFrame({
        "period":           hist["Date"].dt.strftime("%Y-%m-%d"),
        "duoarea":          "ZEU",
        "area-name":        "NA",
        "product":          "EPCBRENT",
        "product-name":     "UK Brent Crude Oil",
        "process":          "PF4",
        "process-name":     "Front-Month Futures",
        "series":           "RBRTE",
        "series-description": "Brent Crude Front-Month Futures (Dollars per Barrel)",
        "value":            hist["Close"].round(2),
        "units":            "$/BBL",
    })

    df = df.sort_values("period", ascending=False).reset_index(drop=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved {len(df)} price records to {OUTPUT_PATH}")
    print(f"Date range: {df['period'].iloc[-1]} to {df['period'].iloc[0]}")
    print(f"Latest price: ${df['value'].iloc[0]}")

    return df


if __name__ == "__main__":
    fetch_prices()
    print("Price extraction complete.")
