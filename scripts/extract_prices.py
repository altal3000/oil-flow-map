"""
extract_prices.py
Fetches 5 years of Brent crude front-month futures (BZ=F) from Yahoo Finance.

Outputs:
    data/raw/eia_prices_raw.csv  — for train_prophet.py (backward compatibility)
    Neo4j Price nodes            — for app.py price chart

Run from project root:
    python scripts/extract_prices.py
"""

import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, UTC
from pathlib import Path
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

URI      = os.getenv("NEO4J_URI")
USER     = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

RAW_DATA_PATH = Path("data/raw")
RAW_DATA_PATH.mkdir(exist_ok=True)
OUTPUT_PATH = RAW_DATA_PATH / "eia_prices_raw.csv"
# Named eia_prices_raw.csv for backward compatibility with train_prophet.py


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

    df = pd.DataFrame({
        "period":             hist["Date"].dt.strftime("%Y-%m-%d"),
        "duoarea":            "ZEU",
        "area-name":          "NA",
        "product":            "EPCBRENT",
        "product-name":       "UK Brent Crude Oil",
        "process":            "PF4",
        "process-name":       "Front-Month Futures",
        "series":             "RBRTE",
        "series-description": "Brent Crude Front-Month Futures (Dollars per Barrel)",
        "value":              hist["Close"].round(2),
        "units":              "$/BBL",
    })

    df = df.sort_values("period", ascending=False).reset_index(drop=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Saved {len(df)} price records to {OUTPUT_PATH}")
    print(f"Date range: {df['period'].iloc[-1]} to {df['period'].iloc[0]}")
    print(f"Latest price: ${df['value'].iloc[0]}")

    return df


def write_to_neo4j(df):
    """Write price series to Neo4j as Price nodes."""
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    with driver.session() as session:
        # Clear existing price nodes
        session.run("MATCH (p:Price) DETACH DELETE p")

        # Create uniqueness constraint if not exists
        session.run("""
            CREATE CONSTRAINT price_date IF NOT EXISTS
            FOR (p:Price) REQUIRE p.date IS UNIQUE
        """)

        # Write all price records
        brent_df = df[df["series"] == "RBRTE"].copy()
        for _, row in brent_df.iterrows():
            session.run("""
                MERGE (p:Price {date: $date})
                SET p.price      = $price,
                    p.series     = 'BRENT',
                    p.updated_at = $updated_at
            """,
            date=row["period"],
            price=float(row["value"]),
            updated_at=datetime.now(UTC).isoformat())

    driver.close()
    print(f"  {len(brent_df)} Price nodes written to Neo4j")


def main():
    df = fetch_prices()
    if df is None:
        return

    print("Writing prices to Neo4j...")
    write_to_neo4j(df)
    print("Done.")


if __name__ == "__main__":
    main()
