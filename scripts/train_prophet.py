"""
train_prophet.py
Trains a Prophet model on Brent crude daily prices
and produces a 90-day forward forecast.

Outputs:
    data/processed/price_forecast.csv
    Writes PriceForecast nodes to Neo4j

Run from project root:
    python scripts/train_prophet.py
"""

import os
import pandas as pd
from datetime import datetime, UTC
from dotenv import load_dotenv
from neo4j import GraphDatabase
from prophet import Prophet

load_dotenv()

URI      = os.getenv("NEO4J_URI")
USER     = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

EIA_CSV   = "data/raw/eia_prices_raw.csv"
OUT_PATH  = "data/processed/price_forecast.csv"
FORECAST_DAYS = 90


def load_brent():
    """Load and prepare Brent price series for Prophet."""
    df = pd.read_csv(EIA_CSV, parse_dates=["period"])
    df = df[df["series"] == "RBRTE"].copy()
    df = df.rename(columns={"period": "ds", "value": "y"})
    df = df[["ds", "y"]].sort_values("ds").reset_index(drop=True)
    df = df.dropna(subset=["y"])
    print(f"  Brent data: {len(df)} trading days, "
          f"{df['ds'].min().date()} to {df['ds'].max().date()}")
    return df


def train_and_forecast(df):
    """Train Prophet and generate 90-day forecast."""
    model = Prophet(
        daily_seasonality=False,   # oil prices don't have intraday patterns
        weekly_seasonality=True,   # trading week patterns
        yearly_seasonality=True,   # annual demand cycles
        changepoint_prior_scale=0.05,  # moderate flexibility for trend changes
        interval_width=0.80,       # 80% confidence interval
    )

    model.fit(df)

    # Create future dataframe — trading days only (freq='B' = business days)
    future = model.make_future_dataframe(
        periods=FORECAST_DAYS,
        freq='B',
        include_history=False,
    )

    forecast = model.predict(future)

    # Keep relevant columns only
    forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    forecast.columns = ["date", "price_forecast", "price_lower", "price_upper"]

    # Round to 2 decimal places
    for col in ["price_forecast", "price_lower", "price_upper"]:
        forecast[col] = forecast[col].round(2)

    return forecast


def write_to_neo4j(driver, forecast):
    """Write forecast to Neo4j as PriceForecast nodes."""
    with driver.session() as session:
        # Clear existing forecast
        session.run("MATCH (pf:PriceForecast) DETACH DELETE pf")

        # Create constraint if not exists
        session.run("""
            CREATE CONSTRAINT priceForecast_date IF NOT EXISTS
            FOR (pf:PriceForecast) REQUIRE pf.date IS UNIQUE
        """)

        for _, row in forecast.iterrows():
            session.run("""
                MERGE (pf:PriceForecast {date: $date})
                SET pf.price_forecast = $price_forecast,
                    pf.price_lower    = $price_lower,
                    pf.price_upper    = $price_upper,
                    pf.series         = 'BRENT',
                    pf.created_at     = $created_at
            """,
            date=row["date"].strftime("%Y-%m-%d"),
            price_forecast=float(row["price_forecast"]),
            price_lower=float(row["price_lower"]),
            price_upper=float(row["price_upper"]),
            created_at=datetime.now(UTC).isoformat())

    print(f"  {len(forecast)} forecast nodes written to Neo4j")


def main():
    print("Loading Brent price data...")
    df = load_brent()

    print("Training Prophet model...")
    forecast = train_and_forecast(df)

    print(f"Forecast generated: {forecast['date'].min().date()} "
          f"to {forecast['date'].max().date()}")
    print("\nSample forecast (first 5 days):")
    print(forecast.head().to_string(index=False))

    print("\nSaving forecast to CSV...")
    forecast.to_csv(OUT_PATH, index=False)
    print(f"  Saved to {OUT_PATH}")

    print("\nWriting to Neo4j...")
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    write_to_neo4j(driver, forecast)
    driver.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
