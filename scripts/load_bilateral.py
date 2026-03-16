"""
load_bilateral.py
Loads 4 years of bilateral crude trade data into Neo4j.
Creates :Region nodes and :FLOW relationships with year + volume_mt properties.

Run from project root:
    python scripts/load_bilateral.py
"""

import os
import csv
from pathlib import Path
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

URI      = os.getenv("NEO4J_URI")
USER     = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

# Maps CSV column/row names → clean region IDs and display names
# Also holds approximate centroid coordinates for map rendering
REGION_MAP = {
    "Canada":                {"id": "region_canada",           "name": "Canada",                    "lat": 60.0,  "lon": -95.0},
    "Mexico":                {"id": "region_mexico",           "name": "Mexico",                    "lat": 23.6,  "lon": -102.5},
    "US":                    {"id": "region_us",               "name": "United States",             "lat": 37.1,  "lon": -95.7},
    "S. & Cent. America":    {"id": "region_s_cent_america",   "name": "S. & Cent. America",        "lat": -8.0,  "lon": -55.0},
    "Europe":                {"id": "region_europe",           "name": "Europe",                    "lat": 50.0,  "lon": 15.0},
    "Russia":                {"id": "region_russia",           "name": "Russia",                    "lat": 61.5,  "lon": 90.0},
    "Russian Federation":    {"id": "region_russia",           "name": "Russia",                    "lat": 61.5,  "lon": 90.0},
    "Other CIS":             {"id": "region_other_cis",        "name": "Other CIS",                 "lat": 48.0,  "lon": 63.0},
    "Iraq":                  {"id": "region_iraq",             "name": "Iraq",                      "lat": 33.0,  "lon": 44.0},
    "Kuwait":                {"id": "region_kuwait",           "name": "Kuwait",                    "lat": 29.3,  "lon": 47.7},
    "Saudi Arabia":          {"id": "region_saudi_arabia",     "name": "Saudi Arabia",              "lat": 23.9,  "lon": 45.1},
    "UAE":                   {"id": "region_uae",              "name": "UAE",                       "lat": 23.4,  "lon": 53.8},
    "Other Middle East":     {"id": "region_other_middle_east","name": "Other Middle East",         "lat": 29.0,  "lon": 57.0},
    "Middle East":           {"id": "region_other_middle_east","name": "Other Middle East",         "lat": 29.0,  "lon": 57.0},
    "North Africa":          {"id": "region_north_africa",     "name": "North Africa",              "lat": 25.0,  "lon": 17.0},
    "West Africa":           {"id": "region_west_africa",      "name": "West Africa",               "lat": 5.0,   "lon": 5.0},
    "East & S. Africa":      {"id": "region_east_s_africa",    "name": "East & S. Africa",          "lat": -10.0, "lon": 37.0},
    "Africa":                {"id": "region_africa",           "name": "Africa",                    "lat": 0.0,   "lon": 20.0},
    "Australasia":           {"id": "region_australasia",      "name": "Australasia",               "lat": -25.0, "lon": 133.0},
    "China":                 {"id": "region_china",            "name": "China",                     "lat": 35.0,  "lon": 105.0},
    "India":                 {"id": "region_india",            "name": "India",                     "lat": 20.0,  "lon": 78.0},
    "Japan":                 {"id": "region_japan",            "name": "Japan",                     "lat": 36.2,  "lon": 138.3},
    "Singapore":             {"id": "region_singapore",        "name": "Singapore",                 "lat": 1.35,  "lon": 103.8},
    "Other Asia Pacific":    {"id": "region_other_asia_pacific","name": "Other Asia Pacific",       "lat": 15.0,  "lon": 110.0},
}

# CSV column headers → region keys
COLUMN_MAP = {
    "to_canada":              "Canada",
    "to_mexico":              "Mexico",
    "to_us":                  "US",
    "to_s_cent_america":      "S. & Cent. America",
    "to_europe":              "Europe",
    "to_russia":              "Russia",
    "to_other_cis":           "Other CIS",
    "to_middle_east":         "Middle East",
    "to_africa":              "Africa",
    "to_australasia":         "Australasia",
    "to_china":               "China",
    "to_india":               "India",
    "to_japan":               "Japan",
    "to_singapore":           "Singapore",
    "to_other_asia_pacific":  "Other Asia Pacific",
}

FILES = {
    2021: "data/raw/bilateral/2021_bp_stats_review_2022.csv",
    2022: "data/raw/bilateral/2022_bp_stats_review_2023.csv",
    2023: "data/raw/bilateral/2023_ei_stats_review_2024.csv",
    2024: "data/raw/bilateral/2024_ei_stats_review_2025.csv",
}

# Minimum volume threshold — skip trace flows
MIN_VOLUME_MT = 0.5


def merge_regions(tx, regions):
    """MERGE all Region nodes (idempotent)."""
    for r in regions:
        tx.run("""
            MERGE (r:Region {id: $id})
            SET r.name = $name,
                r.lat  = $lat,
                r.lon  = $lon
        """, id=r["id"], name=r["name"], lat=r["lat"], lon=r["lon"])


def merge_flow(tx, from_id, to_id, year, volume_mt):
    """MERGE a FLOW relationship for a given year."""
    tx.run("""
        MATCH (from:Region {id: $from_id})
        MATCH (to:Region   {id: $to_id})
        MERGE (from)-[f:FLOW {year: $year, to: $to_id}]->(to)
        SET f.volume_mt = $volume_mt
    """, from_id=from_id, to_id=to_id, year=year, volume_mt=volume_mt)


def load_csv(path, year):
    flows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            from_key = row["from_region"].strip()

            # Skip the totals row
            if from_key.lower().startswith("total"):
                continue

            if from_key not in REGION_MAP:
                print(f"  WARNING: unknown from_region '{from_key}' — skipping")
                continue

            from_id = REGION_MAP[from_key]["id"]

            for col, to_key in COLUMN_MAP.items():
                val = row.get(col, "0").strip()
                try:
                    volume = float(val)
                except ValueError:
                    volume = 0.0

                if volume < MIN_VOLUME_MT:
                    continue

                to_id = REGION_MAP[to_key]["id"]

                # Skip self-loops
                if from_id == to_id:
                    continue

                flows.append({
                    "from_id":   from_id,
                    "to_id":     to_id,
                    "year":      year,
                    "volume_mt": volume,
                })

    return flows


def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    # Collect all unique regions across all files
    regions_to_create = list({
        r["id"]: r for k, r in REGION_MAP.items()
    }.values())

    print(f"Merging {len(regions_to_create)} Region nodes...")
    with driver.session() as session:
        session.execute_write(merge_regions, regions_to_create)
    print("  Done.")

    # Load each year
    total_flows = 0
    for year, path in FILES.items():
        p = Path(path)
        if not p.exists():
            print(f"  MISSING: {path} — skipping")
            continue

        print(f"Loading {year} from {path}...")
        flows = load_csv(p, year)
        print(f"  {len(flows)} flows above {MIN_VOLUME_MT} Mt threshold")

        with driver.session() as session:
            for flow in flows:
                session.execute_write(
                    merge_flow,
                    flow["from_id"],
                    flow["to_id"],
                    flow["year"],
                    flow["volume_mt"],
                )

        total_flows += len(flows)
        print(f"  Loaded.")

    driver.close()
    print(f"\nDone. Total flows loaded: {total_flows}")


if __name__ == "__main__":
    main()
