"""
load_stability.py
Cleans World Bank Political Stability Index data and loads into Neo4j.
Adds stability_score_{year} properties to existing Country nodes.
Also creates StabilityScore nodes for time-series queries.

Run from project root:
    python scripts/load_stability.py
"""

import os
import csv
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

URI      = os.getenv("NEO4J_URI")
USER     = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

INPUT_FILE = "data/raw/wb_political_stability.csv"

# Map World Bank country names to ISO3 codes
# Handles WB naming conventions
COUNTRY_NAME_TO_ISO3 = {
    "Algeria":                    "DZA",
    "Angola":                     "AGO",
    "Azerbaijan":                 "AZE",
    "Belarus":                    "BLR",
    "Canada":                     "CAN",
    "Denmark":                    "DNK",
    "Djibouti":                   "DJI",
    "Egypt, Arab Rep.":           "EGY",
    "Ethiopia":                   "ETH",
    "Georgia":                    "GEO",
    "Hungary":                    "HUN",
    "Indonesia":                  "IDN",
    "Iran, Islamic Rep.":         "IRN",
    "Iraq":                       "IRQ",
    "Kazakhstan":                 "KAZ",
    "Kuwait":                     "KWT",
    "Libya":                      "LBY",
    "Malaysia":                   "MYS",
    "Nigeria":                    "NGA",
    "Oman":                       "OMN",
    "Pakistan":                   "PAK",
    "Panama":                     "PAN",
    "Poland":                     "POL",
    "Qatar":                      "QAT",
    "Russian Federation":         "RUS",
    "Russia":                     "RUS",
    "Saudi Arabia":               "SAU",
    "Singapore":                  "SGP",
    "Somalia":                    "SOM",
    "South Africa":               "ZAF",
    "South Sudan":                "SSD",
    "Spain":                      "ESP",
    "Sudan":                      "SDN",
    "Sweden":                     "SWE",
    "Turkey":                     "TUR",
    "Turkiye":                    "TUR",
    "Ukraine":                    "UKR",
    "United Arab Emirates":       "ARE",
    "United Kingdom":             "GBR",
    "United States":              "USA",
    "Yemen":                      "YEM",
    "Yemen, Rep.":                "YEM",
}


def parse_year(col_header):
    """Extract year integer from '2015 [YR2015]' format."""
    return int(col_header.strip().split(" ")[0])


def parse_score(val):
    """Parse score value, return None if missing."""
    val = val.strip()
    if val in ("", "..", "N/A"):
        return None
    try:
        return round(float(val), 4)
    except ValueError:
        return None


def load_csv():
    """Parse the World Bank CSV into a list of {iso3, year, score} dicts."""
    records = []
    with open(INPUT_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        year_cols = [c for c in reader.fieldnames if c.startswith("20")]

        for row in reader:
            country_name = row["Country Name"].strip()
            iso3 = COUNTRY_NAME_TO_ISO3.get(country_name)

            if not iso3:
                print(f"  WARNING: no ISO3 mapping for '{country_name}' — skipping")
                continue

            for col in year_cols:
                year = parse_year(col)
                score = parse_score(row[col])
                if score is not None:
                    records.append({
                        "iso3":  iso3,
                        "year":  year,
                        "score": score,
                    })

    return records


def load_to_neo4j(driver, records):
    """
    For each record:
    - Set stability_score_{year} on the Country node
    - MERGE a StabilityScore node linked to the Country
    """
    with driver.session() as session:
        for r in records:
            session.run("""
                MATCH (c:Country {iso3: $iso3})
                SET c[$prop] = $score
            """, iso3=r["iso3"], prop=f"stability_score_{r['year']}", score=r["score"])

            session.run("""
                MATCH (c:Country {iso3: $iso3})
                MERGE (s:StabilityScore {id: $id})
                SET s.year  = $year,
                    s.score = $score,
                    s.iso3  = $iso3
                MERGE (c)-[:HAS_STABILITY_SCORE]->(s)
            """, iso3=r["iso3"],
                 id=f"{r['iso3']}_{r['year']}",
                 year=r["year"],
                 score=r["score"])


def main():
    print(f"Loading {INPUT_FILE}...")
    records = load_csv()
    print(f"  Parsed {len(records)} country-year records")

    if not records:
        print("No records to load. Check the input file.")
        return

    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    print("Loading into Neo4j...")
    load_to_neo4j(driver, records)
    driver.close()

    print(f"\nDone. {len(records)} stability scores loaded.")
    print("\nSample records:")
    for r in records[:5]:
        print(f"  {r['iso3']} {r['year']}: {r['score']}")


if __name__ == "__main__":
    main()
