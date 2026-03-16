"""
transform.py
Builds the static component of the Strategic Vulnerability Index
for chokepoints and pipelines.

Writes static_vulnerability_score to Neo4j — this is the base score.
Final exposure_score (static + sentiment) is calculated by update_risk_scores.py.

Strategic Vulnerability Index measures:
    "If disruption happens here, how serious are the consequences?"
    = flow importance × regional instability context

Formula (chokepoints):
    static_vulnerability_score = 0.80 * flow_norm + 0.20 * instability_norm

Formula (pipelines):
    static_vulnerability_score = instability_norm
    (no flow data available for pipelines on free sources)

Where:
    flow_norm:        normalised total oil flow through the chokepoint (mbpd, 2024)
    instability_norm: normalised inverted WB Political Stability score
                      of controlling countries (2023, most recent available)

Outputs:
    data/processed/chokepoint_vulnerability.csv
    data/processed/pipeline_vulnerability.csv
    Writes static_vulnerability_score to Neo4j chokepoint/pipeline nodes

Run from project root:
    python scripts/transform.py
"""

import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from neo4j import GraphDatabase
from pathlib import Path

load_dotenv()

URI      = os.getenv("NEO4J_URI")
USER     = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(exist_ok=True)

# Flow pairs per chokepoint — (exporting region, importing region)
# Based on 2024 bilateral flow data
CHOKEPOINT_FLOW_MAP = {
    "chk_hormuz": [
        ("region_saudi_arabia",       "region_china"),
        ("region_saudi_arabia",       "region_india"),
        ("region_saudi_arabia",       "region_japan"),
        ("region_saudi_arabia",       "region_other_asia_pacific"),
        ("region_iraq",               "region_china"),
        ("region_iraq",               "region_india"),
        ("region_iraq",               "region_other_asia_pacific"),
        ("region_kuwait",             "region_china"),
        ("region_kuwait",             "region_india"),
        ("region_kuwait",             "region_japan"),
        ("region_kuwait",             "region_other_asia_pacific"),
        ("region_uae",                "region_china"),
        ("region_uae",                "region_india"),
        ("region_uae",                "region_japan"),
        ("region_uae",                "region_other_asia_pacific"),
        ("region_other_middle_east",  "region_china"),
        ("region_other_middle_east",  "region_india"),
        ("region_other_middle_east",  "region_japan"),
        ("region_other_middle_east",  "region_other_asia_pacific"),
    ],
    "chk_malacca": [
        ("region_saudi_arabia",       "region_china"),
        ("region_saudi_arabia",       "region_japan"),
        ("region_saudi_arabia",       "region_other_asia_pacific"),
        ("region_iraq",               "region_china"),
        ("region_iraq",               "region_other_asia_pacific"),
        ("region_uae",                "region_china"),
        ("region_uae",                "region_japan"),
        ("region_uae",                "region_other_asia_pacific"),
        ("region_other_middle_east",  "region_china"),
        ("region_other_middle_east",  "region_other_asia_pacific"),
        ("region_west_africa",        "region_china"),
        ("region_west_africa",        "region_other_asia_pacific"),
        ("region_s_cent_america",     "region_china"),
    ],
    "chk_suez": [
        ("region_saudi_arabia",       "region_europe"),
        ("region_iraq",               "region_europe"),
        ("region_uae",                "region_europe"),
        ("region_other_middle_east",  "region_europe"),
        ("region_north_africa",       "region_europe"),
        ("region_east_s_africa",      "region_europe"),
        ("region_russia",             "region_europe"),
    ],
    "chk_bab": [
        ("region_saudi_arabia",       "region_europe"),
        ("region_iraq",               "region_europe"),
        ("region_uae",                "region_europe"),
        ("region_other_middle_east",  "region_europe"),
        ("region_east_s_africa",      "region_europe"),
        ("region_saudi_arabia",       "region_china"),
        ("region_saudi_arabia",       "region_india"),
        ("region_uae",                "region_china"),
        ("region_uae",                "region_india"),
    ],
    "chk_bosphorus": [
        ("region_russia",             "region_europe"),
        ("region_other_cis",          "region_europe"),
        ("region_russia",             "region_other_asia_pacific"),
    ],
    "chk_danish": [
        ("region_russia",             "region_europe"),
        ("region_other_cis",          "region_europe"),
    ],
    "chk_gibraltar": [
        ("region_west_africa",        "region_europe"),
        ("region_north_africa",       "region_europe"),
        ("region_s_cent_america",     "region_europe"),
        ("region_us",                 "region_europe"),
    ],
    "chk_panama": [
        ("region_us",                 "region_other_asia_pacific"),
        ("region_us",                 "region_china"),
        ("region_s_cent_america",     "region_china"),
        ("region_s_cent_america",     "region_other_asia_pacific"),
    ],
    "chk_cape": [
        ("region_west_africa",        "region_china"),
        ("region_west_africa",        "region_other_asia_pacific"),
        ("region_east_s_africa",      "region_china"),
    ],
    # chk_lombok: no flow assigned — instability only
}

# Countries controlling each chokepoint
CHOKEPOINT_COUNTRIES = {
    "chk_hormuz":    ["IRN", "OMN"],
    "chk_malacca":   ["MYS", "SGP", "IDN"],
    "chk_suez":      ["EGY"],
    "chk_bab":       ["DJI", "YEM"],
    "chk_bosphorus": ["TUR"],
    "chk_danish":    ["DNK", "SWE"],
    "chk_gibraltar": ["GBR", "ESP"],
    "chk_panama":    ["PAN"],
    "chk_cape":      ["ZAF"],
    "chk_lombok":    ["IDN"],
}

# Countries controlling each pipeline
PIPELINE_COUNTRIES = {
    "pipe_druzhba":          ["RUS", "BLR", "UKR", "POL", "HUN"],
    "pipe_btc":              ["AZE", "GEO", "TUR"],
    "pipe_cpc":              ["KAZ", "RUS"],
    "pipe_kirkuk_ceyhan":    ["IRQ", "TUR"],
    "pipe_sumed":            ["EGY"],
    "pipe_petroline":        ["SAU"],
    "pipe_habshan_fujairah": ["ARE"],
    "pipe_espo":             ["RUS"],
    "pipe_baku_supsa":       ["AZE", "GEO"],
    "pipe_keystone":         ["CAN", "USA"],
}


def get_driver():
    return GraphDatabase.driver(URI, auth=(USER, PASSWORD))


def fetch_flows(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (from:Region)-[f:FLOW]->(to:Region)
            WHERE f.year = 2024
            RETURN from.id as from_id, to.id as to_id, f.volume_mt as volume_mt
        """)
        return pd.DataFrame([dict(r) for r in result])


def fetch_stability(driver):
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Country)
            WHERE c.iso3 IS NOT NULL AND c.stability_score_2023 IS NOT NULL
            RETURN c.iso3 as iso3, c.stability_score_2023 as score
        """)
        return {r["iso3"]: r["score"] for r in result}


def compute_flow(flow_df, chk_id):
    pairs = CHOKEPOINT_FLOW_MAP.get(chk_id, [])
    total = 0.0
    for from_id, to_id in pairs:
        match = flow_df[
            (flow_df["from_id"] == from_id) &
            (flow_df["to_id"] == to_id)
        ]
        if not match.empty:
            total += match["volume_mt"].values[0]
    # Convert million tonnes/year to million bpd
    return round(total * 7.33 / 365, 3)


def compute_instability(stability, iso3_list):
    scores = [stability[iso3] for iso3 in iso3_list if iso3 in stability]
    if not scores:
        return None
    return round(-np.mean(scores), 4)


def normalise(series):
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - mn) / (mx - mn)


def build_index(flow_df, stability):
    # ── Chokepoints ───────────────────────────────────────────────────────────
    chk_rows = []
    for chk_id, countries in CHOKEPOINT_COUNTRIES.items():
        chk_rows.append({
            "id":          chk_id,
            "flow_mbpd":   compute_flow(flow_df, chk_id),
            "instability": compute_instability(stability, countries),
        })

    chk_df      = pd.DataFrame(chk_rows)
    median_inst = chk_df["instability"].median()
    chk_df["instability"] = chk_df["instability"].fillna(median_inst)
    chk_df["flow_norm"]        = normalise(chk_df["flow_mbpd"])
    chk_df["instability_norm"] = normalise(chk_df["instability"])
    chk_df["static_vulnerability_score"] = (
        0.80 * chk_df["flow_norm"] +
        0.20 * chk_df["instability_norm"]
    ).round(4)

    # ── Pipelines — instability only (no free flow data available) ────────────
    pipe_rows = []
    for pipe_id, countries in PIPELINE_COUNTRIES.items():
        pipe_rows.append({
            "id":          pipe_id,
            "instability": compute_instability(stability, countries),
        })

    pipe_df = pd.DataFrame(pipe_rows)
    pipe_df["instability"] = pipe_df["instability"].fillna(median_inst)
    pipe_df["instability_norm"] = normalise(pipe_df["instability"])
    pipe_df["static_vulnerability_score"] = pipe_df["instability_norm"].round(4)

    return chk_df, pipe_df


def write_to_neo4j(driver, chk_df, pipe_df):
    with driver.session() as session:
        for _, row in chk_df.iterrows():
            session.run("""
                MATCH (c:Chokepoint {id: $id})
                SET c.static_vulnerability_score = $score,
                    c.static_risk_score          = $score,
                    c.flow_mbpd                  = $flow_mbpd,
                    c.instability                = $instability
            """, id=row["id"],
                 score=float(row["static_vulnerability_score"]),
                 flow_mbpd=float(row["flow_mbpd"]),
                 instability=float(row["instability"]))

        for _, row in pipe_df.iterrows():
            session.run("""
                MATCH (p:Pipeline {id: $id})
                SET p.static_vulnerability_score = $score,
                    p.static_risk_score          = $score,
                    p.instability                = $instability
            """, id=row["id"],
                 score=float(row["static_vulnerability_score"]),
                 instability=float(row["instability"]))


def main():
    print("Connecting to Neo4j...")
    driver = get_driver()

    print("Fetching 2024 flow data...")
    flow_df = fetch_flows(driver)
    print(f"  {len(flow_df)} flow records")

    print("Fetching stability scores...")
    stability = fetch_stability(driver)
    print(f"  {len(stability)} countries")

    print("Building Strategic Vulnerability Index...")
    chk_df, pipe_df = build_index(flow_df, stability)

    print("Writing static_vulnerability_score to Neo4j...")
    write_to_neo4j(driver, chk_df, pipe_df)
    driver.close()

    chk_df.to_csv(OUT_DIR / "chokepoint_vulnerability.csv", index=False)
    pipe_df.to_csv(OUT_DIR / "pipeline_vulnerability.csv", index=False)

    print("\nChokepoint Strategic Vulnerability Scores:")
    print(chk_df[["id", "flow_mbpd", "instability", "static_vulnerability_score"]]
          .sort_values("static_vulnerability_score", ascending=False)
          .to_string(index=False))

    print("\nPipeline Strategic Vulnerability Scores:")
    print(pipe_df[["id", "instability", "static_vulnerability_score"]]
          .sort_values("static_vulnerability_score", ascending=False)
          .to_string(index=False))


if __name__ == "__main__":
    main()
