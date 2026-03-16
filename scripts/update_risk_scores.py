"""
update_risk_scores.py
Pulls news headlines from NewsData.io for each chokepoint/pipeline,
scores sentiment using VADER, and updates the Strategic Exposure Index
in Neo4j.

Runs daily via GitHub Actions.

Strategic Exposure Index:
    Measures the importance of a point in global oil flows combined with
    the political instability of the region it operates in. A high score
    indicates a point where disruption would have significant global impact
    and where conditions exist that could escalate to disruption.

    This is a structural exposure measure, not a probability of disruption.

Weighting:
    news available:  80% static_exposure_score + 20% sentiment_score
    no news:         100% static_exposure_score (sentiment contributes nothing)

Run from project root:
    python scripts/update_risk_scores.py
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, UTC
from dotenv import load_dotenv
from neo4j import GraphDatabase
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

load_dotenv()

NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY")
URI              = os.getenv("NEO4J_URI")
USER             = os.getenv("NEO4J_USERNAME")
PASSWORD         = os.getenv("NEO4J_PASSWORD")

NEWS_URL = "https://newsdata.io/api/1/news"

# One focused query per entity — keeps API usage within free tier (200/day)
SEARCH_QUERIES = {
    "chk_hormuz":            "Strait of Hormuz oil",
    "chk_malacca":           "Strait of Malacca shipping",
    "chk_suez":              "Suez Canal shipping",
    "chk_bab":               "Bab el-Mandeb Red Sea",
    "chk_bosphorus":         "Bosphorus strait oil",
    "chk_danish":            "Danish straits Baltic oil",
    "chk_gibraltar":         "Strait of Gibraltar oil",
    "chk_panama":            "Panama Canal oil tanker",
    "chk_cape":              "Cape of Good Hope tanker",
    "chk_lombok":            "Lombok Strait shipping",
    "pipe_druzhba":          "Druzhba pipeline",
    "pipe_btc":              "BTC pipeline Azerbaijan",
    "pipe_kirkuk_ceyhan":    "Kirkuk Ceyhan pipeline Iraq",
    "pipe_espo":             "ESPO pipeline Russia",
    "pipe_sumed":            "SUMED pipeline Egypt",
    "pipe_cpc":              "CPC pipeline Kazakhstan",
    "pipe_petroline":        "Petroline Saudi Arabia",
    "pipe_baku_supsa":       "Baku Supsa pipeline",
    "pipe_habshan_fujairah": "Habshan Fujairah pipeline UAE",
    "pipe_keystone":         "Keystone pipeline Canada",
}

W_STATIC    = 0.80
W_SENTIMENT = 0.20


def get_driver():
    return GraphDatabase.driver(URI, auth=(USER, PASSWORD))


def fetch_headlines(query):
    params = {
        "apikey":   NEWSDATA_API_KEY,
        "q":        query,
        "language": "en",
        "category": "business,politics,world",
    }
    try:
        resp = requests.get(NEWS_URL, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        print(f"    Error for '{query}': {e}")
        return []


def score_article(analyzer, article):
    text = ""
    if article.get("title"):
        text += article["title"] + ". "
    if article.get("description"):
        text += article["description"]
    text = text.strip()
    if not text:
        return None
    return analyzer.polarity_scores(text)["compound"]


def compute_sentiment_score(analyzer, articles):
    """
    Score sentiment from a list of articles.
    Inverts compound score so negative news → higher exposure contribution.
    Returns None if no articles.
    """
    scores = []
    for article in articles:
        score = score_article(analyzer, article)
        if score is not None:
            scores.append(score)
    if not scores:
        return None
    mean_compound     = sum(scores) / len(scores)
    risk_contribution = -mean_compound
    return round((risk_contribution + 1) / 2, 4)


def compute_exposure_score(static, sentiment):
    """
    Combine static exposure score with sentiment nudge.
    Sentiment can move the score by at most ±0.05 given 80/20 weighting.
    Falls back to static if no sentiment available.
    """
    if sentiment is None:
        return static
    return round(W_STATIC * static + W_SENTIMENT * sentiment, 4)


def fetch_static_scores():
    scores = {}
    driver = get_driver()
    with driver.session() as session:
        for label in ["Chokepoint", "Pipeline"]:
            result = session.run(f"""
                MATCH (n:{label})
                WHERE n.id IS NOT NULL
                RETURN n.id as id, 
                        COALESCE(n.static_vulnerability_score, n.static_risk_score) as score
            """)
            for r in result:
                scores[r["id"]] = r["score"] or 0.5
    driver.close()
    return scores


def update_neo4j(entity_id, sentiment_score, exposure_score, is_pipeline=False):
    label  = "Pipeline" if is_pipeline else "Chokepoint"
    driver = get_driver()
    with driver.session() as session:
        session.run(f"""
            MATCH (n:{label} {{id: $id}})
            SET n.sentiment_score    = $sentiment_score,
                n.exposure_score     = $exposure_score,
                n.risk_score         = $exposure_score,
                n.score_updated_at   = $updated_at
        """, id=entity_id,
             sentiment_score=float(sentiment_score) if sentiment_score is not None else None,
             exposure_score=float(exposure_score),
             updated_at=datetime.now(UTC).isoformat())
    driver.close()


def main():
    print(f"Updating Strategic Exposure Index — "
          f"{datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}")

    analyzer = SentimentIntensityAnalyzer()

    print("Fetching static exposure scores from Neo4j...")
    static_scores = fetch_static_scores()
    print(f"  {len(static_scores)} entities found")

    results = []

    for entity_id, query in SEARCH_QUERIES.items():
        is_pipeline = entity_id.startswith("pipe_")
        print(f"  {entity_id}...", end=" ")

        articles       = fetch_headlines(query)
        sentiment      = compute_sentiment_score(analyzer, articles)
        static         = static_scores.get(entity_id, 0.5)
        exposure_score = compute_exposure_score(static, sentiment)

        time.sleep(2)  # respect rate limit — 20 entities × 1 request = 20 req/run

        if sentiment is not None:
            delta = round(exposure_score - static, 4)
            sign  = "+" if delta >= 0 else ""
            print(f"sentiment={sentiment:.4f} → {exposure_score:.4f} "
                  f"({sign}{delta:.4f} vs static)")
        else:
            print(f"no news → {exposure_score:.4f} (static)")

        update_neo4j(entity_id, sentiment, exposure_score, is_pipeline)

        results.append({
            "id":             entity_id,
            "static_score":   static,
            "sentiment_score": sentiment,
            "exposure_score": exposure_score,
            "updated_at":     datetime.now(UTC).isoformat(),
        })

    df = pd.DataFrame(results)
    os.makedirs("data/processed", exist_ok=True)
    df.to_csv("data/processed/risk_scores.csv", index=False)

    print("\nStrategic Exposure Index:")
    print(df[["id", "static_score", "sentiment_score", "exposure_score"]]
          .sort_values("exposure_score", ascending=False)
          .to_string(index=False))


if __name__ == "__main__":
    main()
