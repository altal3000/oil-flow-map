import json
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(
    URI,
    auth=(USERNAME, PASSWORD)
)


def load_json(filename):
    with open(f"data/static/{filename}") as f:
        return json.load(f)


# ── NODES ───

def load_chokepoints(tx, chokepoints):
    for c in chokepoints:
        tx.run("""
            MERGE (chk:Chokepoint {id: $id})
            SET chk.name = $name,
                chk.lat = $lat,
                chk.lon = $lon,
                chk.controlling_countries = $controlling_countries,
                chk.flow_bpd = $flow_bpd,
                chk.closure_risk = $closure_risk,
                chk.risk_score = $risk_score
        """, **c)


def load_pipelines(tx, pipelines):
    for p in pipelines:
        tx.run("""
            MERGE (p:Pipeline {id: $id})
            SET p.name = $name,
                p.product = $product,
                p.capacity_bpd = $capacity_bpd,
                p.flow_bpd = $flow_bpd,
                p.operator = $operator,
                p.status = $status,
                p.strategic_role = $strategic_role,
                p.countries_traversed = $countries_traversed,
                p.lat_start = $lat_start,
                p.lon_start = $lon_start,
                p.lat_end = $lat_end,
                p.lon_end = $lon_end,
                p.risk_score = $risk_score
        """, **p)


def load_countries(tx, countries):
    for c in countries:
        tx.run("""
            MERGE (c:Country {iso3: $iso3})
            SET c.id = $id,
                c.name = $name,
                c.region = $region,
                c.lat = $lat,
                c.lon = $lon,
                c.net_position = $net_position,
                c.production_bpd = $production_bpd,
                c.consumption_bpd = $consumption_bpd,
                c.opec_member = $opec_member,
                c.exposure_score = $exposure_score,
                c.dependency_score = $dependency_score
        """, **c)


def load_terminals(tx, terminals):
    for t in terminals:
        tx.run("""
            MERGE (t:Terminal {id: $id})
            SET t.name = $name,
                t.type = $type,
                t.country_iso = $country_iso,
                t.capacity_bpd = $capacity_bpd,
                t.lat = $lat,
                t.lon = $lon
        """, **{k: v for k, v in t.items() if k != "routes_through"})


# ── RELATIONSHIPS ──

def load_country_terminal_relationships(tx, terminals):
    for t in terminals:
        tx.run("""
            MATCH (c:Country {iso3: $country_iso})
            MATCH (t:Terminal {id: $terminal_id})
            MERGE (c)-[:HAS_TERMINAL]->(t)
        """, country_iso=t["country_iso"], terminal_id=t["id"])


def load_terminal_chokepoint_relationships(tx, terminals):
    for t in terminals:
        for chk_id in t["routes_through"]:
            tx.run("""
                MATCH (t:Terminal {id: $terminal_id})
                MATCH (chk:Chokepoint {id: $chk_id})
                MERGE (t)-[:ROUTES_THROUGH]->(chk)
            """, terminal_id=t["id"], chk_id=chk_id)


def load_pipeline_country_relationships(tx, pipelines):
    for p in pipelines:
        countries = p["countries_traversed"]
        if len(countries) >= 1:
            tx.run("""
                MATCH (origin:Country {iso3: $origin_iso})
                MATCH (pipe:Pipeline {id: $pipeline_id})
                MERGE (origin)-[:EXPORTS_VIA]->(pipe)
            """, origin_iso=countries[0], pipeline_id=p["id"])
        if len(countries) >= 2:
            tx.run("""
                MATCH (dest:Country {iso3: $dest_iso})
                MATCH (pipe:Pipeline {id: $pipeline_id})
                MERGE (pipe)-[:DELIVERS_TO]->(dest)
            """, dest_iso=countries[-1], pipeline_id=p["id"])


def load_terminal_country_import_relationships(tx, terminals):
    for t in terminals:
        if t["type"] == "import":
            tx.run("""
                MATCH (t:Terminal {id: $terminal_id})
                MATCH (c:Country {iso3: $country_iso})
                MERGE (t)-[:IMPORTS_TO]->(c)
            """, terminal_id=t["id"], country_iso=t["country_iso"])


# ── MAIN ──

def main():
    chokepoints = load_json("chokepoints.json")["chokepoints"]
    pipelines = load_json("pipelines.json")["pipelines"]
    countries = load_json("countries.json")["countries"]
    terminals = load_json("terminals.json")["terminals"]

    with driver.session() as session:
        print("Loading chokepoints...")
        session.execute_write(load_chokepoints, chokepoints)

        print("Loading pipelines...")
        session.execute_write(load_pipelines, pipelines)

        print("Loading countries...")
        session.execute_write(load_countries, countries)

        print("Loading terminals...")
        session.execute_write(load_terminals, terminals)

        print("Creating country -> terminal relationships...")
        session.execute_write(load_country_terminal_relationships, terminals)

        print("Creating terminal -> chokepoint relationships...")
        session.execute_write(load_terminal_chokepoint_relationships, terminals)

        print("Creating pipeline -> country relationships...")
        session.execute_write(load_pipeline_country_relationships, pipelines)

        print("Creating terminal -> country import relationships...")
        session.execute_write(load_terminal_country_import_relationships, terminals)

    driver.close()
    print("Done. All static data loaded into Neo4j.")


if __name__ == "__main__":
    main()
