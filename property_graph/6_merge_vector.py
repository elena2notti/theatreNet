from neo4j import GraphDatabase, basic_auth
from dotenv import load_dotenv
import os
import sys

# =========================
# CONFIG
# =========================
dotenv_path = "/Users/elenabinotti/Documents/scuola/unibo/LM-43 DHDK/promemoria group/env.env"
load_dotenv(dotenv_path=dotenv_path)

URI = "bolt://archiuidev.promemoriagroup.com:7687"
USER = os.getenv("ID")
PASSWORD = os.getenv("SECRET_KEY")

if not USER or not PASSWORD:
    print(f"ERROR: Missing credentials from {dotenv_path}")
    sys.exit(1)

# Vector matching params
K = 5
LINK_THRESHOLD = 0.944       # threshold to CREATE SAME_AS
MERGE_THRESHOLD = 0.955      # stricter threshold to MERGE nodes

# =========================
# CYPHER QUERIES
# =========================

# 1) Create SAME_AS links (only Persons without QID, with embedding)
CYPHER_CREATE_SAME_AS = """
WITH $k AS k, $threshold AS threshold
MATCH (p:Person)
WHERE (p.wikidata_qid IS NULL OR trim(p.wikidata_qid) = '')
  AND p.embedding IS NOT NULL
CALL db.index.vector.queryNodes('person_embeddings', k, p.embedding)
YIELD node, score
WHERE node <> p
  AND id(p) < id(node)  // avoid duplicates (A->B and B->A)
  AND (node.wikidata_qid IS NULL OR trim(node.wikidata_qid) = '')
  AND node.embedding IS NOT NULL
  AND score >= threshold
MERGE (p)-[r:SAME_AS]->(node)
SET r.confidence = score,
    r.method = 'sbert_name_dates'
RETURN count(r) AS created;
"""

# 2) Merge connected components of SAME_AS above threshold into a single golden record
# Uses APOC to find connected subgraphs and merges them.
CYPHER_MERGE_SAME_AS_COMPONENTS = """
WITH $threshold AS threshold
MATCH (a:Person)-[r:SAME_AS]->(b:Person)
WHERE r.confidence >= threshold
WITH collect(DISTINCT a) + collect(DISTINCT b) AS seeds
UNWIND seeds AS seed
WITH DISTINCT seed

CALL apoc.path.subgraphAll(seed, {
  relationshipFilter: "SAME_AS",
  minLevel: 1
}) YIELD nodes AS component

WITH component
WHERE size(component) > 1

CALL apoc.refactor.mergeNodes(component, {
  properties: {
    source: 'combine',
    trace_ids: 'combine',
    name: 'overwrite',
    birth_date: 'overwrite',
    death_date: 'overwrite',
    wikidata_qid: 'overwrite',
    wikidata_uri: 'overwrite',
    internal_id_regio: 'overwrite',
    internal_id_fondazione: 'overwrite',
    embedding: 'discard'   // embeddings become stale after merge; safer to recompute later
  },
  mergeRels: true
}) YIELD node

RETURN count(node) AS merged;
"""

# 3) Cleanup SAME_AS relationships (after merges)
CYPHER_DELETE_SAME_AS = """
MATCH ()-[r:SAME_AS]->()
DELETE r
RETURN count(r) AS deleted;
"""

# Optional: remove self-loops if any remain (defensive)
CYPHER_DELETE_SAME_AS_SELF_LOOPS = """
MATCH (n)-[r:SAME_AS]->(n)
DELETE r
RETURN count(r) AS deleted_self_loops;
"""

# =========================
# RUNNER
# =========================

def run_vector_reconciliation_people():
    driver = GraphDatabase.driver(URI, auth=basic_auth(USER, PASSWORD))
    try:
        with driver.session() as session:
            print("\n--- STEP 1: Create SAME_AS links (vector similarity) ---")
            res = session.run(CYPHER_CREATE_SAME_AS, k=K, threshold=LINK_THRESHOLD).single()
            created = res["created"] if res else 0
            print(f"SAME_AS created: {created}")

            print("\n--- STEP 2: Merge SAME_AS components into Golden Records ---")
            res = session.run(CYPHER_MERGE_SAME_AS_COMPONENTS, threshold=MERGE_THRESHOLD).single()
            merged = res["merged"] if res else 0
            print(f"People merged (components): {merged}")

            print("\n--- STEP 3: Cleanup SAME_AS relationships ---")
            res = session.run(CYPHER_DELETE_SAME_AS_SELF_LOOPS).single()
            deleted_loops = res["deleted_self_loops"] if res else 0
            if deleted_loops:
                print(f"SAME_AS self-loops deleted: {deleted_loops}")

            res = session.run(CYPHER_DELETE_SAME_AS).single()
            deleted = res["deleted"] if res else 0
            print(f"SAME_AS deleted: {deleted}")

            print("\nDONE. Note: embeddings were discarded on merged nodes; recompute embeddings after merges if needed.")

    finally:
        driver.close()

if __name__ == "__main__":
    run_vector_reconciliation_people()
