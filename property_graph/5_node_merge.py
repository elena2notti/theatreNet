from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# === CONFIG ===
dotenv_path = "/Users/elenabinotti/Documents/scuola/unibo/LM-43 DHDK/promemoria group/env.env"
load_dotenv(dotenv_path=dotenv_path)

user = os.getenv("ID")
password = os.getenv("SECRET_KEY")
uri_db = "bolt://archiuidev.promemoriagroup.com:7687"

# === 1. FUSIONE PERSONE (Già testata) ===
cypher_merge_people = """
MATCH (p:Person)
WHERE p.wikidata_qid IS NOT NULL AND TRIM(p.wikidata_qid) <> ''
WITH p.wikidata_qid as qid, collect(p) as nodes
WHERE size(nodes) > 1
CALL apoc.refactor.mergeNodes(nodes, {
    properties: {
        source: 'combine',              // Diventa ['Regio', 'Fondazione']
        name: 'overwrite',              // Vince il primo (o quello più aggiornato)
        wikidata_qid: 'discard',        // Sono identici
        wikidata_uri: 'discard',
        internal_id_regio: 'overwrite', // Mantiene l'ID Regio
        internal_id_fondazione: 'overwrite', // Mantiene l'ID Fondazione
        trace_ids: 'combine'            // Unisce eventuali ID storici
    },
    mergeRels: true                     // Unisce le relazioni (PERFORMED_IN, etc.)
}) YIELD node
RETURN count(node) as persone_unite
"""

# === 2. FUSIONE OPERE (Già testata) ===
cypher_merge_works = """
MATCH (w:Work)
WHERE w.wikidata_qid IS NOT NULL AND TRIM(w.wikidata_qid) <> ''
WITH w.wikidata_qid as qid, collect(w) as nodes
WHERE size(nodes) > 1
CALL apoc.refactor.mergeNodes(nodes, {
    properties: {
        source: 'combine',
        title: 'overwrite',
        wikidata_qid: 'discard',
        internal_id_regio: 'overwrite',
        internal_id_fondazione: 'overwrite',
        year: 'overwrite'
    },
    mergeRels: true
}) YIELD node
RETURN count(node) as opere_unite
"""

# === 3. FUSIONE BUILDING (NUOVO - Gestito bene) ===
# Questa query fonde i teatri (es. Teatro Regio di Torino) se hanno lo stesso QID.
cypher_merge_buildings = """
MATCH (b:Building)
WHERE b.wikidata_qid IS NOT NULL AND TRIM(b.wikidata_qid) <> ''
WITH b.wikidata_qid as qid, collect(b) as nodes
WHERE size(nodes) > 1
CALL apoc.refactor.mergeNodes(nodes, {
    properties: {
        source: 'combine',               // Diventa ['Regio', 'Fondazione']
        
        // Gestione Nomi e Città
        name: 'overwrite',               // Es. "Teatro Regio"
        city: 'overwrite',               // Es. "Torino"
        address: 'overwrite',            // Se c'è l'indirizzo
        
        // Gestione ID Tecnici
        wikidata_qid: 'discard',
        internal_id_regio: 'overwrite',      // Mantiene ID Regio
        internal_id_fondazione: 'overwrite'  // Mantiene ID Fondazione
    },
    mergeRels: true  // Importante: Unisce le relazioni HELD_IN (dove si è tenuta la recita)
}) YIELD node
RETURN count(node) as edifici_uniti
"""

# === 4. PULIZIA FINALE (Rimuove eventuali SAME_AS residui) ===
cypher_clean_same_as = """
MATCH (n)-[r:SAME_AS]->(m)
DELETE r
"""

def run_reconciliation(driver):
    with driver.session() as session:
        print("\n--- INIZIO RICONCILIAZIONE (FUSIONE FISICA) ---")
        print("L'obiettivo è creare un Golden Record unico per entità condivisa.")

        # A. Persone
        print("\n1. Fusione Persone...")
        res = session.run(cypher_merge_people).single()
        print(f"   -> Persone unite: {res['persone_unite'] if res else 0}")

        # B. Opere
        print("\n2. Fusione Opere...")
        res = session.run(cypher_merge_works).single()
        print(f"   -> Opere unite: {res['opere_unite'] if res else 0}")

        # C. Edifici (Building)
        print("\n3. Fusione Edifici (Teatri/Luoghi)...")
        res = session.run(cypher_merge_buildings).single()
        print(f"   -> Edifici uniti: {res['edifici_uniti'] if res else 0}")

        # D. Pulizia SAME_AS (Sicurezza)
        print("\n4. Rimozione relazioni SAME_AS residue...")
        session.run(cypher_clean_same_as)
        print("   -> Relazioni SAME_AS rimosse.")
        
        print("\n PROCESSO COMPLETATO.")
        print("   Ora cerca 'Teatro Regio' o 'Fiorenza Cossotto': dovresti vedere un solo nodo con source=['Regio', 'Fondazione'].")

if __name__ == "__main__":
    driver = GraphDatabase.driver(uri_db, auth=(user, password))
    try:
        run_reconciliation(driver)
    except Exception as e:
        print(f"Errore: {e}")
    finally:
        driver.close()