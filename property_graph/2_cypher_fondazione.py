from neo4j import GraphDatabase
from dotenv import load_dotenv
import os, sys, traceback

dotenv_path = "/Users/elenabinotti/Documents/scuola/unibo/LM-43 DHDK/promemoria group/env.env"
load_dotenv(dotenv_path=dotenv_path)

user = os.getenv("ID")
password = os.getenv("SECRET_KEY")
uri_db = "bolt://archiuidev.promemoriagroup.com:7687"

FILE_FONDAZIONE_OPERE = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/fondazione-iteatri-opere-musicali-wiki-reconciled.csv'
FILE_FONDAZIONE_PERSONE = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/persone.csv' 
FILE_FONDAZIONE_STAGIONI = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/stagioni.csv'
FILE_FONDAZIONE_PRODUZIONI = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/produzioni_clean.csv'
FILE_FONDAZIONE_RECITE = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/recite_fondazione_con_qid.csv'
FILE_FONDAZIONE_LINKS = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/20251125_fondazione-iteatri-export-produzione-recite.csv'

def execute_cypher_script(tx, script):
    result = tx.run(script)
    try:
        summary = result.single().value()
    except AttributeError:
        summary = "Nessun risultato restituito."
    return summary

def run_import_step(driver, command, step_name):
    with driver.session() as session:
        print(f"\n--- Inizio: {step_name} ---")
        try:
            result_summary = session.execute_write(execute_cypher_script, command)
            print(f"SUCCESSO: {step_name} completato.")
            print(f"Risultati: {result_summary}")
        except Exception as e:
            print(f"ERRORE CRITICO in {step_name}: {e}")
            print(">>> Il processo continua con lo step successivo...")

def create_constraints_fondazione(driver):
    print("\n--- 0.1 CREAZIONE VINCOLI (ENGLISH) ---")
    with driver.session() as session:
        constraints = [
            "CREATE CONSTRAINT person_internal_id_fond_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.internal_id_fondazione IS UNIQUE",
            "CREATE CONSTRAINT work_internal_id_fondazione_unique IF NOT EXISTS FOR (o:Work) REQUIRE o.internal_id_fondazione IS UNIQUE",
            "CREATE CONSTRAINT id_code_unique IF NOT EXISTS FOR (i:ID) REQUIRE i.code IS UNIQUE",
            "CREATE CONSTRAINT season_internal_id_fondazione_unique IF NOT EXISTS FOR (s:Season) REQUIRE s.internal_id_fondazione IS UNIQUE",
            "CREATE CONSTRAINT production_internal_id_fondazione_unique IF NOT EXISTS FOR (p:Production) REQUIRE p.internal_id_fondazione IS UNIQUE",
            "CREATE CONSTRAINT performance_internal_id_fondazione_unique IF NOT EXISTS FOR (r:Performance) REQUIRE r.internal_id_fondazione IS UNIQUE",
            "CREATE CONSTRAINT ensemble_internal_id_fondazione_unique IF NOT EXISTS FOR (e:Ensemble) REQUIRE e.internal_id_fondazione IS UNIQUE",
            "CREATE CONSTRAINT building_id_fondazione_unique IF NOT EXISTS FOR (b:Building) REQUIRE b.internal_id_fondazione IS UNIQUE"
        ]
        for c in constraints:
            try:
                session.run(c).consume()
            except Exception:
                pass
        print("Vincoli verificati.")

# 1. Importazione Persone
cypher_import_persone = f"""
LOAD CSV WITH HEADERS FROM '{FILE_FONDAZIONE_PERSONE}' AS row 
FIELDTERMINATOR ',' 
WITH row WHERE row.id IS NOT NULL AND TRIM(row.id) <> ''

MERGE (p:Person {{internal_id_fondazione: row.id}})
ON CREATE SET 
    p.name = row.dcTitle,
    p.wikidata_qid = row.entity,
    p.wikidata_uri = row.uri,
    p.source = 'Fondazione'
ON MATCH SET
    p.wikidata_qid = row.entity,
    p.wikidata_uri = row.uri

// CREAZIONE NODO ID
MERGE (id_node:ID {{code: 'fondazione_' + row.id}})
ON CREATE SET id_node.source = 'Fondazione'
MERGE (id_node)-[:IS_ID_OF]->(p)

RETURN count(p) as persone_aggiornate
"""

# 2. Importazione Opere
cypher_import_opere = f"""
LOAD CSV WITH HEADERS FROM '{FILE_FONDAZIONE_OPERE}' AS row FIELDTERMINATOR ','
WITH row WHERE row.id IS NOT NULL AND TRIM(row.id) <> ''

MERGE (o:Work {{internal_id_fondazione: row.id}})
ON CREATE SET o.title = row.dcTitle, o.wikidata_qid = row.entity_id, o.source = 'Fondazione'

// CREAZIONE NODO ID
MERGE (id_node:ID {{code: 'fondazione_' + row.id}})
ON CREATE SET id_node.source = 'Fondazione'
MERGE (id_node)-[:IS_ID_OF]->(o)

// Collegamento persone (Crea stub se non esistono, o collega a quelle esistenti)
WITH o, row
WHERE row.persone_collegate IS NOT NULL
UNWIND SPLIT(row.persone_collegate, ',') AS path
WITH o, SPLIT(SPLIT(path, '(')[1], ')')[0] AS p_id
WHERE p_id IS NOT NULL AND TRIM(p_id) <> ''

MERGE (p:Person {{internal_id_fondazione: p_id}})
ON CREATE SET p.source = 'Fondazione' // Setta solo se creato nuovo
MERGE (p)-[:HAD_ROLE_IN {{source: 'Fondazione'}}]->(o)

RETURN count(o)
"""

# 3. Produzioni
cypher_import_produzioni = f"""
LOAD CSV WITH HEADERS FROM '{FILE_FONDAZIONE_PRODUZIONI}' AS row 
FIELDTERMINATOR ';' 
WITH row WHERE row.id IS NOT NULL AND TRIM(row.id) <> ''

// --- 1. CREAZIONE PRODUZIONE E ID ---
MERGE (p:Production {{internal_id_fondazione: row.id}})

// Creazione ID
MERGE (id_node:ID {{code: 'fondazione_' + row.id}})
ON CREATE SET id_node.source = 'Fondazione'
MERGE (id_node)-[:IS_ID_OF]->(p)

// Set proprietà base
SET
    p.title = row.dcTitle,
    p.start_date = row.from,
    p.end_date = row.to,
    p.source = 'Fondazione',
    p.city = row.luogo_rappresentazione,
    p.venue = row.edificio_rappresentazione

// --- 2. COLLEGAMENTO OPERE ---
WITH p, row
UNWIND SPLIT(COALESCE(row.opere_collegate_id, ''), ',') AS work_id_raw
WITH p, row, TRIM(work_id_raw) AS work_id
WHERE work_id <> ''

MERGE (w:Work {{internal_id_fondazione: work_id}})
ON CREATE SET w.source = 'Fondazione'
MERGE (p)-[:RELATED_TO_WORK]->(w)
MERGE (w)-[:RELATES_TO]->(p)

// --- 3. COLLEGAMENTO PERSONE ---
WITH p, row
WHERE row.persone_collegate_id IS NOT NULL AND TRIM(row.persone_collegate_id) <> ''

WITH p, 
     SPLIT(row.persone_collegate_id, ',') AS ids, 
     SPLIT(COALESCE(row.persone_collegate_ruolo, ''), ',') AS roles

UNWIND range(0, size(ids)-1) AS i
WITH p, TRIM(ids[i]) AS pid, TRIM(roles[i]) AS role_text
WHERE pid IS NOT NULL AND pid <> ''

MERGE (per:Person {{internal_id_fondazione: pid}})
ON CREATE SET per.source = 'Fondazione'

// Logica Ruoli
WITH p, per, role_text,
     CASE
        WHEN role_text CONTAINS 'Regista' OR role_text CONTAINS 'regia' THEN 'DIRECTED'
        WHEN role_text CONTAINS 'Scenografo' OR role_text CONTAINS 'scene' THEN 'DESIGNED_SET'
        WHEN role_text CONTAINS 'Coreografo' OR role_text CONTAINS 'coreografia' THEN 'CHOREOGRAPHED'
        WHEN role_text CONTAINS 'Costumista' OR role_text CONTAINS 'costumi' THEN 'DESIGNED_COSTUMES'
        ELSE 'HAD_ROLE_IN'
     END AS relation_type

FOREACH(ignore IN CASE WHEN relation_type = 'DIRECTED' THEN [1] ELSE [] END | MERGE (per)-[:DIRECTED]->(p))
FOREACH(ignore IN CASE WHEN relation_type = 'DESIGNED_SET' THEN [1] ELSE [] END | MERGE (per)-[:DESIGNED_SET]->(p))
FOREACH(ignore IN CASE WHEN relation_type = 'CHOREOGRAPHED' THEN [1] ELSE [] END | MERGE (per)-[:CHOREOGRAPHED]->(p))
FOREACH(ignore IN CASE WHEN relation_type = 'DESIGNED_COSTUMES' THEN [1] ELSE [] END | MERGE (per)-[:DESIGNED_COSTUMES]->(p))

FOREACH(ignore IN CASE WHEN relation_type = 'HAD_ROLE_IN' THEN [1] ELSE [] END | 
    MERGE (per)-[r:HAD_ROLE_IN]->(p) 
    SET r.role = role_text
)

RETURN count(distinct p) as total_productions
"""

# 4. Recite
cypher_import_recite = f"""
LOAD CSV WITH HEADERS FROM '{FILE_FONDAZIONE_RECITE}' AS row FIELDTERMINATOR ','
WITH row WHERE row.id IS NOT NULL AND TRIM(row.id) <> ''

// 1. Creazione Performance
MERGE (r:Performance {{internal_id_fondazione: row.id}})
ON CREATE SET 
    r.title = row.titolo_breve,
    r.date = row.from,
    r.venue = row.luogo_nome,
    r.building_text = row.edificio_nome,
    r.source = 'Fondazione'
    
// 2. Nodo ID
MERGE (id_node:ID {{code: 'fondazione_' + row.id}})
ON CREATE SET id_node.source = 'Fondazione'
MERGE (id_node)-[:IS_ID_OF]->(r)

// 3. Gestione Building
WITH r, row
WHERE row.edificio_id IS NOT NULL AND TRIM(row.edificio_id) <> ''
MERGE (b:Building {{internal_id_fondazione: row.edificio_id}})
ON CREATE SET 
    b.name = row.edificio_nome,
    b.city = row.luogo_nome,
    b.wikidata_qid = row.entity,
    b.wikidata_uri = row.uri,
    b.source = 'Fondazione'
ON MATCH SET
    b.wikidata_qid = CASE WHEN row.entity IS NOT NULL AND row.entity <> '' THEN row.entity ELSE b.wikidata_qid END
MERGE (r)-[:HELD_IN]->(b)

// 4. Collega Opera (Work) - Prima connessione
WITH r, row
WHERE row.composizione_id IS NOT NULL AND TRIM(row.composizione_id) <> ''
MERGE (o:Work {{internal_id_fondazione: row.composizione_id}})
MERGE (r)-[:RELATED_TO_WORK]->(o)
MERGE (o)-[:RELATES_TO]->(r)

// 5. Direttore (Conductor)
// Qui usiamo WITH r, row -> La variabile 'o' viene persa qui, ma va bene così
WITH r, row
WHERE row.curatore_id IS NOT NULL AND TRIM(row.curatore_id) <> ''
MERGE (cur:Person {{internal_id_fondazione: row.curatore_id}})
ON CREATE SET cur.name = row.curatore_nome, cur.source = 'Fondazione'
MERGE (cur)-[:CONDUCTED]->(r)

// 6. Esecutore (Ensemble)
WITH r, row
WHERE row.esecutore_id IS NOT NULL AND TRIM(row.esecutore_id) <> ''
MERGE (esec:Ensemble {{internal_id_fondazione: row.esecutore_id}})
ON CREATE SET esec.name = row.esecutore_nome, esec.source = 'Fondazione'
MERGE (esec)-[rel:PARTICIPATED_IN]->(r)
SET rel.role = row.esecutore_ruolo

// 7. Interprete (Person)
WITH r, row
WHERE row.interprete_id IS NOT NULL AND TRIM(row.interprete_id) <> ''
MERGE (int:Person {{internal_id_fondazione: row.interprete_id}})
ON CREATE SET int.name = row.interprete, int.source = 'Fondazione'

MERGE (int)-[rel_int:PERFORMED_IN]->(r)
FOREACH (_ IN CASE WHEN row.ruolo IS NOT NULL AND TRIM(row.ruolo) <> '' THEN [1] ELSE [] END |
    SET rel_int.role = row.ruolo
)

// 8. GESTIONE PERSONAGGI (Character)
// Fix: Non chiediamo 'o' nel WITH, la recuperiamo sotto
WITH r, row, int
WHERE row.personaggio IS NOT NULL AND TRIM(row.personaggio) <> ''

MERGE (char:Character {{name: row.personaggio}})
ON CREATE SET char.voice_type = row.personaggio_voce, char.source = 'Fondazione'

// Triangolo
MERGE (int)-[:INTERPRETED]->(char)
MERGE (char)-[:APPEARED_IN]->(r)

// *** FIX RECUPERO OPERA ***
// Recuperiamo l'Opera usando l'ID nella riga, così siamo sicuri di averla
WITH char, row
WHERE row.composizione_id IS NOT NULL AND TRIM(row.composizione_id) <> ''
MATCH (o_final:Work {{internal_id_fondazione: row.composizione_id}})

// Creiamo il collegamento Opera -> Personaggio (Richiesta Supervisor)
MERGE (o_final)-[:HAS_CHARACTER]->(char)

RETURN count(distinct char)
"""

# 4.5 Collegamento Produzione -> Recite (HAS_PERFORMANCE)
cypher_link_produzioni_recite = f"""
LOAD CSV WITH HEADERS FROM '{FILE_FONDAZIONE_LINKS}' AS row
FIELDTERMINATOR ';'
WITH row
WHERE row.id IS NOT NULL AND row.recite_collegate IS NOT NULL AND TRIM(row.recite_collegate) <> ''

// Trova Produzione
MATCH (p:Production {{internal_id_fondazione: row.id}})

// Trova e collega le Recite
UNWIND SPLIT(row.recite_collegate, ',') AS path_raw
WITH p, path_raw, SPLIT(path_raw, '(') AS parts
WITH p, SPLIT(parts[-1], ')')[0] AS recita_id
WHERE recita_id IS NOT NULL AND TRIM(recita_id) <> ''

MATCH (r:Performance {{internal_id_fondazione: TRIM(recita_id)}})
MERGE (p)-[:HAS_PERFORMANCE]->(r)

RETURN count(*) as links_created
"""

# 5. Stagioni
cypher_import_stagioni = f"""
LOAD CSV WITH HEADERS FROM '{FILE_FONDAZIONE_STAGIONI}' AS row FIELDTERMINATOR ','
WITH row WHERE row.id IS NOT NULL AND TRIM(row.id) <> ''

MERGE (s:Season {{internal_id_fondazione: row.id}})
ON CREATE SET 
    s.title = row.dcTitle,
    s.type = row.dcType,
    s.start_date = row.from,
    s.end_date = row.to,
    s.source = 'Fondazione'

// CREAZIONE NODO ID
MERGE (id_node:ID {{code: 'fondazione_' + row.id}})
ON CREATE SET id_node.source = 'Fondazione'
MERGE (id_node)-[:IS_ID_OF]->(s)

// Collega Produzioni (INCLUDES_PRODUCTION)
WITH s, row
UNWIND SPLIT(row.produzioni_collegate_id, ',') AS pid
OPTIONAL MATCH (p:Production {{internal_id_fondazione: TRIM(pid)}})
FOREACH (_ IN CASE WHEN p IS NOT NULL THEN [1] ELSE [] END |
    MERGE (s)-[:INCLUDES_PRODUCTION]->(p)
    MERGE (p)-[:IS_PART_OF]->(s)
)

// Collega Recite (INCLUDES_PERFORMANCE) - Piano B sempre utile
WITH s, row
UNWIND SPLIT(row.manifestazioni_recite_concerti_collegati_id, ',') AS rid
OPTIONAL MATCH (r:Performance {{internal_id_fondazione: TRIM(rid)}})
FOREACH (_ IN CASE WHEN r IS NOT NULL THEN [1] ELSE [] END |
    MERGE (s)-[:INCLUDES_PERFORMANCE]->(r)
)

RETURN count(distinct s)
"""

if __name__ == "__main__":
    driver = None 
    try:
        driver = GraphDatabase.driver(uri_db, auth=(user, password))
        driver.verify_connectivity()
        print(f"Connesso a {uri_db}")
        
        create_constraints_fondazione(driver)
        
        run_import_step(driver, cypher_import_persone, "1. Persone (Arricchimento Wikidata)")
        run_import_step(driver, cypher_import_opere, "2. Opere (Works)")
        run_import_step(driver, cypher_import_produzioni, "3. Produzioni (Productions)")
        run_import_step(driver, cypher_import_recite, "4. Recite (Performances)")
        run_import_step(driver, cypher_link_produzioni_recite, "4.5 Link Produzioni->Recite")
        run_import_step(driver, cypher_import_stagioni, "5. Stagioni (Seasons)")
        
        print("\n>>> IMPORTAZIONE FONDAZIONE COMPLETATA.")
        print("    ORA ESEGUI LO SCRIPT 'reconcile_final.py' PER UNIRE I NODI!")

    except Exception as e:
        print(f"\n!!! ERRORE GENERALE: {e}")
        traceback.print_exc()
    finally:
        if driver: driver.close()