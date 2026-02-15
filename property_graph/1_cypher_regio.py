from neo4j import GraphDatabase
from dotenv import load_dotenv
import os, sys, traceback

dotenv_path = "/Users/elenabinotti/Documents/scuola/unibo/LM-43 DHDK/promemoria group/env.env"
load_dotenv(dotenv_path=dotenv_path)

user = os.getenv("ID")
password = os.getenv("SECRET_KEY")
uri_db = "bolt://archiuidev.promemoriagroup.com:7687"

FILE_REGIO_OPERE = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/regio/regio_opere_pulito_con_anno.csv'
FILE_REGIO_PERSONE = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/regio/regio_persone.csv' 
FILE_REGIO_STAGIONI = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/regio/regio_stagioni.csv'
FILE_REGIO_PRODUZIONI = 'https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/regio/regio_produzioni.csv'
FILE_REGIO_RECITE = 'https://media.githubusercontent.com/media/elena2notti/theatreNet/main/regio/recite-regio-luoghi-qid2.csv'

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

def clean_db(driver):
    print("\n--- 0. PULIZIA DATABASE (DETACH DELETE e rimozione vincoli) ---")
    with driver.session() as session:
        try:
            session.run("CALL apoc.schema.assert({}, {})").consume()
            print("Vincoli e indici rimossi.")
        except Exception:
            try:
                result = session.run("SHOW CONSTRAINTS")
                for record in result:
                    session.run(f"DROP CONSTRAINT {record['name']}").consume()
            except: pass
        
        try:
            session.run("MATCH (n) DETACH DELETE n").consume()
            print("Database pulito con successo.")
        except Exception as e:
            print(f"Errore pulizia: {e}")

def create_constraints(driver):
    print("\n--- 0.1 CREAZIONE VINCOLI DI UNICITÀ REGIO ---")
    with driver.session() as session:
        constraints = [
            "CREATE CONSTRAINT person_id_regio_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.internal_id_regio IS UNIQUE",
            "CREATE CONSTRAINT id_code_unique IF NOT EXISTS FOR (i:ID) REQUIRE i.code IS UNIQUE",   
            "CREATE CONSTRAINT work_id_regio_unique IF NOT EXISTS FOR (o:Work) REQUIRE o.internal_id_regio IS UNIQUE",
            "CREATE CONSTRAINT season_id_regio_unique IF NOT EXISTS FOR (s:Season) REQUIRE s.internal_id_regio IS UNIQUE",
            "CREATE CONSTRAINT performance_id_regio_unique IF NOT EXISTS FOR (r:Performance) REQUIRE r.internal_id_regio IS UNIQUE",
            "CREATE CONSTRAINT production_id_regio_unique IF NOT EXISTS FOR (p:Production) REQUIRE p.internal_id_regio IS UNIQUE",
            "CREATE CONSTRAINT organizer_id_regio_unique IF NOT EXISTS FOR (o:Organizer) REQUIRE o.internal_id_regio IS UNIQUE",
            "CREATE CONSTRAINT ensemble_id_regio_unique IF NOT EXISTS FOR (e:Ensemble) REQUIRE e.internal_id_regio IS UNIQUE"
        ]
        
        for constraint in constraints:
            try:
                session.run(constraint).consume()
            except Exception:
                pass
        print("Vincoli creati.")


# 1. Importazione Persone
cypher_import_persone = f"""
LOAD CSV WITH HEADERS FROM '{FILE_REGIO_PERSONE}' AS row
FIELDTERMINATOR ','
WITH row 
WHERE row.person_id IS NOT NULL AND TRIM(row.person_id) <> ''
MERGE (p:Person {{internal_id_regio: row.person_id}})
ON CREATE SET 
    p.name = row.full_name,
    p.full_name = row.full_name,
    p.wikidata_qid = row.wikidata_id,
    p.wikidata_uri = row.wikidata_uri,
    p.birth_date = row.birth_date,
    p.birth_place = row.birth_place,
    p.death_date = row.death_date,
    p.death_place = row.death_place,
    p.occupation = row.occupation,
    p.viaf = row.viaf,
    p.source = 'Regio'

// CREAZIONE NODO ID
MERGE (id_node:ID {{code: 'regio_' + row.person_id}})
ON CREATE SET id_node.source = 'Regio'
MERGE (id_node)-[:IS_ID_OF]->(p)

RETURN count(p) AS total_people;
"""
# 2. Importazione Opere
cypher_import_opere_complete = f"""
LOAD CSV WITH HEADERS FROM '{FILE_REGIO_OPERE}' AS row
FIELDTERMINATOR ','
WITH row
WHERE row.compositions_id IS NOT NULL AND TRIM(row.compositions_id) <> ''

// --- A. CREAZIONE OPERA (Work) ---
MERGE (o:Work {{internal_id_regio: row.compositions_id}})
ON CREATE SET 
    o.title = row.dcTitle,
    o.year = CASE WHEN row.Anno IS NOT NULL AND row.Anno <> '' THEN toInteger(row.Anno) ELSE NULL END,
    o.wikidata_qid = row.wikidata_entity_id,
    o.wikidata_uri = row.composizione_uri,
    o.from_date = row.from,
    o.to_date = row.to,
    o.source = 'Regio'

// CREAZIONE NODO ID
MERGE (id_node:ID {{code: 'regio_' + row.compositions_id}})
ON CREATE SET id_node.source = 'Regio'
MERGE (id_node)-[:IS_ID_OF]->(o)

// --- B. COLLEGA COMPOSITORE (Bidirezionale) ---
WITH o, row
WHERE row.autore_musica IS NOT NULL AND TRIM(row.autore_musica) <> ''
WITH o, row, SPLIT(SPLIT(row.autore_musica, '(')[1], ')')[0] AS comp_id
MATCH (comp:Person {{internal_id_regio: comp_id}})
// Relazione 1: L'Opera HA il Compositore
MERGE (o)-[:HAS_COMPOSER]->(comp)
// Relazione 2: La Persona È COMPOSITORE dell'Opera
MERGE (comp)-[:IS_COMPOSER]->(o)

// --- C. COLLEGA LIBRETTISTA (Bidirezionale) ---
WITH o, row
WHERE row.autore_testo IS NOT NULL AND TRIM(row.autore_testo) <> ''
WITH o, row, SPLIT(SPLIT(row.autore_testo, '(')[1], ')')[0] AS lib_id
MATCH (lib:Person {{internal_id_regio: lib_id}})
// Relazione 1: L'Opera HA il Librettista
MERGE (o)-[:HAS_LIBRETTIST]->(lib)
// Relazione 2: La Persona È LIBRETTISTA dell'Opera
MERGE (lib)-[:IS_LIBRETTIST]->(o)

// --- D. COLLEGA AUTORE LETTERARIO (Bidirezionale) ---
WITH o, row
WHERE row.literary_author_id IS NOT NULL AND TRIM(row.literary_author_id) <> ''
MATCH (lit:Person {{internal_id_regio: row.literary_author_id}})
// Relazione 1: L'Opera HA l'Autore
MERGE (o)-[:HAS_LITERARY_AUTHOR]->(lit)
// Relazione 2: La Persona È AUTORE dell'Opera
MERGE (lit)-[:IS_LITERARY_AUTHOR]->(o)

// --- E. CREAZIONE PERSONAGGI (HAS_CHARACTER) ---
WITH o, row
WHERE row.character_wikidata_id IS NOT NULL AND TRIM(row.character_wikidata_id) <> ''
MERGE (c:Character {{wikidata_qid: row.character_wikidata_id}})
ON CREATE SET
    c.name = row.character_name,
    c.voice_type = row.voice_type,
    c.gender = row.character_gender,
    c.source = 'Regio'
MERGE (o)-[:HAS_CHARACTER]->(c)

RETURN count(o) AS total_works;
"""

# 3. Importazione Stagioni
cypher_import_stagioni = f"""
LOAD CSV WITH HEADERS FROM '{FILE_REGIO_STAGIONI}' AS row
FIELDTERMINATOR ','
WITH row
WHERE row.season_id IS NOT NULL AND TRIM(row.season_id) <> ''

// --- A. CREAZIONE STAGIONE ---
MERGE (s:Season {{internal_id_regio: row.season_id}})
ON CREATE SET 
    s.title = row.season_title,
    s.type = row.season_type,
    s.start_date = row.season_start_date,
    s.end_date = row.season_end_date,
    s.source = 'Regio'

// CREAZIONE NODO ID
MERGE (id_node:ID {{code: 'regio_' + row.season_id}})
ON CREATE SET id_node.source = 'Regio'
MERGE (id_node)-[:IS_ID_OF]->(s)

// --- B. CREAZIONE ORGANIZZATORE ---
WITH s, row
WHERE row.organizer_id IS NOT NULL AND TRIM(row.organizer_id) <> ''
MERGE (org:Organizer {{internal_id_regio: row.organizer_id}})
ON CREATE SET 
    org.name = row.organizer_name,
    org.source = 'Regio'

// --- C. COLLEGA STAGIONE -> ORGANIZZATORE ---
MERGE (s)-[:ORGANIZED_BY]->(org)

// --- D. COLLEGA STAGIONE -> PRODUZIONI ---
WITH s, row
WHERE row.linked_production_ids IS NOT NULL AND TRIM(row.linked_production_ids) <> ''
UNWIND SPLIT(row.linked_production_ids, ',') AS production_id
WITH s, TRIM(production_id) AS prod_id
WHERE prod_id <> ''
MERGE (p:Production {{internal_id_regio: prod_id}})
ON CREATE SET p.source = 'Regio'
MERGE (s)-[:INCLUDES_PRODUCTION]->(p)
MERGE (p)-[:IS_PART_OF]->(s)

RETURN count(s) AS total_seasons;
"""

# 4. Importazione Produzioni
cypher_import_produzioni_recite = f"""
LOAD CSV WITH HEADERS FROM '{FILE_REGIO_PRODUZIONI}' AS row
FIELDTERMINATOR ','
WITH row
WHERE row.production_id IS NOT NULL AND TRIM(row.production_id) <> ''

// --- A. CREAZIONE PRODUZIONE (Sempre eseguita) ---
MERGE (r:Production {{internal_id_regio: row.production_id}})
ON CREATE SET
    r.title = row.work_title,
    r.start_date = row.performance_start_date,
    r.end_date = row.performance_end_date, 
    r.year = CASE WHEN row.year IS NOT NULL THEN toInteger(row.year) ELSE NULL END,
    r.first_location = row.first_location,
    r.first_venue = row.first_venue,
    r.source = 'Regio'
// Se esiste già, aggiorniamo le info per sicurezza
ON MATCH SET
    r.title = row.work_title

MERGE (id_node:ID {{code: 'regio_' + row.production_id}})
ON CREATE SET id_node.source = 'Regio'
MERGE (id_node)-[:IS_ID_OF]->(r)

// --- B. COLLEGA OPERA (SAFE MODE) ---
// Usiamo OPTIONAL MATCH così se l'opera non c'è, la riga non muore
WITH r, row
WHERE row.related_work_id IS NOT NULL AND TRIM(row.related_work_id) <> ''
OPTIONAL MATCH (w:Work {{internal_id_regio: row.related_work_id}})

// Il FOREACH è un trucco: esegue il MERGE solo se 'w' è stato trovato (non è null)
FOREACH (_ IN CASE WHEN w IS NOT NULL THEN [1] ELSE [] END |
    MERGE (r)-[:RELATED_TO_WORK]->(w)
    MERGE (w)-[:RELATES_TO]->(r)
)

// --- C. COLLEGA PERSONALE (SAFE MODE) ---
// Ripartiamo da 'r' e 'row', ignorando se il passo B ha fallito o no
WITH r, row
WHERE row.person_id IS NOT NULL AND TRIM(row.person_id) <> ''
// Usiamo MATCH normale qui? Meglio OPTIONAL anche qui, se manca la persona nel DB Persone
OPTIONAL MATCH (p:Person {{internal_id_regio: row.person_id}})

WITH r, row, p,
     CASE
        WHEN row.person_role = 'Regista' THEN 'DIRECTED'
        WHEN row.person_role = 'Scenografo' THEN 'DESIGNED_SET'
        WHEN row.person_role = 'Coreografo' THEN 'CHOREOGRAPHED'
        WHEN row.person_role CONTAINS 'Costumista' THEN 'DESIGNED_COSTUMES'
        ELSE 'HAD_ROLE_IN'
     END AS relation_type

// Eseguiamo i merge solo se la persona 'p' è stata trovata
FOREACH (_ IN CASE WHEN p IS NOT NULL AND relation_type = 'DIRECTED' THEN [1] ELSE [] END |
    MERGE (p)-[:DIRECTED]->(r)
)
FOREACH (_ IN CASE WHEN p IS NOT NULL AND relation_type = 'DESIGNED_SET' THEN [1] ELSE [] END |
    MERGE (p)-[:DESIGNED_SET]->(r)
)
FOREACH (_ IN CASE WHEN p IS NOT NULL AND relation_type = 'CHOREOGRAPHED' THEN [1] ELSE [] END |
    MERGE (p)-[:CHOREOGRAPHED]->(r)
)
FOREACH (_ IN CASE WHEN p IS NOT NULL AND relation_type = 'DESIGNED_COSTUMES' THEN [1] ELSE [] END |
    MERGE (p)-[:DESIGNED_COSTUMES]->(r)
)
FOREACH (_ IN CASE WHEN p IS NOT NULL AND relation_type = 'HAD_ROLE_IN' THEN [1] ELSE [] END |
    MERGE (p)-[rel:HAD_ROLE_IN]->(r)
    SET rel.role = row.person_role
)

// Ritorniamo il conteggio dei nodi DISTINTI creati/toccati
RETURN count(DISTINCT r) AS distinct_productions_processed;
"""

# 5. Importazione Dettagli Performance
cypher_import_dettagli_performance = f"""
LOAD CSV WITH HEADERS FROM '{FILE_REGIO_RECITE}' AS row
FIELDTERMINATOR ','
WITH row
WHERE row.production_id IS NOT NULL AND TRIM(row.production_id) <> ''
  AND row.id_recita IS NOT NULL AND TRIM(row.id_recita) <> ''

WITH row, row.production_id + '_' + row.id_recita AS unique_perf_id

// --- 1. MATCH PRODUZIONE E CREA RECITA ---
MERGE (rec:Performance {{internal_id_regio: unique_perf_id}})
ON CREATE SET
    rec.internal_id_dettaglio = row.id_recita,
    rec.title = row.titolo_breve,
    rec.date = row.from,
    rec.venue = row.luogo_nome,
    rec.building = row.edificio_nome,
    rec.source = 'Regio'

MERGE (id_node:ID {{code: 'regio_' + unique_perf_id}})
ON CREATE SET id_node.source = 'Regio'
MERGE (id_node)-[:IS_ID_OF]->(rec)

// Collega alla Produzione Padre
WITH rec, row
MERGE (prod:Production {{internal_id_regio: row.production_id}})
MERGE (prod)-[:HAS_PERFORMANCE]->(rec)

// --- 1.5 (NUOVO) COLLEGA DIRETTAMENTE ALL'OPERA (WORK) ---
// Questo allinea il modello a quello della Fondazione
WITH rec, row
WHERE row.composizione_id IS NOT NULL AND TRIM(row.composizione_id) <> ''
MATCH (w:Work {{internal_id_regio: row.composizione_id}})
MERGE (rec)-[:RELATED_TO_WORK]->(w)
MERGE (w)-[:RELATES_TO]->(rec)

// --- 2. COLLEGA DIRETTORI ---
WITH rec, row
WHERE row.curatore_id IS NOT NULL AND TRIM(row.curatore_id) <> ''
  AND row.curatore_ruolo IS NOT NULL
MERGE (cur:Person {{internal_id_regio: row.curatore_id}})
ON CREATE SET cur.name = row.curatore_nome, cur.source = 'Regio'
WITH rec, row, cur
FOREACH (i IN CASE WHEN row.curatore_ruolo CONTAINS 'Direttore' THEN [1] ELSE [] END |
    MERGE (cur)-[:CONDUCTED]->(rec)
)

// --- 3. GESTIONE INTERPRETI E PERSONAGGI ---
WITH rec, row
WHERE row.interprete_id IS NOT NULL AND TRIM(row.interprete_id) <> ''
  AND row.personaggio IS NOT NULL AND TRIM(row.personaggio) <> ''
MERGE (int:Person {{internal_id_regio: row.interprete_id}})
ON CREATE SET int.name = row.interprete, int.source = 'Regio'

WITH rec, row, int
MERGE (char:Character {{name: row.personaggio}})
ON CREATE SET char.voice_type = row.personaggio_voce, char.source = 'Regio'

MERGE (int)-[:INTERPRETED]->(char)
MERGE (char)-[:APPEARED_IN]->(rec)

MERGE (int)-[r:PERFORMED_IN]->(rec)

FOREACH (_ IN CASE WHEN row.ruolo IS NOT NULL AND TRIM(row.ruolo) <> '' THEN [1] ELSE [] END |
    SET r.role = row.ruolo
)

// --- 4. GESTIONE ESECUTORI DI GRUPPO ---
WITH rec, row
WHERE row.esecutore_id IS NOT NULL AND TRIM(row.esecutore_id) <> ''
MERGE (e:Ensemble {{internal_id_regio: row.esecutore_id}})
ON CREATE SET
    e.name = row.esecutore_nome,
    e.type = row.esecutore_ruolo,
    e.source = 'Regio'
MERGE (e)-[:PARTICIPATED_IN]->(rec)

RETURN count(rec) AS total_performances;
"""

driver = None 
try:
    driver = GraphDatabase.driver(uri_db, auth=(user, password))
    driver.verify_connectivity()
    print(f"Connessione a Neo4j stabilita all'URI: {uri_db}")
    
    print("\n[STEP 0/5] Esecuzione pulizia database e creazione vincoli...")
    clean_db(driver) 
    create_constraints(driver) 
    
    print("\n[STEP 1/5] Importazione Persone...")
    run_import_step(driver, cypher_import_persone, "1. Importazione Nodi Person")

    print("\n[STEP 2/5] Importazione Opere...")
    run_import_step(driver, cypher_import_opere_complete, "2. Importazione Works")

    print("\n[STEP 3/5] Importazione Stagioni...")
    run_import_step(driver, cypher_import_stagioni, "3. Importazione Seasons")

    print("\n[STEP 4/5] Importazione Produzioni...")
    run_import_step(driver, cypher_import_produzioni_recite, "4. Importazione Productions")

    print("\n[STEP 5/5] Importazione Performances...")
    run_import_step(driver, cypher_import_dettagli_performance, "5. Importazione Performances")

    # NOTA: Step 6 rimosso. 
    # Per unire i nodi, lanciare il comando apoc.refactor.mergeNodes DOPO aver caricato anche la Fondazione.

    print("\n>>> SUCCESSO: Importazione Regio (English + Updated ID) completata!")
    
except Exception as e:
    print("\n!!! ERRORE FATALE DURANTE IL PROCESSO DI UPLOAD !!!")
    print(e)
    traceback.print_exc(file=sys.stdout)
finally:
    if driver:
        driver.close()
        print("Connessione a Neo4j chiusa.")