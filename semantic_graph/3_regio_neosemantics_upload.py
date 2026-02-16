from neo4j import GraphDatabase
from dotenv import load_dotenv
import os, sys, traceback

dotenv_path = "/Users/elenabinotti/Documents/scuola/unibo/LM-43 DHDK/promemoria group/env.env"
load_dotenv(dotenv_path=dotenv_path)
user = os.getenv("ID")
password = os.getenv("SECRET_KEY")
uri_db = "bolt://archiuidev.promemoriagroup.com:7687"

# Path del file TTL generato
rdf_file = "/Users/elenabinotti/Library/CloudStorage/GoogleDrive-elena.binotti@promemoriagroup.com/Drive condivisi/tirocinio regio/semantic_graph/upload_to_neo4j/regio_unified.ttl"
rdf_format = "Turtle"

driver = GraphDatabase.driver(uri_db, auth=(user, password))

def clean_db():
    """Pulisce Dati e Configurazione per evitare conflitti"""
    print("1. Pulizia Database in corso...")
    with driver.session(database="neo4j") as session:
        # 1. Cancella tutti i nodi e relazioni (DEVE ESSERE FATTO)
        session.run("MATCH (n) DETACH DELETE n")
        
        # 2. Rimuove la config (try/except per evitare fallimenti)
        try:
            print("   - Tentativo di rimozione configurazione n10s precedente...")
            session.run("CALL n10s.graphconfig.drop()")
            print("   - Configurazione precedente rimossa con successo.")
        except Exception as e:
            # Stampa l'errore, ma continua l'esecuzione
            print(f"   - INFO: Impossibile rimuovere config n10s (Probabilmente non esisteva). Errore: {e}")
        
        # 3. Cancella constraint vecchi
        try:
            print("   - Rimozione constraint n10s_unique_uri...")
            session.run("DROP CONSTRAINT n10s_unique_uri IF EXISTS")
        except Exception as e:
             # Stampa l'errore, ma continua l'esecuzione
             print(f"   - INFO: Impossibile rimuovere constraint. Errore: {e}")

def create_constraint():
    print("2. Creazione Constraint...")
    with driver.session(database="neo4j") as session:
        session.run("""
            CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS
            FOR (r:Resource) REQUIRE r.uri IS UNIQUE
        """)

def init_config():
    print("3. Inizializzazione n10s (Modalità IGNORE)...")
    with driver.session(database="neo4j") as session:
        session.run("""
            CALL n10s.graphconfig.init({
                handleVocabUris: "IGNORE",      // Rimuove i namespace (frbroo:F1 -> :F1)
                handleMultival: "OVERWRITE",    // Gestione proprietà singole
                keepLangTag: false,             // Semplifica le stringhe (rimuove @it)
                keepCustomDataTypes: true,
                applyNeo4jNaming: false         // Mantiene maiuscole/minuscole originali
            })
        """)

def import_ttl_inline():
    print(f"4. Importazione file: {os.path.basename(rdf_file)}...")
    # Controllo dimensione file per sicurezza
    file_size_mb = os.path.getsize(rdf_file) / (1024 * 1024)
    if file_size_mb > 15:
        print(f"ATTENZIONE: Il file è {file_size_mb:.2f} MB. L'import inline potrebbe fallire.")
        print("Se fallisce, usa il metodo 'HTTP Server' (vedi sotto).")

    with driver.session(database="neo4j") as session:
        with open(rdf_file, "r", encoding="utf-8") as f:
            data = f.read()
        
        # Esecuzione import
        result = session.run("""
            CALL n10s.rdf.import.inline($rdf, $format)
        """, rdf=data, format=rdf_format)
        
        summary = result.single()
        print(f"   - Import completato: {summary['triplesLoaded']} triple caricate.")

# === ESECUZIONE ===
try:
    clean_db()          # FONDAMENTALE: Reset totale
    create_constraint() # Ricrea vincoli
    init_config()       # Applica la config "IGNORE"
    import_ttl_inline() # Carica i dati
    print("\n>>> SUCCESSO: Ora puoi usare le query senza prefissi!")
    
except Exception as e:
    print("\n!!! ERRORE CRITICO !!!")
    print(e)
    traceback.print_exc()
finally:
    driver.close()