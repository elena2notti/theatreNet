from neo4j import GraphDatabase
from dotenv import load_dotenv
import os, sys, traceback

# === CONFIG ===
dotenv_path = "/Users/elenabinotti/Documents/scuola/unibo/LM-43 DHDK/promemoria group/env.env"
load_dotenv(dotenv_path=dotenv_path)
user = os.getenv("ID")
password = os.getenv("SECRET_KEY")
uri_db = "bolt://archiuidev.promemoriagroup.com:7687"

# Path del file TTL generato (FONDAZIONE UNIFIED)
rdf_file = "/Users/elenabinotti/Library/CloudStorage/GoogleDrive-elena.binotti@promemoriagroup.com/Drive condivisi/tirocinio regio/semantic_graph/upload_to_neo4j/fondazione_unified.ttl"
rdf_format = "Turtle"

# Inizializzazione del driver
driver = GraphDatabase.driver(uri_db, auth=(user, password))

def create_constraint():
    print("1. Verifica constraint...")
    with driver.session(database="neo4j") as session:
        # Verifica se il constraint esiste già
        result = session.run("SHOW CONSTRAINTS")
        constraints = [record["name"] for record in result]
        
        if "n10s_unique_uri" not in constraints:
            print("   - Creazione constraint n10s_unique_uri...")
            session.run("""
                CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS
                FOR (r:Resource) REQUIRE r.uri IS UNIQUE
            """)
        else:
            print("   - Constraint n10s_unique_uri già presente")

def check_config():
    """Verifica che la configurazione n10s sia corretta per URI unificati"""
    print("2. Verifica configurazione n10s...")
    with driver.session(database="neo4j") as session:
        try:
            # Prova a leggere la config corrente
            result = session.run("CALL n10s.graphconfig.show()")
            config = result.single()
            
            if config:
                print(f"   - Configurazione corrente:")
                print(f"     handleVocabUris: {config.get('handleVocabUris')}")
                print(f"     handleMultival: {config.get('handleMultival')}")
                print(f"     keepLangTag: {config.get('keepLangTag')}")
                
                # Se non è IGNORE, re-inizializza
                if config.get('handleVocabUris') != 'IGNORE':
                    print("   - Re-inizializzazione config (handleVocabUris: IGNORE)...")
                    init_config()
                else:
                    print("   - Configurazione OK (già IGNORE)")
            else:
                print("   - Nessuna config trovata, inizializzazione...")
                init_config()
                
        except Exception as e:
            print(f"   - Errore lettura config: {e}")
            print("   - Inizializzazione nuova config...")
            init_config()

def init_config():
    """Inizializza configurazione n10s per URI unificati"""
    with driver.session(database="neo4j") as session:
        session.run("""
            CALL n10s.graphconfig.init({
                handleVocabUris: "IGNORE",      // ESSENZIALE per URI unificati
                handleMultival: "OVERWRITE",    // Gestione proprietà singole
                keepLangTag: false,             // Semplifica le stringhe
                keepCustomDataTypes: true,
                applyNeo4jNaming: false         // Mantiene maiuscole/minuscole
            })
        """)
        print("   - Configurazione applicata (handleVocabUris: IGNORE)")

def import_ttl_inline():
    print(f"3. Importazione file: {os.path.basename(rdf_file)}...")
    
    # Controllo dimensione file
    file_size_mb = os.path.getsize(rdf_file) / (1024 * 1024)
    print(f"   - Dimensione file: {file_size_mb:.2f} MB")
    
    if file_size_mb > 50:
        print("File grande, import potrebbe richiedere tempo...")
    
    with driver.session(database="neo4j") as session:
        with open(rdf_file, "r", encoding="utf-8") as f:
            data = f.read()
        
        # Esecuzione import
        try:
            result = session.run("""
                CALL n10s.rdf.import.inline($rdf, $format)
            """, rdf=data, format=rdf_format)
            
            summary = result.single()
            if summary:
                print(f"   Import completato: {summary['triplesLoaded']} triple caricate")
                print(f"   - Nodi creati: {summary.get('nodesCreated', 'N/A')}")
                print(f"   - Relazioni create: {summary.get('relationshipsCreated', 'N/A')}")
                
                # Verifica merge automatico degli URI unificati
                if summary['triplesLoaded'] > 0:
                    print(f"\n4. Verifica merge URI unificati...")
                    check_unified_uris(session)
            else:
                print("Import completato, ma nessun riepilogo disponibile")
                
        except Exception as e:
            print(f"Errore durante l'import: {e}")
            # Prova con import a chunk se fallisce
            if "out of memory" in str(e).lower() or "too large" in str(e).lower():
                print("   - Tentativo import in chunk...")
                import_in_chunks(session, data)

def import_in_chunks(session, data):
    """Importa in chunk se il file è troppo grande"""
    print("   - Suddivisione in chunk...")
    
    # Dividi per righe (per TTL)
    lines = data.split('\n')
    chunk_size = 10000  # Linee per chunk
    total_chunks = (len(lines) + chunk_size - 1) // chunk_size
    
    for i in range(total_chunks):
        start = i * chunk_size
        end = start + chunk_size
        chunk = '\n'.join(lines[start:end])
        
        print(f"   - Import chunk {i+1}/{total_chunks}...")
        try:
            result = session.run("""
                CALL n10s.rdf.import.inline($rdf, $format)
            """, rdf=chunk, format=rdf_format)
            
            summary = result.single()
            if summary:
                print(f"     Triple: {summary['triplesLoaded']}")
                
        except Exception as e:
            print(f"Errore chunk {i+1}: {e}")

def check_unified_uris(session):
    """Verifica che gli URI unificati siano stati mergeati correttamente"""
    print("   - Cerca nodi con URI unificati...")
    
    # Conta nodi con URI unificati
    result = session.run("""
        MATCH (n:Resource)
        WHERE n.uri CONTAINS 'unified_'
        RETURN count(n) as unified_count
    """)
    
    unified = result.single()
    if unified and unified["unified_count"] > 0:
        print(f"   - Trovati {unified['unified_count']} nodi con URI unificati")
        
        # Verifica che non ci siano duplicati per lo stesso QID
        result = session.run("""
            MATCH (n:Resource)
            WHERE n.uri CONTAINS 'unified_'
            WITH n.uri as uri, collect(n) as nodes
            WHERE size(nodes) > 1
            RETURN count(*) as duplicate_groups
        """)
        
        dup = result.single()
        if dup and dup["duplicate_groups"] > 0:
            print(f"ATTENZIONE: {dup['duplicate_groups']} gruppi di URI unificati duplicati")
            print("   - Esegui query di merge dopo l'import completo")
        else:
            print("Nessun duplicato trovato tra URI unificati")
            
        # Mostra alcuni esempi
        result = session.run("""
            MATCH (n:Resource)
            WHERE n.uri CONTAINS 'unified_' AND n.uri CONTAINS 'person_'
            RETURN n.uri as uri, n.`rdfs:label` as label
            LIMIT 5
        """)
        
        print("   - Esempi URI unificati:")
        for record in result:
            print(f"     • {record['label']} -> {record['uri']}")
    else:
        print("Nessun URI unificato trovato (controlla il file TTL)")

def run_merge_query():
    """Esegue query per unire nodi con stesso Wikidata (se necessario)"""
    print("\n5. Esecuzione query di merge (se necessario)...")
    
    with driver.session(database="neo4j") as session:
        # Query per trovare potenziali duplicati
        result = session.run("""
            MATCH (n1:Resource)-[:`owl:sameAs`]->(wd:Resource)<-[:`owl:sameAs`]-(n2:Resource)
            WHERE wd.uri CONTAINS 'wikidata.org/entity/Q'
              AND n1 <> n2
              AND n1.uri CONTAINS 'unified_' = false
              AND n2.uri CONTAINS 'unified_' = false
            RETURN count(DISTINCT wd) as wikidata_duplicates,
                   count(DISTINCT n1) + count(DISTINCT n2) as local_nodes
        """)
        
        stats = result.single()
        if stats and stats["wikidata_duplicates"] > 0:
            print(f"   - Trovati {stats['wikidata_duplicates']} Wikidata con nodi duplicati")
            print(f"   - Nodi locali da unire: {stats['local_nodes']}")
            
            # Opzionale: esegui automaticamente il merge
            # result = session.run("""
            #     // Query di merge qui
            # """)
            # print("   - Merge eseguito")
        else:
            print("Nessun duplicato da unire (tutto OK)")

# === ESECUZIONE ===
try:
    print("=== IMPORT FONDAZIONE ITEATRI (URI UNIFICATI) ===")
    print(f"File: {os.path.basename(rdf_file)}")
    
    # IMPORTANTE: NON cancelliamo il database!
    create_constraint()      # Solo se non esiste
    check_config()           # Verifica/Imposta config IGNORE
    import_ttl_inline()      # Importa i dati (si aggiungono a quelli esistenti)
    run_merge_query()        # Verifica merge
    
    print("\nIMPORT COMPLETATO CON SUCCESSO")
    print("I dati della Fondazione sono stati aggiunti al grafo esistente.")
    print("Le entità con URI unificati (unified_*) sono pronte per il merge automatico.")
    
except Exception as e:
    print(f"\nERRORE CRITICO: {e}")
    traceback.print_exc()
finally:
    driver.close()