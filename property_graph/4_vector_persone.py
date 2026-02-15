from neo4j import GraphDatabase, basic_auth
from sentence_transformers import SentenceTransformer
import os
import sys
from dotenv import load_dotenv

dotenv_path = "/Users/elenabinotti/Documents/scuola/unibo/LM-43 DHDK/promemoria group/env.env"
load_dotenv(dotenv_path=dotenv_path)

URI = "bolt://archiuidev.promemoriagroup.com:7687"
USER = os.getenv("ID")
PASSWORD = os.getenv("SECRET_KEY")

if not USER or not PASSWORD:
    print("ERRORE: Credenziali mancanti.")
    sys.exit(1)

print("Loading AI Model...")
model = SentenceTransformer('all-MiniLM-L6-v2') 

def add_person_embeddings(driver):
    with driver.session() as session:
        print("Fetching people from database...")
        
        result = session.run("""
            MATCH (p:Person)
            WHERE p.wikidata_qid IS NULL OR trim(p.wikidata_qid) = ''
            RETURN elementId(p) AS id, p.name AS name, p.birth_date AS bdate, p.death_date AS ddate
        """)
        
        operations = []
        count = 0
        print("Calculating vectors for People...")
        
        records = list(result)
        
        for record in records:
            name = record['name']
            if not name: continue 
            
            # aiuta l'AI a distinguere omonimi di secoli diversi.
            bdate = str(record['bdate']) if record['bdate'] else ""
            ddate = str(record['ddate']) if record['ddate'] else ""
            
            # Stringa finale da vettorizzare
            text_to_embed = f"{name} {bdate} {ddate}".strip()
            
            # Calcolo Vettore
            vector = model.encode(text_to_embed).tolist()
            
            operations.append({"id": record["id"], "vector": vector})
            count += 1
            
            if count % 500 == 0:
                print(f"Processed {count} people...")

        if operations:
            print(f"Writing {len(operations)} vectors to Neo4j...")
            session.run("""
                UNWIND $batch AS item
                MATCH (p) WHERE elementId(p) = item.id
                CALL db.create.setNodeVectorProperty(p, 'embedding', item.vector)
            """, batch=operations)
        else:
            print("No people found.")

try:
    driver = GraphDatabase.driver(URI, auth=basic_auth(USER, PASSWORD))
    driver.verify_connectivity()
    add_person_embeddings(driver)
except Exception as e:
    print(e)
finally:
    if 'driver' in locals(): driver.close()
    print("Done.")