from neo4j import GraphDatabase, basic_auth
from sentence_transformers import SentenceTransformer
import os
import sys
from dotenv import load_dotenv

# 1. Config
dotenv_path = "/Users/elenabinotti/Documents/scuola/unibo/LM-43 DHDK/promemoria group/env.env"
load_dotenv(dotenv_path=dotenv_path)

URI = "bolt://archiuidev.promemoriagroup.com:7687"
USER = os.getenv("ID")
PASSWORD = os.getenv("SECRET_KEY")

# DEBUG CHECK
if not USER or not PASSWORD:
    print(f"ERROR: Could not load credentials from {dotenv_path}")
    print(f"USER: {USER}, PASSWORD: {'SET' if PASSWORD else 'NONE'}")
    sys.exit(1)

print("Loading AI Model...")
model = SentenceTransformer('all-MiniLM-L6-v2') 

def add_embeddings(driver):
    with driver.session() as session:
        print("Fetching works from database...")
        # A. Fetch all Works 
        result = session.run("""
            MATCH (w:Work)
            WHERE w.wikidata_qid IS NULL OR trim(w.wikidata_qid) = ''
            OPTIONAL MATCH (w)-[:HAS_COMPOSER]->(c:Person)
            WITH w, collect(DISTINCT c.name) AS composers
            RETURN elementId(w) AS id, w.title AS title, composers
        """)
        
        operations = []
        count = 0
        
        print("Calculating vectors...")
        
        # Consume the result entirely to avoid timeout/cursor issues
        records = list(result)
        
        for record in records:
            title = record['title']
            # Safe handling if title is missing
            if not title: continue 
            
            composers = record['composers']
            
            # Create text to embed
            text_to_embed = f"{title} {' '.join(composers) if composers else ''}"
            
            # Calculate Vector
            vector = model.encode(text_to_embed).tolist()
            
            # Prepare the update
            operations.append({"id": record["id"], "vector": vector})
            count += 1
            
            if count % 100 == 0:
                print(f"Processed {count} works...")

        # B. Write vectors back to Neo4j
        if operations:
            print(f"Writing {len(operations)} vectors to Neo4j...")
            
            # Batch write for performance
            session.run("""
                UNWIND $batch AS item
                MATCH (w) WHERE elementId(w) = item.id
                CALL db.create.setNodeVectorProperty(w, 'embedding', item.vector)
            """, batch=operations)
        else:
            print("No works found to update.")

# Run
try:
    print(f"Connecting to {URI} as {USER}...")
    driver = GraphDatabase.driver(URI, auth=basic_auth(USER, PASSWORD))
    driver.verify_connectivity() # Check connection before running logic
    
    add_embeddings(driver)
    
except Exception as e:
    print("\nCRITICAL ERROR:")
    print(e)
finally:
    if 'driver' in locals():
        driver.close()
    print("Done.")