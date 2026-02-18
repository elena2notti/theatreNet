import urllib.request
import sys

# URL del file corrotto
url = 'https://raw.githubusercontent.com/elena2notti/rpom/refs/heads/main/fondazione/produzioni.csv'
output_file = 'produzioni_clean.csv'

print(f"Scaricamento da {url}...")

try:
    # Usa urllib che è standard in Python (niente pip install)
    with urllib.request.urlopen(url) as response:
        # Leggi il contenuto come testo
        content = response.read().decode('utf-8')
        
    print("File scaricato. Inizio pulizia...")
    
    # LA CORREZIONE:
    # Il problema sono le virgolette doppie "" usate dentro un campo JSON ma non gestite bene.
    # Le sostituiamo con un apice singolo ' per non rompere il CSV.
    # Sostituiamo anche eventuali virgolette "escaped" male.
    
    # 1. Sostituisci le doppie virgolette interne con apice singolo
    clean_content = content.replace('""', "'")
    
    # Salviamo il file pulito
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(clean_content)
        
    print(f"SUCCESSO! File salvato come: {output_file}")
    print("ORA: Carica questo file su GitHub e copia il nuovo link nello script Neo4j,")
    print("oppure, se usi Neo4j Desktop, metti questo file nella cartella 'import' del database.")

except Exception as e:
    print(f"Errore: {e}")