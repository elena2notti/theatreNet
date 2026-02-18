import pandas as pd
import re
from pathlib import Path
import math

# ================================================================
# 1. CONFIGURAZIONE
# ================================================================

# 🚨 SOSTITUISCI QUESTI PATH CON I TUOI PERCORSI REALI 🚨
CSV_INPUT_PATH = Path("dataset/regio_opere.csv") # Assumendo che questo sia il tuo CSV completo
CSV_OUTPUT_PATH = Path("dataset/regio_opere_pulito_con_anno.csv")

# Nome della nuova colonna come richiesto
NOME_COLONNA_ANNO = 'Anno'

# ================================================================
# 2. FUNZIONI DI PULIZIA
# ================================================================

def clean_year_from_datetext(datetext_value):
    """
    Estrae l'anno finale dalla stringa 'datetext' e lo pulisce.
    
    Esempio: "01-01-1830 - 31-12-1830" -> estrae "31-12-1830" -> restituisce "1830"
    """
    if pd.isna(datetext_value) or datetext_value is None:
        return None
        
    s = str(datetext_value).strip()
    
    # 1. Trova l'ultima data: il separatore è spesso un trattino ('-')
    # La logica è prendere l'ultimo blocco dopo l'ultimo separatore (es. ' - ')
    parts = s.split('-')
    
    # Se ci sono più di due parti, assumiamo che l'ultima parte contenga l'anno finale completo
    # Esempio: "01-01-1830 - 31-12-1830"
    if len(parts) >= 2:
        
        # Prendiamo l'ultima data completa (es. "31-12-1830")
        # Questa data è generalmente l'ultima nell'intervallo
        last_date_str = s.split(' - ')[-1].strip()
        
        # 2. Estrai l'anno (i quattro numeri alla fine della data)
        match = re.search(r'(\d{4})$', last_date_str)
        if match:
            # L'anno pulito
            year = match.group(1)
            
            # Pulisci ID numerici (come nella tua funzione clean_id)
            if year.endswith(".0"): year = year[:-2]
            return year

    return None # Restituisce None se non trova un pattern valido

# ================================================================
# 3. ESECUZIONE
# ================================================================

try:
    # Caricamento del dataset
    df = pd.read_csv(
        CSV_INPUT_PATH, 
        sep=',',  # Assumi la virgola, ma se hai problemi, usa ';'
        encoding='utf-8',
        # Opzioni per robustezza (ignora righe con problemi)
        on_bad_lines='skip', 
        engine='python' 
    )
    print(f"✅ Caricato dataset originale: {len(df)} righe.")

except FileNotFoundError:
    print(f"❌ ERRORE: File {CSV_INPUT_PATH} non trovato.")
    exit()
except Exception as e:
    print(f"❌ ERRORE durante la lettura del CSV: {e}")
    exit() 

# --- APPLICAZIONE DELLA FUNZIONE DI PULIZIA ---
print(f"Inizio pulizia e creazione colonna '{NOME_COLONNA_ANNO}'...")

# Applica la funzione alla colonna 'datetext'
df[NOME_COLONNA_ANNO] = df['datetext'].apply(clean_year_from_datetext)

# Verifica dei primi valori per l'anno pulito
print("\nAnteprima dei dati puliti:")
print(df[['datetext', NOME_COLONNA_ANNO]].head().to_markdown(index=False))

# --- SALVATAGGIO ---
# Tutte le colonne originali (incluso 'datetext') vengono mantenute, 
# e la nuova colonna 'Anno' viene aggiunta in fondo.
CSV_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(CSV_OUTPUT_PATH, index=False, encoding='utf-8')

print("\n--- Risultato ---")
print(f"✅ Pulizia completata. File salvato in: {CSV_OUTPUT_PATH}")
print(f"Il nuovo file contiene tutte le {len(df.columns)} colonne originali + la colonna '{NOME_COLONNA_ANNO}'.")