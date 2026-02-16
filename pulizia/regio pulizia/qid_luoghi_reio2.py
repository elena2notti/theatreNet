import pandas as pd
import os

# === CONFIGURAZIONE REGIO ===
FILE_PRINCIPALE = "dataset/regio/recite/recite_regio_final_fixed_ids.csv"
FILE_MAPPING = "dataset/regio/recite/recite-regio-luoghi-csv.csv"
OUTPUT_FILE = "dataset/regio/recite-regio-luoghi-qid2.csv"

def main():
    print("--- 1. Caricamento file (Forzando Stringhe) ---")
    
    # 1. Carica file principale come stringa pura
    try:
        df_main = pd.read_csv(FILE_PRINCIPALE, sep=',', dtype=str)
        df_main = df_main.fillna("") # Rimuove subito i NaN
        print(f"File principale caricato: {len(df_main)} righe.")
    except FileNotFoundError:
        print(f"❌ Errore: Non trovo {FILE_PRINCIPALE}")
        return

    # 2. Carica mapping come stringa pura
    try:
        df_map = pd.read_csv(FILE_MAPPING, sep=',', dtype=str)
        df_map = df_map.fillna("") # Rimuove subito i NaN
        print(f"File mapping caricato: {len(df_map)} luoghi unici.")
    except FileNotFoundError:
        print(f"❌ Errore: Non trovo {FILE_MAPPING}")
        return

    # === PULIZIA CHIAVI DI JOIN (Trim spazi) ===
    # Questo passaggio è fondamentale per evitare mancati match per colpa di spazi invisibili
    df_main['edificio_nome'] = df_main['edificio_nome'].str.strip()
    df_main['luogo_nome'] = df_main['luogo_nome'].str.strip()
    
    df_map['edificio_nome'] = df_map['edificio_nome'].str.strip()
    df_map['luogo_nome'] = df_map['luogo_nome'].str.strip()

    # === DIAGNOSTICA ===
    print("\n--- DIAGNOSTICA ---")
    match_edifici = set(df_main['edificio_nome']).intersection(set(df_map['edificio_nome']))
    print(f"Edifici in comune: {len(match_edifici)}")
    match_luoghi = set(df_main['luogo_nome']).intersection(set(df_map['luogo_nome']))
    print(f"Luoghi in comune: {len(match_luoghi)}")

    # === 2. PREPARAZIONE MAPPING ===
    # Rimuovi duplicati nel mapping sulla chiave composta
    df_map_clean = df_map.drop_duplicates(subset=['edificio_nome', 'luogo_nome'])
    print(f"\nMapping dopo rimozione duplicati: {len(df_map_clean)} righe")

    # Seleziona solo le colonne necessarie
    cols_to_use = ['edificio_nome', 'luogo_nome', 'entity', 'uri']
    cols_actual = [c for c in cols_to_use if c in df_map_clean.columns]
    df_map_clean = df_map_clean[cols_actual]

    # === 3. UNIONE (Merge) ===
    print("\n--- 3. Unione (Merge) ---")
    
    # LEFT JOIN su edificio E luogo
    df_final = pd.merge(
        df_main, 
        df_map_clean, 
        on=['edificio_nome', 'luogo_nome'], 
        how='left',
        suffixes=('', '_mapping')
    )

    # Contiamo quanti QID sono stati assegnati
    matches = 0
    if 'entity' in df_final.columns:
        matches = df_final[df_final['entity'] != ""].shape[0]
        
    print(f"✅ Assegnati {matches} QID su {len(df_final)} recite totali.")

    # === 4. PULIZIA FINALE KILLER (.0) E SALVATAGGIO ===
    print("\n--- 4. Pulizia Float e Salvataggio ---")
    
    # Rimuove .0 da TUTTE le colonne
    for col in df_final.columns:
        df_final[col] = df_final[col].astype(str).replace('nan', '').replace(r'\.0$', '', regex=True)

    # Crea la directory se non esiste
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    df_final.to_csv(OUTPUT_FILE, index=False)
    print(f"Finito! File salvato come: {OUTPUT_FILE}")
    
    # Statistiche finali veloci
    if 'entity' in df_final.columns and matches > 0:
        print("\nDistribuzione QID per edificio (Top 5):")
        distribuzione = df_final[df_final['entity'] != ""].groupby('edificio_nome').size().sort_values(ascending=False)
        for edificio, count in distribuzione.head(5).items():
            print(f"  {edificio}: {count} recite")

if __name__ == "__main__":
    main()