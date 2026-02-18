import pandas as pd
import os

# === CONFIGURAZIONE FONDAZIONE ===
FILE_PRINCIPALE = "dataset/fondazione/Recite/recite_fixed_ids.csv"
FILE_MAPPING = "dataset/fondazione/Recite/recite-fondazione_luoghi_qid.csv"
OUTPUT_FILE = "dataset/fondazione/recite_fondazione_con_qid.csv"

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
    # Fondamentale per far funzionare il merge
    df_main['edificio_nome'] = df_main['edificio_nome'].str.strip()
    df_main['luogo_nome'] = df_main['luogo_nome'].str.strip()
    
    df_map['edificio_nome'] = df_map['edificio_nome'].str.strip()
    df_map['luogo_nome'] = df_map['luogo_nome'].str.strip()

    # === DIAGNOSTICA VELOCE ===
    print("\n--- DIAGNOSTICA ---")
    match_edifici = set(df_main['edificio_nome']).intersection(set(df_map['edificio_nome']))
    print(f"Edifici in comune: {len(match_edifici)}")
    
    # === 2. PREPARAZIONE MAPPING ===
    # Rimuovi duplicati nel mapping
    df_map_clean = df_map.drop_duplicates(subset=['edificio_nome', 'luogo_nome'])
    
    # Seleziona colonne (assumiamo che nel mapping ci siano 'entity' e 'uri' per i QID)
    cols_to_use = ['edificio_nome', 'luogo_nome', 'entity', 'uri']
    # Filtriamo solo quelle che esistono davvero nel file
    cols_actual = [c for c in cols_to_use if c in df_map_clean.columns]
    df_map_clean = df_map_clean[cols_actual]

    # === 3. UNIONE (Merge) ===
    print("\n--- 3. Unione (Merge) ---")
    
    df_final = pd.merge(
        df_main, 
        df_map_clean, 
        on=['edificio_nome', 'luogo_nome'], 
        how='left',
        suffixes=('', '_mapping') # Se ci sono collisioni
    )

    matches = df_final['entity'].notna().sum() if 'entity' in df_final.columns else 0
    # Nota: se entity è stringa vuota invece di NaN, dobbiamo contare diversamente
    if 'entity' in df_final.columns:
         matches = df_final[df_final['entity'] != ""].shape[0]

    print(f"✅ Recite arricchite con QID: {matches}")

    # === 4. PULIZIA FINALE KILLER (.0) E SALVATAGGIO ===
    print("\n--- 4. Pulizia Float e Salvataggio ---")
    
    # Questo ciclo rimuove .0 da TUTTE le colonne, sia vecchie che nuove
    for col in df_final.columns:
        df_final[col] = df_final[col].astype(str).replace('nan', '').replace(r'\.0$', '', regex=True)

    # Crea cartella
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    df_final.to_csv(OUTPUT_FILE, index=False)
    print(f"Finito! File salvato: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()