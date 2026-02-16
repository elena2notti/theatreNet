import pandas as pd

# === CONFIGURAZIONE ===
# Inserisci qui il nome del tuo file di input
INPUT_FILE = 'dataset/fondazione/Recite/recite_fixed_ids.csv' # Assicurati che il nome sia corretto
OUTPUT_FILE = 'dataset/fondazione/Recite/recite_luoghi.csv'

def main():
    print(f"Lettura del file: {INPUT_FILE}...")
    
    try:
        # 1. Carica il CSV (separatore virgola)
        df = pd.read_csv(INPUT_FILE, sep=',')
        
        # Verifica che le colonne esistano
        required_cols = ['edificio_nome', 'luogo_nome']
        if not all(col in df.columns for col in required_cols):
            print(f"Errore: Il file deve contenere le colonne: {required_cols}")
            return

        # 2. Seleziona solo le colonne di interesse
        print("Estrazione e pulizia dei dati...")
        edifici = df[['edificio_nome', 'luogo_nome']].copy()

        # 3. Rimuove le righe vuote (dove non c'è nome edificio)
        edifici = edifici.dropna(subset=['edificio_nome'])
        
        # 4. Rimuove i duplicati per avere solo la lista unica
        edifici_unici = edifici.drop_duplicates()
        
        # 5. Ordina per Città e poi per Edificio (per comodità di lettura)
        edifici_unici = edifici_unici.sort_values(by=['luogo_nome', 'edificio_nome'])

        # 6. Salva il nuovo file
        edifici_unici.to_csv(OUTPUT_FILE, index=False)
        
        print(f"\n✅ Fatto! Estratti {len(edifici_unici)} edifici unici.")
        print(f"📂 File salvato come: {OUTPUT_FILE}")
        
        # Anteprima a video
        print("\n--- Anteprima dei primi 10 risultati ---")
        print(edifici_unici.head(10))

    except FileNotFoundError:
        print(f"❌ Errore: Il file '{INPUT_FILE}' non è stato trovato nella cartella.")
    except Exception as e:
        print(f"❌ Si è verificato un errore: {e}")

if __name__ == "__main__":
    main()