import pandas as pd

# === CONFIGURAZIONE ===
INPUT_FILE = 'dataset/regio/recite_regio_final.csv'  # Sostituisci con il nome del tuo file
OUTPUT_FILE = 'dataset/regio/Recite/recite_regio_luoghi.csv'

def main():
    print(f"Lettura del file: {INPUT_FILE}...")
    
    try:
        # Legge il CSV (assume separatore virgola, cambia se necessario)
        df = pd.read_csv(INPUT_FILE)
        
        # Verifica che le colonne esistano
        if 'edificio_nome' not in df.columns or 'luogo_nome' not in df.columns:
            print("Errore: Le colonne 'edificio_nome' o 'luogo_nome' non esistono nel file.")
            return

        # 1. Seleziona solo le colonne di interesse
        teatri = df[['edificio_nome', 'luogo_nome']].copy()

        # 2. Rimuove righe dove l'edificio è vuoto/NaN
        teatri = teatri.dropna(subset=['edificio_nome'])
        
        # 3. Rimuove i duplicati per avere una lista unica
        teatri_unici = teatri.drop_duplicates().sort_values(by=['luogo_nome', 'edificio_nome'])

        # 4. Salva il risultato
        teatri_unici.to_csv(OUTPUT_FILE, index=False)
        
        print(f"Fatto! Estratti {len(teatri_unici)} teatri unici.")
        print(f"File salvato come: {OUTPUT_FILE}")
        
        # Mostra un'anteprima
        print("\nAnteprima:")
        print(teatri_unici.head())

    except FileNotFoundError:
        print(f"Errore: Il file '{INPUT_FILE}' non è stato trovato.")
    except Exception as e:
        print(f"Si è verificato un errore: {e}")

if __name__ == "__main__":
    main()