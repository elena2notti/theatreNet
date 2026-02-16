import pandas as pd
import re


INPUT_CSV = "dataset/fondazione/Stagioni/20251124-export-stagioni-teatri-reggioemilia.csv"
OUTPUT_CSV = "dataset/fondazione/stagioni.csv"

COLONNE_DA_PULIRE = [
    "produzioni_collegate",
    "manifestazioni_recite_concerti_collegati",
    "operemusicali_collegate",
    "persone_collegate",
    "enti_collegati",
    "luoghi_collegati"
]

def estrai_id(cella):
    """
    Estrae tutti gli ID nel formato (12345)
    e restituisce una stringa "12345, 67890".
    """
    if not isinstance(cella, str):
        return ""

    ids = re.findall(r'\((\d+)\)', cella)
    return ", ".join(ids)


df = pd.read_csv(
    INPUT_CSV,
    sep=";",          # separatore corretto
    engine="python",  # parser più tollerante
    quotechar='"',    # nel caso ci siano virgolette
    on_bad_lines="skip"  # evita crash se ci sono righe rotte
)

# Per ogni colonna target creo una versione pulita con solo ID
for col in COLONNE_DA_PULIRE:
    if col in df.columns:
        nuova_colonna = col + "_id"
        df[nuova_colonna] = df[col].apply(estrai_id)
        print(f"Creata colonna: {nuova_colonna}")
    else:
        print(f"Colonna non trovata nel dataset: {col}")

df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
print(f"\nFile pulito salvato in: {OUTPUT_CSV}")
