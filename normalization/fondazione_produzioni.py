import pandas as pd
import json
import ast

INPUT = "dataset/fondazione/Produzioni/20251120_teatri-reggio-emilia-.csv"
OUTPUT = "dataset/fondazione/produzioni.csv"

def safe_parse_json(value):
    """Converte una stringa JSON-like in una lista Python."""
    if not isinstance(value, str) or value.strip() == "":
        return []
    try:
        return json.loads(value)
    except:
        try:
            return ast.literal_eval(value)
        except:
            return []


def extract_location_info(lista):
    """Estrae luogo rappresentazione e edificio se presenti."""
    luogo, luogo_id = "", ""
    edificio, edificio_id = "", ""

    for entry in lista:
        rel = entry.get("relazione", "").lower()

        if "luogo" in rel:
            luogo = entry.get("nome", "")
            luogo_id = entry.get("Id", "")

        if "edificio" in rel:
            edificio = entry.get("nome", "")
            edificio_id = entry.get("Id", "")

    return luogo, luogo_id, edificio, edificio_id

def extract_entities(lista):
    """Restituisce nomi, ID e ruoli da Enti collegati."""
    nomi = []
    ids = []
    ruoli = []

    for entry in lista:
        nomi.append(entry.get("Nome", ""))
        ids.append(str(entry.get("Identificativo", "")))
        ruoli.append(entry.get("Ruolo", ""))

    return (
        ", ".join(nomi),
        ", ".join(ids),
        ", ".join(ruoli)
    )

def extract_people(lista):
    nomi = []
    ids = []
    ruoli = []

    for entry in lista:
        raw = entry.get("Nome", "").strip()

        # Se è nel formato "Cognome, Nome"
        if "," in raw:
            cognome, nome = [x.strip() for x in raw.split(",", 1)]
            nome_completo = f"{nome} {cognome}"
        else:
            nome_completo = raw

        nomi.append(nome_completo)
        ids.append(str(entry.get("Identificativo", "")))
        ruoli.append(entry.get("Ruolo", ""))

    return (
        ", ".join(nomi),
        ", ".join(ids),
        ", ".join(ruoli),
    )

def extract_linked(lista):
    """Estrae elementi collegati (recite, opere, etc.)."""
    nomi, ids = [], []

    for entry in lista:
        nomi.append(entry.get("Nome", ""))
        ids.append(str(entry.get("Identificativo", "")))

    return ", ".join(nomi), ", ".join(ids)


df = pd.read_csv(INPUT, sep=";", engine="python")

# Crea nuove colonne vuote
df["luogo_rappresentazione"] = ""
df["luogo_rappresentazione_id"] = ""
df["edificio_rappresentazione"] = ""
df["edificio_rappresentazione_id"] = ""

df["enti_collegati_clean"] = ""
df["enti_collegati_id"] = ""
df["enti_collegati_ruolo"] = ""

df["persone_collegate_clean"] = ""
df["persone_collegate_id"] = ""
df["persone_collegate_ruolo"] = ""

df["recite_collegate_clean"] = ""
df["recite_collegate_id"] = ""

df["opere_collegate_clean"] = ""
df["opere_collegate_id"] = ""


for idx, row in df.iterrows():

    # --- LUOGHI COLLEGATI ---
    loc_list = safe_parse_json(row.get("Luogo rappresentazione", ""))
    luogo, luogo_id, edificio, edificio_id = extract_location_info(loc_list)

    df.at[idx, "luogo_rappresentazione"] = luogo
    df.at[idx, "luogo_rappresentazione_id"] = luogo_id
    df.at[idx, "edificio_rappresentazione"] = edificio
    df.at[idx, "edificio_rappresentazione_id"] = edificio_id

    # --- ENTI COLLEGATI ---
    enti_list = safe_parse_json(row.get("Enti collegati", ""))
    enti, enti_ids, enti_roles = extract_entities(enti_list)

    df.at[idx, "enti_collegati"] = enti
    df.at[idx, "enti_collegati_id"] = enti_ids
    df.at[idx, "enti_collegati_ruolo"] = enti_roles

    # --- PERSONE COLLEGATE ---
    pers_list = safe_parse_json(row.get("Persone collegate", ""))
    pers, pers_ids, pers_roles = extract_people(pers_list)

    df.at[idx, "persone_collegate"] = pers
    df.at[idx, "persone_collegate_id"] = pers_ids
    df.at[idx, "persone_collegate_ruolo"] = pers_roles

    # --- RECITE COLLEGATE ---
    rec_list = safe_parse_json(row.get("Recite collegate", ""))
    rec, rec_ids = extract_linked(rec_list)

    df.at[idx, "recite_collegate"] = rec
    df.at[idx, "recite_collegate_id"] = rec_ids

    # --- OPERE COLLEGATE ---
    op_list = safe_parse_json(row.get("Opere musicali collegate", ""))
    op, op_ids = extract_linked(op_list)

    df.at[idx, "opere_collegate"] = op
    df.at[idx, "opere_collegate_id"] = op_ids

# ================================
# SALVATAGGIO
# ================================

df.to_csv(OUTPUT, index=False, encoding="utf-8", sep=";")
print(f"✔️ File pulito salvato in: {OUTPUT}")
