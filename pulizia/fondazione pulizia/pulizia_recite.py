import pandas as pd
import json
import re
from ast import literal_eval
import numpy as np 

# === CONFIG ===
# Adatta questi percorsi al tuo ambiente
INPUT = "dataset/fondazione/Recite/20251120_recite_collegate.csv" 
OUTPUT_MAIN = "dataset/fondazione/Recite/recite_clean.csv"
OUTPUT_INTERPRETI = "dataset/fondazione/Recite/recite_interpreti.csv"
OUTPUT_CURATORI = "dataset/fondazione/Recite/recite_curatori.csv"
OUTPUT_ESECUTORI = "dataset/fondazione/Recite/recite_esecutori.csv"
OUTPUT_FINAL = "dataset/fondazione/recite.csv"

# Nomi delle nuove colonne nel CSV adattato
COL_FULLPATH = "fullpath"
COL_DC_TITLE = "dcTitle"
COL_LUOGHI = "Luoghi"
COL_PERSONE = "Persone"
COL_ENTI = "Enti"
COL_OPERE_MUSICALI = "operemusicali_collegate"


# === FUNZIONI ===

def clean_id(value):
    """
    Rimuove .0 e pulisce gli ID
    """
    if pd.isna(value) or value == "" or str(value).lower() == "nan":
        return ""
    s = str(value).strip()
    if s.endswith(".0"):
        return s[:-2]
    return s

def fix_name_format(name):
    """
    NUOVA FUNZIONE: Converte 'Cognome, Nome' in 'Nome Cognome'.
    Es: 'Brown, Antonia' -> 'Antonia Brown'
    Es: 'Neschling, John' -> 'John Neschling'
    """
    if pd.isna(name) or not isinstance(name, str):
        return name
    
    # Se c'è una virgola, provo a splittare
    if ',' in name:
        parts = name.split(',', 1)
        # Se ho esattamente due parti (Cognome e Nome)
        if len(parts) == 2:
            cognome = parts[0].strip()
            nome = parts[1].strip()
            return f"{nome} {cognome}"
            
    return name

def try_parse_json(value):
    if pd.isna(value) or not isinstance(value, str) or not value.strip():
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        try:
            return literal_eval(value)
        except Exception:
            return None

def extract_path_label(value, col_title):
    if pd.isna(value):
        return col_title if not pd.isna(col_title) else None
    s = str(value).strip()
    match = re.search(r"/([^/]+)\s*\(\d+\)$", s)
    if match:
        label = match.group(1).strip()
        date_match = re.match(r"^\d{2}-\d{2}-\d{4}\s+(.*)$", label)
        return date_match.group(1).strip() if date_match else label
    return col_title if not pd.isna(col_title) else None

def extract_production_id(fullpath):
    return None

def extract_composizione(value):
    if pd.isna(value):
        return None, None
    s = str(value).strip()
    last_seg = s.rsplit('/', 1)[-1]
    m = re.match(r"^(.*)\s*\((\d+)\)\s*$", last_seg)
    if m:
        return m.group(1).strip(), clean_id(m.group(2))
    return last_seg.strip(), None

def parse_luoghi(json_data):
    luogo_nome, luogo_id = None, None
    edificio_nome, edificio_id = None, None
    
    if not isinstance(json_data, list):
        return luogo_nome, luogo_id, edificio_nome, edificio_id
        
    for item in json_data:
        nome = item.get("nome", "").strip()
        id_val = clean_id(item.get("Id", ""))
        relazione = item.get("relazione", "").strip().lower()
        
        if "luogo della" in relazione:
            luogo_nome = nome
            luogo_id = id_val
        elif "edificio della" in relazione:
            edificio_nome = nome
            edificio_id = id_val
            
    return luogo_nome, luogo_id, edificio_nome, edificio_id

def parse_persone(json_data):
    interpreti = []
    curatori = []
    
    if not isinstance(json_data, list):
        return interpreti, curatori
        
    for item in json_data:
        pid = clean_id(item.get("Identificativo", ""))
        nome = item.get("Nome", "").strip()
        ruolo = item.get("Ruolo", "").strip() 
        relazione = item.get("Relazione", "").strip().lower()
        personaggio_raw = item.get("Personaggio", "").strip() 

        if relazione == "interprete":
            personaggio_nome = personaggio_raw
            personaggio_voce = ruolo
            interpreti.append((personaggio_nome, personaggio_voce, nome, pid, relazione))
        elif relazione:
            curatore_ruolo = relazione if not ruolo else f"{ruolo} ({relazione})"
            curatori.append((nome, pid, curatore_ruolo))
            
    return interpreti, curatori

def parse_enti(json_data):
    esecutori = []
    if not isinstance(json_data, list):
        return esecutori
    for item in json_data:
        pid = clean_id(item.get("Identificativo", ""))
        nome = item.get("Nome", "").strip()
        ruolo = item.get("Ruolo", "").strip()
        esecutori.append((nome, pid, ruolo))
    return esecutori


print("Lettura file originale...")
try:
    df = pd.read_csv(INPUT, encoding='utf-8', sep=';', dtype=str)
except UnicodeDecodeError:
    try:
        df = pd.read_csv(INPUT, encoding='latin1', sep=';', dtype=str)
    except Exception as e:
        print(f"ERRORE GRAVE: {e}")
        exit()
except FileNotFoundError:
    print(f"ERRORE: File non trovato: {INPUT}")
    exit()

df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df.columns = df.columns.str.strip()


print("Conversione colonne JSON...")
df[COL_LUOGHI] = df[COL_LUOGHI].apply(try_parse_json)
df[COL_PERSONE] = df[COL_PERSONE].apply(try_parse_json)
df[COL_ENTI] = df[COL_ENTI].apply(try_parse_json)


print("Estrazione metadati...")
df["titolo_breve"] = df.apply(lambda row: extract_path_label(row[COL_FULLPATH], row[COL_DC_TITLE]), axis=1)
df["production_id"] = df[COL_FULLPATH].apply(extract_production_id)

luoghi_data = df[COL_LUOGHI].apply(lambda x: pd.Series(parse_luoghi(x), index=["luogo_nome", "luogo_id", "edificio_nome", "edificio_id"]))
df = pd.concat([df, luoghi_data], axis=1)

composizione_data = df[COL_OPERE_MUSICALI].apply(lambda x: pd.Series(extract_composizione(x), index=["composizione_nome", "composizione_id"]))
df = pd.concat([df, composizione_data], axis=1)


print("Parsing Persone...")
interpreti_rows = []
curatori_rows = []

for _, row in df.iterrows():
    interpreti_list, curatori_list = parse_persone(row[COL_PERSONE])
    id_recita = clean_id(row["id"])
    
    for p_nome, p_voce, interp, pid, rel in interpreti_list:
        interpreti_rows.append({
            "id_recita": id_recita,
            "personaggio": p_nome,
            "personaggio_voce": p_voce, 
            "interprete": interp,
            "interprete_id": pid,
            "ruolo": rel
        })

    for nome, pid, ruolo in curatori_list:
        curatori_rows.append({
            "id_recita": id_recita,
            "curatore_nome": nome,
            "curatore_id": pid,
            "curatore_ruolo": ruolo
        })
        
df_interpreti = pd.DataFrame(interpreti_rows)
df_curatori = pd.DataFrame(curatori_rows)

# === FIX FORMATO NOMI (Cognome, Nome -> Nome Cognome) ===
print("Correzione formato nomi (Cognome, Nome -> Nome Cognome)...")
if not df_interpreti.empty and 'interprete' in df_interpreti.columns:
    df_interpreti['interprete'] = df_interpreti['interprete'].apply(fix_name_format)

if not df_curatori.empty and 'curatore_nome' in df_curatori.columns:
    df_curatori['curatore_nome'] = df_curatori['curatore_nome'].apply(fix_name_format)


print("Parsing Enti...")
esecutori_rows = []
for _, row in df.iterrows():
    id_recita = clean_id(row["id"])
    esecutori_list = parse_enti(row[COL_ENTI])
    for nome, pid, ruolo in esecutori_list:
        esecutori_rows.append({
            "id_recita": id_recita,
            "esecutore_nome": nome,
            "esecutore_id": pid,
            "esecutore_ruolo": ruolo 
        })
df_esecutori = pd.DataFrame(esecutori_rows)


print("Salvataggio file intermedi...")
main_cols = [
    "id", "titolo_breve", "production_id", COL_DC_TITLE, "from", "to", "datetext",
    "luogo_nome", "luogo_id", "edificio_nome", "edificio_id",
    "composizione_nome", "composizione_id", COL_FULLPATH
]
df_main_out = df[main_cols].copy()

for col in df_main_out.columns:
    df_main_out[col] = df_main_out[col].astype(str).replace(r'\.0$', '', regex=True).replace('nan', '')

df_main_out.to_csv(OUTPUT_MAIN, index=False)
df_interpreti.to_csv(OUTPUT_INTERPRETI, index=False)
df_curatori.to_csv(OUTPUT_CURATORI, index=False)
df_esecutori.to_csv(OUTPUT_ESECUTORI, index=False)


# === MERGE FINALE ===
print("Creazione file finale unificato...")
df_base = df[[
    "id", "titolo_breve", "production_id", "from", "to", "datetext",   
    "luogo_nome", "luogo_id", "edificio_nome", "edificio_id",
    "composizione_nome", "composizione_id", COL_FULLPATH
]].copy()

final = (
    df_base
    .merge(df_curatori, left_on="id", right_on="id_recita", how="left")
    .merge(df_esecutori, left_on="id", right_on="id_recita", how="left", suffixes=("", "_esecutore"))
    .merge(df_interpreti, left_on="id", right_on="id_recita", how="left", suffixes=("_curatore", "_interprete"))
)

final = final.drop(columns=['id_recita_curatore', 'id_recita_esecutore', 'id_recita_interprete'], errors='ignore')

# === FIX FINALE PER RIMUOVERE FLOAT (.0) ===
print("Pulizia finale di tutti i campi...")
for col in final.columns:
    final[col] = final[col].astype(str).replace('nan', '').replace(r'\.0$', '', regex=True)

# === SALVATAGGIO ===
final.to_csv(OUTPUT_FINAL, index=False)

print("Processo completato.")
print(f" - File finale: {OUTPUT_FINAL}")