import pandas as pd
import json
import re
from ast import literal_eval

# === CONFIG ===
INPUT = "dataset/regio/recite/20251111-Regio-Export-Recite-csv.csv"
OUTPUT_MAIN = "dataset/regio/recite/recite_regio_clean.csv"
OUTPUT_INTERPRETI = "dataset/regio/recite/recite_interpreti.csv"
OUTPUT_CURATORI = "dataset/regio/recite/recite_curatori.csv"
OUTPUT_ESECUTORI = "dataset/regio/recite/recite_esecutori.csv"
OUTPUT_FINAL = "dataset/regio/recite/recite_regio_final.csv"


# === FUNZIONI ===
def try_parse_json(value):
    if pd.isna(value) or not isinstance(value, str) or not value.strip():
        return None
    try:
        return json.loads(value)
    except Exception:
        try:
            return literal_eval(value)
        except Exception:
            return None

def clean_id(value):
    """
    # FIX: Funzione helper per pulire qualsiasi ID
    Trasforma in stringa, toglie spazi e rimuove il .0 finale
    """
    if pd.isna(value) or value == "":
        return ""
    s = str(value).strip()
    if s.endswith(".0"):
        return s[:-2]
    return s

def extract_last_label_id(value):
    if pd.isna(value):
        return None, None
    s = str(value).strip()
    last_seg = s.rsplit('/', 1)[-1]
    m = re.match(r"^(.*)\s*\((\d+)\)\s*$", last_seg)
    if m:
        return m.group(1).strip(), m.group(2)
    return last_seg.strip(), None

def extract_path_label(value):
    if pd.isna(value):
        return None
    match = re.search(r"/([^/]+)\s*\(\d+\)$", str(value))
    return match.group(1).strip() if match else value

def extract_production_id(fullpath):
    if pd.isna(fullpath): return None
    parts = [p for p in fullpath.split('/') if p.strip()]
    if len(parts) >= 2:
        production_part = parts[-2]
        match = re.search(r'\((\d+)\)$', production_part)
        if match:
            return match.group(1)
    return None

def extract_altre_recite_ids(value):
    if pd.isna(value):
        return ""
    ids = re.findall(r"\((\d+)\)", str(value))
    return ", ".join(ids)

def parse_personaggi(json_data):
    results = []
    if not json_data:
        return results
    for item in json_data:
        # FIX: Pulizia immediata dell'ID estratto dal JSON
        pid = clean_id(item.get("Identificativo", ""))
        nome = item.get("Nome", "").strip()
        ruolo = item.get("Ruolo", "").strip()

        parts = nome.split(" - ", 1)
        personaggio_raw = parts[0]
        interprete = parts[1] if len(parts) > 1 else ""

        match = re.match(r"^(.*)\(([^)]+)\)\s*$", personaggio_raw.strip())
        if match:
            personaggio_nome = match.group(1).strip()
            personaggio_voce = match.group(2).strip()
        else:
            personaggio_nome = personaggio_raw.strip()
            personaggio_voce = ""

        results.append((personaggio_nome, personaggio_voce, interprete.strip(), pid, ruolo))
    return results

def parse_generic_dict(json_data, key):
    results = []
    if not json_data or not isinstance(json_data, dict):
        return results
    target_key = f"{key}:each" if f"{key}:each" in json_data else key
    items = json_data.get(target_key, [])
    if not isinstance(items, list): return []

    for item in items:
        # FIX: Pulizia immediata dell'ID
        pid = clean_id(item.get("Identificativo", ""))
        nome = item.get("Nome", "").strip()
        ruolo = item.get("Ruolo", "").strip()
        results.append((nome, pid, ruolo))
    return results

# === LETTURA ===
print("Lettura file originale...")
# FIX: dtype=str forza Pandas a leggere tutto come testo, evitando conversioni automatiche in numeri
df = pd.read_csv(INPUT, dtype=str)

# === ESTRAZIONE ===
print("Estrazione metadati base...")
df["titolo_breve"] = df["fullpath"].apply(extract_path_label)
df["production_id"] = df["fullpath"].apply(extract_production_id)

df[["luogo_nome", "luogo_id"]] = df["luogo_rappresentazione"].apply(lambda x: pd.Series(extract_last_label_id(x)))
df[["edificio_nome", "edificio_id"]] = df["edificio_rappresentazione"].apply(lambda x: pd.Series(extract_last_label_id(x)))
df[["composizione_nome", "composizione_id"]] = df["composizioni_collegate"].apply(lambda x: pd.Series(extract_last_label_id(x)))
df["altre_recite_ids"] = df["altre_recite"].apply(extract_altre_recite_ids)

# === PARSING ===
print("Parsing Interpreti...")
interpreti_rows = []
for _, row in df.iterrows():
    json_data = try_parse_json(row.get("Personaggi e interpreti - json"))
    if not json_data: continue
    items = json_data if isinstance(json_data, list) else []
    for p_nome, p_voce, interp, pid, ruolo in parse_personaggi(items):
        interpreti_rows.append({
            "id_recita": row["id"],
            "personaggio": p_nome,
            "personaggio_voce": p_voce,
            "interprete": interp,
            "interprete_id": pid,
            "ruolo": ruolo
        })
df_interpreti = pd.DataFrame(interpreti_rows)

print("Parsing Curatori...")
curatori_rows = []
for _, row in df.iterrows():
    json_data = try_parse_json(row.get("Curatori Esecuzione Musicale - json"))
    if not json_data: continue
    for nome, pid, ruolo in parse_generic_dict(json_data, "curatori_esecuzione_musicale"):
        curatori_rows.append({
            "id_recita": row["id"],
            "curatore_nome": nome,
            "curatore_id": pid,
            "curatore_ruolo": ruolo
        })
df_curatori = pd.DataFrame(curatori_rows)

print("Parsing Esecutori...")
esecutori_rows = []
for _, row in df.iterrows():
    json_data = try_parse_json(row.get("Esecutori - json"))
    if not json_data: continue
    for nome, pid, ruolo in parse_generic_dict(json_data, "esecutori"):
        esecutori_rows.append({
            "id_recita": row["id"],
            "esecutore_nome": nome,
            "esecutore_id": pid,
            "esecutore_ruolo": ruolo
        })
df_esecutori = pd.DataFrame(esecutori_rows)

# === MERGE FINALE ===
print("Creazione file finale unificato...")
final = (
    df[[
        "id", "titolo_breve", "production_id", "from", "to", "datetext",   
        "luogo_nome", "luogo_id", "edificio_nome", "edificio_id",
        "composizione_nome", "composizione_id", "fullpath"
    ]]
    .merge(df_curatori, left_on="id", right_on="id_recita", how="left")
    .merge(df_esecutori, left_on="id", right_on="id_recita", how="left", suffixes=("", "_esecutore"))
    .merge(df_interpreti, left_on="id", right_on="id_recita", how="left", suffixes=("", "_interprete"))
)

# === FIX FINALE PER SICUREZZA ===
# Questo blocco passa su TUTTE le colonne e se trova una stringa che finisce con .0 la pulisce.
# È la "rete di sicurezza" finale.
print("Pulizia finale float (.0)...")
for col in final.columns:
    # Applichiamo la pulizia solo se la colonna è di tipo object (stringa) o numerico
    final[col] = final[col].astype(str).replace(r'\.0$', '', regex=True)
    # Rimpiazza i "nan" (stringa) con stringa vuota per pulizia visiva
    final[col] = final[col].replace('nan', '')

# === SALVATAGGIO ===
final.to_csv(OUTPUT_FINAL, index=False)

print("✅ Processo completato.")
print(f" - File finale: {OUTPUT_FINAL}")