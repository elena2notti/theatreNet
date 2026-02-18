import pandas as pd
import os

# === CONFIGURAZIONE PERCORSI ===
INPUT_RECITE = "dataset/fondazione/recite.csv"
INPUT_PERSONE = "dataset/fondazione/persone.csv"
OUTPUT_FINAL = "dataset/fondazione/recite/recite_fixed_ids.csv"

# === FUNZIONI DI PULIZIA ===
def clean_id(val):
    """Rimuove .0 e converte in stringa pulita"""
    if pd.isna(val) or val == "" or str(val).lower() == "nan":
        return ""
    s = str(val).strip()
    if s.endswith(".0"):
        return s[:-2]
    return s

def clean_name(val):
    if pd.isna(val): return ""
    return str(val).strip()

def flip_name(name):
    if "," in name:
        parts = name.split(",", 1)
        if len(parts) == 2:
            return f"{parts[1].strip()} {parts[0].strip()}"
    return name

# === 1. CARICAMENTO MASTER (PERSONE) ===
print("--- 1. Caricamento Mappa Persone (Master) ---")
try:
    df_persone = pd.read_csv(INPUT_PERSONE, dtype=str)
except Exception as e:
    print(f"Errore caricamento persone: {e}")
    exit()

id_map = {}
for _, row in df_persone.iterrows():
    nome = clean_name(row.get('dcTitle', ''))
    pid = clean_id(row.get('id', ''))
    if nome and pid:
        id_map[nome] = pid

print(f"✅ Mappatura creata su {len(id_map)} persone.")

# === 2. ELABORAZIONE RECITE ===
print("\n--- 2. Correzione File Recite ---")
try:
    # 1. Leggi tutto come stringa
    df_recite = pd.read_csv(INPUT_RECITE, dtype=str)
    # 2. Riempi i NaN con stringhe vuote SUBITO per evitare conversioni automatiche
    df_recite = df_recite.fillna("")
except Exception as e:
    print(f"Errore caricamento recite: {e}")
    exit()

updated_interpreti = 0
updated_curatori = 0

def find_id_in_map(raw_name, mapping):
    name = clean_name(raw_name)
    if not name: return None
    if name in mapping: return mapping[name]
    flipped = flip_name(name)
    if flipped in mapping: return mapping[flipped]
    return None

def update_row(row):
    global updated_interpreti, updated_curatori
    
    # --- A. CORREZIONE INTERPRETI ---
    nome_int = str(row.get('interprete', ''))
    old_id_int = clean_id(row.get('interprete_id', ''))
    
    found_id_int = find_id_in_map(nome_int, id_map)
    
    if found_id_int:
        if found_id_int != old_id_int:
            # FIX: Forziamo esplicitamente a stringa qui
            row['interprete_id'] = str(found_id_int)
            updated_interpreti += 1
    else:
        row['interprete_id'] = str(old_id_int) # FIX: Sempre stringa

    # --- B. CORREZIONE CURATORI ---
    nome_cur = str(row.get('curatore_nome', ''))
    old_id_cur = clean_id(row.get('curatore_id', ''))
    
    found_id_cur = find_id_in_map(nome_cur, id_map)
    
    if found_id_cur:
        if found_id_cur != old_id_cur:
            row['curatore_id'] = str(found_id_cur) # FIX
            updated_curatori += 1
    else:
        row['curatore_id'] = str(old_id_cur) # FIX

    # --- C. CORREZIONE ESECUTORI ---
    if 'esecutore_id' in row:
        row['esecutore_id'] = str(clean_id(row['esecutore_id'])) # FIX
        
    return row

print("   Analisi e sostituzione in corso...")
df_recite = df_recite.apply(update_row, axis=1)

print(f"✅ Correzione completata.")
print(f"   -> Interpreti riconciliati: {updated_interpreti}")
print(f"   -> Curatori riconciliati: {updated_curatori}")

# === 3. SALVATAGGIO E PULIZIA FINALE KILLER ===
print("\n--- 3. Salvataggio ---")

# FIX FINALE: Questo passaggio sovrascrive qualsiasi decisione stupida presa da Pandas
# durante l'apply. Rimuove .0 da TUTTE le colonne prima di salvare.
for col in df_recite.columns:
    df_recite[col] = df_recite[col].astype(str).replace('nan', '').replace(r'\.0$', '', regex=True)

folder_path = os.path.dirname(OUTPUT_FINAL)
if folder_path and not os.path.exists(folder_path):
    os.makedirs(folder_path)

df_recite.to_csv(OUTPUT_FINAL, index=False)
print(f"💾 File salvato: {OUTPUT_FINAL}")