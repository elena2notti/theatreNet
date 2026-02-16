import pandas as pd

# === CONFIGURAZIONE PERCORSI ===
# 1. Il file delle recite che hai già generato (quello con gli ID "sbagliati" o misti)
INPUT_RECITE = "dataset/regio/recite/recite_regio_final.csv"

# 2. Il file "Master" delle persone che contiene gli ID corretti (es. 2502, 2644)
INPUT_PERSONE = "dataset/regio/regio_persone.csv"

# 3. Dove salvare il file corretto
OUTPUT_FINAL = "dataset/regio/recite/recite_regio_final_fixed_ids.csv"


# === FUNZIONI DI PULIZIA ===
def clean_id(val):
    """Rimuove .0 e converte in stringa pulita"""
    if pd.isna(val): return ""
    s = str(val).strip()
    if s.endswith(".0"):
        return s[:-2]
    return s

def clean_name(val):
    """Rimuove spazi extra dai nomi per migliorare il confronto"""
    if pd.isna(val): return ""
    return str(val).strip()

# === 1. CARICAMENTO MASTER (PERSONE) ===
print("--- 1. Caricamento Mappa Persone (Master) ---")
# Leggiamo tutto come stringa per evitare float indesiderati
df_persone = pd.read_csv(INPUT_PERSONE, dtype=str)

# Creiamo il dizionario di mappatura: { "Nome Pulito": "ID Ufficiale" }
# Usiamo le colonne viste nel tuo snippet: 'full_name' e 'person_id'
id_map = {}

for _, row in df_persone.iterrows():
    nome = clean_name(row.get('full_name', ''))
    pid = clean_id(row.get('person_id', ''))
    
    if nome and pid:
        id_map[nome] = pid

print(f"✅ Mappatura creata su {len(id_map)} persone.")
print(f"   Esempio: 'Georges Bizet' -> ID '{id_map.get('Georges Bizet', 'Non trovato')}'")

# === 2. ELABORAZIONE RECITE ===
print("\n--- 2. Correzione File Recite ---")
df_recite = pd.read_csv(INPUT_RECITE, dtype=str)

# Contatori per statistiche
updated_interpreti = 0
updated_curatori = 0

# Funzione per aggiornare una riga
def update_row(row):
    global updated_interpreti, updated_curatori
    
    # --- A. CORREZIONE INTERPRETI ---
    nome_int = clean_name(row.get('interprete', ''))
    old_id_int = clean_id(row.get('interprete_id', ''))
    
    # Se il nome è nel dizionario master, USIAMO L'ID MASTER
    if nome_int in id_map:
        new_id_int = id_map[nome_int]
        if new_id_int != old_id_int:
            row['interprete_id'] = new_id_int
            updated_interpreti += 1
    else:
        # Se non c'è nel master, puliamo comunque l'ID vecchio (es. togliamo .0)
        row['interprete_id'] = old_id_int

    # --- B. CORREZIONE CURATORI (BONUS) ---
    # Anche i direttori d'orchestra sono persone, correggiamo anche loro se possibile
    nome_cur = clean_name(row.get('curatore_nome', ''))
    old_id_cur = clean_id(row.get('curatore_id', ''))
    
    if nome_cur in id_map:
        new_id_cur = id_map[nome_cur]
        if new_id_cur != old_id_cur:
            row['curatore_id'] = new_id_cur
            updated_curatori += 1
    else:
        row['curatore_id'] = old_id_cur
        
    return row

# Applichiamo la correzione riga per riga
df_recite = df_recite.apply(update_row, axis=1)

print(f"✅ Correzione completata.")
print(f"   -> Interpreti aggiornati all'ID ufficiale: {updated_interpreti}")
print(f"   -> Curatori aggiornati all'ID ufficiale: {updated_curatori}")

# === 3. SALVATAGGIO ===
print("\n--- 3. Salvataggio ---")
# Rimuoviamo eventuali Nan residui prima di scrivere
df_recite = df_recite.fillna('')

df_recite.to_csv(OUTPUT_FINAL, index=False)
print(f"💾 File salvato: {OUTPUT_FINAL}")
print("   Ora puoi usare questo file nello script Neo4j!")