import pandas as pd
import json
import ast
import re
import sys
from datetime import datetime

def flatten_regio_dataset(input_file, output_prefix):
    print("Avvio elaborazione: Estrazione TITOLO PRODUZIONE da Fullpath...\n")

    try:
        df = pd.read_csv(input_file, sep=';', encoding='utf-8')
    except Exception as e:
        print(f"ERRORE lettura file: {e}")
        sys.exit(1)

    column_mapping = {
        'id': 'production_id',
        'Crediti artistici': 'artistic_credits',
        'Crediti tecnici': 'technical_credits',
        'fullpath': 'full_path',
        'composizioni_collegate': 'related_compositions', 
        'from': 'performance_start_date',
        'to': 'performance_end_date',
        'datetext': 'date_text',
        'source_id': 'source_id',
        'luogo_prima_rappresentazione': 'first_location_path',
        'edificio_prima_rappresentazione': 'first_venue_path'
    }
    df = df.rename(columns=column_mapping)

    def extract_clean_info(path):
       
        if pd.isna(path) or not isinstance(path, str) or path.strip() == '':
            return '', ''
        
        # 1. Gestione percorsi multipli (prende il primo)
        first_path = path.split(',')[0].strip()
        
        # 2. Prende l'ultimo segmento dopo lo slash
        last_segment = first_path.split('/')[-1].strip()
        
        # 3. REGEX:
        # ^(?:\d+\s+)?   -> Ignora numeri iniziali seguiti da spazio (es "4 ")
        # (.*?)          -> CATTURA il Titolo (Gruppo 1)
        # \s*\((\d+)\)\s*$ -> CATTURA l'ID finale (Gruppo 2)
        match = re.search(r'^(?:\d+\s+)?(.*?)\s*\((\d+)\)\s*$', last_segment)
        
        if match:
            return match.group(1).strip(), match.group(2).strip()
        
        # Fallback: se non c'è ID, pulisci almeno il numero iniziale
        clean_fallback = re.sub(r'^\d+\s+', '', last_segment)
        return clean_fallback, ''

    print("Estrazione Titoli e ID...")
    
    # 1. Titolo Produzione (dal full_path)

    df['work_title'], _ = zip(*df['full_path'].apply(extract_clean_info))
    
    # 2. ID Opera collegata (da related_compositions) - Solo l'ID mi serve qui
    def extract_work_id_only(path):
        _, wid = extract_clean_info(path)
        return wid
    
    df['related_work_id'] = df['related_compositions'].apply(extract_work_id_only)

    # 3. Luoghi ed Edifici (Stessa logica di pulizia)
    df['first_location'], _ = zip(*df['first_location_path'].apply(extract_clean_info))
    df['first_venue'], _ = zip(*df['first_venue_path'].apply(extract_clean_info))

    # --- GESTIONE DATE ---
    for col in ['performance_start_date', 'performance_end_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    df['year'] = df['performance_start_date'].dt.year.fillna(0).astype(int)
    df['year'] = df['year'].apply(lambda x: x if x != 0 else '')

    # --- PARSING CREDITI ---
    def parse_credits_safe(value):
        if pd.isna(value) or value == '': return []
        try: return json.loads(value)
        except:
            try: return ast.literal_eval(value)
            except: return []

    records = []
    print("Espansione crediti...")
    for _, row in df.iterrows():
        prod_id = row['production_id']
        for c_type, col_name in [('artistic', 'artistic_credits'), ('technical', 'technical_credits')]:
            for c in parse_credits_safe(row.get(col_name)):
                records.append({
                    'production_id': prod_id,
                    'credit_type': c_type,
                    'person_id': c.get('Identificativo') or c.get('identificativo'),
                    'person_name': c.get('Nome'),
                    'person_role': c.get('Ruolo')
                })

    credits_df = pd.DataFrame(records)

    # --- MERGE FINALE ---
    prod_cols = [
        'production_id', 'work_title', 
        'performance_start_date', 'performance_end_date', 'year', 'date_text', 
        'first_location', 'first_venue', 
        'related_work_id', 'source_id'
    ]
    performances_df = df[prod_cols].copy()

    if not credits_df.empty:
        full_df = credits_df.merge(performances_df, on='production_id', how='left')
    else:
        full_df = performances_df

    # --- SALVATAGGIO ---
    try:
        cols = [
            'production_id', 'credit_type', 'person_id', 'person_name', 'person_role',
            'work_title', 
            'performance_start_date', 'performance_end_date', 'year',
            'date_text', 'first_location', 'first_venue', 
            'related_work_id', 'source_id'
        ]
        
        final_cols = [c for c in cols if c in full_df.columns]
        
        output_file = f"{output_prefix}_regio_produzioni.csv"
        full_df[final_cols].to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nSUCCESS: File creato: {output_file}")
        
        # Anteprima per verifica
        print("\nAnteprima estrazione:")
        print(full_df[['production_id', 'work_title']].drop_duplicates().head(5))
        
    except Exception as e:
        print(f"Errore salvataggio: {e}")


# === ESECUZIONE ===
if __name__ == "__main__":
    # Sostituisci con il tuo percorso reale
    input_path = "dataset/regio/produzioni/20251103_export_produzioni_regio.csv"
    output_prefix = "dataset/regio/"
    
    flatten_regio_dataset(input_path, output_prefix)