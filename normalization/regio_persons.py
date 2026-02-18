import pandas as pd
import re

def pulisci_voci_autorita(value):
    """Rimuove path e ID, restituendo solo i nomi"""
    if not isinstance(value, str) or not value.strip():
        return ""
    
    autori = []
    
    # Cerca tutti i pattern: /Voci.../Nome (ID)
    matches = re.findall(r'/Voci di autorità/Persone/([^\(]+) \(\d+\)', value)
    
    if matches:
        for match in matches:
            autori.append(match.strip())
    else:
        nome = re.sub(r'.*/', '', value)  # Rimuove tutto fino all'ultimo slash
        nome = re.sub(r'\s*\(\d+\)\s*', '', nome)  # Rimuove (numero)
        if nome.strip():
            autori.append(nome.strip())
    
    return ", ".join(autori)

def estrai_id_voci_autorita(value):
    """Estrae solo gli ID numerici, se presenti"""
    if not isinstance(value, str) or not value.strip():
        return ""
    
    ids = re.findall(r'\((\d+)\)', value)
    return ", ".join(ids)

input_file = "dataset/opere/regio-composizioni-clean-wiki-reconciled-xlsx-csv.csv"
df = pd.read_csv(input_file)
colonna = "autore_opera_letteraria"
if colonna not in df.columns:
    print(f"Colonna '{colonna}' non trovata nel dataset")
    print(f"Colonne disponibili: {list(df.columns)}")
else:
    # Crea due nuove colonne con i risultati
    df["autori_nome_pulito"] = df[colonna].apply(pulisci_voci_autorita)
    df["autori_id"] = df[colonna].apply(estrai_id_voci_autorita)

    output_file = "dataset/opere/regio-composizioni-clean-pulito.csv"
    df.to_csv(output_file, index=False)

    print(f"File pulito salvato in: {output_file}")
    