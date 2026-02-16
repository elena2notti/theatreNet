import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, OWL
from pathlib import Path
import unidecode
import re

# ================================================================
# 1. CONFIGURAZIONE
# ================================================================

CSV_OPERE = "https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/regio/regio_opere_pulito_con_anno.csv"
CSV_PERSONE = "https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/regio/regio_persone.csv"
CSV_PRODUZIONI = "https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/regio/regio_produzioni.csv"
CSV_STAGIONI = "https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/regio/regio_stagioni.csv"
CSV_RECITE = "https://media.githubusercontent.com/media/elena2notti/theatreNet/refs/heads/main/regio/recite-regio-luoghi-qid2.csv"

OUTPUT_TTL = Path("semantic_graph/upload_to_neo4j/regio.ttl")

# Namespace
BASE = Namespace("https://teatroregio.it/archivio/data/")
CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
FRBROO = Namespace("http://iflastandards.info/ns/fr/frbr/frbroo/")
CORAGO = Namespace("http://corago.unibo.it/sm/")

# ================================================================
# 2. UTILS CORRETTE
# ================================================================

def clean_id(value):
    """Pulisce ID numerici - VERSIONE ROBUSTA CONTRO .0"""
    if pd.isna(value) or value is None:
        return None
    
    s = str(value).strip()
    if not s or s.lower() in ["nan", "none", "null", "n/a"]:
        return None
    
    # Rimuovi TUTTI i .0 finali (anche multipli)
    original_s = s
    while s.endswith('.0'):
        s = s[:-2]
    
    # Se dopo la rimozione è vuoto, ritorna None
    if not s:
        return None
    
    # Se sembra un numero float, converti a int
    if '.' in s:
        try:
            # Prova a parsare come float e poi converti a int se è intero
            f = float(s)
            if f.is_integer():
                s = str(int(f))
            else:
                # Se non è intero, mantieni come stringa ma pulisci
                s = s.rstrip('0').rstrip('.') if '.' in s else s
        except ValueError:
            pass
    
    return s

def clean_uri(value):
    """Pulisce stringhe per URI - USES clean_id PER NUMERI"""
    if pd.isna(value) or value is None:
        return None
    
    s = str(value).strip()
    if s.lower() in ["none", "nan", "null", "", "n/a"]:
        return None
    
    # Se sembra un ID numerico, usa clean_id
    if re.match(r'^\d+(\.\d+)?$', s):
        cleaned = clean_id(s)
        if cleaned:
            s = cleaned
    
    # Altrimenti procedi con la pulizia normale
    if s.endswith(".0"):
        s = s[:-2]
    
    s = unidecode.unidecode(s)
    s = re.sub(r"\W+", "_", s)
    s = s.strip("_")
    
    return s if s else None

def literal(value):
    if pd.isna(value) or value is None:
        return None
    v = str(value).strip()
    return Literal(v) if v and v.lower() not in ["nan", "none", "null"] else None

def safe_date_literal(value):
    if pd.isna(value) or value is None:
        return None
    val = str(value).strip().split(" ")[0].split("T")[0]
    val = val.replace(".", "-").replace("/", "-")

    if re.match(r"^\d{4}$", val):
        val = f"{val}-01-01"
    elif re.match(r"^\d{4}-\d{1,2}$", val):
        val = f"{val}-01"

    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        return Literal(val, datatype=XSD.date)
    return None

def wikidata_canonical(value):
    """Normalizza URI Wikidata - VERSIONE ROBUSTA"""
    if value is None or pd.isna(value):
        return None
    
    s = str(value).strip().upper()
    
    if not s or s in ["NONE", "NAN", "NULL", ""]:
        return None

    if re.match(r"^Q\d+$", s):
        return URIRef(f"http://www.wikidata.org/entity/{s}")

    if "WIKIDATA.ORG/WIKI/" in s:
        s = s.replace("HTTPS://WWW.WIKIDATA.ORG/WIKI/", "http://www.wikidata.org/entity/")
        s = s.replace("HTTP://WWW.WIKIDATA.ORG/WIKI/", "http://www.wikidata.org/entity/")
        return URIRef(s)

    if "WIKIDATA.ORG/ENTITY/" in s:
        s = s.replace("HTTPS://WWW.WIKIDATA.ORG/ENTITY/", "http://www.wikidata.org/entity/")
        return URIRef(s)

    match = re.search(r'(Q\d+)', s, re.IGNORECASE)
    if match:
        return URIRef(f"http://www.wikidata.org/entity/{match.group(1).upper()}")
    
    return None

def extract_qid(value):
    """Estrai QID da valore Wikidata - SEMPRE MAIUSCOLO"""
    wd = wikidata_canonical(value)
    if wd:
        match = re.search(r'(Q\d+)$', str(wd), re.IGNORECASE)
        return match.group(1).upper() if match else None
    return None

def get_unified_uri(kind, wikidata_qid, label, entity_id=None):
    """
    Crea URI unificati basati su Wikidata QID.
    Se non c'è Wikidata, usa URI locale.
    """
    # Prova con Wikidata
    qid = extract_qid(wikidata_qid)
    
    if qid:
        # URI unificato basato su Wikidata
        if kind == "PERSON":
            return BASE[f"unified_person_{qid}"]
        elif kind == "WORK":
            return BASE[f"unified_work_{qid}"]
        elif kind == "CHAR":
            return BASE[f"unified_character_{qid}"]
        elif kind == "PLACE":
            return BASE[f"unified_place_{qid}"]
    
    # Fallback: URI locale
    # PER ID: usa clean_id (rimuove .0)
    if entity_id:
        key = clean_id(entity_id)
    else:
        key = clean_uri(label)
    
    if not key:
        return None
    
    if kind == "PERSON":
        return BASE[f"regio_person_{key}"]
    elif kind == "WORK":
        return BASE[f"regio_work_{key}"]
    elif kind == "CHAR":
        return BASE[f"regio_character_{key}"]
    elif kind == "PLACE":
        return BASE[f"regio_place_{key}"]
    elif kind == "TYPE":
        return BASE[f"type_{key}"]
    
    return None

def add_sameas_if_wikidata(g, local_uri, qid_or_url):
    """Aggiunge owl:sameAs a Wikidata"""
    wd = wikidata_canonical(qid_or_url)
    if local_uri is None or wd is None:
        return
    
    # Non aggiungere se l'URI è già basato su quello stesso Wikidata
    if "unified_" in str(local_uri):
        local_qid_match = re.search(r'unified_[a-z]+_(Q\d+)', str(local_uri))
        wd_qid = extract_qid(qid_or_url)
        if local_qid_match and wd_qid and local_qid_match.group(1).upper() == wd_qid.upper():
            return
    
    g.add((local_uri, OWL.sameAs, wd))

def add_viaf_sameas(g, local_uri, viaf_value):
    if local_uri is None or viaf_value is None or pd.isna(viaf_value):
        return
    v = str(viaf_value).strip()
    if not v or v.lower() in ["none", "nan", "null"]:
        return
    v = v.replace(".0", "")
    g.add((local_uri, OWL.sameAs, URIRef(f"http://viaf.org/viaf/{v}")))

def add_triple_with_inverse(g, s, p, o):
    if s is None or p is None or o is None:
        return
    g.add((s, p, o))

    inverses = {
        CRM.P14_carried_out_by: CRM.P14i_performed,
        CRM.P9_consists_of: CRM.P9i_forms_part_of,
        CRM.P102_has_title: CRM.P102i_is_title_of,
        CRM.P1_is_identified_by: CRM.P1i_identifies,
        CRM.P2_has_type: CRM.P2i_is_type_of,
        CRM.P4_has_time_span: CRM.P4i_is_time_span_of,
        CRM.P7_took_place_at: CRM.P7i_witnessed,
        FRBROO.R16i_was_initiated_by: FRBROO.R16_initiated,
        FRBROO.R9_is_realised_in: FRBROO.R9i_realises,
        FRBROO.R25_performed: FRBROO.R25i_was_performed_by,
        FRBROO.R17_created: FRBROO.R17i_was_created_by,
        FRBROO.R64_has_character: FRBROO.R64i_is_character_of,
    }
    if p in inverses:
        g.add((o, inverses[p], s))

# ================================================================
# 3. CACHE ENTITÀ CORRETTA
# ================================================================

CACHE = {
    "PERSON": {},    # Chiave: "Q123" o "6441" (NO "6441.0")
    "WORK": {},      # Chiave: "Q789" o "ID_123"
    "CHAR": {},      # Chiave: "Q555" o "LABEL_Nome"
    "PLACE": {},     # Chiave: "Q777" o "LABEL_Luogo"
    "TYPE": {}       # Chiave: "LABEL_Ruolo"
}

g = Graph()

def get_entity(kind, label, entity_id=None, wikidata_qid=None):
    """
    Crea o recupera entità con cache robusta.
    """
    global g

    # Determina chiave cache UNAMBIGUA
    cache_key = None
    
    # 1. Wikidata QID
    if wikidata_qid:
        qid = extract_qid(wikidata_qid)
        if qid:
            cache_key = qid
    
    # 2. ID locale (USA clean_id!)
    if not cache_key and entity_id:
        clean_entity_id = clean_id(entity_id)  # Questo rimuove .0
        if clean_entity_id:
            cache_key = clean_entity_id
    
    # 3. Label pulita
    if not cache_key and label:
        clean_label = clean_uri(label)
        if clean_label:
            cache_key = f"LABEL_{clean_label}"
    
    if not cache_key:
        return None
    
    # VERIFICA CACHE (con gestione .0)
    if cache_key in CACHE[kind]:
        cached_uri = CACHE[kind][cache_key]
        
        # Se l'URI cached contiene .0, correggilo
        uri_str = str(cached_uri)
        if ".0" in uri_str and not uri_str.endswith("/data/"):
            # Crea nuovo URI senza .0
            new_uri_str = uri_str.replace(".0", "")
            new_uri = URIRef(new_uri_str)
            
            # Sostituisci tutte le occorrenze nel grafo
            for s, p, o in list(g):
                if s == cached_uri:
                    g.remove((s, p, o))
                    g.add((new_uri, p, o))
                if o == cached_uri:
                    g.remove((s, p, o))
                    g.add((s, p, new_uri))
            
            # Aggiorna cache
            CACHE[kind][cache_key] = new_uri
            return new_uri
        
        return cached_uri
    
    # Crea nuovo URI
    uri = get_unified_uri(kind, wikidata_qid, label, entity_id)
    if not uri:
        return None
    
    # ASSICURATI che l'URI non contenga .0
    uri_str = str(uri)
    if ".0" in uri_str and not uri_str.endswith("/data/"):
        # Crea nuovo URI senza .0
        new_uri_str = uri_str.replace(".0", "")
        uri = URIRef(new_uri_str)
    
    # Aggiungi tipi RDF
    if kind == "PERSON":
        g.add((uri, RDF.type, CRM.E21_Person))
        g.add((uri, RDF.type, CRM.E39_Actor))
    elif kind == "WORK":
        g.add((uri, RDF.type, FRBROO.F1_Work))
    elif kind == "CHAR":
        g.add((uri, RDF.type, FRBROO.F38_Character))
    elif kind == "PLACE":
        g.add((uri, RDF.type, CRM.E53_Place))
    elif kind == "TYPE":
        g.add((uri, RDF.type, CRM.E55_Type))
    
    # Aggiungi label
    if label:
        add_triple_with_inverse(g, uri, RDFS.label, literal(label))
    
    # Aggiungi Wikidata sameAs
    if wikidata_qid:
        add_sameas_if_wikidata(g, uri, wikidata_qid)
    
    # Salva in cache
    CACHE[kind][cache_key] = uri
    return uri

def add_author_corago(g, opera_uri, name, role_label, wikidata_qid=None):
    """Crea F27 Work Conception e C2 Actor Role (C2 univoco per autore)"""
    if not name or not opera_uri:
        return

    # Persona
    person_uri = get_entity("PERSON", name, wikidata_qid=wikidata_qid)
    if not person_uri:
        return

    role_type_uri = get_entity("TYPE", role_label)

    # Slug stabili
    op_slug = str(opera_uri).split("_")[-1][:30]
    role_slug = (clean_uri(role_label) or "role")[:30]

    # Unicità per autore (evita collasso di più autori nello stesso C2)
    person_key = extract_qid(wikidata_qid)
    if not person_key:
        person_key = (clean_uri(name) or "person")[:30]

    # F27 Work Conception (una per opera+ruolo)
    conception_uri = BASE[f"conception_{op_slug}_{role_slug}"]
    if (conception_uri, RDF.type, FRBROO.F27_Work_Conception) not in g:
        g.add((conception_uri, RDF.type, FRBROO.F27_Work_Conception))
        g.add((conception_uri, RDF.type, CRM.E7_Activity))
        add_triple_with_inverse(
            g,
            conception_uri,
            RDFS.label,
            literal(f"Concezione {role_label} per opera {op_slug}")
        )
        add_triple_with_inverse(g, opera_uri, FRBROO.R16i_was_initiated_by, conception_uri)

    # C2 Actor Role (una per opera+ruolo+persona)
    c2_role_uri = BASE[f"role_{role_slug}_{op_slug}_{person_key}"]
    g.add((c2_role_uri, RDF.type, CORAGO.C2_Actor_Role))
    add_triple_with_inverse(
        g,
        c2_role_uri,
        RDFS.label,
        literal(f"Ruolo {role_label} per opera {op_slug} ({name})")
    )

    # Collegamenti Corago
    g.add((conception_uri, CORAGO.CP2_carried_out_role, c2_role_uri))
    g.add((c2_role_uri, CORAGO.CP3_carried_out_actor, person_uri))

    if role_type_uri:
        add_triple_with_inverse(g, c2_role_uri, CRM.P2_has_type, role_type_uri)

def find_work_safely(wid_clean):
    """
    Cerca opera nella cache senza falsi positivi.
    Usa match esatti sulle chiavi.
    """
    if not wid_clean:
        return None
    
    # 1. Cerca per QID (se wid_clean è un QID)
    if wid_clean.startswith("Q"):
        qid_upper = wid_clean.upper()
        if qid_upper in CACHE["WORK"]:
            return CACHE["WORK"][qid_upper]
    
    # 2. Cerca per ID locale
    if wid_clean in CACHE["WORK"]:
        return CACHE["WORK"][wid_clean]
    
    return None

# ================================================================
# 4. INIZIALIZZAZIONE
# ================================================================

print("=== REGIO TEATRO - GENERAZIONE GRAFO (VERSIONE CORRETTA) ===")
print("=== APPLICATE TUTTE LE CORREZIONI CP2 E RUOLI CONTESTUALI ===")

g.bind("crm", CRM)
g.bind("frbroo", FRBROO)
g.bind("corago", CORAGO)
g.bind("tr", BASE)
g.bind("owl", OWL)

# ================================================================
# 5. OPERE (F1) - SENZA PERSONAGGI
# ================================================================

print("1. Opere...")
opere = pd.read_csv(CSV_OPERE)
opere.columns = opere.columns.str.strip()

for _, row in opere.iterrows():
    # ID opera (usa clean_id per rimuovere .0)
    op_id = clean_id(row.get("compositions_id") or row.get("id") or row.get("composizione_id"))
    if not op_id:
        continue
    
    # Titolo
    title = row.get("dcTitle") or row.get("operaLabel") or f"Opera {op_id}"
    
    # Wikidata
    wikidata_qid = row.get("composizioni_uri") or row.get("wikidata_entity_id")
    
    # Crea opera
    work_uri = get_entity("WORK", title, entity_id=op_id, wikidata_qid=wikidata_qid)
    if not work_uri:
        continue
    
    # Titolo come E35 Title
    title_node = BASE[f"title_work_{op_id}"]
    g.add((title_node, RDF.type, CRM.E35_Title))
    add_triple_with_inverse(g, title_node, RDFS.label, literal(title))
    add_triple_with_inverse(g, work_uri, CRM.P102_has_title, title_node)
    
    # Time-span
    date_from = safe_date_literal(row.get("from"))
    date_to = safe_date_literal(row.get("to"))
    if date_from or date_to:
        ts = BASE[f"ts_work_{op_id}"]
        g.add((ts, RDF.type, CRM.E52_Time_Span))
        if date_from:
            g.add((ts, CRM.P82a_begin_of_the_begin, date_from))
        if date_to:
            g.add((ts, CRM.P82b_end_of_the_end, date_to))
        add_triple_with_inverse(g, work_uri, CRM.P4_has_time_span, ts)
    
    # VIAF
    if row.get("viaf"):
        add_viaf_sameas(g, work_uri, row.get("viaf"))
    
    # Autori
    if row.get("autore_musica_clean"):
        add_author_corago(g, work_uri, row.get("autore_musica_clean"), "Autore musica")
    
    if row.get("autore_testo_clean"):
        add_author_corago(g, work_uri, row.get("autore_testo_clean"), "Autore testo")
    
    # Autore opera letteraria
    literary = row.get("literary_author_name") or row.get("autore_opera_letteraria.1") or row.get("autore_opera_letteraria")
    if literary:
        add_author_corago(g, work_uri, literary, "Autore opera letteraria")
    
    # NOTE: RIMOSSA LA SEZIONE PERSONAGGI DALLE OPERE
    # I personaggi verranno presi dalle recite

# ================================================================
# 6. PERSONE
# ================================================================

print("2. Persone...")
persone = pd.read_csv(CSV_PERSONE)
persone.columns = persone.columns.str.strip()

for _, row in persone.iterrows():
    # ID persona (usa clean_id)
    pid = clean_id(row.get("person_id"))
    name = row.get("full_name") or row.get("original_name")
    if not name:
        continue
    
    # Wikidata
    wikidata_qid = row.get("wikidata_uri") or row.get("wikidata_id")
    
    # Crea persona
    person_uri = get_entity("PERSON", name, entity_id=pid, wikidata_qid=wikidata_qid)
    if not person_uri:
        continue
    
    # VIAF
    add_viaf_sameas(g, person_uri, row.get("viaf"))
    
    # Nascita
    birth = safe_date_literal(row.get("birth_date"))
    if birth:
        birth_id = f"birth_{pid}" if pid else f"birth_{clean_uri(name)}"
        evt = BASE[birth_id]
        g.add((evt, RDF.type, CRM.E67_Birth))
        add_triple_with_inverse(g, evt, RDFS.label, literal(f"Nascita di {name}"))
        
        ts = BASE[f"ts_{birth_id}"]
        g.add((ts, RDF.type, CRM.E52_Time_Span))
        g.add((ts, CRM.P82a_begin_of_the_begin, birth))
        g.add((ts, CRM.P82b_end_of_the_end, birth))
        add_triple_with_inverse(g, evt, CRM.P4_has_time_span, ts)
        add_triple_with_inverse(g, person_uri, CRM.P98i_was_born, evt)
        
        if row.get("birth_place"):
            place_wikidata = extract_qid(row.get("birth_place"))
            place = get_entity("PLACE", row.get("birth_place"), wikidata_qid=place_wikidata)
            if place:
                add_triple_with_inverse(g, evt, CRM.P7_took_place_at, place)
    
    # Morte
    death = safe_date_literal(row.get("death_date"))
    if death:
        death_id = f"death_{pid}" if pid else f"death_{clean_uri(name)}"
        evt_death = BASE[death_id]
        g.add((evt_death, RDF.type, CRM.E69_Death))
        add_triple_with_inverse(g, evt_death, RDFS.label, literal(f"Morte di {name}"))
        
        ts_death = BASE[f"ts_{death_id}"]
        g.add((ts_death, RDF.type, CRM.E52_Time_Span))
        g.add((ts_death, CRM.P82a_begin_of_the_begin, death))
        g.add((ts_death, CRM.P82b_end_of_the_end, death))
        add_triple_with_inverse(g, evt_death, CRM.P4_has_time_span, ts_death)
        add_triple_with_inverse(g, person_uri, CRM.P100i_died_in, evt_death)
        
        if row.get("death_place"):
            place_wikidata = extract_qid(row.get("death_place"))
            place = get_entity("PLACE", row.get("death_place"), wikidata_qid=place_wikidata)
            if place:
                add_triple_with_inverse(g, evt_death, CRM.P7_took_place_at, place)

# ================================================================
# 7. STAGIONI
# ================================================================

print("3. Stagioni...")
stagioni = pd.read_csv(CSV_STAGIONI)
stagioni.columns = stagioni.columns.str.strip()

for _, row in stagioni.iterrows():
    sid = clean_id(row.get("season_id"))
    if not sid:
        continue

    season_uri = BASE[f"season_{sid}"]
    g.add((season_uri, RDF.type, FRBROO.F8_Event_Set))
    add_triple_with_inverse(g, season_uri, RDFS.label, literal(row.get("season_title")))

    # Time-span
    date_from = safe_date_literal(row.get("season_start_date"))
    date_to = safe_date_literal(row.get("season_end_date"))
    if date_from or date_to:
        ts = BASE[f"ts_season_{sid}"]
        g.add((ts, RDF.type, CRM.E52_Time_Span))
        if date_from:
            g.add((ts, CRM.P82a_begin_of_the_begin, date_from))
        if date_to:
            g.add((ts, CRM.P82b_end_of_the_end, date_to))
        add_triple_with_inverse(g, season_uri, CRM.P4_has_time_span, ts)

    # Organizer
    if row.get("organizer_name"):
        organizer_wikidata = extract_qid(row.get("organizer_wikidata"))
        org = get_entity("PERSON", row.get("organizer_name"), 
                        entity_id=row.get("organizer_id"), 
                        wikidata_qid=organizer_wikidata)
        if org:
            add_triple_with_inverse(g, season_uri, CRM.P14_carried_out_by, org)

    # Link a produzioni
    linked = row.get("linked_production_ids")
    if linked:
        for pid in str(linked).split(","):
            pid_clean = clean_id(pid)
            if pid_clean:
                prod_uri = BASE[f"production_{pid_clean}"]
                add_triple_with_inverse(g, season_uri, CRM.P9_consists_of, prod_uri)

# ================================================================
# 8. PRODUZIONI - CON CORREZIONI CP2
# ================================================================

print("4. Produzioni...")
prod_df = pd.read_csv(CSV_PRODUZIONI)
prod_df.columns = prod_df.columns.str.strip()

for _, row in prod_df.iterrows():
    pid = clean_id(row.get("production_id"))
    if not pid:
        continue

    prod_uri = BASE[f"production_{pid}"]
    g.add((prod_uri, RDF.type, FRBROO.F25_Performance_Plan))
    g.add((prod_uri, RDF.type, CRM.E7_Activity))

    title = row.get("work_title") or row.get("titolo") or f"Produzione {pid}"
    add_triple_with_inverse(g, prod_uri, RDFS.label, literal(title))

    # Time-span
    date_start = safe_date_literal(row.get("performance_start_date"))
    date_end = safe_date_literal(row.get("performance_end_date"))
    if date_start or date_end:
        ts = BASE[f"ts_prod_{pid}"]
        g.add((ts, RDF.type, CRM.E52_Time_Span))
        if date_start:
            g.add((ts, CRM.P82a_begin_of_the_begin, date_start))
        if date_end:
            g.add((ts, CRM.P82b_end_of_the_end, date_end))
        add_triple_with_inverse(g, prod_uri, CRM.P4_has_time_span, ts)

    # Luoghi
    if row.get("first_location"):
        place = get_entity("PLACE", row.get("first_location"))
        if place:
            add_triple_with_inverse(g, prod_uri, CRM.P7_took_place_at, place)
    
    if row.get("first_venue"):
        place = get_entity("PLACE", row.get("first_venue"))
        if place:
            add_triple_with_inverse(g, prod_uri, CRM.P7_took_place_at, place)

    # Link Opera
    wid = clean_id(row.get("related_work_id"))
    if wid:
        # Cerca opera
        work_uri = find_work_safely(wid)
        
        if work_uri:
            add_triple_with_inverse(g, work_uri, FRBROO.R9_is_realised_in, prod_uri)

    # Crediti - CON CORREZIONE CP2
    if row.get("person_name"):
        person_wikidata = row.get("person_wikidata") or row.get("wikidata_id") or row.get("entity")
        p_uri = get_entity("PERSON", row.get("person_name"), 
                          entity_id=row.get("person_id"), 
                          wikidata_qid=person_wikidata)
        
        if p_uri:
            role_label = row.get("person_role") or row.get("credit_type") or "Contributo"
            role_type = get_entity("TYPE", role_label)

            c2 = BASE[f"prod_role_{pid}_{clean_uri(row.get('person_name'))}_{clean_uri(role_label)}"]
            g.add((c2, RDF.type, CORAGO.C2_Actor_Role))
            add_triple_with_inverse(g, c2, RDFS.label, 
                                   literal(f"{role_label} di {row.get('person_name')} (Produzione {pid})"))

            g.add((c2, CORAGO.CP3_carried_out_actor, p_uri))
            if role_type:
                add_triple_with_inverse(g, c2, CRM.P2_has_type, role_type)
            
            # CORREZIONE: usa CP2 invece di P9_consists_of
            g.add((prod_uri, CORAGO.CP2_carried_out_role, c2))

# ================================================================
# 9. RECITE - CON PERSONAGGI E TUTTE LE CORREZIONI CP2/RUOLI CONTESTUALI
# ================================================================

print("5. Recite...")
recite = pd.read_csv(CSV_RECITE)
recite.columns = recite.columns.str.strip()

# Dizionario per tenere traccia dei personaggi già collegati alle opere
# Evita duplicati quando lo stesso personaggio appare in più recite della stessa opera
character_work_pairs = set()

for _, row in recite.iterrows():
    rid = clean_id(row.get("id"))
    if not rid:
        continue

    perf_uri = BASE[f"performance_{rid}"]
    g.add((perf_uri, RDF.type, FRBROO.F31_Performance))
    g.add((perf_uri, RDF.type, CRM.E7_Activity))

    if row.get("titolo_breve"):
        add_triple_with_inverse(g, perf_uri, RDFS.label, literal(row.get("titolo_breve")))

    # Time-span
    date_from = safe_date_literal(row.get("from"))
    date_to = safe_date_literal(row.get("to"))
    if date_from or date_to:
        ts = BASE[f"ts_perf_{rid}"]
        g.add((ts, RDF.type, CRM.E52_Time_Span))
        if date_from:
            g.add((ts, CRM.P82a_begin_of_the_begin, date_from))
        if date_to:
            g.add((ts, CRM.P82b_end_of_the_end, date_to))
        add_triple_with_inverse(g, perf_uri, CRM.P4_has_time_span, ts)

    # Link a produzione
    prod_id = clean_id(row.get("production_id"))
    if prod_id:
        add_triple_with_inverse(g, perf_uri, FRBROO.R25_performed, BASE[f"production_{prod_id}"])

    # Luoghi
    if pd.notna(row.get("luogo_nome")):
        place_wikidata = extract_qid(row.get("luogo_wikidata"))
        place = get_entity("PLACE", row.get("luogo_nome"), 
                          entity_id=row.get("luogo_id"), 
                          wikidata_qid=place_wikidata)
        if place:
            add_triple_with_inverse(g, perf_uri, CRM.P7_took_place_at, place)
    
    if pd.notna(row.get("edificio_nome")):
        place_wikidata = extract_qid(row.get("edificio_wikidata"))
        place = get_entity("PLACE", row.get("edificio_nome"), 
                          entity_id=row.get("edificio_id"), 
                          wikidata_qid=place_wikidata)
        if place:
            add_triple_with_inverse(g, perf_uri, CRM.P7_took_place_at, place)

    # Curatore - CON CORREZIONE (C2 per-recita)
    if pd.notna(row.get("curatore_nome")):
        curator_wikidata = extract_qid(row.get("curatore_wikidata"))
        cur = get_entity("PERSON", row.get("curatore_nome"), 
                        entity_id=row.get("curatore_id"), 
                        wikidata_qid=curator_wikidata)
        
        if cur:
            role_label = row.get("curatore_ruolo") or "Curatore"
            role_type = get_entity("TYPE", role_label)

            c2 = BASE[f"perf_curator_role_{rid}_{clean_uri(row.get('curatore_nome'))}_{clean_uri(role_label)}"]
            g.add((c2, RDF.type, CORAGO.C2_Actor_Role))
            add_triple_with_inverse(
                g,
                c2,
                RDFS.label,
                literal(f"{role_label}: {row.get('curatore_nome')} (Recita {rid})")
            )

            g.add((c2, CORAGO.CP3_carried_out_actor, cur))
            if role_type:
                add_triple_with_inverse(g, c2, CRM.P2_has_type, role_type)

            g.add((perf_uri, CORAGO.CP2_carried_out_role, c2))

    # Esecutore - CON CORREZIONE (C2 per-recita)
    if pd.notna(row.get("esecutore_nome")):
        executor_wikidata = extract_qid(row.get("esecutore_wikidata"))
        exe = get_entity("PERSON", row.get("esecutore_nome"), 
                        entity_id=row.get("esecutore_id"), 
                        wikidata_qid=executor_wikidata)
        
        if exe:
            role_label = row.get("esecutore_ruolo") or "Esecutore"
            role_type = get_entity("TYPE", role_label)

            c2 = BASE[f"perf_executor_role_{rid}_{clean_uri(row.get('esecutore_nome'))}_{clean_uri(role_label)}"]
            g.add((c2, RDF.type, CORAGO.C2_Actor_Role))
            add_triple_with_inverse(
                g,
                c2,
                RDFS.label,
                literal(f"{role_label}: {row.get('esecutore_nome')} (Recita {rid})")
            )

            g.add((c2, CORAGO.CP3_carried_out_actor, exe))
            if role_type:
                add_triple_with_inverse(g, c2, CRM.P2_has_type, role_type)

            g.add((perf_uri, CORAGO.CP2_carried_out_role, c2))

    # Interpreti E PERSONAGGI - CON TUTTE LE CORREZIONI
    if pd.notna(row.get("interprete")):
        wd_actor = row.get("uri") or row.get("entity")
        actor = get_entity("PERSON", row.get("interprete"), 
                          entity_id=row.get("interprete_id"), 
                          wikidata_qid=wd_actor)

        if actor:
            if pd.notna(row.get("personaggio")):
                # Crea/recupera personaggio
                char_wikidata = extract_qid(row.get("personaggio_wikidata"))
                char = get_entity("CHAR", row.get("personaggio"), 
                                 wikidata_qid=char_wikidata)
                
                if char:
                    # Aggiungi tipo voce se presente
                    if pd.notna(row.get("personaggio_voce")):
                        vt = get_entity("TYPE", row.get("personaggio_voce"))
                        if vt:
                            add_triple_with_inverse(g, char, CRM.P2_has_type, vt)
                    
                    # NUOVO: Collega personaggio all'opera se abbiamo composizione_id
                    work_id = clean_id(row.get("composizione_id"))
                    if work_id:
                        work_uri = find_work_safely(work_id)
                        if work_uri:
                            # Crea chiave univoca per evitare duplicati
                            pair_key = f"{work_uri}_{char}"
                            if pair_key not in character_work_pairs:
                                # Collega opera -> personaggio
                                add_triple_with_inverse(g, work_uri, FRBROO.R64_has_character, char)
                                character_work_pairs.add(pair_key)
                                
                                # Aggiungi anche gender se deducibile dalla voce
                                # (soprano, mezzosoprano, contralto -> femmina; tenore, basso, baritono -> maschio)
                                if pd.notna(row.get("personaggio_voce")):
                                    voce = str(row.get("personaggio_voce")).lower()
                                    gender = None
                                    if any(gender_term in voce for gender_term in ["soprano", "mezzo", "contralto"]):
                                        gender = "femmina"
                                    elif any(gender_term in voce for gender_term in ["tenore", "basso", "baritono"]):
                                        gender = "maschio"
                                    
                                    if gender:
                                        gt = get_entity("TYPE", f"gender:{gender}")
                                        if gt:
                                            add_triple_with_inverse(g, char, CRM.P2_has_type, gt)

                    # Crea C6 Performer Role
                    c6 = BASE[f"perf_role_{rid}_{clean_uri(row.get('interprete'))}_{clean_uri(row.get('personaggio'))}"]
                    g.add((c6, RDF.type, CORAGO.C6_Performer_Role))
                    add_triple_with_inverse(
                        g,
                        c6,
                        RDFS.label,
                        literal(f"Interprete: {row.get('interprete')} nel ruolo di {str(row.get('personaggio')).strip()}"),
                    )

                    # CORREZIONI APPLICATE:
                    # 1. C6 -> actor, character (restano uguali)
                    g.add((c6, CORAGO.CP3_carried_out_actor, actor))
                    g.add((c6, CORAGO.CP8_performed_character, char))

                    # 2. CORREZIONE: usa CP2 invece di P9_consists_of
                    g.add((perf_uri, CORAGO.CP2_carried_out_role, c6))

                    # 3. CORREZIONE: ruolo (tipo) sul C6, non sull'attore
                    if pd.notna(row.get("ruolo")):
                        rt = get_entity("TYPE", row.get("ruolo"))
                        if rt:
                            add_triple_with_inverse(g, c6, CRM.P2_has_type, rt)
            else:
                # senza personaggio: usa C2 Actor Role per mantenere ruolo contestuale
                role_label = row.get("ruolo") or "Partecipazione"
                role_type = get_entity("TYPE", role_label)

                c2 = BASE[f"perf_actor_role_{rid}_{clean_uri(row.get('interprete'))}_{clean_uri(role_label)}"]
                g.add((c2, RDF.type, CORAGO.C2_Actor_Role))
                add_triple_with_inverse(
                    g,
                    c2,
                    RDFS.label,
                    literal(f"{role_label}: {row.get('interprete')} (Recita {rid})")
                )

                g.add((c2, CORAGO.CP3_carried_out_actor, actor))
                if role_type:
                    add_triple_with_inverse(g, c2, CRM.P2_has_type, role_type)

                # Performance -> ruolo (CP2)
                g.add((perf_uri, CORAGO.CP2_carried_out_role, c2))

# ================================================================
# 10. SALVATAGGIO E STATISTICHE
# ================================================================

print("\n6. Salvataggio file TTL...")
OUTPUT_TTL.parent.mkdir(parents=True, exist_ok=True)
g.serialize(destination=OUTPUT_TTL, format="turtle")

# Statistiche
print(f"\n=== STATISTICHE ===")
print(f"File generato: {OUTPUT_TTL}")
print(f"Triple totali: {len(g)}")

# Conta entità per tipo
entity_counts = {}
for kind, cache in CACHE.items():
    entity_counts[kind] = len(cache)

print("\nEntità create:")
for kind, count in entity_counts.items():
    print(f"  {kind}: {count}")

# Conta personaggi collegati alle opere
character_links = 0
for s, p, o in g:
    if str(p) == "http://iflastandards.info/ns/fr/frbr/frbroo/R64_has_character":
        character_links += 1

print(f"\nPersonaggi collegati alle opere: {character_links}")
print(f"Coppie opera-personaggio uniche: {len(character_work_pairs)}")

# Conta URI unificati vs locali
unified_count = 0
local_count = 0
for kind, cache in CACHE.items():
    for uri in cache.values():
        if "unified_" in str(uri):
            unified_count += 1
        else:
            local_count += 1

print(f"\nURI unificati (basati su Wikidata): {unified_count}")
print(f"URI locali (senza Wikidata): {local_count}")

# Conta quante triple CP2 sono state create
cp2_count = 0
for s, p, o in g:
    if str(p) == "http://corago.unibo.it/sm/CP2_carried_out_role":
        cp2_count += 1

print(f"\nTriple CP2_carried_out_role create: {cp2_count}")

# Conta quante triple P2_has_type su persona
PERSONS = set()
for s, p, o in g.triples((None, RDF.type, CRM.E21_Person)):
    PERSONS.add(s)
for s, p, o in g.triples((None, RDF.type, CRM.E39_Actor)):
    PERSONS.add(s)

person_type_links = 0
for s, p, o in g.triples((None, CRM.P2_has_type, None)):
    if s in PERSONS:
        person_type_links += 1

print(f"P2_has_type su persona (dovrebbe essere ~0 per ruoli di recita): {person_type_links}")


print(f"P2_has_type su persona (dovrebbe essere ~0 per ruoli di recita): {person_type_links}")

print("\n=== TUTTE LE CORREZIONI APPLICATE CON SUCCESSO ===")
print("✓ P9_consists_of sostituito con CP2_carried_out_role")
print("✓ Ruolo contestuale spostato da attore a C6/C2")
print("✓ add_author_corago: C2 univoco per autore (no collasso)")
print("✓ Interpreti senza personaggio: usano C2 invece di P14 + P2")
print("✓ Curatore/esecutore: usano C2 invece di P14 + P2")
print("✓ Pattern Corago completo a granularità recita")

print("\nFinito! Il file contiene entità con URI unificati quando disponibile Wikidata.")
print("I personaggi sono presi dalle recite e collegati alle opere usando composizione_id.")