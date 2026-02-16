import pandas as pd
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD, OWL
from pathlib import Path
import unidecode
import re
import ast

CSV_OPERE = "https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/fondazione-iteatri-opere-musicali-wiki-reconciled.csv"
CSV_PERSONE = "https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/persone.csv"
CSV_STAGIONI = "https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/stagioni.csv"
CSV_PRODUZIONI = "https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/produzioni_clean.csv"
CSV_RECITE = "https://raw.githubusercontent.com/elena2notti/theatreNet/refs/heads/main/fondazione/recite_fondazione_con_qid.csv"
OUTPUT_TTL = Path("semantic_graph/upload_to_neo4j/fondazione.ttl")

BASE = Namespace("https://fondazioneiteatri.it/archivio/data/")
CRM = Namespace("http://www.cidoc-crm.org/cidoc-crm/")
FRBROO = Namespace("http://iflastandards.info/ns/fr/frbr/frbroo/")
CORAGO = Namespace("http://corago.unibo.it/sm/")

def clean_id(value):
    """Pulisce ID numerici - VERSIONE ROBUSTA CONTRO .0"""
    if pd.isna(value) or value is None:
        return None
    
    s = str(value).strip()
    if not s or s.lower() in ["nan", "none", "null", "n/a"]:
        return None
    
    # Rimuovi TUTTI i .0 finali (anche multipli)
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
    if value is None or pd.isna(value):
        return None
    s = str(value).strip().upper()  # Converti a maiuscolo per consistenza
    
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

    # Cerca QID in qualsiasi stringa
    match = re.search(r'(Q\d+)', s, re.IGNORECASE)
    if match:
        return URIRef(f"http://www.wikidata.org/entity/{match.group(1).upper()}")
    
    return None

def extract_qid(value):
    """Estrai QID da valore Wikidata - SEMPRE MAIUSCOLO"""
    wd = wikidata_canonical(value)
    if wd:
        match = re.search(r'(Q\d+)$', str(wd), re.IGNORECASE)
        return match.group(1).upper() if match else None  # Sempre maiuscolo
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
        return BASE[f"fondazione_person_{key}"]
    elif kind == "WORK":
        return BASE[f"fondazione_work_{key}"]
    elif kind == "CHAR":
        return BASE[f"fondazione_character_{key}"]
    elif kind == "PLACE":
        return BASE[f"fondazione_place_{key}"]
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
            return  # Evita auto-riferimento
    
    g.add((local_uri, OWL.sameAs, wd))

def add_triple_with_inverse(g, s, p, o):
    """Aggiunge tripla e la sua inversa"""
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

def parse_pimcore_people_paths(value):
    """Estrae nome e ID da stringhe tipo "/Persone/Bellini, Vincenzo (9582)" """
    if value is None or pd.isna(value):
        return []
    s = str(value).strip()
    if not s or s.lower() in ["nan", "none", "null"]:
        return []
    
    result = []
    for m in re.finditer(r"([^/]+?)\s*\((\d+)\)", s):
        name = m.group(1).strip().strip(",")
        pid = clean_id(m.group(2).strip())  # Usa clean_id per l'ID
        if name and pid:
            result.append((name, pid))
    return result

def try_parse_embedded_list(value):
    """Prova a interpretare stringhe che potrebbero contenere liste JSON."""
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    if not s or s.lower() in ["nan", "none", "null"]:
        return None
    try:
        return ast.literal_eval(s)
    except:
        return None


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
    Crea o recupera entità con URI unificati.
    Focus: tipi RDF, label, relazioni essenziali.
    """
    global g

    # Determina chiave cache UNAMBIGUA
    cache_key = None
    
    # PRIMA PRIORITÀ: Wikidata QID
    if wikidata_qid:
        qid = extract_qid(wikidata_qid)
        if qid:
            cache_key = qid  # Es: "Q123"
    
    # SECONDA PRIORITÀ: ID locale (USA clean_id!)
    if not cache_key and entity_id:
        clean_entity_id = clean_id(entity_id)  # Questo rimuove .0
        if clean_entity_id:
            cache_key = clean_entity_id  # Es: "456" non "ID_456"
    
    # TERZA PRIORITÀ: Label pulita
    if not cache_key and label:
        clean_label = clean_uri(label)
        if clean_label:
            cache_key = f"LABEL_{clean_label}"  # Es: "LABEL_VERDI_GIUSEPPE"
    
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
    
    # Crea URI
    uri = get_unified_uri(kind, wikidata_qid, label, entity_id)
    if not uri:
        return None
    
    # ASSICURATI che l'URI non contenga .0
    uri_str = str(uri)
    if ".0" in uri_str and not uri_str.endswith("/data/"):
        # Crea nuovo URI senza .0
        new_uri_str = uri_str.replace(".0", "")
        uri = URIRef(new_uri_str)
    
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
    
    if label:
        add_triple_with_inverse(g, uri, RDFS.label, literal(label))
    
    # Wikidata sameAs
    if wikidata_qid:
        add_sameas_if_wikidata(g, uri, wikidata_qid)
    
    # Cache con chiave UNAMBIGUA
    CACHE[kind][cache_key] = uri
    return uri

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
    
    # 3. Cerca per REFerence
    ref_key = f"REF_{wid_clean}"
    if ref_key in CACHE["WORK"]:
        return CACHE["WORK"][ref_key]
    
    return None

def add_author_corago(g, opera_uri, person_uri, role_label):
    """
    RELAZIONE AUTORE: F27 Work Conception + C2 Actor Role
    FIX: C2 univoco per (opera + ruolo + persona) => niente collasso di più autori nello stesso C2
    """
    if not opera_uri or not person_uri or not role_label:
        return

    role_type_uri = get_entity("TYPE", role_label)

    op_slug = str(opera_uri).split("_")[-1][:30]
    role_slug = (clean_uri(role_label) or "role")[:30]

    # chiave persona stabile: ultimo segmento URI (unified_person_Qxxx o fondazione_person_123)
    person_key = str(person_uri).split("/")[-1]
    person_key = (clean_uri(person_key) or "person")[:30]

    # F27 Work Conception (una per opera+ruolo)
    conception_uri = BASE[f"conception_{op_slug}_{role_slug}"]
    if (conception_uri, RDF.type, FRBROO.F27_Work_Conception) not in g:
        g.add((conception_uri, RDF.type, FRBROO.F27_Work_Conception))
        g.add((conception_uri, RDF.type, CRM.E7_Activity))
        add_triple_with_inverse(g, opera_uri, FRBROO.R16i_was_initiated_by, conception_uri)

    # C2 Actor Role (una per opera+ruolo+persona)
    c2_role_uri = BASE[f"role_{role_slug}_{op_slug}_{person_key}"]
    g.add((c2_role_uri, RDF.type, CORAGO.C2_Actor_Role))

    # Bonora: CP2/CP3
    g.add((conception_uri, CORAGO.CP2_carried_out_role, c2_role_uri))
    g.add((c2_role_uri, CORAGO.CP3_carried_out_actor, person_uri))

    if role_type_uri:
        add_triple_with_inverse(g, c2_role_uri, CRM.P2_has_type, role_type_uri)


PERSON_INDEX = {}

def load_people_index():
    global PERSON_INDEX
    try:
        dfp = pd.read_csv(CSV_PERSONE)
        dfp.columns = dfp.columns.str.strip()
        for _, r in dfp.iterrows():
            pid = clean_id(r.get("id"))
            if pid:
                PERSON_INDEX[pid] = {
                    "label": r.get("dcTitle"),
                    "wikidata": extract_qid(r.get("entity") or r.get("uri"))
                }
    except:
        print("INFO: CSV_PERSONE non caricato, siamo senza indice")

load_people_index()

def person_from_id(pid, fallback_label=None):
    """Crea persona da ID usando l'indice"""
    pid_clean = clean_id(pid)
    if not pid_clean:
        return None
    
    info = PERSON_INDEX.get(pid_clean, {})
    label = info.get("label") or fallback_label or f"Persona {pid_clean}"
    wikidata = info.get("wikidata")
    
    return get_entity("PERSON", label, entity_id=pid_clean, wikidata_qid=wikidata)


g.bind("crm", CRM)
g.bind("frbroo", FRBROO)
g.bind("corago", CORAGO)
g.bind("tr", BASE)
g.bind("owl", OWL)

print("=== FONDAZIONE ITEATRI - GENERAZIONE GRAFO ===")

print("1. Opere...")
opere = pd.read_csv(CSV_OPERE)
opere.columns = opere.columns.str.strip()

for _, row in opere.iterrows():
    op_id = clean_id(row.get("id"))
    if not op_id:
        continue
    
    # Titolo
    title = row.get("dcTitle") or f"Opera {op_id}"
    
    # Wikidata
    wikidata_qid = None
    for col in ["composizione_uri", "entity_id", "entity"]:
        if col in row and pd.notna(row[col]):
            wikidata_qid = extract_qid(row[col])
            if wikidata_qid:
                break
    
    # Opera (URI unificato)
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
    
    # PERSONE COLLEGATE (autori) - FIX: RUOLO NEUTRO (non dedotto da nome)
    people_paths = parse_pimcore_people_paths(row.get("persone_collegate"))
    for pname, pid in people_paths:
        if pname and pid:
            # Crea persona
            p_uri = person_from_id(pid, fallback_label=pname)
            if p_uri:
                # FIX: Ruolo neutro (non dedotto da nome, che non contiene "compositore/librettista")
                role = "Autore/Contributore"
                
                # RELAZIONE AUTORE (Bonora)
                add_author_corago(g, work_uri, p_uri, role)


print("2. Stagioni...")
stagioni = pd.read_csv(CSV_STAGIONI)
stagioni.columns = stagioni.columns.str.strip()

for _, row in stagioni.iterrows():
    sid = clean_id(row.get("id"))
    if not sid:
        continue
    
    season_uri = BASE[f"season_{sid}"]
    g.add((season_uri, RDF.type, FRBROO.F8_Event_Set))
    add_triple_with_inverse(g, season_uri, RDFS.label, literal(row.get("dcTitle")))
    
    # Time-span
    date_from = safe_date_literal(row.get("from"))
    date_to = safe_date_literal(row.get("to"))
    if date_from or date_to:
        ts = BASE[f"ts_season_{sid}"]
        g.add((ts, RDF.type, CRM.E52_Time_Span))
        if date_from:
            g.add((ts, CRM.P82a_begin_of_the_begin, date_from))
        if date_to:
            g.add((ts, CRM.P82b_end_of_the_end, date_to))
        add_triple_with_inverse(g, season_uri, CRM.P4_has_time_span, ts)
    
    # RELAZIONI: stagione -> produzioni
    prod_ids = row.get("produzioni_collegate_id")
    if prod_ids:
        for pid in str(prod_ids).split(","):
            pid_clean = clean_id(pid)
            if pid_clean:
                prod_uri = BASE[f"production_{pid_clean}"]
                add_triple_with_inverse(g, season_uri, CRM.P9_consists_of, prod_uri)
    
    # RELAZIONI: stagione -> recite
    perf_ids = row.get("manifestazioni_recite_concerti_collegati_id")
    if perf_ids:
        for rid in str(perf_ids).split(","):
            rid_clean = clean_id(rid)
            if rid_clean:
                perf_uri = BASE[f"performance_{rid_clean}"]
                add_triple_with_inverse(g, season_uri, CRM.P9_consists_of, perf_uri)


print("3. Produzioni...")
prod_df = pd.read_csv(CSV_PRODUZIONI, sep=";", encoding="utf-8", engine="python")
prod_df.columns = prod_df.columns.str.strip()

for _, row in prod_df.iterrows():
    pid = clean_id(row.get("id"))
    if not pid:
        continue
    
    prod_uri = BASE[f"production_{pid}"]
    g.add((prod_uri, RDF.type, FRBROO.F25_Performance_Plan))
    g.add((prod_uri, RDF.type, CRM.E7_Activity))
    
    add_triple_with_inverse(g, prod_uri, RDFS.label, literal(row.get("dcTitle")))
    
    # Time-span
    date_from = safe_date_literal(row.get("from"))
    date_to = safe_date_literal(row.get("to"))
    if date_from or date_to:
        ts = BASE[f"ts_prod_{pid}"]
        g.add((ts, RDF.type, CRM.E52_Time_Span))
        if date_from:
            g.add((ts, CRM.P82a_begin_of_the_begin, date_from))
        if date_to:
            g.add((ts, CRM.P82b_end_of_the_end, date_to))
        add_triple_with_inverse(g, prod_uri, CRM.P4_has_time_span, ts)
    
    # RELAZIONE: produzione -> luoghi
    if row.get("luogo_rappresentazione"):
        place = get_entity("PLACE", row.get("luogo_rappresentazione"))
        if place:
            add_triple_with_inverse(g, prod_uri, CRM.P7_took_place_at, place)
    
    # RELAZIONE: opere -> produzione (R9) - CON RICERCA SICURA
    work_ids = row.get("opere_collegate_id") or row.get("Opere musicali collegate")
    if work_ids:
        for wid in str(work_ids).split(","):
            wid_clean = clean_id(wid)
            if wid_clean:
                # CERCA CON FUNZIONE SICURA (no falsi positivi)
                work_uri = find_work_safely(wid_clean)
                
                if not work_uri:
                    # Crea reference solo se non esiste
                    work_uri = BASE[f"fondazione_work_ref_{wid_clean}"]
                    g.add((work_uri, RDF.type, FRBROO.F1_Work))
                    add_triple_with_inverse(g, work_uri, RDFS.label, 
                                           literal(f"Opera riferimento ID {wid_clean}"))
                    # Salva in cache per future ricerche
                    CACHE["WORK"][f"REF_{wid_clean}"] = work_uri
                
                # RELAZIONE FONDAMENTALE: opera realizzata in produzione
                add_triple_with_inverse(g, work_uri, FRBROO.R9_is_realised_in, prod_uri)
    
    # CREDITI (C2 Actor Role) - RELAZIONI PERSONE
    names = [x.strip() for x in str(row.get("persone_collegate_clean") or "").split(",") if x.strip()]
    ids = [clean_id(x) for x in str(row.get("persone_collegate_id") or "").split(",") if str(x).strip()]
    roles = [x.strip() for x in str(row.get("persone_collegate_ruolo") or "").split(",") if x.strip()]
    
    n = max(len(names), len(ids), len(roles))
    for i in range(n):
        pname = names[i] if i < len(names) else None
        pid_i = ids[i] if i < len(ids) else None
        role_i = roles[i] if i < len(roles) else "Contributo"
        
        if not pname and not pid_i:
            continue
        
        # Persona
        p_uri = None
        if pid_i:
            p_uri = person_from_id(pid_i, fallback_label=pname)
        elif pname:
            p_uri = get_entity("PERSON", pname)
        
        if not p_uri:
            continue
        
        # C2 Actor Role
        role_type = get_entity("TYPE", role_i)
        c2 = BASE[f"prod_role_{pid}_{clean_uri(pname or pid_i)}_{clean_uri(role_i)}"]
        g.add((c2, RDF.type, CORAGO.C2_Actor_Role))
        
        # RELAZIONI: C2 -> persona (CP3) e tipo ruolo
        g.add((c2, CORAGO.CP3_carried_out_actor, p_uri))
        if role_type:
            add_triple_with_inverse(g, c2, CRM.P2_has_type, role_type)
        
        # RELAZIONE: produzione -> C2 (FIX: CP2 invece di P9)
        g.add((prod_uri, CORAGO.CP2_carried_out_role, c2))


print("4. Recite...")
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
    
    # RELAZIONE FONDAMENTALE: recita -> produzione (R25)
    prod_id = clean_id(row.get("production_id"))
    if prod_id:
        add_triple_with_inverse(g, perf_uri, FRBROO.R25_performed, BASE[f"production_{prod_id}"])
    
    # Luoghi - FIX: sia luogo (città) che edificio (teatro)
    if pd.notna(row.get("luogo_nome")):
        place_city = get_entity("PLACE", row.get("luogo_nome"), entity_id=row.get("luogo_id"))
        if place_city:
            add_triple_with_inverse(g, perf_uri, CRM.P7_took_place_at, place_city)
    
    # Edificio (teatro) come place separato
    if pd.notna(row.get("edificio_nome")):
        place_venue = get_entity("PLACE", row.get("edificio_nome"), entity_id=row.get("edificio_id"))
        if place_venue:
            add_triple_with_inverse(g, perf_uri, CRM.P7_took_place_at, place_venue)
    
    # Curatore (C2 per-recita, ruolo contestuale) - FIX: C2 invece di P14 + tipo su persona
    if pd.notna(row.get("curatore_nome")):
        cur = person_from_id(row.get("curatore_id"), fallback_label=row.get("curatore_nome"))
        if cur:
            role_label = row.get("curatore_ruolo") or "Curatore"
            role_type = get_entity("TYPE", role_label)

            c2 = BASE[f"perf_curator_role_{rid}_{clean_uri(row.get('curatore_nome'))}_{clean_uri(role_label)}"]
            g.add((c2, RDF.type, CORAGO.C2_Actor_Role))
            g.add((c2, CORAGO.CP3_carried_out_actor, cur))
            if role_type:
                add_triple_with_inverse(g, c2, CRM.P2_has_type, role_type)

            g.add((perf_uri, CORAGO.CP2_carried_out_role, c2))
    
    # Esecutore (C2 per-recita, ruolo contestuale) - NUOVO: aggiunto
    if pd.notna(row.get("esecutore_nome")):
        exe = person_from_id(row.get("esecutore_id"), fallback_label=row.get("esecutore_nome"))
        if exe:
            role_label = row.get("esecutore_ruolo") or "Esecutore"
            role_type = get_entity("TYPE", role_label)

            c2 = BASE[f"perf_executor_role_{rid}_{clean_uri(row.get('esecutore_nome'))}_{clean_uri(role_label)}"]
            g.add((c2, RDF.type, CORAGO.C2_Actor_Role))
            g.add((c2, CORAGO.CP3_carried_out_actor, exe))
            if role_type:
                add_triple_with_inverse(g, c2, CRM.P2_has_type, role_type)

            g.add((perf_uri, CORAGO.CP2_carried_out_role, c2))
    
    # INTERPRETI E PERSONAGGI
    if pd.notna(row.get("interprete")):
        # Wikidata interprete
        wd_actor = row.get("uri") or row.get("entity")
        actor = get_entity("PERSON", row.get("interprete"), row.get("interprete_id"), wikidata_qid=wd_actor)
        
        if actor:
            if pd.notna(row.get("personaggio")):
                # Personaggio (senza Wikidata, solo locale)
                char = get_entity("CHAR", row.get("personaggio"))
                
                if char:
                    # Voce personaggio
                    voice_type = None
                    if pd.notna(row.get("personaggio_voce")):
                        vt = get_entity("TYPE", row.get("personaggio_voce"))
                        if vt:
                            add_triple_with_inverse(g, char, CRM.P2_has_type, vt)
                            voice_type = vt
                    
                    # Collega personaggio all'opera se abbiamo composizione_id
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
                                
                                # Aggiungi gender se deducibile dalla voce
                                if voice_type and pd.notna(row.get("personaggio_voce")):
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
                    
                    # C6 Performer Role
                    c6 = BASE[f"perf_role_{rid}_{clean_uri(row.get('interprete'))}_{clean_uri(row.get('personaggio'))}"]
                    g.add((c6, RDF.type, CORAGO.C6_Performer_Role))
                    
                    # RELAZIONI C6:
                    # 1. C6 -> attore (CP3)
                    g.add((c6, CORAGO.CP3_carried_out_actor, actor))
                    # 2. C6 -> personaggio (CP8)
                    g.add((c6, CORAGO.CP8_performed_character, char))
                    # 3. recita -> C6 (FIX: CP2 invece di P9)
                    g.add((perf_uri, CORAGO.CP2_carried_out_role, c6))
                    
                    # Tipo ruolo specifico (FIX: su C6 invece che su actor)
                    if pd.notna(row.get("ruolo")):
                        rt = get_entity("TYPE", row.get("ruolo"))
                        if rt:
                            add_triple_with_inverse(g, c6, CRM.P2_has_type, rt)
            else:
                # Senza personaggio: C2 Actor Role per mantenere ruolo contestuale alla recita - FIX
                role_label = row.get("ruolo") or "Partecipazione"
                role_type = get_entity("TYPE", role_label)

                c2 = BASE[f"perf_actor_role_{rid}_{clean_uri(row.get('interprete'))}_{clean_uri(role_label)}"]
                g.add((c2, RDF.type, CORAGO.C2_Actor_Role))
                g.add((c2, CORAGO.CP3_carried_out_actor, actor))
                if role_type:
                    add_triple_with_inverse(g, c2, CRM.P2_has_type, role_type)

                g.add((perf_uri, CORAGO.CP2_carried_out_role, c2))


print("\n5. Statistiche di validazione...")

# Conta CP2_carried_out_role
cp2_count = 0
for s, p, o in g:
    if str(p) == "http://corago.unibo.it/sm/CP2_carried_out_role":
        cp2_count += 1
print(f"   CP2_carried_out_role: {cp2_count}")

# Conta P2_has_type su PERSON (dovrebbero essere 0 per ruoli di recita)
p2_on_person_count = 0
for s, p, o in g:
    if str(p) == "http://www.cidoc-crm.org/cidoc-crm/P2_has_type":
        # Verifica se s è una persona (E21_Person o E39_Actor)
        if (s, RDF.type, CRM.E21_Person) in g or (s, RDF.type, CRM.E39_Actor) in g:
            p2_on_person_count += 1
print(f"   P2_has_type su PERSON (dovrebbe essere ~0 per ruoli di recita): {p2_on_person_count}")

# Conta C2 Actor Role e C6 Performer Role
c2_count = len(set(g.subjects(RDF.type, CORAGO.C2_Actor_Role)))
c6_count = len(set(g.subjects(RDF.type, CORAGO.C6_Performer_Role)))
place_count = len(set(g.subjects(RDF.type, CRM.E53_Place)))
print(f"   C2 Actor Role: {c2_count}")
print(f"   C6 Performer Role: {c6_count}")
print(f"   Luoghi (E53_Place): {place_count}")


# Conta luoghi (città + edifici)
place_count = 0
for s, p, o in g:
    if (s, RDF.type, CRM.E53_Place) in g:
        place_count += 1
print(f"   Luoghi (E53_Place): {place_count}")

print("\n6. Salvataggio...")
OUTPUT_TTL.parent.mkdir(parents=True, exist_ok=True)
g.serialize(destination=OUTPUT_TTL, format="turtle")

print(f"\n=== COMPLETATO ===")
print(f"File: {OUTPUT_TTL}")
print(f"Triple: {len(g)}")

# Statistiche essenziali
print(f"\nEntità create:")
for kind, cache in CACHE.items():
    print(f"  {kind}: {len(cache)}")

# Conta personaggi collegati alle opere
character_links = 0
for s, p, o in g:
    if str(p) == "http://iflastandards.info/ns/fr/frbr/frbroo/R64_has_character":
        character_links += 1

print(f"\nPersonaggi collegati alle opere: {character_links}")
print(f"Coppie opera-personaggio uniche: {len(character_work_pairs)}")

# Conta URI unificati
unified_count = 0
for cache in CACHE.values():
    for uri in cache.values():
        if "unified_" in str(uri):
            unified_count += 1

print(f"\nURI unificati (basati su Wikidata): {unified_count}")
print(f"URI locali (senza Wikidata): {sum(len(c) for c in CACHE.values()) - unified_count}")

print("\nIl grafo contiene:")
print("- Tutte le relazioni CIDOC-CRM/FRBROO/CORAGO")
print("- Personaggi presi dalle recite e collegati alle opere")
print("- Esecutori e curatori modellati con C2 per-recita")
print("- Luoghi (città) e edifici (teatri) come place separati")
print("- Pronto per merge automatico con dataset Regio in Neo4j")