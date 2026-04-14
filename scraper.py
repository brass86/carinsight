"""
CarInsight Gaspedaal Scraper v3
================================
Slim scrapen: alleen voorraad-combinaties.

Vergelijkingslogica per voorraadauto:
  - Merk exact
  - Model: eerste twee woorden exact
  - Bouwjaar ± 2 jaar
  - Kilometerstand ± 20%
  - Brandstof exact
  - Transmissie exact
  - Carrosserie exact

Per advertentie bijhouden:
  - eerste_gezien, vorige_prijs, huidige_prijs, laatste_gezien, verdwenen_op

Scraping schema: menselijke tijdstippen, verspreid over de dag.
Test: python3 scraper.py --test
Prod: python3 scraper.py
"""
import sqlite3, requests, json, time, random, logging, re, sys
from datetime import datetime, date, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

DB_PATH   = "/opt/carinsight/carinsight.db"
LOG_PATH  = "/opt/carinsight/scraper.log"
BOUWJAAR_VANAF = 2005
KM_MAX    = 300000
PRIJS_MIN = 3000
PRIJS_MAX = 150000
MAX_PAGINAS  = 10
JAAR_MARGE   = 2      # ± 2 jaar
KM_MARGE     = 0.20   # ± 20%

# Menselijk scraping schema
SCHEMA = [
    (9,  0, 11, 30),
    (13, 0, 15, 30),
    (19, 0, 21,  0),
]

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()])
log = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
]

MERK_SLUGS = {
    "alfa romeo":"alfa-romeo","audi":"audi","bmw":"bmw","chevrolet":"chevrolet",
    "citroën":"citroen","citroen":"citroen","dacia":"dacia","ds":"ds","fiat":"fiat",
    "ford":"ford","honda":"honda","hyundai":"hyundai","jaguar":"jaguar","jeep":"jeep",
    "kia":"kia","land rover":"land-rover","lexus":"lexus","mazda":"mazda",
    "mercedes-benz":"mercedes-benz","mercedes":"mercedes-benz","mini":"mini",
    "mitsubishi":"mitsubishi","nissan":"nissan","opel":"opel","peugeot":"peugeot",
    "porsche":"porsche","renault":"renault","seat":"seat","skoda":"skoda","smart":"smart",
    "subaru":"subaru","suzuki":"suzuki","tesla":"tesla","toyota":"toyota",
    "volkswagen":"volkswagen","vw":"volkswagen","volvo":"volvo",
}

# ═══ HELPERS ═══
def get_headers(ref=None):
    h = {"User-Agent": random.choice(USER_AGENTS),
         "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
         "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.7",
         "Accept-Encoding": "gzip, deflate, br",
         "DNT": "1", "Connection": "keep-alive"}
    if ref: h["Referer"] = ref
    return h

def pauze(actie="pagina"):
    t = {"pagina": (4,12), "combinatie": (15,45), "kort": (1,3)}.get(actie, (5,10))
    time.sleep(random.uniform(*t))

def is_scrape_tijd():
    nu = datetime.now(); hm = nu.hour*60+nu.minute
    return any(sh*60+sm <= hm <= eh*60+em for sh,sm,eh,em in SCHEMA)

def wacht_op_tijd():
    if is_scrape_tijd(): return
    nu = datetime.now(); hm = nu.hour*60+nu.minute
    for sh,sm,eh,em in sorted(SCHEMA):
        if sh*60+sm > hm:
            doel = nu.replace(hour=sh, minute=sm+random.randint(0,15), second=random.randint(0,59))
            w = (doel-nu).total_seconds()
            log.info(f"Wacht tot {doel.strftime('%H:%M')} ({w/60:.0f}min)")
            time.sleep(w); return
    sh,sm = SCHEMA[0][0], SCHEMA[0][1]
    doel = (nu+timedelta(days=1)).replace(hour=sh, minute=sm+random.randint(0,15), second=0)
    time.sleep((doel-nu).total_seconds())

def safe_get(url, ref=None):
    try:
        pauze("kort")
        r = requests.get(url, headers=get_headers(ref), timeout=20, allow_redirects=True)
        if r.status_code == 429: log.warning("Rate limited 5min"); time.sleep(300); return None
        if r.status_code == 403: log.warning("403 geblokkeerd 2min"); time.sleep(120); return None
        return r if r.status_code == 200 else None
    except Exception as e: log.error(f"GET: {e}"); return None

def model_woorden(model, n=2):
    """Geeft de eerste n woorden van een modelnaam terug, lowercase."""
    woorden = re.sub(r'[^a-z0-9 ]', '', (model or '').lower().strip()).split()
    return ' '.join(woorden[:n])

def is_vergelijkbaar(voorraad_auto, advertentie):
    """
    Bepaalt of een advertentie echt vergelijkbaar is met een voorraadauto.
    Alle filters moeten kloppen voor een betrouwbare vergelijking.
    """
    v = voorraad_auto
    a = advertentie

    # 1. Merk exact (case insensitive)
    if v['merk'].lower() != (a.get('merk') or '').lower():
        return False, "merk"

    # 2. Model: eerste twee woorden exact
    v_model = model_woorden(v['model'], 2)
    a_model = model_woorden(a.get('model'), 2)
    if not v_model or not a_model or v_model != a_model:
        return False, f"model ({v_model!r} vs {a_model!r})"

    # 3. Bouwjaar ± JAAR_MARGE
    if v.get('jaar') and a.get('jaar'):
        if abs(v['jaar'] - a['jaar']) > JAAR_MARGE:
            return False, f"jaar ({v['jaar']} vs {a['jaar']})"

    # 4. Kilometerstand ± KM_MARGE (alleen als beide bekend)
    if v.get('km') and a.get('km') and v['km'] > 0:
        km_min = v['km'] * (1 - KM_MARGE)
        km_max = v['km'] * (1 + KM_MARGE)
        if not (km_min <= a['km'] <= km_max):
            return False, f"km ({v['km']} vs {a['km']})"

    # 5. Brandstof exact (alleen als beide bekend)
    v_bs = (v.get('brandstof') or '').lower()
    a_bs = (a.get('brandstof') or '').lower()
    if v_bs and a_bs and v_bs != 'onbekend' and a_bs != 'onbekend':
        if v_bs != a_bs:
            return False, f"brandstof ({v_bs} vs {a_bs})"

    # 6. Transmissie exact (alleen als beide bekend)
    v_tr = (v.get('transmissie') or '').lower()
    a_tr = (a.get('transmissie') or '').lower()
    if v_tr and a_tr and v_tr != 'onbekend' and a_tr != 'onbekend':
        if v_tr != a_tr:
            return False, f"transmissie ({v_tr} vs {a_tr})"

    # 7. Carrosserie exact (alleen als beide bekend)
    v_car = (v.get('carrosserie') or v.get('seg') or '').lower()
    a_car = (a.get('carrosserie') or '').lower()
    if v_car and a_car and v_car != 'onbekend' and a_car != 'onbekend':
        if v_car != a_car:
            return False, f"carrosserie ({v_car} vs {a_car})"

    return True, "match"

# ═══ DATABASE ═══
def get_db():
    c = sqlite3.connect(DB_PATH); c.row_factory = sqlite3.Row; return c

def init_db():
    conn = get_db(); c = conn.cursor()

    # Advertenties tabel — met vorige_prijs
    c.execute("""CREATE TABLE IF NOT EXISTS advertenties (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_id TEXT UNIQUE,
        platform TEXT,
        dealer TEXT,
        url TEXT,
        merk TEXT,
        model TEXT,
        jaar INTEGER,
        km INTEGER,
        brandstof TEXT,
        transmissie TEXT,
        carrosserie TEXT,
        regio TEXT,
        eerste_prijs INTEGER,
        vorige_prijs INTEGER,
        huidige_prijs INTEGER,
        prijs_historie TEXT DEFAULT '[]',
        eerste_gezien TEXT DEFAULT (date('now')),
        laatste_gezien TEXT DEFAULT (date('now')),
        verdwenen INTEGER DEFAULT 0,
        verdwenen_op TEXT,
        days_on_market INTEGER DEFAULT 0
    )""")

    # Vergelijkingen — koppelt voorraadauto aan vergelijkbare advertentie
    c.execute("""CREATE TABLE IF NOT EXISTS vergelijkingen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        voorraad_id INTEGER,
        advertentie_listing_id TEXT,
        match_score INTEGER DEFAULT 0,
        aangemaakt TEXT DEFAULT (datetime('now')),
        UNIQUE(voorraad_id, advertentie_listing_id)
    )""")

    # Marktdata snapshots
    c.execute("""CREATE TABLE IF NOT EXISTS marktdata_historie (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datum TEXT DEFAULT (date('now')),
        merk TEXT, model TEXT, platform TEXT,
        n_actief INTEGER, n_nieuw INTEGER, n_verdwenen INTEGER,
        prijs_mediaan INTEGER, prijs_min INTEGER, prijs_max INTEGER,
        gem_dom INTEGER, gem_km INTEGER, pct_verkocht_14d REAL,
        UNIQUE(datum, merk, model, platform)
    )""")

    # Markttrend
    c.execute("""CREATE TABLE IF NOT EXISTS markttrend (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        merk TEXT, model TEXT,
        bijgewerkt TEXT DEFAULT (datetime('now')),
        aanbod_delta INTEGER DEFAULT 0,
        prijs_delta INTEGER DEFAULT 0,
        vt_mediaan INTEGER DEFAULT 25,
        momentum TEXT DEFAULT 'Stabiel',
        pct_verkocht_14d REAL DEFAULT 0,
        n_meetpunten INTEGER DEFAULT 0,
        UNIQUE(merk, model)
    )""")

    # Voeg ontbrekende kolommen toe aan marktdata
    bestaande = [r[1] for r in c.execute("PRAGMA table_info(marktdata)").fetchall()]
    for kol, typ in [("prijs_min","INTEGER"),("prijs_max","INTEGER"),
                     ("n_advertenties","INTEGER"),("platforms","TEXT"),("gescraped_op","TEXT")]:
        if kol not in bestaande:
            try: c.execute(f"ALTER TABLE marktdata ADD COLUMN {kol} {typ}")
            except: pass

    # Voeg vorige_prijs toe aan advertenties als hij ontbreekt
    adv_kols = [r[1] for r in c.execute("PRAGMA table_info(advertenties)").fetchall()]
    if "vorige_prijs" not in adv_kols:
        try: c.execute("ALTER TABLE advertenties ADD COLUMN vorige_prijs INTEGER")
        except: pass

    conn.commit(); conn.close()
    log.info("Database OK")

def get_voorraad():
    """Haal alle actieve voorraadauto's op met hun specs."""
    conn = get_db(); c = conn.cursor()
    rows = c.execute("""
        SELECT id, merk, model, jaar, km, brandstof, seg as carrosserie,
               COALESCE(transmissie, 'Onbekend') as transmissie
        FROM voorraad
        WHERE status='actief' AND merk IS NOT NULL AND merk != ''
        ORDER BY merk, model
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_voorraad_combinaties(voorraad):
    """Bepaal unieke merk/model combinaties om te scrapen."""
    gezien = set(); combinaties = []
    for auto in voorraad:
        merk = auto['merk'].lower().strip()
        model_prefix = model_woorden(auto['model'], 1)  # Eerste woord voor zoekquery
        slug = MERK_SLUGS.get(merk)
        if not slug:
            log.warning(f"Geen Gaspedaal slug voor: {merk}")
            continue
        key = f"{merk}_{model_prefix}"
        if key not in gezien:
            gezien.add(key)
            combinaties.append({
                "merk": auto['merk'],
                "merk_lower": merk,
                "model_prefix": model_prefix,
                "slug": slug
            })
    log.info(f"{len(combinaties)} unieke merk/model combinaties te scrapen")
    return combinaties

# ═══ PARSE HELPERS ═══
def px(t):
    n = re.sub(r"[^\d]", "", t or "")
    try: p = int(n); return p if PRIJS_MIN <= p <= PRIJS_MAX else None
    except: return None

def km_p(t):
    n = re.sub(r"[^\d]", "", t or "")
    try: k = int(n); return k if 0 <= k <= KM_MAX else None
    except: return None

def jaar_p(t):
    m = re.search(r"(20\d{2}|199\d|200\d)", t or "")
    if m:
        j = int(m.group(1)); return j if j >= BOUWJAAR_VANAF else None
    return None

def nb(t):
    t = (t or "").lower()
    if "elektr" in t: return "Elektrisch"
    if "hybride" in t or "hybrid" in t: return "Hybride"
    if "diesel" in t: return "Diesel"
    if "benzine" in t: return "Benzine"
    if "lpg" in t: return "LPG"
    return "Onbekend"

def nt(t):
    t = (t or "").lower()
    if "automaat" in t or "autom" in t: return "Automaat"
    if "hand" in t or "geschakeld" in t: return "Handgeschakeld"
    return "Onbekend"

def nc(t):
    t = (t or "").lower()
    if "station" in t or "estate" in t or "combi" in t: return "Stationwagen"
    if "suv" in t or "terrein" in t or "crossover" in t: return "SUV"
    if "hatchback" in t: return "Hatchback"
    if "sedan" in t or "saloon" in t: return "Sedan"
    if "cabrio" in t or "cabriolet" in t: return "Cabrio"
    if "coupe" in t or "coupé" in t: return "Coupé"
    if "mpv" in t or "people" in t: return "MPV"
    return "Overig"

# ═══ GASPEDAAL ═══
GASPEDAAL = "https://www.gaspedaal.nl"

def bouw_url(slug, model_prefix="", pagina=1):
    base = f"{GASPEDAAL}/occasions/{slug}"
    if model_prefix:
        base += f"/{model_prefix}"
    params = (f"?bouwjaar_van={BOUWJAAR_VANAF}&kmstand_tot={KM_MAX}"
              f"&prijs_van={PRIJS_MIN}&prijs_tot={PRIJS_MAX}&sorteer=datum_aflopend")
    if pagina > 1:
        params += f"&pagina={pagina}"
    return base + params

def parse_pagina(html, merk):
    res = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        cards = (
            soup.find_all("article", class_=re.compile(r"search-result|listing|car", re.I)) or
            soup.find_all(attrs={"data-listing-id": True}) or
            soup.find_all("li", class_=re.compile(r"result|listing|occasion", re.I)) or
            soup.find_all("div", class_=re.compile(r"search-result|car-item|listing-item", re.I))
        )
        if not cards:
            log.warning(f"Geen kaarten gevonden ({merk}) — layout veranderd?")
            Path(f"/opt/carinsight/debug_{merk[:8].lower()}.html").write_text(html[:80000])
            return []

        for card in cards:
            try:
                lid = card.get("data-listing-id") or card.get("data-id") or card.get("id","")
                link = card.find("a", href=True)
                url = ""
                if link:
                    href = link["href"]
                    url = href if href.startswith("http") else GASPEDAAL+href
                    if not lid:
                        m = re.search(r"/(\d{5,})", href)
                        if m: lid = f"gp_{m.group(1)}"
                if not lid or not url: continue
                lid = f"gp_{lid}" if not str(lid).startswith("gp_") else str(lid)

                # Titel → model
                tel = (card.find(class_=re.compile(r"title|naam|car-name", re.I)) or
                       card.find(["h2","h3","h4"]))
                titel = tel.get_text(" ", strip=True) if tel else ""
                model = re.sub(re.escape(merk), "", titel, flags=re.I).strip(" -–—")[:60] or "Onbekend"

                # Prijs
                pel = card.find(class_=re.compile(r"prijs|price|bedrag", re.I))
                prijs = px(pel.get_text() if pel else "") or px(card.get_text(" "))
                if not prijs: continue

                tekst = card.get_text(" ", strip=True)
                jaar = jaar_p(tekst)
                if not jaar: continue

                kel = card.find(class_=re.compile(r"km|kilometer|mileage", re.I))
                km = km_p(kel.get_text() if kel else tekst)

                brandstof = "Onbekend"
                for t in card.find_all(string=re.compile(r"benzine|diesel|elektr|hybride|lpg", re.I)):
                    brandstof = nb(t.strip()); break

                transmissie = nt(tekst)
                carrosserie = nc(tekst)
                for t in card.find_all(string=re.compile(r"sedan|hatchback|station|suv|cabrio|coupe|mpv|terrein", re.I)):
                    carrosserie = nc(t.strip()); break

                del_el = card.find(class_=re.compile(r"dealer|verkoper|seller", re.I))
                dealer = del_el.get_text(strip=True) if del_el else "Onbekend"
                reg_el = card.find(class_=re.compile(r"regio|locatie|plaats|city", re.I))
                regio = reg_el.get_text(strip=True) if reg_el else ""

                res.append({"listing_id":lid, "platform":"Gaspedaal", "dealer":dealer,
                    "url":url, "merk":merk, "model":model, "jaar":jaar, "km":km,
                    "prijs":prijs, "brandstof":brandstof, "transmissie":transmissie,
                    "carrosserie":carrosserie, "regio":regio})
            except Exception as e:
                log.debug(f"Kaart skip: {e}")
    except Exception as e:
        log.error(f"Parse: {e}")
    return res

def scrape_combinatie(merk, model_prefix, slug):
    log.info(f"▶ Scrapen: {merk} {model_prefix}")
    alle = []; ref = GASPEDAAL

    for p in range(1, MAX_PAGINAS+1):
        url = bouw_url(slug, model_prefix, p)
        r = safe_get(url, ref)
        if not r: break
        if "captcha" in r.text.lower() or "geblokkeerd" in r.text.lower():
            log.warning(f"Geblokkeerd bij {merk} {model_prefix}"); break

        resultaten = parse_pagina(r.text, merk)
        if not resultaten: break

        alle.extend(resultaten)
        log.info(f"  p{p}: +{len(resultaten)} ({len(alle)} totaal)")

        soup = BeautifulSoup(r.text, "html.parser")
        if not (soup.find("a", rel="next") or
                soup.find("a", class_=re.compile(r"next|volgende", re.I))): break

        ref = url; pauze("pagina")

    log.info(f"✓ {merk} {model_prefix}: {len(alle)} advertenties gevonden")
    return alle

# ═══ VERGELIJKING & OPSLAAN ═══
def sla_op(conn, adv):
    """Sla advertentie op of update hem. Bewaar prijshistorie."""
    c = conn.cursor(); vd = date.today().isoformat()
    b = c.execute("""SELECT id, huidige_prijs, vorige_prijs, prijs_historie, eerste_gezien
                     FROM advertenties WHERE listing_id=?""",
                  (adv["listing_id"],)).fetchone()
    if b:
        hist = json.loads(b["prijs_historie"] or "[]")
        oude_prijs = b["huidige_prijs"]
        nieuwe_prijs = adv.get("prijs")
        vorige_prijs = b["vorige_prijs"]

        if nieuwe_prijs and nieuwe_prijs != oude_prijs:
            # Prijswijziging — bewaar in historie
            hist.append({
                "datum": vd,
                "prijs": nieuwe_prijs,
                "delta": nieuwe_prijs - (oude_prijs or nieuwe_prijs)
            })
            vorige_prijs = oude_prijs  # Oude prijs wordt vorige_prijs

        dom = (date.today() - date.fromisoformat(b["eerste_gezien"])).days
        c.execute("""UPDATE advertenties SET
            laatste_gezien=?, huidige_prijs=?, vorige_prijs=?,
            prijs_historie=?, days_on_market=?, verdwenen=0, verdwenen_op=NULL
            WHERE listing_id=?""",
            (vd, nieuwe_prijs or oude_prijs, vorige_prijs,
             json.dumps(hist), dom, adv["listing_id"]))
    else:
        # Nieuwe advertentie
        c.execute("""INSERT INTO advertenties (
            listing_id, platform, dealer, url, merk, model, jaar, km,
            brandstof, transmissie, carrosserie, regio,
            eerste_prijs, vorige_prijs, huidige_prijs,
            prijs_historie, eerste_gezien, laatste_gezien, verdwenen, days_on_market)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,'[]',?,?,0,0)""",
            (adv["listing_id"], adv.get("platform"), adv.get("dealer"), adv.get("url"),
             adv.get("merk"), adv.get("model"), adv.get("jaar"), adv.get("km"),
             adv.get("brandstof"), adv.get("transmissie"), adv.get("carrosserie"),
             adv.get("regio"), adv.get("prijs"), adv.get("prijs"), vd, vd))

def koppel_vergelijkingen(conn, voorraad, advertenties):
    """
    Koppel advertenties aan voorraadauto's op basis van vergelijkingslogica.
    Slaat alleen echt vergelijkbare advertenties op in vergelijkingen tabel.
    """
    c = conn.cursor(); vd = datetime.now().isoformat()
    matches = 0; no_matches = 0

    for adv in advertenties:
        for auto in voorraad:
            ok, reden = is_vergelijkbaar(auto, adv)
            if ok:
                try:
                    c.execute("""INSERT OR IGNORE INTO vergelijkingen
                        (voorraad_id, advertentie_listing_id, match_score, aangemaakt)
                        VALUES (?,?,?,?)""",
                        (auto['id'], adv['listing_id'], 100, vd))
                    matches += 1
                except Exception as e:
                    log.debug(f"Vergelijking insert: {e}")
            else:
                no_matches += 1
                log.debug(f"Geen match {auto['merk']} {auto['model']} ↔ {adv['merk']} {adv['model']}: {reden}")

    log.info(f"  Vergelijkingen: {matches} matches, {no_matches} afgewezen")

def markeer_verdwenen(conn, platform, gezien, merk):
    c = conn.cursor(); vd = date.today().isoformat()
    actief = c.execute(
        "SELECT listing_id FROM advertenties WHERE platform=? AND merk=? AND verdwenen=0",
        (platform, merk)).fetchall()
    n = 0
    for r in actief:
        if r["listing_id"] not in gezien:
            c.execute("UPDATE advertenties SET verdwenen=1, verdwenen_op=? WHERE listing_id=?",
                      (vd, r["listing_id"])); n += 1
    if n: log.info(f"  {n} advertenties verdwenen ({merk})")

def bereken_marktdata(conn, merk, model_prefix):
    """Bereken marktstatistieken op basis van vergelijkbare advertenties."""
    c = conn.cursor(); vd = date.today().isoformat()
    d14 = (date.today()-timedelta(days=14)).isoformat()
    d3  = (date.today()-timedelta(days=3)).isoformat()

    # Gebruik alleen advertenties die via vergelijkingen zijn gekoppeld
    actief = c.execute("""
        SELECT a.huidige_prijs, a.km, a.days_on_market
        FROM advertenties a
        INNER JOIN vergelijkingen v ON a.listing_id = v.advertentie_listing_id
        INNER JOIN voorraad vr ON v.voorraad_id = vr.id
        WHERE lower(vr.merk)=lower(?) AND a.verdwenen=0
    """, (merk,)).fetchall()

    if not actief: return

    prijzen = sorted([r["huidige_prijs"] for r in actief if r["huidige_prijs"]])
    if not prijzen: return

    n = len(prijzen); mediaan = prijzen[n//2]
    kms = [r["km"] for r in actief if r["km"]]
    gem_km = int(sum(kms)/len(kms)) if kms else 0
    gem_dom = int(sum(r["days_on_market"] for r in actief)/n)

    n_nieuw = c.execute("SELECT COUNT(*) FROM advertenties WHERE merk=? AND eerste_gezien>=?", (merk,d3)).fetchone()[0]
    n_verd  = c.execute("SELECT COUNT(*) FROM advertenties WHERE merk=? AND verdwenen=1 AND verdwenen_op>=?", (merk,d3)).fetchone()[0]
    tot14   = c.execute("SELECT COUNT(*) FROM advertenties WHERE merk=? AND eerste_gezien>=?", (merk,d14)).fetchone()[0]
    vk14    = c.execute("SELECT COUNT(*) FROM advertenties WHERE merk=? AND verdwenen=1 AND eerste_gezien>=? AND days_on_market<=14", (merk,d14)).fetchone()[0]
    pct14   = round(vk14/max(1,tot14)*100, 1)

    try:
        c.execute("""INSERT OR REPLACE INTO marktdata_historie
            (datum,merk,model,platform,n_actief,n_nieuw,n_verdwenen,
             prijs_mediaan,prijs_min,prijs_max,gem_dom,gem_km,pct_verkocht_14d)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (vd,merk,model_prefix or "*","Gaspedaal",
             n,n_nieuw,n_verd,mediaan,prijzen[0],prijzen[-1],gem_dom,gem_km,pct14))
    except Exception as e: log.error(f"Snapshot: {e}")

    try:
        c.execute("""INSERT OR REPLACE INTO marktdata
            (merk,model,n_advertenties,prijs_mediaan,prijs_min,prijs_max,platforms,gescraped_op)
            VALUES (?,?,?,?,?,?,?,?)""",
            (merk,model_prefix or "*",n,mediaan,prijzen[0],prijzen[-1],"Gaspedaal",vd))
    except: pass

    # Trend berekening
    vorige = c.execute("""SELECT n_actief, prijs_mediaan FROM marktdata_historie
        WHERE merk=? AND datum<? ORDER BY datum DESC LIMIT 1""", (merk,vd)).fetchone()
    if vorige:
        ad = n-vorige["n_actief"]; pd = mediaan-vorige["prijs_mediaan"]
        if ad>15 and pd<0:    mom = "Overaanbod — prijsdruk"
        elif ad<-10:           mom = "Aanbod daalt snel"
        elif pct14>60:         mom = "Vraag sterk"
        elif pct14<20:         mom = "Traag segment"
        else:                  mom = "Stabiel"
        c.execute("""INSERT OR REPLACE INTO markttrend
            (merk,model,bijgewerkt,aanbod_delta,prijs_delta,vt_mediaan,momentum,pct_verkocht_14d,n_meetpunten)
            VALUES (?,?,datetime('now'),?,?,?,?,?,
                COALESCE((SELECT n_meetpunten+1 FROM markttrend WHERE merk=? AND model=?),1))""",
            (merk,model_prefix or "*",ad,pd,gem_dom,mom,pct14,merk,model_prefix or "*"))

    log.info(f"  Markt {merk} {model_prefix}: {n} vergelijkbaar | €{mediaan:,} mediaan | {pct14}% <14d | {gem_dom}d DOM")

# ═══ HOOFDPROGRAMMA ═══
def run(test_modus=False):
    log.info("="*60)
    log.info("CarInsight Gaspedaal Scraper v3")
    log.info(f"Filters: {BOUWJAAR_VANAF}+ | {KM_MAX}km | €{PRIJS_MIN}-€{PRIJS_MAX}")
    log.info(f"Vergelijking: model 2 woorden | jaar±{JAAR_MARGE} | km±{int(KM_MARGE*100)}% | brandstof+transmissie+carrosserie exact")
    log.info("="*60)

    init_db()

    if test_modus:
        # Test met dummy voorraad als er niets in de DB staat
        voorraad = get_voorraad()
        if not voorraad:
            log.info("Geen voorraad gevonden — test met Volkswagen Golf")
            voorraad = [{"id":0,"merk":"Volkswagen","model":"Golf 1.5 TSI",
                         "jaar":2020,"km":60000,"brandstof":"Benzine",
                         "transmissie":"Handgeschakeld","carrosserie":"Hatchback"}]
    else:
        voorraad = get_voorraad()

    if not voorraad:
        log.warning("Geen actieve voorraad gevonden. Voeg eerst auto's toe!")
        return

    log.info(f"Voorraad: {len(voorraad)} auto's")
    combinaties = get_voorraad_combinaties(voorraad)
    if not combinaties:
        log.warning("Geen scrapeable combinaties gevonden")
        return

    random.shuffle(combinaties)
    conn = get_db()
    totaal = 0; fouten = []

    for combo in combinaties:
        if not test_modus and not is_scrape_tijd():
            log.info("Buiten tijdslot — wacht...")
            conn.commit(); wacht_op_tijd()

        merk = combo["merk"]
        model_prefix = combo["model_prefix"]
        slug = combo["slug"]

        try:
            advertenties = scrape_combinatie(merk, model_prefix, slug)
            if not advertenties:
                fouten.append(f"{merk} {model_prefix}"); pauze("combinatie"); continue

            # Sla alle advertenties op
            gezien = set()
            for adv in advertenties:
                try: sla_op(conn, adv); gezien.add(adv["listing_id"])
                except Exception as e: log.debug(f"Sla op: {e}")
            conn.commit()

            # Koppel vergelijkbare advertenties aan voorraadauto's
            koppel_vergelijkingen(conn, voorraad, advertenties)
            conn.commit()

            # Markeer verdwenen
            markeer_verdwenen(conn, "Gaspedaal", gezien, merk)
            conn.commit()

            # Bereken marktdata op basis van vergelijkingen
            bereken_marktdata(conn, merk, model_prefix)
            conn.commit()

            totaal += len(advertenties)
            pauze("combinatie")

        except Exception as e:
            log.error(f"{merk} {model_prefix}: {e}")
            fouten.append(f"{merk} {model_prefix}")
            pauze("combinatie")

    conn.close()
    log.info("="*60)
    log.info(f"Klaar: {totaal} advertenties | {len(combinaties)} combinaties")
    if fouten: log.warning(f"Fouten: {', '.join(fouten)}")
    log.info("="*60)

    try:
        c2 = get_db()
        c2.execute("INSERT OR REPLACE INTO scraper_status (platform,actief,laatste_run,n_gevonden) VALUES ('gaspedaal',1,datetime('now'),?)",(totaal,))
        c2.commit(); c2.close()
    except: pass

if __name__ == "__main__":
    test = "--test" in sys.argv or "--nu" in sys.argv
    if not test:
        wacht_op_tijd()
    run(test_modus=test)
