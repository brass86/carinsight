"""
CarInsight Gaspedaal Scraper v4
================================
Gebruikt de ingebouwde JSON (schema.org) data — veel betrouwbaarder dan HTML selectors.

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
Test: python scraper.py --test
Prod: python scraper.py
"""
import sqlite3, requests, json, time, random, logging, re, sys, os
from datetime import datetime, date, timedelta
from pathlib import Path
from bs4 import BeautifulSoup

# Pad detectie: server (Linux) of lokaal (Windows)
if os.path.exists("/opt/carinsight/data"):
    DB_PATH  = "/opt/carinsight/data/carinsight.db"
    LOG_PATH = "/opt/carinsight/scraper.log"
else:
    DB_PATH  = str(Path(__file__).parent / "carinsight_local.db")
    LOG_PATH = str(Path(__file__).parent / "scraper.log")

BOUWJAAR_VANAF = 2005
KM_MAX         = 300000
PRIJS_MIN      = 3000
PRIJS_MAX      = 150000
MAX_PAGINAS    = 10
JAAR_MARGE     = 2      # ± 2 jaar
KM_MARGE       = 0.20   # ± 20%

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
    h = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive"
    }
    if ref:
        h["Referer"] = ref
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
        if r.status_code == 429:
            log.warning("Rate limited — 5min wachten")
            time.sleep(300)
            return None
        if r.status_code == 403:
            log.warning("403 geblokkeerd — 2min wachten")
            time.sleep(120)
            return None
        return r if r.status_code == 200 else None
    except Exception as e:
        log.error(f"GET fout: {e}")
        return None

def model_woorden(model, n=2):
    woorden = re.sub(r'[^a-z0-9 ]', '', (model or '').lower().strip()).split()
    return ' '.join(woorden[:n])

def is_vergelijkbaar(voorraad_auto, advertentie):
    v = voorraad_auto
    a = advertentie

    if v['merk'].lower() != (a.get('merk') or '').lower():
        return False, "merk"

    v_model = model_woorden(v['model'], 2)
    a_model = model_woorden(a.get('model'), 2)
    if not v_model or not a_model or v_model != a_model:
        return False, f"model ({v_model!r} vs {a_model!r})"

    if v.get('jaar') and a.get('jaar'):
        if abs(v['jaar'] - a['jaar']) > JAAR_MARGE:
            return False, f"jaar ({v['jaar']} vs {a['jaar']})"

    if v.get('km') and a.get('km') and v['km'] > 0:
        km_min = v['km'] * (1 - KM_MARGE)
        km_max = v['km'] * (1 + KM_MARGE)
        if not (km_min <= a['km'] <= km_max):
            return False, f"km ({v['km']} vs {a['km']})"

    v_bs = (v.get('brandstof') or '').lower()
    a_bs = (a.get('brandstof') or '').lower()
    if v_bs and a_bs and v_bs != 'onbekend' and a_bs != 'onbekend':
        if v_bs != a_bs:
            return False, f"brandstof ({v_bs} vs {a_bs})"

    v_tr = (v.get('transmissie') or '').lower()
    a_tr = (a.get('transmissie') or '').lower()
    if v_tr and a_tr and v_tr != 'onbekend' and a_tr != 'onbekend':
        if v_tr != a_tr:
            return False, f"transmissie ({v_tr} vs {a_tr})"

    v_car = (v.get('carrosserie') or v.get('seg') or '').lower()
    a_car = (a.get('carrosserie') or '').lower()
    if v_car and a_car and v_car != 'onbekend' and a_car != 'onbekend':
        if v_car != a_car:
            return False, f"carrosserie ({v_car} vs {a_car})"

    return True, "match"

# ═══ JSON PARSER ═══
def parse_json_listings(html_text):
    """Haalt advertenties op uit de ingebouwde schema.org JSON op Gaspedaal."""
    soup = BeautifulSoup(html_text, "html.parser")
    advertenties = []

    for script in soup.find_all("script"):
        if not script.string:
            continue
        if "itemListElement" not in script.string:
            continue
        try:
            data = json.loads(script.string)
            items = data.get("itemListElement", [])
            for item in items:
                car = item.get("item", {})
                try:
                    # Listing ID uit @id URL
                    at_id = car.get("@id", "")
                    listing_id = "gaspedaal_" + at_id.split("#")[-1] if "#" in at_id else None
                    if not listing_id:
                        continue

                    merk  = car.get("brand", "")
                    model = car.get("model", "")
                    jaar  = car.get("productionDate") or car.get("vehicleModelDate")
                    km    = (car.get("mileageFromOdometer") or {}).get("value")

                    brandstof    = car.get("fuelType", "")
                    transmissie  = car.get("vehicleTransmission", "")
                    carrosserie  = car.get("bodyType", "")

                    offers = car.get("offers", {})
                    prijs  = offers.get("price")
                    seller = offers.get("seller", {})
                    dealer = seller.get("name", "Onbekend")
                    adres  = seller.get("address", {})
                    regio  = adres.get("addressLocality", "")

                    url = at_id.replace("#", "/") if "#" in at_id else at_id

                    # Filters
                    if jaar and jaar < BOUWJAAR_VANAF:
                        continue
                    if km and km > KM_MAX:
                        continue
                    if prijs and (prijs < PRIJS_MIN or prijs > PRIJS_MAX):
                        continue

                    advertenties.append({
                        "listing_id":  listing_id,
                        "platform":    "Gaspedaal",
                        "url":         url,
                        "merk":        merk,
                        "model":       model,
                        "jaar":        jaar,
                        "km":          km,
                        "brandstof":   brandstof,
                        "transmissie": transmissie,
                        "carrosserie": carrosserie,
                        "dealer":      dealer,
                        "regio":       regio,
                        "prijs":       prijs,
                    })
                except Exception as e:
                    log.debug(f"Parse item fout: {e}")
            break  # Gevonden, stop zoeken
        except Exception as e:
            log.debug(f"JSON parse fout: {e}")
            continue

    return advertenties

# ═══ SCRAPER ═══
def scrape_pagina(url, ref=None):
    r = safe_get(url, ref)
    if not r:
        return [], False
    advertenties = parse_json_listings(r.text)
    # Controleer of er een volgende pagina is
    heeft_volgende = '"nextPage"' in r.text or 'page=' in r.text
    return advertenties, heeft_volgende

def scrape_combinatie(merk, model_prefix, slug):
    base_url = f"https://www.gaspedaal.nl/{slug}"
    if model_prefix:
        model_slug = re.sub(r'[^a-z0-9]', '-', model_prefix.lower().strip())
        base_url = f"https://www.gaspedaal.nl/{slug}/{model_slug}"

    log.info(f"Scraping: {merk} {model_prefix} → {base_url}")

    alle = []
    ref = "https://www.gaspedaal.nl/"

    for pagina in range(1, MAX_PAGINAS + 1):
        url = base_url if pagina == 1 else f"{base_url}?page={pagina}"
        advertenties, heeft_volgende = scrape_pagina(url, ref)

        if not advertenties:
            log.info(f"  Pagina {pagina}: geen resultaten, stop")
            break

        log.info(f"  Pagina {pagina}: {len(advertenties)} advertenties")
        alle.extend(advertenties)
        ref = url

        if not heeft_volgende or pagina >= MAX_PAGINAS:
            break

        pauze("pagina")

    log.info(f"  Totaal {merk} {model_prefix}: {len(alle)} advertenties")
    return alle

# ═══ DATABASE ═══
def get_db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    conn = get_db(); c = conn.cursor()

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

    c.execute("""CREATE TABLE IF NOT EXISTS vergelijkingen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        voorraad_id INTEGER,
        advertentie_listing_id TEXT,
        match_score INTEGER DEFAULT 100,
        aangemaakt TEXT,
        UNIQUE(voorraad_id, advertentie_listing_id)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS marktdata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        merk TEXT,
        model TEXT,
        n_advertenties INTEGER,
        prijs_mediaan INTEGER,
        prijs_min INTEGER,
        prijs_max INTEGER,
        platforms TEXT,
        gescraped_op TEXT,
        UNIQUE(merk, model)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS marktdata_historie (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datum TEXT,
        merk TEXT,
        model TEXT,
        platform TEXT,
        n_actief INTEGER,
        n_nieuw INTEGER,
        n_verdwenen INTEGER,
        prijs_mediaan INTEGER,
        prijs_min INTEGER,
        prijs_max INTEGER,
        gem_dom INTEGER,
        gem_km INTEGER,
        pct_verkocht_14d REAL,
        UNIQUE(datum, merk, model, platform)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS markttrend (
        merk TEXT,
        model TEXT,
        bijgewerkt TEXT,
        aanbod_delta INTEGER,
        prijs_delta INTEGER,
        vt_mediaan INTEGER,
        momentum TEXT,
        pct_verkocht_14d REAL,
        n_meetpunten INTEGER DEFAULT 1,
        PRIMARY KEY(merk, model)
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS scraper_status (
        platform TEXT PRIMARY KEY,
        actief INTEGER DEFAULT 1,
        laatste_run TEXT,
        n_gevonden INTEGER DEFAULT 0
    )""")

    # Zorg dat voorraad tabel bestaat (minimaal voor lokale tests)
    c.execute("""CREATE TABLE IF NOT EXISTS voorraad (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dealer_id INTEGER DEFAULT 1,
        merk TEXT,
        model TEXT,
        jaar INTEGER,
        km INTEGER,
        brandstof TEXT,
        transmissie TEXT,
        carrosserie TEXT,
        seg TEXT,
        status TEXT DEFAULT 'actief'
    )""")

    conn.commit(); conn.close()
    log.info("Database OK")

def get_voorraad():
    try:
        conn = get_db(); c = conn.cursor()
        rows = c.execute("""
            SELECT id, merk, model, jaar, km, brandstof, transmissie,
                   COALESCE(carrosserie, seg) as carrosserie
            FROM voorraad WHERE status='actief'
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log.error(f"Voorraad ophalen: {e}")
        return []

def get_voorraad_combinaties(voorraad):
    seen = set(); combinaties = []
    for auto in voorraad:
        merk = (auto.get('merk') or '').strip()
        model = (auto.get('model') or '').strip()
        if not merk:
            continue
        slug = MERK_SLUGS.get(merk.lower())
        if not slug:
            log.warning(f"Geen slug voor merk: {merk}")
            continue
        model_prefix = model_woorden(model, 1)
        key = (merk.lower(), model_prefix)
        if key not in seen:
            seen.add(key)
            combinaties.append({"merk": merk, "model_prefix": model_prefix, "slug": slug})
    return combinaties

def sla_op(conn, adv):
    c = conn.cursor()
    vd = datetime.now().isoformat()
    bestaand = c.execute(
        "SELECT huidige_prijs, prijs_historie, eerste_gezien FROM advertenties WHERE listing_id=?",
        (adv["listing_id"],)
    ).fetchone()

    if bestaand:
        oude_prijs = bestaand["huidige_prijs"]
        historie = json.loads(bestaand["prijs_historie"] or "[]")
        eerste = bestaand["eerste_gezien"]
        dom = (date.today() - date.fromisoformat(eerste[:10])).days

        if adv["prijs"] and adv["prijs"] != oude_prijs:
            historie.append({"datum": vd[:10], "prijs": adv["prijs"]})

        c.execute("""UPDATE advertenties SET
            laatste_gezien=?, huidige_prijs=?,
            vorige_prijs=COALESCE(?,vorige_prijs),
            prijs_historie=?, days_on_market=?, verdwenen=0, verdwenen_op=NULL
            WHERE listing_id=?""",
            (vd, adv["prijs"],
             oude_prijs if adv["prijs"] != oude_prijs else None,
             json.dumps(historie), dom, adv["listing_id"]))
    else:
        c.execute("""INSERT OR IGNORE INTO advertenties
            (listing_id,platform,dealer,url,merk,model,jaar,km,
             brandstof,transmissie,carrosserie,regio,
             eerste_prijs,vorige_prijs,huidige_prijs,eerste_gezien,laatste_gezien)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (adv["listing_id"], adv["platform"], adv["dealer"], adv["url"],
             adv["merk"], adv["model"], adv["jaar"], adv["km"],
             adv["brandstof"], adv["transmissie"], adv["carrosserie"],
             adv["regio"], adv["prijs"], adv["prijs"], adv["prijs"], vd, vd))

def koppel_vergelijkingen(conn, voorraad, advertenties):
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

    log.info(f"  Vergelijkingen: {matches} matches, {no_matches} afgewezen")

def markeer_verdwenen(conn, platform, gezien, merk):
    c = conn.cursor(); vd = date.today().isoformat()
    actief = c.execute(
        "SELECT listing_id FROM advertenties WHERE platform=? AND merk=? AND verdwenen=0",
        (platform, merk)
    ).fetchall()
    n = 0
    for r in actief:
        if r["listing_id"] not in gezien:
            c.execute("UPDATE advertenties SET verdwenen=1, verdwenen_op=? WHERE listing_id=?",
                      (vd, r["listing_id"]))
            n += 1
    if n:
        log.info(f"  {n} advertenties verdwenen ({merk})")

def bereken_marktdata(conn, merk, model_prefix):
    c = conn.cursor(); vd = date.today().isoformat()
    d14 = (date.today()-timedelta(days=14)).isoformat()
    d3  = (date.today()-timedelta(days=3)).isoformat()

    actief = c.execute("""
        SELECT a.huidige_prijs, a.km, a.days_on_market
        FROM advertenties a
        INNER JOIN vergelijkingen v ON a.listing_id = v.advertentie_listing_id
        INNER JOIN voorraad vr ON v.voorraad_id = vr.id
        WHERE lower(vr.merk)=lower(?) AND a.verdwenen=0
    """, (merk,)).fetchall()

    if not actief:
        return

    prijzen = sorted([r["huidige_prijs"] for r in actief if r["huidige_prijs"]])
    if not prijzen:
        return

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
    except Exception as e:
        log.error(f"Snapshot: {e}")

    try:
        c.execute("""INSERT OR REPLACE INTO marktdata
            (merk,model,n_advertenties,prijs_mediaan,prijs_min,prijs_max,platforms,gescraped_op)
            VALUES (?,?,?,?,?,?,?,?)""",
            (merk,model_prefix or "*",n,mediaan,prijzen[0],prijzen[-1],"Gaspedaal",vd))
    except:
        pass

    log.info(f"  Markt {merk} {model_prefix}: {n} vergelijkbaar | €{mediaan:,} mediaan | {pct14}% <14d | {gem_dom}d DOM")

# ═══ HOOFDPROGRAMMA ═══
def run(test_modus=False):
    log.info("="*60)
    log.info("CarInsight Gaspedaal Scraper v4 (JSON-gebaseerd)")
    log.info(f"Database: {DB_PATH}")
    log.info(f"Filters: {BOUWJAAR_VANAF}+ | {KM_MAX}km | €{PRIJS_MIN}-€{PRIJS_MAX}")
    log.info("="*60)

    init_db()

    if test_modus:
        voorraad = get_voorraad()
        if not voorraad:
            log.info("Geen voorraad gevonden — test met Volkswagen Golf")
            voorraad = [{
                "id": 0, "merk": "Volkswagen", "model": "Golf 1.5 TSI",
                "jaar": 2020, "km": 60000, "brandstof": "Benzine",
                "transmissie": "Handgeschakeld", "carrosserie": "Hatchback"
            }]
    else:
        voorraad = get_voorraad()

    if not voorraad:
        log.warning("Geen actieve voorraad gevonden.")
        return

    log.info(f"Voorraad: {len(voorraad)} auto's")
    combinaties = get_voorraad_combinaties(voorraad)
    if not combinaties:
        log.warning("Geen scrapeable combinaties")
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
                fouten.append(f"{merk} {model_prefix}")
                pauze("combinatie")
                continue

            gezien = set()
            for adv in advertenties:
                try:
                    sla_op(conn, adv)
                    gezien.add(adv["listing_id"])
                except Exception as e:
                    log.debug(f"Sla op: {e}")
            conn.commit()

            koppel_vergelijkingen(conn, voorraad, advertenties)
            conn.commit()

            markeer_verdwenen(conn, "Gaspedaal", gezien, merk)
            conn.commit()

            bereken_marktdata(conn, merk, model_prefix)
            conn.commit()

            totaal += len(advertenties)

            if not test_modus:
                pauze("combinatie")

        except Exception as e:
            log.error(f"{merk} {model_prefix}: {e}")
            fouten.append(f"{merk} {model_prefix}")

    conn.close()
    log.info("="*60)
    log.info(f"Klaar: {totaal} advertenties | {len(combinaties)} combinaties")
    if fouten:
        log.warning(f"Fouten: {', '.join(fouten)}")
    log.info("="*60)

    try:
        c2 = get_db()
        c2.execute("""INSERT OR REPLACE INTO scraper_status
            (platform,actief,laatste_run,n_gevonden) VALUES ('gaspedaal',1,datetime('now'),?)""",
            (totaal,))
        c2.commit(); c2.close()
    except:
        pass

if __name__ == "__main__":
    test = "--test" in sys.argv or "--nu" in sys.argv
    if not test:
        wacht_op_tijd()
    run(test_modus=test)
