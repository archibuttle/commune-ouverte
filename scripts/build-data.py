#!/usr/bin/env python3
"""
Build data pipeline for commune-ouverte.
Fetches finances, elected officials, and election results for the 10 largest French cities.
Outputs JSON files in src/data/communes/ and public/data/communes-index.json.
"""

import json
import os
import csv
import io
import urllib.request
import urllib.parse
import time
import sys

# 10 largest cities
CITIES = [
    {"code": "75056", "nom": "Paris", "dep_code": "75", "dep_nom": "Paris"},
    {"code": "13055", "nom": "Marseille", "dep_code": "13", "dep_nom": "Bouches-du-Rhône"},
    {"code": "69123", "nom": "Lyon", "dep_code": "69", "dep_nom": "Rhône"},
    {"code": "31555", "nom": "Toulouse", "dep_code": "31", "dep_nom": "Haute-Garonne"},
    {"code": "06088", "nom": "Nice", "dep_code": "06", "dep_nom": "Alpes-Maritimes"},
    {"code": "44109", "nom": "Nantes", "dep_code": "44", "dep_nom": "Loire-Atlantique"},
    {"code": "34172", "nom": "Montpellier", "dep_code": "34", "dep_nom": "Hérault"},
    {"code": "67482", "nom": "Strasbourg", "dep_code": "67", "dep_nom": "Bas-Rhin"},
    {"code": "33063", "nom": "Bordeaux", "dep_code": "33", "dep_nom": "Gironde"},
    {"code": "59350", "nom": "Lille", "dep_code": "59", "dep_nom": "Nord"},
]

AGREGATS = [
    "Encours de dette",
    "Dépenses d'investissement",
    "Recettes de fonctionnement",
    "Dépenses de fonctionnement",
    "Frais de personnel",
    "Epargne brute",
]

AGREGAT_KEYS = {
    "Encours de dette": "encours_dette",
    "Dépenses d'investissement": "depenses_investissement",
    "Recettes de fonctionnement": "recettes_fonctionnement",
    "Dépenses de fonctionnement": "depenses_fonctionnement",
    "Frais de personnel": "frais_personnel",
    "Epargne brute": "epargne_brute",
}

AGREGAT_LABELS = {
    "encours_dette": "Encours de dette",
    "depenses_investissement": "Dépenses d'investissement",
    "recettes_fonctionnement": "Recettes de fonctionnement",
    "depenses_fonctionnement": "Dépenses de fonctionnement",
    "frais_personnel": "Frais de personnel",
    "epargne_brute": "Épargne brute",
}

# Score weights for global financial score
SCORE_WEIGHTS = {
    "encours_dette": 0.25,        # lower is better (inverted)
    "depenses_investissement": 0.20,  # higher is better
    "epargne_brute": 0.20,        # higher is better
    "frais_personnel": 0.15,      # lower is better (inverted)
    "depenses_fonctionnement": 0.10,  # lower is better (inverted)
    "recettes_fonctionnement": 0.10,  # higher is better
}

# KPIs where lower is better
INVERTED_KPIS = {"encours_dette", "frais_personnel", "depenses_fonctionnement"}

OFGL_BASE = "https://data.ofgl.fr/api/explore/v2.1/catalog/datasets/ofgl-base-communes-consolidee/records"
RNE_URL = "https://static.data.gouv.fr/resources/repertoire-national-des-elus-1/20251223-103336/elus-conseillers-municipaux-cm.csv"
ELECTIONS_T1_URL = "https://static.data.gouv.fr/resources/elections-municipales-2020-resultats/20200525-133704/2020-05-18-resultats-communes-de-1000-et-plus.txt"
ELECTIONS_T2_URL = "https://static.data.gouv.fr/resources/municipales-2020-resultats-2nd-tour/20200629-192435/2020-06-29-resultats-t2-communes-de-1000-hab-et-plus.txt"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "raw")
DATA_DIR = os.path.join(BASE_DIR, "src", "data", "communes")
PUBLIC_DIR = os.path.join(BASE_DIR, "public", "data")


def fetch_json(url):
    """Fetch JSON from URL."""
    req = urllib.request.Request(url, headers={"User-Agent": "commune-ouverte/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_text(url, encoding="utf-8"):
    """Fetch text from URL with specified encoding."""
    req = urllib.request.Request(url, headers={"User-Agent": "commune-ouverte/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read().decode(encoding)


def fetch_finances(city_code):
    """Fetch financial data for a city from OFGL API."""
    agregats_str = ",".join(f'"{a}"' for a in AGREGATS)
    where = f'com_code="{city_code}" AND agregat IN ({agregats_str})'
    params = urllib.parse.urlencode({
        "where": where,
        "select": "exer,agregat,euros_par_habitant,ptot,tranche_population",
        "order_by": "exer ASC",
        "limit": 100,
    })
    url = f"{OFGL_BASE}?{params}"
    data = fetch_json(url)
    return data.get("results", [])


def fetch_strate_averages(strate, year="2024"):
    """Fetch average values for a population strate for scoring."""
    averages = {}
    for agregat in AGREGATS:
        key = AGREGAT_KEYS[agregat]
        where = f'tranche_population="{strate}" AND agregat="{agregat}" AND year(exer)={year}'
        params = urllib.parse.urlencode({
            "where": where,
            "select": "AVG(euros_par_habitant) as avg_val, COUNT(*) as cnt",
            "limit": 1,
        })
        url = f"{OFGL_BASE}?{params}"
        try:
            data = fetch_json(url)
            results = data.get("results", [])
            if results and results[0].get("avg_val") is not None:
                averages[key] = round(results[0]["avg_val"], 1)
        except Exception as e:
            print(f"  Warning: could not fetch strate avg for {agregat}: {e}")
        time.sleep(0.2)  # Be polite to the API
    return averages


def compute_badge(value, strate_avg, inverted=False):
    """Compute a badge based on comparison with strate average."""
    if strate_avg is None or strate_avg == 0:
        return "MOYEN"
    ratio = value / strate_avg
    if inverted:
        # Lower is better
        if ratio < 0.75:
            return "BON"
        elif ratio < 0.90:
            return "CORRECT"
        elif ratio < 1.10:
            return "MOYEN"
        elif ratio < 1.25:
            return "FAIBLE"
        else:
            return "CRITIQUE"
    else:
        # Higher is better
        if ratio > 1.25:
            return "BON"
        elif ratio > 1.10:
            return "CORRECT"
        elif ratio > 0.90:
            return "MOYEN"
        elif ratio > 0.75:
            return "FAIBLE"
        else:
            return "CRITIQUE"


def compute_global_score(kpis, strate_avgs):
    """Compute a 0-100 global financial score."""
    total_weight = 0
    weighted_sum = 0

    for key, weight in SCORE_WEIGHTS.items():
        kpi = kpis.get(key)
        avg = strate_avgs.get(key)
        if kpi is None or avg is None or avg == 0:
            continue
        # Get the latest value (2024)
        value = kpi["series"][-1] if kpi["series"] else None
        if value is None:
            continue

        ratio = value / avg
        inverted = key in INVERTED_KPIS

        if inverted:
            # Lower is better: score = 100 when ratio=0.5, 50 when ratio=1.0, 0 when ratio=1.5
            score = max(0, min(100, 100 - (ratio - 0.5) * 100))
        else:
            # Higher is better: score = 0 when ratio=0.5, 50 when ratio=1.0, 100 when ratio=1.5
            score = max(0, min(100, (ratio - 0.5) * 100))

        weighted_sum += score * weight
        total_weight += weight

    if total_weight == 0:
        return 50
    return round(weighted_sum / total_weight)


def score_to_badge(score):
    """Convert 0-100 score to badge."""
    if score >= 75:
        return "BON"
    elif score >= 60:
        return "CORRECT"
    elif score >= 45:
        return "MOYEN"
    elif score >= 30:
        return "FAIBLE"
    else:
        return "CRITIQUE"


def build_finances(city_code):
    """Build the finances section for a city."""
    print(f"  Fetching finances for {city_code}...")
    records = fetch_finances(city_code)

    if not records:
        print(f"  WARNING: No finance data for {city_code}")
        return None, None, None

    # Extract population and strate from first record
    population = records[0].get("ptot", 0)
    strate = records[0].get("tranche_population", "")

    # Organize by KPI
    kpis = {}
    for agregat, key in AGREGAT_KEYS.items():
        matching = [r for r in records if r["agregat"] == agregat]
        matching.sort(key=lambda r: r["exer"])

        annees = [int(r["exer"][:4]) for r in matching]
        series = [round(r["euros_par_habitant"], 1) if r["euros_par_habitant"] else 0 for r in matching]

        # Delta: compare last year vs 2020 (start of mandate)
        val_2020 = None
        val_latest = series[-1] if series else None
        for i, a in enumerate(annees):
            if a == 2020:
                val_2020 = series[i]
                break

        delta_pct = None
        if val_2020 and val_latest and val_2020 != 0:
            delta_pct = round((val_latest - val_2020) / abs(val_2020) * 100, 1)

        kpis[key] = {
            "label": AGREGAT_LABELS[key],
            "series": series,
            "annees": annees,
            "unite": "\u20ac/hab",
            "badge": "MOYEN",  # Placeholder, computed after strate averages
            "moyenne_strate": None,
            "delta_pct": delta_pct,
        }

    return kpis, population, strate


def parse_rne(text, city_codes):
    """Parse RNE CSV and extract mayors for target cities."""
    mayors = {}
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    for row in reader:
        code = row.get("Code de la commune", "")
        dep = row.get("Code du département", "")
        fonction = row.get("Libellé de la fonction", "")
        # Build full code: dep + commune code
        full_code = dep + code if len(code) <= 3 else code

        if full_code in city_codes and fonction == "Maire":
            date_raw = row.get("Date de début de la fonction", "")
            # Convert DD/MM/YYYY to YYYY-MM-DD
            parts = date_raw.split("/")
            date_iso = f"{parts[2]}-{parts[1]}-{parts[0]}" if len(parts) == 3 else date_raw

            mayors[full_code] = {
                "nom": row.get("Nom de l'élu", ""),
                "prenom": row.get("Prénom de l'élu", ""),
                "sexe": row.get("Code sexe", ""),
                "debut_mandat": date_iso,
            }
    return mayors


def parse_elections(text, city_codes):
    """Parse election results CSV (tab or semicolon separated, latin1, wide format)."""
    results = {}
    lines = text.strip().split("\n")
    if not lines:
        return results

    # Auto-detect separator: tab or semicolon
    sep = "\t" if "\t" in lines[0] else ";"
    header = lines[0].split(sep)

    for line in lines[1:]:
        fields = line.split(sep)
        if len(fields) < 18:
            continue

        dep_code = fields[0].strip().zfill(2)
        com_code_raw = fields[2].strip()
        # Build full INSEE code
        full_code = dep_code + com_code_raw.zfill(3)

        if full_code not in city_codes:
            continue

        try:
            inscrits = int(fields[4].strip()) if fields[4].strip() else 0
            votants = int(fields[7].strip()) if fields[7].strip() else 0
            exprimes = int(fields[15].strip()) if fields[15].strip() else 0
        except (ValueError, IndexError):
            continue

        # Parse participation percentage (French format with comma)
        try:
            participation_pct = float(fields[8].strip().replace(",", ".")) if fields[8].strip() else 0
        except ValueError:
            participation_pct = round(votants / inscrits * 100, 2) if inscrits > 0 else 0

        # Parse candidate lists (starting at field 18, repeating blocks)
        listes = []
        i = 18
        while i + 11 < len(fields):
            try:
                nuance = fields[i + 1].strip() if i + 1 < len(fields) else ""
                sexe = fields[i + 2].strip() if i + 2 < len(fields) else ""
                nom = fields[i + 3].strip() if i + 3 < len(fields) else ""
                prenom = fields[i + 4].strip() if i + 4 < len(fields) else ""
                liste_nom = fields[i + 5].strip() if i + 5 < len(fields) else ""
                voix_pct_str = fields[i + 11].strip().replace(",", ".") if i + 11 < len(fields) else "0"

                if not nom:
                    break

                voix_pct = float(voix_pct_str) if voix_pct_str else 0

                listes.append({
                    "tete": f"{prenom} {nom}".strip(),
                    "nuance": nuance,
                    "nom_liste": liste_nom,
                    "voix_pct": voix_pct,
                })
            except (ValueError, IndexError):
                break
            i += 12  # Each candidate block is ~12 fields wide

        # Sort by votes descending
        listes.sort(key=lambda x: x["voix_pct"], reverse=True)

        results[full_code] = {
            "inscrits": inscrits,
            "votants": votants,
            "participation_pct": participation_pct,
            "listes": listes,
        }

    return results


def main():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(PUBLIC_DIR, exist_ok=True)

    city_codes = {c["code"] for c in CITIES}
    city_map = {c["code"]: c for c in CITIES}

    # --- Step 1: Fetch finances ---
    print("=== Step 1: Fetching finances from OFGL API ===")
    all_finances = {}
    all_populations = {}
    all_strates = {}

    for city in CITIES:
        kpis, pop, strate = build_finances(city["code"])
        if kpis:
            all_finances[city["code"]] = kpis
            all_populations[city["code"]] = pop
            all_strates[city["code"]] = strate
        time.sleep(0.3)

    # --- Step 2: Fetch strate averages for scoring ---
    print("\n=== Step 2: Fetching strate averages ===")
    unique_strates = set(all_strates.values())
    strate_averages = {}
    for strate in unique_strates:
        print(f"  Fetching averages for strate {strate}...")
        strate_averages[strate] = fetch_strate_averages(strate)

    # Apply badges and strate averages
    for code, kpis in all_finances.items():
        strate = all_strates.get(code, "")
        avgs = strate_averages.get(strate, {})
        for key, kpi in kpis.items():
            avg = avgs.get(key)
            kpi["moyenne_strate"] = avg
            if avg and kpi["series"]:
                latest = kpi["series"][-1]
                kpi["badge"] = compute_badge(latest, avg, inverted=(key in INVERTED_KPIS))

    # --- Step 3: Fetch elected officials ---
    print("\n=== Step 3: Fetching elected officials (RNE) ===")
    rne_cache = os.path.join(RAW_DIR, "rne-municipaux.csv")
    if os.path.exists(rne_cache):
        print("  Using cached RNE file...")
        with open(rne_cache, "r", encoding="utf-8") as f:
            rne_text = f.read()
    else:
        print("  Downloading RNE (58MB, may take a moment)...")
        rne_text = fetch_text(RNE_URL, encoding="utf-8")
        with open(rne_cache, "w", encoding="utf-8") as f:
            f.write(rne_text)
        print("  Cached to raw/rne-municipaux.csv")

    mayors = parse_rne(rne_text, city_codes)
    print(f"  Found {len(mayors)} mayors out of {len(city_codes)} cities")

    # --- Step 4: Fetch election results ---
    print("\n=== Step 4: Fetching election results ===")

    # T1
    t1_cache = os.path.join(RAW_DIR, "elections-t1.txt")
    if os.path.exists(t1_cache):
        print("  Using cached T1 file...")
        with open(t1_cache, "r", encoding="utf-8") as f:
            t1_text = f.read()
    else:
        print("  Downloading T1 results...")
        t1_text = fetch_text(ELECTIONS_T1_URL, encoding="latin1")
        with open(t1_cache, "w", encoding="utf-8") as f:
            f.write(t1_text)

    # T2
    t2_cache = os.path.join(RAW_DIR, "elections-t2.txt")
    if os.path.exists(t2_cache):
        print("  Using cached T2 file...")
        with open(t2_cache, "r", encoding="utf-8") as f:
            t2_text = f.read()
    else:
        print("  Downloading T2 results...")
        t2_text = fetch_text(ELECTIONS_T2_URL, encoding="latin1")
        with open(t2_cache, "w", encoding="utf-8") as f:
            f.write(t2_text)

    elections_t1 = parse_elections(t1_text, city_codes)
    elections_t2 = parse_elections(t2_text, city_codes)
    print(f"  T1: {len(elections_t1)} cities, T2: {len(elections_t2)} cities")

    # --- Step 5: Assemble JSON per city ---
    print("\n=== Step 5: Assembling JSON files ===")
    index = []

    for city in CITIES:
        code = city["code"]
        kpis = all_finances.get(code, {})
        strate = all_strates.get(code, "")
        avgs = strate_averages.get(strate, {})
        pop = all_populations.get(code, 0)

        # Compute global score
        global_score = compute_global_score(kpis, avgs)
        global_badge = score_to_badge(global_score)

        commune_data = {
            "code": code,
            "nom": city["nom"],
            "departement": city["dep_nom"],
            "dep_code": city["dep_code"],
            "population": pop,
            "strate": strate,
            "maire": mayors.get(code),
            "finances": {
                "score_global": global_score,
                "badge_global": global_badge,
                "kpis": kpis,
            },
            "elections": {
                "t1": elections_t1.get(code),
                "t2": elections_t2.get(code),
            },
        }

        # Write JSON
        filepath = os.path.join(DATA_DIR, f"{code}.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(commune_data, f, ensure_ascii=False, indent=2)
        print(f"  Generated {code}.json ({city['nom']})")

        # Add to index
        index.append({
            "code": code,
            "nom": city["nom"],
            "dep_code": city["dep_code"],
            "departement": city["dep_nom"],
            "population": pop,
        })

    # Write index
    index_path = os.path.join(PUBLIC_DIR, "communes-index.json")
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"\n  Index written to {index_path}")

    print("\n=== Done! ===")


if __name__ == "__main__":
    main()
