import asyncio
import httpx
from selectolax.parser import HTMLParser
from pathlib import Path
import json
import re
from asyncio import Semaphore
from datetime import datetime

# --- Ścieżki ---
BASE_DIR = Path(__file__).parent
input_file = BASE_DIR / "../public/csv/id.csv"
output_file = BASE_DIR / "../public/json/products.json"

# --- Konfiguracja HTTP ---
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/116.0.0.0 Safari/537.36"
    )
}
semaphore = Semaphore(20)
base_url = "https://www.biedronka.pl/pl/product,id,"

# --- Wczytanie listy ID ---
with open(input_file, newline="", encoding="utf-8") as f:
    ids = [line.strip().strip('"') for line in f if line.strip()]

# --- Funkcje pomocnicze ---
def split_availability_dates(text):
    match = re.search(r"Oferta od\s*(\d{2}\.\d{2})\s*do\s*(\d{2}\.\d{2})", text)
    if match:
        return match.group(1), match.group(2)
    return "", ""

def parse_description(description):
    ceny = {"C": "", "P": "", "J": "", "R": 0}
    match_regular = re.search(r"Cena regularna:\s*([\d,]+)\s*zł/?([^\s,)]*)", description)
    if match_regular:
        ceny["C"] = match_regular.group(1).replace(",", ".")
        ceny["J"] = match_regular.group(2)

    match_rabat = re.search(r"(\d+)% (taniej|mniej)", description)
    procent_rabat = int(match_rabat.group(1)) if match_rabat else 0
    ceny["R"] = procent_rabat

    if ceny["C"] and procent_rabat:
        cena_reg_float = float(ceny["C"])
        cena_promocyjna = round(cena_reg_float * (1 - procent_rabat / 100), 2)
        ceny["P"] = f"{cena_promocyjna:.2f}"
    else:
        ceny["P"] = ""

    return ceny

# --- Funkcja pobierająca dane produktu ---
async def fetch_product(client, product_id):
    async with semaphore:
        url = f"{base_url}{product_id}"
        try:
            response = await client.get(url, timeout=10.0)
            if response.status_code != 200:
                return {"ID": product_id, "Error": f"HTTP {response.status_code}"}

            soup = HTMLParser(response.text)
            title = soup.title.text(strip=True) if soup.title else ""
            unavailable_el = soup.css_first("span.product-unavailable")
            unavailable = unavailable_el.text(strip=True) if unavailable_el else ""
            description_el = soup.css_first("span.product-description")
            description = description_el.text(strip=True) if description_el else ""
            availability_el = soup.css_first("span.product-availability")
            availability_start, availability_end = split_availability_dates(
                availability_el.text(strip=True) if availability_el else ""
            )
            ceny = parse_description(description)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            return {
                "ID": product_id,
                "T": title,
                "U": unavailable,
                "A": [availability_start, availability_end],
                "Ceny": ceny,
                "Timestamp": timestamp
            }

        except Exception as e:
            return {"ID": product_id, "Error": repr(e)}

# --- Główna funkcja ---
async def main():
    # Pobieranie danych z HTTP/2
    async with httpx.AsyncClient(headers=headers, http2=True) as client:
        tasks = [fetch_product(client, pid) for pid in ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Wczytanie istniejącego JSON
    if output_file.exists():
        with open(output_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = []

    id_map = {p["ID"]: p for p in data}

    for product in results:
        if isinstance(product, Exception) or "Error" in product:
            continue
        pid = product["ID"]
        existing = id_map.get(pid)
        if not existing:
            entry = {
                "ID": pid,
                "T": product["T"],
                "A": product["A"],
                "H": []
            }
            if not product["U"]:
                ceny = product["Ceny"]
                entry["H"].append([product["Timestamp"], ceny["C"], ceny["P"], ceny["J"], ceny["R"]])
            data.append(entry)
            id_map[pid] = entry
        else:
            existing["A"] = product["A"]
            if not product["U"]:
                ceny = product["Ceny"]
                new_entry = [product["Timestamp"], ceny["C"], ceny["P"], ceny["J"], ceny["R"]]
                if not existing["H"] or existing["H"][-1] != new_entry:
                    existing["H"].append(new_entry)

    # Zapis JSON
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

    print(f"\n✅ Zapisano/aktualizowano {len(results)} rekordów w {output_file}")

# --- Start ---
if __name__ == "__main__":
    asyncio.run(main())
