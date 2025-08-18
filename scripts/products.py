import asyncio
import httpx
from bs4 import BeautifulSoup
from pathlib import Path
import json
import re
from asyncio import Semaphore
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime

# --- Ścieżki ---
BASE_DIR = Path(__file__).parent
input_file = BASE_DIR / "../public/csv/id.csv"
output_file = BASE_DIR / "../public/json/products.json"

# --- Konfiguracja HTTP ---
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/116.0.0.0 Safari/537.36"
}
semaphore = Semaphore(20)  # maksymalnie 20 zapytań równocześnie
base_url = "https://www.biedronka.pl/pl/product,id,"

# --- Wczytanie listy ID ---
with open(input_file, newline="", encoding="utf-8") as f:
    ids = [line.strip().strip('"') for line in f if line.strip()]

# --- Funkcja pomocnicza do czystego tekstu ---
def get_text_or_empty(soup, selector):
    el = soup.select_one(selector)
    return el.get_text(strip=True) if el else ""

# --- Rozdzielenie dat z pola dostępności ---
def split_availability_dates(text):
    match = re.search(r"Oferta od\s*(\d{2}\.\d{2})\s*do\s*(\d{2}\.\d{2})", text)
    if match:
        return match.group(1), match.group(2)
    return "", ""

# --- Parsowanie ceny i wariantów z opisu ---
def parse_description(description):
    result = {
        "Ceny": {
            "Cena_regularna": "",
            "Cena_promocyjna": "",
            "Jednostka": "",
            "Cena_za_jednostkę": "",
            "Procent_rabat": 0
        },
        "Warianty": {
            "Opcje": "",
            "Limit": ""
        }
    }

    match_regular = re.search(r"Cena regularna:\s*([\d,]+)\s*zł/?([^\s,)]*)", description)
    if match_regular:
        result["Ceny"]["Cena_regularna"] = match_regular.group(1).replace(",", ".")
        result["Ceny"]["Jednostka"] = match_regular.group(2)

    match_rabat = re.search(r"(\d+)% (taniej|mniej)", description)
    procent_rabat = int(match_rabat.group(1)) if match_rabat else 0
    result["Ceny"]["Procent_rabat"] = procent_rabat

    if result["Ceny"]["Cena_regularna"] and procent_rabat:
        cena_reg_float = float(result["Ceny"]["Cena_regularna"])
        cena_promocyjna = round(cena_reg_float * (1 - procent_rabat / 100), 2)
        result["Ceny"]["Cena_promocyjna"] = f"{cena_promocyjna:.2f}"

    match_jednostka = re.search(r"\(([\d,]+)\s*zł/([^\)]+)\)", description)
    if match_jednostka:
        result["Ceny"]["Cena_za_jednostkę"] = match_jednostka.group(1).replace(",", ".")
        if not result["Ceny"]["Jednostka"]:
            result["Ceny"]["Jednostka"] = match_jednostka.group(2)

    match_limit = re.search(r"Limit dzienny\s*([\d\s\w]+)", description)
    if match_limit:
        result["Warianty"]["Limit"] = match_limit.group(1)

    return result

# --- Funkcja pobierająca dane produktu ---
async def fetch_product(client, product_id):
    async with semaphore:
        url = f"{base_url}{product_id}"
        try:
            response = await client.get(url, timeout=10.0)
            if response.status_code != 200:
                return {"ID": product_id, "Error": f"Błąd HTTP {response.status_code}"}

            soup = BeautifulSoup(response.text, "html.parser")

            title = soup.title.string.strip() if soup.title else ""
            product_unavailable = get_text_or_empty(soup, "span.product-unavailable")
            pln = get_text_or_empty(soup, "span.pln")
            gr = get_text_or_empty(soup, "span.gr")
            amount_info = get_text_or_empty(soup, "span.amount")
            description = get_text_or_empty(soup, "span.product-description")
            availability_raw = get_text_or_empty(soup, "span.product-availability")
            availability_start, availability_end = split_availability_dates(availability_raw)
            ceny_warianty = parse_description(description)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            return {
                "ID": product_id,
                "Title": title,
                "Product Unavailable": product_unavailable,
                "PLN": pln,
                "GR": gr,
                "Amount Info": amount_info,
                "Description": description,
                "Availability Start": availability_start,
                "Availability End": availability_end,
                "Ceny": ceny_warianty["Ceny"],
                "Warianty": ceny_warianty["Warianty"],
                "Timestamp": timestamp
            }

        except Exception as e:
            return {"ID": product_id, "Error": repr(e)}

# --- Główna funkcja ---
async def main():
    results = []
    async with httpx.AsyncClient(headers=headers) as client:
        tasks = [fetch_product(client, pid) for pid in ids]
        for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Pobieranie produktów"):
            results.append(await coro)

    # Wczytanie istniejącego JSON
    if output_file.exists():
        with open(output_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
    else:
        existing_data = []

    # --- Aktualizacja historii cen i dat ---
    for product in results:
        product_id = product["ID"]
        if "Error" in product:
            continue  # pomijamy błędy

        existing_product = next((p for p in existing_data if p.get("ID") == product_id), None)

        if existing_product is None:
            # nowy produkt
            entry = {
                "ID": product_id,
                "Title": product["Title"],
                "Availability Start": product["Availability Start"],
                "Availability End": product["Availability End"],
                "History": []
            }
            if not product.get("Product Unavailable"):
                entry["History"].append({
                    "Timestamp": product["Timestamp"],
                    "Ceny": product["Ceny"]
                })
            existing_data.append(entry)
        else:
            # aktualizacja dat
            existing_product["Availability Start"] = product["Availability Start"]
            existing_product["Availability End"] = product["Availability End"]
            if not product.get("Product Unavailable"):
                existing_product.setdefault("History", []).append({
                    "Timestamp": product["Timestamp"],
                    "Ceny": product["Ceny"]
                })

    # Zapis JSON
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Zapisano/aktualizowano {len(results)} rekordów w {output_file}")

# --- Start ---
if __name__ == "__main__":
    asyncio.run(main())
