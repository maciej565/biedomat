import asyncio
import httpx
from bs4 import BeautifulSoup
from pathlib import Path
import json
import re
from asyncio import Semaphore
from tqdm.asyncio import tqdm_asyncio

# --- Ścieżki ---

input_file = "test.csv"
output_file = Path("products.json")

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

# --- Wydzielenie cen, procentu rabatu, jednostki i limitu ---
def parse_prices_from_description(description):
    # Cena regularna
    cena_reg_match = re.search(r'Cena regularna:\s*([\d,]+)\s*zł', description)
    cena_regularna = float(cena_reg_match.group(1).replace(",", ".")) if cena_reg_match else 0.0

    # Procent rabatu
    procent_rabat_match = re.search(r'(\d+)%\s*(taniej|mniej)', description)
    procent_rabat = int(procent_rabat_match.group(1)) if procent_rabat_match else 0

    # Jednostka (np. /kg, /opak.)
    jednostka_match = re.search(r'zł(/[\w]+)', description)
    jednostka = jednostka_match.group(1) if jednostka_match else ""

    # Obliczenie ceny promocyjnej
    cena_promocyjna = round(cena_regularna * (1 - procent_rabat / 100), 2) if procent_rabat else 0.0

    # Warianty i limit
    wariant_match = re.search(r'Limit(?: dzienny)?[:\s]*(\d+)', description)
    limit = wariant_match.group(1) if wariant_match else ""

    return {
        "Ceny": {
            "Cena_regularna": f"{cena_regularna:.2f}" if cena_regularna else "",
            "Cena_promocyjna": f"{cena_promocyjna:.2f}" if cena_promocyjna else "",
            "Jednostka": jednostka,
            "Cena_za_jednostkę": "",  # opcjonalnie można rozbudować
            "Procent_rabat": str(procent_rabat) if procent_rabat else ""
        },
        "Warianty": {
            "Opcje": "",
            "Limit": limit
        }
    }

# --- Funkcja pobierająca dane produktu ---
async def fetch_product(client, product_id):
    async with semaphore:
        url = f"{base_url}{product_id}"
        try:
            response = await client.get(url, timeout=10.0)
            if response.status_code != 200:
                return {
                    "URL": url,
                    "Title": f"Błąd HTTP {response.status_code}",
                    "Product Unavailable": "",
                    "PLN": "",
                    "GR": "",
                    "Amount Info": "",
                    "Description": "",
                    "Availability Start": "",
                    "Availability End": "",
                    "Ceny": {},
                    "Warianty": {}
                }

            soup = BeautifulSoup(response.text, "html.parser")

            title = soup.title.string.strip() if soup.title else ""
            product_unavailable = get_text_or_empty(soup, "span.product-unavailable")
            pln = get_text_or_empty(soup, "span.pln")
            gr = get_text_or_empty(soup, "span.gr")
            amount_info = get_text_or_empty(soup, "span.amount")
            description = get_text_or_empty(soup, "span.product-description")

            availability_raw = get_text_or_empty(soup, "span.product-availability")
            availability_start, availability_end = split_availability_dates(availability_raw)

            prices_info = parse_prices_from_description(description)

            return {
                "URL": url,
                "Title": title,
                "Product Unavailable": product_unavailable,
                "PLN": pln,
                "GR": gr,
                "Amount Info": amount_info,
                "Description": description,
                "Availability Start": availability_start,
                "Availability End": availability_end,
                **prices_info
            }

        except Exception as e:
            return {
                "URL": url,
                "Title": f"Błąd: {repr(e)}",
                "Product Unavailable": "",
                "PLN": "",
                "GR": "",
                "Amount Info": "",
                "Description": "",
                "Availability Start": "",
                "Availability End": "",
                "Ceny": {},
                "Warianty": {}
            }

# --- Główna funkcja ---
async def main():
    results = []
    async with httpx.AsyncClient(headers=headers) as client:
        tasks = [fetch_product(client, pid) for pid in ids]
        for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Pobieranie produktów"):
            results.append(await coro)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Zapisano {len(results)} rekordów do {output_file}")

# --- Start ---
if __name__ == "__main__":
    asyncio.run(main())
