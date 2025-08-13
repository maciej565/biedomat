import asyncio
import httpx
from bs4 import BeautifulSoup
import csv
from asyncio import Semaphore
import re
from tqdm.asyncio import tqdm_asyncio

# --- Ścieżki plików ---
input_file = "../public/csv/urls_200.csv"
output_file = "../public/csv/numbers_dates.csv"

# --- Wczytanie URL-i z pliku CSV ---
urls = []
with open(input_file, newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    for row in reader:
        if row:
            urls.append(row[0].strip('"'))

# --- Nagłówki HTTP ---
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/116.0.0.0 Safari/537.36"
}

# --- Limit równoczesnych połączeń ---
semaphore = Semaphore(20)

# --- Funkcja pobierająca liczby i daty z jednej strony ---
async def fetch_numbers_dates(client, url):
    async with semaphore:
        try:
            response = await client.get(url, timeout=5)
            if response.status_code != 200:
                return []
            soup = BeautifulSoup(response.text, "html.parser")
            title = soup.title.string.strip() if soup.title else "Brak tytułu"
            results = []

            # Wzorce dat
            date_patterns = [
                r"\b\d{4}-\d{2}-\d{2}\b",       # YYYY-MM-DD
                r"\b\d{2}/\d{2}/\d{4}\b",       # DD/MM/YYYY
                r"\b\d{2}\.\d{2}\.\d{4}\b",     # DD.MM.YYYY
            ]

            for tag in soup.find_all(True):
                if tag.string:
                    text = tag.string.strip()
                    # Liczby z JS
                    if tag.name == "script":
                        pattern = r'(?:var\s+|let\s+|const\s+)?([\w$]+)\s*[:=]\s*(\d+(?:[\.,]\d+)?)'
                        matches = re.findall(pattern, text)
                        for var_name, number in matches:
                            results.append({
                                "URL": url,
                                "Title": title,
                                "Tag": tag.name,
                                "Type": "Number",
                                "Value": number.replace(",", "."),
                                "Variable": var_name
                            })
                    # Liczby z HTML
                    numbers = re.findall(r"\d+(?:[\.,]\d+)?", text)
                    for n in numbers:
                        tag_info = tag.name
                        if tag.has_attr("class"):
                            tag_info += "." + ".".join(tag["class"])
                        results.append({
                            "URL": url,
                            "Title": title,
                            "Tag": tag_info,
                            "Type": "Number",
                            "Value": n.replace(",", "."),
                            "Variable": ""
                        })
                    # Daty
                    for dp in date_patterns:
                        dates = re.findall(dp, text)
                        for d in dates:
                            tag_info = tag.name
                            if tag.has_attr("class"):
                                tag_info += "." + ".".join(tag["class"])
                            results.append({
                                "URL": url,
                                "Title": title,
                                "Tag": tag_info,
                                "Type": "Date",
                                "Value": d,
                                "Variable": ""
                            })
            return results
        except Exception as e:
            print(f"Błąd przy pobieraniu {url}: {e}")
            return []

# --- Główna funkcja ---
async def main():
    all_results = []
    async with httpx.AsyncClient(headers=headers, timeout=5.0) as client:
        tasks = [fetch_numbers_dates(client, url) for url in urls]
        for task in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Pobieranie liczb i dat"):
            all_results.extend(await task)

    # --- Zapis do CSV ---
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["URL", "Title", "Tag", "Type", "Value", "Variable"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_results:
            writer.writerow(row)

    print(f"\nZapisano {len(all_results)} rekordów do {output_file}")

# --- Uruchomienie ---
if __name__ == "__main__":
    asyncio.run(main())
