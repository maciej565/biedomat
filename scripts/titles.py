import asyncio
import httpx
from bs4 import BeautifulSoup
import csv
from asyncio import Semaphore
from tqdm.asyncio import tqdm_asyncio  # pasek postępu dla asyncio

# --- Ścieżki plików ---
input_file = "../public/csv/urls_200.csv"
output_file = "../public/csv/titles.csv"

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
semaphore = Semaphore(20)  # maksymalnie 20 zapytań równocześnie

# --- Funkcja pobierająca tytuł strony asynchronicznie ---
async def fetch_title(client, url):
    async with semaphore:
        try:
            response = await client.get(url, timeout=5)
            if response.status_code != 200:
                return {"URL": url, "Title": "Błąd pobrania strony"}
            soup = BeautifulSoup(response.text, "html.parser")
            title = soup.title.string.strip() if soup.title else "Brak tytułu"
            return {"URL": url, "Title": title}
        except Exception as e:
            return {"URL": url, "Title": f"Błąd: {e}"}

# --- Główna funkcja asynchroniczna ---
async def main():
    results = []
    async with httpx.AsyncClient(headers=headers, timeout=5.0) as client:
        tasks = [fetch_title(client, url) for url in urls]
        for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Pobieranie tytułów"):
            results.append(await coro)

    # --- Zapis wyników do CSV ---
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["URL", "Title"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    print(f"\nZapisano {len(results)} rekordów do {output_file}")

# --- Uruchomienie asynchroniczne ---
if __name__ == "__main__":
    asyncio.run(main())
