import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import os
import csv
from datetime import timedelta
import time

# --- Ścieżka względna do pliku CSV ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # katalog skryptu
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "public", "csv")
os.makedirs(OUTPUT_DIR, exist_ok=True)
ID_CSV_PATH = os.path.join(OUTPUT_DIR, "id.csv")

# --- Zakres ID ---
start_id = 1
end_id = 999999
base_url = "https://www.biedronka.pl/pl/product,id,{}"
results = []  # lista krotek: (url, status_code)

# --- Thread-safe sesja ---
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504, 403],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
        })
        thread_local.session = session
    return thread_local.session

# --- Sprawdzanie GET z retry ręcznym dla 403 ---
def check_product_url(product_id):
    url = base_url.format(product_id)
    session = get_session()
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = session.get(url, allow_redirects=True, timeout=5, stream=True)
            if response.status_code == 200:
                print(f"Found 200: {url}")
                return url, 200
            elif response.status_code == 403:
                print(f"Forbidden (403), retry {attempt+1}: {url}")
                time.sleep(1)
            else:
                print(f"Not found ({response.status_code}): {url}")
                return url, response.status_code
        except Exception as e:
            print(f"Error with {url}: {e}")
            time.sleep(1)
    return url, None

# --- Główne wykonanie z partiami ---
MAX_THREADS = 10
BATCH_SIZE = 10000

print("Sprawdzanie stron...")

start_time = time.time()
checked_count = 0
total_to_check = end_id - start_id + 1

for batch_start in range(start_id, end_id + 1, BATCH_SIZE):
    batch_end = min(batch_start + BATCH_SIZE - 1, end_id)
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(check_product_url, pid) for pid in range(batch_start, batch_end + 1)]
        for future in as_completed(futures):
            url, status = future.result()
            checked_count += 1
            results.append((url, status))

            elapsed = time.time() - start_time
            avg_time_per_check = elapsed / checked_count
            remaining = total_to_check - checked_count
            eta_seconds = avg_time_per_check * remaining
            eta_str = str(timedelta(seconds=int(eta_seconds)))

            print(f"Checked: {checked_count}/{total_to_check} | Found 200: {sum(1 for _, s in results if s==200)} | ETA: {eta_str}", end='\r')

print()  # nowa linia po zakończeniu
print(f"\nZnaleziono {sum(1 for _, s in results if s==200)} stron 200.")

# --- Zapis ID 200 w pliku id.csv ---
with open(ID_CSV_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    for url, status in results:
        if status == 200:
            product_id = url.split(",")[-1] if url else ""
            writer.writerow([product_id])

print(f"Plik ID zapisany w: {ID_CSV_PATH}")
