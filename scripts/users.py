import requests
import json
import os

OUTPUT_FILE = "users.json"
START_ID = int(os.environ.get("START_ID", 1))
END_ID = int(os.environ.get("END_ID", 100))

# Wczytaj istniejące dane lub utwórz pustą listę
if os.path.exists(OUTPUT_FILE):
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        try:
            users = json.load(f)
        except json.JSONDecodeError:
            users = []
else:
    users = []

for user_id in range(START_ID, END_ID + 1):
    url = f"https://jbzd.com.pl/mikroblog/user/profile/{user_id}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data.get("status") == "success":
                users.append(data["user"])
                print(f"Pobrano ID {user_id}")
            else:
                print(f"Brak danych dla ID {user_id}")
        else:
            print(f"Błąd HTTP {r.status_code} dla ID {user_id}")
    except Exception as e:
        print(f"Błąd dla ID {user_id}: {e}")

# Zapisz do pliku
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(users, f, ensure_ascii=False, indent=2)

print(f"Pobrano łącznie {len(users)} użytkowników.")
