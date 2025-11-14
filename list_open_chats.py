import os
import requests

API_BASE = "https://api.movidesk.com/public/v1"
API_TOKEN = os.getenv("MOVIDESK_TOKEN")


def main():
    url = f"{API_BASE}/tickets"
    params = {
        "token": API_TOKEN,
        "$select": "id,subject,status,baseStatus,origin,createdDate,chatWidget,chatGroup",
        "$filter": "(origin eq 5 and baseStatus eq 'New')",
        "$top": 100
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    tickets = response.json()
    for t in tickets:
        print(
            t["id"],
            t["subject"],
            t["status"],
            t["baseStatus"],
            t.get("chatWidget"),
            t.get("chatGroup"),
        )


if __name__ == "__main__":
    main()
