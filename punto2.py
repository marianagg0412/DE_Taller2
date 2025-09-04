import requests
import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

# --- MongoDB connection ---
client = pymongo.MongoClient(os.getenv("MONGO_URI"))
db = client["sports_db"]

# --- API key ---
API_KEY = os.getenv("API_KEY")
headers = {"x-apisports-key": API_KEY}

# client.drop_database("sports_db")

# --- Helper function to fetch & store ---
def fetch_and_store(base_url, endpoints, sport_name):
    for endpoint, params in endpoints.items():
        print(f"Fetching {sport_name} -> {endpoint}")
        url = f"{base_url}/{endpoint}"
        response = requests.get(url, headers=headers, params=params).json()
        print(response)

        if "response" in response:
            if len(response["response"]) > 0:
                collection_name = f"{sport_name}_{endpoint}"
                db[collection_name].delete_many({})  # Clear old data
                db[collection_name].insert_many(response["response"])
                print(f"Inserted {len(response['response'])} docs into {collection_name}")
            else:
                print(f"No data for {sport_name} -> {endpoint}")
        else:
            print(f"Error fetching {sport_name} -> {endpoint}: {response}")


# --- Soccer (Premier League 2023 example) ---
soccer_base = "https://v3.football.api-sports.io"
soccer_endpoints = {
    "leagues": {},
    "teams": {"league": 39, "season": 2023},
    "players": {"league": 39, "season": 2023},
    "fixtures": {"league": 39, "season": 2023},
}
# fetch_and_store(soccer_base, soccer_endpoints, "soccer")

# --- Basketball (NBA 2023/24 example) ---
basket_base = "https://v1.basketball.api-sports.io"
basket_endpoints = {
    "leagues": {},
    "teams": {"league": 12, "season": "2023-2024"},
    "players": {"team": 1, "season": "2023-2024"},
    "games": {"league": 12, "season": "2023-2024"},
}
fetch_and_store(basket_base, basket_endpoints, "basketball")

# --- Formula 1 (2023 season example) ---
f1_base = "https://v1.formula-1.api-sports.io"
f1_endpoints = {
    "competitions": {},
    "drivers": {"season": 2023},
    "teams": {"season": 2023},
    "races": {"season": 2023},
    "rankings/drivers": {"season": 2023},
    "rankings/teams": {"season": 2023},
}
# fetch_and_store(f1_base, f1_endpoints, "f1")
