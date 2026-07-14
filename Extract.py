import os
import json
from pymongo import MongoClient
from bson.json_util import dumps
from dotenv import load_dotenv

load_dotenv()

import certifi

client = MongoClient(os.getenv("MONGO_URI"), tlsCAFile=certifi.where())
db = client["test"]

collections = ["users", "sessions", "questions"]
extracted = {}

for name in collections:
    docs = list(db[name].find())
    extracted[name] = json.loads(dumps(docs))
    print(f"{name}: {len(docs)} documents pulled")

with open("extracted_data.json", "w") as f:
    json.dump(extracted, f, indent=2)

print("Saved to extracted_data.json")
print(client.list_database_names())