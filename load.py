import os
import json
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv("POSTGRES_URI"))
cur = conn.cursor()

cur.execute("TRUNCATE TABLE raw_data")

with open("extracted_data.json", "r") as f:
    extracted = json.load(f)

for collection_name, documents in extracted.items():
    for doc in documents:
        source_id = doc["_id"]["$oid"] if isinstance(doc.get("_id"), dict) else str(doc.get("_id"))
        cur.execute(
            "INSERT INTO raw_data (source_collection, source_id, payload) VALUES (%s, %s, %s)",
            (collection_name, source_id, Json(doc))
        )

conn.commit()
cur.close()
conn.close()

print("Loaded all documents into raw_data table")