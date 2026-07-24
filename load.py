# import os
# import json
# import psycopg2
# from psycopg2.extras import Json
# from dotenv import load_dotenv
#
# load_dotenv()
#
# conn = psycopg2.connect(os.getenv("POSTGRES_URI"))
# cur = conn.cursor()
#
# cur.execute("TRUNCATE TABLE raw_data")
#
# with open("extracted_data.json", "r") as f:
#     extracted = json.load(f)
#
# for collection_name, documents in extracted.items():
#     for doc in documents:
#         source_id = doc["_id"]["$oid"] if isinstance(doc.get("_id"), dict) else str(doc.get("_id"))
#         cur.execute(
#             "INSERT INTO raw_data (source_collection, source_id, payload) VALUES (%s, %s, %s)",
#             (collection_name, source_id, Json(doc))
#         )
#
# conn.commit()
# cur.close()
# conn.close()
#
# print("Loaded all documents into raw_data table")



import os
import json
import time
import psycopg2
from psycopg2.extras import Json, execute_values
from dotenv import load_dotenv

load_dotenv()

BATCH_SIZE = 1000

start_time = time.time()

conn = psycopg2.connect(os.getenv("POSTGRES_URI"))
cur = conn.cursor()

print("Clearing raw_data table...")
cur.execute("TRUNCATE TABLE raw_data")
conn.commit()

print("Loading extracted_data.json...")
with open("extracted_data.json", "r") as f:
    extracted = json.load(f)

rows = []
total_rows = 0

print("Starting batch insert...\n")

for collection_name, documents in extracted.items():
    for doc in documents:
        source_id = (
            doc["_id"]["$oid"]
            if isinstance(doc.get("_id"), dict)
            else str(doc.get("_id"))
        )

        rows.append(
            (
                collection_name,
                source_id,
                Json(doc)
            )
        )

        if len(rows) >= BATCH_SIZE:
            execute_values(
                cur,
                """
                INSERT INTO raw_data
                (source_collection, source_id, payload)
                VALUES %s
                """,
                rows
            )

            conn.commit()

            total_rows += len(rows)
            print(f"Inserted {total_rows} rows...")

            rows.clear()

# Insert any remaining rows
if rows:
    execute_values(
        cur,
        """
        INSERT INTO raw_data
        (source_collection, source_id, payload)
        VALUES %s
        """,
        rows
    )

    conn.commit()

    total_rows += len(rows)
    print(f"Inserted {total_rows} rows...")

cur.close()
conn.close()

elapsed = time.time() - start_time

print("\n--------------------------------")
print(f"Successfully loaded {total_rows} rows.")
print(f"Completed in {elapsed:.2f} seconds.")
print("--------------------------------")