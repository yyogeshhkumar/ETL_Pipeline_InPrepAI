import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()
conn = psycopg2.connect(os.getenv("POSTGRES_URI"))
cur = conn.cursor()

cur.execute("SELECT * FROM raw_data ORDER BY id DESC;")
rows = cur.fetchall()
print(rows)

cur.close()
conn.close()