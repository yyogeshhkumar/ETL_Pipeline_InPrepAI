import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv("POSTGRES_URI"))
cur = conn.cursor()

print("=== Row counts ===")
cur.execute("SELECT source_collection, COUNT(*) FROM raw_data GROUP BY source_collection")
for collection, count in cur.fetchall():
    print(f"raw_data ({collection}): {count} rows")

cur.execute("SELECT COUNT(*) FROM session_summary")
session_total = cur.fetchone()[0]
print(f"session_summary: {session_total} rows")

cur.execute("SELECT COUNT(*) FROM daily_session_counts")
print(f"daily_session_counts: {cur.fetchone()[0]} rows")

print("\n=== Null checks (session_summary) ===")
columns_to_check = ["role", "experience", "topics_to_focus", "question_count", "pinned_question_count"]

for col in columns_to_check:
    cur.execute(f"SELECT COUNT(*) FROM session_summary WHERE {col} IS NULL")
    null_count = cur.fetchone()[0]
    null_pct = (null_count / session_total * 100) if session_total > 0 else 0
    print(f"{col}: {null_pct:.1f}% null ({null_count}/{session_total})")


# ---------- Day 11: schema checks ----------

print("\n=== Schema checks ===")
expected_columns = {
    "session_summary": ["id", "session_id", "role", "experience", "topics_to_focus",
                          "question_count", "pinned_question_count", "session_created_at", "created_at"],
    "raw_data": ["id", "source_collection", "source_id", "payload", "ingested_at"],
}

checks_passed = []

for table, expected_cols in expected_columns.items():
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table,)
    )
    actual_cols = [row[0] for row in cur.fetchall()]
    missing = set(expected_cols) - set(actual_cols)
    if missing:
        print(f"FAIL: {table} is missing columns: {missing}")
        checks_passed.append(False)
    else:
        print(f"PASS: {table} has all expected columns")
        checks_passed.append(True)

# ---------- Day 11: business logic checks ----------

print("\n=== Business logic checks ===")

cur.execute("SELECT session_id, question_count, pinned_question_count FROM session_summary")
rows = cur.fetchall()

bad_rows = [r for r in rows if r[2] > r[1]]
if bad_rows:
    print(f"FAIL: {len(bad_rows)} session(s) have more pinned questions than total questions")
    checks_passed.append(False)
else:
    print("PASS: pinned_question_count never exceeds question_count")
    checks_passed.append(True)

cur.execute("SELECT session_id, COUNT(*) FROM session_summary GROUP BY session_id HAVING COUNT(*) > 1")
duplicates = cur.fetchall()
if duplicates:
    print(f"FAIL: {len(duplicates)} duplicate session_id(s) found")
    checks_passed.append(False)
else:
    print("PASS: no duplicate session_ids")
    checks_passed.append(True)

# ---------- Final summary ----------

print("\n=== VALIDATION RESULT ===")
if all(checks_passed):
    print("PASS — all checks passed")
else:
    print(f"FAIL — {checks_passed.count(False)} check(s) failed")


cur.close()
conn.close()