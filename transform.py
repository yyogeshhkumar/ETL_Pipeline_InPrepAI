import os
import psycopg2
import polars as pl
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

conn = psycopg2.connect(os.getenv("POSTGRES_URI"))
cur = conn.cursor()

cur.execute("SELECT source_id, payload FROM raw_data WHERE source_collection = 'sessions'")
session_rows = cur.fetchall()

cur.execute("SELECT source_id, payload FROM raw_data WHERE source_collection = 'questions'")
question_rows = cur.fetchall()

cur.close()
conn.close()

# the time-series piece
def parse_date(raw_date):
    if isinstance(raw_date, dict) and "$date" in raw_date:
        raw_date = raw_date["$date"]
    if isinstance(raw_date, (int, float)):
        return datetime.fromtimestamp(raw_date / 1000, tz=timezone.utc)
    if isinstance(raw_date, str):
        return datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
    return None

sessions_data = []
for source_id, payload in session_rows:
    sessions_data.append({
        "session_id": source_id,
        "role": payload.get("role"),
        "experience": payload.get("experience"),
        "topics_to_focus": payload.get("topicsToFocus"),
        "created_at": parse_date(payload.get("createdAt")),

    })
sessions_df = pl.DataFrame(sessions_data)

questions_data = []
for source_id, payload in question_rows:
    session_ref = payload.get("session")
    session_id = session_ref.get("$oid") if isinstance(session_ref, dict) else session_ref
    questions_data.append({
        "question_id": source_id,
        "session_id": session_id,
        "is_pinned": payload.get("isPinned", False),
    })
questions_df = pl.DataFrame(questions_data)

question_summary = questions_df.group_by("session_id").agg(
    pl.len().alias("question_count"),
    pl.col("is_pinned").sum().alias("pinned_question_count")
)

final = sessions_df.join(question_summary, on="session_id", how="left").fill_null(0)

print(final)

# daily session counts, rolling average, anomaly flag -> "daily_counts"
daily_counts = (
    sessions_df
    .with_columns(pl.col("created_at").dt.date().alias("day"))
    .group_by("day")
    .agg(pl.len().alias("session_count"))
    .sort("day")
)

daily_counts = daily_counts.with_columns(
    pl.col("session_count").rolling_mean(window_size=3).alias("rolling_avg_3day")
)

daily_counts = daily_counts.with_columns(
    (pl.col("session_count") > pl.col("rolling_avg_3day") * 1.5).alias("is_anomaly")
)

print(daily_counts)

# write both results back into Postgres

conn = psycopg2.connect(os.getenv("POSTGRES_URI"))
cur = conn.cursor()

cur.execute("TRUNCATE TABLE session_summary")
for row in final.iter_rows(named=True):
    cur.execute(
        """
        INSERT INTO session_summary 
        (session_id, role, experience, topics_to_focus, question_count, pinned_question_count, session_created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (row["session_id"], row["role"], row["experience"], row["topics_to_focus"],
         row["question_count"], row["pinned_question_count"], row["created_at"])
    )

cur.execute("TRUNCATE TABLE daily_session_counts")
for row in daily_counts.iter_rows(named=True):
    cur.execute(
        """
        INSERT INTO daily_session_counts (day, session_count, rolling_avg_3day, is_anomaly)
        VALUES (%s, %s, %s, %s)
        """,
        (row["day"], row["session_count"], row["rolling_avg_3day"], row["is_anomaly"])
    )

conn.commit()
cur.close()
conn.close()

print("Transformed data written to Postgres")