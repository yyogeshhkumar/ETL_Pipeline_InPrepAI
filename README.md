# InPrep Analytics Pipeline

An end-to-end ETL pipeline that extracts real usage data from the InterviewPrepAI app's MongoDB database, transforms it into analytics-ready tables in PostgreSQL, validates it automatically, and orchestrates the whole workflow with Apache Airflow.

## Problem

InterviewPrepAI stores every practice session, question, and answer in MongoDB — but that data is only usable inside the app itself, one document at a time. There's no way to answer basic analytics questions from it: which roles people are practicing for most, how many questions a typical session generates, which questions get pinned as important, or whether usage is trending up or down day to day.

This pipeline builds that missing analytics layer: a repeatable, automated process that turns raw operational data into a small analytics warehouse that can actually be queried and visualized.

## Tools

- **MongoDB Atlas** — source database (existing InterviewPrepAI data: `users`, `sessions`, `questions`)
- **PostgreSQL (Supabase)** — analytics warehouse, split into a raw landing table and clean transformed output tables
- **Python** — `pymongo` (extraction), `psycopg2` (Postgres I/O), `polars` (transformation), `pandas` + `matplotlib` (analysis/visualization)
- **Apache Airflow** — orchestrates extract → load → transform → validate as one automated, repeatable DAG
- **Jupyter Notebook** — final data story with charts, built directly on the pipeline's output tables

## Approach

1. **Extract** — pull all documents from MongoDB's `users`, `sessions`, and `questions` collections, save as a local JSON snapshot.
2. **Load** — write every document into a single `raw_data` table in Postgres (JSONB payload, tagged by source collection) — an untouched copy of the source, kept separate from anything transformed.
3. **Transform** — using Polars, join session data with question counts (including pinned-question counts) into a `session_summary` table, and build a `daily_session_counts` table with a 3-day rolling average and a simple anomaly flag for unusual days.
4. **Validate** — an automated script checks row counts, null percentages, schema drift (via `information_schema`), and business-logic rules (e.g. pinned questions can never exceed total questions), producing a single PASS/FAIL result.
5. **Orchestrate** — all four steps run as one Airflow DAG, so the entire pipeline executes on a single trigger instead of four manual commands, with automatic failure containment (a failed step blocks everything downstream from running on bad data).
6. **Analyze** — a Jupyter notebook queries the final tables directly and visualizes the results: sessions by target role, question engagement per session, and daily volume trends with anomalies highlighted.

## Outcomes

- A fully automated pipeline, proven idempotent by running the full DAG twice and confirming identical row counts and validation results both times — not just "it ran once."
- Real engineering problems solved along the way, not just a tutorial followed: an SSL certificate trust issue on MongoDB Atlas, discovering the actual database name differed from its connection nickname, and fixing a non-idempotent load step that silently duplicated data on reruns.
- Three visualizations built directly from the pipeline's own output tables, requiring no manual data wrangling — proof the transformed tables are actually analysis-ready, not just "technically populated."

## Project structure

```
├── extract.py                   # MongoDB -> local JSON snapshot
├── load.py                      # JSON -> raw_data table in Postgres
├── transform.py                 # raw_data -> session_summary + daily_session_counts
├── validate.py                  # automated data quality checks
├── pipeline_data_story.ipynb    # final charts and analysis
├── dags/
│   └── inprep_etl_pipeline.py   # Airflow DAG wiring the 4 scripts together
└── .env.example                 # required environment variables (no real secrets)
```

## Running it

1. Create a `.env` file (see `.env.example`) with `MONGO_URI` and `POSTGRES_URI`
2. `pip install -r requirements.txt`
3. Run manually: `python extract.py && python load.py && python transform.py && python validate.py`
4. Or automate it: copy `dags/inprep_etl_pipeline.py` into your Airflow `dags/` folder and trigger it from the Airflow UI
