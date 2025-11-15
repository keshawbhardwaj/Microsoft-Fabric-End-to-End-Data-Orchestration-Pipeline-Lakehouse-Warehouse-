
# 📌 Microsoft Fabric – End-to-End Data Orchestration Pipeline (Lakehouse → Warehouse)

This project demonstrates a production-grade data orchestration workflow in Microsoft Fabric, using a combination of:

Data Pipelines

Invoke Pipeline Activity

Script Activity

Lakehouse SQL Endpoint

Warehouse MERGE logic for incremental loads

The objective of this project is to show how data flows from Raw Zone (Files) → Lakehouse Table → Warehouse Table, fully automated through a Main Pipeline that invokes a Child Pipeline.

📁 Architecture Overview
                ┌───────────────────────────┐
                │     Raw Data (CSV Files)  │
                │     Lakehouse / Files     │
                └──────────────┬────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Child Pipeline    │
                    │  (Load to Lakehouse)│
                    └─────────┬───────────┘
                              │
                              ▼
                ┌──────────────────────────────┐
                │      Main Pipeline           │
                │ 1. Invoke Child Pipeline     │
                │ 2. Run Script Activity       │
                │    (MERGE into Warehouse)    │
                └──────────────┬───────────────┘
                               │
                               ▼
                   ┌───────────────────────┐
                   │   SQL Warehouse       │
                   │ wh_netflix_titles     │
                   └───────────────────────┘

🔹 Dataset Used

Netflix Movies/Series Sample:

Columns include:

show_id

type

title

director

cast

country

date_added

release_year

rating

duration

listed_in

description

🚀 Pipeline Flow – High-Level
1️⃣ Child Pipeline – Load CSV → Lakehouse Table

This pipeline loads the raw CSV file from:

/Files/Netflix_Series_description.csv


Into a Lakehouse table:

LH_Raw → netflix_series_description


Using a Copy Activity.

2️⃣ Main Pipeline – Orchestrator

The main pipeline performs:

Step 1 – Invoke child pipeline
Runs the ingestion pipeline responsible for loading the Lakehouse table.

Step 2 – Script Activity (SQL)
Executes a MERGE to load incrementally into the Warehouse.

🔄 Detailed Steps (for README documentation)

Below is a complete step-by-step outline you can directly include:

📌 Step-by-Step Implementation
STEP 1 — Create Lakehouse & Upload Raw Files

Create a Lakehouse named LH_Raw

Upload CSV file under:
Files/Netflix_Series_description.csv

Use Load to Tables → Load to table → creates:
LH_Raw.dbo.netflix_series_description

STEP 2 — Create Warehouse Table (Target)
CREATE TABLE dbo.wh_netflix_titles (
    show_id BIGINT,
    type VARCHAR(50),
    title VARCHAR(500),
    director VARCHAR(500),
    cast VARCHAR(MAX),
    country VARCHAR(500),
    date_added DATE,
    release_year SMALLINT,
    rating VARCHAR(20),
    duration VARCHAR(50),
    listed_in VARCHAR(200),
    description VARCHAR(MAX),
    load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);

STEP 3 — Create Child Pipeline (Ingestion Pipeline)

Name: pl_load_lakehouse_netflix

Activities:

Copy Data Activity

Source: Lakehouse File (CSV)

Sink: Lakehouse Table (netflix_series_description)

Schema mapping done automatically

✔ This pipeline only ingests data into the Lakehouse table.

STEP 4 — Create Main Orchestration Pipeline

Name: pl_main_orchestration_netflix

Activities:

Invoke Pipeline Activity

Calls: pl_load_lakehouse_netflix

Script Activity (SQL Warehouse)

Runs MERGE to incrementally load data

STEP 5 — MERGE Logic (Incremental Load into Warehouse)

Safe and production-grade:

MERGE INTO dbo.wh_netflix_titles AS tgt
USING (
    SELECT
        TRY_CAST(show_id AS BIGINT) AS show_id,
        LEFT(type, 50) AS type,
        LEFT(title, 500) AS title,
        LEFT(director, 500) AS director,
        cast,
        LEFT(country, 500) AS country,
        TRY_CAST(date_added AS DATE) AS date_added,
        TRY_CAST(release_year AS SMALLINT) AS release_year,
        LEFT(rating, 20) AS rating,
        LEFT(duration, 50) AS duration,
        LEFT(listed_in, 200) AS listed_in,
        description
    FROM LH_Raw.dbo.netflix_series_description
    WHERE TRY_CAST(show_id AS BIGINT) IS NOT NULL
) AS src
ON tgt.show_id = src.show_id

WHEN MATCHED THEN 
    UPDATE SET
        tgt.type = src.type,
        tgt.title = src.title,
        tgt.director = src.director,
        tgt.cast = src.cast,
        tgt.country = src.country,
        tgt.date_added = src.date_added,
        tgt.release_year = src.release_year,
        tgt.rating = src.rating,
        tgt.duration = src.duration,
        tgt.listed_in = src.listed_in,
        tgt.description = src.description,
        tgt.load_timestamp = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN 
    INSERT (
        show_id, type, title, director, cast, country,
        date_added, release_year, rating, duration,
        listed_in, description, load_timestamp
    )
    VALUES (
        src.show_id, src.type, src.title, src.director, src.cast, src.country,
        src.date_added, src.release_year, src.rating, src.duration,
        src.listed_in, src.description, CURRENT_TIMESTAMP
    );

🧪 STEP 6 — Validation Query
SELECT COUNT(*) FROM dbo.wh_netflix_titles;
SELECT TOP 20 * FROM dbo.wh_netflix_titles ORDER BY load_timestamp DESC;
