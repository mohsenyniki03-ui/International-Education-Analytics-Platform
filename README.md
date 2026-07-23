# EduFlow — International Education Analytics Platform

> A production-grade data engineering portfolio project demonstrating end-to-end pipeline design: from synthetic event generation through streaming ingestion, distributed transformation, dimensional modeling, and a live analytics dashboard.

![Dashboard Preview](docs/dashboard-preview.png)

---

## Overview

EduFlow simulates a real university's international student data pipeline. It generates synthetic student lifecycle events (application → visa → enrollment → graduation), streams them through Kafka, orchestrates processing with Airflow, transforms them with PySpark, models them into a star schema with dbt, and serves insights through a FastAPI-powered analytics dashboard.

The project is designed to demonstrate the skills and architectural thinking expected of a mid-level data engineer: pipeline reliability, separation of concerns, idempotency, incremental processing, and infrastructure reproducibility.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         EduFlow Data Pipeline                           │
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌─────────────────────────────────┐  │
│  │Generator │───▶│  Kafka   │───▶│         Airflow (DAG 1)         │  │
│  │(lifecycle│    │ 8 topics │    │  consume_kafka → upload_to_s3   │  │
│  │ FSM)     │    │          │    └──────────────┬──────────────────┘  │
│  └──────────┘    └──────────┘                   │                      │
│                                                  ▼                      │
│                                        ┌─────────────────┐             │
│                                        │  S3 Raw Zone    │             │
│                                        │  (Parquet)      │             │
│                                        └────────┬────────┘             │
│                                                 │                       │
│                                                 ▼                       │
│                                   ┌─────────────────────────┐          │
│                                   │     Airflow (DAG 2)     │          │
│                                   │  SparkSubmitOperator    │          │
│                                   └────────────┬────────────┘          │
│                                                │                        │
│                                                ▼                        │
│                              ┌────────────────────────────┐            │
│                              │  PySpark Transformation    │            │
│                              │  deduplicate · unpack ·    │            │
│                              │  cast timestamps           │            │
│                              └───────────────┬────────────┘            │
│                                              │                          │
│                                              ▼                          │
│                                    ┌──────────────────┐                │
│                                    │  S3 Curated Zone │                │
│                                    │  (Parquet)       │                │
│                                    └────────┬─────────┘                │
│                                             │                           │
│                                             ▼                           │
│                              ┌──────────────────────────┐              │
│                              │     Airflow (DAG 3)      │              │
│                              │  load staging → dbt run  │              │
│                              └─────────────┬────────────┘              │
│                                            │                            │
│                                            ▼                            │
│                              ┌──────────────────────────┐              │
│                              │  PostgreSQL Warehouse    │              │
│                              │  Star Schema (dbt)       │              │
│                              │  dim_students            │              │
│                              │  dim_programs            │              │
│                              │  dim_time                │              │
│                              │  fact_student_events     │              │
│                              └─────────────┬────────────┘              │
│                                            │                            │
│                                            ▼                            │
│                              ┌──────────────────────────┐              │
│                              │  FastAPI + Dashboard     │              │
│                              │  localhost:8000          │              │
│                              └──────────────────────────┘              │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Tool | Role | Why |
|---|---|---|
| **Python** | Event generation, Airflow tasks, API | Primary language throughout |
| **Apache Kafka** | Streaming event ingestion | Decouples producers from consumers, guarantees ordering per student |
| **Apache Airflow** | Pipeline orchestration (CeleryExecutor) | Schedules, monitors, and chains the three pipeline stages |
| **Apache Spark (PySpark)** | Distributed transformation | Deduplication, payload unpacking, timestamp casting at scale |
| **LocalStack** | Local AWS S3 simulation | Zero-cost local development with production-identical S3 API |
| **dbt** | Dimensional modeling | SQL-first star schema with dependency tracking and data tests |
| **PostgreSQL** | Metadata DB (Airflow) + Warehouse | Airflow state + analytics warehouse in local dev |
| **Redis** | Celery broker | Task queue between Airflow scheduler and worker |
| **FastAPI** | Analytics API | Async, auto-documented, queries PostgreSQL and serves JSON |
| **Docker Compose** | Local orchestration | 12-service stack with health checks and dependency ordering |

---

## Key Engineering Decisions

**Kafka partition key = `student_id`**
All events for a given student land in the same partition, guaranteeing chronological ordering per student. This enables correct FSM simulation and downstream lifecycle analysis without sorting.

**Idempotent Kafka producer**
`enable.idempotence: True` prevents duplicate messages on retry, ensuring exactly-once delivery semantics without additional deduplication overhead at the producer level.

**Raw zone immutability**
The S3 raw zone is write-once. Spark reads from raw and writes clean data to a separate curated zone. If a transformation job has a bug, raw data is untouched and the entire curated zone can be rebuilt from scratch.

**MapType casting instead of `from_json`**
Pandas writes Python dicts to Parquet as structs, not JSON strings. Spark reads them back as `Row` objects. Casting to `MapType(StringType(), StringType())` then unpacking with `payload.*` handles this correctly without fragile JSON parsing.

**Consumer group offsets for incremental processing**
Airflow's Kafka consumer uses `airflow-consumer-group` to track offsets, enabling true incremental ingestion. Each hourly DAG run picks up only new events since the last run.

**dbt sources for externally-managed staging tables**
The staging tables are loaded by a Python script (not dbt), so they're registered as `sources` in dbt's `sources.yml`. This correctly separates dbt's dependency graph from externally-managed data.

---

## Pipeline Stages

### Stage 1: Event Generation
`data-generator/lifecycle.py` simulates a student's journey through a finite state machine (FSM) with weighted transitions and configurable early-exit probabilities. Runs in `stream` mode with `--speed 604800` (1 simulated week per real second) for realistic continuous data flow.

### Stage 2: Kafka Ingestion (DAG 1 — `kafka_to_s3_raw`)
Airflow polls Kafka hourly using a consumer group. Events are grouped by `event_type`, converted from JSON to Parquet using pandas + pyarrow, and written to S3 with date partitioning: `s3://eduflow-raw/{event_type}/year=/month=/day=/events.parquet`.

### Stage 3: Spark Transformation (DAG 2 — `s3_raw_to_s3_curated`)
PySpark reads raw Parquet files, deduplicates by `event_id`, unpacks the nested payload struct into flat columns, and casts `event_timestamp` from string to `TimestampType`. Output lands in `s3://eduflow-curated/` with the same partition structure.

### Stage 4: Warehouse Load (DAG 3 — `s3_curated_to_warehouse`)
Curated Parquet files are loaded into PostgreSQL staging tables via pandas + SQLAlchemy. dbt then builds the star schema:
- `dim_students` — one row per student with full biographical profile
- `dim_programs` — one row per unique program/school combination
- `dim_time` — one row per date with academic term, quarter, day-of-week
- `fact_student_events` — all 8 event types unioned into one fact table

### Stage 5: Dashboard
FastAPI serves 10 analytics endpoints. The HTML/JS frontend renders the data as charts and tables using Chart.js.

---

## Project Structure

```
global-student-pipeline/
├── data-generator/          # synthetic event generation
│   ├── schemas.py           # StudentEvent dataclass, EventType enum
│   ├── population.py        # Student dataclass, weighted country distributions
│   ├── lifecycle.py         # FSM simulator with early-exit paths
│   ├── producer.py          # Kafka producer (idempotent, keyed by student_id)
│   ├── generator.py         # CLI entrypoint (dry-run / backfill / stream modes)
│   └── Dockerfile
├── airflow/
│   ├── dags/
│   │   ├── kafka_to_s3_raw.py         # DAG 1: Kafka → S3 raw
│   │   ├── s3_raw_to_s3_curated.py    # DAG 2: Spark transformation
│   │   └── s3_curated_to_warehouse.py # DAG 3: staging load + dbt
│   └── Dockerfile                     # custom image with Java + Spark + dbt
├── spark/
│   └── jobs/
│       ├── transform_raw_to_curated.py # PySpark transformation job
│       └── submit_transform.sh         # spark-submit wrapper
├── eduflow_dbt/
│   ├── models/
│   │   ├── staging/sources.yml         # external staging table definitions
│   │   └── warehouse/
│   │       ├── dim_students.sql
│   │       ├── dim_programs.sql
│   │       ├── dim_time.sql
│   │       └── fact_student_events.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── api/
│   └── main.py              # FastAPI analytics API (10 endpoints)
├── dashboard/
│   └── index.html           # single-page analytics dashboard (Chart.js)
├── scripts/
│   ├── bootstrap.sh         # full pipeline setup in one command
│   └── load_curated_to_postgres.py
├── tests/
│   └── test_generator.py    # 21 pytest tests (CI via GitHub Actions)
├── docs/
│   └── CONCEPTS.md
├── docker-compose.yml       # 12-service local stack
└── .github/
    └── workflows/
        └── ci.yml           # pytest on every push
```

---

## Local Setup

### Prerequisites
- Docker Desktop (8GB RAM allocated)
- Python 3.11+
- Git

### 1. Clone and start the stack

```bash
git clone https://github.com/yourusername/global-student-pipeline.git
cd global-student-pipeline

docker compose up -d
```

### 2. Run the bootstrap script

```bash
./scripts/bootstrap.sh
```

This single command:
- Waits for all 12 services to be healthy
- Triggers DAG 1 (Kafka → S3 raw)
- Triggers DAG 2 (Spark transformation)
- Triggers DAG 3 (staging load + dbt)
- Verifies data landed at each stage

### 3. Start the API and open the dashboard

```bash
pip install fastapi uvicorn psycopg2-binary
uvicorn api.main:app --reload --port 8000
```

Open `http://localhost:8000` in your browser.

### Service URLs

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 (admin/admin) |
| Spark Master UI | http://localhost:8081 |
| Analytics Dashboard | http://localhost:8000 |
| LocalStack | http://localhost:4566 |

---

## Data Model

```
dim_students          dim_programs          dim_time
─────────────         ─────────────         ─────────────
student_id (PK)       program_id (PK)       time_id (PK)
full_name             program_name          full_date
date_of_birth         school_name           year / month / day
gender                                      quarter
country_of_origin                           academic_term
degree_level                                day_of_week
funding_source                              is_weekend
visa_type
program
school
         │                    │                    │
         └────────────────────┼────────────────────┘
                              │
                    fact_student_events
                    ─────────────────────
                    event_id (PK)
                    student_id (FK)
                    program_id (FK)
                    time_id (FK)
                    event_type
                    event_timestamp
                    degree_level
                    funding_source
                    term
```

---

## Testing

```bash
cd data-generator
pytest ../tests/test_generator.py -v
```

21 tests covering:
- Event completeness (all students produce all expected events)
- Country distribution proportionality
- Event ordering guarantees (application always before enrollment)
- FSM branching (early-exit paths produce correct terminal events)

CI runs automatically on every push via GitHub Actions.

---

## What This Demonstrates

- **Streaming ingestion** — Kafka with consumer groups, offset management, and idempotent producers
- **Pipeline orchestration** — Airflow CeleryExecutor with cross-DAG triggering and health-checked dependencies
- **Distributed processing** — PySpark on a standalone cluster with S3A connector and Hadoop configuration
- **Dimensional modeling** — dbt star schema with source declarations, materialization config, and SQL models
- **Infrastructure as code** — 12-service Docker Compose stack with named volumes, health checks, and dependency ordering
- **API design** — FastAPI with CORS, connection pooling, and auto-generated OpenAPI docs
- **Data quality** — deduplication by event_id, null filtering, idempotent writes, pytest coverage

---

## Author

**Nikbakht Mohseny** — CS graduate, Indiana University (May 2026)
AWS Certified Data Engineer · [LinkedIn](www.linkedin.com/in/nikbakht-mohseny-97b2a8295) · [GitHub](https://github.com/mohsenyniki)
