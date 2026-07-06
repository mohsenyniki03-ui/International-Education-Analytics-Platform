# Global Student Pipeline — Project Blueprint

## 1. Business framing

International student offices at universities track a population that moves through a long, multi-stage lifecycle: application, document verification, visa processing, enrollment, term-by-term registration, work authorization (OPT/CPT), and eventually graduation or an early exit (withdrawal, transfer, leave of absence). Today that tracking typically lives across disconnected systems (an SIS, a visa management tool like Sunapsis, spreadsheets), and answering questions like "what's our average visa processing time by country this year" or "what's our enrollment funnel conversion rate by program" requires manual data pulls.

This project builds the data platform that would answer those questions automatically: a streaming ingestion layer that captures lifecycle events as they happen, a processing layer that cleans and models them, and a warehouse layer that serves analytics to stakeholders (international student services, academic departments, university leadership).

Because real student records are protected (FERPA) and unavailable for a portfolio project, the pipeline runs against a synthetic but behaviorally realistic population: weighted country-of-origin distributions, realistic visa denial/document rejection rates, and a proper state machine governing what events can follow what.

## 2. Goals and non-goals

**Goals**
- Build an end-to-end pipeline that is actually runnable, not just diagrammed: clone the repo, stand it up locally, see data flow through every stage.
- Use the tools the way they're used in real teams: Airflow DAGs that do real orchestration (retries, sensors, alerting on failure), Spark jobs that do real distributed transforms, Terraform that actually provisions and tears down infrastructure.
- Demonstrate engineering judgment, not just tool usage: idempotent jobs, data quality checks, a real data model (star schema), cost-aware infrastructure choices, and documentation that explains *why*, not just *how*.
- End with a deployable artifact: a real (temporary) AWS deployment, screenshots/recording of it running, and a polished README.

**Non-goals**
- Not aiming for production-grade scale (this isn't processing billions of events) — the goal is to demonstrate correct use of the tools at a scope that's buildable in weeks, not infinite scalability.
- Not building a polished custom frontend. A BI tool (Metabase) pointed at Redshift is enough to prove the data is queryable and meaningful.
- Not handling real PII or real student data at any point.

## 3. Architecture

Data flows in one direction through five layers (see the diagram above):

1. **Event generator** (Python) — simulates a population of international students and emits their lifecycle events with realistic timing and branching probabilities.
2. **Kafka** — durable, ordered ingestion. Each event category gets its own topic, partitioned by `student_id` so a single student's events stay in order.
3. **Airflow + PySpark** — Airflow DAGs periodically pull batches from Kafka into an S3 raw zone, run data quality checks, and trigger PySpark jobs that clean, deduplicate, and join events into curated, analytics-ready tables.
4. **S3 + Redshift** — S3 holds the data lake (raw and curated zones in Parquet); Redshift holds the warehouse, modeled as a star schema and built with dbt for testable, version-controlled transformations.
5. **BI layer** — Metabase (Dockerized) connects to Redshift for the dashboards that make the data legible to a non-technical stakeholder.

Underneath all of this: every compute component (event generator, Airflow, Spark, Metabase) runs in Docker containers orchestrated by Kubernetes, so the whole stack runs identically on a local Kind/Minikube cluster or on AWS EKS. Terraform provisions everything on the AWS side — VPC, S3 buckets, Redshift Serverless, IAM roles, EKS — so the same code can stand up a real cloud environment and tear it down on command. CI/CD (GitHub Actions) runs tests on every change and can apply Terraform/deploy manifests on merge.

## 4. Tech stack and why each piece is there

| Tool | Role | Why this one |
|---|---|---|
| Kafka | Event ingestion/streaming backbone | Industry standard for durable, ordered, replayable event streams; the skill most distinct from typical "batch ETL" portfolio projects |
| Apache Airflow | Orchestration | The de facto standard DAG scheduler in DE roles; lets you demonstrate retries, sensors, SLAs, and alerting, not just "run script daily" |
| PySpark | Distributed transformation | Standard for large-scale data processing; demonstrates you can reason about partitioning, joins, and shuffles, not just pandas |
| Amazon S3 | Data lake storage | Cheapest, most universal object store; the landing zone pattern (raw → curated) is standard practice |
| Amazon Redshift (Serverless) | Data warehouse | Common analytics warehouse target; Serverless avoids paying for an idle cluster |
| dbt | Warehouse transformation/modeling | The dominant modern tool for SQL-based, tested, version-controlled transformations — one of the highest-demand DE skills right now |
| Docker | Containerization | Universal packaging so every service runs identically everywhere |
| Kubernetes | Container orchestration | Lets the same manifests run locally (Kind/Minikube) or on EKS; demonstrates real deployment skills, not just `docker run` |
| Terraform | Infrastructure as code | Reproducible, reviewable infrastructure instead of manual console clicks; one of the most commonly screened-for DE/DevOps skills |
| GitHub Actions | CI/CD | Free, ubiquitous, low-friction; runs tests and can gate Terraform applies |
| pytest + dbt tests | Data quality / testing | Validates both the Python event logic and the warehouse models |
| Metabase | BI/visualization | Fast way to prove the warehouse data is actually useful, with zero custom frontend code |

## 5. Data model

**Event schema** (Kafka payloads): a common envelope (`event_id`, `event_type`, `student_id`, `event_timestamp`, `produced_at`) wrapping a type-specific payload. Event types: `application_submitted`, `document_submitted`, `visa_status_change`, `enrollment_confirmed`, `term_registration`, `opt_cpt_request`, `status_change`, `graduation`.

**Warehouse schema** (Redshift, star schema):
- `fact_student_events` — one row per event, foreign keys into the dimensions below, plus derived metrics computed in dbt (days-to-visa-decision, days-to-document-acceptance, etc.)
- `dim_student` — student_id, degree level, funding source, visa type (slowly changing as status changes occur)
- `dim_country` — country of origin, region
- `dim_program` — program, school
- `dim_term` — term name, academic year, start/end dates

This is the same star-schema pattern used in real warehouse design, and it's worth being able to explain in an interview: why a fact table, why conformed dimensions, why this grain.

## 6. Repository layout

```
global-student-pipeline/
├── data-generator/      # synthetic population + lifecycle event simulation + Kafka producer
├── airflow/dags/        # orchestration DAGs
├── spark/jobs/          # PySpark transformation jobs
├── dbt/                 # warehouse models and tests
├── terraform/           # AWS infrastructure (modules + environments)
├── k8s/                 # Kubernetes manifests / Helm charts
├── .github/workflows/   # CI/CD pipelines
├── tests/               # pytest suite
├── docs/                # architecture docs, this blueprint, runbooks
└── docker-compose.yml   # local dev environment
```

## 7. Milestones

**Week 1 — Foundations (data model + streaming)**
- Finalize event schema and student lifecycle state machine (`schemas.py`, `population.py`, `lifecycle.py`)
- Kafka producer streaming events into topics, runnable via `docker-compose up`
- pytest suite validating event structure and lifecycle ordering
- Repo scaffolding, README v1, GitHub Actions running tests on push
- *Learning focus: Kafka fundamentals (topics, partitions, keys, consumer groups)*

**Week 2 — Orchestration and processing**
- Airflow DAGs: consume Kafka batches into S3 raw zone, run data quality checks, trigger Spark jobs
- PySpark jobs: dedupe, clean, join into curated Parquet tables in S3
- Local dev fully working end-to-end (generator → Kafka → Airflow → Spark → S3)
- *Learning focus: Airflow DAG design (sensors, retries, task dependencies), Spark transformations and partitioning*

**Week 3 — Warehouse, IaC, and containers**
- dbt models building the star schema on Redshift (local Postgres standing in for Redshift during dev)
- Terraform modules: VPC, S3, Redshift Serverless, IAM, ECR, EKS
- Docker images for every service; Kubernetes manifests running the full stack on Kind/Minikube
- *Learning focus: Terraform module design and state management, dbt modeling and testing, Kubernetes deployments/services*

**Week 4 — Real deployment, CI/CD, and polish**
- Real `terraform apply` against AWS; run the full pipeline against actual S3/Redshift
- GitHub Actions pipeline: build/test images, push to ECR, gated Terraform apply
- Metabase dashboard on top of Redshift; capture screenshots/demo recording
- `terraform destroy`; final README, architecture docs, and a written postmortem of design decisions
- *Learning focus: real cloud deployment, CI/CD gating, cost teardown discipline*

## 8. Definition of done

- `docker-compose up` brings up a fully working local pipeline with no manual steps
- A documented, repeatable process for a real AWS deployment and teardown
- A README that explains design decisions (why a star schema, why Kafka topics are partitioned this way, why dbt instead of raw SQL scripts) — interviewers read READMEs, not just code
- At least one screenshot or short recording of the pipeline actually running against real AWS
- A short "what I'd do differently at scale" section — this is the kind of self-aware engineering judgment interviewers specifically listen for

## 9. Things a real team would worry about (and so will we)

- **Idempotency**: every Spark/Airflow run must be safely re-runnable without duplicating data
- **Schema evolution**: what happens when a new event field is added — addressed via versioned event schemas
- **Cost discipline**: Redshift Serverless and on-demand EKS rather than always-on clusters; explicit teardown step
- **Security**: least-privilege IAM roles per Terraform module, no hardcoded credentials anywhere in the repo
- **Observability**: Airflow's own monitoring plus CloudWatch alarms on pipeline failures
