# Week 2 — concepts reference

## Airflow components

Airflow is not one single service. It's made up of several components that work together:

**Webserver** — a web UI you open in your browser to see your DAGs, check if they ran successfully, view logs, and manually trigger runs. This is what makes Airflow visible and debuggable.

**Scheduler** — the actual brain. Watches the clock, checks which DAGs are due to run, and triggers tasks in the right order. Without this nothing ever runs automatically.

**Worker** — executes the actual tasks the scheduler triggers. When the scheduler says "run this task now," the worker is the one that actually runs it.

**Metadata database (PostgreSQL)** — Airflow stores information about DAG runs, task states, logs, and history here. Required for Airflow to function.

**What Airflow actually does vs what it doesn't:**
- Does: watches the clock, triggers tasks in order, checks success/failure, retries, alerts
- Does NOT: touch the data, clean anything, move files, process events
- Airflow is the project manager. Spark is the engineer who does the actual work.

## Consumer groups and offsets

When Airflow pulls events from Kafka every hour, it needs to remember where it left off. If it read up to message 500 last hour, this hour it should start from message 501, not from the beginning.

**Offset** — the position number of a message within a partition. Message 0 came first, message 1 came second, and so on. Kafka assigns these automatically. The offset is how Kafka tracks "how far has this consumer read."

**Consumer group** — a named group of consumers that collectively remember their position in a topic. Every time Airflow reads from Kafka, it does so as part of a consumer group (we'll call ours `airflow-consumer-group`). Kafka remembers the last offset that group successfully read, so the next run automatically picks up where the last one left off.

Why a group and not just one consumer? Because in production you might have multiple Airflow workers reading from the same topic in parallel for speed. The consumer group coordinates between them so no two workers read the same message twice.

## LocalStack

LocalStack is a tool that runs on your machine and simulates AWS services (S3, Redshift, etc.) locally for free. Instead of paying for a real S3 bucket during development, LocalStack gives you a fake one that behaves identically. The same code works against LocalStack locally and against real AWS in production — you just swap an environment variable pointing to the endpoint.

## The full Week 2 data flow

```
Kafka (holds raw events)
    ↓  [Airflow triggers this on a schedule]
S3 raw zone (raw Parquet files land here)
    ↓  [Airflow triggers Spark after raw files land]
Spark (cleans, deduplicates, joins events)
    ↓
S3 curated zone (clean, analytics-ready Parquet files)
    ↓  [Airflow triggers dbt after Spark finishes]
Redshift (star schema warehouse, queryable)
```

Airflow orchestrates every arrow in that diagram. It never touches the data itself.
