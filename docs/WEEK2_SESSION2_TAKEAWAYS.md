# Week 2 session takeaways — Airflow, S3, and pipeline debugging

## What we accomplished
The full pipeline from event generation to S3 is now working end to end:
```
schemas.py + population.py + lifecycle.py
        ↓
    generator.py
        ↓
    Kafka (8 topics, partitioned by student_id)
        ↓
  Airflow DAG: kafka_to_s3_raw
    Task 1: consume_kafka  →  reads all 8 topics, returns list of events
    Task 2: upload_to_s3   →  groups by event type, writes Parquet to S3
        ↓
  LocalStack S3: eduflow-raw bucket
    application_submitted/year=2026/month=07/day=11/events.parquet
    document_submitted/year=2026/month=07/day=11/events.parquet
    enrollment_confirmed/year=2026/month=07/day=11/events.parquet
    graduation/year=2026/month=07/day=11/events.parquet
    opt_cpt_request/year=2026/month=07/day=11/events.parquet
    status_change/year=2026/month=07/day=11/events.parquet
    term_registration/year=2026/month=07/day=11/events.parquet
    visa_status_change/year=2026/month=07/day=11/events.parquet
```

---

## Key concepts

### Raw zone vs curated zone
The S3 bucket currently holds the **raw zone** — a direct copy of what came out of Kafka, converted from JSON to Parquet but otherwise untouched. Raw zone is immutable, you write once and never modify. It's the safety net: if a downstream job makes a mistake, you can always reprocess from raw.

The **curated zone** comes after PySpark processes the raw files: deduplicated, cleaned, joined, with derived fields computed. That's what Redshift will query.

### Why one Parquet file per event type
Each event type gets its own folder and Parquet file per day. When Spark later wants to analyze only visa events, it reads only the `visa_status_change/` folder and skips the other 7 entirely. This is **partition pruning** — the folder structure itself acts as an index.

### XCom (cross-communication)
How Airflow passes data between tasks. When `consume_kafka` returns a list, Airflow serializes it and stores it in PostgreSQL. When `upload_to_s3` runs, it pulls that list back out. This happens transparently with the `@task` decorator:
```python
events = consume_kafka()   # captures the XCom reference
upload_to_s3(events)       # Airflow passes the stored value automatically
```

### Consumer group offset problem
Kafka tracks where each consumer group left off using an **offset** (a position number per partition). If a consumer group has already read all messages, subsequent runs find nothing. This is correct behavior — it prevents reprocessing the same data every run.

The bug we hit: the Airflow worker was falling back to `localhost:9092` instead of `kafka:29092` because `KAFKA_BOOTSTRAP_SERVERS` wasn't set as an environment variable in the Airflow services. The consumer group never got registered in Kafka because it was connecting to nothing.

Fix: add `KAFKA_BOOTSTRAP_SERVERS: kafka:29092` to the environment block of all three Airflow services (webserver, scheduler, worker).

### Shared log volume
Airflow's webserver fetches task logs from the worker over HTTP. This fails locally because the worker's auto-generated hostname isn't resolvable by the webserver. Fix: mount a shared `airflow_logs` volume at `/opt/airflow/logs` in all three Airflow services so they all read and write logs to the same place.

### Secret key mismatch
All Airflow components must share the same `AIRFLOW__WEBSERVER__SECRET_KEY` so they trust each other when communicating. Without it the webserver gets a 403 Forbidden when trying to fetch logs from the worker.

### Backfilling in Airflow
When a DAG is activated with a past `start_date`, Airflow immediately runs all missed scheduled intervals to catch up. A DAG starting at midnight with an hourly schedule will trigger 22 runs at once to cover all missed hours. This is called **backfilling** and is on by default.

### Backfill mode in the generator (different meaning)
`--mode backfill` in the generator means "push all events to Kafka as fast as possible with no delays," contrasting with `--mode stream` which replays events in compressed real time. Same word, completely different concept from Airflow's backfilling.

### /opt directory
Standard Linux convention for third-party software. `/opt/airflow/` is where Airflow installs itself inside the container, same way software might go in `C:\Program Files` on Windows.

### Mounting
Making external storage accessible at a specific path inside a container. Like plugging in a USB drive — the storage is external but appears as a normal folder. The container has no idea the storage is shared or persists outside it.

Two types used in this project:
- **Named volume** (`postgres_data`, `localstack_data`, `airflow_logs`) — Docker manages the storage on your Mac, persists across restarts
- **Bind mount** (`./airflow/dags:/opt/airflow/dags`) — directly links a specific folder on your Mac to a specific path inside the container, changes appear instantly

---

## CLI commands learned

### Running the generator manually
```bash
# Run generator as a one-off command, auto-delete container after
docker compose run --rm generator --mode backfill --num-students 500 --seed 99 --bootstrap-servers kafka:29092
```

### Checking S3 contents in LocalStack
```bash
# List all buckets
docker exec localstack awslocal s3 ls

# List all files in a specific bucket recursively
docker exec localstack awslocal s3 ls s3://eduflow-raw/ --recursive
```

### Checking Kafka consumer groups
```bash
# Describe a consumer group (shows offset, latest offset, lag per topic/partition)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group airflow-consumer-group \
  --describe

# Reset a consumer group offset to the beginning of all topics
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group airflow-consumer-group \
  --reset-offsets \
  --to-earliest \
  --all-topics \
  --execute
```

### Running Airflow in detached mode
```bash
docker compose up -d   # starts all services in the background, terminal stays free
```

### Reading Airflow task logs directly from the worker
```bash
docker exec airflow-worker bash -c "cat /opt/airflow/logs/dag_id=<dag>/run_id=<run_id>/task_id=<task>/attempt=1.log"
```

---

## Airflow UI explained

### DAG list columns
- **Runs** — circles per state (queued, running, success, failed). Number inside = count in that state
- **Recent Tasks** — 14 circles, each fixed to a specific task state. Position is not random
- **Schedule** — the cron expression or preset (`@hourly`, `0 * * * *`)
- **Next Run** — when Airflow will trigger the next scheduled run

### Audit log vs task log
- **Audit log** — records what Airflow did (triggered, queued, running, success). Does not show Python output
- **Task log** — the actual Python print statements and output from inside the task function. This is where you see `INFO - Done. Returned value was: [...]`

### Log viewing 403 error
If you see `403 FORBIDDEN` when viewing logs in the UI, it means the webserver and worker don't share the same secret key or the worker's hostname isn't resolvable. Fix: shared secret key + shared log volume.

---

## Debugging workflow learned

When something isn't working, the order of investigation:
1. Check `docker compose ps` — are all containers healthy?
2. Trigger a manual DAG run and check the Airflow UI for task status
3. Click on the specific task → Logs tab to see actual Python output
4. Look for the `INFO - Done. Returned value was:` line to see what a task returned
5. If logs show 403, read logs directly from the worker container with `docker exec`
6. If consume_kafka returns empty, check whether the consumer group exists in Kafka
7. If consumer group doesn't exist, the worker can't reach Kafka — check `KAFKA_BOOTSTRAP_SERVERS` environment variable

---

## boto3 and S3 concepts

### boto3.client vs boto3.resource
`boto3.client("s3")` is the low-level interface that maps directly to AWS API calls. We used `put_object`, `head_bucket`, `create_bucket`. This is the right choice when you need precise control over what's happening.

### BytesIO
An in-memory byte stream. Instead of writing a Parquet file to disk and uploading it, we write directly into memory and upload the bytes. No temporary files, no disk I/O.

### bucket vs key
- **Bucket** — the top-level container in S3, like a hard drive. Our bucket is `eduflow-raw`
- **Key** — the full path of a file inside the bucket, like a filepath. Our key is `application_submitted/year=2026/month=07/day=11/events.parquet`

### awslocal
LocalStack's built-in wrapper around the AWS CLI that automatically points at `http://localhost:4566` instead of real AWS. Same commands as real AWS CLI, no credential configuration needed.
