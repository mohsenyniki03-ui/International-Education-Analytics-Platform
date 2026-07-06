# Concepts behind the tools

Plain-language explanation first, then where it actually lives in the code, then the technical terms, since a term sticks much better once it already has a real picture attached to it. Section 1 covers what's built so far (the data generator). Section 2 is a placeholder list for what's coming as we build Kafka ingestion, Airflow, Spark, Terraform, dbt, and Kubernetes.

## 1. Already covered (data generator)

### Tagged union

One shared "top section" that's identical for every event, and a "bottom section" that's allowed to look completely different depending on the event type, like a single intake form with a standardized header and a type-specific body.

**In the script:** `StudentEvent` in `schemas.py`. The top (`event_id`, `event_type`, `student_id`, `event_timestamp`, `produced_at`) is identical across all 8 event types. The bottom (`payload: Dict[str, Any]`) is deliberately open-ended, so a document event and a visa event can carry completely different fields without needing 8 separate classes. Without this, the 4 shared fields would have to be retyped by hand in 8 different classes, and adding one new shared field later would mean remembering to update all 8.

**Terms:** **tagged union** (also called a discriminated union), `event_type` is the **tag** that tells you what shape to expect inside the payload.

**Shows up elsewhere:** JSON API responses with a `type` field plus type-specific data, database tables with a `kind` column next to a JSON blob column.

### Partitioning and ordering guarantees

Multiple clerks split the mail for speed, but every letter for a given student always goes to the same one clerk, so that student's history stays in order, even though clerks aren't synchronized with each other and different students' mail can be processed in any order relative to each other.

**In the script:** `producer.py`, `key=event.student_id.encode("utf-8")`. Kafka hashes that key to deterministically pick which partition a message goes to; the same student_id always hashes to the same number, so all of one student's events land in the same partition every time, with zero memory or lookup table required.

**Terms:** **partition** (Kafka's "clerk"; a topic is split into a fixed number of these), **partition key** (the value used to decide which partition, here `student_id`), **hash function** (deterministic: same input always gives the same output), **offset** (the ever-increasing number stamping order within one partition), **ordering guarantee** (ordering holds within a partition, not across the whole topic), **pure function** (the hash computation has no memory of past calls; consistency comes from math, not from Kafka remembering anything).

**Shows up elsewhere:** sharded databases, partitioned distributed logs, any system where "ordering only matters for related items, not globally."

### Idempotency

If a confirmation receipt gets lost in transit, the sender doesn't know whether the original arrived, so it resends. If resending blindly produces a duplicate (two receipts, or being charged twice), that's a real bug; the fix is making the operation itself safe to repeat.

**In the script:** `producer.py`, `"enable.idempotence": True`. Kafka tags each message with a sequence number and silently discards a retry that arrives with a sequence number it's already seen, so a retried send never becomes a duplicate.

**Terms:** **idempotent operation** (doing it twice produces the same end result as doing it once), **at-least-once delivery** (the realistic guarantee most distributed systems can make; idempotency is what turns that into an effectively-once outcome), **idempotency key** (the general version of Kafka's sequence number, e.g. in payment APIs).

**Shows up elsewhere:** payment APIs (idempotency keys preventing double charges on network retries), HTTP `PUT` (defined idempotent) vs `POST` (not).

### Weighted random sampling

A raffle where everyone gets exactly one ticket is fair but ignores reality if, say, 500 people came from one city and 5 from another. Giving the bigger group more tickets makes the draw still fully random, just reflective of the real proportions.

**In the script:** `population.py`, `COUNTRY_WEIGHTS = [("India", 28), ("China", 22), ..., ("Kyrgyzstan", 2)]`. `_weighted_choice` hands these to `random.choices(options, weights=weights, k=1)`, which draws respecting those odds, so the fake population's country mix looks like a real one instead of uniformly spread across every country on earth.

**Terms:** **weighted random sampling**, **probability distribution** (the set of options and their relative likelihoods), **uniform distribution** (the alternative we deliberately avoided, every option equally likely).

**Shows up elsewhere:** ML training data, load balancers routing more traffic to bigger servers, statistical sampling generally.

### Finite state machine (FSM)

A multi-step process isn't one straight line everyone follows, it's a sequence of checkpoints, and at each one you either continue to the next checkpoint or your journey ends there. Visa example: documents fail → rejected, done. Documents pass → interview. Interview denied → done. Only those who clear every checkpoint make it to the end.

**In the script:** `lifecycle.py` is built exactly this way. Each checkpoint that can end things early does so with an early `return`:
```python
if not all_docs_accepted:
    events.append(StudentEvent(..., payload={"new_status": "withdrawn", ...}))
    return sorted(events, key=lambda e: e.event_timestamp)
```
Same shape repeats for the visa decision, and again inside the term-registration loop (exit mid-program instead of only at the start).

**Terms:** **state** (a distinct stage: applied, visa-pending, enrolled, withdrawn, graduated...), **transition** (an allowed move from one state to another), **terminal state** (a state with no transitions out, nothing happens after), **finite state machine / FSM** (the whole model: fixed states plus the transitions allowed between them).

**Shows up elsewhere:** CI/CD pipelines (queued → running → passed/failed), e-commerce orders (placed → paid → shipped, with cancel/refund branches), traffic lights, network protocols.

### Reproducibility via seeding

Anything involving randomness is hard to test, because the answer changes every run. Fixing the "starting point" of the randomness makes the output fully repeatable, same input in, same exact output out, every time.

**In the script:** `generate_population(num_students, seed=None)` in `population.py`:
```python
if seed is not None:
    random.seed(seed)
    Faker.seed(seed)
```
Call it twice with `seed=42` and get back the identical population both times. Leave `seed=None` and every run genuinely differs. This is what made the earlier lifecycle tests (denial branches, withdrawal branches, graduation) repeatable instead of hoping the right random branch happened to occur.

**Terms:** **seed** (the starting value that fully determines a "random" sequence), **deterministic** (same input always produces the same output), **PRNG**, pseudo-random number generator (what `random` actually is: deterministic math that looks random unless you know the seed).

**Shows up elsewhere:** reproducing ML training runs, shuffled train/test splits, game world generation from a seed (e.g. Minecraft).

### Minimizing coupling (lazy imports)

A module should only require what it actually needs for the specific thing it's doing right now, not pull in every dependency anything in the file might ever need.

**In the script:** `population.py` defines `Student` (a plain data shape) and `generate_population` (the thing that actually needs Faker) in the same file. If `from faker import Faker` sat at the top, just using `Student` anywhere, like in tests, would require Faker installed even though `Student` has nothing to do with Faker. So the import lives inside the function instead, only required when that function actually runs. Same move in `generator.py` for `confluent_kafka`, since dry-run mode never touches Kafka.

**Terms:** **coupling** (how much one piece of code depends on another; lower coupling means easier to change, test, and reuse independently), **lazy import** (importing inside a function rather than at the top of the file).

**Shows up elsewhere:** microservices (each service carries only its own dependencies), dependency injection in OOP design, why a shared "utils" file that imports everything is a common anti-pattern.

### Validation as a gate at system boundaries

The cheapest place to catch bad data is right before it leaves your control, not after, like a final check before sealing and mailing a letter, not after it's already gone.

**In the script:** `StudentEvent.validate()` in `schemas.py` checks structural sanity (student_id present, event_type is a real `EventType`, payload is a dict, timestamps aren't impossible), and `producer.py` calls it right before sending, the last check before the event leaves the program and becomes bytes on the wire.

**Terms:** **fail fast** (surface a problem immediately, at its source, rather than letting it propagate), **boundary** (the edge where data leaves one system's control and enters another's), **assertion** (a runtime check that crashes loudly if a condition isn't met).

**Shows up elsewhere:** API request validation before hitting a database, form validation before submission, type-checking at function boundaries in strongly-typed languages.

## 2. Coming up (placeholders, filled in as we build them)

- **Distributed computing and parallelism** (PySpark) — partitioning data across workers, shuffles, why joins are expensive
- **Orchestration and scheduling** (Airflow) — DAGs as a general concept, retries/backoff, idempotent task design, backfills
- **Schema evolution** (Kafka payloads over time) — what happens when a producer adds a field and a consumer doesn't know about it yet
- **Storage formats** (Parquet vs JSON in S3) — columnar vs row-based storage, why it matters for analytics workloads specifically
- **Streaming semantics** (Kafka consumer groups, offsets) — exactly-once vs at-least-once processing, replay
- **Warehousing patterns** (Redshift + dbt) — star schemas, OLAP vs OLTP, why you denormalize for analytics
- **Infrastructure as code** (Terraform) — declarative vs imperative infrastructure, state management, drift
- **Container orchestration** (Kubernetes) — declarative desired-state reconciliation (the same core idea as Terraform, applied to running processes instead of infrastructure)
