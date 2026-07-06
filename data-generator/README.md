# Data generator — how it works

This covers the three files that make up the synthetic data generator: `schemas.py`, `population.py`, and `lifecycle.py`. Each section has a diagram of the core logic plus a short explanation of why it's built that way.

## schemas.py — the event vocabulary

Every event shares one fixed envelope. The envelope's shape never changes; only the payload inside it does, depending on which of the eight event types it is.

```mermaid
flowchart LR
    subgraph Envelope["Fixed envelope (same shape every time)"]
        E1[event_id]
        E2[event_type]
        E3[student_id]
        E4[event_timestamp]
        E5[produced_at]
    end
    subgraph Payload["Flexible payload (shape depends on event_type)"]
        P1["document_submitted:<br/>document_type, status"]
        P2["visa_status_change:<br/>visa_type, status"]
        P3["...6 other shapes"]
    end
    Envelope --> Payload
```

The envelope/payload split exists so anything downstream (Airflow, Spark) can handle all eight event types with identical code by reading the envelope, and only needs type-specific logic when it actually cares what's inside the payload.

Each event also knows its own destination. The `event_type` is looked up in a single dictionary (`TOPIC_FOR_EVENT`) to find which Kafka topic it belongs to, so the routing logic lives in exactly one place instead of being hardcoded everywhere an event gets sent.

```mermaid
flowchart TD
    A[StudentEvent] -->|reads event_type| B[TOPIC_FOR_EVENT lookup]
    B --> C[intl.student.applications]
    B --> D[intl.student.documents]
    B --> E[intl.student.visa_status]
    B --> F[intl.student.enrollment]
    B --> G[intl.student.registration]
    B --> H[intl.student.opt_cpt]
    B --> I[intl.student.status_change]
    B --> J[intl.student.graduation]
```

## population.py — the student blueprint

Before anything happens to a student, this file decides what a student *is*: a fixed snapshot of attributes like country, program, degree level, and funding source. Realism comes from weighted random choices instead of uniform ones, India and China should be far more likely than Kyrgyzstan, not equally likely.

```mermaid
flowchart TD
    A["Weighted lists<br/>(COUNTRY_WEIGHTS, PROGRAMS,<br/>DEGREE_LEVELS, FUNDING_SOURCES)"] --> B[_weighted_choice]
    B --> C["One Student record<br/>(country, program, degree level,<br/>funding source, visa type)"]
    C --> D["generate_population()<br/>repeats N times"]
    D --> E["Population of N students<br/>with realistic distribution"]
```

`_weighted_choice` exists because `random.choices()` needs two separate parallel lists (the options, and their weights), but the data is naturally stored as bundled pairs for readability. The function's only job is splitting the bundle into the two lists the random function actually requires.

## lifecycle.py — the student's journey

This is the file that decides what *happens* to each student over time, and the core idea is branching: not every student makes it to graduation. There are three points where a student's story can end early, plus one finish line.

```mermaid
flowchart TD
    A[Application submitted] --> B["Documents submitted<br/>(5 required)"]
    B -->|any rejected| X1[Withdrawn:<br/>incomplete documentation]
    B -->|all accepted| C["Visa interview<br/>+ decision"]
    C -->|denied| X2[Withdrawn:<br/>visa denied]
    C -->|issued| D[Enrollment confirmed]
    D --> E[Register for current term]
    E -->|small chance each term| X3[Leave of absence /<br/>withdrawn / transferred]
    E -->|not enough terms yet| E
    E -->|enough terms completed| F[OPT/CPT request]
    F --> G[Graduation]
```

Each exit point (`X1`, `X2`, `X3`) is a real `status_change` event the function generates before returning immediately, no further events get created for that student once they've exited. The term registration loop is the one part that can repeat multiple times per student rather than happening once, since a student registers every term they're active, not just a single time.

This branching is what makes the dataset useful for funnel analysis later: the curated tables will be able to show real drop-off rates at each stage (document rejection rate, visa denial rate, mid-program attrition rate) instead of a fake dataset where everyone sails through to graduation.
