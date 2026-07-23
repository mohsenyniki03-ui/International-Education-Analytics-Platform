"""
api/main.py

FastAPI backend for the EduFlow International Education Analytics Platform.
Queries the PostgreSQL warehouse (star schema) and serves results as JSON
for the dashboard frontend.

Run locally:
    pip install fastapi uvicorn psycopg2-binary
    uvicorn api.main:app --reload --port 8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import psycopg2
import psycopg2.extras
import os

app = FastAPI(title="EduFlow Analytics API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── DATABASE CONNECTION ───────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "localhost"),
        port=int(os.getenv("WAREHOUSE_PORT", "5432")),
        dbname=os.getenv("WAREHOUSE_DB", "eduflow_warehouse"),
        user=os.getenv("WAREHOUSE_USER", "airflow"),
        password=os.getenv("WAREHOUSE_PASSWORD", "airflow"),
    )


def query(sql: str, params=None) -> list[dict]:
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


# ── ENDPOINTS ─────────────────────────────────────────────────────────────────

@app.get("/api/metrics")
def get_metrics():
    students = query("SELECT COUNT(*) as total FROM dim_students")[0]["total"]
    countries = query("SELECT COUNT(DISTINCT country_of_origin) as total FROM dim_students")[0]["total"]

    funnel = query("""
        SELECT
            COUNT(DISTINCT CASE WHEN event_type = 'enrollment_confirmed' THEN student_id END) as enrolled,
            COUNT(DISTINCT CASE WHEN event_type = 'graduation' THEN student_id END) as graduated
        FROM fact_student_events
    """)[0]

    enrolled = funnel["enrolled"]
    graduated = funnel["graduated"]
    enrollment_rate = round(enrolled * 100.0 / students) if students else 0
    graduation_rate = round(graduated * 100.0 / enrolled) if enrolled else 0

    return {
        "total_students": students,
        "total_enrolled": enrolled,
        "total_graduated": graduated,
        "countries_represented": countries,
        "enrollment_rate": enrollment_rate,
        "graduation_rate": graduation_rate,
    }


@app.get("/api/funnel")
def get_funnel():
    result = query("""
        SELECT
            COUNT(DISTINCT CASE WHEN event_type = 'application_submitted' THEN student_id END) as applied,
            COUNT(DISTINCT CASE WHEN event_type = 'enrollment_confirmed'  THEN student_id END) as enrolled,
            COUNT(DISTINCT CASE WHEN event_type = 'graduation'            THEN student_id END) as graduated
        FROM fact_student_events
    """)
    return result[0] if result else {}


@app.get("/api/countries")
def get_countries():
    return query("""
        SELECT country_of_origin, COUNT(*) as students
        FROM dim_students
        GROUP BY country_of_origin
        ORDER BY students DESC
        LIMIT 20
    """)


@app.get("/api/enrollment-rates")
def get_enrollment_rates():
    return query("""
        SELECT
            s.country_of_origin,
            COUNT(DISTINCT s.student_id) as applied,
            COUNT(DISTINCT CASE WHEN f.event_type = 'enrollment_confirmed' THEN f.student_id END) as enrolled,
            ROUND(COUNT(DISTINCT CASE WHEN f.event_type = 'enrollment_confirmed'
                THEN f.student_id END) * 100.0 /
                NULLIF(COUNT(DISTINCT s.student_id), 0)) as enrollment_rate
        FROM dim_students s
        LEFT JOIN fact_student_events f ON s.student_id = f.student_id
        GROUP BY s.country_of_origin
        ORDER BY applied DESC
        LIMIT 15
    """)


@app.get("/api/programs")
def get_programs():
    return query("""
        SELECT program, school, COUNT(*) as students
        FROM dim_students
        WHERE program IS NOT NULL
        GROUP BY program, school
        ORDER BY students DESC
        LIMIT 10
    """)


@app.get("/api/degree-levels")
def get_degree_levels():
    return query("""
        SELECT degree_level, COUNT(*) as students
        FROM dim_students
        WHERE degree_level IS NOT NULL
        GROUP BY degree_level
        ORDER BY students DESC
    """)


@app.get("/api/funding-sources")
def get_funding_sources():
    return query("""
        SELECT
            REPLACE(funding_source, '_', ' ') as funding_source,
            COUNT(*) as students
        FROM dim_students
        WHERE funding_source IS NOT NULL
        GROUP BY funding_source
        ORDER BY students DESC
    """)


@app.get("/api/visa-status")
def get_visa_status():
    """
    Breakdown of visa outcomes: issued, denied, interview_scheduled.
    Pulled from the staging table since fact_student_events doesn't
    carry the visa status payload field directly.
    """
    return query("""
        SELECT
            payload_status as status,
            COUNT(*) as count
        FROM stg_visa_status_change
        WHERE payload_status IS NOT NULL
        GROUP BY payload_status
        ORDER BY count DESC
    """)


@app.get("/api/gender")
def get_gender():
    """Breakdown of students by gender."""
    return query("""
        SELECT gender, COUNT(*) as students
        FROM dim_students
        WHERE gender IS NOT NULL
        GROUP BY gender
        ORDER BY students DESC
    """)


@app.get("/api/trend")
def get_trend():
    """
    Monthly event counts for applications, enrollments, and graduations.
    Shows pipeline activity trends and seasonal patterns.
    """
    return query("""
        SELECT
            TO_CHAR(DATE_TRUNC('month', event_timestamp), 'YYYY-MM') as month,
            event_type,
            COUNT(*) as events
        FROM fact_student_events
        WHERE event_timestamp IS NOT NULL
          AND event_type IN (
              'application_submitted',
              'enrollment_confirmed',
              'graduation'
          )
        GROUP BY DATE_TRUNC('month', event_timestamp), event_type
        ORDER BY month, event_type
    """)


# serve the dashboard static files
app.mount("/", StaticFiles(directory="dashboard", html=True), name="dashboard")
