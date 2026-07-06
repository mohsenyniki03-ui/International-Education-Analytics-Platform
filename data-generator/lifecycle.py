"""
Simulates a single student's lifecycle as an ordered sequence of StudentEvent
objects: application -> documents -> visa -> enrollment -> term-by-term
registration -> (graduation | withdrawal | leave of absence), with branching
probabilities at each stage so the resulting dataset has realistic funnel
drop-off (not every applicant enrolls, not every enrolled student graduates).
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

from population import TERMS, Student
from schemas import DOCUMENT_TYPES, EventType, StudentEvent

TERM_START_DATES = {
    "Fall 2023": date(2023, 8, 21),
    "Spring 2024": date(2024, 1, 8),
    "Fall 2024": date(2024, 8, 26),
    "Spring 2025": date(2025, 1, 13),
    "Fall 2025": date(2025, 8, 25),
    "Spring 2026": date(2026, 1, 12),
}

TERM_LENGTH_DAYS = 110  # ~ a 16-week semester, rounded for simplicity

# Number of registered terms before a student in this degree level graduates.
# Deliberately exceeds the number of terms in TERM_START_DATES for Bachelors
# and PhD, so most of those students simply show up as "currently enrolled"
# rather than graduating within the simulation window — same as reality.
DEGREE_TERMS_REQUIRED = {"Bachelors": 8, "Masters": 4, "PhD": 10}

STEM_PROGRAMS = {"Computer Science", "Data Science", "Electrical Engineering"}


def _to_dt(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 9, 0, 0, tzinfo=timezone.utc) + timedelta(
        hours=random.randint(0, 9), minutes=random.randint(0, 59)
    )


def _next_term(term: str) -> Optional[str]:
    idx = TERMS.index(term)
    return TERMS[idx + 1] if idx + 1 < len(TERMS) else None


def generate_student_events(student: Student) -> List[StudentEvent]:
    events: List[StudentEvent] = []
    term_start = TERM_START_DATES[student.entry_term]

    # 1. Application submitted, ~5-7 months before term start
    app_date = term_start - timedelta(days=random.randint(150, 210))
    events.append(
        StudentEvent(
            event_type=EventType.APPLICATION_SUBMITTED,
            student_id=student.student_id,
            event_timestamp=_to_dt(app_date),
            payload={
                "program": student.program,
                "school": student.school,
                "term": student.entry_term,
                "degree_level": student.degree_level,
                "funding_source": student.funding_source,
            },
        )
    )

    # 2. Required documents, staggered over the following weeks
    cursor = app_date
    all_docs_accepted = True
    for doc_type in DOCUMENT_TYPES:
        cursor += timedelta(days=random.randint(3, 14))
        events.append(
            StudentEvent(
                event_type=EventType.DOCUMENT_SUBMITTED,
                student_id=student.student_id,
                event_timestamp=_to_dt(cursor),
                payload={"document_type": doc_type, "status": "submitted"},
            )
        )
        review_date = cursor + timedelta(days=random.randint(5, 21))
        final_status = random.choices(["accepted", "rejected"], weights=[92, 8], k=1)[0]
        if final_status == "rejected":
            all_docs_accepted = False
        events.append(
            StudentEvent(
                event_type=EventType.DOCUMENT_SUBMITTED,
                student_id=student.student_id,
                event_timestamp=_to_dt(review_date),
                payload={"document_type": doc_type, "status": final_status},
            )
        )
        cursor = review_date

    if not all_docs_accepted:
        events.append(
            StudentEvent(
                event_type=EventType.STATUS_CHANGE,
                student_id=student.student_id,
                event_timestamp=_to_dt(cursor + timedelta(days=10)),
                payload={"new_status": "withdrawn", "reason": "incomplete_documentation"},
            )
        )
        return sorted(events, key=lambda e: e.event_timestamp)

    # 3. Visa process
    interview_date = cursor + timedelta(days=random.randint(7, 30))
    events.append(
        StudentEvent(
            event_type=EventType.VISA_STATUS_CHANGE,
            student_id=student.student_id,
            event_timestamp=_to_dt(interview_date),
            payload={"visa_type": student.visa_type, "status": "interview_scheduled"},
        )
    )
    decision_date = interview_date + timedelta(days=random.randint(3, 21))
    visa_outcome = random.choices(["issued", "denied"], weights=[88, 12], k=1)[0]
    events.append(
        StudentEvent(
            event_type=EventType.VISA_STATUS_CHANGE,
            student_id=student.student_id,
            event_timestamp=_to_dt(decision_date),
            payload={"visa_type": student.visa_type, "status": visa_outcome},
        )
    )

    if visa_outcome == "denied":
        events.append(
            StudentEvent(
                event_type=EventType.STATUS_CHANGE,
                student_id=student.student_id,
                event_timestamp=_to_dt(decision_date + timedelta(days=5)),
                payload={"new_status": "withdrawn", "reason": "visa_denied"},
            )
        )
        return sorted(events, key=lambda e: e.event_timestamp)

    # 4. Enrollment confirmed at term start
    events.append(
        StudentEvent(
            event_type=EventType.ENROLLMENT_CONFIRMED,
            student_id=student.student_id,
            event_timestamp=_to_dt(term_start),
            payload={"program": student.program, "term": student.entry_term},
        )
    )

    # 5. Term-by-term registration until graduation, withdrawal, or window end
    terms_required = DEGREE_TERMS_REQUIRED[student.degree_level]
    current_term: Optional[str] = student.entry_term
    terms_completed = 0
    graduated = False

    while current_term is not None:
        reg_date = TERM_START_DATES[current_term] + timedelta(days=random.randint(-5, 5))
        events.append(
            StudentEvent(
                event_type=EventType.TERM_REGISTRATION,
                student_id=student.student_id,
                event_timestamp=_to_dt(reg_date),
                payload={
                    "term": current_term,
                    "courses_count": random.randint(3, 5),
                    "full_time_status": True,
                },
            )
        )
        terms_completed += 1

        if terms_completed < terms_required and random.random() < 0.05:
            status = random.choices(
                ["leave_of_absence", "withdrawn", "transferred"], weights=[50, 30, 20], k=1
            )[0]
            events.append(
                StudentEvent(
                    event_type=EventType.STATUS_CHANGE,
                    student_id=student.student_id,
                    event_timestamp=_to_dt(reg_date + timedelta(days=random.randint(30, 80))),
                    payload={"new_status": status, "reason": "self_reported"},
                )
            )
            if status != "leave_of_absence":
                return sorted(events, key=lambda e: e.event_timestamp)

        if terms_completed >= terms_required:
            graduated = True
            break

        current_term = _next_term(current_term)

    if graduated and current_term is not None:
        term_end = TERM_START_DATES[current_term] + timedelta(days=TERM_LENGTH_DAYS)
        opt_date = term_end - timedelta(days=random.randint(20, 45))
        opt_type = (
            "STEM_OPT_Extension" if student.program in STEM_PROGRAMS else random.choice(["OPT", "CPT"])
        )
        opt_status = random.choices(["approved", "denied", "requested"], weights=[85, 5, 10], k=1)[0]
        events.append(
            StudentEvent(
                event_type=EventType.OPT_CPT_REQUEST,
                student_id=student.student_id,
                event_timestamp=_to_dt(opt_date),
                payload={"request_type": opt_type, "status": opt_status},
            )
        )
        events.append(
            StudentEvent(
                event_type=EventType.GRADUATION,
                student_id=student.student_id,
                event_timestamp=_to_dt(term_end),
                payload={
                    "program": student.program,
                    "degree_level": student.degree_level,
                    "term": current_term,
                },
            )
        )

    return sorted(events, key=lambda e: e.event_timestamp)