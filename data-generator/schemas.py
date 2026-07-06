"""
Event schema definitions for the international student lifecycle pipeline.

Every event shares a common envelope (event_id, event_type, student_id,
event_timestamp, produced_at) wrapping a payload whose shape depends on
event_type. Topics map 1:1 with the broad event categories below, and the
student_id is always used as the Kafka partition key so a single student's
events stay strictly ordered.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict


class EventType(str, Enum):
    APPLICATION_SUBMITTED = "application_submitted"
    DOCUMENT_SUBMITTED = "document_submitted"
    VISA_STATUS_CHANGE = "visa_status_change"
    ENROLLMENT_CONFIRMED = "enrollment_confirmed"
    TERM_REGISTRATION = "term_registration"
    OPT_CPT_REQUEST = "opt_cpt_request"
    STATUS_CHANGE = "status_change"
    GRADUATION = "graduation"


# Maps each event type to the Kafka topic it is published to.
TOPIC_FOR_EVENT: Dict[EventType, str] = {
    EventType.APPLICATION_SUBMITTED: "intl.student.applications",
    EventType.DOCUMENT_SUBMITTED: "intl.student.documents",
    EventType.VISA_STATUS_CHANGE: "intl.student.visa_status",
    EventType.ENROLLMENT_CONFIRMED: "intl.student.enrollment",
    EventType.TERM_REGISTRATION: "intl.student.registration",
    EventType.OPT_CPT_REQUEST: "intl.student.opt_cpt",
    EventType.STATUS_CHANGE: "intl.student.status_change",
    EventType.GRADUATION: "intl.student.graduation",
}

ALL_TOPICS = sorted(set(TOPIC_FOR_EVENT.values()))

DOCUMENT_TYPES = ["passport", "i20", "financial_proof", "transcript", "english_proficiency"]
DOCUMENT_STATUSES = ["submitted", "under_review", "accepted", "rejected"]

VISA_TYPES = ["F-1", "J-1"]
VISA_STATUSES = ["interview_scheduled", "issued", "denied", "administrative_processing"]

FUNDING_SOURCES = [
    "self_funded",
    "university_scholarship",
    "government_scholarship",
    "graduate_assistantship",
    "employer_sponsored",
]

DEGREE_LEVELS = ["Bachelors", "Masters", "PhD"]

OPT_CPT_TYPES = ["CPT", "OPT", "STEM_OPT_Extension"]
OPT_CPT_STATUSES = ["requested", "approved", "denied"]

STATUS_CHANGE_VALUES = ["active", "leave_of_absence", "withdrawn", "transferred"]


@dataclass
class StudentEvent:
    event_type: EventType
    student_id: str
    event_timestamp: datetime
    payload: Dict[str, Any]
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    produced_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    @property
    # The Kafka topic this event should be published to, derived from its type.
    def topic(self) -> str:
        return TOPIC_FOR_EVENT[self.event_type]

    # Serializes the event to a dict for JSON encoding. Datetime fields are converted to ISO format strings.
    # simply transforming a python object/dataclass to a dict, so we can JSON-encode it for Kafka. 
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "student_id": self.student_id,
            "event_timestamp": self.event_timestamp.isoformat(),
            "produced_at": self.produced_at.isoformat(),
            "payload": self.payload,
        }

    def validate(self) -> None:
        """Raises AssertionError if the event is structurally invalid.

        Kept deliberately simple/dependency-free so it can run both inside
        the generator and inside pytest without extra libraries. A stretch
        goal is swapping this for Great Expectations or a JSON Schema
        validator once the pipeline is end-to-end.
        """
        assert self.student_id, "student_id is required"
        assert isinstance(self.event_type, EventType), "event_type must be an EventType"
        assert isinstance(self.payload, dict), "payload must be a dict"
        assert self.event_timestamp <= self.produced_at, (
            "event_timestamp cannot be after produced_at"
        )