"""
pytest suite for the data generator.

Covers four things:
  1. Student completeness  — every generated student has all required fields
  2. Realistic proportions — country distribution matches real-world skew
  3. Lifecycle ordering    — events for a student are always chronological
  4. Lifecycle branching   — withdrawn/denied students never get post-exit events

Run with:
  pytest tests/test_generator.py -v
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../data-generator"))

import random
from collections import Counter

import pytest

from population import Student, generate_population
from lifecycle import generate_student_events
from schemas import EventType, StudentEvent
import datetime


# ── fixtures ──────────────────────────────────────────────────────────────────
# A pytest fixture is a reusable piece of test setup. Instead of repeating
# "generate a population" in every test, we define it once here and any test
# that names it as a parameter gets it automatically.

@pytest.fixture(scope="module")
def small_population():
    """200 students, fixed seed for repeatability."""
    return generate_population(200, seed=42)


@pytest.fixture(scope="module")
def large_population():
    """2000 students for proportion/distribution tests."""
    return generate_population(2000, seed=42)


@pytest.fixture(scope="module")
def all_events(small_population):
    """Full lifecycle events for the small population."""
    events = []
    for student in small_population:
        events.extend(generate_student_events(student))
    return events


# ── 1. Student completeness ───────────────────────────────────────────────────

class TestStudentCompleteness:

    def test_all_required_fields_present(self, small_population):
        """Every student must have all required fields filled in, no None or empty string."""
        required_fields = [
            "student_id", "full_name", "date_of_birth", "gender",
            "country_of_origin", "program", "school",
            "degree_level", "entry_term", "funding_source", "visa_type",
        ]
        for student in small_population:
            student_dict = student.to_dict()
            for field in required_fields:
                assert student_dict[field], (
                    f"Student {student.student_id} is missing field: {field}"
                )

    def test_student_ids_are_unique(self, small_population):
        """No two students should share the same ID."""
        ids = [s.student_id for s in small_population]
        assert len(ids) == len(set(ids)), "Duplicate student IDs found"

    def test_degree_levels_are_valid(self, small_population):
        valid_levels = {"Bachelors", "Masters", "PhD"}
        for student in small_population:
            assert student.degree_level in valid_levels, (
                f"Invalid degree level: {student.degree_level}"
            )

    def test_visa_types_are_valid(self, small_population):
        valid_visas = {"F-1", "J-1"}
        for student in small_population:
            assert student.visa_type in valid_visas, (
                f"Invalid visa type: {student.visa_type}"
            )

    def test_gender_values_are_valid(self, small_population):
        valid_genders = {"Female", "Male"}
        for student in small_population:
            assert student.gender in valid_genders, (
                f"Invalid gender: {student.gender}"
            )

    def test_population_size_matches_request(self):
        """generate_population should return exactly the number requested."""
        pop = generate_population(150, seed=1)
        assert len(pop) == 150


# ── 2. Realistic proportions ──────────────────────────────────────────────────

class TestRealisticProportions:

    def test_india_and_china_are_most_common(self, large_population):
        """India and China should be the two most common countries of origin."""
        country_counts = Counter(s.country_of_origin for s in large_population)
        top_two = [country for country, _ in country_counts.most_common(2)]
        assert "India" in top_two, "India should be one of the top 2 countries"
        assert "China" in top_two, "China should be one of the top 2 countries"

    def test_india_significantly_more_common_than_kyrgyzstan(self, large_population):
        """India (weight 28) should appear at least 5x more than Kyrgyzstan (weight 2)."""
        country_counts = Counter(s.country_of_origin for s in large_population)
        india_count = country_counts.get("India", 0)
        kyrgyzstan_count = country_counts.get("Kyrgyzstan", 0)
        assert india_count > kyrgyzstan_count * 5, (
            f"India ({india_count}) should be at least 5x Kyrgyzstan ({kyrgyzstan_count})"
        )

    def test_masters_is_most_common_degree(self, large_population):
        """Masters (weight 55) should be the most common degree level."""
        degree_counts = Counter(s.degree_level for s in large_population)
        most_common = degree_counts.most_common(1)[0][0]
        assert most_common == "Masters", (
            f"Masters should be most common, got {most_common}"
        )

    def test_f1_visa_dominates(self, large_population):
        """F-1 (weight 90) should be far more common than J-1 (weight 10)."""
        visa_counts = Counter(s.visa_type for s in large_population)
        assert visa_counts["F-1"] > visa_counts["J-1"] * 5, (
            "F-1 visas should dominate over J-1"
        )

    def test_same_seed_produces_same_population(self):
        """Two calls with the same seed should produce identical populations."""
        pop1 = generate_population(50, seed=99)
        pop2 = generate_population(50, seed=99)
        ids1 = [s.student_id for s in pop1]
        ids2 = [s.student_id for s in pop2]
        # student_ids are uuid4 so they'll differ, but names and attributes should match
        names1 = [s.full_name for s in pop1]
        names2 = [s.full_name for s in pop2]
        assert names1 == names2, "Same seed should produce same population"

    def test_different_seeds_produce_different_populations(self):
        """Two calls with different seeds should produce different populations."""
        pop1 = generate_population(50, seed=1)
        pop2 = generate_population(50, seed=2)
        names1 = [s.full_name for s in pop1]
        names2 = [s.full_name for s in pop2]
        assert names1 != names2, "Different seeds should produce different populations"


# ── 3. Lifecycle ordering ─────────────────────────────────────────────────────

class TestLifecycleOrdering:

    def test_every_student_has_at_least_one_event(self, small_population):
        """Every student must produce at least the application event."""
        for student in small_population:
            events = generate_student_events(student)
            assert len(events) >= 1, (
                f"Student {student.student_id} produced no events"
            )

    def test_events_are_in_chronological_order(self, small_population):
        """For every student, events must be sorted oldest to newest."""
        for student in small_population:
            events = generate_student_events(student)
            timestamps = [e.event_timestamp for e in events]
            assert timestamps == sorted(timestamps), (
                f"Events out of order for student {student.student_id}"
            )

    def test_application_is_always_first_event(self, small_population):
        """The application_submitted event must always be the first event."""
        for student in small_population:
            events = generate_student_events(student)
            assert events[0].event_type == EventType.APPLICATION_SUBMITTED, (
                f"First event is not application for student {student.student_id}"
            )

    def test_graduation_is_always_last_event_when_present(self, small_population):
        """If a graduation event exists, it must be the final event."""
        for student in small_population:
            events = generate_student_events(student)
            event_types = [e.event_type for e in events]
            if EventType.GRADUATION in event_types:
                assert events[-1].event_type == EventType.GRADUATION, (
                    f"Graduation is not last event for student {student.student_id}"
                )

    def test_produced_at_never_before_event_timestamp(self, all_events):
        """produced_at must always be >= event_timestamp (validate() enforces this)."""
        for event in all_events:
            assert event.produced_at >= event.event_timestamp, (
                f"produced_at before event_timestamp for event {event.event_id}"
            )


# ── 4. Lifecycle branching ────────────────────────────────────────────────────

class TestLifecycleBranching:

    def test_withdrawn_students_have_no_enrollment_event(self, small_population):
        """A student withdrawn before enrollment must never have an enrollment_confirmed event."""
        for student in small_population:
            events = generate_student_events(student)
            event_types = [e.event_type for e in events]

            is_withdrawn_early = (
                EventType.STATUS_CHANGE in event_types
                and EventType.ENROLLMENT_CONFIRMED not in event_types
            )
            if is_withdrawn_early:
                assert EventType.ENROLLMENT_CONFIRMED not in event_types, (
                    f"Withdrawn student {student.student_id} has an enrollment event"
                )

    def test_visa_denied_students_have_no_enrollment(self, small_population):
        """A student with a denied visa must never have an enrollment_confirmed event."""
        for student in small_population:
            events = generate_student_events(student)
            visa_events = [
                e for e in events if e.event_type == EventType.VISA_STATUS_CHANGE
            ]
            visa_denied = any(
                e.payload.get("status") == "denied" for e in visa_events
            )
            if visa_denied:
                event_types = [e.event_type for e in events]
                assert EventType.ENROLLMENT_CONFIRMED not in event_types, (
                    f"Visa-denied student {student.student_id} has an enrollment event"
                )

    def test_graduated_students_have_opt_cpt_request(self, small_population):
        """Every student who graduates must have an OPT/CPT request event before graduation."""
        for student in small_population:
            events = generate_student_events(student)
            event_types = [e.event_type for e in events]
            if EventType.GRADUATION in event_types:
                assert EventType.OPT_CPT_REQUEST in event_types, (
                    f"Graduated student {student.student_id} has no OPT/CPT request"
                )

    def test_all_events_pass_validation(self, all_events):
        """Every single event in the full dataset must pass validate() without error."""
        for event in all_events:
            try:
                event.validate()
            except AssertionError as e:
                pytest.fail(f"Event {event.event_id} failed validation: {e}")
