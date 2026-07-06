"""
Generates a synthetic population of international students.

Country weights are loosely modeled on real-world international student
enrollment mixes in the US (India and China dominate, with a long tail of
other countries) so that downstream "enrollment by country" analytics look
like something you'd actually see in an institutional report.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import date
from typing import List

from schemas import DEGREE_LEVELS, FUNDING_SOURCES, VISA_TYPES

# (country, weight) — weights are relative, not percentages.
COUNTRY_WEIGHTS = [
    ("India", 28),
    ("China", 22),
    ("South Korea", 6),
    ("Vietnam", 4),
    ("Nigeria", 4),
    ("Saudi Arabia", 3),
    ("Canada", 3),
    ("Brazil", 3),
    ("Iran", 3),
    ("Bangladesh", 3),
    ("Nepal", 3),
    ("Mexico", 3),
    ("Taiwan", 3),
    ("Pakistan", 3),
    ("Turkey", 2),
    ("Indonesia", 2),
    ("Ghana", 2),
    ("Colombia", 2),
    ("Kyrgyzstan", 2),
    ("Spain", 2),
]

PROGRAMS = [
    ("Computer Science", "Luddy School of Informatics, Computing, and Engineering"),
    ("Data Science", "Luddy School of Informatics, Computing, and Engineering"),
    ("Informatics", "Luddy School of Informatics, Computing, and Engineering"),
    ("Business Analytics", "Kelley School of Business"),
    ("Finance", "Kelley School of Business"),
    ("Public Affairs", "O'Neill School of Public and Environmental Affairs"),
    ("Chemistry", "College of Arts and Sciences"),
    ("Economics", "College of Arts and Sciences"),
    ("Electrical Engineering", "Luddy School of Informatics, Computing, and Engineering"),
]

TERMS = ["Fall 2023", "Spring 2024", "Fall 2024", "Spring 2025", "Fall 2025", "Spring 2026"]


@dataclass
class Student:
    student_id: str
    full_name: str
    date_of_birth: date
    gender: str
    country_of_origin: str
    program: str
    school: str
    degree_level: str
    entry_term: str
    funding_source: str
    visa_type: str
    
    # Serializes the student to a dict for JSON encoding. Datetime fields are converted to ISO format strings.
    def to_dict(self) -> dict:
        return {
            "student_id": self.student_id,
            "full_name": self.full_name,
            "date_of_birth": self.date_of_birth.isoformat(),
            "gender": self.gender,
            "country_of_origin": self.country_of_origin,
            "program": self.program,
            "school": self.school,
            "degree_level": self.degree_level,
            "entry_term": self.entry_term,
            "funding_source": self.funding_source,
            "visa_type": self.visa_type,
        }


# Helper function to make weighted random choices more concise in the main generation loop.
# zip is used to separate the options/countries and weights into two lists, which random.choices can then 
def _weighted_choice(weighted_options: List[tuple]) -> str:
    options, weights = zip(*weighted_options)
    return random.choices(options, weights=weights, k=1)[0]


def generate_population(num_students: int, seed: int | None = None) -> List[Student]:
    from faker import Faker  # imported lazily so this module has no hard Faker dependency

    fake = Faker()
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

    students = []
    for _ in range(num_students):
        program, school = random.choice(PROGRAMS)
        degree_level = random.choices(DEGREE_LEVELS, weights=[35, 55, 10], k=1)[0]
        gender = random.choices(["Female", "Male"], weights=[50, 50], k=1)[0]

        students.append(
            Student(
                student_id=str(uuid.uuid4()),
                full_name=fake.name(),
                date_of_birth=fake.date_of_birth(minimum_age=18, maximum_age=35),
                gender=gender,
                country_of_origin=_weighted_choice(COUNTRY_WEIGHTS),
                program=program,
                school=school,
                degree_level=degree_level,
                entry_term=random.choice(TERMS),
                funding_source=_weighted_choice(
                    [(f, 30 if f == "self_funded" else 17) for f in FUNDING_SOURCES]
                ),
                visa_type=random.choices(VISA_TYPES, weights=[90, 10], k=1)[0],
            )
        )
    return students