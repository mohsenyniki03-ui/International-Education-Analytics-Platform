"""
CLI entrypoint for the synthetic data generator.

Three modes:
  dry-run   Prints every event as JSON. No Kafka needed.
  backfill  Pushes all events to Kafka as fast as possible.
  stream    Replays events in compressed real time (--speed).
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from typing import List, Optional

from lifecycle import generate_student_events
from population import generate_population
from schemas import StudentEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def build_all_events(num_students: int, seed: Optional[int]) -> List[StudentEvent]:
    students = generate_population(num_students, seed=seed)
    all_events: List[StudentEvent] = []
    for student in students:
        all_events.extend(generate_student_events(student))
    all_events.sort(key=lambda e: e.event_timestamp)
    return all_events, students


def run_dry_run(events: List[StudentEvent], group_by_student: bool = False) -> None:
    if not group_by_student:
        for event in events:
            print(json.dumps(event.to_dict()))
        return

    # Group events by student_id, preserving per-student chronological order
    from collections import defaultdict
    grouped = defaultdict(list)
    for event in events:
        grouped[event.student_id].append(event)

    for i, (student_id, student_events) in enumerate(grouped.items(), start=1):
        print(f"\n{'─' * 60}")
        print(f"  Student {i} | ID: {student_id}")
        print(f"  {len(student_events)} events | "
              f"First: {student_events[0].event_type.value} | "
              f"Last: {student_events[-1].event_type.value}")
        print(f"{'─' * 60}")
        for event in student_events:
            print(json.dumps(event.to_dict(), indent=2))


def run_backfill(events: List[StudentEvent], bootstrap_servers: str) -> None:
    from producer import KafkaEventProducer

    producer = KafkaEventProducer(bootstrap_servers)
    for i, event in enumerate(events, start=1):
        producer.send(event)
        if i % 500 == 0:
            logger.info("Published %d/%d events", i, len(events))
    producer.flush()
    logger.info("Done. Published %d events.", len(events))


def run_stream(events: List[StudentEvent], bootstrap_servers: str, speed: float) -> None:
    from producer import KafkaEventProducer

    producer = KafkaEventProducer(bootstrap_servers)
    if not events:
        return

    sim_start = events[0].event_timestamp
    real_start = time.monotonic()

    for event in events:
        sim_elapsed = (event.event_timestamp - sim_start).total_seconds()
        target_real = sim_elapsed / speed
        sleep_for = target_real - (time.monotonic() - real_start)
        if sleep_for > 0:
            time.sleep(sleep_for)
        producer.send(event)
        logger.info("Published %s for student %s...", event.event_type.value, event.student_id[:8])

    producer.flush()
    logger.info("Stream finished.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic student lifecycle events")
    parser.add_argument("--num-students", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--mode", choices=["dry-run", "backfill", "stream"], default="dry-run")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--speed", type=float, default=3600.0)
    parser.add_argument(
        "--group-by-student",
        action="store_true",
        help="dry-run only: print all events per student together instead of global chronological order"
    )
    args = parser.parse_args()

    logger.info("Generating %d students (seed=%s)...", args.num_students, args.seed)
    events, _ = build_all_events(args.num_students, args.seed)
    logger.info("Generated %d events total.", len(events))

    if args.mode == "dry-run":
        run_dry_run(events, group_by_student=args.group_by_student)
    elif args.mode == "backfill":
        run_backfill(events, args.bootstrap_servers)
    elif args.mode == "stream":
        run_stream(events, args.bootstrap_servers, args.speed)


if __name__ == "__main__":
    main()
