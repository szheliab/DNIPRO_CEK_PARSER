"""Parsing of custom schedules that use whole-number queues (1, 2, 3).

Example message:
    📌 1 черга:
    ✔️ 00:00 - 02:00;
    ✔️ 10:00 - 14:00;
"""

import re
from typing import Callable, Dict, List

# "📌 1 черга:" - whole number only (no dot), so standard queues like 1.1, 2.2 are not matched
QUEUE_HEADER = re.compile(r"(?<![\d.])(\d{1,2})\s*черга\s*:", re.IGNORECASE)
TIME_RANGE = re.compile(r"(\d{1,2}):(\d{2})\s*[-–—]\s*(\d{1,2}):(\d{2})")


def is_other_city(message: str) -> bool:
    """True if the message header addresses another city (e.g. Pavlohrad) and not Dnipro."""
    header = message[:200].lower()
    return "павлоград" in header and "дніпро" not in header


def parse_custom_schedule(message: str) -> Dict[int, List[str]]:
    """Extract the schedule for whole-number queues.

    Returns:
        {1: ["00:00-02:00", "10:00-14:00"], 2: [...]}; empty dict if the format does not match
    """
    if is_other_city(message):
        return {}

    headers = list(QUEUE_HEADER.finditer(message))
    schedules: Dict[int, List[str]] = {}

    for i, header in enumerate(headers):
        end = headers[i + 1].start() if i + 1 < len(headers) else len(message)
        slots = [
            f"{int(m[0]):02d}:{m[1]}-{int(m[2]):02d}:{m[3]}"
            for m in TIME_RANGE.findall(message[header.end():end])
        ]
        if slots:
            schedules.setdefault(int(header.group(1)), []).extend(slots)

    return schedules


def build_custom_hours(
    schedules: Dict[int, List[str]],
    hours_builder: Callable[[List[str]], Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    """Convert time slots to hourly statuses: {"1": {"1": "no", ...}, ...}"""
    return {str(q): hours_builder(slots) for q, slots in sorted(schedules.items())}
