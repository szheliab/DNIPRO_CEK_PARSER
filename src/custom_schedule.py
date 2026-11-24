"""Parsing of custom schedules that use whole-number queues (1, 2, 3).

Example message:
    📌 1 черга:
    ✔️ 00:00 - 02:00;
    ✔️ 10:00 - 14:00;
"""

import re
from typing import Callable, Dict, List

# Any queue header ("1 черга:", "1.1 черга:"); standard headers only delimit blocks
QUEUE_HEADER = re.compile(
    r"(?<![\d.])(\d{1,2}(?:\.\d)?)\s*(?:черга|група)\s*:", re.IGNORECASE
)
# Addressed city, e.g. "м. Дніпро" / "м. ПАВЛОГРАД"
CITY = re.compile(r"м\.\s*([А-Яа-яІіЇїЄєҐґ'’ʼ-]+)")
TIME_RANGE = re.compile(r"(\d{1,2}):(\d{2})\s*[-–—]\s*(\d{1,2}):(\d{2})")


def is_other_city(message: str) -> bool:
    """True if the message header addresses a city other than Dnipro (e.g. Pavlohrad)."""
    match = CITY.search(message[:200])
    return bool(match) and match.group(1).lower() != "дніпро"


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
        if "." in header.group(1):
            continue

        slots = []
        for sh, sm, eh, em in TIME_RANGE.findall(message[header.end():end]):
            # A range ending at 00:00 means the end of the day
            if int(eh) == 0 and em == "00" and (int(sh), sm) != (0, "00"):
                eh = "24"
            slots.append(f"{int(sh):02d}:{sm}-{int(eh):02d}:{em}")
        if slots:
            schedules.setdefault(int(header.group(1)), []).extend(slots)

    return schedules


def build_custom_hours(
    schedules: Dict[int, List[str]],
    hours_builder: Callable[[List[str]], Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    """Convert time slots to hourly statuses: {"1": {"1": "no", ...}, ...}"""
    return {str(q): hours_builder(slots) for q, slots in sorted(schedules.items())}
