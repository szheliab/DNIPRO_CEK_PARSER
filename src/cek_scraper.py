#!/usr/bin/env python3
"""
Powercut Schedule Scraper for Telegram Channels
Scrapes electricity outage schedules for ALL queues and outputs to JSON format
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import json
import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional
import argparse


# Setup logging
def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure logging for production use"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # Log format with Kyiv timezone
    kyiv_tz = ZoneInfo("Europe/Kyiv")

    class KyivFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, tz=kyiv_tz)
            if datefmt:
                return dt.strftime(datefmt)
            return dt.isoformat()

    logger = logging.getLogger("cek_scraper")
    logger.setLevel(getattr(logging, log_level.upper()))

    # Console handler - respects the log_level parameter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_formatter = KyivFormatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )
    console_handler.setFormatter(console_formatter)

    # File handler
    file_handler = logging.FileHandler(
        os.path.join(log_dir, "cek_scraper.log"), encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = KyivFormatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def create_session_with_retries(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: tuple = (500, 502, 503, 504, 429),
) -> requests.Session:
    """Create HTTP session with retry logic and timeout"""
    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "HEAD"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # User-Agent актуальний станом на 2025-12, за потреби оновлюйте версію Chrome
    # щоб уникнути блокувань зі сторони Telegram/веб-серверів.
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        }
    )

    return session


def validate_json_structure(data: dict, logger: logging.Logger) -> bool:
    """Validate JSON structure before saving.

    Args:
        data: JSON data for validation
        logger: Logger for output messages; validation errors and warnings are
            logged here with details.

    Returns:
        True if structure is valid, False otherwise.
    """
    try:
        # Check required fields
        required_fields = [
            "regionId",
            "lastUpdated",
            "fact",
            "preset",
            "lastUpdateStatus",
            "meta",
        ]
        for field in required_fields:
            if field not in data:
                logger.error(f"Missing required field: {field}")
                return False

        # Check fact structure
        if "data" not in data["fact"]:
            logger.error("Missing 'data' in fact section")
            return False

        # Check that there is at least one date with data
        if not data["fact"]["data"]:
            logger.warning("No schedule data in fact section")
            # This is a warning, but not a critical error

        # Check structure of each date
        for timestamp, date_data in data["fact"]["data"].items():
            if not isinstance(date_data, dict):
                logger.error(f"Invalid data structure for timestamp {timestamp}")
                return False

            # Check that there is at least one queue
            if not date_data:
                logger.warning(f"No queue data for timestamp {timestamp}")
                continue

            # Check queue structure
            for queue_key, queue_data in date_data.items():
                if not queue_key.startswith("GPV"):
                    logger.error(f"Invalid queue key format: {queue_key}")
                    return False

                if not isinstance(queue_data, dict):
                    logger.error(f"Invalid queue data structure for {queue_key}")
                    return False

                # Check that data exists for all 24 hours
                for hour in range(1, 25):
                    if str(hour) not in queue_data:
                        logger.error(f"Missing hour {hour} in queue {queue_key}")
                        return False

                    # Check valid values
                    valid_values = [
                        "yes",
                        "no",
                        "first",
                        "second",
                        "maybe",
                        "mfirst",
                        "msecond",
                    ]
                    if queue_data[str(hour)] not in valid_values:
                        logger.error(
                            f"Invalid value '{queue_data[str(hour)]}' for hour {hour} in queue {queue_key}"
                        )
                        return False

        logger.debug("JSON structure validation passed")
        return True

    except Exception as e:
        logger.error(f"JSON validation error: {e}", exc_info=True)
        return False


class PowercutScraper:
    # Threshold for determining if a message contains a "full" schedule vs partial/modification
    # Dnipro region has 12 queues (1.1, 1.2, 2.1, 2.2, 3.1, 3.2, 4.1, 4.2, 5.1, 5.2, 6.1, 6.2)
    # Messages with >= 6 queues (50%) are considered "full schedules"
    # Messages with < 6 queues are likely modification-only messages (early start, prolongation, etc.)
    # Adjust this value if the number of queues in the region changes
    # FULL_SCHEDULE_THRESHOLD = 6

    # Порогове значення для визначення повного розкладу (12 черг)
    FULL_SCHEDULE_THRESHOLD = 10

    def __init__(
        self,
        channel_url: str,
        region_id: str = "dnipro",
        start_date: str = None,
        end_date: str = None,
        logger: Optional[logging.Logger] = None,
        session: Optional[requests.Session] = None,
    ):
        self.url = channel_url
        self.region_id = region_id
        self.start_date = start_date
        self.end_date = end_date
        self.logger = logger or logging.getLogger("cek_scraper")
        self.session = session or create_session_with_retries()

        self.months_uk = {
            "січня": "01",
            "лютого": "02",
            "березня": "03",
            "квітня": "04",
            "травня": "05",
            "червня": "06",
            "липня": "07",
            "серпня": "08",
            "вересня": "09",
            "жовтня": "10",
            "листопада": "11",
            "грудня": "12",
        }
        self.current_year = datetime.now().year

        # Set default date range to today and tomorrow if not specified
        kyiv_tz = ZoneInfo("Europe/Kyiv")
        today = datetime.now(kyiv_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)
        if not start_date:
            self.start_date = today.strftime("%d.%m.%Y")
            self.logger.info(f"Using default start_date: {self.start_date} (today)")
        else:
            self.start_date = start_date
        if not end_date:
            self.end_date = tomorrow.strftime("%d.%m.%Y")
            self.logger.info(f"Using default end_date: {self.end_date} (tomorrow)")
        else:
            self.end_date = end_date

    def is_partial_schedule_update(self, message: str) -> bool:
        """Виявити, чи є повідомлення частковим оновленням графіка (тільки для певного діапазону часу)

        Detect if a message is a partial schedule update (only for specific time range)
        Examples:
        - "зміни в ГПВ на 15 січня з 19:00 до 24:00"
        - "зміни на 15 січня з 19:00"
        """
        partial_patterns = [
            r"зміни.*на\s+\d+\s+[а-яА-ЯіІїЇєЄґҐ]+\s+з\s+\d{2}:\d{2}",
            r"зміни.*ГПВ.*з\s+\d{2}:\d{2}",
            r"повідомляємо\s+про\s+зміни.*з\s+\d{2}:\d{2}",
        ]

        for pattern in partial_patterns:
            if re.search(pattern, message, re.IGNORECASE):
                return True
        return False

    def extract_time_range_from_header(self, message: str) -> Optional[tuple]:
        """Витягнути діапазон часу з заголовка повідомлення, якщо це часткове оновлення

        Extract time range from message header if it's a partial update
        Returns: (start_time, end_time) tuple or None
        Example: ('19:00', '24:00')
        """
        # Шукаємо патерн "з HH:MM до HH:MM" в перших 200 символах (область заголовка)
        # Pattern: "з 19:00 до 24:00" or "з 19:00 по 24:00"
        header = message[:200]
        pattern = r"з\s+(\d{2}:\d{2})\s+(?:до|по)\s+(\d{2}:\d{2})"
        match = re.search(pattern, header, re.IGNORECASE)

        if match:
            return (match.group(1), match.group(2))
        return None

    def cleanup_old_data(self, data: dict) -> dict:
        """Remove data for dates before today (Kyiv timezone)

        Args:
            data: The JSON data structure to clean up

        Returns:
            Cleaned data with only today and future dates
        """
        try:
            # Get current time in Kyiv timezone
            kyiv_tz = ZoneInfo("Europe/Kyiv")
            now_kyiv = datetime.now(kyiv_tz)
            today_midnight_kyiv = now_kyiv.replace(
                hour=0, minute=0, second=0, microsecond=0
            )

            # Convert to Unix timestamp for comparison
            today_timestamp = int(today_midnight_kyiv.timestamp())

            self.logger.info(
                f"Cleanup: Current Kyiv time: {now_kyiv.strftime('%Y-%m-%d %H:%M:%S %Z')}"
            )
            self.logger.debug(f"Cleanup: Today midnight timestamp: {today_timestamp}")

            # Get all timestamps from the data
            if "fact" not in data or "data" not in data["fact"]:
                self.logger.warning("Cleanup: No fact data found in JSON")
                return data

            old_timestamps = list(data["fact"]["data"].keys())

            # Remove timestamps older than today
            removed_count = 0
            for timestamp_str in old_timestamps:
                timestamp = int(timestamp_str)
                if timestamp < today_timestamp:
                    del data["fact"]["data"][timestamp_str]
                    removed_count += 1
                    # Convert timestamp to readable date
                    date_obj = datetime.fromtimestamp(timestamp, tz=kyiv_tz)
                    self.logger.debug(
                        f"Cleanup: Removed old data for {date_obj.strftime('%Y-%m-%d')}"
                    )

            if removed_count == 0:
                self.logger.info("Cleanup: No old data to remove")
            else:
                self.logger.info(f"Cleanup: Removed {removed_count} old date(s)")

            # Update the today field
            data["fact"]["today"] = today_timestamp

            return data

        except Exception as e:
            self.logger.warning(f"Cleanup failed: {e}")
            return data

    def scrape_messages(self) -> Dict[str, Dict[float, List[str]]]:
        """Scrape messages from Telegram channel and extract schedules for ALL queues

        Returns:
            Dict with dates as keys, and dict of queue_number -> time_slots as values
            Example: {"05.12.2025": {1.1: ["07:00-10:00"], 3.1: ["14:00-18:00"]}}
        """
        self.logger.info(f"Fetching data from {self.url}")

        try:
            # Fetch the webpage with timeout
            response = self.session.get(self.url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout while fetching {self.url}")
            raise Exception(f"Request timeout after 30 seconds")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch webpage: {e}")
            raise Exception(f"Failed to fetch webpage: {e}")

        content = response.content
        self.logger.debug(f"Received {len(content)} bytes from {self.url}")

        # Parse the webpage content
        soup = BeautifulSoup(content, "html.parser")

        # Find all message widgets (full message containers, not just text)
        message_widgets = soup.find_all("div", class_="tgme_widget_message")

        # Get today's date in Kyiv timezone
        kyiv_tz = ZoneInfo("Europe/Kyiv")
        today = datetime.now(kyiv_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        today_str = today.strftime("%d.%m.%Y")

        # Collect all schedules and modifications from all messages
        # Store messages with their timestamps for prioritization of the latest ones
        messages_by_date = {}
        modifications_by_date = {}

        for message_widget in message_widgets:
            # Extract message timestamp from datetime attribute
            message_timestamp = self.extract_message_timestamp(message_widget)

            # Get message text
            message_text_div = message_widget.find(
                "div", class_="tgme_widget_message_text"
            )
            if not message_text_div:
                continue

            message_text = message_text_div.get_text()

            # Extract date from message - if not found, use message timestamp date
            date = self.extract_date(message_text)

            # If no date found, use message timestamp date
            if not date and message_timestamp:
                date = message_timestamp.strftime("%d.%m.%Y")
            elif not date:
                # Fallback to today if no timestamp
                date = today_str

            if date:
                date_obj = datetime.strptime(date, "%d.%m.%Y")
                # Make date_obj timezone-aware
                date_obj = date_obj.replace(tzinfo=kyiv_tz)

                # Filter by time frame if specified
                if self.start_date or self.end_date:
                    start_filter = (
                        datetime.strptime(self.start_date, "%d.%m.%Y").replace(
                            tzinfo=kyiv_tz
                        )
                        if self.start_date
                        else None
                    )
                    end_filter = (
                        datetime.strptime(self.end_date, "%d.%m.%Y").replace(
                            tzinfo=kyiv_tz
                        )
                        if self.end_date
                        else None
                    )
                    if (start_filter and date_obj < start_filter) or (
                        end_filter and date_obj >= end_filter + timedelta(days=1)
                    ):
                        continue  # Skip this date

                # Only process today or future dates
                if date_obj >= today:
                    self.logger.debug(
                        f'Processing message posted at: {message_timestamp.strftime("%Y-%m-%d %H:%M:%S %Z") if message_timestamp else "Unknown time"} for date {date}'
                    )

                    # Перевірка, чи є це частковим оновленням графіка
                    # Check if this is a partial schedule update (e.g., changes for 19:00-24:00)
                    is_partial = self.is_partial_schedule_update(message_text)
                    time_range = (
                        self.extract_time_range_from_header(message_text)
                        if is_partial
                        else None
                    )

                    # Extract schedules for ALL queues
                    schedules_by_queue = self.extract_all_schedules(message_text)

                    if schedules_by_queue:
                        # Store message with timestamp for later processing
                        # Use timestamp or epoch 0 if timestamp is missing
                        msg_time = (
                            message_timestamp.timestamp() if message_timestamp else 0
                        )

                        if date not in messages_by_date:
                            messages_by_date[date] = []

                        # Помітити повідомлення як часткове оновлення, якщо це так
                        # Mark message as partial update if applicable
                        messages_by_date[date].append(
                            {
                                "timestamp": msg_time,
                                "schedules": schedules_by_queue,
                                "is_partial": is_partial,
                                "time_range": time_range,
                            }
                        )

                    # Extract modifications with timestamp
                    modifications = self.extract_modifications(message_text)
                    if modifications:
                        msg_time = (
                            message_timestamp.timestamp() if message_timestamp else 0
                        )
                        if date not in modifications_by_date:
                            modifications_by_date[date] = []
                        # Store modifications with their message timestamp
                        for mod in modifications:
                            modifications_by_date[date].append(
                                (
                                    mod[0],
                                    mod[1],
                                    mod[2],
                                    msg_time,
                                )  # queue, type, time, msg_timestamp
                            )

        # Process messages: for each date select the best message
        # Strategy: use the last message published BEFORE the start of the schedule day
        # This ensures full schedule for the entire day (00:00-24:00)
        # Messages published during the day only show remaining outages
        all_schedules = {}
        base_schedule_timestamps = (
            {}
        )  # Track when the base schedule was posted for each date

        for date, messages in messages_by_date.items():
            # Sort messages by timestamp (latest last)
            messages.sort(key=lambda x: x["timestamp"])

            # Determine the start of the schedule day (00:00 in Kyiv timezone)
            date_obj = datetime.strptime(date, "%d.%m.%Y").replace(
                tzinfo=kyiv_tz, hour=0, minute=0, second=0, microsecond=0
            )
            day_start_timestamp = date_obj.timestamp()

            # Нова стратегія: розділяємо повідомлення на базові графіки та часткові оновлення
            # New strategy: separate messages into base schedules and partial updates
            # 1. Find the latest FULL schedule (not marked as partial)
            # 2. Collect all PARTIAL updates posted after the base schedule
            # 3. Merge them intelligently

            base_schedule_message = None
            partial_updates = []

            # Separate full schedules from partial updates
            for msg in reversed(messages):
                is_full = len(msg["schedules"]) >= self.FULL_SCHEDULE_THRESHOLD
                is_partial = msg.get("is_partial", False)

                if is_full and not is_partial:
                    # This is a full schedule - use as base if we don't have one yet
                    if base_schedule_message is None:
                        base_schedule_message = msg
                        msg_dt = datetime.fromtimestamp(msg["timestamp"], tz=kyiv_tz)
                        self.logger.debug(
                            f"Found base schedule: {msg_dt.strftime('%Y-%m-%d %H:%M')}, "
                            f"{len(msg['schedules'])} queues"
                        )
                elif is_partial:
                    # This is a partial update - collect it
                    partial_updates.append(msg)
                    msg_dt = datetime.fromtimestamp(msg["timestamp"], tz=kyiv_tz)
                    time_range = msg.get("time_range", "unknown")
                    self.logger.debug(
                        f"Found partial update: {msg_dt.strftime('%Y-%m-%d %H:%M')}, "
                        f"time range: {time_range}, {len(msg['schedules'])} queues"
                    )

            # Fallback: if no base schedule found, use the latest message as base
            fallback_message = messages[-1] if messages else None

            # Process base schedule and apply partial updates
            if base_schedule_message:
                message_to_use = base_schedule_message
                msg_time = datetime.fromtimestamp(
                    message_to_use["timestamp"], tz=kyiv_tz
                )
                queue_count = len(message_to_use["schedules"])
                msg_type = "base schedule"

                # Start with the base schedule
                merged_schedules = {}
                for queue_num, time_slots in base_schedule_message["schedules"].items():
                    merged_schedules[queue_num] = time_slots.copy()

                # Apply partial updates in chronological order (oldest first)
                partial_updates.reverse()  # They were collected in reverse order
                for partial_msg in partial_updates:
                    # Only apply updates that were posted after the base schedule
                    if partial_msg["timestamp"] > base_schedule_message["timestamp"]:
                        time_range = partial_msg.get("time_range")
                        if time_range:
                            start_time_str, end_time_str = time_range
                            self.logger.info(
                                f"Applying partial update for time range {start_time_str}-{end_time_str}"
                            )

                            # For each queue in the partial update
                            for queue_num, update_slots in partial_msg[
                                "schedules"
                            ].items():
                                if queue_num not in merged_schedules:
                                    merged_schedules[queue_num] = []

                                # Remove slots from base schedule that overlap with the update time range
                                # Smart logic: split slots if they partially overlap
                                filtered_slots = []
                                # Handle 24:00 edge case
                                update_start_str = start_time_str
                                update_end_str = (
                                    end_time_str if end_time_str != "24:00" else "23:59"
                                )
                                update_start = datetime.strptime(
                                    f"{date} {update_start_str}", "%d.%m.%Y %H:%M"
                                )
                                update_end = datetime.strptime(
                                    f"{date} {update_end_str}", "%d.%m.%Y %H:%M"
                                )

                                for slot in merged_schedules[queue_num]:
                                    if slot.startswith("MOD:"):
                                        filtered_slots.append(slot)
                                        continue

                                    slot_start_str, slot_end_str = slot.split("-")
                                    if slot_end_str == "24:00":
                                        slot_end_str = "23:59"
                                    slot_start = datetime.strptime(
                                        f"{date} {slot_start_str}", "%d.%m.%Y %H:%M"
                                    )
                                    slot_end = datetime.strptime(
                                        f"{date} {slot_end_str}", "%d.%m.%Y %H:%M"
                                    )

                                    # Check for overlap: slots overlap if start < other_end AND end > other_start
                                    overlaps = (
                                        slot_start < update_end
                                        and slot_end > update_start
                                    )

                                    if not overlaps:
                                        # No overlap - keep the entire slot
                                        filtered_slots.append(slot)
                                    else:
                                        # Partial or full overlap - need to handle carefully
                                        # Case 1: Slot completely within update range - skip it
                                        if (
                                            slot_start >= update_start
                                            and slot_end <= update_end
                                        ):
                                            self.logger.debug(
                                                f"  Removing slot {slot} for queue {queue_num} (fully within update range)"
                                            )
                                            continue

                                        # Case 2: Slot partially overlaps at the end - keep the part before update
                                        if (
                                            slot_start < update_start
                                            and slot_end > update_start
                                        ):
                                            # Keep the part from slot_start to update_start
                                            new_slot = (
                                                f"{slot_start_str}-{update_start_str}"
                                            )
                                            filtered_slots.append(new_slot)
                                            self.logger.debug(
                                                f"  Trimming slot {slot} to {new_slot} for queue {queue_num}"
                                            )

                                        # Case 3: Slot partially overlaps at the beginning - keep the part after update
                                        if (
                                            slot_start < update_end
                                            and slot_end > update_end
                                        ):
                                            # Keep the part from update_end to slot_end
                                            new_end_str = slot.split("-")[
                                                1
                                            ]  # Preserve original format (24:00 if it was 24:00)
                                            new_slot = f"{update_end_str}-{new_end_str}"
                                            filtered_slots.append(new_slot)
                                            self.logger.debug(
                                                f"  Trimming slot {slot} to {new_slot} for queue {queue_num}"
                                            )

                                        # Case 4: Update range completely within slot - split into two parts
                                        # This shouldn't normally happen with our data, but handle it anyway
                                        if (
                                            slot_start < update_start
                                            and slot_end > update_end
                                        ):
                                            new_slot_1 = (
                                                f"{slot_start_str}-{update_start_str}"
                                            )
                                            new_slot_2 = (
                                                f"{update_end_str}-{slot.split('-')[1]}"
                                            )
                                            filtered_slots.append(new_slot_1)
                                            filtered_slots.append(new_slot_2)
                                            self.logger.debug(
                                                f"  Splitting slot {slot} into {new_slot_1} and {new_slot_2} for queue {queue_num}"
                                            )

                                # Add the new slots from the partial update
                                merged_schedules[queue_num] = (
                                    filtered_slots + update_slots
                                )
                        else:
                            # No time range specified - apply to entire day
                            self.logger.warning(
                                f"Partial update without time range - applying to full day"
                            )
                            for queue_num, update_slots in partial_msg[
                                "schedules"
                            ].items():
                                merged_schedules[queue_num] = update_slots

                # Update the message to use with merged schedules
                message_to_use = {
                    "timestamp": base_schedule_message["timestamp"],
                    "schedules": merged_schedules,
                }
                msg_type = (
                    f"base schedule with {len(partial_updates)} partial update(s)"
                )
                queue_count = len(merged_schedules)

            elif fallback_message:
                message_to_use = fallback_message
                msg_time = datetime.fromtimestamp(
                    message_to_use["timestamp"], tz=kyiv_tz
                )
                queue_count = len(message_to_use["schedules"])
                msg_type = "fallback message"
            else:
                # No messages at all for this date
                continue

            # Log the selected message
            msg_time = datetime.fromtimestamp(message_to_use["timestamp"], tz=kyiv_tz)
            self.logger.info(
                f"Using {msg_type} for {date} "
                f"(posted: {msg_time.strftime('%Y-%m-%d %H:%M')}, {queue_count} queues)"
            )

            # Store the timestamp of the base schedule
            base_schedule_timestamps[date] = message_to_use["timestamp"]

            # Store schedules
            all_schedules[date] = {}
            for queue_num, time_slots in message_to_use["schedules"].items():
                all_schedules[date][queue_num] = time_slots

        # Combine time slots for each queue in each date
        for date, queues_schedules in all_schedules.items():
            for queue_num, time_slots in queues_schedules.items():
                if time_slots:
                    combined = self.combine_time_slots(date, time_slots)
                    all_schedules[date][queue_num] = combined
                    self.logger.debug(
                        f"Combined schedules for {date}, Queue {queue_num}: {combined}"
                    )

        # Transform modifications from List[tuple] to Dict structure expected by apply_modifications
        # From: {date: [(queue_num, mod_type, mod_time, msg_timestamp), ...]}
        # To: {date: {queue_num: ["MOD:mod_type:mod_time", ...], ...}}
        # IMPORTANT: Only apply modifications that were posted AFTER the base schedule message
        # This prevents applying old modifications to newer full schedule replacements
        transformed_mods = {}
        for date, mod_list in modifications_by_date.items():
            base_timestamp = base_schedule_timestamps.get(date, 0)
            transformed_mods[date] = {}
            filtered_count = 0
            applied_count = 0

            for queue_num, mod_type, mod_time, msg_timestamp in mod_list:
                # Only apply modification if it was posted AFTER the base schedule
                if msg_timestamp > base_timestamp:
                    if queue_num not in transformed_mods[date]:
                        transformed_mods[date][queue_num] = set()
                    transformed_mods[date][queue_num].add(f"MOD:{mod_type}:{mod_time}")
                    applied_count += 1
                else:
                    filtered_count += 1
                    self.logger.debug(
                        f"Skipping outdated {mod_type} modification for queue {queue_num} on {date} "
                        f"(mod posted at {msg_timestamp}, base schedule at {base_timestamp})"
                    )

            if filtered_count > 0:
                self.logger.info(
                    f"Filtered {filtered_count} outdated modification(s) for {date}, "
                    f"applied {applied_count} newer modification(s)"
                )

        # Convert sets back to lists (stable sorted order)
        for date, queues_mods in transformed_mods.items():
            for queue_num, mods_set in queues_mods.items():
                transformed_mods[date][queue_num] = sorted(mods_set)
        # Apply modifications to schedules
        self.apply_modifications(all_schedules, transformed_mods)

        return all_schedules

    def extract_message_timestamp(self, message_widget) -> Optional[datetime]:
        """Extract the timestamp when a message was posted

        Args:
            message_widget: BeautifulSoup element for the message widget

        Returns:
            datetime object in Kyiv timezone, or None if not found
        """
        try:
            # Look for the time element using the same selector as Home Assistant
            # CSS selector: .tgme_widget_message_meta a.tgme_widget_message_date time.time
            meta_div = message_widget.find(class_="tgme_widget_message_meta")
            if meta_div:
                date_link = meta_div.find("a", class_="tgme_widget_message_date")
                if date_link:
                    time_tag = date_link.find(
                        "time", class_="time", attrs={"datetime": True}
                    )
                    if time_tag:
                        datetime_str = time_tag["datetime"]
                        # Parse ISO format datetime (e.g., "2025-12-08T10:30:00+02:00")
                        msg_datetime = datetime.fromisoformat(datetime_str)

                        # Convert to Kyiv timezone
                        kyiv_tz = ZoneInfo("Europe/Kyiv")
                        msg_datetime_kyiv = msg_datetime.astimezone(kyiv_tz)

                        return msg_datetime_kyiv
        except Exception as e:
            self.logger.warning(f"Could not extract message timestamp: {e}")

        return None

    def is_message_from_today(self, message_timestamp: datetime, kyiv_tz) -> bool:
        """Check if a message timestamp is from today (Kyiv timezone)

        Args:
            message_timestamp: datetime object of the message
            kyiv_tz: ZoneInfo for Kyiv timezone

        Returns:
            True if message is from today, False otherwise
        """
        today = datetime.now(kyiv_tz).replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)

        return today <= message_timestamp < tomorrow

    def apply_modifications(
        self,
        schedules: Dict[str, Dict[float, List[str]]],
        modifications: Dict[str, Dict[float, List[str]]],
    ):
        """Apply schedule modifications (prolongations, early starts, additional outages) to existing schedules

        Args:
            schedules: The main schedules dict to modify
            modifications: Dict of modifications to apply
        """
        for date, queue_mods in modifications.items():
            if date not in schedules:
                schedules[date] = {}

            for queue_num, mod_list in queue_mods.items():
                for mod in mod_list:
                    # Parse modification: "MOD:prolong:13:00" or "MOD:early_start:11:00" or "MOD:additional:06:00-09:00"
                    parts = mod.split(":", 2)  # Split into max 3 parts
                    if len(parts) >= 3:
                        mod_type = parts[1]
                        mod_data = parts[2]

                        if mod_type == "additional":
                            # Additional outage is a new time slot to add
                            if queue_num not in schedules[date]:
                                schedules[date][queue_num] = []
                            schedules[date][queue_num].append(mod_data)
                            # Recombine after adding
                            schedules[date][queue_num] = self.combine_time_slots(
                                date, schedules[date][queue_num]
                            )
                            self.logger.info(
                                f"Added additional outage {mod_data} to queue {queue_num} on {date}"
                            )

                        elif queue_num in schedules[date]:
                            existing_slots = schedules[date][queue_num]
                            modified_slots = self.modify_time_slots(
                                existing_slots, mod_type, mod_data, date, queue_num
                            )
                            schedules[date][queue_num] = modified_slots
                            self.logger.info(
                                f"Applied {mod_type} modification to queue {queue_num} on {date}: {modified_slots}"
                            )

                        elif mod_type == "cancel":
                            # Remove the queue if cancelled
                            if queue_num in schedules[date]:
                                del schedules[date][queue_num]
                                self.logger.info(
                                    f"Cancelled schedule for queue {queue_num} on {date}"
                                )

    def modify_time_slots(
        self,
        time_slots: List[str],
        mod_type: str,
        mod_time: str,
        date: str,
        queue_num: float = None,
    ) -> List[str]:
        """Modify time slots based on modification type

        Args:
            time_slots: Existing time slots like ["07:00-10:00", "14:00-18:00"]
            mod_type: 'prolong' or 'early_start'
            mod_time: Time for modification like "13:00"
            date: Date string for parsing
            queue_num: Queue number for logging (e.g., 2.1)

        Returns:
            Modified list of time slots
        """
        if not time_slots:
            return time_slots

        current_time = datetime.now()
        queue_str = f" for queue {queue_num}" if queue_num else ""

        if mod_type == "prolong":
            # Find which slot to prolong
            # Strategy: Find the slot that is currently ongoing or the last slot that ended most recently
            slot_to_modify = None
            slot_index = None

            for idx, slot in enumerate(time_slots):
                start_time_str, end_time_str = slot.split("-")
                try:
                    slot_start = datetime.strptime(
                        f"{date} {start_time_str}", "%d.%m.%Y %H:%M"
                    )
                    slot_end = datetime.strptime(
                        f"{date} {end_time_str}", "%d.%m.%Y %H:%M"
                    )

                    # Check if this slot is currently ongoing
                    if slot_start <= current_time <= slot_end:
                        slot_to_modify = idx
                        break

                    # Check if this slot just ended (within last 2 hours) - might be getting extended
                    elif (
                        current_time > slot_end
                        and (current_time - slot_end).total_seconds() < 7200
                    ):
                        slot_to_modify = idx
                except (ValueError, TypeError, KeyError) as e:
                    self.logger.warning(
                        f"Failed to parse time slot '{slot}' for date '{date}': {e}"
                    )
                    continue

            # If no specific slot found, extend the last one (default behavior)
            # This fallback is used when no slot is currently ongoing or recently ended
            if slot_to_modify is None:
                self.logger.info(
                    f"Prolong fallback: No ongoing or recent slot found{queue_str}. "
                    f"Extending last slot to {mod_time} as fallback."
                )
                if len(time_slots) == 0:
                    self.logger.error(
                        f"Cannot apply prolongation{queue_str}: no time slots available"
                    )
                    return time_slots  # Return unmodified
                slot_to_modify = len(time_slots) - 1

            # Modify the identified slot
            modified = time_slots.copy()
            start_time = modified[slot_to_modify].split("-")[0]
            modified[slot_to_modify] = f"{start_time}-{mod_time}"

            self.logger.debug(
                f"  Prolonging slot {slot_to_modify + 1} of {len(time_slots)}{queue_str}: {time_slots[slot_to_modify]} → {modified[slot_to_modify]}"
            )

        elif mod_type == "early_start":
            # Find which slot to start earlier
            # Strategy: Find the slot that starts closest to or after the early start time
            # For example, if early start is 07:00, and we have slots 00:00-04:00, 08:00-11:00, 18:00-20:00
            # we should modify 08:00-11:00 to become 07:00-11:00
            slot_to_modify = None
            early_start_time = datetime.strptime(f"{date} {mod_time}", "%d.%m.%Y %H:%M")

            for idx, slot in enumerate(time_slots):
                start_time_str, end_time_str = slot.split("-")
                try:
                    slot_start = datetime.strptime(
                        f"{date} {start_time_str}", "%d.%m.%Y %H:%M"
                    )

                    # Find the first slot that starts at or after the early start time
                    if slot_start >= early_start_time:
                        slot_to_modify = idx
                        break
                except (ValueError, TypeError) as e:
                    self.logger.debug(f"Failed to parse slot start time: {e}")
                    continue

            # If no specific slot found (all slots start before early start time), modify the last one
            # This is a fallback for edge cases - log a warning as this may indicate incorrect data
            if slot_to_modify is None:
                self.logger.warning(
                    f"Early start fallback: All time slots start before {mod_time}{queue_str}. "
                    f"Modifying last slot as fallback. This may indicate incorrect data or timing."
                )
                if len(time_slots) == 0:
                    self.logger.error(
                        f"Cannot apply early start{queue_str}: no time slots available"
                    )
                    return time_slots  # Return unmodified
                slot_to_modify = len(time_slots) - 1

            # Modify the identified slot
            modified = time_slots.copy()
            end_time = modified[slot_to_modify].split("-")[1]
            modified[slot_to_modify] = f"{mod_time}-{end_time}"

            self.logger.debug(
                f"  Early start for slot {slot_to_modify + 1} of {len(time_slots)}{queue_str}: {time_slots[slot_to_modify]} → {modified[slot_to_modify]}"
            )

        else:
            modified = time_slots

        # Recombine slots in case modification created overlap
        return self.combine_time_slots(date, modified)

    def extract_queue_numbers(self, text: str) -> List[float]:
        """Extract queue numbers from text as floats (only decimal numbers like 1.1, 2.1, not 1, 2, 3)"""
        # Only decimal numbers (with dot), not simple integers
        # Pattern: \b\d\.\d\b matches exactly X.Y format (single digit before and after dot)
        # with word boundaries to avoid matching fragments like 0.1 from 10.1
        return [float(q) for q in re.findall(r"\b\d\.\d\b", text)]

    def extract_all_schedules(self, message: str) -> Dict[float, List[str]]:
        """Extract schedule time slots for ALL queue numbers found in message

        Returns:
            Dict mapping queue_number to list of time slots
            Example: {1.1: ["07:00-10:00"], 3.1: ["14:00-18:00"]}
        """
        schedules_by_queue = {}

        # Pattern: "3.1 черга: з 07:00 до 10:00; з 14:00 до 18:00" or "3.1 група: з 07:00 до 10:00; з 14:00 до 18:00"
        # Only decimal numbers (with dot), not simple integers
        # Pattern: \d\.\d matches exactly X.Y format (single digit before and after dot)
        pattern = re.compile(
            r"(\d\.\d)\W*(?:черг|груп)[аиі]:\s*((?:\W+з\s\d{2}:\d{2}\s(?:по|до)\s\d{2}:\d{2};?)+)",
            re.IGNORECASE,
        )
        schedules_pattern = re.compile(r"з\s(\d{2}:\d{2})\s(?:по|до)\s(\d{2}:\d{2})")

        for match in pattern.finditer(message):
            queue_info = match.group(1)
            try:
                queue_number = float(queue_info)
            except ValueError:
                continue

            if queue_number not in schedules_by_queue:
                schedules_by_queue[queue_number] = []

            schedules_info = schedules_pattern.finditer(match.group(2))
            for schedule in schedules_info:
                start_time = schedule.group(1)
                end_time = schedule.group(2)
                # Normalize end time: if end is 00:00 and start is not 00:00, convert to 24:00
                if end_time == "00:00" and start_time != "00:00":
                    end_time = "24:00"
                    self.logger.debug(f"Normalized time range {start_time}-00:00 to {start_time}-24:00 for queue {queue_number}")
                schedules_by_queue[queue_number].append(f"{start_time}-{end_time}")

        # Old style schedule matching: "з 07:00 до 10:00 відключається 1, 2, 3.1 черги" or "з 07:00 до 10:00 відключається 1, 2, 3.1 групи"
        # (from 07:00 to 10:00 is disconnected queues 1, 2, 3.1)
        # or "з 07:00 по 10:00 відключається 1, 2, 3.1 черги" or "з 07:00 по 10:00 відключається 1, 2, 3.1 групи"
        old_pattern = re.compile(
            r"з\s(\d{2}:\d{2})\s(?:по|до)\s(\d{2}:\d{2});?\sвідключа[ює]ться*([0-9\sта,.;:!?]*(?:черг|груп)[аиі])+",
            re.IGNORECASE,
        )
        matches = old_pattern.findall(message)

        if matches:
            for match in matches:
                start_time = match[0]
                end_time = match[1]
                # Normalize end time: if end is 00:00 and start is not 00:00, convert to 24:00
                if end_time == "00:00" and start_time != "00:00":
                    end_time = "24:00"
                    self.logger.debug(f"Normalized time range {start_time}-00:00 to {start_time}-24:00")
                time_range = f"{start_time}-{end_time}"
                queue_info = match[2]

                # Extract all numbers (including decimals) from queue_info
                queue_numbers = self.extract_queue_numbers(queue_info)

                for queue_number in queue_numbers:
                    if queue_number not in schedules_by_queue:
                        schedules_by_queue[queue_number] = []
                    schedules_by_queue[queue_number].append(time_range)

        # New style schedule matching for today's format: "📌 1.1 з 15:00 по 22:00"
        # (📌 1.1 from 15:00 to 22:00)
        # Find all queue blocks (only decimal numbers like 1.1, 2.1, not integers)
        # Pattern: \d\.\d matches exactly X.Y format (single digit before and after dot)
        queue_blocks = re.findall(
            r"📌\s*(\d\.\d)(.*?)(?=📌|$)",
            message,
            re.IGNORECASE | re.DOTALL,
        )
        for queue_info, block in queue_blocks:
            try:
                queue_number = float(queue_info)
                if queue_number not in schedules_by_queue:
                    schedules_by_queue[queue_number] = []
                # Find all time slots in this block
                time_slots = re.findall(
                    r"з\s(\d{2}:\d{2})\s(?:по|до)\s(\d{2}:\d{2})", block, re.IGNORECASE
                )
                for start_time, end_time in time_slots:
                    # Normalize end time: if end is 00:00 and start is not 00:00, convert to 24:00
                    if end_time == "00:00" and start_time != "00:00":
                        end_time = "24:00"
                        self.logger.debug(f"Normalized time range {start_time}-00:00 to {start_time}-24:00 for queue {queue_number}")
                    schedules_by_queue[queue_number].append(f"{start_time}-{end_time}")
            except ValueError:
                continue

        # "Черга X.X" or "Група X.X" format: Works with any emoji or bullet (🔹, 📌, -, •, etc.)
        # Example: "🔹 Черга 1.1\n10:00-12:00\n14:00-20:00" or "🔹 Група 1.1\n10:00-12:00\n14:00-20:00"
        # Pattern matches "Черга" or "Група" keyword followed by queue number, regardless of prefix emoji
        # Matches until next "Черга"/"Група", "Попереджаємо", or end of message
        # Pattern: \d\.\d matches exactly X.Y format (word boundaries removed to handle HTML without spaces)
        cherga_queue_blocks = re.findall(
            r"(?:Черга|Група)\s*(\d\.\d)(.*?)(?=(?:Черга|Група)|Попереджаємо|$)",
            message,
            re.IGNORECASE | re.DOTALL,
        )
        for queue_info, block in cherga_queue_blocks:
            try:
                queue_number = float(queue_info)
                if queue_number not in schedules_by_queue:
                    schedules_by_queue[queue_number] = []
                # Find all time slots in format HH:MM-HH:MM (without "з" and "до" keywords)
                time_slots = re.findall(r"(\d{2}:\d{2})-(\d{2}:\d{2})", block)
                for start_time, end_time in time_slots:
                    # Normalize end time: if end is 00:00 and start is not 00:00, convert to 24:00
                    # This handles cases like 20:00-00:00 which means 20:00-24:00 (until midnight)
                    if end_time == "00:00" and start_time != "00:00":
                        end_time = "24:00"
                        self.logger.debug(f"Normalized time range {start_time}-00:00 to {start_time}-24:00 for queue {queue_number}")
                    schedules_by_queue[queue_number].append(f"{start_time}-{end_time}")
            except ValueError:
                continue

        # Видалити дублікати з кожної черги / Remove duplicates from each queue
        for queue_num in schedules_by_queue:
            # Use dict.fromkeys() to preserve order while removing duplicates
            schedules_by_queue[queue_num] = list(
                dict.fromkeys(schedules_by_queue[queue_num])
            )

        return schedules_by_queue

    def extract_modifications(self, message: str) -> List[tuple]:
        """Extract schedule modifications (prolongations, early starts, etc.)

        Returns:
            List of tuples: (queue_number, modification_type, time)
            modification_type: 'prolong', 'early_start', 'cancel'
        """
        modifications = []
        message_lower = message.lower()

        # Pattern for prolongation variant 1: "до 13:00 подовжено відключення підчерги 2.1" or "до 13:00 подовжено відключення підгрупи 2.1"
        prolong_pattern = re.compile(
            r"до\s(\d{2}:\d{2})\sподовжен[оа]\s+(?:відключення\s+)?(?:під)?(?:черг|груп)[аиуіоє]?\s+([\d.,\s]+)",
            re.IGNORECASE,
        )
        for match in prolong_pattern.finditer(message):
            time = match.group(1)
            queues_str = match.group(2)
            # Extract all queue numbers from the string
            queue_numbers = self.extract_queue_numbers(queues_str)
            for queue in queue_numbers:
                modifications.append((queue, "prolong", time))
                self.logger.info(f"Found prolongation for queue {queue} until {time}")

        # Pattern for prolongation variant 2: "подовжується відключення підчерг 1.2 та 2.2 до 22:00" or "подовжується відключення підгруп 1.2 та 2.2 до 22:00"
        # This handles the format where queues are listed first, then the time
        prolong_pattern_2 = re.compile(
            r"подовжується\s+(?:відключення\s+)?(?:під)?(?:черг|груп)[аиуіоє]?\s+([\d.,\sіта]+?)\s+до\s+(\d{2}:\d{2})",
            re.IGNORECASE,
        )
        for match in prolong_pattern_2.finditer(message):
            queues_str = match.group(1)
            time = match.group(2)
            # Extract all queue numbers from the string (handles "1.2 та 2.2" which means "1.2 and 2.2")
            queue_numbers = self.extract_queue_numbers(queues_str)
            for queue in queue_numbers:
                modifications.append((queue, "prolong", time))
                self.logger.info(f"Found prolongation for queue {queue} until {time}")

        # Pattern for prolongation variant 3: Continuation after comma ", підчерг 3.1 та 3.2 до 24:00"
        # This handles messages like: "подовжується відключення підчерг 1.2 та 2.2 до 22:00, підчерг 3.1 та 3.2 до 24:00"
        # where the second part doesn't repeat the verb
        prolong_pattern_3 = re.compile(
            r",\s*(?:під)?(?:черг|груп)[аиуіоє]?\s+([\d.,\sіта]+?)\s+до\s+(\d{2}:\d{2})",
            re.IGNORECASE,
        )
        for match in prolong_pattern_3.finditer(message):
            # Only process if this appears in a prolongation context (check if "подовжується" is in the message)
            if "подовжується" in message.lower() or "подовжено" in message.lower():
                queues_str = match.group(1)
                time = match.group(2)
                # Extract all queue numbers from the string
                queue_numbers = self.extract_queue_numbers(queues_str)
                for queue in queue_numbers:
                    modifications.append((queue, "prolong", time))
                    self.logger.info(f"Found prolongation (continuation) for queue {queue} until {time}")

        # Pattern for additional outage: handles multiple word orders
        # Pattern 1: "з 06:00 до 09:00 додатково застосовуватиметься відключення підчерг 1.1, 1.2 та 3.1" or "з 06:00 до 09:00 додатково застосовуватиметься відключення підгруп 1.1, 1.2 та 3.1"
        # Pattern 2: "відключення підчерг 2.2 додатково застосовуватимується з 00:00 до 05:30" or "відключення підгруп 2.2 додатково застосовуватимується з 00:00 до 05:30"
        # (from 06:00 to 09:00 additionally applied outage of subqueues 1.1, 1.2 and 3.1)
        # Supports verb forms: застосовуватиметься, застосовується, застосовуватимуться, застосовуватимується, застосовуватимється

        # Pattern 1: Time first
        additional_pattern_1 = re.compile(
            r"з\s(\d{2}:\d{2})(?:\s(?:по|до)\s(\d{2}:\d{2}))?\s+додатково\s+(?:застосовуватиметься|застосовується|застосовуватимуться|застосовуватимується|застосовуватимється)\s+(?:відключення\s+)?(?:під)?(?:черг|груп)[аиуіоє]?\s+([\d.,\sіта]+)",
            re.IGNORECASE,
        )

        for match in additional_pattern_1.finditer(message):
            start_time = match.group(1)
            end_time = match.group(2) if match.group(2) else None
            queues_str = match.group(3)

            # Extract all queue numbers from the string (handles "1.1, 1.2 та 3.1" which means "1.1, 1.2 and 3.1")
            queue_numbers = self.extract_queue_numbers(queues_str)

            for queue in queue_numbers:
                # If there's an end time, this is a new outage slot, not just early start
                if end_time:
                    modifications.append(
                        (queue, "additional", f"{start_time}-{end_time}")
                    )
                    self.logger.info(
                        f"Found additional outage for queue {queue} from {start_time} to {end_time}"
                    )
                else:
                    modifications.append((queue, "early_start", start_time))
                    self.logger.info(
                        f"Found early start for queue {queue} at {start_time}"
                    )

        # Pattern 2: Queue first - "відключення підчерг 2.2 додатково застосовуватимується з 00:00 до 05:30" or "відключення підгруп 2.2 додатково застосовуватимується з 00:00 до 05:30"
        # Also handles: "підчерги 3.2 з 02:00 до 05:00" or "підгрупи 3.2 з 02:00 до 05:00" (continuation after comma)
        # More flexible pattern to handle various word forms and whitespace
        additional_pattern_2 = re.compile(
            r"під(?:черг|груп)[аиуіоє]*\s+([\d.]+)\s+(?:додатково\s+)?(?:застосовуватиметься|застосовується|застосовуватимуться|застосовуватимується|застосовуватимється)?\s*з\s+(\d{2}:\d{2})(?:\s+(?:по|до)\s+(\d{2}:\d{2}))?",
            re.IGNORECASE,
        )

        for match in additional_pattern_2.finditer(message):
            queues_str = match.group(1)
            start_time = match.group(2)
            end_time = match.group(3) if match.group(3) else None

            # Extract all queue numbers from the string (handles "1.1, 1.2 та 3.1" which means "1.1, 1.2 and 3.1")
            queue_numbers = self.extract_queue_numbers(queues_str)

            for queue in queue_numbers:
                # If there's an end time, this is a new outage slot, not just early start
                if end_time:
                    modifications.append(
                        (queue, "additional", f"{start_time}-{end_time}")
                    )
                    self.logger.info(
                        f"Found additional outage for queue {queue} from {start_time} to {end_time}"
                    )
                else:
                    modifications.append((queue, "early_start", start_time))
                    self.logger.info(
                        f"Found early start for queue {queue} at {start_time}"
                    )

        # Pattern for cancellation: "скасовано відключення черги 3.1" or "скасовано відключення групи 3.1"
        cancel_pattern = re.compile(
            r"скасован[оа]\s+(?:відключення\s+)?(?:під)?(?:черг|груп)[аиуіоє]?\s+([\d.,\s]+)",
            re.IGNORECASE,
        )
        for match in cancel_pattern.finditer(message):
            queues_str = match.group(1)
            queue_numbers = self.extract_queue_numbers(queues_str)
            for queue in queue_numbers:
                modifications.append((queue, "cancel", "00:00"))
                self.logger.info(f"Found cancellation for queue {queue}")

        return modifications

    def extract_date(self, message: str) -> Optional[str]:
        """Extract date from message text

        Handles year rollover: if extracted month is before current month,
        assumes next year (e.g., "01 січня" in December → January next year)
        """
        # Pattern includes all Ukrainian letters: а-я, і, ї, є, ґ (and their uppercase versions)
        date_pattern = re.compile(
            r"(\d{1,2})(?:-го)?\s([а-яА-ЯіІїЇєЄґҐ]+)", re.IGNORECASE
        )
        match = date_pattern.search(message)
        if match:
            day = match.group(1)
            month_uk = match.group(2).lower()
            if month_uk in self.months_uk:
                month = self.months_uk[month_uk]

                # Determine the year - handle year rollover
                kyiv_tz = ZoneInfo("Europe/Kyiv")
                now_kyiv = datetime.now(kyiv_tz)
                current_month = now_kyiv.month
                extracted_month = int(month)

                # If extracted month is before current month (e.g., January when we're in December),
                # assume it's for next year
                year = self.current_year
                if extracted_month < current_month:
                    year += 1
                    self.logger.debug(
                        f"Year rollover detected: {day}.{month} interpreted as {year} "
                        f"(current month: {current_month})"
                    )

                return f"{day}.{month}.{year}"
        return None

    def combine_time_slots(self, date: str, time_slots: List[str]) -> List[str]:
        """Combine overlapping or contiguous time slots"""
        if not time_slots:
            return []

        # Filter out MOD entries (modifications) - they should be handled separately
        regular_slots = [slot for slot in time_slots if not slot.startswith("MOD:")]

        if not regular_slots:
            return []

        # Parse and sort the time slots
        slots = []
        for slot in regular_slots:
            start_time, end_time = slot.split("-")
            if end_time == "24:00":
                end_time = "23:59"
            start = datetime.strptime(f"{date} {start_time}", "%d.%m.%Y %H:%M")
            end = datetime.strptime(f"{date} {end_time}", "%d.%m.%Y %H:%M")
            slots.append((start, end))

        slots.sort()

        # Merge overlapping or contiguous time slots
        merged_slots = []
        current_start, current_end = slots[0]

        for start, end in slots[1:]:
            if start <= current_end:
                current_end = max(current_end, end)
            else:
                merged_slots.append((current_start, current_end))
                current_start, current_end = start, end

        merged_slots.append((current_start, current_end))

        # Convert merged time slots back to strings
        combined_slots = [
            f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}"
            for start, end in merged_slots
        ]
        return combined_slots

    def generate_json(
        self,
        schedules: Dict[str, Dict[float, List[str]]],
        existing_data: Optional[dict] = None,
    ) -> dict:
        """Generate JSON data following dnipro.json schema for ALL queues

        Args:
            schedules: Dict with dates as keys, and dict of queue_number -> time_slots as values
        """

        # Use existing data or create new structure
        if existing_data:
            data = existing_data
        else:
            data = self.create_json_structure()

        # Clean up old data (dates before today in Kyiv timezone)
        data = self.cleanup_old_data(data)

        # Update lastUpdated timestamp
        data["lastUpdated"] = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )

        # Get all unique queue numbers from all dates
        all_queues = set()
        for date_schedules in schedules.values():
            all_queues.update(date_schedules.keys())

        self.logger.debug(f"Processing schedules for queues: {sorted(all_queues)}")

        # Process each date and its schedules
        for date_str, queues_schedules in schedules.items():
            # Parse date string and make it timezone-aware in Kyiv timezone
            date_obj = datetime.strptime(date_str, "%d.%m.%Y")
            kyiv_tz = ZoneInfo("Europe/Kyiv")
            date_obj = date_obj.replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=kyiv_tz
            )

            # Convert to Unix timestamp (midnight Kyiv time, not UTC)
            timestamp = int(date_obj.timestamp())
            timestamp_key = str(timestamp)

            # Initialize date entry if it doesn't exist
            if timestamp_key not in data["fact"]["data"]:
                data["fact"]["data"][timestamp_key] = {}

            # Process each queue for this date
            for queue_number, time_slots in queues_schedules.items():
                queue_key = self.get_queue_key(queue_number)

                # Initialize queue entry if it doesn't exist
                if queue_key not in data["fact"]["data"][timestamp_key]:
                    data["fact"]["data"][timestamp_key][
                        queue_key
                    ] = self.create_default_hours()

                # Update hours based on outage schedules
                hours_data = self.create_hours_from_schedules(time_slots)
                data["fact"]["data"][timestamp_key][queue_key] = hours_data

        # Set today's timestamp (using Kyiv timezone)
        kyiv_tz = ZoneInfo("Europe/Kyiv")
        today_midnight = datetime.now(kyiv_tz).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        data["fact"]["today"] = int(today_midnight.timestamp())

        # Update fact metadata
        data["fact"]["update"] = datetime.now(kyiv_tz).strftime("%d.%m.%Y %H:%M")

        # Update lastUpdateStatus
        data["lastUpdateStatus"]["at"] = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )

        return data

    def create_json_structure(self) -> dict:
        """Create initial JSON structure following dnipro.json schema"""
        kyiv_tz = ZoneInfo("Europe/Kyiv")
        today_midnight = datetime.now(kyiv_tz).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        return {
            "regionId": self.region_id,
            "lastUpdated": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "fact": {
                "data": {},
                "update": datetime.now().strftime("%d.%m.%Y %H:%M"),
                "today": int(today_midnight.timestamp()),
            },
            "preset": self.get_preset_data(),
            "lastUpdateStatus": {
                "status": "parsed",
                "ok": True,
                "code": 200,
                "message": None,
                "at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "attempt": 1,
            },
            "meta": {"schemaVersion": "1.0.0", "contentHash": ""},
            "regionAffiliation": "Дніпро та обл.",
        }

    def get_preset_data(self) -> dict:
        """Return preset data structure"""
        return {
            "days": {
                "1": "Понеділок",
                "2": "Вівторок",
                "3": "Середа",
                "4": "Четвер",
                "5": "П'ятниця",
                "6": "Субота",
                "7": "Неділя",
            },
            "days_mini": {
                "1": "Пн",
                "2": "Вт",
                "3": "Ср",
                "4": "Чт",
                "5": "Пт",
                "6": "Сб",
                "7": "Нд",
            },
            "sch_names": {
                "GPV1.1": "Черга 1.1",
                "GPV1.2": "Черга 1.2",
                "GPV2.1": "Черга 2.1",
                "GPV2.2": "Черга 2.2",
                "GPV3.1": "Черга 3.1",
                "GPV3.2": "Черга 3.2",
                "GPV4.1": "Черга 4.1",
                "GPV4.2": "Черга 4.2",
                "GPV5.1": "Черга 5.1",
                "GPV5.2": "Черга 5.2",
                "GPV6.1": "Черга 6.1",
                "GPV6.2": "Черга 6.2",
            },
            "time_zone": {
                str(i): [f"{i-1:02d}-{i:02d}", f"{i-1:02d}:00", f"{i:02d}:00"]
                for i in range(1, 25)
            },
            "time_type": {
                "yes": "Світло є",
                "maybe": "Можливо відключення",
                "no": "Світла немає",
                "first": "Світла не буде перші 30 хв.",
                "second": "Світла не буде другі 30 хв",
                "mfirst": "Світла можливо не буде перші 30 хв.",
                "msecond": "Світла можливо не буде другі 30 хв",
            },
            "data": {},
            "updateFact": datetime.now().strftime("%d.%m.%Y %H:%M"),
        }

    def get_queue_key(self, queue_number: float) -> str:
        """Convert queue number to key format (e.g., 3.1 -> 'GPV3.1')"""
        if queue_number == int(queue_number):
            return f"GPV{int(queue_number)}.1"
        else:
            parts = str(queue_number).split(".")
            return f"GPV{parts[0]}.{parts[1]}"

    def create_default_hours(self) -> Dict[str, str]:
        """Create default hours dict with all hours set to 'yes' (power available)"""
        return {str(i): "yes" for i in range(1, 25)}

    def create_hours_from_schedules(self, time_slots: List[str]) -> Dict[str, str]:
        """Create hours dictionary from time slots

        Args:
            time_slots: List of time ranges like ["07:00-10:00", "14:00-18:00"]

        Returns:
            Dictionary with hour keys (1-24) and values ('yes', 'no', 'first', or 'second')
        """
        # Start with all hours having power
        hours = self.create_default_hours()

        # Mark outage hours
        for slot in time_slots:
            start_time, end_time = slot.split("-")
            start_hour = int(start_time.split(":")[0])
            start_min = int(start_time.split(":")[1])
            end_hour = int(end_time.split(":")[0])
            end_min = int(end_time.split(":")[1])

            # Convert to decimal hours for easier calculation
            outage_start = start_hour + (start_min / 60.0)
            outage_end = end_hour + (end_min / 60.0)

            # Process each hour
            for hour in range(24):
                # Hour N in the JSON represents the time from N:00 to (N+1):00
                # But hour index in loop is 0-23, so hour_start = hour, hour_end = hour + 1
                hour_start = float(hour)
                hour_end = float(hour + 1)
                hour_mid = hour + 0.5

                # Skip if no overlap
                if outage_end <= hour_start or outage_start >= hour_end:
                    continue

                # Calculate overlap
                overlap_start = max(outage_start, hour_start)
                overlap_end = min(outage_end, hour_end)
                overlap_duration = overlap_end - overlap_start

                # Determine the status based on overlap
                if overlap_duration >= 1.0:
                    # Full hour outage
                    hours[str(hour + 1)] = "no"
                elif overlap_duration > 0:
                    # Partial hour outage
                    # Check if outage is in first half or second half
                    if overlap_start < hour_mid and overlap_end <= hour_mid:
                        # Outage only in first half (00-30 minutes)
                        if hours[str(hour + 1)] == "yes":
                            hours[str(hour + 1)] = "first"
                        elif hours[str(hour + 1)] == "second":
                            hours[str(hour + 1)] = "no"  # Both halves affected
                    elif overlap_start >= hour_mid and overlap_end > hour_mid:
                        # Outage only in second half (30-60 minutes)
                        if hours[str(hour + 1)] == "yes":
                            hours[str(hour + 1)] = "second"
                        elif hours[str(hour + 1)] == "first":
                            hours[str(hour + 1)] = "no"  # Both halves affected
                    else:
                        # Outage spans both halves or is ambiguous - mark as full outage
                        hours[str(hour + 1)] = "no"

        return hours


def main():
    parser = argparse.ArgumentParser(
        description="Scrape powercut schedules from Telegram channel for ALL queues"
    )
    parser.add_argument(
        "--url",
        default="https://t.me/s/cek_info",
        help="Telegram channel URL (e.g., https://t.me/s/channelname)",
    )
    parser.add_argument(
        "--start-date",
        help="Start date for filtering schedules (DD.MM.YYYY)",
    )
    parser.add_argument(
        "--end-date",
        help="End date for filtering schedules (DD.MM.YYYY)",
    )
    parser.add_argument(
        "--output", default="out/cek.json", help="Output JSON file path"
    )
    parser.add_argument("--region", default="dnipro", help="Region ID")
    parser.add_argument(
        "--log-level",
        default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.log_level)

    # Initialize session to None for proper cleanup
    session = None

    try:
        # Create HTTP session with retry logic
        session = create_session_with_retries()

        # Create scraper instance
        scraper = PowercutScraper(
            channel_url=args.url,
            region_id=args.region,
            start_date=args.start_date,
            end_date=args.end_date,
            logger=logger,
            session=session,
        )

        # Scrape messages for ALL queues
        schedules = scraper.scrape_messages()

        if not schedules:
            logger.warning("No schedules found")
            sys.exit(0)

        # Count total queues found
        all_queues = set()
        for date_schedules in schedules.values():
            all_queues.update(date_schedules.keys())

        # Load existing JSON if it exists
        existing_data = None
        if os.path.exists(args.output):
            try:
                with open(args.output, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
                logger.info(f"Loaded existing data from {args.output}")
            except Exception as e:
                logger.warning(f"Could not load existing data: {e}")

        # Generate JSON
        data = scraper.generate_json(schedules, existing_data)

        # Validate JSON structure before saving
        if not validate_json_structure(data, logger):
            logger.error("Generated JSON has invalid structure")
            sys.exit(1)

        # Ensure output directory exists (if a directory is specified)
        output_dir = os.path.dirname(args.output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        # Save JSON file
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"✓ Successfully saved schedules to {args.output}")
        logger.info(f"  Found schedules for {len(schedules)} date(s)")
        logger.info(f"  Total queues found: {len(all_queues)} - {sorted(all_queues)}")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Close session if it exists
        if session is not None:
            session.close()


if __name__ == "__main__":
    main()
