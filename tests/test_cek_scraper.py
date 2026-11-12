import logging
import unittest

from src.cek_scraper import PowercutScraper


class ExtractModificationsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = PowercutScraper.__new__(PowercutScraper)
        self.scraper.logger = logging.getLogger(__name__)

    def test_extracts_additional_ranges_from_multiple_queue_groups(self) -> None:
        message = (
            '📢 Шановні споживачі! Повідомляємо про зміни в ГПВ 11 ЛИСТОПАДА, '
            'за розпорядженням диспетчерського центру НЕК "Укренерго" '
            "з 00:00 до 5:00 додатково застосовуються відлючення черги "
            "5.1, 5.2 ,6.1,6.2  та черги 1.1, 1.2 ,2.1,2.2- "
            "з 01:30 до 05:00 ‼️"
        )

        modifications = self.scraper.extract_modifications(message)

        self.assertTrue(self.scraper.is_partial_schedule_update(message))
        self.assertEqual(
            self.scraper.extract_time_range_from_header(message),
            ("00:00", "05:00"),
        )
        self.assertEqual(
            set(modifications),
            {
                (5.1, "additional", "00:00-05:00"),
                (5.2, "additional", "00:00-05:00"),
                (6.1, "additional", "00:00-05:00"),
                (6.2, "additional", "00:00-05:00"),
                (1.1, "additional", "01:30-05:00"),
                (1.2, "additional", "01:30-05:00"),
                (2.1, "additional", "01:30-05:00"),
                (2.2, "additional", "01:30-05:00"),
            },
        )

    def test_applies_early_start_to_nearest_upcoming_slot(self) -> None:
        message = (
            'Шановні споживачі! Повідомляємо про зміни в ГПВ 12 ЛИСТОПАДА, '
            'за розпорядженням диспетчерського центру НЕК "Укренерго" '
            "з 10:40 додатково застосовуються черги 2.1 та 2.2 ‼️"
        )
        modifications = self.scraper.extract_modifications(message)
        self.assertEqual(
            modifications,
            [(2.1, "early_start", "10:40"), (2.2, "early_start", "10:40")],
        )
        schedules = {
            "12.11.2026": {
                2.1: ["18:00-20:00", "12:00-15:00", "07:00-09:00"],
                2.2: ["11:30-13:00"],
                3.1: ["12:00-15:00"],
            }
        }
        self.scraper.apply_modifications(
            schedules,
            {"12.11.2026": {
                queue: [f"MOD:{kind}:{time}"]
                for queue, kind, time in modifications
            }},
        )
        self.assertEqual(schedules["12.11.2026"], {
            2.1: ["07:00-09:00", "10:40-15:00", "18:00-20:00"],
            2.2: ["10:40-13:00"],
            3.1: ["12:00-15:00"],
        })

    def test_additional_ranges_accept_conjunctions_in_both_lists(self) -> None:
        message = (
            "з 00:00 до 5:00 додатково застосовуються відлючення черги "
            "5.1 та 5.2 і 6.1 та черги 1.1 та 1.2 і 2.1 - з 01:30 до 05:00 ‼️"
        )
        self.assertEqual(
            set(self.scraper.extract_modifications(message)),
            {
                (5.1, "additional", "00:00-05:00"),
                (5.2, "additional", "00:00-05:00"),
                (6.1, "additional", "00:00-05:00"),
                (1.1, "additional", "01:30-05:00"),
                (1.2, "additional", "01:30-05:00"),
                (2.1, "additional", "01:30-05:00"),
            },
        )

    def test_cancellation_does_not_create_additional_outage(self) -> None:
        cancellation = "скасовано відключення черги 1.1 - з 12:00 до 14:00"
        self.assertEqual(
            self.scraper.extract_modifications(cancellation),
            [(1.1, "cancel", "00:00")],
        )
        addition = (
            "з 00:00 до 5:00 додатково застосовуються відлючення черги "
            "5.1 та черги 5.2 - з 01:30 до 05:00"
        )
        for message in (
            f"{cancellation}; {addition}",
            f"{addition}; {cancellation}",
        ):
            with self.subTest(message=message):
                self.assertEqual(
                    set(self.scraper.extract_modifications(message)),
                    {
                        (1.1, "cancel", "00:00"),
                        (5.1, "additional", "00:00-05:00"),
                        (5.2, "additional", "01:30-05:00"),
                    },
                )

    def test_early_start_without_upcoming_slot_leaves_schedule_unchanged(self) -> None:
        slots = ["07:00-09:00", "09:30-10:30"]
        with self.assertLogs(self.scraper.logger, level="WARNING"):
            result = self.scraper.modify_time_slots(
                slots, "early_start", "10:40", "12.11.2026", 2.1
            )
        self.assertEqual(result, slots)

    def test_early_start_without_existing_queue_logs_warning(self) -> None:
        schedules = {"12.11.2026": {}}
        with self.assertLogs(self.scraper.logger, level="WARNING"):
            self.scraper.apply_modifications(
                schedules, {"12.11.2026": {2.1: ["MOD:early_start:10:40"]}}
            )
        self.assertEqual(schedules, {"12.11.2026": {}})


class ExtractAllSchedulesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = PowercutScraper.__new__(PowercutScraper)
        self.scraper.logger = logging.getLogger(__name__)

    def test_extracts_time_first_outage_announcement(self) -> None:
        message = (
            "📢 Шановні споживачі‼️\n"
            "Повідомляємо про зміни в ГПВ на 12 ЛИСТОПАДА, за розпорядженням "
            'диспетчерського центру НЕК "Укренерго" з 12:30 до 16:00 '
            "відключаються черги: 1.1, 1.2, 2.1, 2.2, 3.1 та 3.2 ‼️"
        )

        self.assertEqual(
            self.scraper.extract_all_schedules(message),
            {
                1.1: ["12:30-16:00"],
                1.2: ["12:30-16:00"],
                2.1: ["12:30-16:00"],
                2.2: ["12:30-16:00"],
                3.1: ["12:30-16:00"],
                3.2: ["12:30-16:00"],
            },
        )

    def test_time_first_announcements_accept_i_conjunction(self) -> None:
        for verb in ("застосовуються відключення", "відключаються"):
            with self.subTest(verb=verb):
                message = f"з 12:30 до 16:00 {verb} черги: 1.1 і 1.2"
                self.assertEqual(
                    self.scraper.extract_all_schedules(message),
                    {1.1: ["12:30-16:00"], 1.2: ["12:30-16:00"]},
                )

    def test_early_start_message_has_no_empty_schedule_entries(self) -> None:
        message = (
            "Повідомляємо про зміни в ГПВ на 12 ЛИСТОПАДА, "
            "з 10:40 додатково застосовуються черга 2.1"
        )
        self.assertEqual(self.scraper.extract_all_schedules(message), {})
        self.assertEqual(
            self.scraper.extract_modifications(message),
            [(2.1, "early_start", "10:40")],
        )


if __name__ == "__main__":
    unittest.main()
