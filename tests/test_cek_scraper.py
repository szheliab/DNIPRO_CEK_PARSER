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


if __name__ == "__main__":
    unittest.main()
