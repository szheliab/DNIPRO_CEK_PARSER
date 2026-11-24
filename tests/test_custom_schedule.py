import unittest

from src.custom_schedule import parse_custom_schedule

FIRST = """📢 Шановні мешканці м. Дніпро‼️
ГПВ на 24 ЛИСТОПАДА для споживачів міста, які тимчасово вимикаються за окремим графіком:

📌 1 черга:
✔️ 00:00 - 02:00;
✔️ 10:00 - 14:00;
✔️ 22:00 - 24:00.

📌 2 черга:
✔️ 02:00 - 06:00;
✔️ 14:00 - 18:00;

📌 3 черга:
✔️ 06:00 - 10:00;
✔️ 18:00 - 22:00;

Дякуємо за розуміння та вибачаємося за тимчасові незручності‼️"""


class CustomScheduleTest(unittest.TestCase):
    def test_parses_whole_number_queues(self) -> None:
        self.assertEqual(
            parse_custom_schedule(FIRST),
            {
                1: ["00:00-02:00", "10:00-14:00", "22:00-24:00"],
                2: ["02:00-06:00", "14:00-18:00"],
                3: ["06:00-10:00", "18:00-22:00"],
            },
        )

    def test_ignores_standard_queues(self) -> None:
        message = "1.1 черга: з 07:00 до 10:00; з 14:00 до 18:00"
        self.assertEqual(parse_custom_schedule(message), {})


class OtherCityTest(unittest.TestCase):
    def test_pavlohrad_message_is_ignored(self) -> None:
        message = FIRST.replace("м. Дніпро", "м. ПАВЛОГРАД")
        self.assertEqual(parse_custom_schedule(message), {})

    def test_pavlohrad_standard_queues_are_not_custom(self) -> None:
        message = (
            "📢 Шановні мешканці м. ПАВЛОГРАД‼️\n📌 2.2 черга:\n✔️ 00:00 - 03:00;\n"
            "📌 6.2 черга:\n✔️ 03:00 - 08:00;"
        )
        self.assertEqual(parse_custom_schedule(message), {})


class CustomFactDataTest(unittest.TestCase):
    def test_custom_queues_are_written_to_fact_data_and_rendered(self) -> None:
        import json
        import logging
        import tempfile
        from pathlib import Path

        from src import gener_im_1_G as gen
        from src.cek_scraper import PowercutScraper

        scraper = PowercutScraper.__new__(PowercutScraper)
        scraper.logger = logging.getLogger(__name__)
        scraper.custom_schedules = {
            "24.11.2026": (0, {1: ["00:00-02:00"], 2: ["02:00-06:00"]})
        }
        data = {"fact": {"data": {}}, "custom": {"data": {}}}
        scraper.create_hours_from_schedules = (
            lambda slots: PowercutScraper.create_hours_from_schedules(scraper, slots)
        )
        scraper.create_default_hours = lambda: {str(i): "yes" for i in range(1, 25)}
        scraper.apply_custom_schedules(data)

        self.assertNotIn("custom", data)
        (day,) = data["fact"]["data"].values()
        self.assertEqual(set(day), {"GPV1", "GPV2"})
        self.assertEqual(day["GPV1"]["1"], "no")
        self.assertEqual(day["GPV1"]["3"], "yes")

        with tempfile.TemporaryDirectory() as tmp:
            json_path = Path(tmp) / "cek.json"
            json_path.write_text(json.dumps(data), encoding="utf-8")
            gen.OUT_DIR = Path(tmp)
            gen.generate_from_json(str(json_path))
            self.assertTrue((Path(tmp) / "gpv-1-emergency.png").exists())
            self.assertTrue((Path(tmp) / "gpv-2-emergency.png").exists())


if __name__ == "__main__":
    unittest.main()
