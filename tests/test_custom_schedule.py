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


class CustomImageTest(unittest.TestCase):
    def test_generates_image_per_custom_queue(self) -> None:
        import json
        import tempfile
        from pathlib import Path

        from src import gener_im_1_G as gen

        hours = {str(h): "no" if h <= 2 else "yes" for h in range(1, 25)}
        data = {
            "fact": {"data": {}, "update": "24.11.2026 10:00"},
            "custom": {"data": {"1795471200": {"1": hours, "2": hours}}},
            "preset": {},
        }
        with tempfile.TemporaryDirectory() as tmp:
            json_path = Path(tmp) / "cek.json"
            json_path.write_text(json.dumps(data), encoding="utf-8")
            gen.OUT_DIR = Path(tmp)
            gen.generate_from_json(str(json_path))
            self.assertTrue((Path(tmp) / "gpv-custom-1.png").exists())
            self.assertTrue((Path(tmp) / "gpv-custom-2.png").exists())


if __name__ == "__main__":
    unittest.main()
