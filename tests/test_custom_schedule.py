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


if __name__ == "__main__":
    unittest.main()
