import sys
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import gener_im_full  # noqa: E402

KYIV = ZoneInfo("Europe/Kyiv")


def today_ts() -> int:
    now = datetime.now(KYIV)
    return int(datetime(now.year, now.month, now.day, tzinfo=KYIV).timestamp())


class GetDatesToGenerateTest(unittest.TestCase):
    def test_empty_data_raises(self) -> None:
        with self.assertRaises(ValueError):
            gener_im_full.get_dates_to_generate({})

    def test_today_and_tomorrow(self) -> None:
        ts = today_ts()
        data = {str(ts + 86400): {}, str(ts): {}}
        result = gener_im_full.get_dates_to_generate(data)
        self.assertEqual(
            [r[2] for r in result],
            ["gpv-all-today.png", "gpv-all-tomorrow.png"],
        )

    def test_single_old_date_is_rendered_as_today(self) -> None:
        ts = today_ts() - 5 * 86400
        result = gener_im_full.get_dates_to_generate({str(ts): {}})
        self.assertEqual([r[2] for r in result], ["gpv-all-today.png"])

    def test_far_future_date_is_skipped_when_multiple(self) -> None:
        ts = today_ts()
        data = {str(ts): {}, str(ts + 5 * 86400): {}}
        result = gener_im_full.get_dates_to_generate(data)
        self.assertEqual([r[2] for r in result], ["gpv-all-today.png"])


if __name__ == "__main__":
    unittest.main()
