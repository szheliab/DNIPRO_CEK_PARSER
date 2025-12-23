# Dnipro CEK Parser

A Python-based scraper for electricity outage schedules in Dnipro, Ukraine. This tool scrapes powercut schedules from Telegram channels, processes the data into JSON format, and generates visual charts (PNG images) for easy viewing.

## Features

- Scrapes electricity outage schedules for all queues from specified Telegram channels
- Outputs data in JSON format
- Generates hourly outage charts as PNG images
- Supports multiple groups and dates
- Cleans up old data automatically
- Handles Ukrainian date formats and timezones (Europe/Kyiv)

## Requirements

- Python 3.11+
- Dependencies listed in `requirements.txt`

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd DNIPRO_CEK_PARSER
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Scraping Data

Run the scraper to fetch and process outage schedules:

```bash
python src/cek_scraper.py --channel-url <telegram-channel-url> --region-id dnipro
```

Optional arguments:
- `--start-date DD.MM.YYYY`: Start date for scraping (default: today)
- `--end-date DD.MM.YYYY`: End date for scraping (default: tomorrow)

### Generating Images

Generate outage charts from JSON data:

For full schedule:
```bash
python src/gener_im_full.py
```

For 1 group on 2 dates:
```bash
python src/gener_im_1_G.py
```

## Output

- JSON data is saved in the `out/` directory
- Generated images are saved in `out/images/`
- Logs are written to `logs/full_log.log`

## Project Structure

```
.
├── README.md
├── requirements.txt
├── src/
│   ├── cek_scraper.py       # Main scraper script
│   ├── gener_im_full.py     # Full schedule image generator
│   └── gener_im_1_G.py      # 1-group schedule image generator
└── out/                    # Output directory (created automatically)
    ├── images/             # Generated PNG charts
    └── ...                 # JSON data files
```

## Dependencies

- `requests`: HTTP requests
- `beautifulsoup4`: HTML parsing
- `Pillow`: Image generation
