# Copilot Instructions for Dnipro CEK Parser

## Project Overview
This is a Python project that scrapes electricity outage schedules from Telegram channels for the Dnipro region in Ukraine. It processes the data into JSON and generates visual PNG charts using Pillow.

## Key Components
- `cek_scraper.py`: Main scraper using requests and BeautifulSoup to extract outage data from Telegram channels.
- `gener_im_full.py`: Generates full outage schedule images.
- `gener_im_1_G.py`: Generates images for specific groups and dates.

## Coding Guidelines
- Use Ukrainian comments where appropriate, as the project handles Ukrainian text and dates.
- Handle timezones correctly using `ZoneInfo("Europe/Kyiv")`.
- Use pathlib for path operations.
- Log messages with timestamps in Kyiv timezone.
- Follow PEP 8 style guidelines.
- Use type hints for function parameters and return types.

## Common Patterns
- Date parsing: Use Ukrainian month names mapping to convert to DD.MM format.
- Image generation: Use PIL (Pillow) with specific fonts and layouts for charts.
- Data cleanup: Remove old timestamps based on current Kyiv time.
- Error handling: Use try-except blocks and log exceptions.

## Dependencies
- BeautifulSoup for HTML parsing
- Pillow for image creation
- Requests for HTTP calls

## File Structure
- `src/`: Source code
- `out/`: Generated JSON and images
- `logs/`: Log files
- `requirements.txt`: Python dependencies

## When Modifying Code
- Test with real Telegram channel URLs (ensure compliance with terms of service).
- Verify image outputs match expected layouts.
- Check timezone handling for date calculations.
- Update README.md if adding new features.
