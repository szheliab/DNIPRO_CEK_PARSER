#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Parser for Cherkasy Oblenergo (Telegram)

import asyncio
import re
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright
import os

# ================== НАЛАШТУВАННЯ ==================

TZ = ZoneInfo("Europe/Kyiv")
URL = "https://t.me/s/pat_cherkasyoblenergo"
OUTPUT_FILE = "out/Cherkasyoblenergo.json"

LOG_DIR = "logs"
FULL_LOG_FILE = os.path.join(LOG_DIR, "full_log.log")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs("out", exist_ok=True)

KEYWORDS = [
    "графіки погодинних відключень",
    "графіки погодинних вимкнень",
    "графік погодинних відключень",
    "графік погодинних вимкнень",
    "ГПВ",
    "години відсутності електропостачання",
    "застосовуватимуться графіки",
    "оновлений графік"
]

# ================== ЛОГУВАННЯ ==================

def log(message: str):
    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [cherkasy_parser] {message}"
    print(line)
    with open(FULL_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ================== HELPERS ==================

def time_to_hour(hhmm: str) -> float:
    h, m = map(int, hhmm.split(":"))
    return h + m / 60.0


def is_schedule_post(text: str) -> bool:
    return any(k.lower() in text.lower() for k in KEYWORDS)


def is_update_post(text: str) -> bool:
    return any(k in text.lower() for k in [
        "оновлений графік",
        "оновлено графік",
        "скорегований графік"
    ])


def log_group_intervals(group_id: str, intervals: list[tuple[str, str]]):
    if intervals:
        log(f"✔️ {group_id} — {intervals}")

# ================== TELEGRAM ==================

async def fetch_posts() -> list:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.new_page()
        await page.goto(URL, timeout=60000)
        await page.wait_for_selector(".tgme_widget_message")
        await page.wait_for_timeout(3000)

        posts = []
        for msg in await page.query_selector_all(".tgme_widget_message"):
            text_el = await msg.query_selector(".tgme_widget_message_text")
            if not text_el:
                continue

            text = await text_el.inner_text()
            if is_schedule_post(text):
                posts.append({"text": text})

        await browser.close()
        log(f"✔️ Знайдено {len(posts)} постів з графіками")
        return posts

# ================== ДАТА ==================

def extract_date_from_post(text: str) -> str | None:
    months = {
        'січня': '01', 'лютого': '02', 'березня': '03', 'квітня': '04',
        'травня': '05', 'червня': '06', 'липня': '07', 'серпня': '08',
        'вересня': '09', 'жовтня': '10', 'листопада': '11', 'грудня': '12'
    }

    for d, m in re.findall(r'(\d{1,2})\s+(%s)' % "|".join(months), text.lower()):
        return f"{d.zfill(2)}.{months[m]}.{datetime.now(TZ).year}"

    return None

# ================== ПАРСИНГ ==================

def put_interval(result: dict, group_id: str, t1: float, t2: float):
    # зсув +1 година (GPV-логіка)
    t1 += 1
    t2 += 1

    for hour in range(1, 25):
        h = float(hour)

        first = t1 < h + 0.5 and t2 > h
        second = t1 < h + 1.0 and t2 > h + 0.5

        if not first and not second:
            continue

        key = str(hour)

        if first and second:
            result[group_id][key] = "no"
        elif first:
            result[group_id][key] = "first"
        elif second:
            result[group_id][key] = "second"


def parse_schedule_from_text(text: str) -> dict:
    result = {}

    if "Години відсутності електропостачання" not in text:
        return result

    text = text.split("Години відсутності електропостачання", 1)[1]

    for line in text.splitlines():
        m = re.match(r'(\d)\.(\d)\s+(.+)', line.strip())
        if not m:
            continue

        group_id = f"GPV{m.group(1)}.{m.group(2)}"
        content = m.group(3)

        if "не вимикається" in content.lower():
            continue

        if group_id not in result:
            result[group_id] = {}

        intervals = re.findall(
            r'(\d{1,2}:\d{2})\s*[-–—]\s*(\d{1,2}:\d{2})',
            content
        )

        log_group_intervals(group_id, intervals)

        for t1, t2 in intervals:
            put_interval(result, group_id, time_to_hour(t1), time_to_hour(t2))

    return result

# ================== NORMALIZE ==================

def normalize_schedule(schedule: dict) -> dict:
    """
    Гарантує, що кожна група має години 1..24.
    Якщо години немає — 'yes'.
    """
    normalized = {}
    for group_id, hours in schedule.items():
        full = {}
        for h in range(1, 25):
            key = str(h)
            full[key] = hours.get(key, "yes")
        normalized[group_id] = full
    return normalized

# ================== MERGE ==================

def merge_schedules(base: dict, update: dict) -> dict:
    merged = {g: h.copy() for g, h in base.items()}
    for g, hours in update.items():
        if g not in merged:
            merged[g] = {}
        for hour, state in hours.items():
            merged[g][hour] = state
    return merged

# ================== MAIN ==================

async def main():
    posts = await fetch_posts()

    today = datetime.now(TZ).date()
    tomorrow = today + timedelta(days=1)

    schedules = {}

    for post in posts:
        date_str = extract_date_from_post(post["text"])
        if not date_str:
            continue

        date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
        if date_obj not in (today, tomorrow):
            continue

        parsed = parse_schedule_from_text(post["text"])
        if not parsed:
            continue

        if date_str not in schedules:
            schedules[date_str] = parsed
            log(f"📅 Базовий графік для {date_str}")
        elif is_update_post(post["text"]):
            schedules[date_str] = merge_schedules(schedules[date_str], parsed)
            log(f"🔄 Оновлення застосовано для {date_str}")

    if not schedules:
        log("⚠️ Дані не знайдені")
        return False

    # -------- формування data --------
    out_data = {}
    for d, sch in schedules.items():
        dt = datetime.strptime(d, "%d.%m.%Y").replace(tzinfo=TZ)
        out_data[str(int(dt.timestamp()))] = normalize_schedule(sch)

    out_data = dict(sorted(out_data.items(), key=lambda x: int(x[0])))

    new_json = {
        "regionId": "Cherkasy",
        "lastUpdated": datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "fact": {
            "data": out_data,
            "update": datetime.now(TZ).strftime("%d.%m.%Y %H:%M"),
            "today": int(datetime(today.year, today.month, today.day, tzinfo=TZ).timestamp())
        }
    }

    # ================== DIFF CHECK ==================
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                old_json = json.load(f)

            if json.dumps(
                old_json.get("fact", {}).get("data", {}),
                sort_keys=True
            ) == json.dumps(
                new_json.get("fact", {}).get("data", {}),
                sort_keys=True
            ):
                log("ℹ️ Дані не змінилися — JSON не оновлюємо")
                return False

        except Exception as e:
            log(f"⚠️ Помилка DIFF-перевірки: {e}")

    # ================== SAVE ==================
    log(f"💾 Записую JSON → {OUTPUT_FILE}")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(new_json, f, ensure_ascii=False, indent=2)

    log("✔️ JSON успішно оновлено")
    return True

# ================== ENTRY ==================

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        if result:
            log("🎉 Парсинг завершено з оновленням")
        else:
            log("ℹ️ Парсинг завершено без змін")
    except KeyboardInterrupt:
        log("⚠️ Перервано користувачем")
    except Exception as e:
        log(f"❌ Фатальна помилка: {e}")
        # аварійно видаляємо JSON
        try:
            if os.path.exists(OUTPUT_FILE):
                os.remove(OUTPUT_FILE)
                log(f"🗑 JSON видалено через помилку: {OUTPUT_FILE}")
        except Exception:
            pass
        raise
