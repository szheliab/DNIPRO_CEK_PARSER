#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Parser for Cherkasy Oblenergo (Telegram)

import asyncio
import re
import json
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from playwright.async_api import async_playwright
import os

TZ = ZoneInfo("Europe/Kyiv")
URL = "https://t.me/s/pat_cherkasyoblenergo"
OUTPUT_FILE = "out/Cherkasyoblenergo.json"

LOG_DIR = "logs"
FULL_LOG_FILE = os.path.join(LOG_DIR, "full_log.log")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs("out", exist_ok=True)


def log(message: str):
    timestamp = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [cherkasy_parser] {message}"
    print(line)
    with open(FULL_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def time_to_hour(hhmm: str) -> float:
    hh, mm = map(int, hhmm.split(":"))
    return hh + (mm / 60.0)


async def fetch_text() -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, 
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled"
            ]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            log(f"🌐 Завантажую {URL}...")
            await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_selector(".tgme_widget_message", timeout=30000)
            
            # Чекаємо додатково для рендерингу контенту
            await page.wait_for_timeout(3000)
            
            text = await page.inner_text("body")
            log(f"✔️ Отримано {len(text)} символів тексту")
        finally:
            await browser.close()
            
        return text


def put_interval(result: dict, group_id: str, t1: float, t2: float) -> None:
    # Зсув на +1 годину
    t1 += 1.0
    t2 += 1.0
    
    for hour in range(1, 25):
        h_start = float(hour)
        h_mid = h_start + 0.5
        h_end = h_start + 1.0

        first_off = (t1 < h_mid and t2 > h_start)
        second_off = (t1 < h_end and t2 > h_mid)

        if not first_off and not second_off:
            continue

        key = str(hour)

        if first_off and second_off:
            result[group_id][key] = "no"
        elif first_off:
            result[group_id][key] = "first"
        elif second_off:
            result[group_id][key] = "second"


def parse_schedule_block(text: str, date_str: str) -> dict:
    """Парсить блок з графіком відключень"""
    result = {}
    
    # Шукаємо текст після "Години відсутності електропостачання"
    schedule_start = re.search(r'Години\s+відсутності\s+електропостачання', text, re.IGNORECASE)
    if schedule_start:
        text = text[schedule_start.end():]
        log(f"📍 Знайдено початок графіків для {date_str}")
    
    # Обрізаємо до наступного поста (шукаємо дату іншого дня або "Перелік адрес")
    cutoff = re.search(r'(Перелік адрес|Зверніть увагу|Сторінка у Telegram)', text, re.IGNORECASE)
    if cutoff:
        text = text[:cutoff.start()]
        log(f"✂️ Обрізано текст до кінця графіків")
    
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        
        # Шукаємо формат "1.1 00:00 - 02:00, 08:00 - 10:00"
        match = re.match(r'(\d)\.(\d)\s+(.+)', line)
        if not match:
            continue
            
        group_num = f"{match.group(1)}.{match.group(2)}"
        group_id = "GPV" + group_num
        text_content = match.group(3)
        
        # Перевіряємо чи не вимикається
        if 'не вимикається' in text_content.lower() or 'не вимикаються' in text_content.lower():
            log(f"⚪ {group_id} — не вимикається")
            continue
        
        if group_id not in result:
            result[group_id] = {str(h): "yes" for h in range(1, 25)}

        # Шукаємо інтервали відключень
        intervals = re.findall(r'(\d{1,2}:\d{2})\s*[-–—]\s*(\d{1,2}:\d{2})', text_content)
        
        for t1_str, t2_str in intervals:
            try:
                t1 = time_to_hour(t1_str)
                t2 = time_to_hour(t2_str)
                put_interval(result, group_id, t1, t2)
            except:
                continue
        
        if intervals:
            log(f"✔️ {group_id} — {intervals}")
    
    return result


async def main():
    log("⏳ Завантажую Telegram-канал...")
    html_text = await fetch_text()
    log("✔️ Текст отримано!")

    today = datetime.now(TZ).date()
    tomorrow = today + timedelta(days=1)
    today_str = today.strftime("%d.%m.%Y")
    tomorrow_str = tomorrow.strftime("%d.%m.%Y")

    results_for_all_dates = {}
    updates_for_dates = {}
    processed_dates = set()

    months = {
        'січня': '01', 'лютого': '02', 'березня': '03', 'квітня': '04',
        'травня': '05', 'червня': '06', 'липня': '07', 'серпня': '08',
        'вересня': '09', 'жовтня': '10', 'листопада': '11', 'грудня': '12'
    }
    
    # Патерн для пошуку дати в пості
    # "17 грудня за командою НЕК «Укренерго» застосовуватимуться графіки"
    date_pattern = r'(\d{1,2})\s+(' + '|'.join(months.keys()) + r')\s+за\s+командою\s+НЕК'
    
    for match in re.finditer(date_pattern, html_text, re.IGNORECASE):
        day = match.group(1).zfill(2)
        month_name = match.group(2).lower()
        month = months.get(month_name)
        
        if not month:
            continue
        
        date_str = f"{day}.{month}.{datetime.now(TZ).year}"
        
        # Пропускаємо якщо не today/tomorrow
        if date_str not in (today_str, tomorrow_str):
            log(f"⏭️ Пропускаю {date_str} (не сьогодні/завтра)")
            continue
        
        # Пропускаємо якщо вже оброблено
        if date_str in processed_dates:
            log(f"ℹ️ {date_str} — вже оброблено, пропускаю")
            continue
        
        log(f"📅 Обробляю пост для {date_str}")
        
        # Витягуємо блок поста (шукаємо від початку до наступної дати або кінця)
        match_start = match.start()
        
        # Шукаємо початок поста (зазвичай перед датою є текст про обстріли)
        post_start = max(0, match_start - 500)
        
        # Шукаємо наступну дату
        next_match = re.search(date_pattern, html_text[match.end():], re.IGNORECASE)
        
        if next_match:
            schedule_block = html_text[post_start:match.end() + next_match.start()]
        else:
            schedule_block = html_text[post_start:match.end() + 5000]
        
        log(f"📦 Розмір блоку: {len(schedule_block)} символів")
        
        # Парсимо графік
        result = parse_schedule_block(schedule_block, date_str)
        
        if not result:
            log(f"⚠️ Не знайдено графіків для {date_str}")
            continue
        
        # Час оновлення - беремо поточний час
        current_time = datetime.now(TZ).strftime("%H:%M")
        updates_for_dates[date_str] = f"{current_time} {date_str}"
        log(f"🕒 Час оновлення: {current_time}")
        
        # Створюємо timestamp
        day_int, month_int, year_int = map(int, date_str.split("."))
        date_dt = datetime(year_int, month_int, day_int, tzinfo=TZ)
        date_ts = int(date_dt.timestamp())
        
        results_for_all_dates[str(date_ts)] = result
        processed_dates.add(date_str)
        log(f"✅ Додано графік для {date_str}: {len(result)} груп")

    if not results_for_all_dates:
        log("⚠️ Не знайдено жодних графіків відключень!")
        return False

    # Перевіряємо DIFF
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            old_json = json.load(f)
        old_data = old_json.get("fact", {}).get("data", {})

        if json.dumps(old_data, sort_keys=True) == json.dumps(results_for_all_dates, sort_keys=True):
            log("ℹ️ Дані не змінилися — JSON не оновлюємо")
            return False

    # Вибираємо найновіше оновлення
    #if updates_for_dates:
    #    latest_update_value = max(updates_for_dates.values())
    #    latest_update_formatted = datetime.strptime(
    #        latest_update_value, "%H:%M %d.%m.%Y"
    #    ).strftime("%d.%m.%Y %H:%M")
    #else:
    #    latest_update_formatted = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    # Встановлюємо поточну дату і час оновлення
    update_formatted = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    
    #log(f"🕑 Фінальне оновлення: {latest_update_formatted}")
    log(f"🕑 Фінальне оновлення (поточні дата і час): {update_formatted}")

    # Сортуємо дати від меншої до більшої
    sorted_results = dict(sorted(results_for_all_dates.items(), key=lambda x: int(x[0])))
    results_for_all_dates = sorted_results

    # Формуємо JSON
    new_json = {
        "regionId": "Cherkasy",
        "lastUpdated": datetime.now(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "fact": {
            "data": results_for_all_dates,
            #"update": latest_update_formatted,
            "update": update_formatted,
            "today": int(datetime(today.year, today.month, today.day, tzinfo=TZ).timestamp())
        },
        "preset": {
            "time_zone": {
                str(i): [f"{i - 1:02d}-{i:02d}", f"{i - 1:02d}:00", f"{i:02d}:00"]
                for i in range(1, 25)
            },
            "time_type": {
                "yes": "Світло є",
                "maybe": "Можливе відключення",
                "no": "Світла немає",
                "first": "Світла не буде перші 30 хв.",
                "second": "Світла не буде другі 30 хв"
            }
        }
    }

    # Записуємо JSON
    log(f"💾 Записую JSON → {OUTPUT_FILE}")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(new_json, f, ensure_ascii=False, indent=2)

    log("✔️ JSON успішно оновлено")
    return True


if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        if result:
            log("🎉 Парсинг завершено успішно")
        else:
            log("ℹ️ Парсинг завершено без оновлень")
    except KeyboardInterrupt:
        log("⚠️ Перервано користувачем")
    except Exception as e:
        log(f"❌ Фатальна помилка: {e}")
        import traceback
        log(traceback.format_exc())