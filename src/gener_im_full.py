#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Створення PNG графіка погодинних відключень з JSON.
Генерує:
- gpv-all-today.png для сьогоднішньої дати
- gpv-all-tomorrow.png для завтрашньої дати (якщо є)
"""
import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from PIL import Image, ImageDraw, ImageFont
import sys

# --- Налаштування шляхів ---
BASE = Path(__file__).parent.parent.absolute()
JSON_DIR = BASE / "out"
OUT_DIR = BASE / "out/images"
OUT_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = BASE / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
FULL_LOG_FILE = LOG_DIR / "full_log.log"

def log(message):
    timestamp = datetime.now(ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp} [gener_im_full] {message}"
    print(line)
    try:
        with open(FULL_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# --- Візуальні параметри ---
CELL_W = 44
CELL_H = 36
LEFT_COL_W = 140
HEADER_H = 34
SPACING = 60
LEGEND_H = 60
HOUR_ROW_H = 90
HEADER_SPACING = 35
HOUR_LINE_GAP = 15

# --- Шрифти ---
TITLE_FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
TITLE_FONT_SIZE = 34
HOUR_FONT_SIZE = 15
GROUP_FONT_SIZE = 20
SMALL_FONT_SIZE = 16
LEGEND_FONT_SIZE = 14

# --- Кольори ---
BG = (250, 250, 250)
TABLE_BG = (255, 255, 255)
GRID_COLOR = (139, 139, 139)
TEXT_COLOR = (0, 0, 0)
OUTAGE_COLOR = (147, 170, 210)
POSSIBLE_COLOR = (255, 220, 115)
AVAILABLE_COLOR = (255, 255, 255)
HEADER_BG = (245, 247, 250)
FOOTER_COLOR = (140, 140, 140)

# --- Завантаження останнього JSON ---
def load_latest_json(json_dir: Path):
    files = sorted(json_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError("Не знайдено JSON файлів у " + str(json_dir))
    with open(files[0], "r", encoding="utf-8") as f:
        data = json.load(f)
    return data, files[0]

# --- Вибір шрифту з fallback ---
def pick_font(size, bold=False):
    try:
        path = TITLE_FONT_PATH if bold else FONT_PATH
        return ImageFont.truetype(path, size=size)
    except Exception:
        try:
            return ImageFont.load_default()
        except Exception:
            return None

# --- Визначення дат для генерації ---
def get_dates_to_generate(fact_data: dict) -> list:
    """
    Повертає список кортежів (timestamp, day_key, filename, date_label) для генерації.
    
    Args:
        fact_data: Словник з даними fact.data
        
    Returns:
        list: Список кортежів для кожної дати
    """
    available_dates = list(fact_data.keys())
    
    if not available_dates:
        raise ValueError("Немає доступних дат у fact.data")
    
    # Сортуємо дати як числа (timestamp) у зростаючому порядку
    try:
        sorted_dates = sorted(available_dates, key=lambda x: int(x))
    except (ValueError, TypeError):
        sorted_dates = sorted(available_dates)
    
    # Отримуємо поточну дату (початок доби) в Києві
    kyiv_tz = ZoneInfo("Europe/Kyiv")
    now = datetime.now(kyiv_tz)
    today_start = datetime(now.year, now.month, now.day, tzinfo=kyiv_tz)
    today_ts = int(today_start.timestamp())
    tomorrow_ts = today_ts + 86400  # +1 день
    
    result = []
    
    for day_key in sorted_dates:
        timestamp = int(day_key)
        date_obj = datetime.fromtimestamp(timestamp, kyiv_tz)
        date_str = date_obj.strftime("%d.%m.%Y")
        
        # Визначаємо, це сьогодні чи завтра
        day_diff = (timestamp - today_ts) // 86400
        
        if day_diff == 0:
            # Сьогодні
            filename = "gpv-all-today.png"
            date_label = "сьогодні"
            log(f"Знайдено дату для СЬОГОДНІ: {day_key} ({date_str})")
        elif day_diff == 1:
            # Завтра
            filename = "gpv-all-tomorrow.png"
            date_label = "завтра"
            log(f"Знайдено дату для ЗАВТРА: {day_key} ({date_str})")
        else:
            # Інша дата - пропускаємо або використовуємо як сьогодні
            log(f"Знайдено іншу дату: {day_key} ({date_str}), різниця днів: {day_diff}")
            if len(sorted_dates) == 1:
                # Якщо тільки одна дата, генеруємо як today
                filename = "gpv-all-today.png"
                date_label = date_str
            else:
                continue
        
        result.append((timestamp, day_key, filename, date_str))
    
    if not result:
        # Якщо не знайшли підходящих дат, беремо останню як today
        day_key = sorted_dates[-1]
        timestamp = int(day_key)
        date_str = datetime.fromtimestamp(timestamp, kyiv_tz).strftime("%d.%m.%Y")
        result.append((timestamp, day_key, "gpv-all-today.png", date_str))
        log(f"Використовую останню дату як today: {day_key} ({date_str})")
    
    return result

# --- Функція для отримання кольору за станом ---
def get_color_for_state(state: str) -> tuple:
    color_map = {
        "yes": AVAILABLE_COLOR,
        "no": OUTAGE_COLOR,
        "maybe": POSSIBLE_COLOR,
        "first": OUTAGE_COLOR,
        "second": OUTAGE_COLOR,
        "mfirst": POSSIBLE_COLOR,
        "msecond": POSSIBLE_COLOR
    }
    return color_map.get(state, AVAILABLE_COLOR)

# --- Функція для отримання опису стану ---
def get_description_for_state(state: str, preset: dict) -> str:
    time_type = preset.get("time_type", {})
    descriptions = {
        "yes": "Світло є",
        "no": "Світла немає", 
        "maybe": "Можливе відключення",
        "first": "Світла не буде перші 30 хв.",
        "second": "Світла не буде другі 30 хв.",
        "mfirst": "Світла можливо не буде перші 30 хв.",
        "msecond": "Світла можливо не буде другі 30 хв."
    }
    return time_type.get(state, descriptions.get(state, "Невідомий стан"))

# --- Функція для малювання розділеної клітинки ---
def draw_split_cell(draw, x0: int, y0: int, x1: int, y1: int, state: str, prev_state: str, next_state: str, outline_color: tuple):
    cell_width = x1 - x0
    half_width = cell_width // 2
    
    if state == "no":
        left_color = right_color = OUTAGE_COLOR
    elif state == "maybe":
        left_color = right_color = POSSIBLE_COLOR
    elif state == "yes":
        left_color = right_color = AVAILABLE_COLOR
    elif state == "first":
        left_color = OUTAGE_COLOR
        if next_state == "no":
            right_color = OUTAGE_COLOR
        elif next_state == "maybe":
            right_color = POSSIBLE_COLOR
        elif next_state in ["first", "mfirst"]:
            right_color = OUTAGE_COLOR if next_state == "first" else POSSIBLE_COLOR
        elif next_state in ["second", "msecond"]:
            right_color = AVAILABLE_COLOR
        else:
            right_color = AVAILABLE_COLOR
    elif state == "second":
        right_color = OUTAGE_COLOR
        if prev_state == "no":
            left_color = OUTAGE_COLOR
        elif prev_state == "maybe":
            left_color = POSSIBLE_COLOR
        elif prev_state in ["second", "msecond"]:
            left_color = OUTAGE_COLOR if prev_state == "second" else POSSIBLE_COLOR
        elif prev_state in ["first", "mfirst"]:
            left_color = AVAILABLE_COLOR
        else:
            left_color = AVAILABLE_COLOR
    elif state == "mfirst":
        left_color = POSSIBLE_COLOR
        if next_state == "no":
            right_color = OUTAGE_COLOR
        elif next_state == "maybe":
            right_color = POSSIBLE_COLOR
        elif next_state in ["first", "mfirst"]:
            right_color = OUTAGE_COLOR 
        elif next_state in ["second", "msecond"]:
            right_color = OUTAGE_COLOR
        else:
            right_color = AVAILABLE_COLOR
    elif state == "msecond":
        right_color = POSSIBLE_COLOR
        if prev_state == "no":
            left_color = OUTAGE_COLOR
        elif prev_state == "maybe":
            left_color = POSSIBLE_COLOR
        elif prev_state in ["second", "msecond"]:
            left_color = OUTAGE_COLOR
        elif prev_state in ["first", "mfirst"]:
            left_color = OUTAGE_COLOR 
        else:
            left_color = AVAILABLE_COLOR
    else:
        left_color = right_color = AVAILABLE_COLOR
    
    if left_color == right_color:
        draw.rectangle([x0, y0, x1, y1], fill=left_color, outline=outline_color)
    else:
        draw.rectangle([x0, y0, x0 + half_width, y1], fill=left_color)
        draw.rectangle([x0 + half_width, y0, x1, y1], fill=right_color)

# --- Основна функція рендерингу ---
def render_single_date(data: dict, day_ts: int, day_key: str, output_filename: str, date_str: str):
    fact = data.get("fact", {})
    preset = data.get("preset", {}) or {}
    
    day_map = fact["data"].get(day_key, {})

    # Сортування груп
    def sort_key(s):
        try:
            if "GPV" in s:
                import re
                m = re.search(r"(\d+)", s)
                return (0, int(m.group(1)) if m else s)
        except Exception:
            pass
        return (1, s)
    groups = sorted(list(day_map.keys()), key=sort_key)
    rows = groups

    n_hours = 24
    n_rows = max(1, len(rows))
    width = SPACING*2 + LEFT_COL_W + n_hours*CELL_W
    height = SPACING*2 + HEADER_H + HOUR_ROW_H + n_rows*CELL_H + LEGEND_H + 40

    img = Image.new("RGB", (width, height), BG)
    draw = ImageDraw.Draw(img)

    # --- Шрифти ---
    font_title = pick_font(TITLE_FONT_SIZE, bold=True)
    font_hour = pick_font(HOUR_FONT_SIZE)
    font_group = pick_font(GROUP_FONT_SIZE)
    font_small = pick_font(SMALL_FONT_SIZE)
    font_legend = pick_font(LEGEND_FONT_SIZE)

    # --- Заголовок ---
    title_text = f"Графік погодинних відключень на {date_str}"
    bbox = draw.textbbox((0,0), title_text, font=font_title)
    w_title = bbox[2] - bbox[0]
    h_title = bbox[3] - bbox[1]
    title_x = SPACING + (LEFT_COL_W + n_hours*CELL_W - w_title) / 2
    title_y = SPACING + 6
    draw.text((title_x, title_y), title_text, fill=TEXT_COLOR, font=font_title)

    # --- Таблиця ---
    table_x0 = SPACING
    table_y0 = SPACING + HEADER_H + HOUR_ROW_H + HEADER_SPACING
    table_x1 = table_x0 + LEFT_COL_W + n_hours*CELL_W
    table_y1 = table_y0 + n_rows*CELL_H
    draw.rectangle([table_x0, table_y0, table_x1, table_y1], fill=TABLE_BG, outline=GRID_COLOR)

    # --- Рядок годин ---
    hour_y0 = table_y0 - HOUR_ROW_H
    hour_y1 = table_y0
    for h in range(24):
        x0 = table_x0 + LEFT_COL_W + h*CELL_W
        x1 = x0 + CELL_W
        draw.rectangle([x0, hour_y0, x1, hour_y1], fill=HEADER_BG, outline=GRID_COLOR)
        start = f"{h:02d}"
        middle = "-"
        end = f"{(h+1)%24:02d}"
        bbox1 = draw.textbbox((0,0), start, font=font_hour)
        bbox2 = draw.textbbox((0,0), middle, font=font_hour)
        bbox3 = draw.textbbox((0,0), end, font=font_hour)
        h1 = bbox1[3]-bbox1[1]
        h2 = bbox2[3]-bbox2[1]
        h3 = bbox3[3]-bbox3[1]
        total_h = h1 + HOUR_LINE_GAP + h2 + HOUR_LINE_GAP + h3
        y_cursor = hour_y0 + (HOUR_ROW_H - total_h)/2
        draw.text((x0 + (CELL_W - (bbox1[2]-bbox1[0]))/2, y_cursor), start, fill=TEXT_COLOR, font=font_hour)
        y_cursor += h1 + HOUR_LINE_GAP
        draw.text((x0 + (CELL_W - (bbox2[2]-bbox2[0]))/2, y_cursor), middle, fill=TEXT_COLOR, font=font_hour)
        y_cursor += h2 + HOUR_LINE_GAP
        draw.text((x0 + (CELL_W - (bbox3[2]-bbox3[0]))/2, y_cursor), end, fill=TEXT_COLOR, font=font_hour)

    # --- Ліва колонка ---
    left_label = "Черга"
    draw.rectangle([table_x0, hour_y0, table_x0+LEFT_COL_W, hour_y1], fill=HEADER_BG, outline=GRID_COLOR)
    bbox = draw.textbbox((0,0), left_label, font=font_hour)
    draw.text((table_x0 + (LEFT_COL_W - (bbox[2]-bbox[0]))/2, hour_y0 + (HOUR_ROW_H - (bbox[3]-bbox[1]))/2),
              left_label, fill=TEXT_COLOR, font=font_hour)

    # --- Рядки груп і клітинки ---
    for r, group in enumerate(rows):
        y0 = table_y0 + r*CELL_H
        y1 = y0 + CELL_H
        draw.rectangle([table_x0, y0, table_x0 + LEFT_COL_W, y1], outline=GRID_COLOR, fill=TABLE_BG)
        label = group.replace("GPV", "").strip()
        bbox = draw.textbbox((0,0), label, font=font_group)
        draw.text((table_x0 + (LEFT_COL_W - (bbox[2]-bbox[0]))/2, y0 + (CELL_H - (bbox[3]-bbox[1]))/2),
                  label, fill=TEXT_COLOR, font=font_group)

        gp_hours = day_map.get(group, {}) if isinstance(day_map.get(group, {}), dict) else {}
        for h in range(24):
            h_key = str(h + 1)
            state = gp_hours.get(h_key, "yes")
            
            prev_h_key = str(h) if h > 0 else "24"
            next_h_key = str(h + 2) if h < 23 else "1"
            prev_state = gp_hours.get(prev_h_key, "yes")
            next_state = gp_hours.get(next_h_key, "yes")
            
            x0h = table_x0 + LEFT_COL_W + h*CELL_W
            x1h = x0h + CELL_W
            
            draw_split_cell(draw, x0h, y0, x1h, y1, state, prev_state, next_state, GRID_COLOR)

    # --- Лінії сітки ---
    for i in range(0, 25):
        x = table_x0 + LEFT_COL_W + i*CELL_W
        draw.line([(x, table_y0 - HOUR_ROW_H), (x, table_y1)], fill=GRID_COLOR)
    for r in range(n_rows+1):
        y = table_y0 + r*CELL_H
        draw.line([(table_x0, y), (table_x1, y)], fill=GRID_COLOR)

    # --- Легенда ---
    legend_states = ["yes", "no", "maybe"]
    legend_y_start = table_y1 + 15
    box_size = 18
    gap = 15
    
    x_cursor = SPACING
    for state in legend_states:
        color = get_color_for_state(state)
        description = get_description_for_state(state, preset)
        text_bbox = draw.textbbox((0,0), description, font=font_legend)
        w_text = text_bbox[2] - text_bbox[0]
        
        draw.rectangle([x_cursor, legend_y_start, x_cursor + box_size, legend_y_start + box_size], 
                      fill=color, outline=GRID_COLOR)
        draw.text((x_cursor + box_size + 4, legend_y_start + (box_size - (text_bbox[3]-text_bbox[1]))/2), 
                 description, fill=TEXT_COLOR, font=font_legend)
        x_cursor += box_size + 4 + w_text + gap

    # --- Інформація про публікацію ---
    pub_text = fact.get("update") or data.get("lastUpdated") or datetime.now(ZoneInfo('Europe/Kyiv')).strftime("%d.%m.%Y")
    pub_label = f"Опубліковано {pub_text}"
    bbox_pub = draw.textbbox((0,0), pub_label, font=font_small)
    w_pub = bbox_pub[2] - bbox_pub[0]
    pub_x = width - w_pub - SPACING
    pub_y = legend_y_start + box_size + 20
    draw.text((pub_x, pub_y), pub_label, fill=FOOTER_COLOR, font=font_small)

    out_path = OUT_DIR / output_filename
    scale = 3
    img_resized = img.resize((img.width*scale, img.height*scale), resample=Image.LANCZOS)
    img_resized.save(out_path, optimize=True)
    log(f"✅ Збережено {out_path}")

# --- Головна функція рендерингу ---
def render(data: dict, json_path: Path):
    fact = data.get("fact", {})
    if "today" not in fact or "data" not in fact:
        raise ValueError("JSON не містить ключі 'fact.today' або 'fact.data'")

    # Отримуємо всі дати для генерації
    dates_to_generate = get_dates_to_generate(fact["data"])
    
    log(f"📅 Буде згенеровано {len(dates_to_generate)} зображень(я)")
    
    # Генеруємо зображення для кожної дати
    for day_ts, day_key, filename, date_str in dates_to_generate:
        log(f"🖼️ Генерую {filename} для дати {date_str}")
        render_single_date(data, day_ts, day_key, filename, date_str)

def generate_from_json(json_path):
    path = Path(json_path)
    if not path.exists():
        log(f"❌ JSON файл не знайдено: {json_path}")
        raise FileNotFoundError(f"JSON файл не знайдено: {json_path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    log(f"▶️ Запускаю генерацію зображень з {json_path}")
    render(data, path)

def main():
    try:
        data, path = load_latest_json(JSON_DIR)
    except Exception as e:
        log(f"❌ Помилка при завантаженні JSON: {e}")
        sys.exit(1)
    
    log("▶️ Запускаю генерацію зображень з " + str(path))
    try:
        render(data, path)
    except Exception as e:
        log(f"❌ Помилка під час рендерингу: {e}")
        raise

if __name__ == "__main__":
    main()