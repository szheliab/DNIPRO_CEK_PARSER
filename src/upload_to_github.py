#!/usr/bin/env python3
import os
import shutil
import subprocess
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

# ----------------- Налаштування -----------------
REGION = "Cherkasyoblenergo"   # <<<<<<<<<<<<<<<<<< ОБЛЕНЕРГО
BASE_DIR = Path(__file__).parent.parent.absolute()

#SOURCE_JSON = os.path.join(BASE_DIR, "out", f"{REGION}.json")
SOURCE_JSON = os.path.join(BASE_DIR, "out", "Cherkasyoblenergo.json")
SOURCE_IMAGES = os.path.join(BASE_DIR, "out/images")

# ----------------- ПРАВИЛЬНИЙ REPO -----------------
REPO_DIR = "/home/yaroslav/bots/OE_OUTAGE_DATA"

DATA_DIR = os.path.join(REPO_DIR, "data") # папка для json файлів
IMAGES_DIR = os.path.join(REPO_DIR, f"images/{REGION}") # папка для зображень цього регіону
#METADATA_FILE = os.path.join(DATA_DIR, f"last_updated_{REGION}.json")

LOG_FILE = os.path.join(BASE_DIR, "logs", "full_log.log")


def log(message):
    timestamp = datetime.now(ZoneInfo("Europe/Kyiv")).strftime("%Y-%m-%d %H:%M:%S")
    text = f"{timestamp} [upload_to_github_new] {message}"
    print(text)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    except:
        pass


def run_upload():
    log(f"🚀 Початок оновлення даних для {REGION}...")

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(os.path.join(REPO_DIR, "images"), exist_ok=True)

    # ------------------- JSON -------------------
    target_json = os.path.join(DATA_DIR, f"{REGION}.json")

    if os.path.exists(SOURCE_JSON):
        shutil.copy2(SOURCE_JSON, target_json)
        log(f"✅ JSON оновлено → {target_json}")
    else:
        log("❗ JSON не знайдено — припиняю оновлення!")
        return

    # ------------------- ЗОБРАЖЕННЯ -------------------
    if os.path.exists(IMAGES_DIR):
        shutil.rmtree(IMAGES_DIR)
        log("🗑 Видалено старі зображення")

    if os.path.exists(SOURCE_IMAGES):
        shutil.copytree(SOURCE_IMAGES, IMAGES_DIR)
        log(f"🖼 Нові зображення скопійовано → {IMAGES_DIR}")
    else:
        log("⚠️ Папка з новими зображеннями не знайдена")

    # ------------------- last_updated -------------------
    current_time = datetime.now(ZoneInfo('Europe/Kyiv'))
    #with open(METADATA_FILE, "w", encoding="utf-8") as f:
    #    json.dump({
    #        "region": REGION,
    #        "last_updated": current_time.strftime("%Y-%m-%d %H:%M:%S"),
    #        "timestamp": current_time.timestamp()
    #    }, f, indent=2)
#
    #log(f"🕒 Оновлено файл → {METADATA_FILE}")

    # ------------------- GIT -------------------
    try:
        log("▶️ git pull --rebase --autostash")
        subprocess.check_call(["git", "pull", "--rebase", "--autostash"], cwd=REPO_DIR)

        log("▶️ git add .")
        subprocess.check_call(["git", "add", "."], cwd=REPO_DIR)

        commit_msg = f"{REGION} update {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
        log(f"▶️ git commit -m '{commit_msg}'")

        if subprocess.run(["git", "diff", "--staged", "--quiet"], cwd=REPO_DIR).returncode != 0:
            subprocess.check_call(["git", "commit", "-m", commit_msg], cwd=REPO_DIR)
            log(f"✔️ Коміт: {commit_msg}")
        else:
            log("ℹ️ Змін для коміту немає")
            return

        log("▶️ git push")
        subprocess.check_call(["git", "push"], cwd=REPO_DIR)

        log("🎉 Дані опубліковано в GitHub")

    except subprocess.CalledProcessError as e:
        log(f"❌ ПОМИЛКА Git: {e}")
        raise e


if __name__ == "__main__":
    try:
        run_upload()
    except Exception as e:
        log(f"❌ Завантаження на GitHub не вдалося: {e}")
