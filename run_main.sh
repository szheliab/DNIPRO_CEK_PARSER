#!/bin/bash
# ----------------- run_main.sh -----------------

BASE_DIR="/home/yaroslav/bots/CHERKASY_PARSER"
VENV_DIR="$BASE_DIR/venv"
LOG_FILE="$BASE_DIR/logs/cron_main.log"
LOG_DIR="$BASE_DIR/logs"
FULL_LOG_FILE="${LOG_DIR}/full_log.log"


# --- Підготовка ---
mkdir -p "$BASE_DIR/out" "$LOG_DIR" # створення директорій, якщо їх немає


# Активуємо venv
source "$VENV_DIR/bin/activate"

# Переходимо в папку проекту
cd "$BASE_DIR"

# Запускаємо main.py
# Завантажити зображення і обробити їх
python3 src/main.py --parse #>> "$LOG_FILE" 2>&1

# Додаємо порожній рядок для кращої читабельності логів
echo | tee -a "$FULL_LOG_FILE"
