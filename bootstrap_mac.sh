#!/usr/bin/env bash

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"

if ! command -v python3 >/dev/null 2>&1; then
  cat <<'EOF'
Python 3 не знайдено.

Встановіть Python 3 одним із способів:
1. Завантажте інсталятор з https://www.python.org/downloads/macos/
2. Або встановіть Homebrew і виконайте: brew install python

Після цього запустіть bootstrap ще раз.
EOF
  exit 1
fi

echo "==> Перехід до каталогу проєкту: ${PROJECT_DIR}"
cd "${PROJECT_DIR}"

if [[ ! -d "${VENV_DIR}" ]]; then
  echo "==> Створення віртуального оточення"
  python3 -m venv "${VENV_DIR}"
fi

echo "==> Активація віртуального оточення"
source "${VENV_DIR}/bin/activate"

echo "==> Оновлення pip"
python -m pip install --upgrade pip

echo "==> Встановлення залежностей"
pip install -r requirements.txt

echo "==> Запуск локального застосунку"
exec streamlit run app.py
