#!/usr/bin/env bash
set -euo pipefail

python -m pip install --upgrade pip
python -m pip install --no-cache-dir -r /workspace/requirements.txt

exec python -m uvicorn main:app --host 0.0.0.0 --port "${APP_PORT:-8000}"





