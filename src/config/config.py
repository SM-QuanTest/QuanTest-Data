import json
import os
from pathlib import Path

from dotenv import load_dotenv, find_dotenv

env_path = find_dotenv(filename=".env", usecwd=True)

if not env_path:
    PROJ_ROOT = Path(__file__).resolve().parents[2]  # .../QuanTest-Data
    candidate = PROJ_ROOT / ".env"
    if candidate.exists():
        env_path = str(candidate)

if env_path:
    load_dotenv(env_path)
else:
    pass


DB_URL = os.getenv("DB_URL")
# API_ENDPOINT = os.getenv("API_ENDPOINT")
# CRON_EXPR    = os.getenv("CRON_EXPR", "0 0 * * *")  # 기본: 매일 자정
PROJECT_ROOT = os.getenv("PROJECT_ROOT")

CYBOS_TICKER_LIST = json.loads(os.getenv("CYBOS_TICKER_LIST"))
