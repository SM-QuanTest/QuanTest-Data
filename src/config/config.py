from dotenv import load_dotenv
import os
from pathlib import Path

# config.py 파일 위치 기준으로 상위(또는 프로젝트) 루트의 .env 파일 로드
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DB_URL = os.getenv("DB_URL")
# API_ENDPOINT = os.getenv("API_ENDPOINT")
# CRON_EXPR    = os.getenv("CRON_EXPR", "0 0 * * *")  # 기본: 매일 자정
PROJECT_ROOT = os.getenv("PROJECT_ROOT")