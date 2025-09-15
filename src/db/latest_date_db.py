from datetime import datetime
from sqlalchemy import text
from src.db.database import engine


def update_latest_date(name: str, date: int):
    """
        1) name, date 입력
        2) name의 이름을 가진 컬럼의 latest_date를 업데이트
        """

    latest_date = datetime.strptime(str(date), "%Y%m%d").date()

    sql = text("""
               UPDATE public.latest_date
               SET latest_date = :latest_date
               WHERE latest_date_name = :name
               """)

    with engine.begin() as conn:
        result = conn.execute(sql, {"latest_date": latest_date, "name": name})
        print(f">>> {name} 최신 날짜 {latest_date} 로 업데이트 (rowcount={result.rowcount})")