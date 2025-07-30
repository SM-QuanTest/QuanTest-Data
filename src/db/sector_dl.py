import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.database import engine


def insert_sectors(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    1) input_df['sector_name']에서 중복 제거
    2) 기존 public.sectors 테이블에 없는 이름만 INSERT
    3) 신규로 삽입된 sector_name 목록 리턴
    """
    # transaction 시작
    with engine.begin() as conn:
        # 기존 업종 조회
        try:
            existing = pd.read_sql(
                text("SELECT sector_name FROM public.sectors"),
                con=conn
            )['sector_name'].tolist()
        except SQLAlchemyError as e:
            raise RuntimeError(f"DB 조회 실패: {e}")

        # 유니크
        unique_sectors = input_df['sector_name'].drop_duplicates()
        to_insert = unique_sectors[~unique_sectors.isin(existing)]
        if to_insert.empty:
            print(">>> 추가할 신규 업종이 없습니다.")
            return pd.DataFrame(columns=['sector_name'])

        insert_df = to_insert.to_frame(name='sector_name')

        # 대량 INSERT
        try:
            insert_df.to_sql(
                'sectors',
                con=conn,  # 커넥션 객체 전달
                schema='public',
                if_exists='append',
                index=False
            )
        except SQLAlchemyError as e:
            raise RuntimeError(f"DB 삽입 실패: {e}")

        print(f">>> 신규 업종 {len(to_insert)}건 삽입 완료.")
        return insert_df
