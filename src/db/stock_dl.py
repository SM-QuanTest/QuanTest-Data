import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.database import engine

def insert_stocks(
        stock_df: pd.DataFrame,
    ) -> pd.DataFrame:
    """
    1) cybos_ticker, ticker, stock_name, sector_name을 가진 df 입력
    2) sectors 테이블에서 sector_id 매핑
    3) stock(ticker, stock_name, sector_id)에 INSERT
    """

    with engine.begin() as conn:
        # 기존 종목 조회
        try:
            existing = pd.read_sql(
                text("SELECT stock_name FROM public.stocks"),
                con=conn
            )['stock_name'].tolist()
        except SQLAlchemyError as e:
            raise RuntimeError(f"DB 조회 실패: {e}")

        unique_df = stock_df[['ticker','stock_name','sector_name']].drop_duplicates()
        to_insert = unique_df[~unique_df['stock_name'].isin(existing)]
        if to_insert.empty:
            print(">>> 추가할 신규 종목이 없습니다.")
            return pd.DataFrame(columns=['ticker', 'stock_name', 'sector_id'])

        # sectors 테이블에서 sector_id 가져오기
        sectors_df = pd.read_sql(
            text("SELECT id AS sector_id, sector_name FROM public.sectors"),
            con=conn
        )

        # sector_name → sector_id 매핑
        merged = to_insert.merge(
            sectors_df,
            on='sector_name',
            how='left'
        )
        # 매핑 실패
        if merged['sector_id'].isna().any():
            missing = merged.loc[merged['sector_id'].isna(), 'sector_name'].unique()
            raise RuntimeError(f"알 수 없는 sector_name 발생: {missing.tolist()}")

        # INSERT용 df
        insert_df = merged[['ticker', 'stock_name', 'sector_id']]

        # bulk INSERT
        try:
            insert_df.to_sql(
                'stocks',
                con=conn,
                schema='public',
                if_exists='append',
                index=False
            )
        except SQLAlchemyError as e:
            raise RuntimeError(f"DB 삽입 실패: {e}")

        print(f">>> 신규 종목 {len(insert_df)}건 삽입 완료.")
        return insert_df

