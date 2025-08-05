import io

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.database import engine


def insert_chart(input_df: pd.DataFrame):
    """
    1) 등락률 제외한 chart - input_df 입력
    2) chart_date 타입 변환
    3) stocks 테이블에서 stock_id 매핑
    4) db에 INSERT, 중복 무시
    """

    df = input_df.copy()
    df['chart_date'] = pd.to_datetime(df['chart_date']).dt.date

    # ticker → stock_id 매핑
    with engine.begin() as conn:
        stock_map = pd.read_sql(
            text("SELECT id AS stock_id, ticker FROM public.stocks"),
            conn
        )
    print(df.head(), flush=True)

    df = df.merge(stock_map, on='ticker', how='left')
    df = df.drop(columns='ticker')

    # execute_values 로 대량 INSERT + 중복 무시
    records = df[[
        'chart_date', 'chart_open', 'chart_high',
        'chart_low', 'chart_close', 'chart_volume',
        'chart_turnover', 'stock_id'
    ]].itertuples(index=False, name=None)

    sql = text("""
               INSERT INTO public.charts (chart_date,
                                          chart_open,
                                          chart_high,
                                          chart_low,
                                          chart_close,
                                          chart_volume,
                                          chart_turnover,
                                          stock_id)
               VALUES %s ON CONFLICT
               ON CONSTRAINT charts_stock_date_unique
                   DO NOTHING
                   RETURNING (xmax = 0) AS inserted;
               """)

    with engine.begin() as conn:
        cur = conn.connection.cursor()
        execute_values(cur, sql.text, records)

        # RETURNING 결과가 있을 때만 fetch
        if cur.description is not None:
            results = cur.fetchall()
            inserted = sum(r[0] for r in results)
            updated = len(results) - inserted
        else:
            inserted = 0
            updated = 0

        print(f">>> 삽입: {inserted}건, >>> 갱신: {updated}건")
