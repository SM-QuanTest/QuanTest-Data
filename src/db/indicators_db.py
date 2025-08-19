import pandas as pd

import psycopg2
from psycopg2.extras import execute_values

from src.db.database import engine


def insert_indicator(input_df: pd.DataFrame):
    """
    1) 등락률 제외한 chart - input_df 입력
    2) chart_date 타입 변환
    3) stocks 테이블에서 stock_id 매핑
    4) db에 INSERT, 중복 무시
    """

    df = input_df.copy()
    total = len(df)

    records = df[[
        'chart_id', 'indicators_value', 'indicators_name'
    ]].itertuples(index=False, name=None)

    sql = """
          INSERT INTO public.daily_indicators (chart_id,
                                               indicators_value,
                                               indicators_name)
          VALUES %s ON CONFLICT
          ON CONSTRAINT daily_indicators_name_chart_unique
              DO NOTHING
              RETURNING id AS daily_indicators_id;
          """

    with engine.begin() as conn:
        cur = conn.connection.cursor()
        rows = psycopg2.extras.execute_values(cur, sql, records, page_size=1000, fetch=True)
    inserted = len(rows)  # 실제 삽입된 행 수
    skipped = total - inserted  # 충돌로 스킵된 행 수
    print(">>> 삽입: ", inserted, ">>> 스킵: ", skipped)
