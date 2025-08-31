import pandas as pd

import psycopg2
from psycopg2.extras import execute_values

from src.db.database import engine


def insert_daily_indicator(input_df: pd.DataFrame):
    """
    1) input_df 입력(long_df)
    2)
    """

    df = input_df[['chart_id', 'indicator_line_name', 'daily_indicator_value']].copy()
    df.dropna(subset=['chart_id', 'indicator_line_name', 'daily_indicator_value'], inplace=True)
    df['indicator_line_name'] = df['indicator_line_name'].astype(str).str.strip()

    records = [
        (int(row.chart_id), row.indicator_line_name, float(row.daily_indicator_value))
        for row in df.itertuples(index=False)
    ]
    total = len(records)
    if total == 0:
        print(">>> 삽입: 0 >>> 스킵: 0 (빈 입력)")
        return

    sql = """
          WITH v(chart_id, indicator_line_name, daily_indicator_value) AS (VALUES %s)
          INSERT 
          INTO public.daily_indicators (chart_id,
                                        indicator_line_id,
                                        daily_indicator_value)
          SELECT v.chart_id,
                 il.id AS indicator_line_id,
                 v.daily_indicator_value
          FROM v
                   JOIN public.indicators_lines AS il
                        ON il.indicator_line_name = v.indicator_line_name 
          ON CONFLICT
                   ON CONSTRAINT indicator_line_id_chart_unique
                   DO NOTHING
          RETURNING id AS daily_indicators_id;
          """

    with engine.begin() as conn:
        cur = conn.connection.cursor()
        rows = psycopg2.extras.execute_values(cur, sql, records, template="(%s, %s, %s)", page_size=1000, fetch=True)
    inserted = len(rows)  # 실제 삽입된 행 수
    skipped = total - inserted  # 충돌로 스킵된 행 수
    print(">>> 삽입: ", inserted, ">>> 스킵: ", skipped)
