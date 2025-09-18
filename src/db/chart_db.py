from datetime import datetime, date

import pandas as pd
# from sqlalchemy.dialects.postgresql import psycopg2
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import text

from src.db.database import engine
from src.utils.utils import process_tickers


def insert_chart(input_df: pd.DataFrame):
    """
    1) 등락률 제외한 chart - input_df 입력
    2) chart_date 타입 변환
    3) stocks 테이블에서 stock_id 매핑
    4) db에 INSERT, 중복 무시
    """

    df = input_df.copy()
    df['chart_date'] = pd.to_datetime(df['chart_date']).dt.date
    total = len(df)

    # ticker → stock_id 매핑
    with engine.begin() as conn:
        stock_map = pd.read_sql(
            text("SELECT id AS stock_id, ticker FROM public.stocks"),
            conn
        )
    print(df.head(), flush=True)

    df = df.merge(stock_map, on='ticker', how='left')
    df = df.drop(columns='ticker')

    print(df[['chart_date', 'stock_id']].tail(10))

    # execute_values 로 대량 INSERT + 중복 무시
    records = df[[
        'chart_date', 'chart_open', 'chart_high',
        'chart_low', 'chart_close', 'chart_volume',
        'chart_turnover', 'stock_id'
    ]].itertuples(index=False, name=None)

    sql = """
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
              RETURNING id AS chart_id;
          """

    with engine.begin() as conn:
        cur = conn.connection.cursor()
        rows = psycopg2.extras.execute_values(cur, sql, records, page_size=1000, fetch=True)
    inserted = len(rows)  # 실제 삽입된 행 수
    skipped = total - inserted  # 충돌로 스킵된 행 수
    print(">>> 삽입: ", inserted, ">>> 스킵: ", skipped)


def fetch_chart_to_df_by_date(start: int, end: int):
    """
    1) start, end date 입력받고 해당 날짜에 해당하는 chart 차트가 있는지 조회
    2) * df 리턴
    """
    start_date = datetime.strptime(str(start), "%Y%m%d").date()
    end_date = datetime.strptime(str(end), "%Y%m%d").date()

    sql = text("""
               SELECT c.id,
                      c.stock_id,
                      s.ticker,
                      c.chart_date,
                      c.chart_open,
                      c.chart_high,
                      c.chart_low,
                      c.chart_close,
                      c.chart_volume,
                      c.chart_turnover,
                      c.chart_change_percentage
               FROM public.charts AS c
                        LEFT JOIN public.stocks AS s ON s.id = c.stock_id
               WHERE c.chart_date BETWEEN :start_date AND :end_date
               ORDER BY c.stock_id, c.chart_date
               """)

    with engine.begin() as conn:
        df = pd.read_sql_query(
            sql,
            conn,
            params={"start_date": start_date, "end_date": end_date},
            parse_dates=["chart_date"]
        )
    return df


def fetch_chart_to_df_by_ticker_and_date(ticker: str, start: int, end: int):
    """
    1) cybos_ticker, start, end date 입력받고 해당하는 chart 차트가 있는지 조회
    2) 날짜,시고저종거래량,id,stock_id df 리턴
    """
    start_date = datetime.strptime(str(start), "%Y%m%d").date()
    end_date = datetime.strptime(str(end), "%Y%m%d").date()

    sql = text("""
               SELECT c.id AS chart_id,
                      c.chart_date,
                      c.chart_open,
                      c.chart_high,
                      c.chart_low,
                      c.chart_close,
                      c.chart_volume,
                      c.stock_id
               FROM public.charts AS c
                        LEFT JOIN public.stocks AS s ON s.id = c.stock_id
               WHERE c.chart_date BETWEEN :start_date AND :end_date
                 AND s.ticker = :ticker
               ORDER BY c.stock_id, c.chart_date
               """)

    with engine.begin() as conn:
        df = pd.read_sql_query(
            sql,
            conn,
            params={"start_date": start_date, "end_date": end_date, "ticker": ticker},
            parse_dates=["chart_date"]
        )
    return df


def update_chart_change_percentage(start: int, end: int):
    """
        1) start, end date 입력받고 date 변환
        2) 윈도우 함수로 이전 종가(lag) 가져와 등락률 계산, DB 업데이트
        """

    start_date = datetime.strptime(str(start), "%Y%m%d").date()
    end_date = datetime.strptime(str(end), "%Y%m%d").date()

    sql = text("""
               WITH chart_pct AS (SELECT stock_id,
                                         chart_date,
                                         -- lag()로 전일 종가
                                         lag(chart_close) OVER (PARTITION BY stock_id ORDER BY chart_date) AS prev_close, chart_close
                                  FROM public.charts)
               UPDATE public.charts AS c
               SET chart_change_percentage =
                       ((c.chart_close - cp.prev_close):: double precision
                   / cp.prev_close) * 100
               FROM chart_pct AS cp
               WHERE
                   c.stock_id = cp.stock_id
                 AND c.chart_date = cp.chart_date
                 AND cp.prev_close IS NOT NULL -- 첫 날 건너뛰기
                 AND c.chart_date BETWEEN :start_date
                 AND :end_date
               ;
               """)

    with engine.begin() as conn:
        result = conn.execute(sql, {"start_date": start_date, "end_date": end_date})
        print(f">>> 등락률 업데이트: {result.rowcount}건")


def select_previous_chart(stock_id: int, chart_date: date) -> pd.DataFrame:
    """
    1) chart_id 입력
    2) chart_id의 날짜를 포함해서 이전 60일까지의 시고저종거래량을 db에서 받아오고 리턴(날짜는내림차순)
    3) 만약 df의 총 행 개수가 60을 넘지 않는다면 함수 바깥에서 continue
    """
    sql = text("""
               SELECT c.id AS chart_id,
                      c.chart_date,
                      c.chart_open,
                      c.chart_high,
                      c.chart_low,
                      c.chart_close,
                      c.chart_volume,
                      c.stock_id
               FROM public.charts AS c
               WHERE c.stock_id = :stock_id
                 AND c.chart_date < :chart_date
               ORDER BY c.chart_date DESC LIMIT 60
               """)

    with engine.begin() as conn:
        df = pd.read_sql(sql, conn, params={'stock_id': stock_id, 'chart_date': chart_date})
        print(f">>> 이전 chart 조회 완료: {len(df)}건")
        return df
