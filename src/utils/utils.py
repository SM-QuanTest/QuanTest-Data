from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.database import engine


def load_cybos_tickers_db() -> pd.DataFrame:
    """
    1) db에 저장돼있는 tickers를 읽어서
    2) cybos_ticker를 컬럼으로 갖는 DataFrame 리턴 (ticker명 'A'포함(cybos))
    """
    with engine.connect() as conn:
        try:
            df = pd.read_sql(
                text("SELECT ticker FROM public.stocks"),
                con=conn
            )
        except SQLAlchemyError as e:
            raise RuntimeError(f"DB 조회 실패: {e}")

    df['cybos_ticker'] = 'A' + df['ticker']
    return df[['cybos_ticker']]


def load_cybos_tickers_csv(input_csv: Path) -> pd.DataFrame:
    """
    1) input_csv를 읽어서
    2) cybos_ticker를 컬럼으로 갖는 DataFrame 리턴 (ticker명 'A'포함(cybos))
    """
    df = pd.read_csv(input_csv, header=None, names=['cybos_ticker'])
    return df


def process_tickers(cybos_ticker: str) -> str:
    """
    1) cybos_ticker 하나를 입력받아서
    2) 'A'를 뗀 일반 ticker str 리턴
    """

    return cybos_ticker.lstrip('A')


def process_cybos_tickers(ticker: str) -> str:
    """
    1) ticker 하나를 입력받아서
    2) 'A'를 붙인 cybos ticker str 리턴
    """

    return 'A' + ticker


def cybos_ticker_list_to_df(cybos_ticker_list: list) -> pd.DataFrame:
    """
    1) cybos_ticker_list를 입력받아서
    2) cybos_ticker df로 변환 후 리턴
    """
    df = pd.DataFrame()
    df['cybos_ticker'] = cybos_ticker_list
    return df


def process_chart_list_to_df(chart_list: list) -> pd.DataFrame:
    """
    1) chart 리스트 입력
    2) cols 설정
    2) date 형식 변환
    3) 거래대금까지 계산 (등락률은 x)
    4) df 리턴
    """
    cols = [
        'ticker',
        'chart_date',
        'chart_open',
        'chart_high',
        'chart_low',
        'chart_close',
        'chart_volume'
    ]

    chart_df = pd.DataFrame(chart_list, columns=cols)

    chart_df['ticker'] = chart_df['ticker'].apply(process_tickers)
    chart_df['chart_date'] = pd.to_datetime(
        chart_df['chart_date'].astype(str),
        format='%Y%m%d'
    ).dt.date
    chart_df['chart_turnover'] = chart_df['chart_close'] * chart_df['chart_volume']

    return chart_df



def process_indicator_df_to_long_df(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    1) wide 형식의 indicator_df 입력(id AS chart_id + 각종 지표들)
    2) 리턴해야하는 df의 cols [chart_id, indicators_value, indicators_name]
    3) input_df에 col 이름으로 들어가있는 지표명을 indicators_name 컬럼에다가 넣어주고, 그 컬럼에 있는 row들의 값을 indicators_value에 넣어줘야함
    """

    df = input_df.copy()

    id_cols = ["chart_id"]
    value_cols = [c for c in input_df.columns if c not in id_cols]

    long_df = input_df.melt(
        id_vars=id_cols,
        value_vars=value_cols,
        var_name="indicator_line_name",
        value_name="daily_indicator_value"
    )

    long_df["daily_indicator_value"] = pd.to_numeric(long_df["daily_indicator_value"], errors="coerce")
    long_df = long_df.dropna(subset=["daily_indicator_value"])

    long_df = (long_df
    .sort_values(["chart_id", "indicator_line_name"])
    .reset_index(drop=True)
    [["chart_id", "daily_indicator_value", "indicator_line_name"]]
    )
    return long_df
