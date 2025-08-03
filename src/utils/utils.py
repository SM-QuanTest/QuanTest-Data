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

    return 'A'+ticker
