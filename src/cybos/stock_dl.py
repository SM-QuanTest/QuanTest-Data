import pandas as pd

from .sector_name_dl import process_sector_name
from ..config.cybos_config import objCpCodeMgr, process_tickers


def find_stock_name(cybos_ticker: pd.DataFrame) -> pd.DataFrame:
    """
    1) cybos_ticker df를 입력
    2) 주식 이름 df 리턴
    """
    stock_name = objCpCodeMgr.CodeToName(cybos_ticker)
    return stock_name

def save_stock(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    1) cybos_ticker df를 입력
    2) 그에 따른 ticker, 주식이름, 종목이름 리턴
    """

    stock_df = input_df.copy()
    stock_df['ticker'] = stock_df['cybos_ticker'].apply(process_tickers)
    stock_df['stock_name'] = stock_df['cybos_ticker'].apply(find_stock_name)
    stock_df['sector_name'] = stock_df['cybos_ticker'].apply(process_sector_name)

    print(stock_df, len(stock_df))
    return stock_df
