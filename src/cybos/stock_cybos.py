import pandas as pd
import win32com

from .sector_cybos import process_sector_name
from ..config.cybos_config import get_obj_cp_code_mgr
from ..utils.utils import process_tickers


def get_code_cybos_ticker() -> pd.DataFrame:
    """
    1) 모든 kosdaq_cybos_ticker의 값 가져오기
    2) df로 kosdaq_cybos_ticker 리턴
    """
    print("get_kosdaq_cybos_ticker 시작")
    obj_cp_code_mgr = get_obj_cp_code_mgr()
    kosdaq_cybos_ticker_list = obj_cp_code_mgr.GetStockListByMarket(2)  # 2는 코스닥 시장
    df = pd.DataFrame({'cybos_ticker': kosdaq_cybos_ticker_list})
    return df


def find_stock_name(cybos_ticker: str) -> pd.DataFrame:
    """
    1) cybos_ticker string을 입력
    2) 주식 이름 df 리턴
    """
    obj_cp_code_mgr = get_obj_cp_code_mgr()
    stock_name = obj_cp_code_mgr.CodeToName(cybos_ticker)
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
