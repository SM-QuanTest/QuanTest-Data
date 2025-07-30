import pandas as pd
import win32com.client
from pathlib import Path

objCpCodeMgr = win32com.client.Dispatch("CpUtil.CpCodeMgr")


def load_cybos_tickers(input_csv: Path) -> pd.DataFrame:
    """
    1) input_csv를 읽어서
    2) ticker를 컬럼으로 갖는 DataFrame 리턴 (ticker명 'A'포함(cybos))
    """
    df = pd.read_csv(input_csv, header=None, names=['cybos_ticker'])
    return df


def process_tickers(cybos_ticker: str) -> str:
    """
    1) cybos_ticker 하나를 입력받아서
    2) 'A'를 뗀 일반 ticker str 리턴
    """

    return cybos_ticker.lstrip('A')
