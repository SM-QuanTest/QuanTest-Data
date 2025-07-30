import pandas as pd
from pathlib import Path
from ..config.cybos_config import objCpCodeMgr

def find_cybos_sector_code(cybos_ticker: str) -> str:
    """
    1) cybos_ticker를 입력
    2) cybos 업종 코드를 리턴
    """
    cybos_sector_code = objCpCodeMgr.GetStockIndustryCode(cybos_ticker)
    return cybos_sector_code


def find_cybos_sector_name(cybos_sector_code) -> str:
    """
    1) cybos_sector_code를 입력
    2) 업종 이름을 리턴
    """
    cybos_sector_name = objCpCodeMgr.GetIndustryName(cybos_sector_code)
    return cybos_sector_name

def process_sector_name(input: str) -> str:
    """
    1. cybos_ticker 입력
    2. sector_name 리턴
    """
    cybos_sector_code = find_cybos_sector_code(input)
    cybos_sector_name = find_cybos_sector_name(cybos_sector_code)
    if cybos_sector_name.startswith("코스닥 "):
        cybos_sector_name = cybos_sector_name[4:]
    return cybos_sector_name

def save_sector_name(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    1) cybos_ticker df를 입력
    2) 해당 ticker들이 가지고있는 업종 이름 df를 리턴
    """
    sector_set = set()

    for ct in input_df['cybos_ticker']:
        cybos_sector_name = process_sector_name(ct)
        sector_set.add(cybos_sector_name)

    print(sector_set)
    sector_df = pd.DataFrame(list(sector_set), columns=['sector_name'])
    print(sector_df)
    return sector_df



