import pandas as pd

from src.config.cybos_config import get_obj_market_eye


def get_market_cap(cybos_ticker: list[str]) -> dict[str, int]:
    """
    1) cybos_ticker 리스트 입력
    2) cybos_ticker, 시가총액 리턴
    """
    obj_market_eye = get_obj_market_eye()
    obj_market_eye.SetInputValue(0, (67, 4))  # 67: 시가총액(억원), 4: 현재가
    obj_market_eye.SetInputValue(1, cybos_ticker)
    obj_market_eye.BlockRequest()
    return {code: obj_market_eye.GetDataValue(0, idx)
            for idx, code in enumerate(cybos_ticker)}


def get_market_caps(input_df: pd.DataFrame, chunk_size: int = 200) -> pd.DataFrame:
    """
    1) 최소기준액 입력(기본값 5000)
    2) 모든 kosdaq_cybos_ticker의 값 df로 가져오기
    3) 시가총액 가져와서 df에 추가
    4) df 리턴
    """
    market_cap_df = input_df.copy()
    cybos_tickers = market_cap_df['cybos_ticker'].tolist()

    results: dict[str, int] = {}

    for i in range(0, len(cybos_tickers), chunk_size):
        batch = cybos_tickers[i:i + chunk_size]
        caps = get_market_cap(batch)
        results.update(caps)
        # time.sleep(0.25)

    df = pd.DataFrame.from_dict(results, orient='index', columns=['cap'])
    return df.reset_index().rename(columns={'index': 'cybos_ticker'})
