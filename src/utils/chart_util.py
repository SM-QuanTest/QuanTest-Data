import pandas as pd

from src.utils.utils import process_tickers


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
