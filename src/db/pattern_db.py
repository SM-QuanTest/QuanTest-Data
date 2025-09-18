from datetime import date

import pandas as pd

from src.config.config import CYBOS_TICKER_LIST
from src.db.chart_db import fetch_chart_to_df_by_ticker_and_date, select_previous_chart
from src.utils.utils import process_tickers


def fetch_candle_chart_pattern(start_date: date, end_date: date) -> pd.DataFrame:
    """
    1) start, end만큼의 날짜를 가지고 있는 종목별 chart_by_stock_df
    2) 해당 df서 start 이전의 60일치를 previous_chart_by_stock_df 추가
    3) 날짜와 시고저종, chart_id만 포함해 가공한 df
    4) 모든 함수에 두고 돌림 (이것도 함수에 따로 빼기), 이떄 true가 나오는 값이 있다면, df에다가 계속 추가함. (추가하는 것도 함수로 빼기) -> 추가 항목은 pattern_id, chart_id
    5) for문을 돌아가며 계속 추가된 pattern_df를 리턴

    """
    pattern_df = None

    for cybos_ticker in CYBOS_TICKER_LIST:
        chart_by_stock_df = fetch_chart_to_df_by_ticker_and_date(process_tickers(cybos_ticker), start_date, end_date)
        if chart_by_stock_df.empty:
            print("해당 기간 데이터 없음")
            continue
        stock_id = int(chart_by_stock_df.stock_id.iloc[0])
        chart_date = pd.to_datetime(chart_by_stock_df.chart_date.iloc[0]).date()  # TODO: 이게 start_date여야 함!!!!!!!!!
        previous_chart_by_stock_df = select_previous_chart(stock_id, chart_date)
        if previous_chart_by_stock_df is None or len(previous_chart_by_stock_df) < 60:
            print("이전 데이터 60건보다 부족")
            continue

        cols = ["chart_date", "chart_open", "chart_high", "chart_low", "chart_close", "chart_id"]

        previous_chart_by_stock_df_sel = previous_chart_by_stock_df.loc[:, cols]
        chart_by_stock_df_sel = chart_by_stock_df.loc[:, cols]

        concat_df_filtered = pd.concat([previous_chart_by_stock_df_sel, chart_by_stock_df_sel], ignore_index=True)

        df = concat_df_filtered.rename(
            columns={
                "chart_date": "날짜",
                "chart_open": "시가",
                "chart_high": "고가",
                "chart_low": "저가",
                "chart_close": "종가"
            }
        )

        df = df.sort_values("날짜").reset_index(drop=True)



        # dectected_df = detect_pattern(df)
        # pattern_df = pd.concat([pattern_df, dectected_df], ignore_index=True)
        #
        # print(pattern_df.head())
        # print(pattern_df.tail())

        pattern_df = None

    return pattern_df

    # if (concat_df_filtered is None) or (concat_df_filtered.empty):
    #     return None


