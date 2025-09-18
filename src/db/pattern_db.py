from datetime import date

import psycopg2
from psycopg2.extras import execute_values

from src.config.config import CYBOS_TICKER_LIST
from src.db.chart_db import fetch_chart_to_df_by_ticker_and_date, select_previous_chart
from src.db.database import engine
from src.db.pattern import *
from src.utils.utils import process_tickers


def detect_pattern(df: pd.DataFrame) -> pd.DataFrame:
    """
    1) df에는 같은 stock_id를 가진 df가 들어옴.
    2) pattern.py 안의 모든 함수를 돌린 후, 해당하는 패턴이 있으면 -> 해당 chart_id와 pattern_id를 찾아 df에 추가
    3) 전부 돌린 후 df 리턴
    """

    pattern_functions = [
        (is_hammer, 1), (is_hanging_man, 2), (is_bullish_engulfing, 3), (is_bearish_engulfing, 4),
        (is_morning_star, 7), (is_evening_star, 8), (is_morning_doji_star, 9), (is_evening_doji_star, 10),
        (is_shooting_star, 11), (is_inverted_hammer, 12),
        (is_harami_after_uptrend, 13), (is_harami_after_downtrend, 14), (is_harami_cross_after_uptrend, 15),
        (is_harami_cross_after_downtrend, 16), (is_tweezers_bottom, 17), (is_tweezers_top, 18),
        (is_belt_hold_line_after_downtrend, 19), (is_belt_hold_line_after_uptrend, 20), (is_upside_gap_two_crows, 21),
        (is_three_black_crow, 22), (is_three_advancing_white_soldier, 23),
        (is_counterattack_lines_after_downtrend, 26), (is_counterattack_lines_after_uptrend, 27),
        (is_rising_window_df, 32), (is_falling_window_df, 33), (is_rising_gap_tasuki, 34), (is_falling_gap_tasuki, 35),
        (is_high_gapping_play, 36), (is_low_gapping_play, 37),
        (is_rising_side_by_side, 38), (is_falling_side_by_side, 39), (is_rising_three_methods, 40),
        (is_falling_three_methods, 41),
        (is_rising_separating_lines, 42), (is_falling_separating_lines, 43),
        (is_northern_doji, 44), (is_falling_long_legged_doji, 45), (is_falling_gravestone_doji, 46),
        (is_rising_dragonfly_doji, 47), (is_falling_three_stars, 48), (is_rising_three_stars, 49)
    ]

    detected_pattern = pd.DataFrame(columns=["chart_id", "pattern_id"])

    for function, pattern_id in pattern_functions:
        detected_pattern = pd.concat([detected_pattern, function(df, pattern_id)], ignore_index=True)

    return detected_pattern


def fetch_candle_chart_pattern(start_date: date, end_date: date) -> pd.DataFrame:
    """
    1) start, end만큼의 날짜를 가지고 있는 종목별 chart_by_stock_df
    2) 해당 df서 start 이전의 60일치를 previous_chart_by_stock_df 추가
    3) 날짜와 시고저종, chart_id만 포함해 가공한 df
    4) 모든 함수에 두고 돌림 (이것도 함수에 따로 빼기), 이떄 true가 나오는 값이 있다면, df에다가 계속 추가함. (추가하는 것도 함수로 빼기) -> 추가 항목은 pattern_id, chart_id
    5) for문을 돌아가며 계속 추가된 pattern_df를 리턴

    """
    results = []

    for cybos_ticker in CYBOS_TICKER_LIST:
        chart_by_stock_df = fetch_chart_to_df_by_ticker_and_date(process_tickers(cybos_ticker), start_date, end_date)
        if chart_by_stock_df.empty:
            print("해당 기간 데이터 없음")
            continue
        stock_id = int(chart_by_stock_df.stock_id.iloc[0])
        chart_date = pd.to_datetime(chart_by_stock_df.chart_date.iloc[0]).date()
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

        dectected_df = detect_pattern(df)
        if not dectected_df.empty:
            results.append(dectected_df)

    if results:
        pattern_df = pd.concat(results, ignore_index=True)
    else:
        pattern_df = pd.DataFrame(columns=["chart_id", "pattern_id"])

    return pattern_df


def insert_patterns(input_df: pd.DataFrame):
    """
    1) input_df 입력
    2) db에 insert
    """

    df = input_df.copy()
    total = len(df)

    records = df[['pattern_id', 'chart_id']].itertuples(index=False, name=None)

    sql = """
          INSERT INTO public.pattern_records (pattern_id, chart_id)
          VALUES %s ON CONFLICT
          ON CONSTRAINT pattern_chart_id_unique
              DO NOTHING
              RETURNING id, pattern_id, chart_id;
          """

    with engine.begin() as conn:
        cur = conn.connection.cursor()
        rows = psycopg2.extras.execute_values(cur, sql, records, page_size=1000, fetch=True)
    inserted = len(rows)  # 실제 삽입된 행 수
    skipped = total - inserted  # 충돌로 스킵된 행 수
    print(">>> 삽입: ", inserted, ">>> 스킵: ", skipped)
