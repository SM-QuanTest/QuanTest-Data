# from pathlib import Path
import faulthandler;

from src.config.config import CYBOS_TICKER_LIST, CYBOS_INDICATOR_LIST
from src.db.indicators_db import insert_daily_indicator
from src.db.latest_date_db import update_latest_date

faulthandler.enable()

import sys
import time

import win32com.client
from tqdm import tqdm

from datetime import datetime
from zoneinfo import ZoneInfo

from src.cybos.chart_cybos import fetch_cybos_chart_data, did_market_open_today
from src.cybos.indicators_cybos import fetch_cybos_indicator_data
from src.cybos.stock_cybos import save_stock
from src.db.chart_db import insert_chart, update_chart_change_percentage, fetch_chart_to_df_by_date
from src.db.stock_dl import insert_stocks
from src.utils.utils import cybos_ticker_list_to_df, process_chart_list_to_df, process_indicator_df_to_long_df
from .cybos.sector_cybos import *
from .db.sector_dl import *

import os, sys, builtins, functools
os.environ.setdefault("PYTHONUNBUFFERED", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")
    sys.stderr.reconfigure(line_buffering=True, encoding="utf-8")
except Exception:
    pass
builtins.print = functools.partial(builtins.print, flush=True)



def InitPlusCheck():
    try:
        # Cybos API 객체 생성
        cybos = win32com.client.Dispatch("CpUtil.CpCybos")

        # Cybos Plus 연결 상태 확인
        is_connected = cybos.IsConnect

        if is_connected == 0:
            print("Cybos Plus에 연결되지 않았습니다.")
        else:
            print("Cybos Plus에 성공적으로 연결되었습니다.")

    except Exception as e:
        print(f"에러 발생: {e}")


def chart_process_cybos_ticker_list(cybos_ticker: str, start_date: int, end_date: int):
    """
    1) cybos_ticker, 시작date, 종료date 입력
    2) cybos api 호출 제한 - sleep 설정
    3) chart_data list
    4) list를 df로 변환
    5) db에 chart 데이터 삽입
    """

    # time.sleep(1.0)  # 15초 60건 제한
    time.sleep(0.2)

    try:
        name = get_obj_cp_code_mgr().CodeToName(cybos_ticker)
    except Exception as e:
        print(f"[SKIP] CodeToName 예외: code={repr(cybos_ticker)} err={e}")
        return False
    if not name:
        print(f"[SKIP] 유효하지 않은(또는 상폐) 코드: {repr(cybos_ticker)}")
        return False

    hist_list = fetch_cybos_chart_data(cybos_ticker, start_date, end_date)
    hist_df = process_chart_list_to_df(hist_list)

    print(hist_df.head())
    insert_chart(hist_df)


def indicator_process_indicator_input_df(cybos_ticker: str, start_date: int, end_date: int, cybos_indicator_list: list):
    """
    1) cybos_ticker, 시작date, 종료date 입력
    2) cybos api 호출 제한 - sleep 설정
    3) chart_data list
    4) list를 df로 변환
    5) db에 chart 데이터 삽입
    """

    time.sleep(0.2)
    # time.sleep(1.0)  # 15초 60건 제한

    try:
        get_obj_cp_code_mgr().CodeToName(cybos_ticker)
    except Exception as e:
        print(f"유효하지 않은 종목코드 에러 발생: {e}")
        return

    indicator_df = fetch_cybos_indicator_data(cybos_ticker, start_date, end_date, cybos_indicator_list)
    if indicator_df is None:
        print(f"[SKIP] 지표 없음: {cybos_ticker}")
        return None

    print(indicator_df.head())

    long_indicator_df = process_indicator_df_to_long_df(indicator_df)
    insert_daily_indicator(long_indicator_df)


# TODO: 나중에 main()으로 변경
if __name__ == "__main__":
    InitPlusCheck()

    # cybos_ticker_df = get_code_cybos_ticker()
    # cybos_ticker_df = load_cybos_tickers_db()

    cybos_ticker_df = cybos_ticker_list_to_df(CYBOS_TICKER_LIST)

    # ######################################################
    KST = ZoneInfo("Asia/Seoul")
    today_kst_int = int(datetime.now(KST).strftime("%Y%m%d"))
    print("오늘 날짜: ", today_kst_int)

    if not did_market_open_today(today_kst_int, CYBOS_TICKER_LIST[0]):
        print("오늘은 휴장입니다.")
        # sys.exit(0)

    print("오늘자 장 마감 확인")

    ############################sector###########################
    # sector_df = save_sector_name(cybos_ticker_df)
    # print(sector_df.head())

    # sector db download
    # insert_sectors(sector_df)

    # ###########################stock###########################
    # stock_df = save_stock(cybos_ticker_df)
    #
    # # stock db download
    # insert_stocks(stock_df)

    # #########################market_cap####################
    #     market_cap_df = get_market_caps(kosdaq_cybos_ticker, 200)
    #
    #     # market_cap db download
    #     upsert_market_cap(market_cap_df)
    #     #
    # #########################chart####################
    start = 20250804
    end = 20250808
    #
    # start,end = today_kst_int


    for t in tqdm(CYBOS_TICKER_LIST, total=len(CYBOS_TICKER_LIST), desc="Processing"):
        chart_process_cybos_ticker_list(t, start, end)

    # 여기서 조회 확인, 만약 업데이트된 차트 없으면 exit
    chart_df = fetch_chart_to_df_by_date(start, end)
    if (chart_df is None) or (chart_df.empty):
        print("추가된 차트 데이터가 없어서 프로그램을 종료합니다.")
        sys.exit(0)

    update_chart_change_percentage(start, end)
    update_latest_date('charts', end)

    ########################daily_indicators####################
    # db에서 start, end 기간 가지는 df 불러온 다음, 지표 df 불러오기

    indicator_input_df = (
        chart_df[['id', 'chart_date', 'stock_id']]
        .sort_values(['stock_id', 'chart_date'])
        .reset_index(drop=True)
    )

    print(indicator_input_df)

    for t in tqdm(CYBOS_TICKER_LIST, total=len(CYBOS_TICKER_LIST), desc="Processing"):
        indicator_process_indicator_input_df(t, start, end, CYBOS_INDICATOR_LIST)

    update_latest_date('daily_indicators', end)
