# from pathlib import Path
import faulthandler;

faulthandler.enable()

import sys
import time
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat

import pythoncom
# from tabulate import tabulate
import win32com.client
from tqdm import tqdm

from datetime import datetime, date
from zoneinfo import ZoneInfo

from src.cybos.chart_cybos import fetch_chart_data, did_market_open_today
from src.cybos.indicators_cybos import fetch_indicator_data, get_all_index_names
from src.cybos.market_cap_cybos import get_market_caps
from src.cybos.stock_cybos import save_stock, get_code_cybos_ticker
from src.db.chart_db import insert_chart, update_chart_change_percentage
from src.db.market_cap_db import upsert_market_cap
from src.db.stock_dl import insert_stocks
from src.utils.chart_util import process_chart_list_to_df
from src.utils.utils import load_cybos_tickers_db
from .cybos.sector_cybos import *
from .db.sector_dl import *


# project_root = Path(os.getenv("PROJECT_ROOT"))


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
    수정필요
    """
    # time.sleep(1.0) #15초 60건 제한
    time.sleep(0.2)

    hist_list = fetch_chart_data(cybos_ticker, start_date, end_date)
    chart_df = process_chart_list_to_df(hist_list)

    insert_chart_df = insert_chart(chart_df)


    if (insert_chart_df.empty):
        print(">>> 지표를 넣을 신규 chart 데이터가 없습니다.")
        return

    daily_indicator_df = fetch_indicator_data(insert_chart_df)
    print("지표데이터")

    # insert_daily_indicator(daily_indicator_df)

    return None



# TODO: 나중에 main()으로 변경
if __name__ == "__main__":
    pythoncom.CoInitialize()
    try:
        InitPlusCheck()

        cybos_ticker_df = get_code_cybos_ticker()
        print(get_all_index_names())

        #
        # # ######################################################
        # KST = ZoneInfo("Asia/Seoul")
        # today_kst_int = int(datetime.now(KST).strftime("%Y%m%d"))
        #
        # if not did_market_open_today(today_kst_int) :
        #     print("휴장입니다.")
        #     sys.exit(0)
        #
        # print("장 마감 확인")
        #
        # # cybos_ticker_df = load_cybos_tickers_db()
        # #
        # ############################sector###########################
        # sector_df = save_sector_name(cybos_ticker_df)
        #
        # # sector db download
        # insert_sectors(sector_df)
        #
        # ###########################stock###########################
        # stock_df = save_stock(cybos_ticker_df)
        #
        # # stock db download
        # insert_stocks(stock_df)
        #
        # # #########################market_cap####################
        # # market_cap_df = get_market_caps(kosdaq_cybos_ticker, 200)
        # #
        # # # market_cap db download
        # # upsert_market_cap(market_cap_df)
        #
        # #########################chart####################
        cybos_ticker_list = cybos_ticker_df['cybos_ticker'].to_list()
        start = 20250807
        end = 20250807
        #
        # start,end = today_kst_int

        # for t in tqdm(cybos_ticker_list, total=len(cybos_ticker_list), desc="Processing"):
        #     chart_process_cybos_ticker_list(t, start, end)
        for t in cybos_ticker_list:
            chart_process_cybos_ticker_list(t, start, end)

        #
        # with ProcessPoolExecutor(max_workers=4) as executor:
        #     for _ in tqdm(
        #             executor.map(
        #                 process_cybos_ticker_list,
        #                 cybos_ticker_list,
        #                 repeat(start),
        #                 repeat(end)
        #             ),
        #             total=len(cybos_ticker_list),
        #             desc="Processing"
        #     ):
        #         pass
        #

        # update_chart_change_percentage(start, end)
        #
        # #########################daily_indicators####################


    finally:
        pythoncom.CoUninitialize()
