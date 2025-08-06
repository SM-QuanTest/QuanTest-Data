# from pathlib import Path
import time
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat

# from tabulate import tabulate
import win32com.client
from tqdm import tqdm

from src.cybos.chart_cybos import fetch_chart_data
from src.cybos.market_cap_cybos import get_market_caps
from src.cybos.stock_cybos import save_stock, get_kosdaq_cybos_ticker
from src.db.chart_dl import insert_chart, update_chart_change_percentage
from src.db.market_cap_db import upsert_market_cap
from src.db.stock_dl import insert_stocks
from src.utils.chart_util import process_chart_list_to_df
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


def process_cybos_ticker_list(cybos_ticker: str, start_date: int, end_date: int):
    """
    1) cybos_ticker, 시작date, 종료date 입력
    2) cybos api 호출 제한 - sleep 설정
    3) chart_data list
    4) list를 df로 변환
    5) db에 chart 데이터 삽입
    """
    time.sleep(1.0) #15초 60건 제한

    hist_list = fetch_chart_data(cybos_ticker, start_date, end_date)
    hist_df = process_chart_list_to_df(hist_list)

    insert_chart(hist_df)


if __name__ == "__main__":
    InitPlusCheck()
    kosdaq_cybos_ticker = get_kosdaq_cybos_ticker()

    ######################################################
    #     # input_csv = project_root / "stockdata" / "filteredCode.csv"
    #     # load cybos tickers
    #     # cybos_ticker_df = load_cybos_tickers_csv(input_csv)
    #     cybos_ticker_df = load_cybos_tickers_db()

    ############################sector###########################
    sector_df = save_sector_name(kosdaq_cybos_ticker)

    # sector db download
    insert_sectors(sector_df)

    ###########################stock###########################
    stock_df = save_stock(kosdaq_cybos_ticker)

    # stock db download
    insert_stocks(stock_df)

    #########################market_cap####################
    market_cap_df = get_market_caps(kosdaq_cybos_ticker, 200)

    # market_cap db download
    upsert_market_cap(market_cap_df)

    #########################chart####################
    cybos_ticker_list = kosdaq_cybos_ticker['cybos_ticker'].to_list()
    start = 20150414
    end = 20250731

    with ProcessPoolExecutor(max_workers=4) as executor:
        for _ in tqdm(
                executor.map(
                    process_cybos_ticker_list,
                    cybos_ticker_list,
                    repeat(start),
                    repeat(end)
                ),
                total=len(cybos_ticker_list),
                desc="Processing tickers"
        ):
            pass

    update_chart_change_percentage(start, end)
