# from pathlib import Path

# from tabulate import tabulate
import win32com.client

from src.cybos.market_cap_cybos import get_market_caps
from src.cybos.stock_cybos import save_stock, get_kosdaq_cybos_ticker
from src.db.market_cap_db import upsert_market_cap
from src.db.stock_dl import insert_stocks
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
