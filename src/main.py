import os

# from tabulate import tabulate
import win32com.client

from src.cybos.stock_dl import save_stock
from src.db.stock_dl import insert_stocks
from .config.cybos_config import load_cybos_tickers
from .cybos.sector_name_dl import *
from .db.sector_dl import *

project_root = Path(os.getenv("PROJECT_ROOT"))


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

    input_csv = project_root / "stockdata" / "filteredCode.csv"
    # load cybos tickers
    cybos_ticker_df = load_cybos_tickers(input_csv)

    ############################sector###########################
    sector_df = save_sector_name(cybos_ticker_df)

    # sector db download
    insert_sectors(sector_df)

    ###########################stock###########################
    stock_df = save_stock(cybos_ticker_df)

    # stock db download
    insert_stocks(stock_df)
