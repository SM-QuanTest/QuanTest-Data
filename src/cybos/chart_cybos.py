from src.config.cybos_config import get_obj_stock_chart


def fetch_chart_data(cybos_ticker: str, start_date: int, end_date: int) -> list:
    objStockChart = get_obj_stock_chart()
    total_data = []

    objStockChart.SetInputValue(0, cybos_ticker)  # 종목코드
    objStockChart.SetInputValue(1, ord('1'))  # 개수로 요청
    objStockChart.SetInputValue(2, end_date)  # 종료일 (YYYY1231)
    objStockChart.SetInputValue(3, start_date)  # 시작일 (YYYY0101)
    objStockChart.SetInputValue(5, (0, 2, 3, 4, 5, 8))  # 필드: 날짜, 시가, 고가, 저가, 종가, 거래량
    objStockChart.SetInputValue(6, ord('D'))  # 일간 데이터
    objStockChart.SetInputValue(9, ord('1'))  # 수정주가 사용
    # 데이터 요청
    objStockChart.BlockRequest()

    num_data = objStockChart.GetHeaderValue(3)  # 수신된 데이터 개수

    # 데이터 수집
    for i in range(num_data):
        date = objStockChart.GetDataValue(0, i)  # 날짜
        open_price = objStockChart.GetDataValue(1, i)  # 시가
        high_price = objStockChart.GetDataValue(2, i)  # 고가
        low_price = objStockChart.GetDataValue(3, i)  # 저가
        close_price = objStockChart.GetDataValue(4, i)  # 종가
        volume = objStockChart.GetDataValue(5, i)  # 거래량
        total_data.append((cybos_ticker, date, open_price, high_price, low_price, close_price, volume))

    return total_data
