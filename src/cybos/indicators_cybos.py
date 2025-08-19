import pandas as pd

from src.config.cybos_config import create_obj_cp_series, create_obj_cp_index, get_obj_cp_code_mgr
from src.db.chart_db import select_previous_chart, fetch_chart_to_df_by_ticker_and_date


def get_all_index_names() -> list:
    """
    156개의 전체 지표 이름 list로 리턴
    """
    objIndex = create_obj_cp_index()
    all_index_names = []

    for category in range(6):
        try:
            index_names = objIndex.GetChartIndexCodeListByIndex(category)
            all_index_names.extend(index_names)
        except Exception as e:
            print(f"카테고리 {category} 처리 중 오류 발생:", e)

    return all_index_names


def calculate_all_indexes(input_df: pd.DataFrame, index_names: list) -> pd.DataFrame:
    """
    1) 60일 + 입력받은 날짜/종목 해당하는 df, 지표 리스트 입력
    2) 오름차순 날짜 순서대로 obj_cp_series에 add
    3) 지표 리스트 돌며 지표 계산, 60일치 제외한 데이터만 dict 형태로 넣기
    4) 구하는 날짜 df만 합쳐서 리턴 (only chart_id)
    """

    df = input_df.copy().reset_index(drop=True)
    obj_cp_series = create_obj_cp_series()
    index_data = {}

    count = len(df)
    print("count 개수: ", count)

    for i in range(count):
        chart_date = df.iloc[i, 1]
        chart_open = df.iloc[i, 2]
        chart_high = df.iloc[i, 3]
        chart_low = df.iloc[i, 4]
        chart_close = df.iloc[i, 5]
        chart_volume = df.iloc[i, 6]
        obj_cp_series.Add(chart_close, chart_open, chart_high, chart_low, chart_volume)

    for idx, index_name in enumerate(index_names):
        # TODO: 지표 필터링
        if idx in [2, 103, 160]:
            continue
        try:
            print(f"[{idx}/{len(index_names)}] ⏳ {index_name} 계산 중...")
            obj_cp_index = create_obj_cp_index()
            obj_cp_index.put_IndexKind(index_name)
            obj_cp_index.put_IndexDefault(index_name)
            obj_cp_index.Series = obj_cp_series

            obj_cp_index.Calculate()

            # 지표가 몇 개의 라인을 가지고 있는지
            item_count = obj_cp_index.ItemCount

            for i in range(item_count):
                get_count = obj_cp_index.GetCount(i)
                vals = [obj_cp_index.GetResult(i, j) for j in range(get_count)]
                vals = vals[60:]

                colname = f"{index_name}_{i}"
                index_data[colname] = vals

        except Exception as e:
            print(f"❌ {index_name} 실패: {e}")
            continue

    index_data_to_df = pd.DataFrame(index_data)
    print(index_data_to_df.head())

    original_df = df[60:].reset_index(drop=True)
    print(original_df.head())

    calculate_df = pd.concat([original_df[["chart_id"]], index_data_to_df], axis=1)
    return calculate_df


def fetch_cybos_indicator_data(cybos_ticker: str, start_date: int, end_date: int) -> list:
    """
    1) cybos_ticker, start/end date 입력
    2) 해당 값들을 가지고있는 df 찾기(날짜,시고저종거래량,id,stock_id df)
    3) df 이전 60일치 값들 불러온 뒤 합치기
    4) 지표값 계산 후 df 리턴
    """

    try:
        name = get_obj_cp_code_mgr().CodeToName(cybos_ticker)
    except Exception as e:
        print(f"[SKIP] CodeToName 예외: code={repr(cybos_ticker)} err={e}")
        return False  # ← 여기서 바로 함수 종료
    if not name:
        print(f"[SKIP] 유효하지 않은(또는 상폐) 코드: {repr(cybos_ticker)}")
        return False  # ← 여기서 바로 함수 종료

    df = fetch_chart_to_df_by_ticker_and_date(cybos_ticker, start_date, end_date)
    print(df.head())
    chart_df = None
    all_index_names = get_all_index_names()
    daily_indicator_list = []

    for row in df.itertuples(index=False):
        stock_id = int(row.stock_id)
        chart_date = pd.to_datetime(row.chart_date).date()
        previous_chart_df = select_previous_chart(stock_id, chart_date)
        if previous_chart_df is None or len(previous_chart_df) < 60:
            print("이전 데이터 60건보다 부족")
            continue

        chart_df = pd.concat([previous_chart_df, df], ignore_index=True)
        chart_df = chart_df.sort_values("chart_date").reset_index(drop=True)
        break

    if (chart_df is None) or (chart_df.empty):
        return None

    calculate_df = calculate_all_indexes(chart_df, all_index_names)

    print(calculate_df.head())
    print(calculate_df.tail())

    return calculate_df
