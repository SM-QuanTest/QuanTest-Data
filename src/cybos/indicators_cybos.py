import pandas as pd

from src.config.cybos_config import create_obj_cp_series, create_obj_cp_index
from src.db.chart_db import select_previous_chart


def get_all_index_names() -> list:
    """
    156개의 지표 이름 list로 리턴
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


def calculate_all_indexes(input_df: pd.DataFrame, index_names: list) -> pd.DataFrame:  # pddataframe으로리턴?
    """
    1) chart_id, stock_id(input_df에서 다 동일), 날짜시고저종거래량 담은 input_df와, 지표 이름 156개 리스트 받아오기
    2)

    chart_id, index_names의 indicators_name, 그에 맞는 indicators_value 담은 df 리턴

    """
    # print("calculate_all_indexes")

    df = input_df.copy()
    obj_cp_series = create_obj_cp_series()
    index_data = {}

    count = len(df)
    print("count 개수: ", count)

    # 날짜 오름차순으로 series에 add (60+a일차)
    for i in range(count):
        chart_open = df.iloc[i, 2]
        chart_high = df.iloc[i, 3]
        chart_low = df.iloc[i, 4]
        chart_close = df.iloc[i, 5]
        chart_volume = df.iloc[i, 6]
        obj_cp_series.Add(chart_close, chart_open, chart_high, chart_low, chart_volume)
    print("series 성공")

    # 지표 for문
    for idx, index_name in enumerate(index_names):
        # 일부 지표 패스
        # TODO: 지표 필터링
        if idx in [2, 103, 160]:
            continue
        try:
            print(f"[{idx}/{len(index_names)}] ⏳ {index_name} 계산 중...")
            obj_cp_index = create_obj_cp_index()
            obj_cp_index.put_IndexKind(index_name)
            print("indexkined")
            obj_cp_index.put_IndexDefault(index_name)
            print("indexdefault")
            obj_cp_index.Series = obj_cp_series
            print("series")

            obj_cp_index.Calculate()
            print("calculate");

            # 지표가 몇 개의 라인을 가지고 있는지
            item_count = obj_cp_index.ItemCount
            print("지표 라인 개수: ", item_count)

            for i in range(item_count):
                get_count = obj_cp_index.GetCount(i)
                vals = [obj_cp_index.GetResult(i, j) for j in range(get_count)]

                # TODO: label 이름 수정 확인
                try:
                    label = obj_cp_index.GetResultName(i)
                    label = label if label else f"{idx}_{i}"
                except Exception:
                    label = f"{idx}_{i}"

                colname = f"{idx}:{label}" if label and label != f"{idx}_{i}" else f"{idx}_{i}"
                # 앞의 60개 빼고 지표값 저장
                index_data[colname] = vals[60:]

                # print("dict에 넣기 완료")

        except Exception as e:
            print(f"❌ {index_name} 실패: {e}")
            continue

    # 앞의 60일치 뺀 original_df
    original_df = df[60:]

    # key: value 쌍
    valid_columns = {
        key: values for key, values in index_data.items() if len(values) == len(original_df)
    }

    calculate_df = pd.DataFrame(valid_columns, index=original_df.index)
    c_calculate_df = pd.concat([original_df, calculate_df], axis=1)
    return c_calculate_df.iloc[60:].reset_index(drop=True)


#########

def fetch_indicator_data(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    1) 날짜 오름차순 input_df 입력 -> "chart_id", "chart_date", "stock_id" (한 종목, 지표 구해야하는 날짜들이 담긴 -> csv랑 같은 형식)
    2) for문
    3) chart db 호출 -> 처음의 chart_date부터 시작해서, 가장 최근의 60일 정보 불러옴?
    4) 60일 정보가 없으면 skip, 60일 정보가 나올 때부터 시작
    5) calculate_all_indexes로 지표와 날짜 담긴 df를 리턴 -> 지표명, chart_id, 지표값 필요
    6) df 리턴
    """

    print("fetch_indicator_data")

    df = input_df.copy()
    chart_df = None
    all_index_names = get_all_index_names()
    daily_indicator_list = []

    # input_df에서, 맨 처음 들어온 날짜의 이전 데이터가 60일치 데이터가 있는지 확인
    for row in df.itertuples(index=False):
        stock_id = int(row.stock_id)
        chart_date = pd.to_datetime(row.chart_date)
        previous_chart_df = select_previous_chart(stock_id, chart_date)
        if previous_chart_df is None or len(previous_chart_df) < 60:
            print("이전 데이터 60건보다 부족")
            continue

        chart_df = pd.concat([previous_chart_df, df], ignore_index=True)
        chart_df = chart_df.sort_values("chart_date").reset_index(drop=True)
        break

    if (chart_df is None) or (chart_df.empty):
        return None

    print("len(chart_df): 60개+date개수 -> ", len(chart_df))

    calculate_df = calculate_all_indexes(chart_df, all_index_names)

    print(calculate_df.head())
    print(calculate_df.tail())

    return calculate_df
