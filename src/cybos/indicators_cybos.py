import pandas as pd

from src.config.cybos_config import create_obj_cp_series, create_obj_cp_index, get_obj_cp_code_mgr
from src.db.chart_db import select_previous_chart, fetch_chart_to_df_by_ticker_and_date

from typing import Dict, List


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

LINE_NAMES: Dict[str, List[str]] = {
    # 가격지표
    "이동평균(라인3개)": ["단기 이동평균", "중기 이동평균", "장기 이동평균"],
    "고저이동평균": ["고가이평", "저가이평"],
    "Bollinger Band": ["Boll 상한선", "Boll 중앙선", "Boll 하한선"],
    "중간값(Median Price)": ["중간값"],
    "DEMA": ["DEMA"],
    "Envelope": ["Env 상한선", "Env 중앙선", "Env 하한선"],
    "Price Channel": ["P.C 상한선", "하한선", "중앙선"],
    "Projection Bands": ["Prj 상한선", "Prj 하한선"],
    "대표값(Typical Price)": ["대표값"],
    "VIDYA": ["VIDYA", "VIDYA_Signal"],

    # 추세지표
    "DMI": ["+DI", "-DI", "ADX", "ADXR"],
    "Aroon": ["Aroon Up", "Aroon Down"],
    "Aroon Osillator": ["Aroon Osillator"],
    "CCI": ["CCI", "CCI_Signal"],
    "Elder-Ray": ["Bull Power", "Bear Power"],
    "Force Index": ["단기 FI", "장기 FI"],
    "MACD": ["MACD", "MACD_Signal", "MACD_Oscillator"],
    "MAO": ["MAO", "MAO_Signal"],
    "MFI": ["MFI", "MFI_Signal"],
    "SONAR": ["SONAR", "SONAR_Signal"],
    "TRIX": ["TRIX", "TRIX_Signal"],
    "TSI": ["TSI", "TSI_Signal"],
    "VHF": ["VHF", "VHF_Signal"],

    # 변동성지표
    "ATR": ["ATR", "ATR_Signal"],
    "Chaikin's Volatility": ["Chaikin's Volatility"],
    "Relative Volatility Index": ["RVI", "RVI_Signal"],
    "Inertia": ["Inertia", "Inertia_Signal"],

    # 모멘텀지표
    "이격도": ["이격도1", "이격도2", "이격도3", "이격도4"],
    "Ease of Movement": ["EOM", "EOM_Signal"],
    "Chande Momentum Oscillator": ["CMO"],
    "Mass Index": ["Mass Index", "Mass Index_Signal"],
    "Momentum": ["Momentum", "Momentum_Signal"],
    "Negative Volume Index": ["NVI", "NVI_Signal"],
    "RSI": ["RSI", "RSI_Signal"],
    "SMI": ["SMI", "SMI_Signal"],
    "Stochastic Fast": ["Fast %K", "Fast %D"],
    "Stochastic Slow": ["Slow %K", "Slow %D"],
    "Ultimate Oscillator": ["UOSC", "UOSC_Signal"],

    # 시장강도/거래량
    "Chaikin's Money Flow": ["Chaikin's Money Flow"],
    "Williams Accumulation Distribution": ["Williams Accumulation Distribution", "Williams Accumulation Distribution_Signal"],
    "Volume Oscillator": ["VO", "VO_Signal"],

    # 업종분석
    "A/D Line": ["A/D Line", "A/D Line_Signal"],
    "McCellan Oscillator": ["McCellan Oscillator"],
}


def findLineName(index_name:str, i:int) -> str:
    """
    1) 지표 이름과 지표 라인 순서를 입력받는다
    2) 정해진 라인이름을 출력, 없을 경우엔 index_name_int로 출력
    """

    lines = LINE_NAMES.get(index_name)
    if not lines:
        return f"{index_name}_{i}"
    if 0 <= i < len(lines):
        return lines[i]
    return f"{index_name}_{i}"


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
        try:
            print(f"[{idx}/{len(index_names)}] ⏳ {index_name} 계산 중...")
            obj_cp_index = create_obj_cp_index()
            obj_cp_index.put_IndexKind(index_name)
            obj_cp_index.put_IndexDefault(index_name)
            obj_cp_index.Series = obj_cp_series

            obj_cp_index.Calculate()

            # 지표가 몇 개의 라인을 가지고 있는지(지표개수)
            item_count = obj_cp_index.ItemCount
            print("라인 수: ", item_count)

            #####################################################
            for i in range(item_count):
                get_count = obj_cp_index.GetCount(i)
                vals = [obj_cp_index.GetResult(i, j) for j in range(get_count)]
                vals = vals[60:]

                # vals = obj_cp_index.GetResult(i, get_count - 1)

                colname = findLineName(index_name, i);
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


def fetch_cybos_indicator_data(cybos_ticker: str, start_date: int, end_date: int, cybos_indicator_list: list) -> list:
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
        return None
    if not name:
        print(f"[SKIP] 유효하지 않은(또는 상폐) 코드: {repr(cybos_ticker)}")
        return None

    df = fetch_chart_to_df_by_ticker_and_date(cybos_ticker, start_date, end_date)
    print(df.head())
    chart_df = None
    # all_index_names = get_all_index_names()
    all_index_names = cybos_indicator_list

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

###########################################################################
    calculate_df = calculate_all_indexes(chart_df, all_index_names)

    print(calculate_df.head())
    print(calculate_df.tail())

    return calculate_df
