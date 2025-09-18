from datetime import date

import pandas as pd

# 상승추세
def is_uptrend(df):
    ma5 = df['종가'].rolling(window=5).mean()
    ma10 = df['종가'].rolling(window=10).mean()

    # 5일간 종가가 5일 전 종가를 밑돌지 않음
    cond = df['종가'].shift(5)
    no_drop = df['종가'].rolling(window=5).apply(lambda x: (x >= cond.loc[x.index[0]]).all(), raw=False)

    return (ma5 > ma10) & no_drop.fillna(False)


# 하락추세
def is_downtrend(df):
    ma5 = df['종가'].rolling(window=5).mean()
    ma10 = df['종가'].rolling(window=10).mean()
    cond = df['종가'].shift(5)
    no_rally = df['종가'].rolling(window=5).apply(lambda x: (x <= cond.loc[x.index[0]]).all(), raw=False)

    return (ma5 < ma10) & no_rally.fillna(False)


#

def is_hammer(df):  # 망치형
    downtrend = is_downtrend(df)
    body = (df['시가'] - df['종가']).abs()
    total_length = df['고가'] - df['저가']
    lower_tail = df[['시가', '종가']].min(axis=1) - df['저가']
    hammer_condition = (
            downtrend &
            (body <= total_length / 3) &
            (lower_tail >= 2 * body)
    )
    return hammer_condition.astype(int)


def is_hanging_man(df):  # 교수형
    uptrend = is_uptrend(df)
    body = (df['시가'] - df['종가']).abs()
    total_length = df['고가'] - df['저가']
    lower_tail = df[['시가', '종가']].min(axis=1) - df['저가']
    hanging_man_condition = (
            uptrend &
            (body <= total_length / 3) &
            (lower_tail >= 2 * body)
    )
    return hanging_man_condition.astype(int)


def is_bullish_engulfing(df):  # 상승장악형
    downtrend = is_downtrend(df)
    prev_open = df['시가'].shift(1)
    prev_close = df['종가'].shift(1)
    curr_open = df['시가']
    curr_close = df['종가']
    prev_bearish = prev_close < prev_open
    curr_bullish = curr_close > curr_open
    engulfing_condition = (curr_close > prev_open) & (curr_open < prev_close)
    bullish_engulfing_condition = (
            downtrend &
            prev_bearish &
            curr_bullish &
            engulfing_condition
    )
    return bullish_engulfing_condition.astype(int)


def is_bearish_engulfing(df):  # 하락장악형
    uptrend = is_uptrend(df)
    prev_open = df['시가'].shift(1)
    prev_close = df['종가'].shift(1)
    curr_open = df['시가']
    curr_close = df['종가']
    prev_bullish = prev_close > prev_open
    curr_bearish = curr_close < curr_open
    engulfing_condition = (curr_close < prev_open) & (curr_open > prev_close)
    bearish_engulfing_condition = (
            uptrend &
            prev_bullish &
            curr_bearish &
            engulfing_condition
    )
    return bearish_engulfing_condition.astype(int)


#############################################################################################################################
# 샛별형 (하락추세 이후에 나타남)
def is_morning_star(df):
    downtrend = is_downtrend(df).shift(2).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    # 1일 전, 2일 전 값
    o1, o2 = o.shift(1), o.shift(2)
    c1, c2 = c.shift(1), c.shift(2)
    h1, h2 = h.shift(1), h.shift(2)
    l1, l2 = l.shift(1), l.shift(2)
    body_1, body_2 = body.shift(1), body.shift(2)

    # 첫 번째 캔들
    first_bearish = c2 < o2
    first_long = ((o2 - c2) / o2) >= 0.03
    cond1 = (first_bearish & first_long).fillna(False)
    # 두 번째 캔들
    second_small_body = body_1 < (h1 - l1) * 0.3
    # 둘째 날 몸통의 상단이 첫째 날 몸통의 하단보다 밑에 있어야 함
    top_body_1 = pd.concat([o1, c1], axis=1).max(axis=1)
    bottom_body_2 = pd.concat([o2, c2], axis=1).min(axis=1)
    second_gap = top_body_1 < bottom_body_2
    cond2 = (second_small_body & second_gap).fillna(False)

    # 세 번째 캔들
    third_bullish = c > o
    third_penetration = (c - c2) / (o2 - c2) >= 0.5
    cond3 = (third_bullish & third_penetration).fillna(False)

    return (downtrend & cond1 & cond2 & cond3).fillna(False)


# 저녁별형 (상승추세 이후에 나타남)

'''
세 개의 캔들이 있음.
상승추세 - 이미 구현해둔 상승추세 사용
첫 번째 캔들 - 장대 양봉.(시가보다 종가가 크게 높을 때 -> 종가가 시가보다 3% 이상 상승하는 경우로 정의)
두 번째 캔들 - 몸통이 앞의 양봉 몸통에 닿지 않는다.몸통이 작다(이미 구현해둔 small_body 사용) 양/음봉상관없음
세 번째 캔들 - 첫 번째 캔들의 몸통 안으로 깊숙이 파고드는 음봉 -> 깊숙이 기준 0.5%로 정의
'''


def is_evening_star(df):
    uptrend = is_uptrend(df).shift(2).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    # 1일 전, 2일 전 값
    o1, o2 = o.shift(1), o.shift(2)
    c1, c2 = c.shift(1), c.shift(2)
    h1, h2 = h.shift(1), h.shift(2)
    l1, l2 = l.shift(1), l.shift(2)
    body_1, body_2 = body.shift(1), body.shift(2)

    # 첫 번째 캔들
    first_bullish = c2 > o2
    first_long = ((c2 - o2) / o2) >= 0.03
    cond1 = (first_bullish & first_long).fillna(False)

    # 두 번째 캔들
    second_small_body = body_1 < (h1 - l1) * 0.3
    # 둘째 날 몸통의 하단이 첫째 날 몸통의 상단보다 위에 있어야 함
    bottom_body_1 = pd.concat([o1, c1], axis=1).min(axis=1)
    top_body_2 = pd.concat([o2, c2], axis=1).max(axis=1)
    second_gap = bottom_body_1 > top_body_2
    cond2 = (second_small_body & second_gap).fillna(False)

    # 세 번째 캔들
    third_bullish = c < o
    third_penetration = (c2 - c) / (c2 - o2) >= 0.5
    cond3 = (third_bullish & third_penetration).fillna(False)

    return (uptrend & cond1 & cond2 & cond3).fillna(False)


# 십자샛별형 (하락추세 이후에 나타남)

'''
샛별형 중 두 번째 캔들이 도지인 경우 (전체 캔들 폭의 10% 이하인 경우로 정의)
'''


def is_morning_doji_star(df):
    downtrend = is_downtrend(df).shift(2).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    # 1일 전, 2일 전 값
    o1, o2 = o.shift(1), o.shift(2)
    c1, c2 = c.shift(1), c.shift(2)
    h1, h2 = h.shift(1), h.shift(2)
    l1, l2 = l.shift(1), l.shift(2)
    body_1, body_2 = body.shift(1), body.shift(2)

    # 첫 번째 캔들
    first_bearish = c2 < o2
    first_long = ((o2 - c2) / o2) >= 0.03
    cond1 = (first_bearish & first_long).fillna(False)

    # 두 번째 캔들
    second_doji_body = body_1 < (h1 - l1) * 0.1
    # 둘째 날 몸통의 상단이 첫째 날 몸통의 하단보다 밑에 있어야 함
    top_body_1 = pd.concat([o1, c1], axis=1).max(axis=1)
    bottom_body_2 = pd.concat([o2, c2], axis=1).min(axis=1)
    second_gap = top_body_1 < bottom_body_2
    cond2 = (second_doji_body & second_gap).fillna(False)

    # 세 번째 캔들
    third_bullish = c > o
    third_penetration = (c - c2) / (o2 - c2) >= 0.5
    cond3 = (third_bullish & third_penetration).fillna(False)

    return (downtrend & cond1 & cond2 & cond3).fillna(False)


# 십자저녁별형 (상승추세 이후에 나타남)

'''
저녁별형 중 두 번째 캔들이 도지인 경우 (전체 캔들 폭의 10% 이하인 경우로 정의)

'''


def is_evening_doji_star(df):
    uptrend = is_uptrend(df).shift(2).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    # 1일 전, 2일 전 값
    o1, o2 = o.shift(1), o.shift(2)
    c1, c2 = c.shift(1), c.shift(2)
    h1, h2 = h.shift(1), h.shift(2)
    l1, l2 = l.shift(1), l.shift(2)
    body_1, body_2 = body.shift(1), body.shift(2)

    # 첫 번째 캔들
    first_bullish = c2 > o2
    first_long = ((c2 - o2) / o2) >= 0.03
    cond1 = (first_bullish & first_long).fillna(False)

    # 두 번째 캔들
    second_doji_body = body_1 < (h1 - l1) * 0.1
    # 둘째 날 몸통의 하단이 첫째 날 몸통의 상단보다 위에 있어야 함
    bottom_body_1 = pd.concat([o1, c1], axis=1).min(axis=1)
    top_body_2 = pd.concat([o2, c2], axis=1).max(axis=1)
    second_gap = bottom_body_1 > top_body_2
    cond2 = (second_doji_body & second_gap).fillna(False)

    # 세 번째 캔들
    third_bullish = c < o
    third_penetration = (c2 - c) / (c2 - o2) >= 0.5
    cond3 = (third_bullish & third_penetration).fillna(False)

    return (uptrend & cond1 & cond2 & cond3).fillna(False)


# 유성형(하락반전)

'''
주가상승 마지막에 출현
하나의 캔들
1. 몸통이 작다, 몸통의 색깔은 중요하지 않다
2. 윗그림자가 길다
3. 아랫그림자가 거의 없다


'''


def is_shooting_star(df):
    uptrend = is_uptrend(df).shift(1).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    # 작은몸통
    small_body = body < (h - l) * 0.3

    # 긴 윗그림자
    higher_shadow = h - pd.concat([o, c], axis=1).max(axis=1)
    long_upper_tail = higher_shadow > 2 * body

    # 거의 없는 아랫그림자
    lower_shadow = pd.concat([o, c], axis=1).min(axis=1) - l
    thin_lower = lower_shadow < 0.1 * body

    return (uptrend & small_body & long_upper_tail & thin_lower).fillna(False)


# 역망치형(상승반전)

'''
하락세 마지막에 출현
하나의 캔들
1. 몸통이 작다, 몸통의 색깔은 중요하지 않다
2. 윗그림자가 길다
3. 아랫그림자가 거의 없다
'''


def is_inverted_hammer(df):
    downtrend = is_downtrend(df).shift(1).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    # 작은몸통
    small_body = body < (h - l) * 0.3

    # 긴 윗그림자
    higher_shadow = h - pd.concat([o, c], axis=1).max(axis=1)
    long_upper_tail = higher_shadow > 2 * body

    # 거의 없는 아랫그림자
    lower_shadow = pd.concat([o, c], axis=1).min(axis=1) - l
    thin_lower = lower_shadow < 0.1 * body

    return (downtrend & small_body & long_upper_tail & thin_lower).fillna(False)


###############################################################################################################################


# 상승추세 이후의 잉태형(상승잉태형)
def is_harami_after_uptrend(df):
    uptrend = is_uptrend(df).shift(2).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    o1 = o.shift(1)
    c1 = c.shift(1)

    body_1 = body.shift(1)

    # 몸통이 비정상적으로 큰 음봉 또는 양봉
    avg_body = body_1.rolling(20).mean().shift(1)
    big_body = body_1 >= avg_body * 1.5

    # 우선 전 날이 비정상적으로 크고, 그 다음날이 해당 몸통의 30퍼센트 안으로 들어와야함,
    # 글고 종가와 시가가 앞 차트의 종가와 시가 안에 포함돼있어야함

    # 작은몸통
    small_body = body < (h - l) * 0.3

    # 작은몸통이 큰 몸통 안에 들어와있음
    big_body_max = pd.concat([o1, c1], axis=1).max(axis=1)
    big_body_min = pd.concat([o1, c1], axis=1).min(axis=1)
    small_body_max = pd.concat([o, c], axis=1).max(axis=1)
    small_body_min = pd.concat([o, c], axis=1).min(axis=1)

    small_in_big = (small_body_max <= big_body_max) & (small_body_min >= big_body_min)

    return (uptrend & big_body & small_body & small_in_big).fillna(False)


# 하락추세 이후의 잉태형(하락잉태형)
def is_harami_after_downtrend(df):
    downtrend = is_downtrend(df).shift(2).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    o1 = o.shift(1)
    c1 = c.shift(1)

    body_1 = body.shift(1)

    # 몸통이 비정상적으로 큰 음봉 또는 양봉
    avg_body = body_1.rolling(20).mean().shift(1)
    big_body = body_1 >= avg_body * 1.5

    # 우선 전 날이 비정상적으로 크고, 그 다음날이 해당 몸통의 30퍼센트 안으로 들어와야함,
    # 글고 종가와 시가가 앞 차트의 종가와 시가 안에 포함돼있어야함

    # 작은몸통
    small_body = body < (h - l) * 0.3

    # 작은몸통이 큰 몸통 안에 들어와있음
    big_body_max = pd.concat([o1, c1], axis=1).max(axis=1)
    big_body_min = pd.concat([o1, c1], axis=1).min(axis=1)
    small_body_max = pd.concat([o, c], axis=1).max(axis=1)
    small_body_min = pd.concat([o, c], axis=1).min(axis=1)

    small_in_big = (small_body_max <= big_body_max) & (small_body_min >= big_body_min)

    return (downtrend & big_body & small_body & small_in_big).fillna(False)


# 십자잉태형

'''
팽이형(작은몸통)이 등장

도지형 캔들의 몸통이 - 몸통이 비정상적으로 큰 음봉 또는 양봉 - 안으로 들어가 있음 (기존 정의한 도지형 사용)
몸통이 비정상적으로 큰 음봉/양봉 기준 : 20일간의 몸통 평균의 1.5배 이상으로 정의
잉태형의 두 번째 캔들은 음/양봉 상관없음

'''


# 상승추세 이후의 십자잉태형(상승십자잉태형)
def is_harami_cross_after_uptrend(df):
    uptrend = is_uptrend(df).shift(2).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    o1 = o.shift(1)
    c1 = c.shift(1)
    h1 = h.shift(1)
    l1 = l.shift(1)
    body_1 = body.shift(1)

    # 몸통이 비정상적으로 큰 음봉 또는 양봉
    avg_body = body_1.rolling(20).mean().shift(1)
    big_body = body_1 >= avg_body * 1.5

    # 우선 전 날이 비정상적으로 크고, 그 다음날이 해당 몸통의 30퍼센트 안으로 들어와야함,
    # 글고 종가와 시가가 앞 차트의 종가와 시가 안에 포함돼있어야함

    # 작은몸통
    doji_body = body < (h - l) * 0.1

    # 작은몸통이 큰 몸통 안에 들어와있음
    big_body_max = pd.concat([o1, c1], axis=1).max(axis=1)
    big_body_min = pd.concat([o1, c1], axis=1).min(axis=1)
    doji_body_max = pd.concat([o, c], axis=1).max(axis=1)
    doji_body_min = pd.concat([o, c], axis=1).min(axis=1)

    doji_in_big = (doji_body_max <= big_body_max) & (doji_body_min >= big_body_min)

    return (uptrend & big_body & doji_body & doji_in_big).fillna(False)


# 하락추세 이후의 십자잉태형(하락십자잉태형)
def is_harami_cross_after_downtrend(df):
    downtrend = is_downtrend(df).shift(2).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    o1 = o.shift(1)
    c1 = c.shift(1)
    h1 = h.shift(1)
    l1 = l.shift(1)
    body_1 = body.shift(1)

    # 몸통이 비정상적으로 큰 음봉 또는 양봉
    avg_body = body_1.rolling(20).mean().shift(1)
    big_body = body_1 >= avg_body * 1.5

    # 우선 전 날이 비정상적으로 크고, 그 다음날이 해당 몸통의 30퍼센트 안으로 들어와야함,
    # 글고 종가와 시가가 앞 차트의 종가와 시가 안에 포함돼있어야함

    # 작은몸통
    doji_body = body < (h - l) * 0.1

    # 작은몸통이 큰 몸통 안에 들어와있음
    big_body_max = pd.concat([o1, c1], axis=1).max(axis=1)
    big_body_min = pd.concat([o1, c1], axis=1).min(axis=1)
    doji_body_max = pd.concat([o, c], axis=1).max(axis=1)
    doji_body_min = pd.concat([o, c], axis=1).min(axis=1)

    doji_in_big = (doji_body_max <= big_body_max) & (doji_body_min >= big_body_min)

    return (downtrend & big_body & doji_body & doji_in_big).fillna(False)


# 집게형

'''
집게형은 고가 또는 저가가 일치하는 두 개의 캔들로 이루어져있다.

하락집게형 - 상승장, 두 개 이상의 캔들이 연속적으로 고가가 일치하는 경우
상승집게형 - 하락장, 두 개 이상의 캔들이 연속적으로 저가가 일치하는 경우
-> 0.1% 이내의 오차범위일 때 동일하다고 정의

이상적인 경우 첫 번째 캔들은 크고 두 번째 캔들은 작아야 한다.
-> 두 번째 캔들이 첫 번째 캔들보다 50% 이상 작아야 탐지되도록 정의

'''


# 상승집게형(하락장이후)
def is_tweezers_bottom(df):
    downtrend = is_downtrend(df).fillna(False)

    l = df['저가']
    l1 = l.shift(1)
    body = (df['시가'] - df['종가']).abs()
    body_1 = body.shift(1)

    same_low = (l - l1).abs() <= l * 0.001

    # 두 번째 캔들이 첫 번째 캔들보다 50% 작아야함
    small_body = body <= body_1 * 0.5

    return (downtrend & same_low & small_body).fillna(False)


# 하락집게형(상승장이후)
def is_tweezers_top(df):
    uptrend = is_uptrend(df).fillna(False)

    h = df['고가']
    h1 = h.shift(1)
    body = (df['시가'] - df['종가']).abs()
    body_1 = body.shift(1)

    same_high = (h - h1).abs() <= h * 0.001

    # 두 번째 캔들이 첫 번째 캔들보다 작아야함
    small_body = body <= body_1 * 0.5

    return (uptrend & same_high & small_body).fillna(False)


# 샅바형

'''
상승샅바형 - 시가와 저가가 매우 비슷, 종가와 고가가 똑같거나 비슷, 장대 양봉 - 종가가 시가보다 3% 이상 상승
하락샅바형 - 시가와 고가가 매우 비슷, 종가와 저가가 똑같거나 비슷, 장대 음봉 - 종가가 시가보다 3% 이상 하락하는 경우
'''


# 상승샅바형(하락장이후)
def is_belt_hold_line_after_downtrend(df):
    downtrend = is_downtrend(df).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    same_open_low = (o - l).abs() <= l * 0.001
    same_close_high = (c - h).abs() <= h * 0.001

    long_bullish = ((c - o) / o) >= 0.03

    return (downtrend & same_open_low & same_close_high & long_bullish).fillna(False)


# 하락샅바형(상승장이후)
def is_belt_hold_line_after_uptrend(df):
    uptrend = is_uptrend(df).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    same_open_high = (h - o).abs() <= o * 0.001
    same_close_low = (l - c).abs() <= c * 0.001

    long_bearish = ((o - c) / o) >= 0.03

    return (uptrend & same_open_high & same_close_low & long_bearish).fillna(False)


# 까마귀형

'''
두 개의 음봉과 그 앞에 나타나는 캔들 사이에 상승갭 (앞에 나타나는 캔들은 보통 양봉)
두 번째 음봉의 시가가 첫 번째 음봉의 시가보다 위, 종가는 첫 번째 음봉의 종가보다 아래
'''


# 까마귀형 (하락 반전신호)
def is_upside_gap_two_crows(df):
    uptrend = is_uptrend(df).shift(2).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    o1, o2 = o.shift(1), o.shift(2)
    c1, c2 = c.shift(1), c.shift(2)
    h1, h2 = h.shift(1), h.shift(2)
    l1, l2 = l.shift(1), l.shift(2)
    body_1, body_2 = body.shift(1), body.shift(2)

    first_bullish = c2 > o2
    second_bearish = c1 < o1
    third_bearish = c < o

    gap = (h2 < o1) & (h2 < o)
    cond = (o > o1) & (c < c1)

    return (uptrend & first_bullish & second_bearish & third_bearish & gap & cond).fillna(False)


# 흑삼병

'''
연속적으로 하락하는 세 개의 음봉이 있는 경우
음봉은 종가가 저가와같거나 비슷(꼬리 종가 대비 10% 이내로 정의),
각 음봉의 시가는 앞 캔들의 몸통 안에 위치
'''


# 흑삼병(하락 반전)
def is_three_black_crow(df):
    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']

    o1, o2 = o.shift(1), o.shift(2)
    c1, c2 = c.shift(1), c.shift(2)
    l1, l2 = l.shift(1), l.shift(2)

    all_bearish = (c2 < o2) & (c1 < o1) & (c < o)
    sim_close_low = ((l - c).abs() <= c * 0.1) & ((l1 - c1).abs() <= c * 0.1) & ((l2 - c2).abs() <= c * 0.1)
    open_in_last_bar = ((o > c1) & (o < o1)) & ((o1 > c2) & (o1 < o2))

    return (all_bearish & sim_close_low & open_in_last_bar).fillna(False)


# 상승적삼병

'''
흑삼병과 반대
장대 양봉 세 개의 종가가 연속적으로 올라가는 패턴
각 양봉은 종가와 고가나 같거나 비슷 (0.03)
각 양봉의 시가는 앞 캔들 몸통의 내부나 가까이(종가/시가기준 2%)에 위치
'''


# 상승적삼병
def is_three_advancing_white_soldier(df):
    o = df['시가']
    h = df['고가']
    c = df['종가']

    o1, o2 = o.shift(1), o.shift(2)
    c1, c2 = c.shift(1), c.shift(2)
    h1, h2 = h.shift(1), h.shift(2)

    all_bullish = (c2 > o2) & (c1 > o1) & (c > o)
    all_long = (((c2 - o2) / o2) >= 0.03) & (((c1 - o1) / o1) >= 0.03) & (((c - o) / o) >= 0.03)

    sim_close_high = ((h - c).abs() <= c * 0.1) & ((h1 - c1).abs() <= c1 * 0.1) & ((h2 - c2).abs() <= c2 * 0.1)
    open_in_last_bar = ((o < c1 * 1.02) & (o > o1 * 0.98)) & ((o1 < c2 * 1.02) & (o1 > o2 * 0.98))

    return (all_bullish & all_long & sim_close_high & open_in_last_bar).fillna(False)


##
##
##

# 상승반격형 (상승반전신호)
def is_counterattack_lines_after_downtrend(df):
    downtrend = is_downtrend(df).shift(1).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    o1 = o.shift(1)
    c1 = c.shift(1)
    h1 = h.shift(1)
    l1 = l.shift(1)
    body_1 = body.shift(1)

    long_bearish = ((o1 - c1) / o1) >= 0.03

    second_bullish = c > o
    gap_down = (o - c1) / c1 <= -0.02

    touch = (c - c1).abs() <= body_1 * 0.1

    return (downtrend & long_bearish & second_bullish & gap_down & touch).fillna(False)


# 하락반격형 (상승반전신호)
def is_counterattack_lines_after_uptrend(df):
    uptrend = is_uptrend(df).shift(1).fillna(False)

    o = df['시가']
    h = df['고가']
    l = df['저가']
    c = df['종가']
    body = (o - c).abs()

    o1 = o.shift(1)
    c1 = c.shift(1)

    body_1 = body.shift(1)

    long_bullish = ((c1 - o1) / o1) >= 0.03

    second_bullish = c < o
    gap_up = (o - c1) / c1 >= 0.02

    touch = (c - c1).abs() <= body_1 * 0.1

    return (uptrend & long_bullish & second_bullish & gap_up & touch).fillna(False)


##
##
##


#############################################################################################################################


def is_rising_window(df):  # 상승창형
    prev_high = df['고가'].shift(1)
    curr_low = df['저가']
    gap_up = curr_low > prev_high
    rising_window_condition = gap_up
    return rising_window_condition.astype(int)


def is_falling_window(df):  # 하락창형
    prev_low = df['저가'].shift(1)
    curr_high = df['고가']
    gap_down = curr_high < prev_low
    falling_window_condition = gap_down
    return falling_window_condition.astype(int)


def is_rising_gap_tasuki(df):  # 상승갭 타스키형
    rising_window = is_rising_window(df)
    prev_open = df['시가'].shift(1)
    prev_close = df['종가'].shift(1)
    curr_open = df['시가']
    curr_close = df['종가']
    gap_prev_high = df['고가'].shift(2)
    prev_bullish = prev_close > prev_open
    curr_bearish = curr_close < curr_open
    prev_body = (prev_close - prev_open).abs()
    curr_body = (curr_close - curr_open).abs()
    similar_body = (curr_body >= 0.9 * prev_body) & (curr_body <= 1.1 * prev_body)
    gap_maintained = curr_close > gap_prev_high
    rising_gap_tasuki_condition = (
            rising_window.shift(1) &
            prev_bullish &
            curr_bearish &
            similar_body &
            gap_maintained
    )
    return rising_gap_tasuki_condition.astype(int)


def is_falling_gap_tasuki(df):  # 히릭갭타스키형
    falling_window = is_falling_window(df)
    prev_open = df['시가'].shift(1)
    prev_close = df['종가'].shift(1)
    curr_open = df['시가']
    curr_close = df['종가']
    gap_prev_low = df['저가'].shift(2)
    prev_bearish = prev_close < prev_open
    curr_bullish = curr_close > curr_open
    prev_body = (prev_open - prev_close).abs()
    curr_body = (curr_close - curr_open).abs()
    similar_body = (curr_body >= 0.9 * prev_body) & (curr_body <= 1.1 * prev_body)
    gap_maintained = curr_close < gap_prev_low
    falling_gap_tasuki_condition = (
            falling_window.shift(1) &
            prev_bearish &
            curr_bullish &
            similar_body &
            gap_maintained
    )
    return falling_gap_tasuki_condition.astype(int)


def is_high_gapping_play(df):  # 고가 갭핑플레이형
    rising_window = is_rising_window(df)
    high_gapping_conditions = []
    for lookback in range(4, 8):
        long_candle_idx = lookback + 1
        long_open = df['시가'].shift(long_candle_idx)
        long_close = df['종가'].shift(long_candle_idx)
        long_body = (long_close - long_open).abs()
        long_bullish = long_close > long_open
        small_bodies = []
        small_candles_in_range = True
        for i in range(1, lookback + 1):
            small_open = df['시가'].shift(i)
            small_close = df['종가'].shift(i)
            small_body = (small_close - small_open).abs()
            small_bodies.append(small_body)
        max_small_body = pd.concat(small_bodies, axis=1).max(axis=1)
        is_long_candle = long_body >= 3 * max_small_body
        long_body_half_high = long_open + long_body / 2
        long_body_half_low = long_open - long_body / 2
        price_constraint_met = True
        for i in range(1, lookback + 1):
            small_open = df['시가'].shift(i)
            small_close = df['종가'].shift(i)
            small_high = df[['시가', '종가']].shift(i).max(axis=1)
            small_low = df[['시가', '종가']].shift(i).min(axis=1)
            intrusion = (small_high > long_body_half_high) | (small_low < long_body_half_low)
            price_constraint_met = price_constraint_met & ~intrusion
        condition = (
                rising_window &
                long_bullish &
                is_long_candle &
                price_constraint_met
        )
        high_gapping_conditions.append(condition)
    final_condition = pd.concat(high_gapping_conditions, axis=1).any(axis=1)
    return final_condition.astype(int)


def is_low_gapping_play(df):  # 저가 갭핑플레이형
    falling_window = is_falling_window(df)
    low_gapping_conditions = []
    for lookback in range(4, 8):
        long_candle_idx = lookback + 1
        long_open = df['시가'].shift(long_candle_idx)
        long_close = df['종가'].shift(long_candle_idx)
        long_body = (long_open - long_close).abs()
        long_bearish = long_close < long_open
        small_bodies = []
        for i in range(1, lookback + 1):
            small_open = df['시가'].shift(i)
            small_close = df['종가'].shift(i)
            small_body = (small_close - small_open).abs()
            small_bodies.append(small_body)
        max_small_body = pd.concat(small_bodies, axis=1).max(axis=1)
        is_long_candle = long_body >= 3 * max_small_body
        long_body_half_high = long_open + long_body / 2
        long_body_half_low = long_open - long_body / 2
        price_constraint_met = True
        for i in range(1, lookback + 1):
            small_open = df['시가'].shift(i)
            small_close = df['종가'].shift(i)
            small_high = df[['시가', '종가']].shift(i).max(axis=1)
            small_low = df[['시가', '종가']].shift(i).min(axis=1)
            intrusion = (small_high > long_body_half_high) | (small_low < long_body_half_low)
            price_constraint_met = price_constraint_met & ~intrusion
        condition = (
                falling_window &
                long_bearish &
                is_long_candle &
                price_constraint_met
        )
        low_gapping_conditions.append(condition)
    final_condition = pd.concat(low_gapping_conditions, axis=1).any(axis=1)
    return final_condition.astype(int)


def is_rising_side_by_side(df):  # 상승나란히형
    rising_window_2days_ago = is_rising_window(df).shift(2)
    first_open = df['시가'].shift(1)
    first_close = df['종가'].shift(1)
    first_bullish = first_close > first_open
    first_body = first_close - first_open
    second_open = df['시가']
    second_close = df['종가']
    second_bullish = second_close > second_open
    second_body = second_close - second_open
    similar_open = (second_open >= 0.9 * first_open) & (second_open <= 1.1 * first_open)
    appropriate_body_size = (second_body >= 0.5 * first_body) & (second_body <= 2 * first_body)
    rising_side_by_side_condition = (
            rising_window_2days_ago &
            first_bullish &
            second_bullish &
            similar_open &
            appropriate_body_size
    )
    return rising_side_by_side_condition.astype(int)


def is_falling_side_by_side(df):  # 하락나란히형
    falling_window_2days_ago = is_falling_window(df).shift(2)
    first_open = df['시가'].shift(1)
    first_close = df['종가'].shift(1)
    first_bearish = first_close < first_open
    first_body = first_open - first_close
    second_open = df['시가']
    second_close = df['종가']
    second_bearish = second_close < second_open
    second_body = second_open - second_close
    similar_open = (second_open >= 0.9 * first_open) & (second_open <= 1.1 * first_open)
    appropriate_body_size = (second_body >= 0.5 * first_body) & (second_body <= 2 * first_body)
    falling_side_by_side_condition = (
            falling_window_2days_ago &
            first_bearish &
            second_bearish &
            similar_open &
            appropriate_body_size
    )
    return falling_side_by_side_condition.astype(int)


def is_rising_three_methods(df):  # 상승삼법형
    rising_three_conditions = []
    for small_candles in range(2, 5):
        first_long_idx = small_candles + 1
        first_open = df['시가'].shift(first_long_idx)
        first_close = df['종가'].shift(first_long_idx)
        first_bullish = first_close > first_open
        first_body = first_close - first_open
        second_open = df['시가']
        second_close = df['종가']
        second_bullish = second_close > second_open
        second_body = second_close - second_open
        small_bodies = []
        for i in range(1, small_candles + 1):
            small_open = df['시가'].shift(i)
            small_close = df['종가'].shift(i)
            small_body = (small_close - small_open).abs()
            small_bodies.append(small_body)
        max_small_body = pd.concat(small_bodies, axis=1).max(axis=1)
        first_is_long = first_body >= 3 * max_small_body
        second_is_long = second_body >= 3 * max_small_body
        higher_close = second_close > first_close
        higher_open = second_open > first_open
        condition = (
                first_bullish &
                second_bullish &
                first_is_long &
                second_is_long &
                higher_close &
                higher_open
        )
        rising_three_conditions.append(condition)
    final_condition = pd.concat(rising_three_conditions, axis=1).any(axis=1)
    return final_condition.astype(int)


def is_falling_three_methods(df):  # 하락삼법형
    falling_three_conditions = []
    for small_candles in range(2, 5):
        first_long_idx = small_candles + 1
        first_open = df['시가'].shift(first_long_idx)
        first_close = df['종가'].shift(first_long_idx)
        first_bearish = first_close < first_open
        first_body = first_open - first_close
        second_open = df['시가']
        second_close = df['종가']
        second_bearish = second_close < second_open
        second_body = second_open - second_close
        small_bodies = []
        for i in range(1, small_candles + 1):
            small_open = df['시가'].shift(i)
            small_close = df['종가'].shift(i)
            small_body = (small_close - small_open).abs()
            small_bodies.append(small_body)
        max_small_body = pd.concat(small_bodies, axis=1).max(axis=1)
        first_is_long = first_body >= 3 * max_small_body
        second_is_long = second_body >= 3 * max_small_body
        lower_close = second_close < first_close
        lower_open = second_open < first_open
        condition = (
                first_bearish &
                second_bearish &
                first_is_long &
                second_is_long &
                lower_close &
                lower_open
        )
        falling_three_conditions.append(condition)
    final_condition = pd.concat(falling_three_conditions, axis=1).any(axis=1)
    return final_condition.astype(int)


def is_rising_separating_lines(df):  # 상승갈림길형
    uptrend = is_uptrend(df)
    prev_open = df['시가'].shift(1)
    prev_close = df['종가'].shift(1)
    prev_bearish = prev_close < prev_open
    curr_open = df['시가']
    curr_close = df['종가']
    curr_bullish = curr_close > curr_open
    similar_open = (curr_open >= 0.9 * prev_open) & (curr_open <= 1.1 * prev_open)
    rising_separating_condition = (
            uptrend &
            prev_bearish &
            curr_bullish &
            similar_open
    )
    return rising_separating_condition.astype(int)


def is_falling_separating_lines(df):  # 하락갈림길형
    downtrend = is_downtrend(df)
    prev_open = df['시가'].shift(1)
    prev_close = df['종가'].shift(1)
    prev_bullish = prev_close > prev_open
    curr_open = df['시가']
    curr_close = df['종가']
    curr_bearish = curr_close < curr_open
    similar_open = (curr_open >= 0.9 * prev_open) & (curr_open <= 1.1 * prev_open)
    falling_separating_condition = (
            downtrend &
            prev_bullish &
            curr_bearish &
            similar_open
    )
    return falling_separating_condition.astype(int)


def is_doji(df):
    body = (df['시가'] - df['종가']).abs()
    total_length = df['고가'] - df['저가']
    doji_condition = body < (total_length * 0.05)
    return doji_condition.astype(int)


def is_northern_doji(df):  # 북향도지형
    uptrend = is_uptrend(df)
    doji = is_doji(df)
    doji_count_20days = is_doji(df).rolling(window=20, min_periods=1).sum()
    valid_pattern = doji_count_20days < 5
    northern_doji_condition = (
            uptrend &
            doji &
            valid_pattern
    )
    return northern_doji_condition.astype(int)


# def is_rising_long_legged_doji(df):#상승키다리도지형
#     uptrend = is_uptrend(df)
#     doji = is_doji(df)
#     doji_count_20days = is_doji(df).rolling(window=20, min_periods=1).sum()
#     valid_pattern = doji_count_20days < 5
#     body_center = (df['시가'] + df['종가']) / 2
#     high = df['고가']
#     low = df['저가']
#     range_third = (high - low) / 3
#     middle_low = low + range_third
#     middle_high = high - range_third
#     in_middle_third = (body_center >= middle_low) & (body_center <= middle_high)
#     rising_long_legged_doji_condition = (
#         uptrend &
#         doji &
#         valid_pattern &
#         in_middle_third
#     )
#     return rising_long_legged_doji_condition.astype(int)

def is_falling_long_legged_doji(df):  # 하락키다리도지형
    downtrend = is_downtrend(df)
    doji = is_doji(df)
    doji_count_20days = is_doji(df).rolling(window=20, min_periods=1).sum()
    valid_pattern = doji_count_20days < 5
    body_center = (df['시가'] + df['종가']) / 2
    high = df['고가']
    low = df['저가']
    range_third = (high - low) / 3
    middle_low = low + range_third
    middle_high = high - range_third
    in_middle_third = (body_center >= middle_low) & (body_center <= middle_high)
    falling_long_legged_doji_condition = (
            downtrend &
            doji &
            valid_pattern &
            in_middle_third
    )
    return falling_long_legged_doji_condition.astype(int)


# def is_rising_gravestone_doji(df):#상승비석도지형
#     uptrend = is_uptrend(df)
#     doji = is_doji(df)
#     doji_count_20days = is_doji(df).rolling(window=20, min_periods=1).sum()
#     valid_pattern = doji_count_20days < 5
#     body_center = (df['시가'] + df['종가']) / 2
#     high = df['고가']
#     low = df['저가']
#     bottom_quarter_top = low + (high - low) / 4
#     in_bottom_quarter = body_center <= bottom_quarter_top
#     rising_gravestone_doji_condition = (
#         uptrend &
#         doji &
#         valid_pattern &
#         in_bottom_quarter
#     )
#     return rising_gravestone_doji_condition.astype(int)

def is_falling_gravestone_doji(df):  # 하락비석도지형
    downtrend = is_downtrend(df)
    doji = is_doji(df)
    doji_count_20days = is_doji(df).rolling(window=20, min_periods=1).sum()
    valid_pattern = doji_count_20days < 5
    body_center = (df['시가'] + df['종가']) / 2
    high = df['고가']
    low = df['저가']
    bottom_quarter_top = low + (high - low) / 4
    in_bottom_quarter = body_center <= bottom_quarter_top
    falling_gravestone_doji_condition = (
            downtrend &
            doji &
            valid_pattern &
            in_bottom_quarter
    )
    return falling_gravestone_doji_condition.astype(int)


def is_rising_dragonfly_doji(df):  # 상승잠자리도지형
    uptrend = is_uptrend(df)
    doji = is_doji(df)
    doji_count_20days = is_doji(df).rolling(window=20, min_periods=1).sum()
    valid_pattern = doji_count_20days < 5
    body_center = (df['시가'] + df['종가']) / 2
    high = df['고가']
    low = df['저가']
    top_quarter_bottom = high - (high - low) / 4
    in_top_quarter = body_center >= top_quarter_bottom
    rising_dragonfly_doji_condition = (
            uptrend &
            doji &
            valid_pattern &
            in_top_quarter
    )
    return rising_dragonfly_doji_condition.astype(int)


# def is_falling_dragonfly_doji(df):#하락잠자리도지형
#     downtrend = is_downtrend(df)
#     doji = is_doji(df)
#     doji_count_20days = is_doji(df).rolling(window=20, min_periods=1).sum()
#     valid_pattern = doji_count_20days < 5
#     body_center = (df['시가'] + df['종가']) / 2
#     high = df['고가']
#     low = df['저가']
#     top_quarter_bottom = high - (high - low) / 4
#     in_top_quarter = body_center >= top_quarter_bottom
#     falling_dragonfly_doji_condition = (
#         downtrend &
#         doji &
#         valid_pattern &
#         in_top_quarter
#     )
#     return falling_dragonfly_doji_condition.astype(int)

# def is_rising_three_stars(df):#상승삼별형
#     doji_today = is_doji(df)
#     doji_1day_ago = is_doji(df).shift(1)
#     doji_2day_ago = is_doji(df).shift(2)
#     three_consecutive_doji = doji_today & doji_1day_ago & doji_2day_ago
#     doji_count_20days = is_doji(df).rolling(window=20, min_periods=1).sum()
#     valid_pattern = doji_count_20days < 6
#     high_20days = df['고가'].rolling(window=20, min_periods=1).max()
#     high_today = df['고가']
#     high_1day_ago = df['고가'].shift(1)
#     high_2day_ago = df['고가'].shift(2)
#     high_breakout = (
#         (high_today > high_20days) |
#         (high_1day_ago > high_20days.shift(1)) |
#         (high_2day_ago > high_20days.shift(2))
#     )
#     rising_three_stars_condition = (
#         three_consecutive_doji &
#         valid_pattern &
#         high_breakout
#     )
#     return rising_three_stars_condition.astype(int)

def is_falling_three_stars(df):  # 하락삼별형
    doji_today = is_doji(df)
    doji_1day_ago = is_doji(df).shift(1)
    doji_2day_ago = is_doji(df).shift(2)
    three_consecutive_doji = doji_today & doji_1day_ago & doji_2day_ago
    doji_count_20days = is_doji(df).rolling(window=20, min_periods=1).sum()
    valid_pattern = doji_count_20days < 6
    low_20days = df['저가'].rolling(window=20, min_periods=1).min()
    low_today = df['저가']
    low_1day_ago = df['저가'].shift(1)
    low_2day_ago = df['저가'].shift(2)
    low_breakdown = (
            (low_today < low_20days) |
            (low_1day_ago < low_20days.shift(1)) |
            (low_2day_ago < low_20days.shift(2))
    )
    falling_three_stars_condition = (
            three_consecutive_doji &
            valid_pattern &
            low_breakdown
    )
    return falling_three_stars_condition.astype(int)



