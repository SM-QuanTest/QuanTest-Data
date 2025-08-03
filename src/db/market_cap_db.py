import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.database import engine
from src.utils.utils import process_tickers


def upsert_market_cap(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    1) cybos_ticker, cap을 가진 df 입력
    2) input_df['cybos_ticker']에서 ticker 변환
    3) stocks 테이블에서 stock_id 매핑
    4) market_caps 테이블에서 입력받은 df와 동일한 ticker값 가지고 있는 경우(stock_id로 참조) cap값 업데이트
    5) 신규 생성된 stock이라 markdet_caps에 행 없는 경우 insert
    """

    market_cap_df = input_df.copy()
    market_cap_df['ticker'] = market_cap_df['cybos_ticker'].apply(process_tickers)

    # transaction 시작
    with engine.begin() as conn:
        # 기존 업종 조회
        try:
            stock_id_ticker = pd.read_sql(
                text("SELECT id AS stock_id, ticker FROM public.stocks"),
                con=conn
            )
        except SQLAlchemyError as e:
            raise RuntimeError(f"DB 조회 실패: {e}")

        # 머지
        merged = market_cap_df.merge(stock_id_ticker, on='ticker', how='left')
        missing = merged[merged['stock_id'].isna()]['ticker'].unique()
        if len(missing) > 0:
            raise RuntimeError(f"알 수 없는 stock_id/ticker 발생: {missing.tolist()}")

        # UPSERT
        upsert_sql = text("""
                          INSERT INTO public.market_caps (stock_id, cap)
                          VALUES (:stock_id, :cap) ON CONFLICT (stock_id) DO
                          UPDATE SET cap = EXCLUDED.cap;
                          """)

        # 실행
        records = merged[['stock_id', 'cap']].to_dict(orient='records')
        try:
            conn.execute(upsert_sql, records)
        except SQLAlchemyError as e:
            raise RuntimeError(f"market_caps upsert 실패: {e}")

    update_df = merged[['cybos_ticker', 'ticker', 'stock_id', 'cap']]
    print(f">>> 시가총액 {len(update_df)}건 업데이트 완료.")
    return update_df
