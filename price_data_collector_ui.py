# -*- coding: utf-8 -*-
"""
시세 데이터 수집 (Upbit 일봉) - 독립 실행형 UI

인터넷이 되는 환경(Streamlit Cloud 등)에서만 사용.
Upbit API로 일봉 데이터를 수집해 CSV로 다운로드하고,
오프라인 SPPO 앱의 [시세 데이터 관리]에서 업로드해 사용합니다.
"""

from datetime import datetime, timedelta

import pandas as pd
import requests
import streamlit as st

from logger_simple import get_logger

logger = get_logger(__name__)

UPBIT_DAYS_URL = "https://api.upbit.com/v1/candles/days"
UPBIT_START_DATE = datetime(2017, 9, 25)  # 업비트 서비스 시작일


def fetch_upbit_daily_candles(ticker: str, timeout: int = 10) -> pd.DataFrame:
    """
    Upbit API로 특정 ticker의 전체 일봉 데이터를 가져옵니다.
    Returns DataFrame with columns: ticker, date, open, high, low, close.
    """
    market = f"KRW-{ticker}"
    end_date = datetime.now()
    start_date = UPBIT_START_DATE
    all_data = []
    current_date = end_date

    while current_date >= start_date:
        params = {
            "market": market,
            "to": current_date.strftime("%Y-%m-%d %H:%M:%S"),
            "count": 200,
        }
        try:
            response = requests.get(UPBIT_DAYS_URL, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.Timeout:
            logger.warning(f"API 타임아웃: {market}")
            st.error("API 요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요.")
            break
        except requests.exceptions.ConnectionError:
            logger.warning("인터넷 연결 오류")
            st.error("인터넷 연결을 확인해주세요.")
            break
        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP 에러: {market} - {e}")
            st.error(f"API 요청 중 오류가 발생했습니다: {e}")
            break
        except Exception as e:
            logger.warning(f"데이터 수집 실패: {market} - {e}")
            st.error(f"데이터를 가져오는 중 오류가 발생했습니다: {e}")
            break

        if not data:
            break

        for item in data:
            all_data.append({
                "ticker": ticker,
                "date": item["candle_date_time_kst"].split("T")[0],
                "open": float(item["opening_price"]),
                "high": float(item["high_price"]),
                "low": float(item["low_price"]),
                "close": float(item["trade_price"]),
            })

        last_date_str = data[-1]["candle_date_time_kst"].split("T")[0]
        current_date = datetime.strptime(last_date_str, "%Y-%m-%d") - timedelta(days=1)

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    # 날짜 오름차순 정렬
    df = df.sort_values("date").drop_duplicates(subset=["ticker", "date"]).reset_index(drop=True)
    return df


def show_page():
    """시세 데이터 수집(Upbit 일봉) 페이지를 표시합니다."""
    logger.info("=== 시세 데이터 수집 (Upbit 일봉) 페이지 시작 ===")

    st.title("원화가치 환산용 시세 데이터 수집")
    st.caption(
        "Upbit API로 코인 일봉 시세를 수집해 CSV로 저장합니다. "
        "**인터넷이 되는 환경에서만** 실행해 주세요. "
        "다운로드한 CSV는 오프라인 SPPO 앱의 [시세 데이터 관리]에서 업로드해 사용합니다."
    )

    st.info(
        "원화가치 환산용 시세 데이터는 이 페이지에서 수집·다운로드한 뒤, [시세 데이터 관리]에서 업로드해 주세요."
    )

    st.subheader("수집 조건")
    ticker = st.text_input(
        "Ticker (코인 심볼)",
        value="BTC",
        help="예: BTC, ETH, XRP",
        key="price_collector_ticker",
    ).strip().upper() or "BTC"

    if st.button("시세 데이터 수집 실행", type="primary"):
        with st.spinner(f"{ticker} 일봉 데이터 수집 중..."):
            df = fetch_upbit_daily_candles(ticker)
        if df.empty:
            st.warning(f"{ticker}에 대한 데이터가 없거나 수집에 실패했습니다.")
        else:
            st.session_state["price_collector_last_df"] = df
            st.session_state["price_collector_last_ticker"] = ticker
            st.success(f"총 {len(df):,}건 수집 완료. 아래에서 CSV를 다운로드해 주세요.")

    if "price_collector_last_df" in st.session_state:
        df = st.session_state["price_collector_last_df"]
        ticker_name = st.session_state.get("price_collector_last_ticker", "BTC")

        st.subheader("수집 결과")
        st.dataframe(df.head(100), use_container_width=True)
        if len(df) > 100:
            st.caption(f"상위 100건만 표시. 전체 {len(df):,}건은 CSV 다운로드로 저장됩니다.")

        st.subheader("CSV 저장")
        filename = f"upbit_daily_{ticker_name}_{df['date'].min()}_{df['date'].max()}.csv"
        csv_bytes = df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="CSV 파일 다운로드",
            data=csv_bytes,
            file_name=filename,
            mime="text/csv",
            key="price_collector_download",
        )
        st.caption(f"파일명: {filename}")
        st.caption("이 CSV를 SPPO 앱의 [시세 데이터 관리] → [시세 데이터 업로드]에서 업로드하면 DB에 반영됩니다.")
