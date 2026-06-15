# -*- coding: utf-8 -*-
"""
타거래소 가격 비교 UI (독립 실행형)

- 사건 발생 거래소와 비교군 거래소(복수)의 혐의기간 내 최고가를 비교
- 비교군 거래소 최고가 평균, 사건 발생 거래소 가격의 평균 대비 백분율 표시
- 결제통화가 USDT인 거래소는 업비트 KRW/USDT 혐의기간 최고가로 원화 환산
"""

from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st

from exchange_apis import fetch_ohlcv, get_supported_exchanges
from logger_simple import get_logger

logger = get_logger(__name__)

KST = timezone(timedelta(hours=9))


def _parse_kst_range(
    start_date_str: str, start_time_str: str, end_date_str: str, end_time_str: str
):
    start_str = f"{start_date_str.strip()} {start_time_str.strip()}"
    end_str = f"{end_date_str.strip()} {end_time_str.strip()}"
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"]:
        try:
            start_dt_kst = datetime.strptime(start_str, fmt).replace(tzinfo=KST)
            end_dt_kst = datetime.strptime(end_str, fmt).replace(tzinfo=KST)
            return start_dt_kst, end_dt_kst
        except ValueError:
            continue
    raise ValueError("날짜/시간 형식을 인식할 수 없습니다. 예: 2024-07-22 09:00:00")


def _fetch_period_high(exchange_id: str, base: str, quote: str, start_dt: datetime, end_dt: datetime):
    """혐의기간(start_dt~end_dt, UTC) 내 1분봉 high의 최댓값을 반환. 데이터 없으면 None."""
    df = fetch_ohlcv(
        exchange_id=exchange_id,
        base=base,
        quote=quote,
        start_dt=start_dt,
        end_dt=end_dt,
        interval_unit="minute",
        interval_value=1,
    )
    if df is None or df.empty or "high" not in df.columns:
        return None
    return float(df["high"].max())


def _fmt_price(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    if abs(value) >= 1:
        return f"{value:,.2f}"
    return f"{value:,.8f}"


def show_page():
    """타거래소 가격 비교 페이지를 표시합니다."""
    logger.info("=== 타거래소 가격 비교 페이지 시작 ===")

    st.title("타거래소 가격 비교")
    st.caption(
        "사건 발생 거래소와 비교군 거래소(복수 선택)의 혐의기간 내 최고가를 비교합니다. "
        "결제통화가 USDT인 거래소는 업비트 KRW/USDT 혐의기간 최고가로 원화 환산하여 비교합니다. "
        "**인터넷이 되는 환경에서만 실행해 주세요.**"
    )

    exchanges = get_supported_exchanges()
    exchange_options = [f"{name} ({eid})" for eid, name in exchanges]
    exchange_id_by_label = {f"{name} ({eid})": eid for eid, name in exchanges}
    exchange_name_by_id = {eid: name for eid, name in exchanges}

    st.subheader("비교 조건 입력")

    c1, c2, c3 = st.columns(3)
    with c1:
        incident_label = st.selectbox(
            "사건 발생 거래소", exchange_options, key="cmp_incident_exchange"
        )
        incident_exchange_id = exchange_id_by_label[incident_label]
    with c2:
        coin = (
            st.text_input("코인명", value="BTC", help="예: BTC, ETH", key="cmp_coin")
            .strip()
            .upper()
            or "BTC"
        )
    with c3:
        incident_quote = st.selectbox(
            "사건 발생 거래소 결제통화", ["KRW", "USDT"], key="cmp_incident_quote"
        )

    compare_labels = st.multiselect(
        "비교군 거래소 (복수 선택)",
        exchange_options,
        key="cmp_compare_exchanges",
    )
    compare_exchange_ids = [exchange_id_by_label[label] for label in compare_labels]

    compare_quotes = {}
    if compare_exchange_ids:
        st.caption("비교군 거래소별 결제통화")
        cols = st.columns(min(len(compare_exchange_ids), 4))
        for i, eid in enumerate(compare_exchange_ids):
            with cols[i % len(cols)]:
                compare_quotes[eid] = st.selectbox(
                    f"{exchange_name_by_id[eid]}",
                    ["KRW", "USDT"],
                    key=f"cmp_quote_{eid}",
                )

    st.markdown("**혐의기간 (KST)**")
    c4, c5, c6, c7 = st.columns(4)
    today_kst = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d")
    with c4:
        start_date_str = st.text_input("시작일", value=today_kst, key="cmp_start_date")
        st.caption("시작일 (KST)")
    with c5:
        start_time_str = st.text_input("시작시간", value="00:00:00", key="cmp_start_time")
        st.caption("시작시간 (KST)")
    with c6:
        end_date_str = st.text_input("종료일", value=today_kst, key="cmp_end_date")
        st.caption("종료일 (KST)")
    with c7:
        end_time_str = st.text_input("종료시간", value="23:59:59", key="cmp_end_time")
        st.caption("종료시간 (KST)")

    st.caption(
        "최고가는 1분봉(high) 기준으로 계산됩니다. 혐의기간이 길수록 조회 시간이 오래 걸릴 수 있습니다."
    )

    submitted = st.button("비교 실행", type="primary", use_container_width=True)

    if not submitted:
        return

    if not compare_exchange_ids:
        st.error("비교군 거래소를 1개 이상 선택해주세요.")
        st.stop()

    try:
        start_dt_kst, end_dt_kst = _parse_kst_range(
            start_date_str, start_time_str, end_date_str, end_time_str
        )
    except ValueError as e:
        st.error(f"날짜/시간 입력 오류: {e}")
        st.stop()

    if start_dt_kst >= end_dt_kst:
        st.error("시작일시가 종료일시보다 이전이어야 합니다.")
        st.stop()

    start_dt_utc = start_dt_kst.astimezone(timezone.utc)
    end_dt_utc = end_dt_kst.astimezone(timezone.utc)

    # USDT 결제통화 거래소가 있으면 업비트 KRW/USDT 혐의기간 최고가를 환산 기준으로 사용
    krw_usdt_rate = None
    needs_rate = incident_quote == "USDT" or any(
        q == "USDT" for q in compare_quotes.values()
    )
    if needs_rate:
        with st.spinner("업비트 KRW/USDT 혐의기간 최고가 조회 중..."):
            try:
                krw_usdt_rate = _fetch_period_high(
                    "upbit", "USDT", "KRW", start_dt_utc, end_dt_utc
                )
            except Exception as e:
                logger.error(f"업비트 KRW/USDT 조회 오류: {e}", exc_info=True)
                st.error(f"업비트 KRW/USDT 최고가 조회 중 오류: {e}")
                st.stop()
        if krw_usdt_rate is None:
            st.error(
                "업비트 KRW/USDT 혐의기간 내 데이터를 조회할 수 없습니다. "
                "혐의기간을 확인해주세요."
            )
            st.stop()

    rows = []
    errors = []

    def fetch_row(role: str, exchange_id: str, quote: str):
        try:
            with st.spinner(
                f"{exchange_name_by_id[exchange_id]} ({quote}) 최고가 조회 중..."
            ):
                high = _fetch_period_high(exchange_id, coin, quote, start_dt_utc, end_dt_utc)
        except Exception as e:
            logger.error(f"{exchange_id} 조회 오류: {e}", exc_info=True)
            errors.append(f"{exchange_name_by_id[exchange_id]} ({quote}): {e}")
            return None
        if high is None:
            errors.append(
                f"{exchange_name_by_id[exchange_id]} ({quote}): 혐의기간 내 데이터가 없습니다."
            )
            return None
        krw_high = high * krw_usdt_rate if quote == "USDT" else high
        return {
            "구분": role,
            "거래소": exchange_name_by_id[exchange_id],
            "결제통화": quote,
            "최고가": high,
            "원화환산 최고가": krw_high,
        }

    incident_row = fetch_row("사건발생", incident_exchange_id, incident_quote)
    if incident_row:
        rows.append(incident_row)

    for eid in compare_exchange_ids:
        row = fetch_row("비교군", eid, compare_quotes[eid])
        if row:
            rows.append(row)

    for err in errors:
        st.warning(err)

    if not rows:
        st.error("조회된 데이터가 없습니다.")
        st.stop()

    df = pd.DataFrame(rows)
    compare_df = df[df["구분"] == "비교군"]
    incident_df = df[df["구분"] == "사건발생"]

    avg_compare_krw = (
        compare_df["원화환산 최고가"].mean() if not compare_df.empty else None
    )

    pct = None
    if avg_compare_krw and avg_compare_krw > 0 and not incident_df.empty:
        incident_krw = incident_df.iloc[0]["원화환산 최고가"]
        pct = incident_krw / avg_compare_krw * 100

    st.subheader("비교 결과")

    display_rows = []
    for _, r in df.iterrows():
        is_incident = r["구분"] == "사건발생"
        display_rows.append(
            {
                "구분": r["구분"],
                "거래소": r["거래소"],
                "결제통화": r["결제통화"],
                "최고가": f"{_fmt_price(r['최고가'])} {r['결제통화']}",
                "원화환산 최고가": f"{_fmt_price(r['원화환산 최고가'])} KRW",
                "비교군 평균 대비(%)": f"{pct:,.2f}%" if is_incident and pct is not None else "-",
            }
        )

    if avg_compare_krw is not None:
        display_rows.append(
            {
                "구분": "비교군 평균",
                "거래소": "-",
                "결제통화": "-",
                "최고가": "-",
                "원화환산 최고가": f"{_fmt_price(avg_compare_krw)} KRW",
                "비교군 평균 대비(%)": "-",
            }
        )

    df_display = pd.DataFrame(display_rows)
    st.dataframe(df_display, use_container_width=True, hide_index=True)

    if krw_usdt_rate is not None:
        st.caption(
            f"※ 결제통화가 USDT인 거래소의 원화환산 최고가는 혐의기간 내 업비트 KRW/USDT 최고가 "
            f"**{krw_usdt_rate:,.2f} KRW**를 기준으로 계산되었습니다."
        )

    st.caption(
        f"혐의기간 (KST): {start_dt_kst.strftime('%Y-%m-%d %H:%M:%S')} ~ "
        f"{end_dt_kst.strftime('%Y-%m-%d %H:%M:%S')}"
    )
