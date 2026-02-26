# -*- coding: utf-8 -*-
"""
타거래소 데이터 수집 UI (독립 실행형)
- 사용자 입력: 코인명, 기간, 구간(일/시/분/초), 구간값
- 공개 API로 캔들 데이터 수집 후 CSV 저장
- 라이센스 검증 없음 (인터넷 가능 PC 전용 배포용)
"""

from datetime import datetime, timezone, timedelta

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from exchange_apis import (
    EXCHANGE_APIS,
    OHLCV_COLUMNS,
    fetch_ohlcv,
    get_supported_exchanges,
)
from logger_simple import get_logger

logger = get_logger(__name__)


def fill_missing_ohlcv_gaps(
    df: pd.DataFrame,
    start_dt: datetime,
    end_dt: datetime,
    interval_unit: str,
    interval_value: int,
) -> pd.DataFrame:
    """
    요청 구간에서 API에 데이터가 없는(거래량 없는) 구간을
    직전 구간의 close로 OHLC를 채우고, 거래량을 0으로 채워 연속된 표/차트를 만듭니다.
    """
    if df is None or df.empty or "datetime_utc" not in df.columns or "close" not in df.columns:
        return df

    if interval_unit == "minute":
        delta = timedelta(minutes=interval_value)
    elif interval_unit == "hour":
        delta = timedelta(hours=interval_value)
    elif interval_unit == "day":
        delta = timedelta(days=interval_value)
    elif interval_unit == "second":
        delta = timedelta(seconds=interval_value)
    else:
        return df

    if "volume" not in df.columns:
        df = df.copy()
        df["volume"] = 0.0

    df_sorted = df.sort_values("datetime_utc").reset_index(drop=True)
    
    # 실제 데이터의 첫 번째와 마지막 시간을 기준으로 expected_times 생성
    if len(df_sorted) == 0:
        return df
    
    actual_start = df_sorted["datetime_utc"].min()
    actual_end = df_sorted["datetime_utc"].max()
    
    # 실제 데이터 범위 내에서만 gap을 채움
    expected_times = []
    t = actual_start
    while t <= actual_end:
        expected_times.append(t)
        t = t + delta

    if not expected_times:
        return df

    expected_df = pd.DataFrame({"datetime_utc": expected_times})
    merged = expected_df.merge(
        df_sorted[
            [c for c in OHLCV_COLUMNS if c in df_sorted.columns]
        ],
        on="datetime_utc",
        how="left",
    )
    merged["close"] = merged["close"].ffill()
    merged["close"] = merged["close"].bfill()
    mask = merged["open"].isna()
    merged.loc[mask, "open"] = merged.loc[mask, "close"]
    merged.loc[mask, "high"] = merged.loc[mask, "close"]
    merged.loc[mask, "low"] = merged.loc[mask, "close"]
    merged.loc[mask, "volume"] = 0.0
    out_cols = [c for c in OHLCV_COLUMNS if c in merged.columns]
    return merged[out_cols].reset_index(drop=True)


def show_page():
    """타거래소 데이터 수집 페이지를 표시합니다."""
    logger.info("=== 타거래소 데이터 수집 페이지 시작 ===")

    st.title("타거래소 데이터 수집")
    st.caption(
        "코인명, 기간, 구간(일/시/분/초)을 입력하면 공개 API로 차트 데이터를 수집해 CSV로 저장합니다. "
        "해외·국내 거래소 모두 계정 없이 이용 가능합니다. **인터넷이 되는 환경에서만 실행해 주세요.**"
    )

    # 지원 거래소 안내
    with st.expander("지원 거래소 안내 (계정 없이 공개 API만 사용)", expanded=True):
        exchanges = get_supported_exchanges()
        cols = st.columns(min(len(exchanges), 4))
        for i, (eid, name) in enumerate(exchanges):
            cols[i % 4].markdown(f"- **{name}**")
        st.caption(
            "위 거래소는 로그인 없이 캔들/OHLCV 데이터를 조회할 수 있습니다. "
            "구간(interval)은 거래소별로 지원 범위가 다릅니다. 국내 거래소는 KRW, 해외는 USDT/USD 등을 선택하세요."
        )

    # 입력 폼
    st.subheader("수집 조건 입력")
    interval_unit_map = {"일": "day", "시": "hour", "분": "minute", "초": "second"}

    if "exchange_collector_start" not in st.session_state:
        st.session_state["exchange_collector_start"] = (
            pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=30)
        ).strftime("%Y-%m-%d %H:%M:%S")
    if "exchange_collector_end" not in st.session_state:
        st.session_state["exchange_collector_end"] = pd.Timestamp.now(
            tz="UTC"
        ).strftime("%Y-%m-%d %H:%M:%S")
    if "exchange_collector_coin" not in st.session_state:
        st.session_state["exchange_collector_coin"] = "BTC"
    if "exchange_collector_exchange_id" not in st.session_state:
        exchanges = get_supported_exchanges()
        st.session_state["exchange_collector_exchange_id"] = (
            exchanges[0][0] if exchanges else "binance"
        )

    # 첫 번째 줄: 거래소, 코인, 결제통화
    c1, c2, c3 = st.columns(3)

    with c1:
        exchange_options = [
            f"{name} ({eid})" for eid, name in get_supported_exchanges()
        ]
        current_exchange_id = st.session_state.get(
            "exchange_collector_exchange_id", ""
        )
        exchanges = get_supported_exchanges()
        current_index = 0
        for i, (eid, _) in enumerate(exchanges):
            if eid == current_exchange_id:
                current_index = i
                break
        widget_key = "exchange_collector_exchange"
        if widget_key in st.session_state:
            widget_selected = st.session_state[widget_key]
            widget_eid = widget_selected.split(" (")[-1].rstrip(")")
            valid_indices = [
                i
                for i, (eid, _) in enumerate(exchanges)
                if eid == widget_eid
            ]
            if valid_indices:
                current_index = valid_indices[0]
        exchange_choice = st.selectbox(
            "거래소",
            exchange_options,
            index=current_index,
            label_visibility="collapsed",
            key=widget_key,
        )
        exchange_id = exchange_choice.split(" (")[-1].rstrip(")")
        st.caption("거래소")

    with c2:
        coin_base = (
            st.text_input(
                "코인",
                value=st.session_state.get("exchange_collector_coin", "BTC"),
                help="예: BTC, ETH",
                label_visibility="collapsed",
                key="exchange_collector_coin_input",
            )
            .strip()
            .upper()
            or "BTC"
        )
        st.caption("코인")

    with c3:
        quote_options = ["KRW", "USDT", "USD", "BUSD", "BTC"]
        quote = st.selectbox(
            "결제통화",
            quote_options,
            index=1,  # USDT 기본값
            label_visibility="collapsed",
            key="exchange_collector_quote",
        )
        st.caption("결제통화")

    # 두 번째 줄: 시작일, 시작시간, 종료일, 종료시간, 구간단위, 구간값
    c4, c5, c6, c7, c8, c9 = st.columns(6)
    
    with c4:
        start_date_str = st.text_input(
            "시작일",
            value=(pd.Timestamp.now(tz="Asia/Seoul") - pd.Timedelta(days=30)).strftime("%Y-%m-%d"),
            help="예: 2024-07-22 (KST 기준)",
            label_visibility="collapsed",
            key="start_date_input",
        )
        st.caption("시작일 (KST)")
    
    with c8:
        interval_type = st.selectbox(
            "구간단위",
            ["일", "시", "분", "초"],
            index=0,
            label_visibility="collapsed",
            key="interval_type_select",
        )
        st.caption("구간단위")
    
    with c9:
        if interval_type == "초":
            interval_value = st.number_input(
                "구간값(초)",
                min_value=1,
                max_value=60,
                value=1,
                step=1,
                label_visibility="collapsed",
                key="interval_value_input",
            )
        else:
            interval_value = st.number_input(
                "구간값",
                min_value=1,
                max_value=30,
                value=1,
                step=1,
                label_visibility="collapsed",
                key="interval_value_input",
            )
        st.caption("구간값")
    
    interval_unit = interval_unit_map[interval_type]
    is_daily = (interval_type == "일")
    
    with c5:
        start_time_str = st.text_input(
            "시작시간",
            value="09:00:00" if is_daily else "00:00:00",
            help="일봉: 09:00:00 고정 (UTC 00:00 기준)",
            label_visibility="collapsed",
            key="start_time_input",
            disabled=is_daily,
        )
        st.caption("시작시간")
    
    with c6:
        end_date_str = st.text_input(
            "종료일",
            value=pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d"),
            help="예: 2024-10-25 (KST 기준)",
            label_visibility="collapsed",
            key="end_date_input",
        )
        st.caption("종료일 (KST)")
    
    with c7:
        end_time_str = st.text_input(
            "종료시간",
            value="09:00:00" if is_daily else "23:59:59",
            help="일봉: 09:00:00 고정 (UTC 00:00 기준)",
            label_visibility="collapsed",
            key="end_time_input",
            disabled=is_daily,
        )
        st.caption("종료시간")

    submitted = st.button(
        "데이터 수집 실행", type="primary", use_container_width=True
    )

    if submitted:
        st.session_state["exchange_collector_coin"] = coin_base
        st.session_state["exchange_collector_exchange_id"] = exchange_id

    if submitted:
        try:
            kst = timezone(timedelta(hours=9))
            
            # 일봉: 날짜 + 09:00:00 KST 고정 (= UTC 00:00:00)
            if is_daily:
                start_str_clean = start_date_str.strip()
                end_str_clean = end_date_str.strip()
                
                # 날짜 파싱 (KST 09:00:00 고정)
                for fmt in ["%Y-%m-%d", "%Y/%m/%d"]:
                    try:
                        start_dt_kst = datetime.strptime(start_str_clean, fmt).replace(hour=9, minute=0, second=0, tzinfo=kst)
                        end_dt_kst = datetime.strptime(end_str_clean, fmt).replace(hour=9, minute=0, second=0, tzinfo=kst)
                        break
                    except ValueError:
                        continue
                else:
                    raise ValueError("날짜 형식을 인식할 수 없습니다. 예: 2024-07-22")
            
            # 분봉/시봉: 날짜 + 시간 사용
            else:
                start_datetime_str = f"{start_date_str.strip()} {start_time_str.strip()}"
                end_datetime_str = f"{end_date_str.strip()} {end_time_str.strip()}"
                
                # 날짜+시간 파싱
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"]:
                    try:
                        start_dt_kst = datetime.strptime(start_datetime_str, fmt).replace(tzinfo=kst)
                        end_dt_kst = datetime.strptime(end_datetime_str, fmt).replace(tzinfo=kst)
                        break
                    except ValueError:
                        continue
                else:
                    raise ValueError("날짜/시간 형식을 인식할 수 없습니다. 예: 2024-07-22 09:00:00")

            # KST를 UTC로 변환
            start_dt = start_dt_kst.astimezone(timezone.utc)
            end_dt = end_dt_kst.astimezone(timezone.utc)
            
            if is_daily:
                st.info(
                    f"**일봉 데이터 수집 (KST 09:00 기준)**\n\n"
                    f"- 시작: {start_dt_kst.strftime('%Y-%m-%d %H:%M:%S')} KST → {start_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                    f"- 종료: {end_dt_kst.strftime('%Y-%m-%d %H:%M:%S')} KST → {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
                    f"**참고**: 일봉은 KST 09:00 (= UTC 00:00) 기준으로 수집됩니다. "
                    f"CSV의 `datetime_kst`는 `2024-07-22 09:00:00` 형태로 저장되며, "
                    f"이는 사건 데이터와 정확히 매칭됩니다."
                )
            else:
                st.info(
                    f"**입력 시간 (KST) → API 호출 시간 (UTC)**\n\n"
                    f"- 시작: {start_dt_kst.strftime('%Y-%m-%d %H:%M:%S')} KST → {start_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                    f"- 종료: {end_dt_kst.strftime('%Y-%m-%d %H:%M:%S')} KST → {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC"
                )
        except Exception as e:
            st.error(f"날짜/시간 입력 오류: {e}")
            st.stop()

        if start_dt >= end_dt:
            st.error("시작일시가 종료일시보다 이전이어야 합니다.")
            st.stop()

        api = EXCHANGE_APIS.get(exchange_id)
        if api and not api.get_interval_param(
            interval_unit, int(interval_value)
        ):
            st.warning(
                f"선택한 거래소({api.name})에서 해당 구간({interval_type} {interval_value})을 지원하지 않습니다. "
                "다른 구간(예: 1분, 5분, 1시간, 1일)을 선택해 보세요."
            )
            st.stop()

        with st.spinner(f"{exchange_id}에서 데이터 수집 중..."):
            try:
                df = fetch_ohlcv(
                    exchange_id=exchange_id,
                    base=coin_base,
                    quote=quote,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    interval_unit=interval_unit,
                    interval_value=int(interval_value),
                )
            except Exception as e:
                logger.error(f"데이터 수집 중 오류 발생: {e}", exc_info=True)
                err_msg = str(e).strip()
                st.error("**데이터 수집 오류**")
                st.code(err_msg, language=None)
                st.caption("위 메시지는 거래소 API에서 반환된 내용입니다.")
                api_dbg = EXCHANGE_APIS.get(exchange_id)
                dbg = (
                    getattr(api_dbg, "last_debug", None)
                    if api_dbg
                    else None
                )
                if dbg:
                    with st.expander("진단 정보(마지막 API 호출)", expanded=False):
                        st.json(dbg)
                st.stop()

        if df.empty:
            api_dbg = EXCHANGE_APIS.get(exchange_id)
            dbg = (
                getattr(api_dbg, "last_debug", None) if api_dbg else None
            )
            
            # 로그에 상세 정보 기록
            logger.warning(
                f"데이터 없음: exchange={exchange_id}, base={coin_base}, quote={quote}, "
                f"start={start_dt.isoformat()}, end={end_dt.isoformat()}, "
                f"interval={interval_unit}:{interval_value}"
            )
            if dbg:
                logger.warning(f"진단 정보: {dbg}")
            
            st.warning(
                "조회된 데이터가 없습니다. (아래 '진단 정보'를 확인하세요.)"
            )
            
            # 미래 날짜 체크
            now_utc = datetime.now(timezone.utc)
            if start_dt > now_utc:
                st.error(
                    f"⚠️ **시작 시간이 미래입니다!**\n\n"
                    f"- 요청한 시작 시간: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                    f"- 현재 시간: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
                    f"과거 날짜로 다시 시도해주세요."
                )
            elif end_dt > now_utc:
                st.warning(
                    f"⚠️ **종료 시간이 미래입니다.**\n\n"
                    f"- 요청한 종료 시간: {end_dt.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                    f"- 현재 시간: {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n"
                    f"현재 시간까지만 데이터가 존재합니다."
                )
            
            if dbg:
                with st.expander("진단 정보(마지막 API 호출)", expanded=True):
                    st.json(dbg)
                    
                    # 추가 힌트 제공
                    note = dbg.get("note", "")
                    if "빈 배열" in note or dbg.get("raw_count") == 0:
                        st.info(
                            "💡 **가능한 원인:**\n"
                            "1. 해당 거래 페어가 거래소에서 지원되지 않음\n"
                            "2. 요청한 기간에 거래 데이터가 없음 (상장 이전 기간 등)\n"
                            "3. 거래소 API의 데이터 제공 범위 제한\n"
                            "4. 미래 날짜를 요청함\n\n"
                            "**해결 방법:**\n"
                            "- 다른 거래 페어로 시도해보세요 (예: BTC/USDT, ETH/USDT)\n"
                            "- 더 최근 날짜로 시도해보세요\n"
                            "- 거래소 웹사이트에서 해당 페어가 존재하는지 확인해보세요"
                        )
            st.stop()

        df = fill_missing_ohlcv_gaps(
            df, start_dt, end_dt, interval_unit, int(interval_value)
        )

        interval_label = f"{interval_value}{interval_type}"
        kst = timezone(timedelta(hours=9))
        start_dt_kst = start_dt.astimezone(kst)
        end_dt_kst = end_dt.astimezone(kst)
        st.session_state["last_ohlcv"] = df
        st.session_state["last_meta"] = {
            "exchange_id": exchange_id,
            "exchange_name": api.name if api else exchange_id,
            "coin": coin_base,
            "quote": quote,
            "interval_label": interval_label,
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
            "start_kst": start_dt_kst.isoformat(),
            "end_kst": end_dt_kst.isoformat(),
        }

        logger.info(f"데이터 수집 완료: {len(df):,}건")
        st.success(f"총 {len(df):,}건 수집 완료.")

        api_dbg = EXCHANGE_APIS.get(exchange_id)
        dbg = getattr(api_dbg, "last_debug", None) if api_dbg else None
        if dbg:
            raw_min = dbg.get("raw_min_utc")
            raw_max = dbg.get("raw_max_utc")
            raw_cnt = dbg.get("raw_count")
            req_start = dbg.get("requested_start_utc")
            req_end = dbg.get("requested_end_utc")
            filtered_cnt = dbg.get("filtered_count", len(df))
            if raw_cnt and raw_min and raw_max:
                with st.expander("📊 데이터 수집 진단 정보", expanded=False):
                    st.caption(
                        f"API 원본 응답: {raw_cnt}건 → 필터링 후: {filtered_cnt}건"
                    )
                    if req_start and req_end:
                        st.text(
                            f"요청한 기간 (KST): "
                            f"{req_start.astimezone(kst).strftime('%Y-%m-%d %H:%M:%S')} ~ "
                            f"{req_end.astimezone(kst).strftime('%Y-%m-%d %H:%M:%S')}"
                        )

        kst = timezone(timedelta(hours=9))
        df_display = df.copy()
        if "datetime_utc" in df_display.columns:
            df_display["datetime_kst"] = df_display["datetime_utc"].apply(
                lambda x: x.astimezone(kst)
                if isinstance(x, datetime) and x.tzinfo
                else x
            )
            df_display["일시 (KST)"] = df_display["datetime_kst"].apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(x, datetime)
                else str(x)
            )
            display_cols = ["일시 (KST)"] + [
                c
                for c in df_display.columns
                if c
                not in ["datetime_utc", "datetime_kst", "일시 (KST)"]
            ]
            df_display = df_display[display_cols]

        st.dataframe(df_display.head(100), use_container_width=True)
        if len(df) > 100:
            st.caption(
                f"상위 100건만 표시. 전체 {len(df):,}건은 CSV 다운로드로 저장됩니다."
            )

        # 데이터 없던 구간 채움 안내 (직전 종가 + 거래량 0으로 채운 구간 수)
        if len(df) > 0 and "volume" in df.columns and "open" in df.columns:
            filled = (
                (df["volume"] == 0)
                & (df["open"] == df["close"])
                & (df["open"] == df["high"])
                & (df["open"] == df["low"])
            )
            filled_count = filled.sum()
            if filled_count > 0:
                st.info(
                    f"**{int(filled_count)}개** 구간에 데이터가 없어 직전 종가로 OHLC를 채우고 거래량을 0으로 넣었습니다."
                )

        # OHLCV 차트
        if len(df) > 0 and "datetime_utc" in df.columns:
            st.subheader("OHLCV 차트 (KST 기준)")
            df_chart = df.copy()
            df_chart["datetime_kst"] = df_chart["datetime_utc"].apply(
                lambda x: x.astimezone(kst)
                if isinstance(x, datetime) and x.tzinfo
                else x
            )
            required_cols = [
                "datetime_kst",
                "open",
                "high",
                "low",
                "close",
            ]
            has_volume = "volume" in df_chart.columns
            if all(col in df_chart.columns for col in required_cols):
                exchange_display_name = api.name if api else exchange_id
                if has_volume:
                    fig = make_subplots(
                        rows=2,
                        cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.1,
                        row_heights=[0.7, 0.3],
                        subplot_titles=(
                            f"{exchange_display_name} {coin_base}/{quote} - {interval_label}",
                            "거래량",
                        ),
                    )
                    fig.add_trace(
                        go.Candlestick(
                            x=df_chart["datetime_kst"],
                            open=df_chart["open"],
                            high=df_chart["high"],
                            low=df_chart["low"],
                            close=df_chart["close"],
                            name="OHLC",
                            increasing_line_color="red",
                            increasing_fillcolor="red",
                            decreasing_line_color="blue",
                            decreasing_fillcolor="blue",
                        ),
                        row=1,
                        col=1,
                    )
                    volume_colors = [
                        "red"
                        if df_chart.iloc[i]["close"]
                        >= df_chart.iloc[i]["open"]
                        else "blue"
                        for i in range(len(df_chart))
                    ]
                    fig.add_trace(
                        go.Bar(
                            x=df_chart["datetime_kst"],
                            y=df_chart["volume"],
                            name="거래량",
                            marker_color=volume_colors,
                            opacity=0.7,
                        ),
                        row=2,
                        col=1,
                    )
                    fig.update_layout(
                        height=700,
                        showlegend=True,
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1,
                        ),
                        xaxis_rangeslider_visible=False,
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                    )
                    fig.update_xaxes(
                        showgrid=True,
                        gridwidth=1,
                        gridcolor="lightgray",
                        row=1,
                        col=1,
                    )
                    fig.update_xaxes(
                        showgrid=True,
                        gridwidth=1,
                        gridcolor="lightgray",
                        title_text="일시 (KST)",
                        row=2,
                        col=1,
                    )
                    fig.update_yaxes(
                        showgrid=True,
                        gridwidth=1,
                        gridcolor="lightgray",
                        title_text=f"가격 ({quote})",
                        row=1,
                        col=1,
                    )
                    fig.update_yaxes(
                        showgrid=True,
                        gridwidth=1,
                        gridcolor="lightgray",
                        title_text="거래량",
                        row=2,
                        col=1,
                    )
                else:
                    fig = go.Figure(
                        data=go.Candlestick(
                            x=df_chart["datetime_kst"],
                            open=df_chart["open"],
                            high=df_chart["high"],
                            low=df_chart["low"],
                            close=df_chart["close"],
                            name="OHLC",
                            increasing_line_color="red",
                            increasing_fillcolor="red",
                            decreasing_line_color="blue",
                            decreasing_fillcolor="blue",
                        )
                    )
                    fig.update_layout(
                        title=f"{exchange_display_name} {coin_base}/{quote} - {interval_label}",
                        xaxis_title="일시 (KST)",
                        yaxis_title=f"가격 ({quote})",
                        xaxis_rangeslider_visible=False,
                        height=600,
                        showlegend=True,
                        plot_bgcolor="white",
                        paper_bgcolor="white",
                    )
                    fig.update_xaxes(
                        showgrid=True, gridwidth=1, gridcolor="lightgray"
                    )
                    fig.update_yaxes(
                        showgrid=True, gridwidth=1, gridcolor="lightgray"
                    )
                st.plotly_chart(fig, use_container_width=True)

    # CSV 다운로드
    if "last_ohlcv" in st.session_state and "last_meta" in st.session_state:
        st.subheader("CSV 저장")
        meta = st.session_state["last_meta"]
        df = st.session_state["last_ohlcv"]
        safe_name = meta["exchange_name"].replace(" ", "_")
        kst = timezone(timedelta(hours=9))
        if "start_kst" in meta and "end_kst" in meta:
            start_kst_str = (
                meta["start_kst"]
                .replace("+09:00", "")
                .replace("T", "_")
                .replace(":", "-")[:19]
            )
            end_kst_str = (
                meta["end_kst"]
                .replace("+09:00", "")
                .replace("T", "_")
                .replace(":", "-")[:19]
            )
        else:
            start_dt_utc = datetime.fromisoformat(
                meta["start"].replace("Z", "+00:00")
            )
            end_dt_utc = datetime.fromisoformat(
                meta["end"].replace("Z", "+00:00")
            )
            start_kst_str = start_dt_utc.astimezone(kst).strftime(
                "%Y-%m-%d_%H-%M-%S"
            )
            end_kst_str = end_dt_utc.astimezone(kst).strftime(
                "%Y-%m-%d_%H-%M-%S"
            )
        filename = f"{safe_name}_{meta['coin']}_{meta['quote']}_{meta['interval_label']}_{start_kst_str}_{end_kst_str}.csv"
        df_export = df.copy()
        if "datetime_utc" in df_export.columns:
            df_export["datetime_kst"] = df_export["datetime_utc"].apply(
                lambda x: x.astimezone(kst)
                if isinstance(x, datetime) and x.tzinfo
                else x
            )
            df_export["datetime_utc"] = df_export["datetime_utc"].astype(str)
            df_export["datetime_kst"] = df_export["datetime_kst"].apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(x, datetime)
                else str(x)
            )
            export_cols = ["datetime_kst", "datetime_utc"] + [
                c
                for c in df_export.columns
                if c not in ["datetime_kst", "datetime_utc"]
            ]
            df_export = df_export[export_cols]
        csv_bytes = df_export.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="CSV 파일 다운로드",
            data=csv_bytes,
            file_name=filename,
            mime="text/csv",
        )
        st.caption(f"파일명: {filename}")
        st.info(
            "다운로드한 CSV 파일을 SPPO 앱의 **차트 분석 → 타거래소와 데이터 비교** 메뉴에서 업로드해 사용하세요."
        )
