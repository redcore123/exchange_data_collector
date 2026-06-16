# -*- coding: utf-8 -*-
"""
타거래소 가격 비교 UI (독립 실행형)

- 사건 발생 거래소와 비교군 거래소(복수)의 혐의기간 내 OHLCV 비교
- 혐의기간 최고가 / 비교군 평균 / 평균 대비 백분율 표
- 가격 비교 차트 (거래소별 종가, 원화환산)
- 구간별 거래량 비교 피벗 표
- USDT 거래소: 업비트 KRW/USDT 혐의기간 최고가로 원화 환산
"""

from datetime import datetime, timezone, timedelta

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from exchange_apis import EXCHANGE_APIS, fetch_ohlcv, get_supported_exchanges
from logger_simple import get_logger

logger = get_logger(__name__)

KST = timezone(timedelta(hours=9))
INTERVAL_UNIT_MAP = {"일": "day", "시": "hour", "분": "minute", "초": "second"}


# ── 유틸리티 ─────────────────────────────────────────────────────────────────

def _parse_kst_range(start_date_str, start_time_str, end_date_str, end_time_str):
    """입력된 KST 날짜/시간 문자열을 KST-aware datetime 쌍으로 파싱."""
    start_str = f"{start_date_str.strip()} {start_time_str.strip()}"
    end_str = f"{end_date_str.strip()} {end_time_str.strip()}"
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"]:
        try:
            s = datetime.strptime(start_str, fmt).replace(tzinfo=KST)
            e = datetime.strptime(end_str, fmt).replace(tzinfo=KST)
            return s, e
        except ValueError:
            continue
    raise ValueError("날짜/시간 형식을 인식할 수 없습니다. 예: 2024-07-22 09:00:00")


def _fmt_price(v) -> str:
    """수 크기에 따라 소수점 자리를 조정하여 포맷."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "-"
    return f"{v:,.2f}" if abs(v) >= 1 else f"{v:,.8f}"


def _fmt_volume(v) -> str:
    """거래량 포맷 (정수에 가까우면 0자리, 소수면 4자리)."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "-"
    return f"{v:,.4f}"


def _to_kst_str(dt) -> str:
    """UTC-aware datetime을 KST 문자열로 변환 (표 표시용)."""
    if isinstance(dt, datetime) and dt.tzinfo:
        return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
    return str(dt)


# ── 차트 / 표 생성 ────────────────────────────────────────────────────────────

def _build_ohlcv_chart(entries: list, coin: str, krw_usdt_rate) -> go.Figure:
    """거래소별 OHLCV 캔들스틱 + 거래량을 세로 서브플롯으로 구성.
    데이터가 1개뿐인(기간이 짧은) 경우에도 캔들 1개로 정상 표시된다."""
    valid = [e for e in entries if e["df"] is not None and not e["df"].empty]
    if not valid:
        return go.Figure()

    n = len(valid)
    # 캔들(70%) + 거래량(30%) 비율로 각 거래소마다 2개 행
    row_heights = [h for _ in range(n) for h in (0.7, 0.3)]
    subplot_titles = [t for e in valid for t in (e["chart_label"], "거래량")]

    fig = make_subplots(
        rows=n * 2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=row_heights,
        subplot_titles=subplot_titles,
    )

    for i, entry in enumerate(valid):
        df_c = entry["df"].copy()
        df_c["datetime_kst"] = df_c["datetime_utc"].apply(
            lambda x: x.astimezone(KST) if isinstance(x, datetime) and x.tzinfo else x
        )
        factor = krw_usdt_rate if (entry["quote"] == "USDT" and krw_usdt_rate) else 1.0
        x_vals = df_c["datetime_kst"].tolist()
        candle_row = i * 2 + 1
        vol_row = i * 2 + 2

        fig.add_trace(
            go.Candlestick(
                x=x_vals,
                open=(df_c["open"] * factor).tolist(),
                high=(df_c["high"] * factor).tolist(),
                low=(df_c["low"] * factor).tolist(),
                close=(df_c["close"] * factor).tolist(),
                name=entry["chart_label"],
                increasing_line_color="red",
                increasing_fillcolor="red",
                decreasing_line_color="blue",
                decreasing_fillcolor="blue",
            ),
            row=candle_row, col=1,
        )

        if "volume" in df_c.columns:
            vol_colors = [
                "red" if df_c.iloc[j]["close"] >= df_c.iloc[j]["open"] else "blue"
                for j in range(len(df_c))
            ]
            fig.add_trace(
                go.Bar(
                    x=x_vals,
                    y=df_c["volume"].tolist(),
                    name=f"{entry['chart_label']} 거래량",
                    marker_color=vol_colors,
                    opacity=0.7,
                    showlegend=False,
                ),
                row=vol_row, col=1,
            )

        price_unit = "KRW" if (entry["quote"] == "KRW" or factor != 1.0) else entry["quote"]
        fig.update_yaxes(title_text=f"가격 ({price_unit})", row=candle_row, col=1,
                         showgrid=True, gridwidth=1, gridcolor="lightgray")
        fig.update_yaxes(title_text="거래량", row=vol_row, col=1,
                         showgrid=True, gridwidth=1, gridcolor="lightgray")

    usdt_note = f"  ※ USDT→KRW 환산: {krw_usdt_rate:,.2f}" if krw_usdt_rate else ""
    fig.update_layout(
        title=f"{coin} OHLCV 비교 — 원화환산 (KST){usdt_note}",
        height=max(500, n * 380),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.01, xanchor="right", x=1),
    )
    fig.update_xaxes(
        showgrid=True, gridwidth=1, gridcolor="lightgray",
        rangeslider_visible=False,
    )
    fig.update_xaxes(title_text="일시 (KST)", row=n * 2, col=1)
    return fig


def _build_volume_table(entries: list) -> pd.DataFrame:
    """각 거래소의 구간별 거래량을 시간 기준으로 피벗한 DataFrame 반환."""
    sub_dfs = []
    for entry in entries:
        df = entry["df"]
        if df is None or df.empty or "volume" not in df.columns:
            continue
        sub = df[["datetime_utc", "volume"]].copy()
        sub["datetime_kst"] = sub["datetime_utc"].apply(_to_kst_str)
        sub = (
            sub[["datetime_kst", "volume"]]
            .rename(columns={"volume": entry["chart_label"]})
            .set_index("datetime_kst")
        )
        sub_dfs.append(sub)

    if not sub_dfs:
        return pd.DataFrame()

    merged = sub_dfs[0]
    for d in sub_dfs[1:]:
        merged = merged.join(d, how="outer")
    return merged.sort_index().reset_index().rename(columns={"datetime_kst": "일시 (KST)"})


# ── 메인 페이지 ───────────────────────────────────────────────────────────────

def show_page():
    """타거래소 가격 비교 페이지를 표시합니다."""
    logger.info("=== 타거래소 가격 비교 페이지 시작 ===")

    st.title("타거래소 가격 비교")
    st.caption(
        "사건 발생 거래소와 비교군 거래소(복수 선택)의 혐의기간 내 OHLCV 데이터를 비교합니다. "
        "결제통화가 USDT인 거래소는 업비트 KRW/USDT 혐의기간 최고가로 원화 환산합니다. "
        "**인터넷이 되는 환경에서만 실행해 주세요.**"
    )

    exchanges = get_supported_exchanges()
    exchange_options = [f"{name} ({eid})" for eid, name in exchanges]
    exchange_id_by_label = {f"{name} ({eid})": eid for eid, name in exchanges}
    exchange_name_by_id = {eid: name for eid, name in exchanges}

    st.subheader("비교 조건 입력")

    # ── 거래소 / 코인 / 결제통화 ──────────────────────────────────────────────
    c1, c2, c3 = st.columns(3)
    with c1:
        incident_label = st.selectbox(
            "사건 발생 거래소", exchange_options, key="cmp_incident_exchange"
        )
        incident_exchange_id = exchange_id_by_label[incident_label]
        st.caption("사건 발생 거래소")
    with c2:
        coin = (
            st.text_input("코인명", value="BTC", help="예: BTC, ETH", key="cmp_coin")
            .strip()
            .upper()
            or "BTC"
        )
        st.caption("코인명")
    with c3:
        incident_quote = st.selectbox(
            "사건 발생 거래소 결제통화", ["KRW", "USDT"], key="cmp_incident_quote"
        )
        st.caption("사건 발생 거래소 결제통화")

    compare_labels = st.multiselect(
        "비교군 거래소 (복수 선택)", exchange_options, key="cmp_compare_exchanges"
    )
    compare_exchange_ids = [exchange_id_by_label[label] for label in compare_labels]

    compare_quotes: dict[str, str] = {}
    if compare_exchange_ids:
        st.caption("비교군 거래소별 결제통화")
        cols = st.columns(min(len(compare_exchange_ids), 4))
        for i, eid in enumerate(compare_exchange_ids):
            with cols[i % len(cols)]:
                compare_quotes[eid] = st.selectbox(
                    exchange_name_by_id[eid], ["KRW", "USDT"], key=f"cmp_quote_{eid}"
                )

    # ── 혐의기간 + 구간단위/구간값 ────────────────────────────────────────────
    st.markdown("**혐의기간 (KST) · 구간 설정**")
    c4, c5, c6, c7, c8, c9 = st.columns(6)
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
    with c8:
        interval_type = st.selectbox(
            "구간단위", ["일", "시", "분", "초"], index=2, key="cmp_interval_type"
        )
        st.caption("구간단위")
    with c9:
        max_iv = 60 if interval_type == "초" else 30
        interval_value = st.number_input(
            "구간값", min_value=1, max_value=max_iv, value=1, step=1, key="cmp_interval_value"
        )
        st.caption("구간값")

    interval_unit = INTERVAL_UNIT_MAP[interval_type]
    interval_label = f"{interval_value}{interval_type}"

    # 선택 거래소들의 interval 지원 여부 사전 안내
    all_selected_ids = list({incident_exchange_id} | set(compare_exchange_ids))
    unsupported = [
        exchange_name_by_id[eid]
        for eid in all_selected_ids
        if (api := EXCHANGE_APIS.get(eid)) and not api.get_interval_param(interval_unit, int(interval_value))
    ]
    if unsupported:
        st.warning(
            f"다음 거래소는 선택한 구간({interval_type} {interval_value})을 지원하지 않습니다: "
            f"{', '.join(unsupported)} — 해당 거래소는 데이터를 조회할 수 없습니다."
        )

    submitted = st.button("비교 실행", type="primary", use_container_width=True)
    if not submitted:
        return

    # ── 입력 검증 ─────────────────────────────────────────────────────────────
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

    # ── KRW/USDT 환산 기준가 조회 (USDT 거래소 포함 시) ──────────────────────
    krw_usdt_rate = None
    needs_rate = incident_quote == "USDT" or any(
        q == "USDT" for q in compare_quotes.values()
    )
    if needs_rate:
        with st.spinner("업비트 KRW/USDT 혐의기간 최고가 조회 중..."):
            try:
                df_rate = fetch_ohlcv(
                    "upbit", "USDT", "KRW", start_dt_utc, end_dt_utc, "minute", 1
                )
                if df_rate is not None and not df_rate.empty and "high" in df_rate.columns:
                    krw_usdt_rate = float(df_rate["high"].max())
            except Exception as e:
                logger.error(f"업비트 KRW/USDT 조회 오류: {e}", exc_info=True)
                st.error(f"업비트 KRW/USDT 최고가 조회 중 오류: {e}")
                st.stop()
        if krw_usdt_rate is None:
            st.error(
                "업비트 KRW/USDT 혐의기간 내 데이터를 조회할 수 없습니다. 혐의기간을 확인해주세요."
            )
            st.stop()

    # ── 거래소별 OHLCV 수집 ───────────────────────────────────────────────────
    all_entries_meta = [(incident_exchange_id, incident_quote, "사건발생")] + [
        (eid, compare_quotes[eid], "비교군") for eid in compare_exchange_ids
    ]

    # chart_label: 동일 eid가 두 번 나올 경우 역할(role)을 명시, 그 외에는 간결하게
    eid_count: dict[str, int] = {}
    for eid, _, _ in all_entries_meta:
        eid_count[eid] = eid_count.get(eid, 0) + 1

    entries: list[dict] = []
    for eid, quote, role in all_entries_meta:
        name = exchange_name_by_id[eid]
        label = (
            f"{name} ({quote}) [{role}]" if eid_count[eid] > 1 else f"{name} ({quote})"
        )
        entries.append({"eid": eid, "name": name, "quote": quote, "role": role,
                        "chart_label": label, "df": None, "error": None})

    for entry in entries:
        eid, quote, name = entry["eid"], entry["quote"], entry["name"]
        api = EXCHANGE_APIS.get(eid)
        if api and not api.get_interval_param(interval_unit, int(interval_value)):
            entry["error"] = f"구간 {interval_label} 미지원"
            st.warning(f"{name} ({quote}): 구간 {interval_label}을 지원하지 않아 건너뜁니다.")
            continue
        with st.spinner(f"{name} ({quote}) 데이터 수집 중..."):
            try:
                df = fetch_ohlcv(
                    eid, coin, quote, start_dt_utc, end_dt_utc,
                    interval_unit, int(interval_value)
                )
            except Exception as e:
                logger.error(f"{eid} 조회 오류: {e}", exc_info=True)
                entry["error"] = str(e)
                st.warning(f"{name} ({quote}) 수집 오류: {e}")
                continue
        if df is None or df.empty:
            entry["error"] = "데이터 없음"
            st.warning(f"{name} ({quote}): 혐의기간 내 데이터가 없습니다.")
        else:
            entry["df"] = df

    valid_entries = [e for e in entries if e["df"] is not None]
    if not valid_entries:
        st.error("모든 거래소에서 데이터를 가져오지 못했습니다.")
        st.stop()

    # ══════════════════════════════════════════════════════════════════════════
    # 1. 수집 데이터 (거래소별 OHLCV 원본 표)
    # ══════════════════════════════════════════════════════════════════════════
    st.subheader("수집 데이터")
    for entry in valid_entries:
        df = entry["df"]
        df_disp = df.copy()
        if "datetime_utc" in df_disp.columns:
            df_disp.insert(0, "일시 (KST)", df_disp["datetime_utc"].apply(_to_kst_str))
            df_disp = df_disp.drop(columns=["datetime_utc"])
        max_high = float(df["high"].max()) if "high" in df.columns else None
        header = (
            f"{entry['chart_label']} — {len(df):,}건"
            + (f"  |  최고가: {_fmt_price(max_high)} {entry['quote']}" if max_high else "")
        )
        with st.expander(header, expanded=False):
            st.dataframe(df_disp.head(1000), use_container_width=True, hide_index=True)
            if len(df) > 1000:
                st.caption(f"상위 1,000건만 표시. 전체 {len(df):,}건.")

    # ══════════════════════════════════════════════════════════════════════════
    # 2. 가격 비교 차트 (종가 원화환산, 거래소 오버레이)
    # ══════════════════════════════════════════════════════════════════════════
    st.subheader(f"{coin} OHLCV 비교 차트 ({interval_label}, 원화환산, KST 기준)")
    ohlcv_chart = _build_ohlcv_chart(valid_entries, coin, krw_usdt_rate)
    st.plotly_chart(ohlcv_chart, use_container_width=True)

    # ══════════════════════════════════════════════════════════════════════════
    # 3. 혐의기간 내 최고가 비교 표
    # ══════════════════════════════════════════════════════════════════════════
    st.subheader("혐의기간 내 최고가 비교")

    price_rows = []
    for entry in entries:
        df = entry["df"]
        high_raw = float(df["high"].max()) if (df is not None and not df.empty and "high" in df.columns) else None
        krw_high = (high_raw * krw_usdt_rate if entry["quote"] == "USDT" and krw_usdt_rate and high_raw is not None
                    else high_raw)
        price_rows.append({
            "구분": entry["role"],
            "거래소": entry["name"],
            "결제통화": entry["quote"],
            "_high_raw": high_raw,
            "_krw_high": krw_high,
        })

    price_df = pd.DataFrame(price_rows)
    compare_valid = price_df[(price_df["구분"] == "비교군") & price_df["_krw_high"].notna()]
    incident_valid = price_df[(price_df["구분"] == "사건발생") & price_df["_krw_high"].notna()]
    avg_compare_krw = compare_valid["_krw_high"].mean() if not compare_valid.empty else None
    pct = (
        incident_valid.iloc[0]["_krw_high"] / avg_compare_krw * 100
        if avg_compare_krw and avg_compare_krw > 0 and not incident_valid.empty
        else None
    )

    display_price_rows = []
    for _, r in price_df.iterrows():
        is_incident = r["구분"] == "사건발생"
        display_price_rows.append({
            "구분": r["구분"],
            "거래소": r["거래소"],
            "결제통화": r["결제통화"],
            "최고가": (f"{_fmt_price(r['_high_raw'])} {r['결제통화']}"
                      if r["_high_raw"] is not None else "-"),
            "원화환산 최고가 (KRW)": _fmt_price(r["_krw_high"]) if r["_krw_high"] is not None else "-",
            "비교군 평균 대비(%)": f"{pct:,.2f}%" if is_incident and pct is not None else "-",
        })

    if avg_compare_krw is not None:
        display_price_rows.append({
            "구분": "비교군 평균",
            "거래소": "-",
            "결제통화": "-",
            "최고가": "-",
            "원화환산 최고가 (KRW)": _fmt_price(avg_compare_krw),
            "비교군 평균 대비(%)": "-",
        })

    st.dataframe(pd.DataFrame(display_price_rows), use_container_width=True, hide_index=True)

    if krw_usdt_rate is not None:
        st.caption(
            f"※ USDT 결제통화 거래소의 원화환산은 혐의기간 내 업비트 KRW/USDT 최고가 "
            f"**{krw_usdt_rate:,.2f} KRW**를 기준으로 계산되었습니다."
        )

    # ══════════════════════════════════════════════════════════════════════════
    # 4. 거래량 비교 표 (구간별 피벗)
    # ══════════════════════════════════════════════════════════════════════════
    st.subheader(f"구간별 거래량 비교 ({interval_label}, 단위: {coin})")
    vol_df = _build_volume_table(valid_entries)
    if not vol_df.empty:
        incident_entry = next((e for e in valid_entries if e["role"] == "사건발생"), None)
        incident_col = incident_entry["chart_label"] if incident_entry else None
        compare_valid_entries = [e for e in valid_entries if e["role"] == "비교군"]

        # 컬럼명 매핑: chart_label → 표시용 컬럼명
        vol_col_map: dict[str, str] = {}
        if incident_col and incident_col in vol_df.columns:
            vol_col_map[incident_col] = f"{incident_col} (개)"
        for entry in compare_valid_entries:
            col = entry["chart_label"]
            if col in vol_df.columns:
                vol_col_map[col] = f"{col} (개)"
        all_disp_cols = ["일시 (KST)"] + list(vol_col_map.values())

        disp_rows = []
        for _, row in vol_df.iterrows():
            # ── 거래량 행 ────────────────────────────────────────────────────
            vol_row: dict = {"일시 (KST)": row["일시 (KST)"]}
            incident_v_raw = None
            if incident_col and incident_col in vol_col_map:
                v = row[incident_col]
                vol_row[vol_col_map[incident_col]] = (
                    f"{_fmt_volume(v)} 개" if pd.notna(v) else "-"
                )
                if pd.notna(v) and v != 0:
                    incident_v_raw = v
            for entry in compare_valid_entries:
                col = entry["chart_label"]
                if col not in vol_col_map:
                    continue
                v = row[col]
                vol_row[vol_col_map[col]] = f"{_fmt_volume(v)} 개" if pd.notna(v) else "-"
            disp_rows.append(vol_row)

            # ── 사건발생 대비(%) 행 ──────────────────────────────────────────
            if incident_v_raw is not None and compare_valid_entries:
                ratio_row: dict = {"일시 (KST)": "비교군 대비 사건발생 거래소 거래량 비율(%)"}
                if incident_col and incident_col in vol_col_map:
                    ratio_row[vol_col_map[incident_col]] = "-"
                for entry in compare_valid_entries:
                    col = entry["chart_label"]
                    if col not in vol_col_map:
                        continue
                    v = row[col]
                    ratio_row[vol_col_map[col]] = (
                        f"{incident_v_raw / v * 100:,.2f}%"
                        if pd.notna(v) and v != 0
                        else "-"
                    )
                disp_rows.append(ratio_row)

        vol_display = pd.DataFrame(disp_rows, columns=all_disp_cols)
        st.dataframe(vol_display, use_container_width=True, hide_index=True)
        if len(vol_df) > 1000:
            st.caption(f"전체 {len(vol_df):,}행.")
    else:
        st.info("거래량 데이터가 없습니다.")

    st.caption(
        f"혐의기간 (KST): {start_dt_kst.strftime('%Y-%m-%d %H:%M:%S')} ~ "
        f"{end_dt_kst.strftime('%Y-%m-%d %H:%M:%S')}  |  구간: {interval_label}"
    )
