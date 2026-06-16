# -*- coding: utf-8 -*-
"""
시세 데이터 수집 - 독립 실행형 Streamlit 앱 (Streamlit Cloud 배포용)

인터넷이 되는 환경에서만 실행하세요.
- 시세 데이터 수집(Upbit 일봉): CSV 다운로드 → SPPO [시세 데이터 관리]에서 업로드
- 타거래소 데이터 수집: CSV 다운로드 → SPPO [타거래소와 데이터 비교]에서 업로드

실행 방법 (이 폴더에서):
  pip install -r requirements.txt
  streamlit run app.py --server.port 8506

Streamlit Cloud 배포:
  메인 스크립트: standalone_exchange_collector/app.py
"""

import streamlit as st
from streamlit_option_menu import option_menu

from collector_ui import show_page as show_exchange_collector
from price_data_collector_ui import show_page as show_price_collector
from compare_exchange_ui import show_page as show_compare_exchange

st.set_page_config(
    page_title="가상자산 시세 데이터 수집",
    page_icon="📊",
    layout="wide",
)

with st.sidebar:
    page = option_menu(
        menu_title="가상자산 시세 데이터 수집",
        options=[
            "타거래소 데이터 수집",
            "원화가치 환산용 시세 데이터 수집",
            "타거래소 가격 비교",
        ],
        icons=["globe", "graph-up", "bar-chart"],
        default_index=0,
        menu_icon="collection",
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        """
**개발 정보**
- **개발기관**: 서울남부지방검찰청 가상자산합동수사부
- **개발부서**: 412호 박지향 검사방
- **개발기간**: 2026.03 ~ 2026.06
- **개발자**: 코스콤 김남경 팀장

**라이선스**
- **유형**: 내부용
- **사용권한**: 서울남부지방검찰청 내부 직원
- **배포제한**: 무단 복제 및 배포 금지
"""
    )

if page == "원화가치 환산용 시세 데이터 수집":
    show_price_collector()
elif page == "타거래소 가격 비교":
    show_compare_exchange()
else:
    show_exchange_collector()
