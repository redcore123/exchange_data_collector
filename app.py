# -*- coding: utf-8 -*-
"""
타거래소 데이터 수집 - 독립 실행형 Streamlit 앱

인터넷이 되는 PC에서만 실행하세요.
수집한 CSV는 USB 등으로 오프라인 SPPO 환경으로 가져간 뒤,
SPPO 앱의 [차트 분석 → 타거래소와 데이터 비교]에서 업로드해 사용합니다.

실행 방법 (이 폴더에서):
  pip install -r requirements.txt
  streamlit run app.py --server.port 8506

또는 프로젝트 루트에서:
  pip install -r standalone_exchange_collector/requirements.txt
  streamlit run standalone_exchange_collector/app.py --server.port 8506
"""

import streamlit as st

from collector_ui import show_page

st.set_page_config(
    page_title="타거래소 데이터 수집",
    page_icon="📊",
    layout="wide",
)

show_page()
