# -*- coding: utf-8 -*-
"""
PyInstaller exe 빌드용 런처.
exe 실행 시 streamlit run app.py 를 동일 프로세스에서 실행합니다.
"""
import sys
import os

# exe로 패키징된 경우 작업 디렉토리를 exe 위치로 (스크립트/데이터가 여기 있음)
if getattr(sys, "frozen", False):
    app_dir = os.path.dirname(sys.executable)
else:
    app_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(app_dir)

# streamlit run app.py --server.port 8506 --server.headless true
sys.argv = [
    "streamlit",
    "run",
    "app.py",
    "--server.port=8506",
    "--server.headless=true",
]

if __name__ == "__main__":
    import streamlit.web.cli as stcli
    stcli.main()
