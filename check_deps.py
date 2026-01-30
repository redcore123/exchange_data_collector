# -*- coding: utf-8 -*-
"""
의존성 확인 스크립트.
run.bat에서 Streamlit 실행 전에 이 스크립트로 필요한 패키지가 모두 로드되는지 확인합니다.
성공 시 0, 실패 시 1을 반환합니다.
"""
import sys

def main():
    try:
        import streamlit
        import pandas
        import plotly
        import requests
        return 0
    except ImportError as e:
        print(f"필수 패키지가 없습니다: {e}", file=sys.stderr)
        print("이 폴더에서 다음을 실행한 뒤 다시 시도하세요:", file=sys.stderr)
        print("  python -m pip install -r requirements.txt", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(main())
