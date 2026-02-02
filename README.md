# 시세 데이터 수집 앱 (Streamlit Cloud 배포용)

인터넷이 되는 환경에서만 사용하는 독립 실행형 Streamlit 앱입니다.  
SPPO 본 프로그램은 오프라인 환경이므로, 시세·타거래소 데이터는 이 앱에서 수집한 뒤 CSV로 다운로드하여 SPPO에서 업로드해 사용합니다.

## 메뉴

- **시세 데이터 수집 (Upbit 일봉)**  
  Upbit API로 코인 일봉을 수집해 CSV 다운로드 → SPPO [시세 데이터 관리]에서 업로드

- **타거래소 데이터 수집**  
  여러 거래소 공개 API로 OHLCV 수집 후 CSV 다운로드 → SPPO [타거래소와 데이터 비교]에서 업로드

## 로컬 실행

```bash
# 이 폴더에서
pip install -r requirements.txt
streamlit run app.py --server.port 8506
```

또는 프로젝트 루트에서:

```bash
pip install -r standalone_exchange_collector/requirements.txt
streamlit run standalone_exchange_collector/app.py --server.port 8506
```

## Streamlit Cloud 배포

1. 이 저장소를 GitHub에 푸시한 뒤 [Streamlit Cloud](https://share.streamlit.io/)에서 앱 생성
2. **Main file path**: `standalone_exchange_collector/app.py`
3. **Working directory**: 비워 두거나 프로젝트 루트
4. **Requirements**: 프로젝트 루트의 `requirements.txt` 사용 시 해당 파일에 `standalone_exchange_collector/requirements.txt` 내용이 포함되어 있어야 하며, 또는 Advanced settings에서 Requirements file을 `standalone_exchange_collector/requirements.txt`로 지정

필요 패키지: `streamlit`, `pandas`, `plotly`, `requests`
