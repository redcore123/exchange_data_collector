# -*- coding: utf-8 -*-
"""
타거래소(해외·국내) 공개 API를 이용한 캔들/OHLCV 데이터 수집 모듈.
계정 없이 공개 API만으로 차트 데이터를 조회합니다.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pandas as pd
import requests

# 공통 컬럼명 (정규화된 OHLCV)
OHLCV_COLUMNS = ["datetime_utc", "open", "high", "low", "close", "volume"]


def _parse_ts_ms(ts: Any) -> Optional[datetime]:
    """밀리초 또는 초 단위 타임스탬프를 UTC datetime으로 변환."""
    if ts is None:
        return None
    try:
        t = int(float(ts))
        if t > 1e12:  # ms
            return datetime.fromtimestamp(t / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(t, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


class BaseExchangeAPI(ABC):
    """거래소 API 공통 인터페이스."""

    name: str = ""
    base_url: str = ""

    @abstractmethod
    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[Any]:
        """interval_unit: 'day'|'hour'|'minute'|'second', value: 숫자. API용 interval 값 반환."""
        pass

    @abstractmethod
    def get_symbol(self, base: str, quote: str) -> str:
        """거래소별 심볼 문자열 반환 (예: BTCUSDT, BTC-USDT)."""
        pass

    @abstractmethod
    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        """캔들 데이터 조회 후 정규화된 DataFrame 반환 (datetime_utc, open, high, low, close, volume)."""
        pass

    def _to_dataframe(self, rows: list, columns: list) -> pd.DataFrame:
        """공통 컬럼명으로 DataFrame 생성."""
        df = pd.DataFrame(rows, columns=columns)
        if "datetime_utc" not in df.columns and len(df) > 0:
            return df
        df = df[df.columns.intersection(OHLCV_COLUMNS)]
        return df.reindex(columns=[c for c in OHLCV_COLUMNS if c in df.columns])

    def _set_last_debug(self, **kwargs: Any) -> None:
        """마지막 호출의 진단 정보 저장(스트림릿 UI 표시용)."""
        try:
            current = getattr(self, "last_debug", {}) or {}
            current.update(kwargs)
            setattr(self, "last_debug", current)
        except Exception:
            # 진단 정보는 부가 기능이므로 실패해도 무시
            pass


class BinanceAPI(BaseExchangeAPI):
    """Binance 공개 API. 데이터 API: data-api.binance.vision."""

    name = "Binance"
    base_url = "https://data-api.binance.vision/api/v3"

    INTERVAL_MAP = {
        ("second", 1): "1s",
        ("second", 3): "3s",
        ("second", 5): "5s",
        ("second", 15): "15s",
        ("second", 30): "30s",
        ("minute", 1): "1m",
        ("minute", 3): "3m",
        ("minute", 5): "5m",
        ("minute", 15): "15m",
        ("minute", 30): "30m",
        ("hour", 1): "1h",
        ("hour", 2): "2h",
        ("hour", 4): "4h",
        ("hour", 6): "6h",
        ("hour", 8): "8h",
        ("hour", 12): "12h",
        ("day", 1): "1d",
        ("day", 3): "3d",
        ("day", 7): "1w",
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[str]:
        return self.INTERVAL_MAP.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        return f"{base.upper()}{quote.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        interval_str = self.get_interval_param(interval_unit, interval_value)
        if not interval_str:
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        symbol = self.get_symbol(base, quote)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        url = f"{self.base_url}/klines"
        all_rows = []
        while start_ms < end_ms:
            params = {
                "symbol": symbol,
                "interval": interval_str,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": 1000,
            }
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            for row in data:
                dt = _parse_ts_ms(row[0])
                all_rows.append(
                    {
                        "datetime_utc": dt,
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                    }
                )
            start_ms = int(data[-1][0]) + 1
        return pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)


class KrakenAPI(BaseExchangeAPI):
    """Kraken 공개 API. OHLC는 분 단위 interval만 지원, 최대 720개."""

    name = "Kraken"
    base_url = "https://api.kraken.com/0/public"

    # Kraken: interval in minutes: 1, 5, 15, 30, 60, 240, 1440, 10080, 21600
    INTERVAL_MINUTES = {
        ("minute", 1): 1,
        ("minute", 5): 5,
        ("minute", 15): 15,
        ("minute", 30): 30,
        ("hour", 1): 60,
        ("hour", 4): 240,
        ("day", 1): 1440,
        ("day", 7): 10080,
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[int]:
        return self.INTERVAL_MINUTES.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        # Kraken: XBTUSD, ETHUSD 등 (BTC -> XBT)
        b = "XBT" if base.upper() == "BTC" else base.upper()
        return f"{b}{quote.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        interval_min = self.get_interval_param(interval_unit, interval_value)
        if interval_min is None:
            self._set_last_debug(
                exchange=self.name,
                error=f"지원하지 않는 구간: {interval_unit} {interval_value}",
                requested_start_utc=start_dt,
                requested_end_utc=end_dt,
            )
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        pair = self.get_symbol(base, quote)
        since = int(start_dt.timestamp())
        url = f"{self.base_url}/OHLC"
        params = {"pair": pair, "interval": interval_min, "since": since}
        
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            j = r.json()
            
            # Kraken API 오류 처리
            error_list = j.get("error")
            if error_list:
                error_msg = ", ".join(error_list) if isinstance(error_list, list) else str(error_list)
                self._set_last_debug(
                    exchange=self.name,
                    url=url,
                    params=params,
                    http_status=r.status_code,
                    api_status="error",
                    error=error_msg,
                    requested_start_utc=start_dt,
                    requested_end_utc=end_dt,
                )
                raise ValueError(
                    f"Kraken API 오류: {error_msg}. "
                    f"거래 페어({pair})가 올바른지 확인해주세요. "
                    f"Kraken은 BTC를 XBT로 표기하며, 페어 형식이 다를 수 있습니다."
                )
            
            result = j.get("result", {})
            # pair 이름이 키로 올 수 있음 (예: XXBTZUSD)
            ohlc_key = None
            for k in result:
                if k != "last":
                    ohlc_key = k
                    break
            
            if not ohlc_key:
                self._set_last_debug(
                    exchange=self.name,
                    url=url,
                    params=params,
                    http_status=r.status_code,
                    api_status="no_data",
                    raw_count=0,
                    requested_start_utc=start_dt,
                    requested_end_utc=end_dt,
                    note="Kraken API 응답에 OHLC 데이터 키를 찾을 수 없음",
                )
                return pd.DataFrame(columns=OHLCV_COLUMNS)
            
            rows = result[ohlc_key]
            end_ts = int(end_dt.timestamp())
            out = []
            raw_min_utc = None
            raw_max_utc = None
            
            for row in rows:
                ts = int(row[0])
                if ts > end_ts:
                    break
                dt = _parse_ts_ms(ts * 1000)
                if dt:
                    if raw_min_utc is None or dt < raw_min_utc:
                        raw_min_utc = dt
                    if raw_max_utc is None or dt > raw_max_utc:
                        raw_max_utc = dt
                    out.append(
                        {
                            "datetime_utc": dt,
                            "open": float(row[1]),
                            "high": float(row[2]),
                            "low": float(row[3]),
                            "close": float(row[4]),
                            "volume": float(row[6]),
                        }
                    )
            
            df = pd.DataFrame(out, columns=OHLCV_COLUMNS)
            self._set_last_debug(
                exchange=self.name,
                url=url,
                params=params,
                http_status=r.status_code,
                api_status="success",
                raw_count=len(rows),
                raw_min_utc=raw_min_utc,
                raw_max_utc=raw_max_utc,
                requested_start_utc=start_dt,
                requested_end_utc=end_dt,
                filtered_count=len(df),
            )
            return df
            
        except ValueError:
            raise
        except Exception as e:
            self._set_last_debug(
                exchange=self.name,
                url=url,
                params=params,
                http_status=getattr(r, "status_code", None) if 'r' in locals() else None,
                error=str(e),
                requested_start_utc=start_dt,
                requested_end_utc=end_dt,
            )
            raise


class BybitAPI(BaseExchangeAPI):
    """Bybit V5 공개 API. spot 기준."""

    name = "Bybit"
    base_url = "https://api.bybit.com/v5/market"

    INTERVAL_MAP = {
        ("minute", 1): "1",
        ("minute", 3): "3",
        ("minute", 5): "5",
        ("minute", 15): "15",
        ("minute", 30): "30",
        ("hour", 1): "60",
        ("hour", 2): "120",
        ("hour", 4): "240",
        ("hour", 6): "360",
        ("hour", 12): "720",
        ("day", 1): "D",
        ("day", 7): "W",
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[str]:
        return self.INTERVAL_MAP.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        return f"{base.upper()}{quote.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        interval_str = self.get_interval_param(interval_unit, interval_value)
        if not interval_str:
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        symbol = self.get_symbol(base, quote)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        url = f"{self.base_url}/kline"
        all_rows = []
        while start_ms < end_ms:
            params = {
                "category": "spot",
                "symbol": symbol,
                "interval": interval_str,
                "start": start_ms,
                "end": end_ms,
                "limit": 1000,
            }
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data.get("retCode") != 0:
                break
            lst = data.get("result", {}).get("list", [])
            if not lst:
                break
            for row in lst:
                # Bybit: [start, open, high, low, close, volume, turn over]
                ts_ms = int(row[0])
                dt = _parse_ts_ms(ts_ms)
                all_rows.append(
                    {
                        "datetime_utc": dt,
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                    }
                )
            start_ms = int(lst[0][0]) + 1
        return pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)


class OKXAPI(BaseExchangeAPI):
    """OKX v5 공개 API. spot 캔들."""

    name = "OKX"
    base_url = "https://www.okx.com/api/v5/market"

    INTERVAL_MAP = {
        ("minute", 1): "1m",
        ("minute", 3): "3m",
        ("minute", 5): "5m",
        ("minute", 15): "15m",
        ("minute", 30): "30m",
        ("hour", 1): "1H",
        ("hour", 2): "2H",
        ("hour", 4): "4H",
        ("hour", 6): "6H",
        ("hour", 12): "12H",
        ("day", 1): "1D",
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[str]:
        return self.INTERVAL_MAP.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        return f"{base.upper()}-{quote.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        interval_str = self.get_interval_param(interval_unit, interval_value)
        if not interval_str:
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        inst_id = self.get_symbol(base, quote)
        url = f"{self.base_url}/history-candles"
        all_rows = []
        after = int(end_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        start_ms = int(start_dt.timestamp() * 1000)
        while after > start_ms:
            params = {
                "instId": inst_id,
                "bar": interval_str,
                "after": after,
                "limit": 300,
            }
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data.get("code") != "0":
                break
            lst = data.get("data", [])
            if not lst:
                break
            for row in lst:
                # OKX: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
                ts_ms = int(row[0])
                if ts_ms < start_ms:
                    continue
                dt = _parse_ts_ms(ts_ms)
                all_rows.append(
                    {
                        "datetime_utc": dt,
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                    }
                )
            after = int(lst[-1][0]) - 1
        df = pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)
        return df.sort_values("datetime_utc").reset_index(drop=True) if not df.empty else df


class CoinbaseAPI(BaseExchangeAPI):
    """Coinbase Exchange (Pro) 공개 API. granularity는 초 단위."""

    name = "Coinbase"
    base_url = "https://api.exchange.coinbase.com"

    # granularity: 60, 300, 900, 3600, 21600, 86400
    GRANULARITY_SEC = {
        ("minute", 1): 60,
        ("minute", 5): 300,
        ("minute", 15): 900,
        ("minute", 30): 1800,
        ("hour", 1): 3600,
        ("hour", 6): 21600,
        ("day", 1): 86400,
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[int]:
        return self.GRANULARITY_SEC.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        return f"{base.upper()}-{quote.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        granularity = self.get_interval_param(interval_unit, interval_value)
        if granularity is None:
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        product_id = self.get_symbol(base, quote)
        start_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
        end_iso = end_dt.strftime("%Y-%m-%dT%H:%M:%S")
        url = f"{self.base_url}/products/{product_id}/candles"
        params = {
            "start": start_iso,
            "end": end_iso,
            "granularity": granularity,
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data or (isinstance(data, dict) and data.get("message")):
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        out = []
        for row in data:
            # [ time, low, high, open, close, volume ]
            ts = int(row[0])
            dt = _parse_ts_ms(ts)
            out.append(
                {
                    "datetime_utc": dt,
                    "open": float(row[3]),
                    "high": float(row[2]),
                    "low": float(row[1]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                }
            )
        df = pd.DataFrame(out, columns=OHLCV_COLUMNS)
        return df.sort_values("datetime_utc").reset_index(drop=True) if not df.empty else df


class KuCoinAPI(BaseExchangeAPI):
    """KuCoin 공개 API. type: 1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week."""

    name = "KuCoin"
    base_url = "https://api.kucoin.com/api/v1/market"

    INTERVAL_MAP = {
        ("minute", 1): "1min",
        ("minute", 3): "3min",
        ("minute", 5): "5min",
        ("minute", 15): "15min",
        ("minute", 30): "30min",
        ("hour", 1): "1hour",
        ("hour", 2): "2hour",
        ("hour", 4): "4hour",
        ("hour", 6): "6hour",
        ("hour", 8): "8hour",
        ("hour", 12): "12hour",
        ("day", 1): "1day",
        ("day", 7): "1week",
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[str]:
        return self.INTERVAL_MAP.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        return f"{base.upper()}-{quote.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        type_str = self.get_interval_param(interval_unit, interval_value)
        if not type_str:
            self._set_last_debug(
                exchange=self.name,
                error=f"지원하지 않는 구간: {interval_unit} {interval_value}",
                requested_start_utc=start_dt,
                requested_end_utc=end_dt,
            )
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        symbol = self.get_symbol(base, quote)
        start_at = int(start_dt.timestamp() * 1000)
        end_at = int(end_dt.timestamp() * 1000)
        url = f"{self.base_url}/candles"
        all_rows = []
        raw_min_utc = None
        raw_max_utc = None
        last_http_status = None
        last_error = None
        
        try:
            while start_at < end_at:
                params = {
                    "symbol": symbol,
                    "type": type_str,
                    "startAt": start_at,
                    "endAt": end_at,
                }
                r = requests.get(url, params=params, timeout=30)
                last_http_status = r.status_code
                r.raise_for_status()
                data = r.json()
                
                # KuCoin API 오류 처리
                code = data.get("code")
                msg = data.get("msg", "")
                error_field = data.get("error")  # 일부 응답에 error 필드가 있을 수 있음
                
                # code가 200000이 아니거나, error 필드가 있거나, msg에 오류 메시지가 있는 경우
                if code != "200000" or error_field or (msg and "error" in msg.lower()):
                    error_msg = error_field or msg or f"KuCoin API 오류 코드: {code}"
                    last_error = error_msg
                    self._set_last_debug(
                        exchange=self.name,
                        url=url,
                        params=params,
                        http_status=last_http_status,
                        api_status=f"error_code_{code}" if code != "200000" else "error_in_response",
                        error=error_msg,
                        requested_start_utc=start_dt,
                        requested_end_utc=end_dt,
                    )
                    raise ValueError(
                        f"KuCoin API 오류: {error_msg}. "
                        f"거래 페어({symbol})가 올바른지 확인해주세요. "
                        f"오류 코드: {code if code != '200000' else 'N/A'}"
                    )
                
                lst = data.get("data", [])
                # data가 비어있거나 리스트가 아닌 경우도 확인
                if not lst or not isinstance(lst, list):
                    # 첫 번째 요청에서 데이터가 없으면 오류로 처리
                    if len(all_rows) == 0:
                        error_msg = f"KuCoin API에서 데이터를 반환하지 않았습니다. 거래 페어({symbol})가 올바른지 확인해주세요."
                        self._set_last_debug(
                            exchange=self.name,
                            url=url,
                            params=params,
                            http_status=last_http_status,
                            api_status="no_data",
                            error=error_msg,
                            requested_start_utc=start_dt,
                            requested_end_utc=end_dt,
                        )
                        raise ValueError(error_msg)
                    break
                
                # KuCoin API는 가장 오래된 데이터부터 반환합니다
                # 다음 페이지를 가져오려면 가장 최신 데이터의 타임스탬프를 사용해야 합니다
                max_ts_ms = 0
                for row in lst:
                    # KuCoin: [time, open, high, low, close, volume, quoteVolume]
                    # time은 초 단위 Unix 타임스탬프
                    ts_val = row[0]
                    ts_sec = int(float(ts_val))
                    ts_ms = ts_sec * 1000 if ts_sec < 1e10 else ts_sec
                    dt = _parse_ts_ms(ts_ms)
                    if dt:
                        if raw_min_utc is None or dt < raw_min_utc:
                            raw_min_utc = dt
                        if raw_max_utc is None or dt > raw_max_utc:
                            raw_max_utc = dt
                        all_rows.append(
                            {
                                "datetime_utc": dt,
                                "open": float(row[1]),
                                "high": float(row[2]),
                                "low": float(row[3]),
                                "close": float(row[4]),
                                "volume": float(row[5]),
                            }
                        )
                        # 가장 최신 타임스탬프 추적
                        if ts_ms > max_ts_ms:
                            max_ts_ms = ts_ms
                
                # 다음 페이지: 가장 최신 데이터의 타임스탬프 + 1밀리초
                if max_ts_ms > 0:
                    next_start_at = max_ts_ms + 1
                    # 무한 루프 방지: start_at이 증가하지 않으면 break
                    if next_start_at <= start_at:
                        break
                    start_at = next_start_at
                else:
                    # 데이터가 없거나 타임스탬프를 파싱할 수 없으면 종료
                    break
            
            df = pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)
            
            # 데이터가 없고 오류가 있었던 경우
            if len(df) == 0 and last_error:
                self._set_last_debug(
                    exchange=self.name,
                    url=url,
                    params={"symbol": symbol, "type": type_str, "startAt": start_at, "endAt": end_at},
                    http_status=last_http_status,
                    api_status="error",
                    error=last_error,
                    requested_start_utc=start_dt,
                    requested_end_utc=end_dt,
                    raw_count=0,
                    filtered_count=0,
                )
                raise ValueError(
                    f"KuCoin API 오류: {last_error}. "
                    f"거래 페어({symbol})가 올바른지 확인해주세요."
                )
            
            self._set_last_debug(
                exchange=self.name,
                url=url,
                params={"symbol": symbol, "type": type_str, "startAt": start_at, "endAt": end_at},
                http_status=last_http_status,
                api_status="success" if len(df) > 0 else "no_data",
                raw_count=len(all_rows),
                raw_min_utc=raw_min_utc,
                raw_max_utc=raw_max_utc,
                requested_start_utc=start_dt,
                requested_end_utc=end_dt,
                filtered_count=len(df),
            )
            return df
            
        except ValueError:
            raise
        except Exception as e:
            self._set_last_debug(
                exchange=self.name,
                url=url,
                params={"symbol": symbol, "type": type_str, "startAt": start_at, "endAt": end_at},
                http_status=last_http_status,
                error=str(e),
                requested_start_utc=start_dt,
                requested_end_utc=end_dt,
            )
            raise


# --- 국내 거래소 ---


class UpbitAPI(BaseExchangeAPI):
    """업비트 공개 API. 분봉(1,3,5,15,30,60), 일봉 지원. 시장: KRW-BTC."""

    name = "Upbit"
    base_url = "https://api.upbit.com/v1"

    # 분봉 unit: 1,3,5,15,30,60 / 일봉은 별도 엔드포인트
    INTERVAL_MINUTES = {
        ("minute", 1): 1,
        ("minute", 3): 3,
        ("minute", 5): 5,
        ("minute", 15): 15,
        ("minute", 30): 30,
        ("hour", 1): 60,
        ("day", 1): None,  # days 엔드포인트
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[Any]:
        if (interval_unit, interval_value) == ("day", 1):
            return "days"
        return self.INTERVAL_MINUTES.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        return f"{quote.upper()}-{base.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        market = self.get_symbol(base, quote)
        all_rows = []
        to_dt = end_dt
        count = 200
        if (interval_unit, interval_value) == ("day", 1):
            url = f"{self.base_url}/candles/days"
            while to_dt >= start_dt:
                params = {
                    "market": market,
                    "to": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "count": count,
                }
                r = requests.get(url, params=params, timeout=30)
                
                # 업비트 API 오류 처리: 404 Not Found인 경우 더 명확한 메시지 제공
                if r.status_code == 404:
                    try:
                        error_data = r.json()
                        error_info = error_data.get("error", {})
                        error_name = str(error_info.get("name", "") or "")
                        error_message = str(error_info.get("message", "") or "")
                        
                        if "not_found" in error_name.lower() or "does not exist" in error_message.lower():
                            raise ValueError(
                                f"업비트에서 지원하지 않는 거래 페어입니다: {market} ({base}/{quote}). "
                                f"업비트에서 거래 가능한 마켓인지 확인해주세요. "
                                f"일반적으로 업비트는 KRW 마켓을 주로 지원하며, 일부 USDT 마켓도 제공합니다. "
                                f"오류 상세: {error_message or error_name}"
                            )
                        else:
                            raise ValueError(
                                f"업비트 API 오류 (404): {error_message or error_name}. "
                                f"요청 파라미터: market={market}, to={to_dt.strftime('%Y-%m-%dT%H:%M:%S')}, count={count}"
                            )
                    except (ValueError, KeyError, TypeError):
                        # JSON 파싱 실패 또는 이미 ValueError가 발생한 경우
                        raise ValueError(
                            f"업비트 API 오류 (404 Not Found): {market} 페어가 존재하지 않거나 "
                            f"요청한 엔드포인트를 찾을 수 없습니다. 응답: {r.text[:200]}"
                        )
                
                r.raise_for_status()
                data = r.json()
                if not data:
                    break
                for item in data:
                    # candle_date_time_kst 또는 candle_date_time_utc
                    ts_str = item.get("candle_date_time_kst") or item.get("candle_date_time_utc", "")
                    if not ts_str:
                        continue
                    dt = _parse_upbit_time(ts_str)
                    if dt and dt < start_dt:
                        continue
                    if dt and dt > end_dt:
                        continue
                    vol = item.get("candle_acc_trade_volume") or 0
                    all_rows.append({
                        "datetime_utc": dt,
                        "open": float(item["opening_price"]),
                        "high": float(item["high_price"]),
                        "low": float(item["low_price"]),
                        "close": float(item["trade_price"]),
                        "volume": float(vol),
                    })
                to_dt = datetime.strptime(data[-1]["candle_date_time_kst"].split("T")[0], "%Y-%m-%d").replace(tzinfo=timezone.utc) - timedelta(days=1)
        else:
            unit = self.get_interval_param(interval_unit, interval_value)
            if unit is None or unit == "days":
                return pd.DataFrame(columns=OHLCV_COLUMNS)
            url = f"{self.base_url}/candles/minutes/{unit}"
            while to_dt >= start_dt:
                params = {
                    "market": market,
                    "to": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "count": count,
                }
                r = requests.get(url, params=params, timeout=30)
                
                # 업비트 API 오류 처리: 404 Not Found인 경우 더 명확한 메시지 제공
                if r.status_code == 404:
                    try:
                        error_data = r.json()
                        error_info = error_data.get("error", {})
                        error_name = str(error_info.get("name", "") or "")
                        error_message = str(error_info.get("message", "") or "")
                        
                        if "not_found" in error_name.lower() or "does not exist" in error_message.lower():
                            raise ValueError(
                                f"업비트에서 지원하지 않는 거래 페어입니다: {market} ({base}/{quote}). "
                                f"업비트에서 거래 가능한 마켓인지 확인해주세요. "
                                f"일반적으로 업비트는 KRW 마켓을 주로 지원하며, 일부 USDT 마켓도 제공합니다. "
                                f"오류 상세: {error_message or error_name}"
                            )
                        else:
                            raise ValueError(
                                f"업비트 API 오류 (404): {error_message or error_name}. "
                                f"요청 파라미터: market={market}, to={to_dt.strftime('%Y-%m-%dT%H:%M:%S')}, count={count}"
                            )
                    except (ValueError, KeyError, TypeError):
                        # JSON 파싱 실패 또는 이미 ValueError가 발생한 경우
                        raise ValueError(
                            f"업비트 API 오류 (404 Not Found): {market} 페어가 존재하지 않거나 "
                            f"요청한 엔드포인트를 찾을 수 없습니다. 응답: {r.text[:200]}"
                        )
                
                r.raise_for_status()
                data = r.json()
                if not data:
                    break
                for item in data:
                    ts_str = item.get("candle_date_time_kst") or item.get("candle_date_time_utc", "")
                    if not ts_str:
                        continue
                    dt = _parse_upbit_time(ts_str)
                    if dt and dt < start_dt:
                        continue
                    if dt and dt > end_dt:
                        continue
                    vol = item.get("candle_acc_trade_volume") or 0
                    all_rows.append({
                        "datetime_utc": dt,
                        "open": float(item["opening_price"]),
                        "high": float(item["high_price"]),
                        "low": float(item["low_price"]),
                        "close": float(item["trade_price"]),
                        "volume": float(vol),
                    })
                last_parsed = _parse_upbit_time(data[-1]["candle_date_time_kst"])
                to_dt = (last_parsed - timedelta(minutes=unit)) if last_parsed else (to_dt - timedelta(minutes=unit))
                if to_dt.tzinfo is None:
                    to_dt = to_dt.replace(tzinfo=timezone.utc)
        if not all_rows:
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        df = pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)
        df = df.drop_duplicates(subset=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True)
        return df


def _parse_upbit_time(ts_str: str, force_kst: bool = False) -> Optional[datetime]:
    """업비트/빗썸 KST/UTC 문자열을 UTC datetime으로 변환.
    
    Args:
        ts_str: 시간 문자열 (예: "2024-12-06T08:00:00" 또는 "2024-12-06T08:00:00Z")
        force_kst: True이면 무조건 KST로 해석 (빗썸 API의 candle_date_time_kst 필드용)
    """
    try:
        if "T" in ts_str:
            dt = datetime.strptime(ts_str.replace("Z", "").split(".")[0].strip(), "%Y-%m-%dT%H:%M:%S")
        else:
            dt = datetime.strptime(ts_str.split(" ")[0].strip(), "%Y-%m-%d")
        # 업비트/빗썸 candle_date_time_kst는 KST(UTC+9). UTC로 변환.
        kst = timezone(timedelta(hours=9))
        if force_kst or "KST" in ts_str.upper() or (len(ts_str) <= 19 and not ts_str.endswith("Z") and not force_kst):
            dt = dt.replace(tzinfo=kst).astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


class BithumbAPI(BaseExchangeAPI):
    """빗썸 공개 API (API 2.0).

    - 분봉: `GET /v1/candles/minutes/{unit}` (unit=1,3,5,10,15,30,60,240...)
    - 일봉: `GET /v1/candles/days`
    - 공통 파라미터: market(예: KRW-BTC), to(ISO), count
    """

    name = "Bithumb"
    base_url = "https://api.bithumb.com/v1"

    INTERVAL_MAP = {
        ("minute", 1): ("minutes", 1),
        ("minute", 3): ("minutes", 3),
        ("minute", 5): ("minutes", 5),
        ("minute", 10): ("minutes", 10),
        ("minute", 15): ("minutes", 15),
        ("minute", 30): ("minutes", 30),
        ("hour", 1): ("minutes", 60),
        ("hour", 4): ("minutes", 240),
        ("day", 1): ("days", None),
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[Any]:
        return self.INTERVAL_MAP.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        # Bithumb API 2.0 market code: KRW-BTC
        return f"{quote.upper()}-{base.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        interval_param = self.get_interval_param(interval_unit, interval_value)
        if not interval_param:
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        endpoint, unit = interval_param
        market = self.get_symbol(base, quote)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)

        if endpoint == "days":
            url = f"{self.base_url}/candles/days"
        else:
            url = f"{self.base_url}/candles/minutes/{unit}"

        all_rows: list[dict] = []
        # 빗썸 API: to = "마지막 캔들 시각(exclusive). 기본적으로 KST 기준 시간" (공식 문서)
        # → UTC를 KST로 변환하여 전달
        kst = timezone(timedelta(hours=9))
        to_dt_kst = end_dt.astimezone(kst)  # UTC → KST 변환
        start_dt_kst = start_dt.astimezone(kst)
        raw_total = 0
        raw_min_utc = None
        raw_max_utc = None
        last_status = None
        last_http = None

        while to_dt_kst >= start_dt_kst:
            params = {
                "market": market,
                "to": to_dt_kst.strftime("%Y-%m-%dT%H:%M:%S"),  # KST로 변환된 시간 전달
                "count": 200,
            }
            try:
                r = requests.get(url, params=params, timeout=30)
                last_http = r.status_code
                r.raise_for_status()
                data = r.json()
                last_status = "ok"
            except Exception as e:
                self._set_last_debug(exchange=self.name, url=url, params=params, http_status=last_http, error=str(e))
                raise

            if not data or not isinstance(data, list):
                # data가 비어있거나 리스트가 아닌 경우
                if isinstance(data, str) and data:
                    # 응답이 문자열인 경우 오류로 처리
                    error_msg = f"Bithumb API 오류: {data}"
                    self._set_last_debug(exchange=self.name, url=url, params=params, http_status=last_http, api_status="error", error=error_msg, requested_start_utc=start_dt, requested_end_utc=end_dt)
                    raise ValueError(f"{error_msg}. 거래 페어({market})가 올바른지 확인해주세요.")
                break

            raw_total += len(data)

            for item in data:
                # 빗썸 API 2.0 응답: candle_date_time_kst를 우선 사용 (KST 명시적)
                # candle_date_time_kst가 있으면 KST로 해석, 없으면 candle_date_time_utc를 UTC로 해석
                dt_str_kst = item.get("candle_date_time_kst")
                dt_str_utc = item.get("candle_date_time_utc")
                
                if dt_str_kst:
                    # KST 필드가 있으면 강제로 KST로 해석
                    dt = _parse_upbit_time(dt_str_kst, force_kst=True)
                elif dt_str_utc:
                    # UTC 필드만 있으면 UTC로 해석
                    dt = _parse_upbit_time(dt_str_utc, force_kst=False)
                else:
                    dt = None
                
                if dt is None:
                    continue
                # 원본 데이터의 시간 범위 기록 (필터링 전)
                if raw_min_utc is None or dt < raw_min_utc:
                    raw_min_utc = dt
                if raw_max_utc is None or dt > raw_max_utc:
                    raw_max_utc = dt
                # 필터링: 요청 기간 내 데이터만 추가
                ts_ms = int(item.get("timestamp") or int(dt.timestamp() * 1000))
                if ts_ms < start_ts or ts_ms > end_ts:
                    continue
                all_rows.append(
                    {
                        "datetime_utc": dt,
                        "open": float(item["opening_price"]),
                        "high": float(item["high_price"]),
                        "low": float(item["low_price"]),
                        "close": float(item["trade_price"]),
                        "volume": float(item.get("candle_acc_trade_volume", 0) or 0),
                    }
                )

            # 다음 페이지: 가장 오래된 캔들의 시간 이전으로 이동
            oldest_kst = data[-1].get("candle_date_time_kst")
            oldest_utc = data[-1].get("candle_date_time_utc")
            if oldest_kst:
                oldest_dt_utc = _parse_upbit_time(oldest_kst, force_kst=True)
            elif oldest_utc:
                oldest_dt_utc = _parse_upbit_time(oldest_utc, force_kst=False)
            else:
                oldest_dt_utc = None
            if not oldest_dt_utc:
                break
            
            # oldest_dt를 KST로 변환해서 다음 to 파라미터 계산
            oldest_dt_kst = oldest_dt_utc.astimezone(kst)
            
            # 빗썸 API는 to 시각 이전의 최근 N개를 반환하므로, oldest_dt보다 더 이전으로 이동
            # 단, start_dt_kst보다는 이후여야 함
            if endpoint == "days":
                next_to_kst = oldest_dt_kst - timedelta(days=1)
            else:
                # unit은 분 단위. oldest_dt보다 더 이전으로 이동 (예: 1분봉이면 1분 전)
                next_to_kst = oldest_dt_kst - timedelta(minutes=int(unit))
            
            # start_dt_kst보다 이전이면 종료
            if next_to_kst < start_dt_kst:
                break
            to_dt_kst = next_to_kst

        df = pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)
        df = df.drop_duplicates(subset=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True) if not df.empty else df
        # 요청 기간 정보도 진단에 포함
        # 마지막 요청의 to 파라미터 (KST로 변환된 값)
        last_to_kst = end_dt.astimezone(kst) if raw_total == 0 else to_dt_kst
        
        self._set_last_debug(
            exchange=self.name,
            url=url,
            params={"market": market, "to": last_to_kst.strftime("%Y-%m-%dT%H:%M:%S"), "count": 200},
            http_status=last_http,
            api_status=last_status,
            raw_count=raw_total,
            raw_min_utc=raw_min_utc,
            raw_max_utc=raw_max_utc,
            requested_start_utc=start_dt,
            requested_end_utc=end_dt,
            filtered_count=len(df),
            note="Bithumb 공식 문서: to는 '기본적으로 KST 기준 시간'. params의 to는 KST로 전달됨.",
        )
        return df


class CoinoneAPI(BaseExchangeAPI):
    """코인원 공개 API. interval: 1m, 3m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 6h, 1d, 1w, 1mon."""

    name = "Coinone"
    base_url = "https://api.coinone.co.kr/public/v2"

    INTERVAL_MAP = {
        ("minute", 1): "1m",
        ("minute", 3): "3m",
        ("minute", 5): "5m",
        ("minute", 10): "10m",
        ("minute", 15): "15m",
        ("minute", 30): "30m",
        ("hour", 1): "1h",
        ("hour", 2): "2h",
        ("hour", 4): "4h",
        ("hour", 6): "6h",
        ("day", 1): "1d",
        ("day", 7): "1w",
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[str]:
        return self.INTERVAL_MAP.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        return f"{quote.upper()}-{base.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        interval_str = self.get_interval_param(interval_unit, interval_value)
        if not interval_str:
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        # path: chart/{quote_currency}/{target_currency} -> KRW/BTC
        quote_currency = quote.upper()
        target_currency = base.upper()
        url = f"{self.base_url}/chart/{quote_currency}/{target_currency}"
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
        all_rows = []
        cursor_ms = end_ms
        while cursor_ms > start_ms:
            params = {"interval": interval_str, "timestamp": cursor_ms, "size": 500}
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data.get("result") != "success" or data.get("error_code") != "0":
                break
            chart = data.get("chart", [])
            if not chart:
                break
            for c in chart:
                ts_ms = int(c["timestamp"])
                if ts_ms < start_ms:
                    continue
                if ts_ms > end_ms:
                    continue
                dt = _parse_ts_ms(ts_ms)
                all_rows.append({
                    "datetime_utc": dt,
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"]),
                    "volume": float(c.get("target_volume", 0) or 0),
                })
            cursor_ms = min(int(c["timestamp"]) for c in chart) - 1
        df = pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)
        return df.drop_duplicates(subset=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True) if not df.empty else df


class KorbitAPI(BaseExchangeAPI):
    """코빗 공개 API. interval: 1, 5, 15, 30, 60, 240, 1D, 1W. 인증 없이 시세 조회 가능."""

    name = "Korbit"
    base_url = "https://api.korbit.co.kr/v2"

    INTERVAL_MAP = {
        ("minute", 1): "1",
        ("minute", 5): "5",
        ("minute", 15): "15",
        ("minute", 30): "30",
        ("hour", 1): "60",
        ("hour", 4): "240",
        ("day", 1): "1D",
        ("day", 7): "1W",
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[str]:
        return self.INTERVAL_MAP.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        return f"{base.lower()}_{quote.lower()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        interval_str = self.get_interval_param(interval_unit, interval_value)
        if not interval_str:
            self._set_last_debug(
                exchange=self.name,
                error=f"지원하지 않는 구간: {interval_unit} {interval_value}",
                requested_start_utc=start_dt,
                requested_end_utc=end_dt,
            )
            return pd.DataFrame(columns=OHLCV_COLUMNS)
        symbol = self.get_symbol(base, quote)
        end_ms = int(end_dt.timestamp() * 1000)
        start_ms = int(start_dt.timestamp() * 1000)
        url = f"{self.base_url}/candles"
        all_rows = []
        raw_min_utc = None
        raw_max_utc = None
        last_http_status = None
        last_error = None
        
        try:
            # 코빗 API는 end 시점부터 역순으로 최대 limit개를 반환합니다.
            # start 파라미터를 보내도 end부터 역순으로 반환하므로, 페이지네이션은 end를 줄여가면서 진행합니다.
            while end_ms > start_ms:
                # 코빗 문서: start(선택), end(선택), limit(필수)
                # start와 end를 모두 보내면, end 시점부터 역순으로 최대 limit개를 반환합니다.
                # 따라서 페이지네이션을 위해 end를 줄여가면서 요청합니다.
                params = {
                    "symbol": symbol,
                    "interval": interval_str,
                    "start": start_ms,  # start도 보내지만, 실제로는 end부터 역순으로 반환됨
                    "end": end_ms,
                    "limit": 200,
                }
                r = requests.get(url, params=params, timeout=30)
                last_http_status = r.status_code
                r.raise_for_status()
                data = r.json()
                
                # Korbit API 오류 처리
                if not isinstance(data, dict):
                    error_msg = f"Korbit API 응답 형식 오류: 예상된 dict, 받은 {type(data).__name__}"
                    last_error = error_msg
                    self._set_last_debug(
                        exchange=self.name,
                        url=url,
                        params=params,
                        http_status=last_http_status,
                        api_status="invalid_response",
                        error=error_msg,
                        requested_start_utc=start_dt,
                        requested_end_utc=end_dt,
                    )
                    raise ValueError(f"{error_msg}. 응답: {str(data)[:200]}")
                
                success = data.get("success")
                if not success:
                    error_obj = data.get("error")
                    error_msg = error_obj.get("message", "Korbit API 오류: success가 false입니다") if isinstance(error_obj, dict) else "Korbit API 오류: success가 false입니다"
                    last_error = error_msg
                    self._set_last_debug(
                        exchange=self.name,
                        url=url,
                        params=params,
                        http_status=last_http_status,
                        api_status="error",
                        error=error_msg,
                        requested_start_utc=start_dt,
                        requested_end_utc=end_dt,
                    )
                    raise ValueError(
                        f"Korbit API 오류: {error_msg}. "
                        f"거래 페어({symbol})가 올바른지 확인해주세요."
                    )
                
                lst = data.get("data", [])
                if not lst or not isinstance(lst, list):
                    # success=True, data=[] → 요청 기간이 코빗 제공 범위를 벗어났을 가능성 (과거 데이터 제한)
                    if len(all_rows) == 0:
                        raw_preview = str(data)[:500] if data else ""
                        df = pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)
                        self._set_last_debug(
                            exchange=self.name,
                            url=url,
                            params=params,
                            http_status=last_http_status,
                            api_status="no_data",
                            raw_count=0,
                            raw_min_utc=None,
                            raw_max_utc=None,
                            requested_start_utc=start_dt,
                            requested_end_utc=end_dt,
                            filtered_count=0,
                            raw_response_preview=raw_preview,
                            note="success=True, data=[] → 요청 기간이 코빗 제공 범위를 벗어났을 수 있음. 더 최근 기간으로 조회해 보세요.",
                        )
                        return df
                    break
                
                # 코빗 API는 end 시점부터 역순으로 데이터를 반환하므로, 
                # 받은 데이터를 역순으로 처리하여 시간 순서대로 정렬합니다.
                valid_items = []
                for item in lst:
                    if not isinstance(item, dict):
                        continue
                    ts_ms = int(item.get("timestamp", 0))
                    # start_ms 이상 end_ms 이하인 데이터만 수집
                    if ts_ms >= start_ms and ts_ms <= end_ms:
                        valid_items.append(item)
                
                # valid_items를 timestamp 순서대로 정렬 (오름차순)
                valid_items.sort(key=lambda x: int(x.get("timestamp", 0)))
                
                for item in valid_items:
                    ts_ms = int(item.get("timestamp", 0))
                    dt = _parse_ts_ms(ts_ms)
                    if dt:
                        if raw_min_utc is None or dt < raw_min_utc:
                            raw_min_utc = dt
                        if raw_max_utc is None or dt > raw_max_utc:
                            raw_max_utc = dt
                        all_rows.append({
                            "datetime_utc": dt,
                            "open": float(item.get("open", 0)),
                            "high": float(item.get("high", 0)),
                            "low": float(item.get("low", 0)),
                            "close": float(item.get("close", 0)),
                            "volume": float(item.get("volume", 0) or 0),
                        })
                
                # 다음 페이지: 가장 오래된 타임스탬프 이전으로 이동
                if valid_items:
                    min_ts = min(int(item.get("timestamp", 0)) for item in valid_items)
                    if min_ts > start_ms:
                        # 다음 요청의 end를 가장 오래된 타임스탬프 - 1로 설정
                        end_ms = min_ts - 1
                    else:
                        # start_ms에 도달했으므로 종료
                        break
                else:
                    # 유효한 데이터가 없으면 종료
                    break
            
            df = pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)
            df = df.drop_duplicates(subset=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True) if not df.empty else df
            
            # 데이터가 없고 오류가 있었던 경우
            if len(df) == 0 and last_error:
                self._set_last_debug(
                    exchange=self.name,
                    url=url,
                    params={"symbol": symbol, "interval": interval_str, "end": end_ms, "limit": 200},
                    http_status=last_http_status,
                    api_status="error",
                    error=last_error,
                    requested_start_utc=start_dt,
                    requested_end_utc=end_dt,
                    raw_count=0,
                    filtered_count=0,
                )
                raise ValueError(
                    f"Korbit API 오류: {last_error}. "
                    f"거래 페어({symbol})가 올바른지 확인해주세요."
                )
            
            # 마지막 요청 파라미터 (진단 정보용)
            last_params = {
                "symbol": symbol,
                "interval": interval_str,
                "start": start_ms,
                "end": end_ms,
                "limit": 200,
            }
            self._set_last_debug(
                exchange=self.name,
                url=url,
                params=last_params,
                http_status=last_http_status,
                api_status="success" if len(df) > 0 else "no_data",
                raw_count=len(all_rows),
                raw_min_utc=raw_min_utc,
                raw_max_utc=raw_max_utc,
                requested_start_utc=start_dt,
                requested_end_utc=end_dt,
                filtered_count=len(df),
            )
            return df
            
        except ValueError:
            raise
        except Exception as e:
            self._set_last_debug(
                exchange=self.name,
                url=url,
                params={"symbol": symbol, "interval": interval_str, "end": end_ms, "limit": 200},
                http_status=last_http_status,
                error=str(e),
                requested_start_utc=start_dt,
                requested_end_utc=end_dt,
            )
            raise


# --- 해외 거래소 추가: Gate.io, HTX ---


class GateioAPI(BaseExchangeAPI):
    """Gate.io API v4 (Spot) 공개 캔들."""

    name = "Gate.io"
    base_url = "https://api.gateio.ws/api/v4"

    # Gate: interval 예) 1m, 5m, 15m, 30m, 1h, 4h, 8h, 1d, 7d, 30d, 1s ...
    INTERVAL_MAP = {
        ("second", 1): "1s",
        ("second", 10): "10s",
        ("minute", 1): "1m",
        ("minute", 5): "5m",
        ("minute", 15): "15m",
        ("minute", 30): "30m",
        ("hour", 1): "1h",
        ("hour", 4): "4h",
        ("hour", 8): "8h",
        ("day", 1): "1d",
        ("day", 7): "7d",
        ("day", 30): "30d",
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[str]:
        return self.INTERVAL_MAP.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        # Gate spot: BTC_USDT
        return f"{base.upper()}_{quote.upper()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        interval_str = self.get_interval_param(interval_unit, interval_value)
        if not interval_str:
            return pd.DataFrame(columns=OHLCV_COLUMNS)

        currency_pair = self.get_symbol(base, quote)
        url = f"{self.base_url}/spot/candlesticks"

        start_s = int(start_dt.timestamp())
        end_s = int(end_dt.timestamp())
        all_rows: list[dict] = []

        # Gate는 from/to(초) + limit(최대 1000). 한 번에 다 못 받으면 구간을 잘라서 반복.
        # (정확한 정렬 순서는 API에 따라 다를 수 있어 최종적으로 정렬/중복제거)
        cursor_from = start_s
        while cursor_from < end_s:
            params = {
                "currency_pair": currency_pair,
                "interval": interval_str,
                "from": cursor_from,
                "to": end_s,
                "limit": 1000,
            }
            r = requests.get(url, params=params, timeout=30)
            
            # Gate.io API 오류 처리: 400 Bad Request인 경우 더 명확한 메시지 제공
            if r.status_code == 400:
                try:
                    error_data = r.json()
                    error_label = error_data.get("label", "")
                    error_message = error_data.get("message", "")
                    
                    # INVALID_CURRENCY_PAIR 오류인 경우
                    if "INVALID_CURRENCY_PAIR" in error_label or "currency_pair" in error_message.lower():
                        raise ValueError(
                            f"Gate.io에서 지원하지 않는 거래 페어입니다: {currency_pair} ({base}/{quote}). "
                            f"Gate.io Spot 시장에서 거래 가능한 페어인지 확인해주세요. "
                            f"오류 상세: {error_message or error_label}"
                        )
                    # "Candlestick too long ago" 오류: 최대 10,000개 캔들 제한
                    elif "too long ago" in error_message.lower() or "10000 points" in error_message.lower():
                        # 현재 시점으로부터 최대 조회 가능한 기간 계산
                        now_s = int(datetime.now(timezone.utc).timestamp())
                        max_points = 10000
                        
                        # interval에 따른 최대 조회 가능 기간 계산
                        if interval_str.endswith("s"):
                            seconds_per_candle = int(interval_str[:-1])
                        elif interval_str.endswith("m"):
                            seconds_per_candle = int(interval_str[:-1]) * 60
                        elif interval_str.endswith("h"):
                            seconds_per_candle = int(interval_str[:-1]) * 3600
                        elif interval_str.endswith("d"):
                            seconds_per_candle = int(interval_str[:-1]) * 86400
                        else:
                            seconds_per_candle = 60  # 기본값 1분
                        
                        max_seconds_ago = max_points * seconds_per_candle
                        max_datetime_utc = datetime.fromtimestamp(now_s - max_seconds_ago, tz=timezone.utc)
                        
                        # 요청한 시작 시간과 비교
                        requested_start_utc = datetime.fromtimestamp(cursor_from, tz=timezone.utc)
                        
                        raise ValueError(
                            f"Gate.io API 제한: 현재 시점으로부터 최대 10,000개의 캔들 데이터까지만 조회할 수 있습니다.\n"
                            f"  - 요청한 시작 시간: {requested_start_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                            f"  - 최대 조회 가능 시작 시간: {max_datetime_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                            f"  - 현재 시점: {datetime.fromtimestamp(now_s, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
                            f"  - {interval_str} 기준 약 {max_points * seconds_per_candle / 86400:.1f}일치 데이터만 조회 가능\n"
                            f"웹사이트에서는 더 오래된 데이터를 보여줄 수 있지만, API에는 이 제한이 있습니다. "
                            f"더 최근 기간으로 조회 범위를 조정해주세요."
                        )
                    else:
                        # 기타 400 오류
                        raise ValueError(
                            f"Gate.io API 오류 (400): {error_message or error_label}. "
                            f"요청 파라미터: currency_pair={currency_pair}, interval={interval_str}, "
                            f"from={cursor_from}, to={end_s}"
                        )
                except (ValueError, KeyError):
                    # JSON 파싱 실패 또는 이미 ValueError가 발생한 경우
                    raise ValueError(
                        f"Gate.io API 오류 (400 Bad Request): {currency_pair} 페어가 지원되지 않거나 "
                        f"요청 파라미터가 잘못되었습니다. 응답: {r.text[:200]}"
                    )
            
            r.raise_for_status()
            data = r.json()
            if not data:
                break

            # Gate 응답(대표 형식): [t, quote_volume, close, high, low, open, base_volume, ...]
            for row in data:
                try:
                    ts_s = int(float(row[0]))
                except Exception:
                    continue
                if ts_s < start_s or ts_s > end_s:
                    continue
                dt = _parse_ts_ms(ts_s)
                all_rows.append(
                    {
                        "datetime_utc": dt,
                        "open": float(row[5]),
                        "high": float(row[3]),
                        "low": float(row[4]),
                        "close": float(row[2]),
                        "volume": float(row[6]) if len(row) > 6 else 0.0,
                    }
                )

            # 다음 구간 시작점: 이번 배치에서 가장 큰 timestamp 이후로
            try:
                max_ts = max(int(float(row[0])) for row in data)
            except Exception:
                break
            next_from = max_ts + 1
            if next_from <= cursor_from:
                break
            cursor_from = next_from

        df = pd.DataFrame(all_rows, columns=OHLCV_COLUMNS)
        return df.drop_duplicates(subset=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True) if not df.empty else df


class HtxAPI(BaseExchangeAPI):
    """HTX(구 Huobi) Spot 공개 캔들.

    HTX의 새로운 API 문서(https://www.htx.com/en-us/opend/newApiPages/)에 맞춰 구현.
    from/to 파라미터를 먼저 시도하고, 지원하지 않으면 size 기반으로 폴백.
    
    참고: Market Data API는 공개 API로, API key 인증이 필요하지 않습니다.
    /market/history/kline 엔드포인트는 인증 없이 접근 가능합니다.
    """

    name = "HTX"
    base_url = "https://api.huobi.pro"  # HTX는 여전히 api.huobi.pro 도메인 사용 (공개 Market Data API)

    PERIOD_MAP = {
        ("minute", 1): "1min",
        ("minute", 5): "5min",
        ("minute", 15): "15min",
        ("minute", 30): "30min",
        ("hour", 1): "1hour",
        ("hour", 4): "4hour",
        ("day", 1): "1day",
    }

    def get_interval_param(self, interval_unit: str, interval_value: int) -> Optional[str]:
        return self.PERIOD_MAP.get((interval_unit, interval_value))

    def get_symbol(self, base: str, quote: str) -> str:
        # HTX spot: btcusdt 형태(소문자)
        return f"{base.lower()}{quote.lower()}"

    def fetch_klines(
        self,
        base: str,
        quote: str,
        start_dt: datetime,
        end_dt: datetime,
        interval_unit: str,
        interval_value: int,
    ) -> pd.DataFrame:
        period = self.get_interval_param(interval_unit, interval_value)
        if not period:
            return pd.DataFrame(columns=OHLCV_COLUMNS)

        symbol = self.get_symbol(base, quote)
        url = f"{self.base_url}/market/history/kline"

        start_s = int(start_dt.timestamp())
        end_s = int(end_dt.timestamp())

        # HTX 새로운 API: from/to 파라미터를 먼저 시도
        # from/to는 초 단위 Unix timestamp
        params_with_from_to = {
            "symbol": symbol,
            "period": period,
            "from": start_s,
            "to": end_s,
        }
        
        try:
            self._set_last_debug(
                api_name="HTX",
                url=url,
                params=params_with_from_to,
                note="HTX 새로운 API: from/to 파라미터를 사용하여 시도합니다.",
            )
            
            r = requests.get(url, params=params_with_from_to, timeout=30)
            r.raise_for_status()
            j = r.json()
            
            # from/to가 지원되는 경우
            if j.get("status") == "ok":
                data = j.get("data", []) or []
                if data:
                    # from/to가 성공적으로 작동한 경우
                    return self._process_htx_data(data, start_s, end_s, url, params_with_from_to, start_dt, end_dt)
            
            # from/to가 지원되지 않거나 데이터가 없는 경우, size 기반으로 폴백
            self._set_last_debug(
                api_name="HTX",
                url=url,
                params=params_with_from_to,
                note="from/to 파라미터가 지원되지 않거나 데이터가 없어 size 기반으로 폴백합니다.",
            )
            
        except (requests.exceptions.RequestException, ValueError) as e:
            # from/to 시도 실패 시 size 기반으로 폴백
            pass
        
        # size 기반 폴백: 요청 기간에 필요한 데이터 개수 추정
        duration_seconds = end_s - start_s
        if interval_unit == "minute":
            estimated_count = duration_seconds // (interval_value * 60) + 50  # 여유분 추가
        elif interval_unit == "hour":
            estimated_count = duration_seconds // (interval_value * 3600) + 50
        elif interval_unit == "day":
            estimated_count = duration_seconds // (interval_value * 86400) + 50
        else:
            estimated_count = 2000  # 기본값

        # HTX API 최대 size는 2000
        size = min(int(estimated_count), 2000)
        
        params = {"symbol": symbol, "period": period, "size": size}
        
        self._set_last_debug(
            api_name="HTX",
            url=url,
            params=params,
            note="HTX API: size 기반으로 최신 N개 데이터를 조회합니다.",
        )
        
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        if j.get("status") != "ok":
            err_code = j.get("err-code", "")
            err_msg = j.get("err-msg", "")
            raise ValueError(
                f"HTX API 오류: {err_code} - {err_msg}. "
                f"요청 파라미터: symbol={symbol}, period={period}, size={size}"
            )
        
        data = j.get("data", []) or []
        if not data:
            raise ValueError(
                f"HTX API에서 데이터를 반환하지 않았습니다. "
                f"요청 파라미터: symbol={symbol}, period={period}, size={size}. "
                f"해당 거래쌍이 지원되는지, 또는 요청한 기간에 데이터가 있는지 확인해주세요."
            )
        
        return self._process_htx_data(data, start_s, end_s, url, params, start_dt, end_dt)
    
    def _process_htx_data(
        self,
        data: list,
        start_s: int,
        end_s: int,
        url: str,
        params: dict,
        start_dt: datetime,
        end_dt: datetime,
    ) -> pd.DataFrame:
        """HTX API 응답 데이터를 처리하여 DataFrame으로 변환."""
        # 반환된 데이터의 시간 범위 확인
        timestamps = [int(item.get("id", 0)) for item in data if item.get("id")]
        if not timestamps:
            raise ValueError(
                f"HTX API 응답에 유효한 타임스탬프가 없습니다. "
                f"응답 데이터: {data[:3] if len(data) > 3 else data}"
            )
        
        min_ts = min(timestamps)
        max_ts = max(timestamps)
        min_dt = _parse_ts_ms(min_ts)
        max_dt = _parse_ts_ms(max_ts)
        
        self._set_last_debug(
            api_name="HTX",
            url=url,
            params=params,
            response_time_range_utc=f"{min_dt} ~ {max_dt}",
            response_count=len(data),
            requested_time_range_utc=f"{start_dt} ~ {end_dt}",
            note="HTX API 응답 데이터의 시간 범위를 확인했습니다.",
        )
        
        # 데이터 필터링 및 변환
        out = []
        for item in data:
            ts_s = int(item.get("id", 0))
            if ts_s < start_s or ts_s > end_s:
                continue
            dt = _parse_ts_ms(ts_s)
            out.append(
                {
                    "datetime_utc": dt,
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    # HTX: vol(거래량), amount(거래대금) 등이 함께 존재
                    "volume": float(item.get("vol", 0) or 0),
                }
            )
        
        if not out:
            # 데이터가 필터링되어 비어있는 경우
            raise ValueError(
                f"HTX API에서 데이터를 받았지만, 요청한 기간({start_dt} ~ {end_dt})에 해당하는 데이터가 없습니다. "
                f"반환된 데이터의 시간 범위: {min_dt} ~ {max_dt}. "
                f"요청한 기간이 반환된 데이터 범위와 겹치지 않을 수 있습니다."
            )
        
        df = pd.DataFrame(out, columns=OHLCV_COLUMNS)
        return df.drop_duplicates(subset=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True) if not df.empty else df


# 등록된 거래소 목록 (앱에서 선택용)
# 참고: 모든 거래소는 공개 Market Data API를 사용하므로 API key가 필요하지 않습니다.
# HTX는 from/to 파라미터를 지원하지 않아 과거 데이터 조회가 제한되므로 제외됨.
EXCHANGE_APIS = {
    "binance": BinanceAPI(),      # 공개 API (인증 불필요)
    # "kraken": KrakenAPI(),      # 제외: USDT 페어 지원 제한적 (주로 USD, EUR 사용)
    "bybit": BybitAPI(),          # 공개 API (인증 불필요)
    "okx": OKXAPI(),              # 공개 API (인증 불필요)
    "coinbase": CoinbaseAPI(),    # 공개 API (인증 불필요)
    # "kucoin": KuCoinAPI(),      # 제외: 과거 데이터 조회 제한 (약 1년 이상 전 데이터 반환 안됨)
    # "gate": GateioAPI(),        # 제외: 현재 시점으로부터 최대 10,000개 캔들만 조회 가능 (1분봉 기준 약 7일)으로 과거 데이터 조회 불가
    # "htx": HtxAPI(),            # 제외: from/to 파라미터 미지원으로 과거 데이터 조회 불가
    "upbit": UpbitAPI(),          # 공개 API (인증 불필요)
    "bithumb": BithumbAPI(),      # 공개 API (인증 불필요)
    "coinone": CoinoneAPI(),     # 공개 API (인증 불필요)
    "korbit": KorbitAPI(),        # 공개 API (인증 불필요)
}


def get_supported_exchanges() -> list[tuple[str, str]]:
    """(id, 표시명) 리스트 반환."""
    return [(eid, api.name) for eid, api in EXCHANGE_APIS.items()]


def fetch_ohlcv(
    exchange_id: str,
    base: str,
    quote: str,
    start_dt: datetime,
    end_dt: datetime,
    interval_unit: str,
    interval_value: int,
) -> pd.DataFrame:
    """지정 거래소에서 OHLCV 조회. 지원하지 않는 interval이면 빈 DataFrame."""
    api = EXCHANGE_APIS.get(exchange_id)
    if not api or not api.get_interval_param(interval_unit, interval_value):
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    return api.fetch_klines(base, quote, start_dt, end_dt, interval_unit, interval_value)
