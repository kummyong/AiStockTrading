# manage_data.py (재무 + 일봉 데이터 수집 최종본)

import requests
import sqlite3
from datetime import datetime, timedelta
import time
import os
import zipfile
import io
import xml.etree.ElementTree as ET
import configparser
import logging
from tqdm import tqdm

# --- 1. 설정 ---
DATA_START_YEAR = 2018  # 재무 데이터 수집 시작 연도
CHART_START_DATE = "20180101"  # 일봉 차트 데이터 수집 시작 날짜 (YYYYMMDD)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_manager.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


# --- 2. 설정 로드 클래스 ---
class ConfigManager:
    """config.ini 파일에서 실서버용 설정을 읽어 관리합니다."""

    def __init__(self, config_file="config.ini"):
        if not os.path.isabs(config_file):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_file = os.path.join(script_dir, config_file)
        config = configparser.ConfigParser()
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"설정 파일({config_file})을 찾을 수 없습니다.")
        config.read(config_file, encoding="utf-8")
        self.base_url = config.get("KIWOOM_REAL", "base_url").strip("'\"")
        self.kiwoom_app_key = config.get("KIWOOM_REAL", "app_key").strip("'\"")
        self.kiwoom_app_secret = config.get("KIWOOM_REAL", "app_secret").strip("'\"")
        self.account_no = config.get("KIWOOM_REAL", "account_no").strip("'\"")
        self.dart_api_key = config.get("DART", "api_key").strip("'\"")
        logging.info("✅ 실서버용 설정을 성공적으로 불러왔습니다.")


# --- 3. DART API 관리 클래스 ---
class DartManager:
    """DART API 관련 기능을 관리합니다."""

    def __init__(self, api_key, script_dir):
        self.api_key = api_key
        self.script_dir = script_dir
        self.corp_codes = self._load_corp_codes()

    def _load_corp_codes(self):
        file_path = os.path.join(self.script_dir, "CORPCODE.xml")
        if not os.path.exists(file_path):
            logging.info(
                "'CORPCODE.xml' 파일이 없어 DART API를 통해 자동으로 다운로드합니다..."
            )
            try:
                url = "https://opendart.fss.or.kr/api/corpCode.xml"
                params = {"crtfc_key": self.api_key}
                res = requests.get(url, params=params)
                res.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(res.content)) as zfile:
                    zfile.extractall(self.script_dir)
                logging.info("✅ 'CORPCODE.xml' 파일 다운로드 및 압축 해제 성공!")
            except Exception as e:
                logging.error(f"❌ 'CORPCODE.xml' 파일 다운로드 실패: {e}")
                return {}
        try:
            with open(file_path, "rb") as f:
                root = ET.parse(f).getroot()
            corp_codes = {
                item.find("stock_code")
                .text.strip(): item.find("corp_code")
                .text.strip()
                for item in root.findall(".//list")
                if item.find("stock_code").text.strip()
            }
            logging.info(
                f"✅ 총 {len(corp_codes)}개의 상장사 DART 코드를 로드했습니다."
            )
            return corp_codes
        except Exception as e:
            logging.error(f"❌ DART 고유번호 파일 로드 중 오류 발생: {e}")
            return {}

    def get_financial_info_for_year(self, ticker: str, year: int):
        """DART API를 통해 특정 연도의 상세 재무 정보를 가져옵니다."""
        dart_code = self.corp_codes.get(ticker)
        if not dart_code:
            return None

        try:
            url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
            params = {
                "crtfc_key": self.api_key,
                "corp_code": dart_code,
                "bsns_year": year,
                "reprt_code": "11011",
                "fs_div": "CFS",
            }
            res = requests.get(url, params=params, timeout=5)
            res.raise_for_status()
            data = res.json()

            if data.get("status") != "000":
                params["fs_div"] = "OFS"  # 연결재무제표 없으면 일반재무제표로 재시도
                res = requests.get(url, params=params, timeout=5)
                data = res.json()
                if data.get("status") != "000":
                    return None

            accounts = {}
            account_map = {
                "당기순이익": "net_income",
                "자본총계": "total_equity",
                "영업이익": "operating_income",
                "유형자산상각비": "depreciation",
                "무형자산상각비": "amortization",
                "부채총계": "total_debt",
                "현금및현금성자산": "cash_and_equivalents",
            }

            for item in data.get("list", []):
                account_nm = item.get("account_nm", "").strip()
                for key, value in account_map.items():
                    if key in account_nm:
                        amount_str = item.get("thstrm_amount", "0").replace(",", "")
                        accounts[value] = float(amount_str) if amount_str else 0.0

            if "net_income" in accounts and "total_equity" in accounts:
                accounts["business_year"] = year
                return accounts

        except Exception as e:
            logging.warning(f"재무(DART): [{ticker}] {year}년 정보 조회 중 오류: {e}")
        return None


# --- 4. 키움 API 관리 클래스 ---
class KiwoomApiManager:
    """키움증권 API 요청을 관리합니다. (일봉 데이터 수집 기능 포함)"""

    def __init__(self, config):
        self.config = config
        self.access_token = self._get_access_token()

    def _get_access_token(self):
        res_json = self._request_api(
            path="/oauth2/token",
            headers={"Content-Type": "application/json;charset=UTF-8"},
            body={
                "grant_type": "client_credentials",
                "appkey": self.config.kiwoom_app_key,
                "secretkey": self.config.kiwoom_app_secret,
            },
        )
        if res_json and "token" in res_json:
            logging.info("✅ 토큰 발급 성공")
            return res_json["token"]
        logging.error(f"❌ 토큰 발급 실패: {res_json}")
        return None

    def get_kospi_tickers(self):
        if not self.access_token:
            return []
        headers = {
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.config.kiwoom_app_key,
            "appsecret": self.config.kiwoom_app_secret,
            "api-id": "ka10099",
        }
        body = {"mrkt_tp": "0"}
        res_json = self._request_api(
            path="/api/dostk/stkinfo", headers=headers, body=body
        )
        return res_json.get("list", []) if res_json else []

    def get_financial_info(self, ticker: str):
        """API를 통해 시가총액 정보를 가져옵니다."""
        if not self.access_token:
            return None
        headers = {
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.config.kiwoom_app_key,
            "appsecret": self.config.kiwoom_app_secret,
            "api-id": "ka10001",
        }
        body = {"stk_cd": ticker}
        res_json = self._request_api(
            path="/api/dostk/stkinfo", headers=headers, body=body
        )
        if res_json and res_json.get("mac"):
            try:
                return {"mac": float(res_json.get("mac", 0)) * 100000000}
            except (ValueError, TypeError):
                return None
        return None

    def get_daily_chart_data(self, ticker, start_date, end_date):
        """지정된 기간의 일봉 데이터를 API로 요청합니다."""
        if not self.access_token:
            return []

        headers = {
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.config.kiwoom_app_key,
            "appsecret": self.config.kiwoom_app_secret,
            "api-id": "kt10002",
        }
        body = {
            "stk_cd": ticker,
            "bgng_dt": start_date,
            "end_dt": end_date,
            "period_tp": "D",
            "out_flag": "0",
        }
        res_json = self._request_api(
            path="/api/dostk/stk-hist", headers=headers, body=body
        )
        return res_json.get("list", []) if res_json else []

    def _request_api(self, path, headers=None, body=None, params=None):
        url = f"{self.config.base_url}{path}"
        try:
            if body:
                res = requests.post(url, headers=headers, json=body, timeout=10)
            else:
                res = requests.get(url, headers=headers, params=params, timeout=10)
            time.sleep(1)  # API 과부하 방지를 위해 요청 간격 조절
            res.raise_for_status()
            return res.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"API 요청 실패: {url} - {e}")
            return None


# --- 5. 데이터베이스 관리 클래스 ---
class DatabaseManager:
    """SQLite 데이터베이스 작업을 관리합니다. (일봉 데이터 관리 기능 포함)"""

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        """DB 테이블들을 생성합니다."""
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS stocks (ticker TEXT PRIMARY KEY, name TEXT)"
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS financial_info (
                ticker TEXT, business_year INTEGER,
                roe REAL, ev_ebitda REAL,
                mac REAL, net_income REAL, total_equity REAL,
                operating_income REAL, depreciation REAL, amortization REAL,
                total_debt REAL, cash_and_equivalents REAL,
                PRIMARY KEY (ticker, business_year)
            )"""
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_charts (
                ticker TEXT, date TEXT, open REAL, high REAL,
                low REAL, close REAL, volume INTEGER,
                PRIMARY KEY (ticker, date)
            )"""
        )
        self.conn.commit()

    def get_all_tickers(self):
        return self.cursor.execute("SELECT ticker, name FROM stocks").fetchall()

    def update_tickers(self, tickers):
        self.cursor.executemany(
            "INSERT OR IGNORE INTO stocks (ticker, name) VALUES (?, ?)", tickers
        )
        self.conn.commit()
        logging.info(f"{len(tickers)}개 종목 정보를 DB에 업데이트했습니다.")

    def update_financial_info(self, data):
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO financial_info (
                ticker, business_year, roe, ev_ebitda, mac, net_income, total_equity,
                operating_income, depreciation, amortization, total_debt, cash_and_equivalents
            ) VALUES (
                :ticker, :business_year, :roe, :ev_ebitda, :mac, :net_income, :total_equity,
                :operating_income, :depreciation, :amortization, :total_debt, :cash_and_equivalents
            )""",
            data,
        )

    def get_latest_chart_date(self, ticker):
        """DB에 저장된 특정 종목의 가장 마지막 일봉 날짜를 반환합니다."""
        self.cursor.execute(
            "SELECT MAX(date) FROM daily_charts WHERE ticker=?", (ticker,)
        )
        result = self.cursor.fetchone()
        return result[0] if result and result[0] else None

    def update_daily_charts(self, chart_data):
        """일봉 데이터 리스트를 DB에 저장합니다."""
        self.cursor.executemany(
            """
            INSERT OR IGNORE INTO daily_charts (ticker, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            chart_data,
        )

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


# --- 6. 계산 로직 함수 ---
def calculate_metrics(kiwoom_info, dart_info):
    """수집한 정보로 ROE, EV/EBITDA를 계산합니다."""
    if not dart_info:
        return None, None

    # ROE 계산
    roe = None
    net_income = dart_info.get("net_income")
    total_equity = dart_info.get("total_equity")
    if net_income is not None and total_equity is not None and total_equity > 0:
        roe = (net_income / total_equity) * 100

    # EV/EBITDA 계산
    ev_ebitda = None
    if kiwoom_info:
        market_cap = kiwoom_info.get("mac")
        total_debt = dart_info.get("total_debt")
        cash = dart_info.get("cash_and_equivalents")
        operating_income = dart_info.get("operating_income")
        depreciation = dart_info.get("depreciation", 0)
        amortization = dart_info.get("amortization", 0)

        if all(v is not None for v in [market_cap, total_debt, cash, operating_income]):
            ev = market_cap + total_debt - cash
            ebitda = operating_income + depreciation + amortization
            if ev > 0 and ebitda > 0:
                ev_ebitda = ev / ebitda

    return roe, ev_ebitda


# --- 7. 메인 실행 로직 ---
def main():
    """메인 실행 함수: 재무 정보와 일봉 데이터를 모두 수집합니다."""
    db_manager = None
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config = ConfigManager()
        db_path = os.path.join(script_dir, "stocks.db")
        db_manager = DatabaseManager(db_path)
        dart_manager = DartManager(config.dart_api_key, script_dir)
        kiwoom_manager = KiwoomApiManager(config)

        if not kiwoom_manager.access_token:
            return

        # --- KOSPI 종목 목록 업데이트 ---
        logging.info("KOSPI 종목 목록을 API로부터 가져와 DB에 업데이트합니다...")
        tickers_from_api = kiwoom_manager.get_kospi_tickers()
        if tickers_from_api:
            ticker_data = [(t["code"], t["name"].strip()) for t in tickers_from_api]
            db_manager.update_tickers(ticker_data)

        tickers_from_db = db_manager.get_all_tickers()

        # --- 1. 과거 재무 정보 수집 ---
        logging.info(
            f"전체 {len(tickers_from_db)}개 종목의 과거 재무 정보 수집을 시작합니다 (시작 연도: {DATA_START_YEAR})..."
        )
        commit_interval = 50  # 50개 종목마다 중간 저장
        for i, (ticker, name) in enumerate(tqdm(tickers_from_db, desc="과거 재무 정보 수집")):
            kiwoom_info = kiwoom_manager.get_financial_info(ticker)
            for year in range(DATA_START_YEAR, datetime.now().year):
                dart_info = dart_manager.get_financial_info_for_year(ticker, year)
                if dart_info:
                    roe, ev_ebitda = calculate_metrics(kiwoom_info, dart_info)
                    db_data = {
                        "ticker": ticker,
                        "business_year": dart_info.get("business_year"),
                        "roe": roe,
                        "ev_ebitda": ev_ebitda,
                        "mac": kiwoom_info.get("mac") if kiwoom_info else None,
                        "net_income": dart_info.get("net_income"),
                        "total_equity": dart_info.get("total_equity"),
                        "operating_income": dart_info.get("operating_income"),
                        "depreciation": dart_info.get("depreciation"),
                        "amortization": dart_info.get("amortization"),
                        "total_debt": dart_info.get("total_debt"),
                        "cash_and_equivalents": dart_info.get("cash_and_equivalents"),
                    }
                    db_manager.update_financial_info(db_data)
            
            if (i + 1) % commit_interval == 0:
                db_manager.commit()
                logging.info(f"💾 재무 정보 중간 저장 ({i + 1}/{len(tickers_from_db)})")
        db_manager.commit()

        # --- 2. 과거 일봉 데이터 수집 ---
        logging.info(
            f"전체 {len(tickers_from_db)}개 종목의 일봉 데이터 수집을 시작합니다 (시작 날짜: {CHART_START_DATE})..."
        )
        today_str = datetime.now().strftime("%Y%m%d")
        for i, (ticker, name) in enumerate(tqdm(tickers_from_db, desc="과거 일봉 데이터 수집")):
            latest_date = db_manager.get_latest_chart_date(ticker)
            start_date = latest_date if latest_date else CHART_START_DATE

            # start_date가 None이 아니고, 오늘 날짜보다 크거나 같으면 건너뛰기
            if latest_date and datetime.strptime(
                latest_date, "%Y%m%d"
            ) >= datetime.now() - timedelta(days=1):
                continue

            # DB에 데이터가 있다면 다음 날부터 조회
            if latest_date:
                start_date = (
                    datetime.strptime(latest_date, "%Y%m%d") + timedelta(days=1)
                ).strftime("%Y%m%d")

            chart_data_from_api = kiwoom_manager.get_daily_chart_data(
                ticker, start_date, today_str
            )
            if chart_data_from_api:
                chart_data_to_db = [
                    (
                        ticker,
                        item["stk_dt"],
                        item["stk_oprc"],
                        item["stk_hgpr"],
                        item["stk_lwpr"],
                        item["stk_prpr"],
                        item["acml_vol"],
                    )
                    for item in chart_data_from_api
                ]
                db_manager.update_daily_charts(chart_data_to_db)

            # 주기적으로 DB에 커밋
            if (i + 1) % commit_interval == 0:
                db_manager.commit()
                logging.info(f"💾 일봉 정보 중간 저장 ({i + 1}/{len(tickers_from_db)})")
        db_manager.commit()  # 마지막으로 남은 데이터 커밋

        logging.info("\n✅ 모든 데이터 수집 작업이 완료되었습니다.")
    except Exception as e:
        logging.critical(f"💥 스크립트 실행 중 심각한 오류 발생: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()


if __name__ == "__main__":
    main()
