import requests
import sqlite3
from datetime import datetime
import time
import os
import zipfile
import io
import xml.etree.ElementTree as ET
import configparser
import logging
from tqdm import tqdm

# --- 1. 로깅 설정 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_manager.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)


# --- 2. 설정 로드 클래스 (실서버 전용) ---
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

        # [KIWOOM_REAL] 섹션의 설정 값을 직접 로드
        self.base_url = config.get("KIWOOM_REAL", "base_url").strip("'\"")
        self.kiwoom_app_key = config.get("KIWOOM_REAL", "app_key").strip("'\"")
        self.kiwoom_app_secret = config.get("KIWOOM_REAL", "app_secret").strip("'\"")
        self.account_no = config.get("KIWOOM_REAL", "account_no").strip("'\"")
        self.dart_api_key = config.get("DART", "api_key").strip("'\"")
        logging.info("✅ 실서버용 설정을 성공적으로 불러왔습니다.")


# --- 3. DART API 관리 클래스 (이전과 동일) ---
class DartManager:
    # (이전 답변의 DartManager 클래스 코드와 동일)
    """DART API 관련 기능을 관리합니다."""

    def __init__(self, api_key, script_dir):
        self.api_key = api_key
        self.script_dir = script_dir
        self.corp_codes = self._load_corp_codes()

    def _load_corp_codes(self):
        """DART 고유번호 XML 파일을 로드하거나 다운로드합니다."""
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

                # 응답이 zip 파일인지 확인
                content_type = res.headers.get("Content-Type", "")
                is_zip = (
                    "application/zip" in content_type
                    or "application/x-msdownload" in content_type
                )
                if not is_zip:
                    logging.error(
                        f"❌ DART API에서 zip 파일을 반환하지 않았습니다. API 키가 유효한지 확인해주세요."
                    )
                    logging.error(
                        f"응답 내용: {res.text[:200]}"
                    )  # 응답 내용 일부를 로깅
                    return {}

                with zipfile.ZipFile(io.BytesIO(res.content)) as zfile:
                    zfile.extractall(self.script_dir)
                logging.info("✅ 'CORPCODE.xml' 파일 다운로드 및 압축 해제 성공!")
            except Exception as e:
                logging.error(f"❌ 'CORPCODE.xml' 파일 다운로드 실패: {e}")
                return {}

        logging.info("DART 고유번호 파일(CORPCODE.xml)을 로드합니다...")
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

    def get_dart_code(self, ticker: str):
        return self.corp_codes.get(ticker)

    def get_financial_info(self, ticker: str):
        """DART API를 통해 최신 재무 정보를 가져옵니다."""
        dart_code = self.get_dart_code(ticker)
        if not dart_code:
            logging.debug(
                f"재무(DART): [{ticker}] DART 고유번호 없음 (ETF 등으로 추정)"
            )
            return None

        current_year = datetime.now().year
        for year in [current_year - 1, current_year - 2]:
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
                    params["fs_div"] = "OFS"
                    res = requests.get(url, params=params, timeout=5)
                    data = res.json()
                    if data.get("status") != "000":
                        continue

                net_income, total_equity = None, None
                for item in data.get("list", []):
                    account_name = item.get("account_nm", "")
                    if "당기순이익" in account_name:
                        amount_str = item.get("thstrm_amount", "0").replace(",", "")
                        net_income = int(amount_str) if amount_str else 0
                    if account_name == "자본총계":
                        amount_str = item.get("thstrm_amount", "0").replace(",", "")
                        total_equity = int(amount_str) if amount_str else 0

                if net_income is not None and total_equity not in [None, 0]:
                    roe = (net_income / total_equity) * 100
                    return {"roe": roe, "business_year": year}

            except Exception as e:
                logging.warning(
                    f"재무(DART): [{ticker}] {year}년 정보 조회 중 오류: {e}"
                )
                continue
        return None


# --- 4. 키움 API 관리 클래스 (이전과 동일) ---
class KiwoomApiManager:
    # (이전 답변의 KiwoomApiManager 클래스 코드와 동일)
    """키움증권 API 요청을 관리합니다."""

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
        """키움증권 API를 통해 PER, PBR 등의 재무 정보를 가져옵니다."""
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

        if res_json and res_json.get("per"):
            try:
                return {
                    "per": float(res_json.get("per", 0)),
                    "pbr": float(res_json.get("pbr", 0)),
                    "bps": int(res_json.get("bps", 0)),
                }
            except (ValueError, TypeError):
                logging.warning(f"재무(키움): [{ticker}] 데이터 변환 중 오류 발생")
                return None
        return None

    def _request_api(self, path, headers=None, body=None, params=None):
        url = f"{self.config.base_url}{path}"
        try:
            if body:  # POST 요청
                res = requests.post(url, headers=headers, json=body, timeout=10)
            else:  # GET 요청
                res = requests.get(url, headers=headers, params=params, timeout=10)
            time.sleep(1)
            res.raise_for_status()
            return res.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"API 요청 실패: {url} - {e}")
            return None


# --- 5. 데이터베이스 관리 클래스 (이전과 동일) ---
class DatabaseManager:
    # (이전 답변의 DatabaseManager 클래스 코드와 동일)
    """SQLite 데이터베이스 작업을 관리합니다."""

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS stocks (ticker TEXT PRIMARY KEY, name TEXT)"
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS financial_info (
                ticker TEXT PRIMARY KEY, business_year INTEGER,
                per REAL, pbr REAL, roe REAL, ev_ebitda REAL, bps INTEGER
            )"""
        )
        self.conn.commit()

    def get_all_tickers(self):
        return self.cursor.execute("SELECT ticker, name FROM stocks").fetchall()

    def get_latest_financial_year(self, ticker: str):
        self.cursor.execute(
            "SELECT business_year FROM financial_info WHERE ticker=?", (ticker,)
        )
        result = self.cursor.fetchone()
        return result[0] if result else None

    def update_tickers(self, tickers):
        self.cursor.executemany(
            "INSERT OR IGNORE INTO stocks (ticker, name) VALUES (?, ?)", tickers
        )
        self.conn.commit()
        logging.info(f"{len(tickers)}개 종목 정보를 DB에 업데이트했습니다.")

    def update_financial_info(self, data):
        self.cursor.execute(
            """INSERT OR REPLACE INTO financial_info (ticker, business_year, per, pbr, roe, ev_ebitda, bps)
               VALUES (:ticker, :business_year, :per, :pbr, :roe, :ev_ebitda, :bps)""",
            data,
        )

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


# --- 6. 메인 실행 로직 ---
def main():
    """메인 실행 함수"""
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config = ConfigManager()

        db_path = os.path.join(script_dir, "stocks.db")
        db_manager = DatabaseManager(db_path)
        dart_manager = DartManager(config.dart_api_key, script_dir)
        kiwoom_manager = KiwoomApiManager(config)

        if not kiwoom_manager.access_token:
            return

        logging.info("KOSPI 종목 목록을 실서버 API로부터 가져옵니다...")
        tickers_from_api = kiwoom_manager.get_kospi_tickers()
        if tickers_from_api:
            ticker_data = [(t["code"], t["name"].strip()) for t in tickers_from_api]
            db_manager.update_tickers(ticker_data)

        tickers_from_db = db_manager.get_all_tickers()
        target_year = datetime.now().year - 1

        for ticker, name in tqdm(tickers_from_db, desc="재무 정보 수집 중"):
            latest_year_in_db = db_manager.get_latest_financial_year(ticker)
            if latest_year_in_db and latest_year_in_db >= target_year:
                continue

            # 1. 키움 API에서 PER, PBR 등 수집
            fin_info = kiwoom_manager.get_financial_info(ticker)
            if not fin_info:
                fin_info = {}  # 기본 dict 생성

            # 2. DART API에서 ROE, 사업연도 수집
            dart_info = dart_manager.get_financial_info(ticker)
            if dart_info:
                fin_info.update(dart_info)  # 두 API의 결과를 합침

            # 3. 데이터가 하나라도 있으면 DB에 저장
            if "business_year" in fin_info:  # DART 정보가 있는 경우를 기준으로
                db_data = {
                    "ticker": ticker,
                    "business_year": fin_info.get("business_year"),
                    "per": fin_info.get("per"),
                    "pbr": fin_info.get("pbr"),
                    "roe": fin_info.get("roe"),
                    "ev_ebitda": None,
                    "bps": fin_info.get("bps"),
                }
                db_manager.update_financial_info(db_data)

        db_manager.commit()
        logging.info("\n✅ 모든 데이터 수집 작업이 완료되었습니다.")

    except Exception as e:
        logging.critical(f"💥 스크립트 실행 중 심각한 오류 발생: {e}", exc_info=True)
    finally:
        if "db_manager" in locals():
            db_manager.close()


if __name__ == "__main__":
    main()
