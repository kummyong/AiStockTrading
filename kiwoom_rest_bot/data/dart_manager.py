# kiwoom_rest_bot/data/dart_manager.py
import os
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import logging

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
