import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import sqlite3
import configparser
import requests
from datetime import datetime, timedelta

# 테스트 대상 모듈 임포트
from kiwoom_rest_bot.data import (
    ConfigManager,
    DartManager,
    KiwoomApiManager,
    DatabaseManager,
    calculate_metrics,
)

class TestConfigManager(unittest.TestCase):
    """ConfigManager 클래스 테스트"""

    def setUp(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_file = os.path.join(self.script_dir, "temp_config_for_test.ini")

    def tearDown(self):
        if os.path.exists(self.config_file):
            os.remove(self.config_file)

    def test_config_loading_success(self):
        """설정 파일 로드 성공 테스트"""
        config = configparser.ConfigParser()
        config["KIWOOM_REAL"] = {
            "base_url": "https://test.com",
            "app_key": "test_key",
            "app_secret": "test_secret",
            "account_no": "12345",
        }
        config["DART"] = {"api_key": "dart_key"}
        with open(self.config_file, "w", encoding="utf-8") as f:
            config.write(f)

        cm = ConfigManager(self.config_file)
        self.assertEqual(cm.base_url, "https://test.com")
        self.assertEqual(cm.kiwoom_app_key, "test_key")
        self.assertEqual(cm.dart_api_key, "dart_key")

    def test_config_file_not_found(self):
        """설정 파일이 없을 때 FileNotFoundError 발생 테스트"""
        with self.assertRaises(FileNotFoundError):
            ConfigManager("non_existent_file.ini")


class TestDatabaseManager(unittest.TestCase):
    """DatabaseManager 클래스 테스트 (인메모리 DB 사용)"""

    def setUp(self):
        self.db = DatabaseManager(":memory:")
        self.db.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        
    def tearDown(self):
        self.db.close()

    def test_create_tables(self):
        """테이블 생성 확인 테스트"""
        tables = [row[0] for row in self.db.cursor.fetchall()]
        self.assertIn("stocks", tables)
        self.assertIn("financial_info", tables)
        self.assertIn("daily_charts", tables)

    def test_update_and_get_tickers(self):
        """종목 정보 업데이트 및 조회 테스트"""
        tickers = [("005930", "삼성전자"), ("000660", "SK하이닉스")]
        self.db.update_tickers(tickers)
        
        retrieved_tickers = self.db.get_all_tickers()
        self.assertEqual(len(retrieved_tickers), 2)
        self.assertIn(("005930", "삼성전자"), retrieved_tickers)

    def test_update_and_get_financial_info(self):
        """재무 정보 업데이트 및 조회 테스트"""
        data = {
            "ticker": "005930", "business_year": 2023, "roe": 10.5, "ev_ebitda": 8.2,
            "mac": 500000000000000, "net_income": 50000000000000, "total_equity": 400000000000000,
            "operating_income": 60000000000000, "depreciation": 5000000000000, "amortization": 100000000000,
            "total_debt": 100000000000000, "cash_and_equivalents": 20000000000000
        }
        self.db.update_financial_info(data)
        
        years = self.db.get_existing_financial_years("005930")
        self.assertEqual(years, {2023})

    def test_update_and_get_daily_charts(self):
        """일봉 차트 업데이트 및 조회 테스트"""
        chart_data = [("005930", "20230101", 50000, 51000, 49000, 50500, 1000000)]
        self.db.update_daily_charts(chart_data)

        latest_date = self.db.get_latest_chart_date("005930")
        self.assertEqual(latest_date, "20230101")

    def test_get_overall_latest_chart_date(self):
        """전체 차트 데이터 중 최신 날짜 조회 테스트"""
        self.assertIsNone(self.db.get_overall_latest_chart_date()) # 데이터가 없을 때
        self.db.update_daily_charts([("005930", "20230102", 1,1,1,1,1)])
        self.db.update_daily_charts([("000660", "20230103", 1,1,1,1,1)])
        self.db.update_daily_charts([("005380", "20230101", 1,1,1,1,1)])
        self.assertEqual(self.db.get_overall_latest_chart_date(), "20230103")

    def test_get_tickers_for_date(self):
        """특정 날짜의 종목 조회 테스트"""
        self.assertEqual(self.db.get_tickers_for_date("20230102"), set()) # 데이터가 없을 때
        self.db.update_daily_charts([("005930", "20230102", 1,1,1,1,1)])
        self.db.update_daily_charts([("000660", "20230103", 1,1,1,1,1)])
        self.db.update_daily_charts([("005380", "20230102", 1,1,1,1,1)])
        self.assertEqual(self.db.get_tickers_for_date("20230102"), {"005930", "005380"})
        self.assertEqual(self.db.get_tickers_for_date("20230103"), {"000660"})


class TestCalculateMetrics(unittest.TestCase):
    """calculate_metrics 함수 테스트"""

    def test_calculate_metrics_success(self):
        """정상적인 경우 계산 성공 테스트"""
        kiwoom_info = {"mac": 1000}
        dart_info = {
            "net_income": 100, "total_equity": 500, "total_debt": 200,
            "cash_and_equivalents": 50, "operating_income": 150,
            "depreciation": 20, "amortization": 5
        }
        roe, ev_ebitda = calculate_metrics(kiwoom_info, dart_info)
        self.assertAlmostEqual(roe, 20.0) # (100 / 500) * 100
        self.assertAlmostEqual(ev_ebitda, (1000 + 200 - 50) / (150 + 20 + 5)) # 1150 / 175

    def test_calculate_metrics_missing_data(self):
        """필수 데이터가 없을 때 None 반환 테스트"""
        roe, ev_ebitda = calculate_metrics({}, {})
        self.assertIsNone(roe)
        self.assertIsNone(ev_ebitda)

        roe, ev_ebitda = calculate_metrics(None, None)
        self.assertIsNone(roe)
        self.assertIsNone(ev_ebitda)

    def test_calculate_metrics_division_by_zero(self):
        """0으로 나누는 경우 테스트"""
        kiwoom_info = {"mac": 1000}
        dart_info_zero_equity = {"net_income": 100, "total_equity": 0}
        roe, _ = calculate_metrics(kiwoom_info, dart_info_zero_equity)
        self.assertIsNone(roe)

        dart_info_zero_ebitda = {
            "total_debt": 200, "cash_and_equivalents": 50, "operating_income": -25,
            "depreciation": 20, "amortization": 5
        }
        _, ev_ebitda = calculate_metrics(kiwoom_info, dart_info_zero_ebitda)
        self.assertIsNone(ev_ebitda)

    def test_calculate_metrics_no_dart_info(self):
        """계산 함수에 dart_info가 없을 경우 테스트"""
        roe, ev_ebitda = calculate_metrics({"mac": 1000}, None)
        self.assertIsNone(roe)
        self.assertIsNone(ev_ebitda)


class TestApiManagers(unittest.TestCase):
    """DartManager와 KiwoomApiManager 테스트"""

    def setUp(self):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(self.script_dir, "test_config.ini")
        self.config = ConfigManager(config_path)
        self.script_dir = os.path.dirname(os.path.abspath(__file__))

    @patch("requests.get")
    def test_dart_get_financial_info(self, mock_get):
        """DartManager 재무 정보 조회 테스트"""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": "000",
            "list": [
                {"account_nm": "당기순이익", "thstrm_amount": "1,000"},
                {"account_nm": "자본총계", "thstrm_amount": "10,000"},
            ]
        }
        mock_get.return_value = mock_response
        
        mock_xml_content = b'<result><list><stock_code>005930</stock_code><corp_code>00126380</corp_code></list></result>'
        with patch("os.path.exists") as mock_exists, patch("builtins.open", mock_open(read_data=mock_xml_content)):
            mock_exists.return_value = True
            dart_manager = DartManager(self.config.dart_api_key, self.script_dir)
        
        info = dart_manager.get_financial_info_for_year("005930", 2023)
        
        self.assertIsNotNone(info)
        self.assertEqual(info["net_income"], 1000.0)
        self.assertEqual(info["total_equity"], 10000.0)
        self.assertEqual(info["business_year"], 2023)

    @patch("requests.post")
    def test_kiwoom_get_access_token(self, mock_post):
        """KiwoomApiManager 토큰 발급 테스트"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"token": "fake_token"}
        mock_post.return_value = mock_response

        kiwoom_manager = KiwoomApiManager(self.config)
        self.assertEqual(kiwoom_manager.access_token, "fake_token")

    @patch("requests.get", side_effect=requests.exceptions.RequestException("API Error"))
    def test_kiwoom_api_request_exception(self, mock_get):
        """KiwoomApiManager API 요청 실패 테스트"""
        with patch.object(KiwoomApiManager, "_get_access_token", return_value="fake_token"):
            kiwoom_manager = KiwoomApiManager(self.config)
            # get_kospi_tickers는 내부적으로 GET을 사용하지 않으므로 다른 메서드로 테스트
            with patch.object(kiwoom_manager, "_request_api") as mock_req:
                mock_req.return_value = None
                result = kiwoom_manager.get_kospi_tickers()
                self.assertEqual(result, [])

    @patch("zipfile.ZipFile")
    @patch("requests.get")
    @patch("os.path.exists")
    def test_dart_load_corp_codes_download(self, mock_exists, mock_get, mock_zipfile):
        """DartManager CORPCODE.xml 자동 다운로드 테스트"""
        mock_exists.return_value = False # 파일이 없는 상황
        mock_response = MagicMock()
        mock_response.content = b"zip_content"
        mock_get.return_value = mock_response
        
        mock_zip_instance = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_instance

        dart_manager = DartManager(self.config.dart_api_key, self.script_dir)
        mock_zip_instance.extractall.assert_called_with(self.script_dir)

    @patch("requests.get", side_effect=requests.exceptions.RequestException("Download Failed"))
    @patch("os.path.exists", return_value=False)
    def test_dart_download_fail(self, mock_exists, mock_get):
        dart_manager = DartManager(self.config.dart_api_key, self.script_dir)
        self.assertEqual(dart_manager.corp_codes, {})

    @patch("xml.etree.ElementTree.parse", side_effect=Exception("XML Parse Error"))
    def test_dart_load_corp_codes_parse_fail(self, mock_parse):
        """CORPCODE.xml 파싱 실패 시 예외 처리 테스트"""
        with patch("os.path.exists", return_value=True), patch("builtins.open", mock_open(read_data=b'malformed xml')):
            dart_manager = DartManager(self.config.dart_api_key, self.script_dir)
            self.assertEqual(dart_manager.corp_codes, {})

    @patch("requests.get")
    def test_dart_info_fallback(self, mock_get):
        """DART API 연결재무제표(CFS) 실패 후 일반재무제표(OFS) 조회 테스트"""
        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: {"status": "013", "message": "조회된 데이터가 없습니다."}),
            MagicMock(status_code=200, json=lambda: {"status": "000", "list": [
                {"account_nm": "당기순이익", "thstrm_amount": "100"},
                {"account_nm": "자본총계", "thstrm_amount": "1000"}
            ]})
        ]
        with patch("os.path.exists", return_value=True), patch("builtins.open", mock_open(read_data=b'<result><list><stock_code>005930</stock_code><corp_code>00126380</corp_code></list></result>')):
            dart_manager = DartManager(self.config.dart_api_key, self.script_dir)
            info = dart_manager.get_financial_info_for_year("005930", 2023)
            self.assertIsNotNone(info)
            self.assertEqual(mock_get.call_count, 2)

    @patch("requests.get")
    def test_dart_api_error(self, mock_get):
        """DART API에서 status가 000이 아닐 때 테스트"""
        mock_get.return_value = MagicMock(status_code=200, json=lambda: {"status": "900", "message": "API Key Error"})
        with patch("os.path.exists", return_value=True), patch("builtins.open", mock_open(read_data=b'<result><list><stock_code>005930</stock_code><corp_code>00126380</corp_code></list></result>')):
            dart_manager = DartManager(self.config.dart_api_key, self.script_dir)
            info = dart_manager.get_financial_info_for_year("005930", 2023)
            self.assertIsNone(info)

    def test_dart_corp_code_not_found(self):
        """DART 고유번호가 없는 종목 조회 시 None 반환 테스트"""
        with patch("os.path.exists", return_value=True), patch("builtins.open", mock_open(read_data=b'<result><list><stock_code>005930</stock_code><corp_code>00126380</corp_code></list></result>')):
            dart_manager = DartManager(self.config.dart_api_key, self.script_dir)
            info = dart_manager.get_financial_info_for_year("999999", 2023) # 없는 종목 코드
            self.assertIsNone(info)

    @patch("requests.get", side_effect=requests.exceptions.RequestException("API Error"))
    def test_dart_api_request_exception(self, mock_get):
        """DART API 요청 실패 테스트"""
        with patch("os.path.exists", return_value=True), patch("builtins.open", mock_open(read_data=b'<result><list><stock_code>005930</stock_code><corp_code>00126380</corp_code></list></result>')):
            dart_manager = DartManager(self.config.dart_api_key, self.script_dir)
            info = dart_manager.get_financial_info_for_year("005930", 2023)
            self.assertIsNone(info)

    @patch("requests.post")
    def test_kiwoom_token_fail(self, mock_post):
        mock_post.return_value.json.return_value = {"error": "failed"} # no token
        kiwoom_manager = KiwoomApiManager(self.config)
        self.assertIsNone(kiwoom_manager.access_token)

    @patch.object(KiwoomApiManager, "_get_access_token", return_value=None)
    def test_kiwoom_no_token_for_methods(self, mock_token):
        kiwoom_manager = KiwoomApiManager(self.config)
        self.assertIsNone(kiwoom_manager.get_financial_info("005930"))
        self.assertEqual(kiwoom_manager.get_daily_chart_data("005930", ""), [])

    @patch.object(KiwoomApiManager, "_get_access_token", return_value="fake_token")
    def test_kiwoom_get_financial_info_success(self, mock_token):
        """get_financial_info 성공 경로 직접 테스트"""
        kiwoom_manager = KiwoomApiManager(self.config)
        kiwoom_manager._request_api = MagicMock(return_value={"mac": "12345"})
        result = kiwoom_manager.get_financial_info("005930")
        self.assertEqual(result, {"mac": 1234500000000.0})

    @patch.object(KiwoomApiManager, "_get_access_token", return_value="fake_token")
    def test_kiwoom_bad_financial_info(self, mock_token):
        """get_financial_info가 숫자가 아닌 값을 반환할 때 테스트"""
        kiwoom_manager = KiwoomApiManager(self.config)
        kiwoom_manager._request_api = MagicMock(return_value={"mac": "not-a-number"})
        self.assertIsNone(kiwoom_manager.get_financial_info("005930"))

    @patch.object(KiwoomApiManager, "_get_access_token", return_value="fake_token")
    def test_get_daily_chart_data_api_failure(self, mock_token):
        """get_daily_chart_data API 실패 직접 테스트"""
        kiwoom_manager = KiwoomApiManager(self.config)
        kiwoom_manager._request_api = MagicMock(return_value=None)
        result = kiwoom_manager.get_daily_chart_data("005930", "20230101")
        self.assertEqual(result, [])

class TestMainFunction(unittest.TestCase):
    """main 함수 테스트"""

    @patch("kiwoom_rest_bot.manage_data.ConfigManager")
    @patch("kiwoom_rest_bot.manage_data.DatabaseManager")
    @patch("kiwoom_rest_bot.manage_data.DartManager")
    @patch("kiwoom_rest_bot.manage_data.KiwoomApiManager")
    def test_main_no_token(self, mock_kiwoom_cls, mock_dart_cls, mock_db_cls, mock_config_cls):
        """Kiwoom 토큰 발급 실패 시 main 함수 조기 종료 테스트"""
        mock_kiwoom = mock_kiwoom_cls.return_value
        mock_kiwoom.access_token = None # 토큰 없음

        from kiwoom_rest_bot.manage_data import main
        main()

        mock_kiwoom.get_kospi_tickers.assert_not_called()

    @patch("kiwoom_rest_bot.manage_data.ConfigManager", side_effect=Exception("CRITICAL ERROR"))
    def test_main_critical_exception(self, mock_config):
        """main 함수에서 심각한 예외 발생 시 최종 except 블록 테스트"""
        from kiwoom_rest_bot.manage_data import main
        with self.assertLogs('root', level='CRITICAL') as cm:
            main()
            self.assertIn("CRITICAL ERROR", cm.output[0])

    @patch("kiwoom_rest_bot.manage_data.ConfigManager")
    @patch("kiwoom_rest_bot.manage_data.DatabaseManager")
    @patch("kiwoom_rest_bot.manage_data.DartManager")
    @patch("kiwoom_rest_bot.manage_data.KiwoomApiManager")
    def test_main_intersection_and_flow(self, mock_kiwoom_cls, mock_dart_cls, mock_db_cls, mock_config_cls):
        """main 함수의 교집합 로직 및 전체 흐름 테스트"""
        mock_kiwoom = mock_kiwoom_cls.return_value
        mock_kiwoom.access_token = "fake_token"
        mock_kiwoom.get_kospi_tickers.return_value = [
            {"code": "005930", "name": "삼성전자"},  # 교집합 대상
            {"code": "999999", "name": "키움전용"}
        ]
        mock_kiwoom.get_financial_info.return_value = {"mac": 1000}
        mock_kiwoom.get_daily_chart_data.return_value = [{"dt": "20230102", "open_pric": "1", "high_pric": "1", "low_pric": "1", "cur_prc": "1", "trde_qty": "1"}]

        mock_dart = mock_dart_cls.return_value
        mock_dart.corp_codes = {"005930": "SAMSUNG_DART_CODE"}
        mock_dart.get_financial_info_for_year.return_value = {"business_year": 2023, "net_income": 100, "total_equity": 500}

        mock_db = mock_db_cls.return_value
        mock_db.get_overall_latest_chart_date.return_value = "20220101"
        mock_db.get_tickers_for_date.return_value = set()

        from kiwoom_rest_bot.manage_data import main
        with patch("kiwoom_rest_bot.manage_data.tqdm", lambda x, **kwargs: x):
            with patch("kiwoom_rest_bot.manage_data.CHART_START_DATE", "20230101"):
                main()

        # 교집합 결과인 1개 종목만으로 update_tickers가 호출되었는지 확인
        mock_db.update_tickers.assert_called_once_with([("005930", "삼성전자")])

        # 최종적으로 필터링된 1개 종목에 대해서만 차트 수집이 호출되었는지 확인
        mock_kiwoom.get_daily_chart_data.assert_called_once()


    @patch("kiwoom_rest_bot.manage_data.ConfigManager")
    @patch("kiwoom_rest_bot.manage_data.DatabaseManager")
    @patch("kiwoom_rest_bot.manage_data.DartManager")
    @patch("kiwoom_rest_bot.manage_data.KiwoomApiManager")
    def test_main_chart_data_handles_empty_numeric(self, mock_kiwoom_cls, mock_dart_cls, mock_db_cls, mock_config_cls):
        """일봉 데이터의 숫자 필드가 비어있을 때 0으로 처리하는지 테스트"""
        mock_kiwoom = mock_kiwoom_cls.return_value
        mock_kiwoom.access_token = "fake_token"
        mock_kiwoom.get_kospi_tickers.return_value = [{"code": "005930", "name": "삼성전자"}]
        
        # 숫자 필드가 비어있는 데이터 모의
        item_with_empty_numerics = {
            "dt": "20230102", "open_pric": "", "high_pric": "",
            "low_pric": "", "cur_prc": "", "trde_qty": ""
        }
        mock_kiwoom.get_daily_chart_data.return_value = [item_with_empty_numerics]

        mock_dart = mock_dart_cls.return_value
        mock_dart.corp_codes = {"005930": "SAMSUNG_DART_CODE"}

        mock_db = mock_db_cls.return_value
        mock_db.get_overall_latest_chart_date.return_value = "20220101"

        from kiwoom_rest_bot.manage_data import main
        with patch("kiwoom_rest_bot.manage_data.tqdm", lambda x, **kwargs: x):
            main()

        # 에러가 발생하지 않고, update_daily_charts가 호출되어야 함
        mock_db.update_daily_charts.assert_called_once()
        # 전달된 데이터의 숫자 값들이 0으로 채워졌는지 확인
        call_args = mock_db.update_daily_charts.call_args[0][0]
        self.assertEqual(len(call_args), 1)
        self.assertEqual(call_args[0], ("005930", "20230102", 0.0, 0.0, 0.0, 0.0, 0))

    @patch("kiwoom_rest_bot.manage_data.ConfigManager")
    @patch("kiwoom_rest_bot.manage_data.DatabaseManager")
    @patch("kiwoom_rest_bot.manage_data.DartManager")
    @patch("kiwoom_rest_bot.manage_data.KiwoomApiManager")
    def test_main_chart_data_skips_empty_date(self, mock_kiwoom_cls, mock_dart_cls, mock_db_cls, mock_config_cls):
        """일봉 데이터의 날짜 필드가 비어있을 때 건너뛰는지 테스트"""
        mock_kiwoom = mock_kiwoom_cls.return_value
        mock_kiwoom.access_token = "fake_token"
        mock_kiwoom.get_kospi_tickers.return_value = [{"code": "005930", "name": "삼성전자"}]
        
        # 날짜 필드가 비어있는 데이터 모의
        item_with_empty_date = {
            "dt": "", "open_pric": "1", "high_pric": "1",
            "low_pric": "1", "cur_prc": "1", "trde_qty": "1"
        }
        mock_kiwoom.get_daily_chart_data.return_value = [item_with_empty_date]

        mock_dart = mock_dart_cls.return_value
        mock_dart.corp_codes = {"005930": "SAMSUNG_DART_CODE"}

        mock_db = mock_db_cls.return_value
        mock_db.get_overall_latest_chart_date.return_value = "20220101"

        from kiwoom_rest_bot.manage_data import main
        with self.assertLogs("root", level="WARNING") as cm:
            with patch("kiwoom_rest_bot.manage_data.tqdm", lambda x, **kwargs: x):
                main()
            # 경고 로그가 정상적으로 기록되었는지 확인
            self.assertIn("날짜가 없는 일봉 데이터 항목을 건너뜁니다", cm.output[0])

        # 데이터가 건너뛰어졌으므로 update_daily_charts는 호출되지 않아야 함
        mock_db.update_daily_charts.assert_not_called()

    @patch("kiwoom_rest_bot.manage_data.ConfigManager")
    @patch("kiwoom_rest_bot.manage_data.DatabaseManager")
    @patch("kiwoom_rest_bot.manage_data.DartManager")
    @patch("kiwoom_rest_bot.manage_data.KiwoomApiManager")
    def test_main_filter_and_batch_commit(self, mock_kiwoom_cls, mock_dart_cls, mock_db_cls, mock_config_cls):
        """
        main 함수에서 최신이 아닌 종목만 필터링하고,
        50개 단위로 배치 커밋하는지 테스트
        """
        mock_kiwoom = mock_kiwoom_cls.return_value
        mock_kiwoom.access_token = "fake_token"

        # 70개의 모의 종목 데이터 생성
        tickers = [{"code": f"{i:06d}", "name": f"Stock-{i}"} for i in range(70)]
        mock_kiwoom.get_kospi_tickers.return_value = tickers

        mock_dart = mock_dart_cls.return_value
        mock_dart.corp_codes = {t["code"]: f"DART-{t['code']}" for t in tickers}

        # get_daily_chart_data가 첫 호출에는 데이터, 두 번째 호출에는 빈 리스트를 반환
        chart_data_sample = [{"dt": "20230102", "open_pric": "1", "high_pric": "1", "low_pric": "1", "cur_prc": "1", "trde_qty": "1"}]
        call_counts = {}
        def get_daily_chart_side_effect(ticker, base_date):
            count = call_counts.get(ticker, 0)
            call_counts[ticker] = count + 1
            if count == 0:
                return chart_data_sample
            return []
        mock_kiwoom.get_daily_chart_data.side_effect = get_daily_chart_side_effect

        mock_db = mock_db_cls.return_value
        
        # DB의 최신 날짜는 20230302라고 가정
        LATEST_DAY_IN_DB = "20230302"
        mock_db.get_overall_latest_chart_date.return_value = LATEST_DAY_IN_DB
        
        # 70개 종목 중 10개는 이미 최신 날짜 데이터를 가지고 있다고 가정
        up_to_date_tickers = {f"{i:06d}" for i in range(10)}
        mock_db.get_tickers_for_date.return_value = up_to_date_tickers

        from kiwoom_rest_bot.manage_data import main
        
        with patch("kiwoom_rest_bot.manage_data.CHART_START_DATE", "20230101"):
            with patch("kiwoom_rest_bot.manage_data.tqdm", lambda x, **kwargs: x):
                main()

        # 70개 중 10개가 최신이므로 60개만 업데이트 대상
        # 60개 종목, 배치 크기 50 -> commit은 2번 호출 (50개, 10개)
        self.assertEqual(mock_db.commit.call_count, 2)
        # get_daily_chart_data는 60번만 호출되어야 함
        self.assertEqual(mock_kiwoom.get_daily_chart_data.call_count, 60)
        # get_tickers_for_date는 최신 날짜로 정확히 1번 호출되어야 함
        mock_db.get_tickers_for_date.assert_called_once_with(LATEST_DAY_IN_DB)


if __name__ == "__main__":
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
