# kiwoom_rest_bot/data/config_manager.py
import os
import configparser
import logging

class ConfigManager:
    """config.ini 파일에서 실서버용 설정을 읽어 관리합니다."""

    def __init__(self, config_file="config.ini"):
        if not os.path.isabs(config_file):
            # Assuming the config file is in the parent directory of the 'data' directory
            script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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
