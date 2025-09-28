# manage_data.py (Refactored)

from datetime import datetime, timedelta
import logging
import os
from tqdm import tqdm

# kiwoom_rest_bot.data 패키지에서 클래스와 함수를 가져옵니다.
from kiwoom_rest_bot.data import (
    ConfigManager,
    DartManager,
    KiwoomApiManager,
    DatabaseManager,
    calculate_metrics, # calculate_metrics는 현재 main 함수에서 직접 사용되지 않으므로 나중에 제거할 수 있습니다.
)

# --- 1. 설정 ---
CHART_START_DATE = "20180101"  # 일봉 차트 데이터 수집 시작 날짜 (YYYYMMDD)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data_manager.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
    encoding="utf-8",
)


# --- 2. 메인 실행 로직 ---
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

        # --- 1. Kiwoom/DART 교집합 최종 대상 종목 선정 ---
        logging.info("Kiwoom/DART 교집합 종목을 선정합니다...")
        tickers_from_api = kiwoom_manager.get_kospi_tickers()

        if not tickers_from_api:
            logging.warning(
                "Kiwoom API로부터 종목 정보를 가져오지 못했습니다. 실행을 중단합니다."
            )
            return

        dart_ticker_set = set(dart_manager.corp_codes.keys())
        final_target_tickers = [
            (t["code"], t["name"].strip())
            for t in tickers_from_api
            if t["code"] in dart_ticker_set
        ]

        if not final_target_tickers:
            logging.warning(
                "Kiwoom과 DART의 교집합에 해당하는 종목이 없습니다. 실행을 중단합니다."
            )
            return

        logging.info(
            f"Kiwoom API와 DART의 교집합 결과, {len(final_target_tickers)}개 종목을 최종 대상으로 선정했습니다."
        )

        # --- 2. DB의 stocks 테이블 최신화 ---
        db_manager.clear_stocks_table()
        db_manager.update_tickers(final_target_tickers)

        # --- 3. 과거 재무 정보 수집 (DEBUG: SKIPPED) ---
        logging.info("--- DEBUG: SKIPPING FINANCIAL DATA COLLECTION ---")

        # --- 4. 과거 일봉 데이터 수집 ---
        logging.info("일봉 데이터를 업데이트할 대상 종목을 효율적으로 필터링합니다...")

        chart_update_targets = []
        last_recorded_day = db_manager.get_overall_latest_chart_date()

        if not last_recorded_day:
            logging.info(
                "DB에 일봉 데이터가 전혀 없으므로, 모든 최종 대상 종목의 데이터 수집을 시도합니다."
            )
            chart_update_targets = final_target_tickers
        else:
            logging.info(
                f"DB의 최신 날짜({last_recorded_day})를 기준으로 업데이트가 필요한 종목을 확인합니다."
            )
            up_to_date_tickers = db_manager.get_tickers_for_date(last_recorded_day)
            all_target_tickers_set = {ticker for ticker, name in final_target_tickers}
            tickers_to_update_set = all_target_tickers_set - up_to_date_tickers

            if not tickers_to_update_set:
                logging.info("✅ 모든 종목의 일봉 데이터가 최신입니다. 수집을 건너뜁니다.")
                chart_update_targets = []
            else:
                ticker_map = {ticker: name for ticker, name in final_target_tickers}
                chart_update_targets = [
                    (ticker, ticker_map[ticker]) for ticker in tickers_to_update_set
                ]

        if not chart_update_targets:
            logging.info("최종적으로 업데이트할 종목이 없습니다.")
        else:
            logging.info(
                f"총 {len(final_target_tickers)}개 중 {len(chart_update_targets)}개 종목의 일봉 데이터 수집을 시작합니다..."
            )
            ticker_counter = 0
            for ticker, name in tqdm(chart_update_targets, desc="과거 일봉 데이터 수집"):
                current_base_date_str = datetime.now().strftime("%Y%m%d")

                while True:
                    chart_data_from_api = kiwoom_manager.get_daily_chart_data(
                        ticker, current_base_date_str
                    )

                    if not chart_data_from_api:
                        break

                    try:
                        chart_data_to_db = []
                        for item in chart_data_from_api:
                            if not item.get("dt"):
                                logging.warning(
                                    f"[{ticker}] 날짜가 없는 일봉 데이터 항목을 건너뜁니다: {item}"
                                )
                                continue

                            chart_data_to_db.append((
                                ticker,
                                item["dt"],
                                float(item.get("open_pric") or 0),
                                float(item.get("high_pric") or 0),
                                float(item.get("low_pric") or 0),
                                float(item.get("cur_prc") or 0),
                                int(item.get("trde_qty") or 0),
                            ))
                        
                        if chart_data_to_db:
                            db_manager.update_daily_charts(chart_data_to_db)
                            
                    except (ValueError, KeyError) as e:
                        logging.error(
                            f"❌ [{ticker}] 일봉 데이터 처리 중 예기치 않은 오류 발생: {e}, 데이터: {item}"
                        )

                    oldest_date_str = chart_data_from_api[-1]["dt"]
                    if oldest_date_str < CHART_START_DATE:
                        break

                    current_base_date = datetime.strptime(
                        oldest_date_str, "%Y%m%d"
                    ) - timedelta(days=1)
                    current_base_date_str = current_base_date.strftime("%Y%m%d")

                ticker_counter += 1
                if ticker_counter % 50 == 0:
                    logging.info(f"✅ {ticker_counter}개 종목 데이터 처리 후 DB에 커밋합니다.")
                    db_manager.commit()

            logging.info("✅ 모든 종목 데이터 수집 완료 후 최종 커밋을 진행합니다.")
            db_manager.commit()
    except Exception as e:
        logging.critical(f"💥 스크립트 실행 중 심각한 오류 발생: {e}", exc_info=True)
    finally:
        if db_manager:
            db_manager.close()


if __name__ == "__main__":  # pragma: no cover
    main()