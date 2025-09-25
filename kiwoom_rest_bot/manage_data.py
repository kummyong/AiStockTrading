# kiwoom_rest_bot/manage_data.py
import requests
import json
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# --- ⚙️ 사용자 설정 ---
IS_MOCK = True
APP_KEY = "IWxXc-OxrNyAt3jBCkERK4EV7xbW6DYYHXqK3n0x57A"
APP_SECRET = "FBAOtvQj0MJBHOmx3s8UBIdH0XK399iHIudXbO2H2Vo"
ACCOUNT_NO = "81118476"

if IS_MOCK:
    BASE_URL = "https://mockapi.kiwoom.com"
else:
    BASE_URL = "https://api.kiwoom.com"
# --------------------


def _request_api(path: str, headers: dict = None, body: dict = None):
    """Kiwoom REST API 요청을 위한 범용 래퍼 함수"""
    URL = f"{BASE_URL}{path}"
    try:
        res = requests.post(URL, headers=headers, json=body)
        time.sleep(1)  # API 요청 후 1초 대기
        res.raise_for_status()
        return res.json()
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"API 요청 실패: {URL} - {e}")
        if 'res' in locals():
            print(f"응답 내용: {res.text}")
        return None

def get_access_token():
    """API 접근 토큰 발급"""
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "secretkey": APP_SECRET,
    }
    PATH = "/oauth2/token"
    response_json = _request_api(path=PATH, body=body)
    return response_json.get("token") if response_json else None


def get_kospi_tickers(token):
    """모의투자 KOSPI 전체 종목 코드를 받아오는 함수"""
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "ka10099",
    }
    body = {"mrkt_tp": "0"}  # 0: 코스피
    PATH = "/api/dostk/stkinfo"
    print("KOSPI 전체 종목 코드 요청 중...")
    response_json = _request_api(path=PATH, headers=headers, body=body)
    
    if response_json and response_json.get("return_code") == 0:
        return response_json.get("list", [])
    else:
        print(f"종목 코드 요청 실패: {response_json}")
        return []


def get_daily_chart_api(token, ticker: str):
    """API를 호출하여 주식 일봉 데이터를 받아오는 함수"""
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "ka10081",
    }
    PATH = "/api/dostk/chart"
    base_date = datetime.now().strftime("%Y%m%d")
    body = {"stk_cd": ticker, "base_dt": base_date, "upd_stkpc_tp": "1"}

    response_json = _request_api(path=PATH, headers=headers, body=body)

    if response_json and response_json.get("return_code") == 0:
        return response_json.get("stk_dt_pole_chart_qry", [])
    else:
        print(f"[{ticker}] 일봉 데이터 수신 실패: {response_json}")
        return []


def update_database_and_charts():
    """DB에 KOSPI 종목 정보를 업데이트하고, 모든 종목의 최신 일봉 데이터를 저장/업데이트합니다."""

    # 1. 토큰 발급
    print("API 접근 토큰 발급 시도...")
    access_token = get_access_token()
    if not access_token:
        print("토큰 발급에 실패하여 프로그램을 종료합니다.")
        return
    print("토큰 발급 성공.")

    # 2. DB 연결
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "stocks.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 3. 종목 목록 업데이트
    print("\n--- KOSPI 종목 목록 업데이트 시작 ---")
    tickers_from_api = get_kospi_tickers(access_token)
    if tickers_from_api:
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS stocks (ticker TEXT PRIMARY KEY, name TEXT)"
        )
        insert_data = [(t["code"], t["name"].strip()) for t in tickers_from_api]
        cursor.executemany(
            "INSERT OR IGNORE INTO stocks (ticker, name) VALUES (?, ?)", insert_data
        )
        conn.commit()
        print(
            f"✅ API로부터 {len(insert_data)}개 종목 정보를 가져와 DB를 업데이트했습니다."
        )
    else:
        print(
            "API로부터 종목 정보를 가져오는 데 실패했습니다. 기존 DB 정보로 차트 업데이트를 시도합니다."
        )

    # 4. 일봉 차트 데이터 업데이트
    print("\n--- 일봉 차트 데이터 업데이트 시작 ---")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_charts (
            ticker TEXT, date TEXT, open INTEGER, high INTEGER, low INTEGER, close INTEGER, volume INTEGER,
            PRIMARY KEY (ticker, date)
        )
        """
    )

    tickers_from_db = cursor.execute("SELECT ticker, name FROM stocks").fetchall()
    # ticker_list is now a list of tuples (ticker, name)

    print(f"총 {len(tickers_from_db)}개 종목의 일봉 데이터 업데이트를 시작합니다...")

    for i, (ticker, name) in enumerate(tickers_from_db):
        print(f"[{i+1}/{len(tickers_from_db)}] {ticker} ({name}) 데이터 수집 중...")

        # DB에서 해당 종목의 마지막 데이터 날짜를 조회합니다.
        cursor.execute("SELECT MAX(date) FROM daily_charts WHERE ticker=?", (ticker,))
        last_date_in_db = cursor.fetchone()[0]

        # 데이터가 오늘 날짜와 동일하다면 최신 상태로 간주하고 API 호출을 건너뜁니다.
        today_str = datetime.now().strftime("%Y%m%d")
        if last_date_in_db == today_str:
            print("  -> 최신 데이터가 이미 존재합니다. 업데이트를 건너뜁니다.")
            continue

        chart_data = get_daily_chart_api(access_token, ticker)

        if chart_data:
            new_data = []
            if last_date_in_db:
                # 마지막 날짜 이후의 데이터만 필터링합니다.
                new_data = [row for row in chart_data if row["dt"] > last_date_in_db]
            else:
                # DB에 데이터가 없는 경우, 모든 데이터를 새 데이터로 간주합니다.
                new_data = chart_data

            if new_data:
                insert_chart_data = [
                    (
                        ticker,
                        row["dt"],
                        int(row["open_pric"]),
                        int(row["high_pric"]),
                        int(row["low_pric"]),
                        int(row["cur_prc"]),
                        int(row["trde_qty"]),
                    )
                    for row in new_data
                ]
                # 새 데이터만 INSERT 합니다. (중복 방지를 위해 IGNORE 사용)
                cursor.executemany(
                    "INSERT OR IGNORE INTO daily_charts (ticker, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    insert_chart_data,
                )
                conn.commit()
                print(f"  -> {len(insert_chart_data)}개의 신규 데이터 추가됨.")
            else:
                print("  -> 추가할 신규 데이터 없음.")

        # time.sleep(1) # API 호출 제한은 _request_api 함수로 이동

    print("\n✅ 모든 작업이 완료되었습니다.")
    conn.close()


if __name__ == "__main__":
    update_database_and_charts()
