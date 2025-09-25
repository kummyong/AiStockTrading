# setup_database.py
import requests
import json
import sqlite3
import time

# --- 사용자 설정 ---
# main.py와 동일한 본인의 정보를 입력합니다.
IS_MOCK = True
APP_KEY = "IWxXc-OxrNyAt3jBCkERK4EV7xbW6DYYHXqK3n0x57A"
APP_SECRET = "FBAOtvQj0MJBHOmx3s8UBIdH0XK399iHIudXbO2H2Vo"

if IS_MOCK:
    BASE_URL = "https://mockapi.kiwoom.com"
else:
    # 실전투자용 종목코드 조회는 다른 api-id와 주소를 사용해야 합니다.
    BASE_URL = "https://api.kiwoom.com"
# --------------------


def get_access_token():
    """API 접근 토큰 발급"""
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "secretkey": APP_SECRET,
    }
    PATH = "/oauth2/token"
    URL = f"{BASE_URL}{PATH}"
    res = requests.post(URL, json=body)
    try:
        access_token = res.json().get("token")
    except json.JSONDecodeError:
        print(f"JSON 디코딩 실패: {res.text}")
        return None
    return access_token


def get_kospi_tickers(token):
    """모의투자 KOSPI 전체 종목 코드를 받아오는 함수"""

    # 사용자의 원래 요청 구조로 되돌려 모의 API와 호환되도록 합니다.
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "ka10099",
    }
    body = {"mrkt_tp": "0"}  # 0: 코스피
    PATH = "/api/dostk/stkinfo"
    URL = f"{BASE_URL}{PATH}"

    print("모의투자 KOSPI 전체 종목 코드 요청 중...")
    res = requests.post(URL, headers=headers, json=body)  # POST 요청

    try:
        response_json = res.json()
    except json.JSONDecodeError:
        print(f"JSON 디코딩 실패: {res.text}")
        return []

    # 원래 코드에서 사용하던 'return_code'와 'list'를 사용합니다.
    if response_json.get("return_code") == 0:
        return response_json.get("list", [])

    print(f"종목 코드 요청 실패: {response_json}")
    return []


def update_database():
    """SQLite DB를 생성/업데이트하고 KOSPI 종목 정보를 저장합니다."""

    print("API 접근 토큰 발급 시도...")
    access_token = get_access_token()
    if not access_token:
        print("토큰 발급에 실패하여 프로그램을 종료합니다.")
        return
    print("토큰 발급 성공.")
    time.sleep(1)

    tickers = get_kospi_tickers(access_token)
    if not tickers:
        print("종목 정보를 가져오는 데 실패했습니다.")
        return

    conn = sqlite3.connect("stocks.db")
    cursor = conn.cursor()

    print("데이터베이스 테이블 확인 및 생성 중...")
    # 재실행시 오류가 발생하지 않도록 IF NOT EXISTS 사용
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stocks (
            ticker TEXT PRIMARY KEY,
            name TEXT
        )
    """
    )

    # 현재 저장된 종목 수 확인
    initial_count = cursor.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    print(f"현재 데이터베이스에 저장된 종목 수: {initial_count}")

    # get_kospi_tickers 변경에 맞춰 'code'와 'name'을 사용하도록 되돌립니다.
    insert_data = [(t["code"], t["name"].strip()) for t in tickers]

    print(
        f"API로부터 {len(insert_data)}개의 종목 정보를 가져왔습니다. 데이터베이스를 업데이트합니다..."
    )
    # INSERT OR IGNORE를 사용하여 신규 종목만 추가
    cursor.executemany(
        "INSERT OR IGNORE INTO stocks (ticker, name) VALUES (?, ?)", insert_data
    )

    conn.commit()

    # 추가된 종목 수 계산 및 출력
    final_count = cursor.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
    new_stocks_count = final_count - initial_count

    if new_stocks_count > 0:
        print(f"✅ {new_stocks_count}개의 신규 종목이 추가되었습니다.")
    else:
        print("✅ 신규로 추가된 종목이 없습니다.")

    print(
        f"✅ 데이터베이스 'stocks.db' 업데이트 완료. 총 {final_count}개의 KOSPI 종목이 저장되었습니다."
    )

    # 저장된 데이터 샘플 확인
    print("\n--- 저장된 데이터 샘플 (상위 5개) ---")
    sample_data = cursor.execute("SELECT * FROM stocks LIMIT 5").fetchall()
    for row in sample_data:
        print(f"Ticker: {row[0]}, Name: '{row[1]}'")

    conn.close()


if __name__ == "__main__":
    update_database()
