# main.py
import requests
import json
from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from datetime import datetime, timedelta
import time
import sqlite3
import os
from manage_data import update_database_and_charts

# --- ⚙️ 1. 사용자 설정 ---
IS_MOCK = True
APP_KEY = "IWxXc-OxrNyAt3jBCkERK4EV7xbW6DYYHXqK3n0x57A"
APP_SECRET = "FBAOtvQj0MJBHOmx3s8UBIdH0XK399iHIudXbO2H2Vo"
ACCOUNT_NO = "81118476"
# --------------------


# --- 🌐 2. 서버 및 상태 관리 ---
if IS_MOCK:
    BASE_URL = "https://mockapi.kiwoom.com"
else:
    BASE_URL = "https://api.kiwoom.com"

app = FastAPI()
bot_state = {"access_token": None}
# --------------------


# --- 🔑 3. API 연동 함수 ---
def _request_api(path: str, headers: dict = None, body: dict = None):
    """Kiwoom REST API 요청을 위한 범용 래퍼 함수"""
    URL = f"{BASE_URL}{path}"
    try:
        res = requests.post(URL, headers=headers, json=body)
        time.sleep(1)  # API 요청 후 1초 대기
        res.raise_for_status()  # HTTP 4xx/5xx 에러 발생 시 예외 발생
        return res.json()
    except requests.exceptions.RequestException as e:
        # 네트워크/HTTP 레벨 에러
        err_msg = f"Request to {URL} failed: {e}"
        if "res" in locals():
            err_msg += f" - Response: {res.text}"
        raise HTTPException(status_code=500, detail=err_msg)


def get_access_token():
    """API 접근 토큰 발급"""
    PATH = "/oauth2/token"
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "secretkey": APP_SECRET,
    }

    response_json = _request_api(path=PATH, body=body)

    if response_json.get("return_code") == 0:
        access_token = response_json.get("token")
        if access_token:
            bot_state["access_token"] = access_token
            print("✅ 토큰 발급 성공")
            return response_json

    raise HTTPException(status_code=500, detail=f"Token issue failed: {response_json}")


def get_balance():
    """정리된 형태의 계좌 잔고 정보를 조회합니다."""
    token = bot_state.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="토큰이 없습니다.")

    PATH = "/api/dostk/acnt"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "kt00018",
    }
    body = {"qry_tp": "1", "dmst_stex_tp": "KRX"}

    response_json = _request_api(path=PATH, headers=headers, body=body)

    if response_json and response_json.get("return_code") == 0:
        # --- 여기부터 데이터 가공 로직 추가 ---

        # 계좌 요약 정보 추출
        summary = {
            "cash_balance": int(response_json.get("dps_bal", 0)),
            "total_purchase": int(response_json.get("tot_puno_amt", 0)),
            "total_evaluation": int(response_json.get("tot_evlu_amt", 0)),
            "profit_loss_rate": float(response_json.get("prts_rate", 0.0)),
        }

        # 보유 종목 정보 추출
        holdings = []
        stock_list = response_json.get("stk_list", [])
        for stock in stock_list:
            holding_item = {
                "ticker": stock.get("stk_cd"),
                "name": stock.get("stk_nm"),
                "quantity": int(stock.get("hldg_qty", 0)),
                "average_price": int(stock.get("puno_uv", 0)),
                "current_price": int(stock.get("cur_pric", 0)),
                "profit_loss": int(stock.get("evlu_pfls_amt", 0)),
                "profit_loss_rate": float(stock.get("evlu_pfls_rt", 0.0)),
            }
            holdings.append(holding_item)

        # 최종적으로 정리된 데이터 반환
        return {"account_summary": summary, "holdings": holdings}
        # ------------------------------------
    else:
        raise HTTPException(
            status_code=500, detail=f"Balance check failed: {response_json}"
        )


def place_order(ticker: str, quantity: int, price: int, order_type: str):
    """주식 주문 (매수/매도)"""
    token = bot_state.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="토큰이 없습니다.")
    api_id = "kt10000" if order_type.lower() == "buy" else "kt10001"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": api_id,
    }
    body = {
        "dmst_stex_tp": "KRX",
        "stk_cd": ticker,
        "ord_qty": str(quantity),
        "ord_uv": str(price) if price > 0 else "0",
        "trde_tp": "0" if price > 0 else "3",
        "cond_uv": "",
    }
    PATH = "/api/dostk/ordr"

    response_json = _request_api(path=PATH, headers=headers, body=body)

    if response_json.get("return_code") == 0:
        return response_json
    else:
        raise HTTPException(status_code=500, detail=f"Order failed: {response_json}")


# --- 🤖 4. AI 분석 및 자동화 로직 ---
def run_ai_analysis(ticker: str, name: str, db_conn: sqlite3.Connection):
    """이동평균선 골든크로스 전략으로 매매 신호를 생성하는 함수 (DB 데이터 사용)"""
    print(f"[AI 두뇌] {ticker} ({name}) 종목 분석 중 (DB 데이터)...")
    try:
        # DB에서 일봉 데이터를 읽어 DataFrame으로 변환
        daily_df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume FROM daily_charts WHERE ticker = ? ORDER BY date ASC",
            db_conn,
            params=(ticker,),
        )

        if daily_df.empty or len(daily_df) < 20:
            return None

        daily_df["ma5"] = daily_df["close"].rolling(window=5).mean()
        daily_df["ma20"] = daily_df["close"].rolling(window=20).mean()
        daily_df.dropna(inplace=True)

        if len(daily_df) < 2:
            return None

        latest = daily_df.iloc[-1]
        previous = daily_df.iloc[-2]

        if previous["ma5"] < previous["ma20"] and latest["ma5"] > latest["ma20"]:
            print(f"[매수 신호 발생]: {ticker} ({name}) 골든 크로스 감지")
            return {"ticker": ticker, "quantity": 1, "price": 0, "action": "buy"}

    except Exception as e:
        print(f"[오류 발생] AI 분석 중 오류: {ticker} ({name}) - {e}")

    return None


def trading_job():
    """주기적으로 실행될 자동매매 작업"""
    print(f"\n--- [자동매매 작업 시작]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")

    conn = None  # conn 변수 초기화
    try:
        if not bot_state.get("access_token"):
            print("토큰이 없어 새로 발급합니다.")
            get_access_token()

        # DB 경로 설정 및 연결
        script_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_dir, "stocks.db")
        conn = sqlite3.connect(db_path)

        # 종목 코드와 이름을 함께 조회
        ticker_list = conn.execute("SELECT ticker, name FROM stocks").fetchall()
        print(f"총 {len(ticker_list)}개의 KOSPI 종목에 대한 분석을 시작합니다.")

    except Exception as e:
        print(f"[오류 발생] 작업 준비 중 오류: {e}")
        if conn:
            conn.close()
        print(f"--- [자동매매 작업 종료]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
        return

    # 각 종목을 분석하고 주문하는 루프
    for ticker, name in ticker_list:
        try:
            # DB 커넥션과 종목명을 함께 전달
            signal = run_ai_analysis(ticker, name, conn)
            if signal:
                print(f"주문 실행: {signal}")
                place_order(
                    signal["ticker"],
                    signal["quantity"],
                    signal["price"],
                    signal["action"],
                )
        except Exception as e:
            print(f"[오류 발생] {ticker} ({name}) 종목 처리 중 오류: {e}")
            continue

    if conn:
        conn.close()
    print(f"--- [자동매매 작업 종료]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")


# --- 🖥️ 5. 웹 API 엔드포인트 및 스케줄러 ---
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """서버 시작 시 데이터베이스를 최신화하고 스케줄러를 실행합니다."""
    print("--- 서버 시작 프로세스 ---")
    
    # 1. 데이터베이스 최신화
    print("데이터베이스 최신화를 시작합니다...")
    try:
        update_database_and_charts()
        print("데이터베이스 최신화 완료.")
    except Exception as e:
        print(f"데이터베이스 최신화 중 오류 발생: {e}")

    # 2. 초기 토큰 발급
    print("\n초기 토큰 발급을 시도합니다...")
    try:
        get_access_token()
    except Exception as e:
        print(f"초기 토큰 발급 실패: {e}")

    # 3. 자동매매 스케줄러 시작
    print("\n자동매매 스케줄러를 시작합니다.")
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        trading_job,
        "cron",
        hour=9,
        minute=30,
    )
    scheduler.start()
    print("--- 서버 시작 완료 ---")

    yield
    
    print("--- 서버 종료 ---")


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {
        "message": "Kiwoom REST API Bot",
        "token_status": "발급됨" if bot_state.get("access_token") else "미발급",
    }


@app.post("/auth", summary="API 토큰 발급 (수동)")
def authenticate():
    return {
        "message": "인증 성공! 토큰이 발급되었습니다.",
        "details": get_access_token(),
    }


@app.get("/balance", summary="계좌 잔고 조회")
def fetch_balance():
    return get_balance()


@app.post("/order", summary="주문 실행 (수동)")
def execute_order(ticker: str, quantity: int, price: int, action: str):
    return place_order(ticker, quantity, price, action)


@app.post("/run-ai-trade", summary="AI 분석 및 자동매매 1회 실행 (수동)")
def run_ai_trade():
    trading_job()
    return {"message": "AI trading job has been manually triggered."}
