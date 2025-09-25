# main.py
import requests
import json
from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from datetime import datetime, timedelta
import time
import sqlite3

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

    raise HTTPException(
        status_code=500, detail=f"Token issue failed: {response_json}"
    )


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

    if response_json.get("return_code") == 0:
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


def get_daily_chart(ticker: str):
    """주식 일봉 데이터 조회"""
    token = bot_state.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="토큰이 없습니다.")

    PATH = "/api/dostk/chart"
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "ka10081",
    }
    base_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    body = {"stk_cd": ticker, "base_dt": base_date, "upd_stkpc_tp": "1"}

    response_json = _request_api(path=PATH, headers=headers, body=body)

    if response_json.get("return_code") == 0:
        data = response_json.get("stk_dt_pole_chart_qry", [])
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df = df[["dt", "open_pric", "high_pric", "low_pric", "cur_prc", "trde_qty"]]
        df.columns = ["date", "open", "high", "low", "close", "volume"]
        df[["open", "high", "low", "close", "volume"]] = df[
            ["open", "high", "low", "close", "volume"]
        ].astype(int)
        df = df.sort_values(by="date", ascending=True).reset_index(drop=True)
        return df
    else:
        print(f"[{ticker}] 일봉 데이터 수신 실패: {response_json}")
        return pd.DataFrame()


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
        raise HTTPException(
            status_code=500, detail=f"Order failed: {response_json}"
        )


# --- 🤖 4. AI 분석 및 자동화 로직 ---
def run_ai_analysis(ticker: str):
    """이동평균선 골든크로스 전략으로 매매 신호를 생성하는 함수"""
    print(f"[AI 두뇌] '{ticker}' 종목 분석 중...")
    try:
        daily_df = get_daily_chart(ticker)
        if daily_df.empty or len(daily_df) < 20:
            # print(f"[{ticker}] 데이터가 없거나 부족하여 분석을 건너뜁니다.") # 로그가 너무 길어지므로 주석 처리
            return None

        daily_df["ma5"] = daily_df["close"].rolling(window=5).mean()
        daily_df["ma20"] = daily_df["close"].rolling(window=20).mean()
        daily_df.dropna(inplace=True)

        if len(daily_df) < 2:
            return None

        latest = daily_df.iloc[-1]
        previous = daily_df.iloc[-2]

        if previous["ma5"] < previous["ma20"] and latest["ma5"] > latest["ma20"]:
            print(f"[매수 신호 발생]: {ticker} 골든 크로스 감지")
            return {"ticker": ticker, "quantity": 1, "price": 0, "action": "buy"}

    except Exception as e:
        print(f"[오류 발생] AI 분석 중 오류: {ticker} - {e}")

    return None


def trading_job():
    """주기적으로 실행될 자동매매 작업"""
    print(f"\n--- [자동매매 작업 시작]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")

    # DB 접속 등 준비 과정은 try...except로 묶어 조기에 종료될 수 있도록 함
    try:
        if not bot_state.get("access_token"):
            print("토큰이 없어 새로 발급합니다.")
            get_access_token()

        conn = sqlite3.connect("stocks.db")
        tickers = conn.execute("SELECT ticker FROM stocks").fetchall()
        conn.close()

        ticker_list = [t[0] for t in tickers]
        print(f"총 {len(ticker_list)}개의 KOSPI 종목에 대한 분석을 시작합니다.")

    except Exception as e:
        print(f"[오류 발생] 작업 준비 중 오류: {e}")
        print(f"--- [자동매매 작업 종료]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
        return  # 준비 단계에서 오류 시 작업 전체 종료

    # 각 종목을 분석하고 주문하는 루프
    for ticker in ticker_list:
        try:
            signal = run_ai_analysis(ticker)
            if signal:
                print(f"주문 실행: {signal}")
                place_order(
                    signal["ticker"],
                    signal["quantity"],
                    signal["price"],
                    signal["action"],
                )

            # API 호출 한도 준수는 _request_api 함수에서 처리됩니다.

        except Exception as e:
            # 개별 종목 오류는 기록만 하고 건너뜀
            print(f"[오류 발생] '{ticker}' 종목 처리 중 오류: {e}")
            continue  # 다음 종목으로 넘어감

    print(f"--- [자동매매 작업 종료]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")


# --- 🖥️ 5. 웹 API 엔드포인트 및 스케줄러 ---
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """서버 시작 시 토큰을 발급하고 스케줄러를 실행합니다."""
    print("서버 시작... 초기 토큰 발급 및 스케줄러를 시작합니다.")
    try:
        get_access_token()
    except Exception as e:
        print(f"초기 토큰 발급 실패: {e}")

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        trading_job,
        "cron",
        hour=9,
        minute=30,
    )
    scheduler.start()

    yield
    print("서버 종료...")


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
