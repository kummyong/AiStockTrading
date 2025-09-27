# main.py
import requests
import json
from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from datetime import datetime, timedelta
import time
import sqlite3
from contextlib import asynccontextmanager

# from manage_data import update_database_and_charts  # manage_data.py에서 함수 가져오기

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
def get_access_token():
    """API 접근 토큰 발급"""
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "secretkey": APP_SECRET,
    }
    PATH = "/oauth2/token"
    URL = f"{BASE_URL}{PATH}"
    try:
        res = requests.post(URL, json=body)
        res.raise_for_status()
        response_json = res.json()

        if response_json.get("return_code") == 0:
            access_token = response_json.get("token")
            if access_token:
                bot_state["access_token"] = access_token
                print("✅ 토큰 발급 성공")
                return response_json

        raise HTTPException(
            status_code=500, detail=f"Token issue failed: {response_json}"
        )

    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=f"Token request failed: {e} - Response: {res.text if 'res' in locals() else 'No response'}",
        )


def get_balance():
    """정리된 형태의 계좌 잔고 정보를 조회합니다."""
    token = bot_state.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="토큰이 없습니다.")
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "kt00018",
    }
    body = {"qry_tp": "1", "dmst_stex_tp": "KRX"}
    PATH = "/api/dostk/acnt"
    URL = f"{BASE_URL}{PATH}"
    try:
        res = requests.post(URL, headers=headers, json=body)
        res.raise_for_status()
        response_json = res.json()
        if response_json.get("return_code") == 0:
            summary = {
                "cash_balance": int(response_json.get("prsm_dpst_aset_amt", 0)),
                "total_purchase": int(response_json.get("tot_pur_amt", 0)),
                "total_evaluation": int(response_json.get("tot_evlt_amt", 0)),
                "profit_loss_rate": float(response_json.get("tot_prft_rt", 0.0)),
            }
            holdings = []
            for stock in response_json.get("acnt_evlt_remn_indv_tot", []):
                holdings.append(
                    {
                        "ticker": stock.get("stk_cd"),
                        "name": stock.get("stk_nm").strip(),
                        "quantity": int(stock.get("rmnd_qty", 0)),
                        "average_price": int(stock.get("pur_pric", 0)),
                        "current_price": int(stock.get("cur_prc", 0)),
                        "profit_loss": int(stock.get("evltv_prft", 0)),
                        "profit_loss_rate": float(stock.get("prft_rt", 0.0)),
                    }
                )
            return {"account_summary": summary, "holdings": holdings}
        else:
            raise HTTPException(
                status_code=500, detail=f"Balance check failed: {response_json}"
            )
    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500, detail=f"Balance check failed on request: {e}"
        )


def get_current_price(ticker: str):
    """주식 현재가 조회"""
    token = bot_state.get("access_token")
    if not token:
        raise HTTPException(status_code=401, detail="토큰이 없습니다.")
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "api-id": "ka10001",
    }
    body = {"stk_cd": ticker}
    PATH = "/api/dostk/stkinfo"
    URL = f"{BASE_URL}{PATH}"
    try:
        res = requests.post(URL, headers=headers, json=body)
        res.raise_for_status()
        response_json = res.json()
        if response_json.get("return_code") == 0:
            return int(response_json.get("stk_prpr", 0))
        else:
            print(f"[{ticker}] 현재가 조회 실패: {response_json}")
            return 0
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Price check failed: {res.text}")


def get_daily_chart_from_db(ticker: str):
    """SQLite DB에서 특정 종목의 일봉 데이터를 조회합니다."""
    try:
        conn = sqlite3.connect("stocks.db")
        df = pd.read_sql_query(
            f"SELECT * FROM daily_charts WHERE ticker = '{ticker}' ORDER BY date ASC",
            conn,
        )
        conn.close()
        return df
    except Exception as e:
        print(f"[{ticker}] DB에서 일봉 데이터를 읽는 중 오류 발생: {e}")
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
    URL = f"{BASE_URL}{PATH}"
    try:
        res = requests.post(URL, headers=headers, json=body)
        res.raise_for_status()
        response_json = res.json()
        if response_json.get("return_code") == 0:
            return response_json
        else:
            raise HTTPException(
                status_code=500, detail=f"Order failed: {response_json}"
            )
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Order failed on request: {e}")


# --- 🤖 4. AI 분석 및 자동화 로직 ---
def run_ai_analysis(ticker: str, owned_quantity: int = 0):
    """이동평균선 전략으로 매매 신호를 생성하는 함수"""
    daily_df = get_daily_chart_from_db(ticker)
    if daily_df.empty or len(daily_df) < 20:
        return None
    daily_df["ma5"] = daily_df["close"].rolling(window=5).mean()
    daily_df["ma20"] = daily_df["close"].rolling(window=20).mean()
    daily_df.dropna(inplace=True)
    if len(daily_df) < 2:
        return None
    latest = daily_df.iloc[-1]
    previous = daily_df.iloc[-2]

    # 매수 전략: 보유하지 않은 종목의 골든 크로스
    if (
        owned_quantity == 0
        and previous["ma5"] < previous["ma20"]
        and latest["ma5"] > latest["ma20"]
    ):
        # --- 여기를 수정했습니다! ---
        # 신호에 수량 계산을 위한 '어제 종가'를 포함하여 반환
        return {"ticker": ticker, "action": "buy", "price_for_calc": latest["close"]}

    # 매도 전략: 보유 중인 종목의 데드 크로스
    if (
        owned_quantity > 0
        and previous["ma5"] > previous["ma20"]
        and latest["ma5"] < latest["ma20"]
    ):
        return {"ticker": ticker, "quantity": owned_quantity, "action": "sell"}
    return None


def trading_job():
    """포트폴리오 기반 자동매매 작업"""
    print(f"\n--- [자동매매 작업 시작]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    try:
        if not bot_state.get("access_token"):
            print("토큰이 없어 새로 발급합니다.")
            get_access_token()

        balance_info = get_balance()
        total_assets = (
            balance_info["account_summary"]["total_evaluation"]
            + balance_info["account_summary"]["cash_balance"]
        )
        investment_budget = total_assets * 0.5
        owned_stocks = {stock["ticker"]: stock for stock in balance_info["holdings"]}

        conn = sqlite3.connect("stocks.db")
        tickers = conn.execute("SELECT ticker FROM stocks").fetchall()
        conn.close()
        ticker_list = [t[0] for t in tickers]

        buy_signals = []
        sell_signals = []

        print(f"총 {len(ticker_list)}개 종목 분석 시작...")
        # 매도 신호 분석
        for ticker, stock_info in owned_stocks.items():
            signal = run_ai_analysis(ticker, owned_quantity=stock_info["quantity"])
            if signal and signal["action"] == "sell":
                sell_signals.append(signal)

        # 매수 신호 분석
        for ticker in ticker_list:
            if ticker not in owned_stocks:
                signal = run_ai_analysis(ticker, owned_quantity=0)
                if signal and signal["action"] == "buy":
                    buy_signals.append(signal)

        print("\n--- 주문 실행 단계 ---")
        # 매도 주문 실행
        for signal in sell_signals:
            try:
                print(
                    f"SELL Signal: {signal['ticker']}, Quantity: {signal['quantity']}"
                )
                place_order(signal["ticker"], signal["quantity"], 0, "sell")
            except Exception as e:
                print(f"💥 [주문 오류] {signal['ticker']} 매도 주문 실패: {e}")
            finally:
                # 성공하든 실패하든 항상 1초 대기
                time.sleep(1)

        # 매수 주문 실행
        if buy_signals:
            investment_per_stock = investment_budget / len(buy_signals)
            print(
                f"총 매수 예산: {investment_budget:.0f}원, 종목당 예산: {investment_per_stock:.0f}원 ({len(buy_signals)}개 종목)"
            )

            for signal in buy_signals:
                try:
                    price_for_calc = signal["price_for_calc"]
                    if price_for_calc > 0:
                        quantity = int(investment_per_stock // price_for_calc)
                        if quantity > 0:
                            print(
                                f"BUY Signal: {signal['ticker']}, Quantity: {quantity} (기준가: {price_for_calc})"
                            )
                            place_order(signal["ticker"], quantity, 0, "buy")
                except Exception as e:
                    print(f"💥 [주문 오류] {signal['ticker']} 매수 주문 실패: {e}")
                finally:
                    # 성공하든 실패하든 항상 1초 대기
                    time.sleep(1)
        else:
            print("새로운 매수 신호가 없습니다.")

    except Exception as e:
        print(f"💥 [오류] 자동매매 작업 중 심각한 오류 발생: {e}")
    print(f"--- [자동매매 작업 종료]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")


# --- 🖥️ 5. 웹 API 엔드포인트 및 스케줄러 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """서버 시작 시 데이터베이스를 최신화하고 스케줄러를 실행합니다."""
    print("--- 서버 시작 프로세스 ---")
    print("데이터베이스 최신화를 시작합니다...")
    # try:
    #     update_database_and_charts()
    #     print("✅ 데이터베이스 최신화 완료.")
    # except Exception as e:
    #     print(f"💥 데이터베이스 최신화 중 오류 발생: {e}")

    print("\n초기 토큰 발급을 시도합니다...")
    try:
        get_access_token()
    except Exception as e:
        print(f"💥 초기 토큰 발급 실패: {e}")

    print("\n자동매매 스케줄러를 시작합니다.")
    scheduler = BackgroundScheduler()
    scheduler.add_job(trading_job, "cron", hour=18, minute=0)
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
