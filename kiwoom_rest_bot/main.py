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
import os
import configparser

# 📂 마법 공식 전략 함수를 strategy.py에서 가져옵니다.
from magic_formula_analyzer import analyze_magic_formula


# --- ⚙️ 1. 설정 관리 ---
class ConfigManager:
    """config.ini 파일에서 설정을 읽어 관리합니다."""

    def __init__(self, config_file="config.ini"):
        if not os.path.isabs(config_file):
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_file = os.path.join(script_dir, config_file)

        if not os.path.exists(config_file):
            raise FileNotFoundError(f"설정 파일({config_file})을 찾을 수 없습니다.")

        config = configparser.ConfigParser()
        config.read(config_file, encoding="utf-8")

        try:
            self.is_mock = config.getboolean("SETTINGS", "is_mock")
            self.portfolio_stock_count = config.getint(
                "SETTINGS", "portfolio_stock_count", fallback=20
            )
        except (configparser.NoSectionError, configparser.NoOptionError):
            print("⚠️ 'is_mock' 설정을 찾을 수 없어 기본값(True)을 사용합니다.")
            self.is_mock = True
            self.portfolio_stock_count = 20

        section = "KIWOOM_MOCK" if self.is_mock else "KIWOOM_REAL"

        try:
            self.base_url = config.get(section, "base_url").strip("'\"")
            self.app_key = config.get(section, "app_key").strip("'\"")
            self.app_secret = config.get(section, "app_secret").strip("'\"")
            self.account_no = config.get(section, "account_no").strip("'\"")
            print(
                f"✅ 설정 로드 완료. 모드: {'모의투자' if self.is_mock else '실서버'}"
            )
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            raise ValueError(f"'{section}' 섹션에서 설정을 읽는 데 실패했습니다: {e}")


config = ConfigManager()
# --------------------


# --- 🌐 2. 서버 및 상태 관리 ---
BASE_URL = config.base_url
APP_KEY = config.app_key
APP_SECRET = config.app_secret
ACCOUNT_NO = config.account_no
PORTFOLIO_STOCK_COUNT = config.portfolio_stock_count

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
    """(수정) 변수명을 명확히 하고, 실제 현금과 총자산을 구분하여 반환합니다."""
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
                # API의 '추정예탁자산금액'은 실제 총 자산을 의미하므로 변수명 변경
                "estimated_total_assets": int(
                    response_json.get("prsm_dpst_aset_amt", 0)
                ),
                # API의 '예수금총금액'이 실제 현금을 의미 (dnca_tot_amt는 D+2 예수금을 의미하기도 함)
                "real_cash_balance": int(response_json.get("dnca_tot_amt", 0)),
                "total_purchase": int(response_json.get("tot_pur_amt", 0)),
                "total_evaluation": int(response_json.get("tot_evlt_amt", 0)),
                "profit_loss_rate": float(response_json.get("tot_prft_rt", 0.0)),
            }
            holdings = []
            for stock in response_json.get("acnt_evlt_remn_indv_tot", []):
                ticker = stock.get("stk_cd", "")
                holdings.append(
                    {
                        "ticker": ticker.lstrip("A") if ticker else "",
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


# --- 🤖 4. 마법 공식 자동매매 로직 ---
def magic_formula_rebalance_job():
    """(수정) 올바른 총 자산 금액을 사용하여 리밸런싱을 수행합니다."""
    print(
        f"\n--- [마법 공식 리밸런싱 시작]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---"
    )
    try:
        if not bot_state.get("access_token"):
            get_access_token()

        target_tickers = set(analyze_magic_formula(top_n=PORTFOLIO_STOCK_COUNT))
        if not target_tickers:
            print("❌ 목표 포트폴리오 선정 실패. 리밸런싱을 종료합니다.")
            return
        print(
            f"🎯 목표 포트폴리오 ({len(target_tickers)}개): {', '.join(sorted(list(target_tickers)))}"
        )

        balance_info = get_balance()
        owned_stocks = {stock["ticker"]: stock for stock in balance_info["holdings"]}
        owned_tickers = set(owned_stocks.keys())
        print(
            f"현재 보유 종목 ({len(owned_tickers)}개): {', '.join(sorted(list(owned_tickers))) if owned_tickers else '없음'}"
        )

        tickers_to_sell = owned_tickers - target_tickers
        if tickers_to_sell:
            print(f"\n--- 📉 매도 실행 ({len(tickers_to_sell)}개) ---")
            for ticker in tickers_to_sell:
                try:
                    stock_info = owned_stocks[ticker]
                    quantity_to_sell = stock_info["quantity"]
                    print(
                        f"   - 매도 주문: {stock_info['name']}({ticker}), 수량: {quantity_to_sell}"
                    )
                    place_order(ticker, quantity_to_sell, 0, "sell")
                except Exception as e:
                    print(f"   💥 [주문 오류] {ticker} 매도 실패: {e}")
                finally:
                    time.sleep(1)
        else:
            print("\n- 매도할 종목이 없습니다.")

        tickers_to_buy = target_tickers - owned_tickers
        if tickers_to_buy:
            print(f"\n--- 📈 매수 실행 ({len(tickers_to_buy)}개) ---")
            if tickers_to_sell:
                print("   - 매도 주문 처리를 위해 10초 대기...")
                time.sleep(10)

            # ▼▼▼ 여기가 수정된 부분입니다 ▼▼▼
            current_balance = get_balance()
            # 'estimated_total_assets'를 총 자산으로 사용 (이중 계산 방지)
            total_assets = current_balance["account_summary"]["estimated_total_assets"]
            # ▲▲▲ 여기가 수정된 부분입니다 ▲▲▲

            investment_per_stock = total_assets / PORTFOLIO_STOCK_COUNT

            print(
                f"   - 총 자산: {total_assets:,.0f}원 / 종목당 투자 예산: {investment_per_stock:,.0f}원"
            )

            for ticker in tickers_to_buy:
                try:
                    current_price = get_current_price(ticker)
                    if current_price > 0:
                        quantity_to_buy = int(investment_per_stock // current_price)
                        if quantity_to_buy > 0:
                            print(
                                f"   - 매수 주문: {ticker}, 수량: {quantity_to_buy} (현재가: {current_price:,.0f})"
                            )
                            place_order(ticker, quantity_to_buy, 0, "buy")
                        else:
                            print(
                                f"   - [{ticker}] 예산 부족으로 매수 불가 (계산된 수량: 0)"
                            )
                    else:
                        print(f"   - [{ticker}] 현재가 조회 실패로 매수 불가")
                except Exception as e:
                    print(f"   💥 [주문 오류] {ticker} 매수 실패: {e}")
                finally:
                    time.sleep(1)
        else:
            print("\n- 신규 매수할 종목이 없습니다.")

    except Exception as e:
        print(f"💥 [오류] 리밸런싱 작업 중 심각한 오류 발생: {e}")
    finally:
        print(
            f"--- [마법 공식 리밸런싱 종료]: {time.strftime('%Y-%m-%d %H:%M:%S')} ---"
        )


# --- 🖥️ 5. 웹 API 엔드포인트 및 스케줄러 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("--- 서버 시작 프로세스 ---")
    try:
        get_access_token()
    except Exception as e:
        print(f"💥 초기 토큰 발급 실패: {e}")

    scheduler = BackgroundScheduler()
    scheduler.add_job(magic_formula_rebalance_job, "cron", hour=18, minute=0)
    scheduler.start()
    print("✅ 자동매매 스케줄러 시작 (매일 18:00 실행)")
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


@app.post("/run-rebalance", summary="마법 공식 리밸런싱 1회 실행 (수동)")
def run_rebalance_manually():
    """수동으로 리밸런싱 작업을 즉시 실행합니다."""
    magic_formula_rebalance_job()
    return {"message": "마법 공식 리밸런싱 작업이 수동으로 실행되었습니다."}
