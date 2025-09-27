# backtest.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import plotly.express as px
import os
from tqdm import tqdm

# --- 설정 ---
DB_NAME = "stocks.db"


# --- 데이터베이스 로드 함수 (기존과 동일) ---
@st.cache_data(show_spinner="데이터베이스 로딩 중...")
def load_data_from_db():
    """DB에서 주식 이름, 재무 정보, 일봉 데이터를 모두 불러옵니다."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_NAME)
    if not os.path.exists(db_path):
        return None, None

    conn = sqlite3.connect(db_path)
    financial_info = pd.read_sql_query("SELECT * FROM financial_info", conn)
    daily_charts = pd.read_sql_query("SELECT * FROM daily_charts", conn)
    daily_charts["date"] = pd.to_datetime(daily_charts["date"], format="%Y%m%d")
    conn.close()

    return financial_info, daily_charts


# --- 백테스팅 핵심 로직 (🚨 대폭 개선됨) ---
def run_backtest(
    start_date,
    end_date,
    initial_capital,
    num_stocks,
    rebalance_period,
    transaction_cost,
    financial_info,
    daily_charts,
):
    """(수정) 리밸런싱 시점마다 순위를 재계산하여 Look-Ahead Bias를 제거한 백테스팅을 실행합니다."""

    price_df = daily_charts.pivot(index="date", columns="ticker", values="close")
    trade_dates = price_df.loc[start_date:end_date].index
    if trade_dates.empty:
        return pd.DataFrame()

    rebalance_dates = pd.date_range(start_date, end_date, freq=rebalance_period)

    portfolio_history = []
    portfolio = {} # 현재 포트폴리오 (ticker: shares)
    cash = initial_capital
    last_rebalance_date = pd.Timestamp.min
    magic_formula_tickers = [] # 리밸런싱 시점에 동적으로 채워질 리스트

    for date in tqdm(trade_dates, desc="백테스팅 시뮬레이션 진행 중"):
        try:
            next_rebalance_date = rebalance_dates[rebalance_dates > last_rebalance_date].min()
        except (ValueError, IndexError):
            next_rebalance_date = pd.Timestamp.max

        if date >= next_rebalance_date:
            # --- ▼▼▼ 리밸런싱 로직 시작 ▼▼▼ ---

            # 1. 순위 재계산
            current_business_year = date.year - 1
            fi_for_year = financial_info[financial_info['business_year'] == current_business_year]

            if not fi_for_year.empty:
                df_filtered = fi_for_year.dropna(subset=["roe", "ev_ebitda"])
                df_filtered = df_filtered[(df_filtered["roe"] > 0) & (df_filtered["ev_ebitda"] > 0)]
                
                if not df_filtered.empty:
                    df_filtered["rank_roe"] = df_filtered["roe"].rank(ascending=False)
                    df_filtered["rank_ev_ebitda"] = df_filtered["ev_ebitda"].rank(ascending=True)
                    df_filtered["rank_total"] = df_filtered["rank_roe"] + df_filtered["rank_ev_ebitda"]
                    df_sorted = df_filtered.sort_values(by="rank_total")
                    magic_formula_tickers = df_sorted.head(num_stocks)["ticker"].tolist()

            # 2. 기존 포트폴리오 매도 (현금화)
            if portfolio:
                sell_value = 0
                for ticker, shares in portfolio.items():
                    if ticker in price_df.columns and not pd.isna(price_df.loc[date, ticker]):
                        sell_value += price_df.loc[date, ticker] * shares
                cash += sell_value * (1 - transaction_cost)
                portfolio = {}

            # 3. 신규 포트폴리오 매수
            if cash > 0 and magic_formula_tickers:
                capital_per_stock = cash / len(magic_formula_tickers)
                for ticker in magic_formula_tickers:
                    if ticker in price_df.columns and not pd.isna(price_df.loc[date, ticker]):
                        price = price_df.loc[date, ticker]
                        if price > 0:
                            shares_to_buy = capital_per_stock / price
                            portfolio[ticker] = shares_to_buy
                            cash -= (shares_to_buy * price) * (1 + transaction_cost)
            
            last_rebalance_date = date
            # --- ▲▲▲ 리밸런싱 로직 종료 ▲▲▲ ---

        # 일일 포트폴리오 가치 기록
        current_portfolio_value = 0
        for ticker, shares in portfolio.items():
            if ticker in price_df.columns and not pd.isna(price_df.loc[date, ticker]):
                current_portfolio_value += price_df.loc[date, ticker] * shares

        total_assets = cash + current_portfolio_value
        portfolio_history.append({"date": date, "value": total_assets})

    return pd.DataFrame(portfolio_history)


# --- Streamlit UI 구성 ---
st.set_page_config(layout="wide")
st.title("✨ 마법 공식 백테스팅 툴 (개선 ver.)")

# 데이터 로드
financial_df, charts_df = load_data_from_db()

if financial_df is None:
    st.error(
        f"'{DB_NAME}'를 찾을 수 없습니다. 먼저 manage_data.py를 실행하여 데이터를 수집해주세요."
    )
else:
    # --- 사이드바: 사용자 입력 ---
    st.sidebar.header("⚙️ 시뮬레이션 설정")

    min_date = charts_df["date"].min().to_pydatetime()
    max_date = charts_df["date"].max().to_pydatetime()

    start_date = st.sidebar.date_input(
        "시작일", value=min_date, min_value=min_date, max_value=max_date
    )
    end_date = st.sidebar.date_input(
        "종료일", value=max_date, min_value=min_date, max_value=max_date
    )

    initial_capital = st.sidebar.number_input(
        "초기 자본금",
        min_value=1_000_000,
        value=10_000_000,
        step=1_000_000,
        format="%d",
    )
    num_stocks = st.sidebar.slider(
        "포트폴리오 종목 수", min_value=5, max_value=50, value=20
    )

    rebalance_option = st.sidebar.selectbox("리밸런싱 주기", ("월간", "분기별", "연간"))
    rebalance_freq_map = {
        "월간": "BMS",
        "분기별": "BQS",
        "연간": "BYS",
    }  # 영업일 시작일 기준

    # ▼▼▼ 거래 비용 입력 UI 추가 ▼▼▼
    transaction_cost = (
        st.sidebar.number_input(
            "거래 비용 (%)", min_value=0.0, max_value=5.0, value=0.25, step=0.01
        )
        / 100
    )

    if st.sidebar.button("백테스팅 실행", type="primary"):
        with st.spinner(
            "백테스팅을 실행 중입니다... (개선된 로직으로 더 빨라졌습니다!)"
        ):
            result_df = run_backtest(
                start_date,
                end_date,
                initial_capital,
                num_stocks,
                rebalance_freq_map[rebalance_option],
                transaction_cost,
                financial_df,
                charts_df,
            )

        if not result_df.empty:
            st.success("백테스팅 완료!")

            # 최종 결과 지표
            final_value = result_df["value"].iloc[-1]
            total_return = (final_value - initial_capital) / initial_capital

            # MDD (최대 낙폭) 계산
            result_df["peak"] = result_df["value"].cummax()
            result_df["drawdown"] = (
                result_df["value"] - result_df["peak"]
            ) / result_df["peak"]
            max_drawdown = result_df["drawdown"].min()

            st.subheader("📈 최종 결과")
            col1, col2, col3 = st.columns(3)
            col1.metric("최종 자산", f"{final_value:,.0f} 원")
            col2.metric("총 수익률", f"{total_return:.2%}")
            col3.metric("최대 낙폭 (MDD)", f"{max_drawdown:.2%}")

            # 포트폴리오 가치 변화 차트
            st.subheader("📊 포트폴리오 가치 변화")
            fig = px.line(
                result_df,
                x="date",
                y="value",
                title="기간별 자산 변화 그래프",
                labels={"date": "날짜", "value": "자산 가치"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(
                "선택하신 기간에 대한 데이터가 부족하여 결과를 표시할 수 없습니다."
            )
    else:
        st.info("좌측 사이드바에서 설정을 조정한 후 '백테스팅 실행' 버튼을 눌러주세요.")

# --- 💡 백테스팅 결과 해석 시 참고사항 ---
st.markdown("---")
st.subheader("💡 필독: 백테스팅 결과 해석 시 참고사항")
st.info(
    """
**백테스팅은 과거의 데이터에 기반한 모의 투자 결과입니다.**
- 본 시뮬레이션은 리밸런싱 시점에 **이전 년도 결산 재무 데이터를 사용**하여 미래 정보 편향(Look-Ahead Bias)을 제거했습니다.
- 거래 비용(수수료 및 세금)이 고려되었지만, 슬리피지(Slippage, 주문 체결 오차)와 같은 실제 시장의 모든 변수가 포함되지는 않았습니다.
- 과거의 성과가 미래의 수익을 보장하지는 않습니다.

따라서 이 결과는 투자를 결정하는 절대적인 지표가 아닌, 전략의 유효성을 검증하고 참고하는 용도로만 활용해야 합니다.
"""
)
