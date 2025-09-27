import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import plotly.express as px
import os

# --- 설정 ---
DB_NAME = "stocks.db"


# --- 데이터베이스 로드 함수 (캐싱 기능 사용) ---
@st.cache_data
def load_data_from_db():
    """DB에서 주식 이름, 재무 정보, 일봉 데이터를 모두 불러옵니다."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_NAME)
    if not os.path.exists(db_path):
        return None, None, None

    conn = sqlite3.connect(db_path)

    stocks = pd.read_sql_query("SELECT * FROM stocks", conn)
    financial_info = pd.read_sql_query("SELECT * FROM financial_info", conn)
    # 날짜 형식으로 변환하여 처리 속도 향상
    daily_charts = pd.read_sql_query("SELECT * FROM daily_charts", conn)
    daily_charts["date"] = pd.to_datetime(daily_charts["date"], format="%Y%m%d")

    conn.close()

    return stocks, financial_info, daily_charts


# --- 백테스팅 핵심 로직 ---
def run_backtest(
    start_date,
    end_date,
    initial_capital,
    num_stocks,
    rebalance_period,
    financial_info,
    daily_charts,
    stocks,
):
    """마법 공식 백테스팅을 실행합니다."""

    # 1. 마법 공식 순위 계산
    df_filtered = financial_info.dropna(subset=["roe", "ev_ebitda"])
    df_filtered = df_filtered[(df_filtered["roe"] > 0) & (df_filtered["ev_ebitda"] > 0)]
    df_filtered["rank_roe"] = df_filtered["roe"].rank(ascending=False)
    df_filtered["rank_ev_ebitda"] = df_filtered["ev_ebitda"].rank(ascending=True)
    df_filtered["rank_total"] = df_filtered["rank_roe"] + df_filtered["rank_ev_ebitda"]
    df_sorted = df_filtered.sort_values(by="rank_total")

    # 2. 투자 기간 설정 및 리밸런싱 날짜 계산
    dates = pd.date_range(start_date, end_date, freq="D")
    rebalance_dates = []
    if rebalance_period == "월간":
        rebalance_dates = pd.date_range(
            start_date, end_date, freq="BMS"
        )  # 매월 영업일 시작일
    elif rebalance_period == "분기별":
        rebalance_dates = pd.date_range(
            start_date, end_date, freq="BQS"
        )  # 매분기 영업일 시작일

    portfolio_history = []
    current_portfolio = {}
    capital = initial_capital

    # 3. 시뮬레이션 루프
    for date in dates:
        # 리밸런싱 날짜가 되면 포트폴리오 교체
        if date in rebalance_dates:
            # 기존 포트폴리오 매도
            if current_portfolio:
                # 리밸런싱 시점의 자산 가치를 기준으로 자본금 업데이트
                capital = sum(
                    (
                        daily_charts[
                            (daily_charts["ticker"] == ticker)
                            & (daily_charts["date"] == date)
                        ]["close"].iloc[0]
                        * shares
                    )
                    for ticker, shares in current_portfolio.items()
                    if not daily_charts[
                        (daily_charts["ticker"] == ticker)
                        & (daily_charts["date"] == date)
                    ].empty
                )

            # 마법 공식 상위 종목 선정 및 매수
            top_stocks = df_sorted.head(num_stocks)["ticker"].tolist()
            capital_per_stock = capital / len(top_stocks)
            new_portfolio = {}
            for ticker in top_stocks:
                stock_price_info = daily_charts[
                    (daily_charts["ticker"] == ticker) & (daily_charts["date"] == date)
                ]
                if not stock_price_info.empty:
                    price = stock_price_info["close"].iloc[0]
                    if price > 0:
                        shares = capital_per_stock / price
                        new_portfolio[ticker] = shares
            current_portfolio = new_portfolio

        # 현재 포트폴리오 가치 계산
        portfolio_value = 0
        for ticker, shares in current_portfolio.items():
            price_info = daily_charts[
                (daily_charts["ticker"] == ticker) & (daily_charts["date"] == date)
            ]
            if not price_info.empty:
                current_price = price_info["close"].iloc[0]
                portfolio_value += current_price * shares

        if portfolio_value > 0:
            portfolio_history.append({"date": date, "value": portfolio_value})

    return pd.DataFrame(portfolio_history)


# --- Streamlit UI 구성 ---

st.title("✨ 마법 공식 백테스팅 툴")

# 데이터 로드
stocks_df, financial_df, charts_df = load_data_from_db()

if stocks_df is None:
    st.error(
        f"'{DB_NAME}'를 찾을 수 없습니다. 먼저 manage_data.py를 실행하여 데이터를 수집해주세요."
    )
else:
    # --- 사이드바: 사용자 입력 ---
    st.sidebar.header("⚙️ 시뮬레이션 설정")
    start_date = st.sidebar.date_input(
        "시작일",
        datetime(2023, 1, 1),
        min_value=charts_df["date"].min(),
        max_value=charts_df["date"].max(),
    )
    end_date = st.sidebar.date_input(
        "종료일",
        datetime(2023, 12, 31),
        min_value=charts_df["date"].min(),
        max_value=charts_df["date"].max(),
    )
    initial_capital = st.sidebar.number_input(
        "초기 자본금", min_value=1000000, value=10000000, step=1000000
    )
    num_stocks = st.sidebar.slider(
        "포트폴리오 종목 수", min_value=5, max_value=50, value=20
    )
    rebalance_period = st.sidebar.selectbox("리밸런싱 주기", ("월간", "분기별"))

    # --- 메인 화면: 결과 표시 ---
    if st.sidebar.button("백테스팅 실행"):
        with st.spinner("백테스팅을 실행 중입니다... 잠시만 기다려주세요."):
            result_df = run_backtest(
                start_date,
                end_date,
                initial_capital,
                num_stocks,
                rebalance_period,
                financial_df,
                charts_df,
                stocks_df,
            )

        if not result_df.empty:
            st.success("백테스팅 완료!")

            # 최종 결과 지표
            final_value = result_df["value"].iloc[-1]
            total_return = (final_value - initial_capital) / initial_capital * 100

            col1, col2 = st.columns(2)
            col1.metric("최종 자산", f"{final_value:,.0f} 원")
            col2.metric("총 수익률", f"{total_return:.2f} %")

            # 포트폴리오 가치 변화 차트
            st.subheader("포트폴리오 가치 변화")
            fig = px.line(
                result_df,
                x="date",
                y="value",
                title="기간별 자산 변화 그래프",
                labels={"date": "날짜", "value": "자산 가치"},
            )
            st.plotly_chart(fig)
        else:
            st.warning(
                "선택하신 기간에 대한 데이터가 부족하여 결과를 표시할 수 없습니다."
            )
