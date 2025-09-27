# strategy.py
import sqlite3
import pandas as pd
import os

# --- 설정 ---
DB_NAME = "stocks.db"
# -----------


def analyze_magic_formula(top_n=20):
    """
    데이터베이스에서 재무 정보를 읽어와 마법 공식에 따라 종목 순위를 매기고,
    상위 종목 ticker 리스트를 반환합니다.

    Args:
        top_n (int): 선정할 상위 종목의 수.

    Returns:
        list: 마법 공식 상위 종목의 ticker 리스트. 분석 실패 시 빈 리스트를 반환합니다.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_NAME)
    if not os.path.exists(db_path):
        print(f"❌ 데이터베이스 파일('{db_path}')을 찾을 수 없습니다. manage_data.py를 먼저 실행해주세요.")
        return []

    try:
        conn = sqlite3.connect(db_path)
        query = """
        SELECT
            s.ticker, s.name, f.roe, f.ev_ebitda
        FROM
            financial_info f
        JOIN
            stocks s ON s.ticker = f.ticker
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        # 1. 분석에 부적합한 데이터 필터링 (ROE > 0, EV/EBITDA > 0)
        df_filtered = df.dropna(subset=["roe", "ev_ebitda"])
        df_filtered = df_filtered[(df_filtered["roe"] > 0) & (df_filtered["ev_ebitda"] > 0)]

        if len(df_filtered) < top_n:
            print(f"⚠️ 분석 가능한 유효 데이터({len(df_filtered)}개)가 목표 종목 수({top_n}개)보다 적습니다.")
            return []

        # 2. 마법 공식 순위 계산
        df_filtered["rank_roe"] = df_filtered["roe"].rank(ascending=False)
        df_filtered["rank_ev_ebitda"] = df_filtered["ev_ebitda"].rank(ascending=True)
        df_filtered["rank_total"] = df_filtered["rank_roe"] + df_filtered["rank_ev_ebitda"]

        # 3. 최종 순위를 기준으로 정렬하고 상위 N개 종목 선정
        df_final = df_filtered.sort_values(by="rank_total")
        top_stocks = df_final.head(top_n)

        print("\n--- ✨ 마법 공식 분석 결과 (투자 목표) ---")
        print(top_stocks[['ticker', 'name', 'roe', 'ev_ebitda', 'rank_total']].to_string(index=False))

        return top_stocks["ticker"].tolist()

    except Exception as e:
        print(f"❌ 마법 공식 분석 중 오류 발생: {e}")
        return []