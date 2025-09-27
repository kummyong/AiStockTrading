import sqlite3
import pandas as pd
import os

# --- 설정 ---
DB_NAME = "stocks.db"
# 상위 몇 개 종목을 출력할지 결정합니다.
TOP_N = 30
# -----------


def analyze_magic_formula():
    """
    데이터베이스에서 재무 정보를 읽어와 마법 공식에 따라 종목 순위를 매깁니다.
    """
    # 1. 데이터베이스 연결 및 데이터 로드
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_NAME)
    if not os.path.exists(db_path):
        print(
            f"'{DB_NAME}'를 찾을 수 없습니다. 먼저 manage_data.py를 실행하여 데이터를 수집해주세요."
        )
        return

    conn = sqlite3.connect(db_path)

    # SQL 쿼리를 통해 stocks와 financial_info 테이블을 ticker 기준으로 합칩니다.
    query = """
    SELECT
        s.ticker,
        s.name,
        f.roe,
        f.ev_ebitda
    FROM
        financial_info f
    JOIN
        stocks s ON s.ticker = f.ticker
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    print(f"총 {len(df)}개 종목의 재무 정보를 불러왔습니다.")

    # 2. 분석에 부적합한 데이터 필터링
    #    - ROE나 EV/EBITDA 값이 없거나, 0 이하인 경우는 분석에서 제외합니다.
    #      (수익을 내지 못하거나, 비교 기준이 없는 기업은 제외)
    df_filtered = df.dropna(subset=["roe", "ev_ebitda"])
    df_filtered = df_filtered[(df_filtered["roe"] > 0) & (df_filtered["ev_ebitda"] > 0)]

    if len(df_filtered) < TOP_N:
        print("분석 가능한 유효 데이터가 너무 적습니다. 데이터를 더 수집해주세요.")
        return

    print(f"필터링 후 {len(df_filtered)}개 종목으로 분석을 시작합니다.")

    # 3. 마법 공식 순위 계산
    #    - 자본 수익률 순위: ROE가 높을수록 순위가 높다 (내림차순)
    #    - 이익 수익률 순위: EV/EBITDA가 낮을수록 순위가 높다 (오름차순)
    df_filtered["rank_roe"] = df_filtered["roe"].rank(ascending=False)
    df_filtered["rank_ev_ebitda"] = df_filtered["ev_ebitda"].rank(ascending=True)

    # 4. 두 순위를 합산하여 최종 순위 계산
    df_filtered["rank_total"] = df_filtered["rank_roe"] + df_filtered["rank_ev_ebitda"]

    # 5. 최종 순위를 기준으로 정렬
    df_final = df_filtered.sort_values(by="rank_total")

    # 6. 결과 출력
    print("\n--- ✨ 마법 공식 분석 결과 (상위", TOP_N, "개) ---")
    print(df_final.head(TOP_N).to_string(index=False))


if __name__ == "__main__":
    analyze_magic_formula()
