# kiwoom_rest_bot/data/database_manager.py
import sqlite3
import logging

class DatabaseManager:
    """SQLite 데이터베이스 작업을 관리합니다. (일봉 데이터 관리 기능 포함)"""

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        """DB 테이블들을 생성합니다."""
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS stocks (ticker TEXT PRIMARY KEY, name TEXT)"
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS financial_info (
                ticker TEXT, business_year INTEGER,
                roe REAL, ev_ebitda REAL,
                mac REAL, net_income REAL, total_equity REAL,
                operating_income REAL, depreciation REAL, amortization REAL,
                total_debt REAL, cash_and_equivalents REAL,
                PRIMARY KEY (ticker, business_year)
            )"""
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_charts (
                ticker TEXT, date TEXT, open REAL, high REAL,
                low REAL, close REAL, volume INTEGER,
                PRIMARY KEY (ticker, date)
            )"""
        )
        self.conn.commit()

    def get_all_tickers(self):
        return self.cursor.execute("SELECT ticker, name FROM stocks").fetchall()

    def get_existing_financial_years(self, ticker: str) -> set:
        """DB에 저장된 특정 종목의 재무 데이터 수집 연도들을 반환합니다."""
        self.cursor.execute(
            "SELECT business_year FROM financial_info WHERE ticker=?", (ticker,)
        )
        return {row[0] for row in self.cursor.fetchall()}

    def get_financial_update_targets(
        self, latest_required_year: int
    ) -> list[tuple[str, str]]:
        """가장 최근 연도(latest_required_year)의 재무 정보가 없는 종목 목록을 반환합니다."""
        total_stocks_count = self.cursor.execute(
            "SELECT COUNT(*) FROM stocks"
        ).fetchone()[0]
        if total_stocks_count == 0:
            return []

        # 가장 최근 필수 연도의 데이터가 없는 종목을 찾는 쿼리 (LEFT JOIN 사용)
        query = """
            SELECT s.ticker, s.name
            FROM stocks s
            LEFT JOIN financial_info fi ON s.ticker = fi.ticker AND fi.business_year = ?
            WHERE fi.ticker IS NULL
        """
        self.cursor.execute(query, (latest_required_year,))
        targets = self.cursor.fetchall()

        num_targets = len(targets)
        num_complete = total_stocks_count - num_targets

        if num_targets == 0:
            logging.info(
                f"✅ 모든 {total_stocks_count}개 종목의 재무 정보가 최신({latest_required_year}년 기준)입니다. 수집을 건너뜁니다."
            )
        else:
            logging.info(
                f"전체 {total_stocks_count}개 중 {num_complete}개 최신. {num_targets}개 종목의 재무 정보 업데이트를 시작합니다."
            )

        return targets

    def update_tickers(self, tickers):
        self.cursor.executemany(
            "INSERT OR IGNORE INTO stocks (ticker, name) VALUES (?, ?)", tickers
        )
        self.conn.commit()
        logging.info(f"{len(tickers)}개 종목 정보를 DB에 업데이트했습니다.")

    def update_financial_info(self, data):
        self.cursor.execute(
            """
            INSERT OR REPLACE INTO financial_info (
                ticker, business_year, roe, ev_ebitda, mac, net_income, total_equity,
                operating_income, depreciation, amortization, total_debt, cash_and_equivalents
            ) VALUES (
                :ticker, :business_year, :roe, :ev_ebitda, :mac, :net_income, :total_equity,
                :operating_income, :depreciation, :amortization, :total_debt, :cash_and_equivalents
            )""",
            data,
        )

    def get_latest_chart_date(self, ticker):
        """DB에 저장된 특정 종목의 가장 마지막 일봉 날짜를 반환합니다."""
        self.cursor.execute(
            "SELECT MAX(date) FROM daily_charts WHERE ticker=?", (ticker,)
        )
        result = self.cursor.fetchone()
        return result[0] if result and result[0] else None

    def get_overall_latest_chart_date(self) -> str | None:
        """daily_charts 테이블의 모든 종목 중 가장 마지막 날짜를 반환합니다."""
        self.cursor.execute("SELECT MAX(date) FROM daily_charts")
        result = self.cursor.fetchone()
        return result[0] if result and result[0] else None

    def get_tickers_for_date(self, date_str: str) -> set:
        """특정 날짜에 데이터가 있는 모든 티커를 반환합니다."""
        self.cursor.execute("SELECT ticker FROM daily_charts WHERE date = ?", (date_str,))
        return {row[0] for row in self.cursor.fetchall()}

    def update_daily_charts(self, chart_data):
        """일봉 데이터 리스트를 DB에 저장합니다."""
        self.cursor.executemany(
            """
            INSERT OR IGNORE INTO daily_charts (ticker, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            chart_data,
        )

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def clear_stocks_table(self):
        """'stocks' 테이블의 모든 데이터를 삭제합니다."""
        self.cursor.execute("DELETE FROM stocks")
        logging.info("'stocks' 테이블의 모든 데이터를 삭제했습니다.")
        self.conn.commit()
