# kiwoom_rest_bot/data/metrics_calculator.py

def calculate_metrics(kiwoom_info, dart_info):
    """수집한 정보로 ROE, EV/EBITDA를 계산합니다."""
    if not dart_info:
        return None, None

    # ROE 계산
    roe = None
    net_income = dart_info.get("net_income")
    total_equity = dart_info.get("total_equity")
    if net_income is not None and total_equity is not None and total_equity > 0:
        roe = (net_income / total_equity) * 100

    # EV/EBITDA 계산
    ev_ebitda = None
    if kiwoom_info:
        market_cap = kiwoom_info.get("mac")
        total_debt = dart_info.get("total_debt")
        cash = dart_info.get("cash_and_equivalents")
        operating_income = dart_info.get("operating_income")
        depreciation = dart_info.get("depreciation", 0)
        amortization = dart_info.get("amortization", 0)

        if all(v is not None for v in [market_cap, total_debt, cash, operating_income]):
            ev = market_cap + total_debt - cash
            ebitda = operating_income + depreciation + amortization
            if ev > 0 and ebitda > 0:
                ev_ebitda = ev / ebitda

    return roe, ev_ebitda
