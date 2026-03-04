import json
from datetime import datetime, timedelta

# ── REFERENCE DATA ─────────────────────────────────────────────────────────────
TODAY = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

CURRENT_NAVS = {
    "XEF.TO":  32.41,
    "ZAG.TO":  15.87,
    "XEQT.TO": 29.55,
    "BTC":     62400,
    "VFV.TO":  118.33,
    "CASH.TO": 50.01
}

YESTERDAY_NAVS = {
    "XEF.TO":  32.41,
    "ZAG.TO":  15.87,
    "XEQT.TO": 29.22,
    "BTC":     61800,
    "VFV.TO":  117.90,
    "CASH.TO": 50.01
}

STALE_NAV_DRIFT_THRESHOLD = 0.01
ROUNDING_THRESHOLD = 0.001
TFSA_ANNUAL_LIMIT = 7000


def detect_account_anomalies(account: dict) -> list:
    flags = []
    positions = account.get("positions", {})
    pending_cash = account.get("pending_cash", 0)

    # CHECK 1: Partial Settlement
    for sec_id, pos in positions.items():
        settlement = pos.get("settlement_status", "SETTLED")
        if settlement == "PENDING" and pending_cash < 0:
            flags.append({
                "check": "PARTIAL_SETTLEMENT",
                "security_id": sec_id,
                "detail": (
                    f"Position updated for {sec_id} but cash debit not cleared. "
                    f"Settlement status PENDING with negative pending cash of "
                    f"${abs(pending_cash):,.2f}. Position exists in account "
                    f"confirming trade executed. Three-way inconsistency detected."
                ),
                "magnitude": abs(pending_cash)
            })
            break

    # CHECK 2: Stale Pricing
    for sec_id, pos in positions.items():
        nav_used = pos.get("nav_used", 0)
        current_nav = CURRENT_NAVS.get(sec_id, nav_used)
        yesterday_nav = YESTERDAY_NAVS.get(sec_id, nav_used)
        matches_yesterday = abs(nav_used - yesterday_nav) < 0.001
        drift = abs(current_nav - yesterday_nav) / yesterday_nav if yesterday_nav > 0 else 0
        price_moved_today = drift > STALE_NAV_DRIFT_THRESHOLD

        if matches_yesterday and price_moved_today:
            drift_pct = round(drift * 100, 2)
            stale_mv = round(pos.get("units", 0) * yesterday_nav, 2)
            correct_mv = round(pos.get("units", 0) * current_nav, 2)
            dollar_impact = round(abs(correct_mv - stale_mv), 2)
            flags.append({
                "check": "STALE_PRICING",
                "security_id": sec_id,
                "detail": (
                    f"{sec_id} position valued at yesterday NAV of ${yesterday_nav} "
                    f"while current NAV is ${current_nav} — a {drift_pct}% drift. "
                    f"Position market value understated by ${dollar_impact:,.2f}."
                ),
                "magnitude": drift_pct
            })

    # CHECK 3: Fractional Share Rounding
    for sec_id, pos in positions.items():
        units_oms = pos.get("units", 0)
        units_pos = pos.get("units_position_system", units_oms)
        rounding_diff = abs(units_oms - units_pos)

        if rounding_diff > ROUNDING_THRESHOLD:
            current_nav = CURRENT_NAVS.get(sec_id, 0)
            dollar_discrepancy = round(rounding_diff * current_nav, 4)
            flags.append({
                "check": "FRACTIONAL_ROUNDING",
                "security_id": sec_id,
                "detail": (
                    f"OMS records {units_oms} units of {sec_id} but position "
                    f"system records {units_pos} units. Discrepancy of "
                    f"{round(rounding_diff, 4)} units = ${dollar_discrepancy} "
                    f"at current NAV of ${current_nav}."
                ),
                "magnitude": dollar_discrepancy
            })

    # CHECK 4: TFSA Tracker Lag
    if account.get("account_type") == "TFSA":
        tracker_synced = account.get("tfsa_room_tracker_synced", True)
        if not tracker_synced:
            deposit = account.get("tfsa_deposit_untracked", 0)
            over_contribution_risk = account.get("tfsa_over_contribution_risk", 0)
            ytd = account.get("tfsa_contributions_ytd", 0)

            if over_contribution_risk > 0:
                regulatory_note = (
                    f"IMMEDIATE REGULATORY RISK: Client over annual limit "
                    f"by ${over_contribution_risk:,.2f}. CRA 1% monthly penalty applies."
                )
            else:
                room_after_deposit = round(TFSA_ANNUAL_LIMIT - ytd - deposit, 2)
                regulatory_note = (
                    f"True room remaining is ${room_after_deposit:,.2f} "
                    f"but tracker shows ${round(TFSA_ANNUAL_LIMIT - ytd, 2):,.2f}. "
                    f"Next deposit will be based on incorrect room data."
                )

            flags.append({
                "check": "NOVEL_TFSA_TRACKER_LAG",
                "security_id": "TFSA_CONTRIBUTION_TRACKER",
                "detail": (
                    f"Deposit of ${deposit:,.2f} processed to cash balance "
                    f"but contribution room tracker not updated. "
                    f"Cross-system desync between core banking and "
                    f"contribution tracker microservice. {regulatory_note}"
                ),
                "magnitude": deposit
            })

    return flags


def run_detection(
    accounts_path="data/accounts.json",
    output_path="data/detected.json"
):
    with open(accounts_path) as f:
        accounts = json.load(f)

    detected = []
    for account in accounts:
        flags = detect_account_anomalies(account)
        if flags:
            detected.append({
                "account_id": account["account_id"],
                "account_type": account["account_type"],
                "flags": flags,
                "total_market_value": account["total_market_value"],
                "anomaly_pattern": account.get("anomaly_pattern"),
            })

    with open(output_path, "w") as f:
        json.dump(detected, f, indent=2)

    print(f"\nDetection complete")
    print(f"  Total accounts scanned:   {len(accounts)}")
    print(f"  Anomalous accounts found: {len(detected)}")
    print(f"  Clean accounts:           {len(accounts) - len(detected)}")
    print(f"\nBreakdown by pattern:")

    pattern_counts = {}
    for a in detected:
        p = a.get("anomaly_pattern", "UNKNOWN")
        pattern_counts[p] = pattern_counts.get(p, 0) + 1
    for pattern, count in pattern_counts.items():
        print(f"  {pattern}: {count} accounts")

    print(f"\nSaved to {output_path}")
    return detected


if __name__ == "__main__":
    run_detection()