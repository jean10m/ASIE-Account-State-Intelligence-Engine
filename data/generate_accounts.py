import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json

np.random.seed(42)
random.seed(42)

# ── CONFIGURATION ──────────────────────────────────────────────────────────────

N_ACCOUNTS = 500
TODAY = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
YESTERDAY = TODAY - timedelta(days=1)

ACCOUNT_TYPES = ["TFSA", "RRSP", "Personal"]

SECURITIES = [
    {"id": "XEF.TO",  "name": "iShares MSCI EAFE ETF",    "type": "Equity ETF",       "nav": 32.41,  "nav_yesterday": 32.41},
    {"id": "ZAG.TO",  "name": "BMO Aggregate Bond ETF",    "type": "Fixed Income ETF", "nav": 15.87,  "nav_yesterday": 15.87},
    {"id": "XEQT.TO", "name": "iShares Core Equity ETF",  "type": "Equity ETF",       "nav": 29.55,  "nav_yesterday": 29.22},
    {"id": "BTC",     "name": "Bitcoin",                   "type": "Crypto",           "nav": 62400,  "nav_yesterday": 61800},
    {"id": "VFV.TO",  "name": "Vanguard S&P 500 ETF",     "type": "Equity ETF",       "nav": 118.33, "nav_yesterday": 117.90},
    {"id": "CASH.TO", "name": "Purpose Cash ETF",          "type": "Cash ETF",         "nav": 50.01,  "nav_yesterday": 50.01},
]
SEC_MAP = {s["id"]: s for s in SECURITIES}

TFSA_ANNUAL_LIMIT = 7000

# ── ACCOUNT GENERATION ────────────────────────────────────────────────────────

def make_account(account_id):
    acct_type = random.choice(ACCOUNT_TYPES)
    holdings = random.sample(SECURITIES, random.randint(1, 4))

    positions = {}
    total_market_value = 0

    # Realistic retail portfolio: total value between $2,000 and $35,000
    # Units are back-calculated from dollar allocation — not generated directly
    # This ensures exposure numbers reflect real Wealthsimple retail clients
    target_portfolio_value = round(random.uniform(2000, 35000), 2)
    remaining_value = target_portfolio_value

    for i, sec in enumerate(holdings):
        # Last security gets whatever value remains
        if i == len(holdings) - 1:
            allocation = remaining_value
        else:
            allocation = round(random.uniform(
                remaining_value * 0.1,
                remaining_value * 0.6
            ), 2)
            remaining_value = round(remaining_value - allocation, 2)

        # Back-calculate units from dollar allocation
        units = round(allocation / sec["nav"], 4)
        mv = round(units * sec["nav"], 2)
        positions[sec["id"]] = {
            "units": units,
            "units_position_system": units,
            "nav_used": sec["nav"],
            "market_value": mv,
            "settlement_status": "SETTLED",
        }
        total_market_value += mv

    cash_balance = round(random.uniform(0, 2000), 2)
    pending_cash = 0.0

    tfsa_contributions_ytd = round(random.uniform(0, 6500), 2) if acct_type == "TFSA" else None
    tfsa_room_remaining = round(TFSA_ANNUAL_LIMIT - tfsa_contributions_ytd, 2) if acct_type == "TFSA" else None
    tfsa_room_tracker_synced = True

    return {
        "account_id": f"WS{account_id:05d}",
        "account_type": acct_type,
        "positions": positions,
        "cash_balance": cash_balance,
        "pending_cash": pending_cash,
        "total_market_value": round(total_market_value, 2),
        "last_transaction_date": (TODAY - timedelta(days=random.randint(0, 5))).strftime("%Y-%m-%d"),
        "last_settlement_date": (TODAY - timedelta(days=random.randint(1, 3))).strftime("%Y-%m-%d"),
        "tfsa_contributions_ytd": tfsa_contributions_ytd,
        "tfsa_room_remaining": tfsa_room_remaining,
        "tfsa_room_tracker_synced": tfsa_room_tracker_synced,
        "anomaly_flags": [],
        "anomaly_pattern": None,
        "is_clean": True
    }

accounts = [make_account(i) for i in range(1, N_ACCOUNTS + 1)]

# ── BREAK INJECTION ───────────────────────────────────────────────────────────

def accounts_holding(sec_id, n):
    eligible = [a for a in accounts if sec_id in a["positions"]]
    if len(eligible) < n:
        for a in random.sample([x for x in accounts if sec_id not in x["positions"]], n - len(eligible)):
            allocation = round(random.uniform(500, 5000), 2)
            units = round(allocation / SEC_MAP[sec_id]["nav"], 4)
            mv = round(units * SEC_MAP[sec_id]["nav"], 2)
            a["positions"][sec_id] = {
                "units": units,
                "units_position_system": units,
                "nav_used": SEC_MAP[sec_id]["nav"],
                "market_value": mv,
                "settlement_status": "SETTLED"
            }
            a["total_market_value"] = round(a["total_market_value"] + mv, 2)
            eligible.append(a)
    return random.sample(eligible, n)

# ── PATTERN 1: Partial settlement ─────────────────────────────────────────────
pattern1_accounts = accounts_holding("XEF.TO", 23)
for a in pattern1_accounts:
    trade_value = round(a["positions"]["XEF.TO"]["units"] * SEC_MAP["XEF.TO"]["nav"], 2)
    a["positions"]["XEF.TO"]["settlement_status"] = "PENDING"
    a["pending_cash"] = round(a["pending_cash"] - trade_value, 2)
    a["anomaly_flags"].append("POSITION_UPDATED_CASH_NOT_CLEARED")
    a["anomaly_pattern"] = "PARTIAL_SETTLEMENT"
    a["is_clean"] = False

# ── PATTERN 2: Stale pricing ──────────────────────────────────────────────────
pattern2_accounts = accounts_holding("XEQT.TO", 31)
for a in pattern2_accounts:
    stale_nav = SEC_MAP["XEQT.TO"]["nav_yesterday"]
    current_nav = SEC_MAP["XEQT.TO"]["nav"]
    units = a["positions"]["XEQT.TO"]["units"]
    stale_mv = round(units * stale_nav, 2)
    correct_mv = round(units * current_nav, 2)
    a["positions"]["XEQT.TO"]["nav_used"] = stale_nav
    a["positions"]["XEQT.TO"]["market_value"] = stale_mv
    a["total_market_value"] = round(a["total_market_value"] - correct_mv + stale_mv, 2)
    a["anomaly_flags"].append("STALE_NAV_IN_POSITION_VALUATION")
    a["anomaly_pattern"] = "STALE_PRICING"
    a["is_clean"] = False

# ── PATTERN 3: Fractional share rounding ──────────────────────────────────────
pattern3_accounts = accounts_holding("VFV.TO", 17)
for a in pattern3_accounts:
    true_units = a["positions"]["VFV.TO"]["units"]
    position_system_units = round(float(f"{true_units:.2f}"), 2)
    rounding_diff = round(abs(true_units - position_system_units), 4)
    rounding_value = round(rounding_diff * SEC_MAP["VFV.TO"]["nav"], 4)
    a["positions"]["VFV.TO"]["units_position_system"] = position_system_units
    a["anomaly_flags"].append(
        f"FRACTIONAL_ROUNDING_MISMATCH_OMS_{true_units}_vs_POSITION_{position_system_units}_diff_${rounding_value}"
    )
    a["anomaly_pattern"] = "FRACTIONAL_ROUNDING"
    a["is_clean"] = False

# ── PATTERN 4: NOVEL — TFSA contribution room tracker lag ─────────────────────
tfsa_accounts = [a for a in accounts if a["account_type"] == "TFSA" and a["is_clean"]]
pattern4_accounts = random.sample(tfsa_accounts, min(11, len(tfsa_accounts)))
for a in pattern4_accounts:
    deposit_amount = round(random.uniform(500, 2000), 2)
    a["cash_balance"] = round(a["cash_balance"] + deposit_amount, 2)
    a["tfsa_room_tracker_synced"] = False
    a["tfsa_deposit_untracked"] = deposit_amount
    a["tfsa_over_contribution_risk"] = round(
        max(0, (a["tfsa_contributions_ytd"] + deposit_amount) - TFSA_ANNUAL_LIMIT), 2
    )
    a["anomaly_flags"].append(
        f"TFSA_CONTRIBUTION_TRACKER_NOT_UPDATED_deposit_${deposit_amount}_room_tracker_stale"
    )
    a["anomaly_pattern"] = "NOVEL_TFSA_TRACKER_LAG"
    a["is_clean"] = False

# ── SAVE ──────────────────────────────────────────────────────────────────────

with open("data/accounts.json", "w") as f:
    json.dump(accounts, f, indent=2)

clean = sum(1 for a in accounts if a["is_clean"])
broken = N_ACCOUNTS - clean

print(f"Generated {N_ACCOUNTS} accounts")
print(f"  Clean:     {clean}")
print(f"  Anomalous: {broken}")
print(f"  Pattern 1 (Partial Settlement):             {len(pattern1_accounts)} accounts")
print(f"  Pattern 2 (Stale Pricing):                  {len(pattern2_accounts)} accounts")
print(f"  Pattern 3 (Fractional Share Rounding):      {len(pattern3_accounts)} accounts")
print(f"  Pattern 4 (NOVEL — TFSA Tracker Lag):       {len(pattern4_accounts)} accounts")
print(f"\nSaved to data/accounts.json")