import json
from collections import defaultdict

# ── INCIDENT LABELS ────────────────────────────────────────────────────────────
# Human readable metadata for each known pattern type.
# The AI explanation layer reads description and affected_security_context
# as part of its reasoning context.

INCIDENT_LABELS = {
    "PARTIAL_SETTLEMENT": {
        "name": "Partial Settlement — Position Updated, Cash Not Cleared",
        "description": (
            "Accounts where position records updated following a trade but the "
            "corresponding cash debit has not cleared. The trade executed and the "
            "position is live in the account, but the cash that should have left "
            "the account is still showing as available. Likely cause: custody feed "
            "updated positions faster than cash settlement confirmation arrived. "
            "Common during high-volume market open periods when batch processing "
            "creates timing gaps between subsystems."
        ),
        "affected_security_context": "XEF.TO (iShares MSCI EAFE ETF) — RBC Custody"
    },
    "STALE_PRICING": {
        "name": "Stale Pricing — Holdings Valued at Prior NAV",
        "description": (
            "Accounts where position market value was calculated using yesterday's "
            "NAV despite updated pricing being available today. The pricing feed "
            "from the data vendor did not propagate to the position valuation engine "
            "on time. Every account holding this security is affected simultaneously "
            "because the failure is at the feed level, not the account level. "
            "Client portfolio balances, performance reporting, and any rebalancing "
            "logic reading these values is working with incorrect data."
        ),
        "affected_security_context": "XEQT.TO (iShares Core Equity ETF) — pricing feed lag"
    },
    "FRACTIONAL_ROUNDING": {
        "name": "Fractional Share Rounding — OMS vs Position System Discrepancy",
        "description": (
            "Accounts where the unit count recorded by the order management system "
            "differs from the unit count recorded by the position system. Root cause "
            "is a decimal precision mismatch at the boundary between systems — OMS "
            "stores fractional units to 4 decimal places while the position system "
            "truncates to 2. Each individual discrepancy is small but at scale across "
            "all accounts holding this security the aggregate unreconciled position "
            "value becomes material. Unique to Wealthsimple's fractional share model — "
            "traditional whole-share brokerages do not face this failure mode."
        ),
        "affected_security_context": "VFV.TO (Vanguard S&P 500 ETF) — OMS/position system boundary"
    },
    "NOVEL_TFSA_TRACKER_LAG": {
        "name": "NOVEL: TFSA Contribution Room Tracker Desync — Regulatory Risk",
        "description": (
            "TFSA accounts where a deposit was successfully processed to the cash "
            "balance but the contribution room tracker microservice was not updated. "
            "Root cause: message queue failure prevented the deposit event from "
            "reaching the tracker. The tracker still shows pre-deposit room as "
            "available. If the client makes another deposit believing they have full "
            "room remaining, they will over-contribute and trigger a CRA 1% monthly "
            "penalty tax on the excess amount. This failure requires cross-referencing "
            "three separate data sources simultaneously — transaction log, cash balance, "
            "and contribution tracker — which no single-field validation rule is designed "
            "to do. Novel pattern: does not match any known historical failure mode."
        ),
        "affected_security_context": "TFSA contribution tracker microservice — message queue failure"
    }
}


# ── CLUSTERING FUNCTION ────────────────────────────────────────────────────────

def cluster_anomalies(
    detected_path="data/detected.json",
    output_path="data/incidents.json"
):
    with open(detected_path) as f:
        detected = json.load(f)

    # Group accounts by their primary anomaly pattern
    clusters = defaultdict(list)
    for account in detected:
        pattern = account.get("anomaly_pattern", "UNKNOWN")
        clusters[pattern].append(account)

    # Build one incident object per cluster
    incidents = []
    for pattern, affected_accounts in clusters.items():
        label = INCIDENT_LABELS.get(pattern, {
            "name": pattern,
            "description": "Unknown pattern detected.",
            "affected_security_context": "Unknown"
        })

        # Aggregate financial exposure across all affected accounts
        total_exposure = sum(
            a.get("total_market_value", 0) for a in affected_accounts
        )

        # Aggregate anomaly magnitudes
        magnitudes = []
        for a in affected_accounts:
            for flag in a.get("flags", []):
                if flag.get("magnitude") is not None:
                    magnitudes.append(flag["magnitude"])
        avg_magnitude = round(sum(magnitudes) / len(magnitudes), 2) if magnitudes else 0

        # Break down affected accounts by account type
        type_breakdown = {"TFSA": 0, "RRSP": 0, "Personal": 0}
        for a in affected_accounts:
            acct_type = a.get("account_type", "Personal")
            if acct_type in type_breakdown:
                type_breakdown[acct_type] += 1

        # Get the most detailed flag from the first affected account
        # as a concrete example for the AI explanation layer
        sample_flag = ""
        if affected_accounts and affected_accounts[0].get("flags"):
            sample_flag = affected_accounts[0]["flags"][0].get("detail", "")

        # Mark novel incidents explicitly
        is_novel = pattern == "NOVEL_TFSA_TRACKER_LAG"

        # Generate a clean incident ID
        pattern_abbrev = pattern[:4].upper()
        incident_id = f"INC-{pattern_abbrev}-{len(affected_accounts):03d}"

        incidents.append({
            "incident_id": incident_id,
            "check_type": pattern,
            "incident_name": label["name"],
            "description": label["description"],
            "affected_security_context": label["affected_security_context"],
            "accounts_affected": len(affected_accounts),
            "account_ids": [a["account_id"] for a in affected_accounts],
            "account_types_breakdown": type_breakdown,
            "total_portfolio_exposure": round(total_exposure, 2),
            "average_magnitude": avg_magnitude,
            "sample_flag_detail": sample_flag,
            "is_novel": is_novel,
            "severity": None,        # filled by AI explanation layer in Day 3
            "ai_explanation": None   # filled by AI explanation layer in Day 3
        })

    # Sort: novel incidents first, then by accounts affected descending
    incidents.sort(key=lambda x: (not x["is_novel"], -x["accounts_affected"]))

    # Save output
    with open(output_path, "w") as f:
        json.dump(incidents, f, indent=2)

    # Print summary
    print(f"\nClustering complete")
    print(f"  Anomalous accounts:    {len(detected)}")
    print(f"  Systemic incidents:    {len(incidents)}")
    print(f"  Compression ratio:     {len(detected)}:1 → {len(incidents)}:1")
    print(f"\nIncidents (sorted by priority):")
    for inc in incidents:
        novel_tag = " ◆ NOVEL" if inc["is_novel"] else ""
        print(
            f"  {inc['incident_id']}: "
            f"{inc['accounts_affected']} accounts — "
            f"${inc['total_portfolio_exposure']:,.2f} exposure"
            f"{novel_tag}"
        )
    print(f"\nSaved to {output_path}")
    return incidents


if __name__ == "__main__":
    cluster_anomalies()