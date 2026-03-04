from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import json
import csv
import os
from datetime import datetime
from pydantic import BaseModel

app = FastAPI(title="ASIE — Account State Intelligence Engine")

# ── CORS ───────────────────────────────────────────────────────────────────────
# Allow the frontend to call the API from any origin during development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DATA PATHS ─────────────────────────────────────────────────────────────────
EXPLAINED_INCIDENTS_PATH = "data/explained_incidents.json"
ACCOUNTS_PATH = "data/accounts.json"
OVERRIDES_PATH = "logs/overrides.csv"


# ── HELPERS ────────────────────────────────────────────────────────────────────

def load_incidents():
    with open(EXPLAINED_INCIDENTS_PATH) as f:
        return json.load(f)

def load_accounts():
    with open(ACCOUNTS_PATH) as f:
        return json.load(f)


# ── MODELS ─────────────────────────────────────────────────────────────────────

class OverrideLog(BaseModel):
    incident_id: str
    check_type: str
    ai_severity: str
    accounts_affected: int
    operator_decision: str   # ESCALATE | RESOLVE | MONITOR | DISMISS
    operator_notes: str


# ── ROUTES ─────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "system": "ASIE — Account State Intelligence Engine",
        "status": "operational",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/incidents")
def get_incidents():
    """
    Returns all explained incidents sorted by severity and novelty.
    This is the main endpoint the dashboard triage queue reads from.
    """
    incidents = load_incidents()
    return {
        "total_incidents": len(incidents),
        "generated_at": datetime.now().isoformat(),
        "incidents": incidents
    }


@app.get("/incidents/{incident_id}")
def get_incident(incident_id: str):
    """
    Returns a single incident by ID with full AI explanation.
    Called when operator clicks into an incident from the triage queue.
    """
    incidents = load_incidents()
    for inc in incidents:
        if inc["incident_id"] == incident_id:
            return inc
    raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")


@app.get("/incidents/{incident_id}/accounts")
def get_incident_accounts(incident_id: str):
    """
    Returns full account details for all accounts in an incident.
    Called when operator wants to inspect individual accounts within an incident.
    """
    incidents = load_incidents()
    incident = None
    for inc in incidents:
        if inc["incident_id"] == incident_id:
            incident = inc
            break

    if not incident:
        raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")

    account_ids = set(incident["account_ids"])
    all_accounts = load_accounts()
    affected = [a for a in all_accounts if a["account_id"] in account_ids]

    return {
        "incident_id": incident_id,
        "accounts_affected": len(affected),
        "accounts": affected
    }


@app.get("/summary")
def get_summary():
    """
    Returns a high level summary for the dashboard header.
    Shows total accounts monitored, incidents found, exposure, and severity breakdown.
    """
    incidents = load_incidents()
    all_accounts = load_accounts()

    total_exposure = sum(i["total_portfolio_exposure"] for i in incidents)
    severity_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
    for inc in incidents:
        sev = inc.get("severity", "LOW")
        severity_counts[sev] = severity_counts.get(sev, 0) + 1

    novel_count = sum(1 for i in incidents if i.get("is_novel"))
    escalation_count = sum(
        1 for i in incidents
        if i.get("ai_explanation", {}).get("requires_immediate_escalation")
    )
    total_affected_accounts = sum(i["accounts_affected"] for i in incidents)

    return {
        "total_accounts_monitored": len(all_accounts),
        "total_incidents": len(incidents),
        "total_affected_accounts": total_affected_accounts,
        "total_portfolio_exposure": round(total_exposure, 2),
        "severity_breakdown": severity_counts,
        "novel_incidents": novel_count,
        "requires_escalation": escalation_count,
        "generated_at": datetime.now().isoformat()
    }


@app.post("/overrides")
def log_override(override: OverrideLog):
    """
    Logs an operator decision on an incident.
    This is the human-in-the-loop endpoint — AI diagnoses, human authorizes.
    All override decisions are written to logs/overrides.csv for audit trail.
    """
    file_exists = os.path.exists(OVERRIDES_PATH)
    with open(OVERRIDES_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "incident_id", "check_type",
                "ai_severity", "accounts_affected",
                "operator_decision", "operator_notes"
            ])
        writer.writerow([
            datetime.now().isoformat(),
            override.incident_id,
            override.check_type,
            override.ai_severity,
            override.accounts_affected,
            override.operator_decision,
            override.operator_notes
        ])
    return {
        "status": "logged",
        "incident_id": override.incident_id,
        "operator_decision": override.operator_decision,
        "timestamp": datetime.now().isoformat()
    }


@app.get("/overrides")
def get_overrides():
    """
    Returns all operator override decisions.
    Used by the dashboard to show override history and audit trail.
    """
    if not os.path.exists(OVERRIDES_PATH):
        return {"overrides": []}

    overrides = []
    with open(OVERRIDES_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            overrides.append(row)

    return {"overrides": overrides}