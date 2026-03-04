import anthropic
import json
import os
from dotenv import load_dotenv

load_dotenv()
client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

def build_prompt(incident: dict) -> str:
    type_breakdown = incident["account_types_breakdown"]
    tfsa_count = type_breakdown.get("TFSA", 0)
    rrsp_count = type_breakdown.get("RRSP", 0)
    personal_count = type_breakdown.get("Personal", 0)

    novel_tag = "NOVEL PATTERN - no historical protocol exists. Elevated urgency." if incident["is_novel"] else "Known pattern - resolution protocols exist."
    
    reg_tag = ""
    if tfsa_count > 0:
        reg_tag = f"REGULATORY: {tfsa_count} TFSA accounts affected - CRA over-contribution exposure possible."
    if rrsp_count > 0:
        reg_tag += f" {rrsp_count} RRSP accounts - retirement savings integrity at risk."

    return f"""You are ASIE, an AI integrity monitoring system for Wealthsimple, a Canadian retail investing platform.

INCIDENT: {incident['incident_id']}
TYPE: {incident['check_type']}
NAME: {incident['incident_name']}
DESCRIPTION: {incident['description']}
AFFECTED SYSTEM: {incident['affected_security_context']}

SCALE:
- Accounts affected: {incident['accounts_affected']}
- Account types: TFSA={tfsa_count}, RRSP={rrsp_count}, Personal={personal_count}
- Total portfolio exposure: ${incident['total_portfolio_exposure']:,.2f}
- Average anomaly magnitude: {incident['average_magnitude']}

PATTERN: {novel_tag}
{reg_tag}

SAMPLE ANOMALY: {incident['sample_flag_detail'][:300]}

Return ONLY valid JSON, no markdown, no preamble:
{{"severity":"HIGH|MEDIUM|LOW","severity_rationale":"one sentence","root_cause_hypotheses":[{{"rank":1,"hypothesis":"specific cause","confidence":"HIGH|MEDIUM|LOW","reasoning":"why"}},{{"rank":2,"hypothesis":"second cause","confidence":"HIGH|MEDIUM|LOW","reasoning":"why"}}],"recommended_first_action":"specific actionable step naming exact team or system","estimated_resolution_path":"brief resolution sequence","pattern_classification":"KNOWN|NOVEL","pattern_note":"one sentence","requires_immediate_escalation":true,"user_impact_if_unresolved":"what client experiences before market open"}}"""


def explain_incident(incident: dict) -> dict:
    prompt = build_prompt(incident)
    message = client.messages.create(
        model="claude-opus-4-6",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = message.content[0].text.strip()
    
    # Clean markdown fences if present
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                raw = part
                break
    
    raw = raw.strip()
    explanation = json.loads(raw)
    incident["severity"] = explanation["severity"]
    incident["ai_explanation"] = explanation
    return incident


def explain_all_incidents(
    incidents_path="data/incidents.json",
    output_path="data/explained_incidents.json"
):
    with open(incidents_path) as f:
        incidents = json.load(f)

    explained = []
    for i, incident in enumerate(incidents):
        print(f"Explaining incident {i+1}/{len(incidents)}: {incident['incident_id']}...")
        result = explain_incident(incident)
        explained.append(result)

    order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    explained.sort(key=lambda x: (
        order.get(x["severity"], 3),
        not x["is_novel"]
    ))

    with open(output_path, "w") as f:
        json.dump(explained, f, indent=2)

    print(f"\nAll incidents explained and ranked.")
    print(f"\nFinal incident queue:")
    for inc in explained:
        sev = inc["severity"]
        novel_tag = " NOVEL" if inc["is_novel"] else ""
        escalate = " ESCALATE" if inc["ai_explanation"].get("requires_immediate_escalation") else ""
        print(f"  [{sev}] {inc['incident_id']}: {inc['accounts_affected']} accounts — ${inc['total_portfolio_exposure']:,.2f}{novel_tag}{escalate}")

    print(f"\nSaved to {output_path}")
    return explained


if __name__ == "__main__":
    explain_all_incidents()