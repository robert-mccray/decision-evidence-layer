# Decision Evidence Layer (AI Risk Evidence Mini-Platform)

A governed data layer that captures, validates, and surfaces evidence around AI-assisted decisions for audit, underwriting review, and post-incident analysis.

This is **not** an AI product. It is **evidence infrastructure** that makes automated decisions reviewable, auditable, and defensible months later.

## What this demonstrates
- Evidence contracts for automated / AI-assisted decisions
- Bronze/Silver/Gold layering with **rejects-as-first-class data**
- SQL view–enforced governance for analytics consumption
- Operational monitoring hooks (KQL-ready patterns)
- BI-ready facts for executive review (Power BI)

## Architecture (high-level)
**Decision Events (JSON)** → Landing → Bronze (raw preserved) → Silver (validated + rejects) → Gold (star schema facts) → BI / Audit Views

## Tech stack (reference implementation)
- Data Layering: ADLS Gen2 style paths (landing/bronze/silver/gold/rejects)
- Orchestration (conceptual): Azure Data Factory pipelines
- SQL Governance: Serverless SQL views / contracts (Synapse Serverless pattern)
- Monitoring: Log Analytics + KQL queries (pipeline + data quality signals)
- BI: Power BI dashboard (evidence overview)

## Evidence contract (minimum)
Each decision event must include:
- decision_id
- decision_type
- model_version
- confidence_score
- risk_band (LOW | MEDIUM | HIGH)
- policy_id
- facility_code (or default UNKNOWN)
- decision_ts (UTC parseable)

Reject reasons are recorded instead of silently discarding bad data.

## Repo contents
- `data/landing/` synthetic sample inputs (JSONL + CSV)
- `scripts/` local generators and transform scripts (optional)
- `sql/` schema + silver/gold views to show governance contracts
- `pipelines/` ADF pipeline writeups (how you would implement in Azure)
- `docs/` the story: layering, governance, monitoring, dashboard

## Key positioning sentence
> I separate decision-making from decision evidence, so automated or AI-assisted actions remain reviewable, auditable, and defensible long after execution.

## Next steps (if deploying in Azure)
1) Create ADLS containers (landing/bronze/silver/gold/rejects)
2) Add ADF pipelines mirroring `pipelines/adf/*`
3) Create serverless SQL views using `sql/10_silver_views.sql` and `sql/20_gold_views.sql`
4) Build a 1-page Power BI report per `docs/powerbi.md`
