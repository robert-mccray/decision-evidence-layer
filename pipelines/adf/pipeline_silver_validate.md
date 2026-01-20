# ADF Pipeline: Silver Validation

Purpose: enforce evidence contract and split clean vs rejects.

Validations:
- confidence_score exists and numeric
- model_version non-empty
- risk_band in (LOW, MEDIUM, HIGH)
- decision_ts parseable
- facility_code default UNKNOWN when missing

Outputs:
- silver/ai_decisions/decision_events_clean/
- rejects/ai_decisions/decision_events_rejects/ (with reason + raw payload)

