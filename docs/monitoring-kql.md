# Monitoring (KQL)

## Goals
Track AI decision risk as operational signals:
- pipeline failure rate
- spike in rejects
- missing evidence
- model version churn
- confidence distribution shifts

## Queries to implement (placeholders)
1) ADF pipeline failures (Decision Evidence pipelines)
2) Reject rate trend (day over day)
3) Missing confidence_score events
4) New model_version frequency (unexpected churn)
5) Confidence score p50/p95 trend (drift proxy)

> Once the Log Analytics table names are confirmed in your workspace, these become copy/paste KQL.

