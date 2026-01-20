# Architecture

## Purpose
When an AI-assisted decision is questioned later, most systems cannot show what evidence existed at decision time.
This layer makes decisions:
- auditable
- reviewable
- measurable
- defensible

## Logical flow
[ Systems producing decisions ]
        |
        v
Decision Events (JSON)
        |
        v
Landing (raw intake)
        |
        v
Bronze (immutable preservation)
        |
        +--> Silver Clean (contract enforced)
        |
        +--> Rejects (reasons + forensic trace)
        |
        v
Gold (audit-ready star schema)
        |
        v
BI / Underwriting Review / Audit Views

## Why this matters
AI risk is often a governance problem before it is a model problem.
The platform enforces minimum evidence standards and produces executive-readable metrics:
- % decisions missing required evidence
- reject rate by reason
- model version churn
- confidence distribution drift proxies

