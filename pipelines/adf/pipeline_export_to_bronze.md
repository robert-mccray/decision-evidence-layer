# ADF Pipeline: Export to Bronze

Purpose: preserve raw decision events for lineage/replay.

Steps:
1) Source: Landing JSONL OR SQL staging (synthetic)
2) Copy activity → ADLS bronze/ai_decisions/decision_events/
3) Partition by date if available (decision_ts)

