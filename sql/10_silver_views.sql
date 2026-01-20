-- These represent view contracts that sit on top of silver parquet or external tables.
-- Keep the naming consistent with your Grooming platform.

-- Example: silver.v_ai_decisions_clean
-- (Pseudo-SQL: adapt to Synapse Serverless OPENROWSET as needed)

-- risk_band normalization and facility default show governance at access
-- NOTE: Keep logic deterministic; do not hide rejects.

-- silver.v_ai_decisions_rejects is used for quality reporting and drilldowns.

