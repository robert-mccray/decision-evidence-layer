-- Logical schema documentation (works as a “contract” even if you don’t deploy SQL tables)

-- SILVER clean fields (typed + normalized)
-- decision_id        NVARCHAR(64)
-- decision_type      NVARCHAR(64)
-- model_version      NVARCHAR(64)
-- confidence_score   FLOAT
-- risk_band          NVARCHAR(16)  -- LOW/MEDIUM/HIGH
-- policy_id          NVARCHAR(64)
-- facility_code      NVARCHAR(32)  -- default UNKNOWN
-- decision_ts        DATETIME2
-- input_features_hash NVARCHAR(128) NULL
-- override_flag      BIT NULL
-- override_reason_code NVARCHAR(64) NULL

-- REJECTS fields
-- rejected_at        DATETIME2
-- decision_id        NVARCHAR(64) NULL
-- reject_reason      NVARCHAR(64)
-- raw_payload        NVARCHAR(MAX)

