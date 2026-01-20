#!/usr/bin/env python3
"""
Validate landing decision events into Silver Clean + Rejects.

Inputs:
  data/landing/decision_events/*.jsonl

Outputs:
  data/silver/decision_events_clean.jsonl
  data/rejects/decision_events_rejects.jsonl

Rules (Silver contracts):
- decision_id required
- decision_type required
- model_version required and non-empty
- confidence_score required and numeric [0..1]
- risk_band must be LOW|MEDIUM|HIGH (case-insensitive accepted, normalized)
- policy_id required
- decision_ts must be ISO parseable
- facility_code must be present (default UNKNOWN if missing/blank)

Rejects are written with:
- rejected_at_utc
- reject_reason_code (UPPER)
- reject_reason_detail
- decision_id (if available)
- raw_payload
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


VALID_RISK_BANDS = {"LOW", "MEDIUM", "HIGH"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_str(v: Any) -> str:
    return "" if v is None else str(v)


def _parse_iso_ts(s: Any) -> Optional[str]:
    """
    Accepts ISO strings and returns a normalized Z ISO string.
    Returns None if invalid.
    """
    if s is None:
        return None
    if not isinstance(s, str) or not s.strip():
        return None

    raw = s.strip()

    try:
        # Handle Z
        if raw.endswith("Z"):
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(raw)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        fv = float(v)
        return fv
    except Exception:
        return None


def _iter_jsonl_files(indir: Path) -> Iterable[Path]:
    if not indir.exists():
        return []
    return sorted(indir.glob("*.jsonl"))


def validate_event(raw: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Returns (clean_record, reject_record).
    Exactly one will be non-None.
    """
    rejected_at = _utc_now_iso()
    raw_payload = json.dumps(raw, ensure_ascii=False)

    decision_id = _safe_str(raw.get("decision_id")).strip() or None
    decision_type = _safe_str(raw.get("decision_type")).strip() or None
    model_version = raw.get("model_version")

    # facility_code: must be present, default UNKNOWN if empty
    facility_code_raw = _safe_str(raw.get("facility_code")).strip()
    facility_code = facility_code_raw if facility_code_raw else "UNKNOWN"

    # risk_band normalization
    risk_band_raw = _safe_str(raw.get("risk_band")).strip().upper()
    risk_band = risk_band_raw if risk_band_raw else ""

    policy_id = _safe_str(raw.get("policy_id")).strip() or None
    conf = _as_float(raw.get("confidence_score"))
    decision_ts = _parse_iso_ts(raw.get("decision_ts"))

    # Reject conditions
    if not decision_id:
        return None, {
            "rejected_at_utc": rejected_at,
            "decision_id": None,
            "reject_reason_code": "MISSING_DECISION_ID",
            "reject_reason_detail": "decision_id is required",
            "raw_payload": raw_payload,
        }

    if not decision_type:
        return None, {
            "rejected_at_utc": rejected_at,
            "decision_id": decision_id,
            "reject_reason_code": "MISSING_DECISION_TYPE",
            "reject_reason_detail": "decision_type is required",
            "raw_payload": raw_payload,
        }

    if model_version is None or (isinstance(model_version, str) and not model_version.strip()):
        return None, {
            "rejected_at_utc": rejected_at,
            "decision_id": decision_id,
            "reject_reason_code": "MISSING_MODEL_VERSION",
            "reject_reason_detail": "model_version is required and must be non-empty",
            "raw_payload": raw_payload,
        }

    if conf is None:
        return None, {
            "rejected_at_utc": rejected_at,
            "decision_id": decision_id,
            "reject_reason_code": "MISSING_CONFIDENCE_SCORE",
            "reject_reason_detail": "confidence_score is required and must be numeric",
            "raw_payload": raw_payload,
        }

    if conf < 0.0 or conf > 1.0:
        return None, {
            "rejected_at_utc": rejected_at,
            "decision_id": decision_id,
            "reject_reason_code": "INVALID_CONFIDENCE_SCORE",
            "reject_reason_detail": "confidence_score must be between 0 and 1",
            "raw_payload": raw_payload,
        }

    if risk_band not in VALID_RISK_BANDS:
        return None, {
            "rejected_at_utc": rejected_at,
            "decision_id": decision_id,
            "reject_reason_code": "INVALID_RISK_BAND",
            "reject_reason_detail": f"risk_band must be one of {sorted(VALID_RISK_BANDS)}",
            "raw_payload": raw_payload,
        }

    if not policy_id:
        return None, {
            "rejected_at_utc": rejected_at,
            "decision_id": decision_id,
            "reject_reason_code": "MISSING_POLICY_ID",
            "reject_reason_detail": "policy_id is required",
            "raw_payload": raw_payload,
        }

    if decision_ts is None:
        return None, {
            "rejected_at_utc": rejected_at,
            "decision_id": decision_id,
            "reject_reason_code": "INVALID_DECISION_TS",
            "reject_reason_detail": "decision_ts must be ISO parseable",
            "raw_payload": raw_payload,
        }

    # Clean record
    clean: Dict[str, Any] = {
        "decision_id": decision_id,
        "decision_type": decision_type,
        "model_version": _safe_str(model_version).strip(),
        "confidence_score": round(conf, 4),
        "risk_band": risk_band,
        "policy_id": policy_id,
        "facility_code": facility_code,
        "decision_ts": decision_ts,
        "input_features_hash": raw.get("input_features_hash"),
        "override_flag": bool(raw.get("override_flag")) if raw.get("override_flag") is not None else None,
        "override_reason_code": raw.get("override_reason_code"),
        "ingested_at_utc": rejected_at,  # lineage marker for silver
    }

    return clean, None


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--indir", default="data/landing/decision_events", help="Input directory containing JSONL.")
    p.add_argument("--silver-out", default="data/silver/decision_events_clean.jsonl", help="Silver output JSONL.")
    p.add_argument("--rejects-out", default="data/rejects/decision_events_rejects.jsonl", help="Rejects output JSONL.")
    args = p.parse_args()

    indir = Path(args.indir)
    silver_out = Path(args.silver_out)
    rejects_out = Path(args.rejects_out)

    silver_out.parent.mkdir(parents=True, exist_ok=True)
    rejects_out.parent.mkdir(parents=True, exist_ok=True)

    files = list(_iter_jsonl_files(indir))
    if not files:
        raise SystemExit(f"No JSONL files found in {indir.resolve()}")

    clean_count = 0
    reject_count = 0

    with silver_out.open("w", encoding="utf-8") as s_f, rejects_out.open("w", encoding="utf-8") as r_f:
        for fp in files:
            with fp.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        raw = json.loads(line)
                        if not isinstance(raw, dict):
                            raise ValueError("JSONL line is not an object")
                    except Exception:
                        reject = {
                            "rejected_at_utc": _utc_now_iso(),
                            "decision_id": None,
                            "reject_reason_code": "INVALID_JSON",
                            "reject_reason_detail": "Line is not valid JSON object",
                            "raw_payload": line,
                        }
                        r_f.write(json.dumps(reject, ensure_ascii=False) + "\n")
                        reject_count += 1
                        continue

                    clean, reject = validate_event(raw)
                    if clean is not None:
                        s_f.write(json.dumps(clean, ensure_ascii=False) + "\n")
                        clean_count += 1
                    else:
                        # Ensure reject_reason_code uppercase normalized
                        reject["reject_reason_code"] = _safe_str(reject.get("reject_reason_code")).upper()
                        # Ensure facility_code present in rejects? (optional but useful)
                        # We can include facility_code if it existed in payload; else UNKNOWN
                        fc = _safe_str(raw.get("facility_code")).strip() or "UNKNOWN"
                        reject["facility_code"] = fc
                        r_f.write(json.dumps(reject, ensure_ascii=False) + "\n")
                        reject_count += 1

    print(f"Validated landing events from {len(files)} file(s)")
    print(f"Silver clean written to: {silver_out} (rows={clean_count})")
    print(f"Rejects written to:     {rejects_out} (rows={reject_count})")


if __name__ == "__main__":
    main()
