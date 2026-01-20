#!/usr/bin/env python3
"""
Generate synthetic AI decision events as JSON Lines (JSONL).

Outputs:
  data/landing/decision_events/decision_events_synth_<timestamp>.jsonl

Features:
- Controlled percentage of bad records for rejects
- Mix of decision types, model versions, risk bands, facilities, policies
- Optional override flags and reasons

Usage:
  python scripts/generate_synthetic_decisions.py --n 500 --bad-rate 0.15
"""

from __future__ import annotations

import argparse
import json
import os
import random
import string
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


VALID_RISK_BANDS = ["LOW", "MEDIUM", "HIGH"]
VALID_DECISION_TYPES = ["coverage_recommendation", "fraud_flag", "pricing_adjustment", "claim_triage"]
VALID_OVERRIDE_REASONS = ["HUMAN_REVIEW_REQUIRED", "OUT_OF_POLICY", "MISSING_EVIDENCE", "EXCEPTION_APPROVAL"]


@dataclass
class GenConfig:
    n: int
    bad_rate: float
    outdir: Path
    seed: Optional[int]


def _rand_id(prefix: str, n: int = 8) -> str:
    return f"{prefix}_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def _rand_hash(n: int = 7) -> str:
    return "".join(random.choices("abcdef" + string.digits, k=n))


def _rand_ts(start_utc: datetime, span_minutes: int = 60 * 24 * 10) -> datetime:
    """Random timestamp within span_minutes after start_utc."""
    return start_utc + timedelta(minutes=random.randint(0, span_minutes))


def _choose(seq: List[str]) -> str:
    return random.choice(seq)


def _make_good_event(now_utc: datetime) -> Dict[str, Any]:
    decision_id = _rand_id("dec")
    decision_type = _choose(VALID_DECISION_TYPES)
    model_version = _choose(["risk-model-v1.2", "risk-model-v1.3", "risk-model-v1.4"])
    confidence_score = round(random.uniform(0.35, 0.99), 2)
    risk_band = _choose(VALID_RISK_BANDS)

    policy_id = _choose(["POL-1001", "POL-1002", "POL-1003", "POL-1004", "POL-2001"])
    facility_code = _choose(["FAC-001", "FAC-023", "FAC-102", "FAC-210"])

    decision_ts = _rand_ts(now_utc - timedelta(days=10)).replace(tzinfo=timezone.utc)
    override_flag = random.random() < 0.18  # ~18% overrides

    event: Dict[str, Any] = {
        "decision_id": decision_id,
        "decision_type": decision_type,
        "model_version": model_version,
        "confidence_score": confidence_score,
        "risk_band": risk_band,
        "policy_id": policy_id,
        "facility_code": facility_code,
        "decision_ts": decision_ts.isoformat().replace("+00:00", "Z"),
        "input_features_hash": _rand_hash(),
        "override_flag": override_flag,
    }

    if override_flag:
        event["override_reason_code"] = _choose(VALID_OVERRIDE_REASONS)

    return event


def _corrupt_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produce a "bad" record by applying one or more corruptions.
    This is intentionally realistic: missing fields, invalid enums, empty strings, bad timestamps.
    """
    e = dict(event)

    corruptions = [
        "drop_confidence",
        "empty_model_version",
        "invalid_risk_band",
        "missing_policy_id",
        "empty_facility_code",
        "bad_timestamp",
    ]

    # apply 1–2 corruptions
    for _ in range(random.randint(1, 2)):
        c = random.choice(corruptions)

        if c == "drop_confidence":
            e.pop("confidence_score", None)

        elif c == "empty_model_version":
            e["model_version"] = "" if random.random() < 0.7 else None

        elif c == "invalid_risk_band":
            e["risk_band"] = _choose(["MID", "UNKNOWN", "low", "HIGHEST", ""])

        elif c == "missing_policy_id":
            e.pop("policy_id", None)

        elif c == "empty_facility_code":
            e["facility_code"] = ""  # will be defaulted to UNKNOWN in silver clean

        elif c == "bad_timestamp":
            e["decision_ts"] = _choose(["not-a-date", "2026-99-99", "", None])

    return e


def generate(cfg: GenConfig) -> Path:
    cfg.outdir.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc)

    ts_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    outpath = cfg.outdir / f"decision_events_synth_{ts_tag}.jsonl"

    with outpath.open("w", encoding="utf-8") as f:
        for _ in range(cfg.n):
            ev = _make_good_event(now_utc)

            if random.random() < cfg.bad_rate:
                ev = _corrupt_event(ev)

            f.write(json.dumps(ev, ensure_ascii=False) + "\n")

    return outpath


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=250, help="Number of events to generate.")
    p.add_argument("--bad-rate", type=float, default=0.12, help="Fraction of records to corrupt (0..1).")
    p.add_argument("--outdir", type=str, default="data/landing/decision_events", help="Output directory.")
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility.")
    args = p.parse_args()

    if not (0.0 <= args.bad_rate <= 1.0):
        raise SystemExit("--bad-rate must be between 0 and 1")

    if args.seed is not None:
        random.seed(args.seed)

    outpath = generate(
        GenConfig(
            n=args.n,
            bad_rate=args.bad_rate,
            outdir=Path(args.outdir),
            seed=args.seed,
        )
    )

    print(f"Wrote synthetic events to: {outpath}")
    print(f"Tip: run validate_to_silver.py next.")


if __name__ == "__main__":
    main()

