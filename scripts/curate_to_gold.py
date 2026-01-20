#!/usr/bin/env python3
"""
Curate Silver + Rejects into Gold aggregates.

Inputs:
  data/silver/decision_events_clean.jsonl
  data/rejects/decision_events_rejects.jsonl

Outputs:
  data/gold/fact_ai_decisions_daily.csv
  data/gold/fact_ai_decisions_daily.jsonl
  data/gold/fact_ai_rejects_daily.csv
  data/gold/fact_ai_rejects_daily.jsonl

Aggregations:
- Decisions: by day, risk_band, model_version
- Rejects: by day, reject_reason_code
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def _read_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except Exception:
                continue


def _day_bucket(iso_ts: Any) -> str:
    if not isinstance(iso_ts, str) or not iso_ts.strip():
        return "UNKNOWN_DAY"
    s = iso_ts.strip()
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        return dt.date().isoformat()
    except Exception:
        return "UNKNOWN_DAY"


def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})


def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--silver", default="data/silver/decision_events_clean.jsonl", help="Silver clean JSONL.")
    p.add_argument("--rejects", default="data/rejects/decision_events_rejects.jsonl", help="Rejects JSONL.")
    p.add_argument("--outdir", default="data/gold", help="Gold output directory.")
    args = p.parse_args()

    silver_path = Path(args.silver)
    rejects_path = Path(args.rejects)
    outdir = Path(args.outdir)

    # Decisions aggregate: (day, risk_band, model_version) -> count, avg_conf
    decisions_counts: Dict[Tuple[str, str, str], int] = defaultdict(int)
    decisions_conf_sum: Dict[Tuple[str, str, str], float] = defaultdict(float)

    for row in _read_jsonl(silver_path):
        day = _day_bucket(row.get("decision_ts"))
        risk_band = (row.get("risk_band") or "UNKNOWN").strip()
        model_version = (row.get("model_version") or "UNKNOWN").strip()
        key = (day, risk_band, model_version)
        decisions_counts[key] += 1
        try:
            decisions_conf_sum[key] += float(row.get("confidence_score"))
        except Exception:
            pass

    decisions_out: List[Dict[str, Any]] = []
    for (day, risk_band, model_version), cnt in sorted(decisions_counts.items()):
        avg_conf = decisions_conf_sum[(day, risk_band, model_version)] / cnt if cnt else None
        decisions_out.append(
            {
                "decision_day": day,
                "risk_band": risk_band,
                "model_version": model_version,
                "decisions_count": cnt,
                "avg_confidence_score": round(avg_conf, 4) if avg_conf is not None else None,
            }
        )

    # Rejects aggregate: (day, reject_reason_code) -> count
    rejects_counts: Dict[Tuple[str, str], int] = defaultdict(int)
    for row in _read_jsonl(rejects_path):
        day = _day_bucket(row.get("rejected_at_utc"))
        reason = (row.get("reject_reason_code") or "UNKNOWN").strip().upper()
        key = (day, reason)
        rejects_counts[key] += 1

    rejects_out: List[Dict[str, Any]] = []
    for (day, reason), cnt in sorted(rejects_counts.items()):
        rejects_out.append(
            {
                "reject_day": day,
                "reject_reason_code": reason,
                "rejects_count": cnt,
            }
        )

    # Write outputs
    decisions_csv = outdir / "fact_ai_decisions_daily.csv"
    decisions_jsonl = outdir / "fact_ai_decisions_daily.jsonl"
    rejects_csv = outdir / "fact_ai_rejects_daily.csv"
    rejects_jsonl = outdir / "fact_ai_rejects_daily.jsonl"

    _write_csv(decisions_csv, decisions_out, ["decision_day", "risk_band", "model_version", "decisions_count", "avg_confidence_score"])
    _write_jsonl(decisions_jsonl, decisions_out)

    _write_csv(rejects_csv, rejects_out, ["reject_day", "reject_reason_code", "rejects_count"])
    _write_jsonl(rejects_jsonl, rejects_out)

    print("Gold outputs written:")
    print(f"- {decisions_csv} (rows={len(decisions_out)})")
    print(f"- {decisions_jsonl} (rows={len(decisions_out)})")
    print(f"- {rejects_csv} (rows={len(rejects_out)})")
    print(f"- {rejects_jsonl} (rows={len(rejects_out)})")


if __name__ == "__main__":
    main()

