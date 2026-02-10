# -*- coding: utf-8 -*-
"""
reporting.py – Lightweight reporting utilities for pipeline steps.

Generates per-step metrics and summary stats to help debug and audit
how the pipeline transforms the data.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd


class ReportCollector:
    """Collect per-step metrics and write summary/detail reports."""

    def __init__(self):
        self.records: List[Dict[str, Any]] = []

    def capture(self, step_id: int, step_name: str, df: pd.DataFrame,
                extra: Dict[str, Any] | None = None) -> None:
        """Capture metrics for the current step."""
        metrics: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "step_id": step_id,
            "step_name": step_name,
            "rows": int(len(df)) if df is not None else 0,
        }

        if df is None or df.empty:
            if extra:
                metrics.update(extra)
            self.records.append(metrics)
            return

        # Unique key count
        if "unique_key" in df.columns:
            metrics["unique_key_count"] = int(df["unique_key"].nunique())

        # Split distribution
        if "split" in df.columns:
            split_counts = df["split"].value_counts(dropna=False).to_dict()
            metrics["split_counts"] = _stringify_keys(split_counts)

        # Match index distribution
        if "index" in df.columns:
            idx_counts = df["index"].value_counts(dropna=False).to_dict()
            metrics["index_counts"] = _stringify_keys(idx_counts)

        # Matched flag
        if "matched" in df.columns:
            match_counts = df["matched"].value_counts(dropna=False).to_dict()
            metrics["matched_counts"] = _stringify_keys(match_counts)

        # Non-empty counts for key fields
        for col in [
            "last_name", "best_match", "initials", "second_last_name",
            "occ_reg", "occ_reg_2", "parish", "matched_parish",
            "municipality", "location", "income", "income_1", "income_2",
        ]:
            if col in df.columns:
                metrics[f"non_empty_{col}"] = int(_non_empty_count(df[col]))

        # Dummy flags
        for col in ["firm_dummy", "estate_dummy", "fuzzy_v_dash"]:
            if col in df.columns:
                metrics[f"sum_{col}"] = int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())

        # Combined line count
        if "line" in df.columns and "line_complete" in df.columns:
            metrics["combined_line_count"] = int((df["line_complete"] != df["line"]).sum())

        if extra:
            metrics.update(extra)

        self.records.append(metrics)

    def write_reports(self, out_dir: str = "reports") -> None:
        """Write summary and detailed reports to disk."""
        os.makedirs(out_dir, exist_ok=True)

        # Detail report
        detail_path = os.path.join(out_dir, "detail_report.csv")
        detail_df = pd.DataFrame(self.records)
        detail_df.to_csv(detail_path, index=False)

        # Summary report
        summary = _build_summary(self.records)
        summary_path = os.path.join(out_dir, "summary_report.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=True)


# ── Helpers ─────────────────────────────────────────────────────────────

def _non_empty_count(series: pd.Series) -> int:
    if series is None:
        return 0
    return int(series.apply(lambda x: isinstance(x, str) and x.strip() != "").sum())


def _stringify_keys(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {str(k): v for k, v in d.items()}


def _build_summary(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not records:
        return {"steps": 0, "final": {}}

    final = records[-1]
    summary = {
        "steps": len(records),
        "final_rows": final.get("rows", 0),
        "final_split_counts": final.get("split_counts", {}),
        "final_index_counts": final.get("index_counts", {}),
        "final_combined_line_count": final.get("combined_line_count", 0),
        "final_non_empty": {
            k.replace("non_empty_", ""): v
            for k, v in final.items() if k.startswith("non_empty_")
        },
    }
    return summary
