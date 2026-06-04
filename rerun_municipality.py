# -*- coding: utf-8 -*-
"""
Rerun municipality assignment from existing aaa_6_final checkpoints.

Skips Steps 1–12 entirely. Re-runs only:
  Step 5  – location / municipality assignment (fixed find_locations)
  Step 13 – parish quality check (uses municipality for Stockholm filter)
  Step 14 – write final output CSV

Usage:
    python rerun_municipality.py

All runs with an existing  <prefix>_aaa_6_final.csv  are processed
automatically.  Add or remove entries from RUNS to limit scope.
"""

from pathlib import Path

import pandas as pd

from ocr_modules import location, parish

# ── Runs to process ──────────────────────────────────────────────────────
# Each entry: (year, provider, out_dir, checkpoint_prefix, output_prefix, mineru_dir)
# mineru_dir: path to MinerU *_extracted.json files, or None for non-MinerU runs.
RUNS = [
    # 1911-1914: headers contain ads, not city names → mineru_dir=None
    ("1911", "mineru",  "runs/1911/outputs/mineru",  "mineru",   "final_output_mineru",  None),
    ("1912", "amazon",  "runs/1912/outputs/amazon",  "amazon",   "final_output_amazon",  None),
    ("1912", "mineru",  "runs/1912/outputs/mineru",  "mineru",   "final_output_mineru",  None),
    ("1912", "deepseek","runs/1912/outputs/deepseek","deepseek", "final_output_deepseek",None),
    ("1913", "amazon",  "runs/1913/outputs/amazon",  "amazon",   "final_output_amazon",  None),
    ("1913", "mineru",  "runs/1913/outputs/mineru",  "mineru",   "final_output_mineru",  None),
    ("1913", "nano",    "runs/1913/outputs/nano",    "nano",     "final_output_nano",    None),
    ("1913", "nvidia",  "runs/1913/outputs/nvidia",  "nvidia",   "final_output_nvidia",  None),
    ("1913", "glm",     "runs/1913/outputs/glm",     "glm",      "final_output_glm",     None),
    ("1914", "mineru",  "runs/1914/outputs/mineru",  "mineru",   "final_output_mineru",  None),
    # 1920-1921: MinerU headers contain municipality names
    ("1920", "mineru",  "runs/1920/outputs/mineru",  "mineru",   "final_output_mineru",
     "/home7/becomingsweden/data/taxeringskalender_1920/1920/miner_ocr_results/"),
    ("1921", "mineru",  "runs/1921/outputs/mineru",  "mineru",   "final_output_mineru",
     "/home7/becomingsweden/data/taxeringskalender_1921/1921/miner_ocr_results/"),
]

FINAL_COLS = [
    "page", "column", "row", "line", "line_complete", "index", "split",
    "firm_dummy", "estate_dummy", "last_name", "best_match", "initials",
    "occ_reg", "occ_reg_2", "municipality", "parish", "matched_parish",
    "unique_key", "income", "income_1", "income_2",
]


def _load_parish_refs():
    """Load the three reference CSVs needed for the parish quality check."""
    parish_only_matched = pd.read_csv("final_parish_csv_quality_check.csv")
    df_parish_added = pd.read_csv("df_extra_parish_iterative_check.csv")

    parish_mapped = pd.read_csv("parish_county.csv")
    parish_mapped = parish_mapped.rename(columns={"parish": "Parish", "county": "municipality"})
    replacements = {
        "ö": "o", "ä": "a", "à": "a", "å": "a",
        "Ö": "O", "Ä": "A", "Å": "A",
    }
    for old, new in replacements.items():
        parish_mapped["Parish"] = parish_mapped["Parish"].str.replace(old, new)
        parish_mapped["municipality"] = parish_mapped["municipality"].str.replace(old, new)
    parish_mapped["Parish"] = (
        parish_mapped["Parish"].str[0].str.upper() + parish_mapped["Parish"].str[1:]
    )
    return parish_mapped, parish_only_matched, df_parish_added


def rerun_one(year, provider, out_dir, checkpoint_prefix, output_prefix,
              parish_mapped, parish_only_matched, df_parish_added,
              mineru_dir=None):
    out_path = Path(out_dir)
    ckpt = out_path / f"{checkpoint_prefix}_aaa_6_final.csv"
    if not ckpt.exists():
        print(f"  [{year}/{provider}] checkpoint not found, skipping: {ckpt}")
        return

    print(f"\n[{year}/{provider}] Loading {ckpt.name} ...")
    df = pd.read_csv(ckpt, low_memory=False).fillna("")

    # Drop stale municipality / location so we start clean
    df = df.drop(columns=["municipality", "location"], errors="ignore")

    # ── Step 5: location / municipality ──────────────────────────────────
    print(f"[{year}/{provider}] Step 5: assigning municipalities ...")
    df = location.find_locations(df)
    location_list = location.build_location_list_with_headers(df, mineru_dir=mineru_dir)
    df = location.extract_location(df, location_list)
    df = location.location_limit_case(df)

    # Quick sanity: show top-10 municipality values
    top = df["municipality"].value_counts().head(10)
    print(f"[{year}/{provider}] Top municipalities:\n{top.to_string()}\n")

    # ── Step 13: parish quality check (uses municipality) ────────────────
    print(f"[{year}/{provider}] Step 13: parish quality check ...")
    df = parish.run_parish_quality_check(
        df, parish_mapped, parish_only_matched, df_parish_added)

    # ── Step 14: write final output ───────────────────────────────────────
    final_set = df[[c for c in FINAL_COLS if c in df.columns]]
    out_csv = out_path / f"{output_prefix}.csv"
    final_set.to_csv(out_csv, index=False)
    print(f"[{year}/{provider}] Done → {out_csv}")


def main():
    parish_mapped, parish_only_matched, df_parish_added = _load_parish_refs()

    for (year, provider, out_dir, ckpt_prefix, out_prefix, mineru_dir) in RUNS:
        rerun_one(
            year, provider, out_dir, ckpt_prefix, out_prefix,
            parish_mapped, parish_only_matched, df_parish_added,
            mineru_dir=mineru_dir,
        )


if __name__ == "__main__":
    main()
