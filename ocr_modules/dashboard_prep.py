# -*- coding: utf-8 -*-
"""
dashboard_prep.py – Prepare dashboard-ready comparison outputs from OCR results.

This module loads multiple provider outputs (final_output_*.csv),
matches lines page-by-page using edit distance, and writes enriched
CSV files for dashboarding. It also supports a "number/char excluded"
variant with additional normalization and hustru-name handling.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from collections import defaultdict
from functools import partial
from multiprocessing import Pool, cpu_count
from difflib import SequenceMatcher
import re

import numpy as np
import pandas as pd
from rapidfuzz.distance import Levenshtein


PROVIDERS = ["amazon", "nvidia", "mineru", "deepseek", "qwen", "nano"]


@dataclass
class DashboardPrepConfig:
    input_paths: Dict[str, str]
    output_dir: str
    n_workers: int | None = None
    threshold: int = 5


def load_provider_outputs(input_paths: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    dfs: Dict[str, pd.DataFrame] = {}
    for name, path in input_paths.items():
        df = pd.read_csv(path)
        dfs[name] = df
    return dfs


def set_line_from_complete(dfs: Dict[str, pd.DataFrame]) -> None:
    for df in dfs.values():
        if "line_complete" in df.columns:
            df["line"] = df["line_complete"]


def ensure_sorted(df: pd.DataFrame) -> pd.DataFrame:
    if {"page", "column", "row"}.issubset(df.columns):
        return df.sort_values(by=["page", "column", "row"]).reset_index(drop=True).copy()
    return df.reset_index(drop=True).copy()


def prepare_dataframe(df: pd.DataFrame, idx_name: str) -> pd.DataFrame:
    df_nonan = df.dropna(subset=["line"]).reset_index().rename(columns={"index": idx_name})
    df_nonan["line_str"] = df_nonan["line"].astype(str)
    return df_nonan


def build_page_index(df_nonan: pd.DataFrame, idx_col: str) -> Dict[int, List[Tuple[int, str]]]:
    page_index: Dict[int, List[Tuple[int, str]]] = defaultdict(list)
    for row in df_nonan.itertuples():
        page_index[row.page].append((getattr(row, idx_col), row.line_str))
    return page_index


def find_nearest_idx_and_line(
    page: int,
    line_str: str,
    candidates: List[Tuple[int, str]],
) -> Tuple[Any, Any]:
    if not candidates:
        return np.nan, np.nan

    for idx, cand_line in candidates:
        if cand_line == line_str:
            return idx, cand_line

    dists = [Levenshtein.distance(line_str, str(cand_line)) for _, cand_line in candidates]
    min_idx = int(np.argmin(dists))
    best_idx, best_line = candidates[min_idx]
    return best_idx, best_line


def get_edit_distance(src_line: Any, tgt_line: Any) -> Any:
    if pd.isna(src_line) or pd.isna(tgt_line):
        return np.nan
    return Levenshtein.distance(str(src_line), str(tgt_line))


def process_chunk(
    chunk_data: pd.DataFrame,
    page_index: Dict[int, List[Tuple[int, str]]],
    source_col: str = "line",
) -> List[Tuple[int, Any, Any, Any]]:
    results = []
    for idx, row in chunk_data.iterrows():
        page = row["page"]
        line_str = "" if pd.isna(row[source_col]) else str(row[source_col])
        candidates = page_index.get(page, [])
        nearest_idx, nearest_line = find_nearest_idx_and_line(page, line_str, candidates)
        dist = get_edit_distance(row[source_col], nearest_line)
        results.append((idx, nearest_idx, nearest_line, dist))
    return results


def parallel_match(
    df: pd.DataFrame,
    page_index: Dict[int, List[Tuple[int, str]]],
    n_workers: int | None = None,
) -> pd.DataFrame:
    if n_workers is None:
        n_workers = min(cpu_count(), 90)

    if len(df) == 0:
        return pd.DataFrame(columns=["nearest_idx", "nearest_line", "nearest_dist"])

    if n_workers <= 1:
        results = process_chunk(df, page_index)
        return _results_to_frame(results)

    chunk_size = max(1, len(df) // n_workers)
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    process_func = partial(process_chunk, page_index=page_index)

    with Pool(processes=n_workers) as pool:
        chunk_results = pool.map(process_func, chunks)

    all_results = [item for sublist in chunk_results for item in sublist]
    return _results_to_frame(all_results)


def _results_to_frame(results: List[Tuple[int, Any, Any, Any]]) -> pd.DataFrame:
    indices = [r[0] for r in results]
    nearest_idx = [r[1] for r in results]
    nearest_line = [r[2] for r in results]
    nearest_dist = [r[3] for r in results]
    return pd.DataFrame({
        "index": indices,
        "nearest_idx": nearest_idx,
        "nearest_line": nearest_line,
        "nearest_dist": nearest_dist,
    }).set_index("index")


def match_all_providers(
    dfs: Dict[str, pd.DataFrame],
    n_workers: int | None = None,
) -> Dict[str, pd.DataFrame]:
    prepped = {name: prepare_dataframe(df, f"{name}_idx") for name, df in dfs.items()}
    page_indexes = {
        name: build_page_index(df_nonan, f"{name}_idx")
        for name, df_nonan in prepped.items()
    }

    outputs: Dict[str, pd.DataFrame] = {name: df.copy() for name, df in dfs.items()}

    for source_name, source_df in outputs.items():
        for target_name, page_index in page_indexes.items():
            if source_name == target_name:
                continue
            result = parallel_match(source_df, page_index, n_workers=n_workers)
            source_df[f"nearest_{target_name}_idx"] = result["nearest_idx"]
            source_df[f"nearest_{target_name}_line"] = result["nearest_line"]
            source_df[f"nearest_{target_name}_dist"] = result["nearest_dist"]

    return outputs


def clean_line_numbers_and_dashes(line: Any) -> str:
    if not isinstance(line, str):
        line = str(line)
    return re.sub(r"[\d\u2010-\u2015\-\*]", "", line)


def normalize_line(line: Any) -> str:
    if not isinstance(line, str):
        line = str(line)
    replacements = {
        "Å": "A", "å": "a",
        "Ä": "A", "ä": "a",
        "Ö": "O", "ö": "o",
        "æ": "ae",
        "Ø": "O", "ø": "o",
    }
    for old_char, new_char in replacements.items():
        line = line.replace(old_char, new_char)
    line = re.sub(r"[\s|]", "", line)
    return line


def clean_and_normalize_lines(dfs: Dict[str, pd.DataFrame]) -> None:
    for df in dfs.values():
        if "line" in df.columns:
            df["line"] = df["line"].apply(clean_line_numbers_and_dashes)
            df["line"] = df["line"].apply(normalize_line)


def drop_empty_and_recompute_rows(dfs: Dict[str, pd.DataFrame]) -> None:
    for name, df in dfs.items():
        if "line" not in df.columns:
            continue
        df = df[df["line"].str.len() > 0].copy()
        if {"page", "column"}.issubset(df.columns):
            df["row"] = df.groupby(["page", "column"]).cumcount() + 1
        dfs[name] = ensure_sorted(df)


def add_last_name_to_hustru(df: pd.DataFrame) -> pd.DataFrame:
    def is_hustru_line(line: Any) -> bool:
        if not isinstance(line, str) or not line.strip():
            return False
        line_lower = line.lower().strip()
        if line_lower.startswith("hustru"):
            return True
        ocr_variants = ["chustru", "ilustru", "hустru", "hustriu", "hustrи", "hnstru", "hustra"]
        for variant in ocr_variants:
            if line_lower.startswith(variant):
                return True
        check_length = min(7, len(line_lower))
        if check_length >= 5:
            similarity = SequenceMatcher(None, line_lower[:check_length], "hustru").ratio()
            if similarity >= 0.80:
                return True
        return False

    def extract_name_elements(line: Any) -> str | None:
        if not isinstance(line, str) or not line.strip():
            return None
        tokens = [token.strip() for token in line.strip().split(",")]
        valid_tokens = []
        for token in tokens:
            if len(token) >= 2 and any(c.isalpha() for c in token):
                valid_tokens.append(token)
        if not valid_tokens:
            return None
        if len(valid_tokens) == 1:
            return valid_tokens[0]
        if len(valid_tokens) == 2:
            return " ".join(valid_tokens[:2])
        return " ".join(valid_tokens[:3])

    def name_already_present(name_elements: str | None, hustru_line: str | None) -> bool:
        if not name_elements or not hustru_line:
            return False
        name_tokens = name_elements.lower().split()
        hustru_lower = hustru_line.lower()
        matches = sum(1 for token in name_tokens if token in hustru_lower)
        return matches >= len(name_tokens)

    df = ensure_sorted(df)
    df_out = df.copy()

    if not {"page", "column"}.issubset(df_out.columns):
        return df_out

    for _, group_idx in df_out.groupby(["page", "column"]).groups.items():
        idxs = list(group_idx)
        for pos in range(1, len(idxs)):
            idx = idxs[pos]
            prev_idx = idxs[pos - 1]
            current_line = df_out.at[idx, "line"]
            if not is_hustru_line(current_line):
                continue
            prev_line = df_out.at[prev_idx, "line"]
            name_elements = extract_name_elements(prev_line)
            if name_elements and not name_already_present(name_elements, current_line):
                current_clean = str(current_line).strip()
                if not current_clean.startswith(","):
                    current_clean = "," + current_clean
                df_out.at[idx, "line"] = f"{name_elements}{current_clean}"

    return df_out


def apply_hustru_fix(dfs: Dict[str, pd.DataFrame]) -> None:
    for name, df in dfs.items():
        if "line" in df.columns:
            dfs[name] = add_last_name_to_hustru(df)


def compute_quick_stats(
    dfs: Dict[str, pd.DataFrame],
    threshold: int,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for source_name, df in dfs.items():
        for target_name in dfs.keys():
            if source_name == target_name:
                continue
            col = f"nearest_{target_name}_dist"
            if col not in df.columns:
                continue
            matches = (df[col] <= threshold).sum()
            rows.append({
                "source": source_name,
                "target": target_name,
                "threshold": threshold,
                "matches": int(matches),
            })
    return pd.DataFrame(rows)


def write_outputs(
    dfs: Dict[str, pd.DataFrame],
    out_dir: str,
    suffix: str,
) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    for name, df in dfs.items():
        df.to_csv(out_path / f"df_{name}_{suffix}.csv", index=False, encoding="utf-8")


def run_dashboard_prep(
    cfg: DashboardPrepConfig,
    cleaned_variant: bool = True,
    apply_hustru: bool = True,
) -> None:
    dfs = load_provider_outputs(cfg.input_paths)
    set_line_from_complete(dfs)
    dfs = {name: ensure_sorted(df) for name, df in dfs.items()}

    print("Running matches: all_types")
    matched_all = match_all_providers(dfs, n_workers=cfg.n_workers)
    write_outputs(matched_all, cfg.output_dir, "all_types")
    stats_all = compute_quick_stats(matched_all, cfg.threshold)
    stats_all.to_csv(Path(cfg.output_dir) / "match_stats_all_types.csv", index=False)

    if not cleaned_variant:
        return

    print("Running matches: number_char_excluded")
    dfs_clean = load_provider_outputs(cfg.input_paths)
    set_line_from_complete(dfs_clean)
    clean_and_normalize_lines(dfs_clean)
    drop_empty_and_recompute_rows(dfs_clean)
    if apply_hustru:
        apply_hustru_fix(dfs_clean)

    matched_clean = match_all_providers(dfs_clean, n_workers=cfg.n_workers)
    write_outputs(matched_clean, cfg.output_dir, "number_char_excluded")
    stats_clean = compute_quick_stats(matched_clean, cfg.threshold)
    stats_clean.to_csv(
        Path(cfg.output_dir) / "match_stats_number_char_excluded.csv",
        index=False,
    )
