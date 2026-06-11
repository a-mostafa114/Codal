# -*- coding: utf-8 -*-
"""
last_name_matching.py – Surname matching algorithms.

Pipeline steps A1-A5:
  A1 – line starts with a non-occupation word (hustru, fru, …)
  A2 – perfect (exact) match against the death register
  A3 – fuzzy match (first-cut, prefix-filtered)
  A4 – fuzzy match (full scan, relaxed threshold)
  A5 – unmatched

Also handles:
  - ``V.`` prefix (von) fuzzy matching
  - Hyphenated last-name components
  - Dirty last-name look-up
"""

import re
import os
import multiprocessing as mp

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

from .utils import complete_first_word, fuzzy_match_rapidfuzz
from .config import FIRM_PATTERN, NO_OCC_LIST


# ── helpers ─────────────────────────────────────────────────────────────

def adj_unmatch(row, df_death_reg_unacc):
    """Adjust unmatched rows by trimming the last_name field."""
    ln = row.get("last_name", "")
    if not isinstance(ln, str):
        ln = ""

    base_ln = ln.split()[0] if ln else ""
    if base_ln in set(df_death_reg_unacc["last_name"].dropna()):
        ln = base_ln

    parts = ln.split()
    if len(parts) > 0 and parts[0] == "V.":
        ln = ln.replace("V.", "Von")

    row["last_name"] = ln.split()[0] if len(ln.split()) > 1 else ln
    return row


# ── Precomputed register index ──────────────────────────────────────────

def build_register_index(df_death_reg_unacc):
    """Precompute the per-row lookup structures of perf_match / fuzzy_alt.

    The originals re-derived these from the 467k-name register for every
    row (a full Python scan in perf_match, a filter+sort in each fuzzy
    pass) — ~330 ms/row, the dominant cost of the whole pipeline. Building
    them once preserves the exact iteration orders:

    - ``name_pos``: first register position per name; the minimum over
      candidate tokens reproduces perf_match's first-hit-in-register-order.
    - ``sorted_names``: the same ``sort_values`` call pass 2 ran per row.
    - ``prefix_buckets``: pass 1 filtered *then* sorted, so each bucket is
      built with that exact expression (filter-then-sort, default sort
      kind) rather than sliced from ``sorted_names``, keeping tie order
      identical.
    """
    names = df_death_reg_unacc["last_name"].dropna()
    name_pos = {}
    for pos, name in enumerate(names.values):
        if name not in name_pos:
            name_pos[name] = pos

    sorted_names = names.sort_values(
        key=lambda x: x.str.len(), ascending=False).tolist()

    prefix_buckets = {}
    prefixes = {n[:2] for n in names.values if isinstance(n, str) and n}
    for cut in prefixes:
        bucket = names[names.str.startswith(cut)]
        bucket = bucket.sort_values(key=lambda x: x.str.len(), ascending=False)
        prefix_buckets[cut] = bucket.tolist()

    return {
        "name_pos": name_pos,
        "sorted_names": sorted_names,
        "prefix_buckets": prefix_buckets,
    }


# ── A2: perfect match ──────────────────────────────────────────────────

def perf_match(row, df_death_reg_unacc, reg_idx=None):
    """Exact-token match of the line start against the death register."""
    if isinstance(row["line"], str):
        tokens = row["line"].split(",") + row["line"].split() + row["line"].split(".")
        line = row["line"]
        if reg_idx is not None:
            name_pos = reg_idx["name_pos"]
            hits = [t for t in set(tokens)
                    if len(t) > 2 and t in name_pos and line.startswith(t)]
            name = min(hits, key=name_pos.__getitem__) if hits else None
            if name is not None:
                row["best_match"] = name
                row["last_name"] = name
                row["similarity"] = 100
                row["index"] = "A2"
                row["matched"] = True
            return row
        for name in df_death_reg_unacc["last_name"].dropna().values:
            if name in tokens and line.startswith(name) and len(name) > 2:
                row["best_match"] = name
                row["last_name"] = name
                row["similarity"] = 100
                row["index"] = "A2"
                row["matched"] = True
                break
    return row


# ── A3 / A4: fuzzy match ───────────────────────────────────────────────

def fuzzy_alt(row, df_death_reg_unacc, dirty_last_names_list,
              min_score=85, mid_score=90, reg_idx=None):
    """Two-pass fuzzy match (prefix-filtered, then full scan)."""
    line = row["line"]
    cut = line[:2] if isinstance(line, str) else ""
    if reg_idx is not None and len(cut) == 2:
        pass1_names = reg_idx["prefix_buckets"].get(cut, [])
    else:
        pairings = df_death_reg_unacc[df_death_reg_unacc["last_name"].notna()]
        pairings = pairings[pairings["last_name"].str.startswith(cut)]
        pairings = pairings.sort_values(by="last_name", key=lambda x: x.str.len(), ascending=False)
        pass1_names = pairings["last_name"]

    best_score = 0
    best_name = None

    # --- first pass: prefix-filtered ---
    for last_name in pass1_names:
        if len(last_name) > len(line):
            continue
        compare_part = line[:len(last_name)]
        score = fuzz.token_sort_ratio(last_name, compare_part)
        if score > best_score:
            best_score = score
            best_name = last_name
            if best_score > mid_score and _boundary_ok(line, compare_part):
                row["last_name"] = complete_first_word(line[:len(best_name)], line).rstrip('., ').strip()
                break

    if best_score >= mid_score:
        row["matched"] = True
        row["index"] = "A3"
        row["best_match"] = best_name
        row["similarity"] = best_score
        row["last_name"] = complete_first_word(line[:len(best_name)], line).rstrip('., ').strip()
    else:
        # --- second pass: full scan ---
        best_score = 0
        best_name = None
        if reg_idx is not None:
            pass2_names = reg_idx["sorted_names"]
        else:
            pass2_names = df_death_reg_unacc["last_name"].dropna().sort_values(
                key=lambda x: x.str.len(), ascending=False)
        for last_name in pass2_names:
            if len(last_name) > len(line):
                continue
            compare_part = line[:len(last_name)]
            score = fuzz.token_sort_ratio(last_name, compare_part)
            if score > best_score:
                best_score = score
                best_name = last_name
                if best_score > mid_score and _boundary_ok(line, compare_part):
                    row["last_name"] = complete_first_word(line[:len(best_name)], line).rstrip('., ').strip()
                    break

        complete_word = complete_first_word(line[:len(last_name)], line)

        if best_score >= min_score and abs(len(complete_word) - len(best_name)) <= 5:
            row["matched"] = True
            row["index"] = "A4"
            row["best_match"] = best_name
            row["similarity"] = best_score
            row["last_name"] = complete_first_word(line[:len(best_name)], line).rstrip('., ').strip()
        else:
            row["matched"] = False
            row["index"] = "A5"
            row["best_match"] = ""
            row["similarity"] = 0
            row["last_name"] = ""

    # --- post-validation for A3/A4 ---
    if row["index"] in ["A3", "A4"]:
        try:
            line = row.get("line", "")
            best_name = row.get("best_match", "")
            partial = line[:len(best_name)] if best_name else ""
            completed = complete_first_word(partial, line) or ""
            remaining = line[line.find(partial) + len(partial):] if partial in line else ""
            comma_dist = remaining.find(",") if "," in remaining else float('inf')
            space_dist = remaining.find(" ") if " " in remaining else float('inf')
            dot_dist   = remaining.find(".") if "." in remaining else float('inf')
            comma_ok = min(comma_dist, space_dist, dot_dist) == 1
            comp_name = len(completed) > len(best_name)
            if comp_name and not comma_ok:
                row["matched"], row["index"] = False, "A5"
                row["best_match"], row["similarity"], row["last_name"] = "", 0, ""
        except Exception:
            row["matched"], row["index"] = False, "A5"
            row["best_match"], row["similarity"], row["last_name"] = "", 0, ""

    if row["index"] in ["A3", "A4"]:
        line = row.get("line", "")
        if re.search(FIRM_PATTERN, line):
            row["matched"], row["index"] = False, "A5"
            row["best_match"], row["similarity"], row["last_name"] = "", 0, ""

    # --- dirty-name fallback ---
    if row["index"] == "A5":
        for dirty, clean in dirty_last_names_list.itertuples(index=False):
            if dirty in row["line"]:
                row["matched"]    = True
                row["index"]      = "A2"
                row["best_match"] = clean
                row["similarity"] = 100
                row["last_name"]  = clean
                break

    return row


# ── A1 → A4 orchestrator ───────────────────────────────────────────────

def alt_algorithm(row_, df_death_reg_unacc, dirty_last_names_list, reg_idx=None):
    """Run the full last-name matching cascade (A1→A2→A3/A4)."""
    # A1: non-occupation word
    if any(row_["line"].startswith(word) for word in NO_OCC_LIST):
        row_["matched"] = True
        row_["index"] = "A1"
        return row_

    # A2: perfect match
    if not row_["matched"]:
        row_ = perf_match(row_, df_death_reg_unacc, reg_idx=reg_idx)

    # A3 / A4: fuzzy match
    if not row_["matched"]:
        row_ = fuzzy_alt(row_, df_death_reg_unacc, dirty_last_names_list,
                         reg_idx=reg_idx)

    return row_


# ── V. and dash handling ───────────────────────────────────────────────

def fuzzy_v_dot_and_dash_LN(row, surname_list, df_death_reg_unacc,
                            min_score=86, mid_score=90):
    """Handle ``V.`` prefix (→ von) and hyphenated last names."""
    line = row["line"]
    line_split = line.split(",")
    last_name = line_split[0]

    # Case "V."
    if line.startswith("V.") and row["last_name"] == "":
        line_v = line.replace("V.", "von")
        ln = line_v.split(",")[0]
        df_unacc_von = df_death_reg_unacc[df_death_reg_unacc["last_name"].str.startswith("von")]
        df_unacc_von = df_unacc_von[abs(df_unacc_von["last_name"].str.len() - len(ln)) <= 1]
        best_fit, score, _idx = fuzzy_match_rapidfuzz(ln, df_unacc_von["last_name"])
        if min_score <= score:
            row["best_match"] = best_fit
            ln = ln.replace("von", "V.")
            row["last_name"] = ln
            row["similarity"] = score
            if score == 100:
                row["index"], row["fuzzy_v_dash"] = "A2", 1
            elif mid_score <= score < 100:
                row["index"], row["fuzzy_v_dash"] = "A3", 1
            elif min_score <= score < mid_score:
                row["index"], row["fuzzy_v_dash"] = "A4", 1
        row["matched"] = True
        return row

    # Case LN with dash "-"
    if (re.search(r'\w+\s*-\s*\w+', last_name)
            and not re.search(r'\d+', last_name)
            and re.search(r'\w+', last_name)
            and row["last_name"] == ""
            and not surname_list["line"].duplicated(keep=False).loc[row.name]
            and len(line_split) > 1
            and len(last_name.split()) == 1):
        last_name_splitted = last_name.split("-")
        for comp_ in last_name_splitted:
            death_reg_comp_ = df_death_reg_unacc[
                abs(df_death_reg_unacc["last_name"].str.len() - len(comp_)) <= 1
            ]
            best_fit, score, _idx = fuzzy_match_rapidfuzz(comp_, death_reg_comp_["last_name"])
            if min_score <= score:
                row["best_match"] = str(row["best_match"]) + ' ' + best_fit
                row["last_name"] = str(row["last_name"]) + " " + comp_
                row["similarity"] = score
                if score == 100:
                    row["index"], row["fuzzy_v_dash"] = "A2", 1
                elif mid_score <= score < 100:
                    row["index"], row["fuzzy_v_dash"] = "A3", 1
                elif min_score <= score < mid_score:
                    row["index"], row["fuzzy_v_dash"] = "A4", 1
            row["matched"] = True
            row["best_match"] = row["best_match"].strip()
            row["last_name"] = row["last_name"].strip()
        if row["last_name"] != "":
            row["last_name"] = '-'.join(last_name_splitted)
        return row

    return row


# ── private helper ──────────────────────────────────────────────────────

def _boundary_ok(line, compare_part):
    """Check that the character right after compare_part is a delimiter."""
    after = line[len(compare_part):]
    space_d = after.find(" ") if " " in after else float('inf')
    comma_d = after.find(",") if "," in after else float('inf')
    dot_d   = after.find(".") if "." in after else float('inf')
    return abs(min(space_d, comma_d, dot_d)) == 0


# ── parallel helpers ──────────────────────────────────────────────────

# Module-level globals used by pool workers (set once via initializer).
_pool_death_reg = None
_pool_dirty_names = None
_pool_reg_idx = None


def _init_pool_worker(death_reg, dirty_names, reg_idx=None):
    """Load shared reference data into each worker once at pool creation."""
    global _pool_death_reg, _pool_dirty_names, _pool_reg_idx
    _pool_death_reg = death_reg
    _pool_dirty_names = dirty_names
    _pool_reg_idx = reg_idx


def _worker_apply_alt_algorithm(chunk):
    """Process a chunk using the pre-loaded shared reference data."""
    return chunk.apply(
        lambda row: alt_algorithm(row, _pool_death_reg, _pool_dirty_names,
                                  reg_idx=_pool_reg_idx),
        axis=1,
    )


def parallel_alt_algorithm(surname_list, df_death_reg_unacc,
                           dirty_last_names_list, n_workers=None):
    """Run :func:`alt_algorithm` in parallel across multiple CPU cores.

    Parameters
    ----------
    surname_list : DataFrame
        The rows to match.
    df_death_reg_unacc : DataFrame
        Death-register reference data (read-only).
    dirty_last_names_list : DataFrame
        Dirty-name → clean-name mapping (read-only).
    n_workers : int, optional
        Number of parallel workers.  Defaults to ``min(os.cpu_count(), 16)``.
    """
    n_rows = len(surname_list)
    if n_workers is None:
        n_workers = int(os.environ.get("CODAL_WORKERS", 0)) or os.cpu_count() or 1
    n_workers = max(1, min(n_workers, n_rows))

    reg_idx = build_register_index(df_death_reg_unacc)

    if n_workers <= 1:
        return surname_list.apply(
            lambda row: alt_algorithm(
                row, df_death_reg_unacc, dirty_last_names_list,
                reg_idx=reg_idx),
            axis=1,
        )

    # Many small chunks: per-row cost is wildly uneven (an unmatched row
    # costs ~1000x an exact match, and hard rows cluster on ad pages), so
    # worker-sized chunks leave the pool idling on stragglers.
    n_chunks = min(n_rows, n_workers * 16)
    indices = np.array_split(np.arange(n_rows), n_chunks)
    chunks = [surname_list.iloc[idx] for idx in indices if len(idx) > 0]

    print(f"  → Distributing {n_rows:,} rows across {n_workers} workers "
          f"({len(chunks)} chunks) ...")

    with mp.Pool(
        processes=min(n_workers, len(chunks)),
        initializer=_init_pool_worker,
        initargs=(df_death_reg_unacc, dirty_last_names_list, reg_idx),
    ) as pool:
        results = pool.map(_worker_apply_alt_algorithm, chunks)

    return pd.concat(results)
