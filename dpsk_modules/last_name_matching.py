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


# ── A2: perfect match ──────────────────────────────────────────────────

def perf_match(row, df_death_reg_unacc):
    """Exact-token match of the line start against the death register."""
    if isinstance(row["line"], str):
        tokens = row["line"].split(",") + row["line"].split() + row["line"].split(".")
        line = row["line"]
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
              min_score=85, mid_score=90):
    """Two-pass fuzzy match (prefix-filtered, then full scan)."""
    line = row["line"]
    cut = line[:2] if isinstance(line, str) else ""
    pairings = df_death_reg_unacc[df_death_reg_unacc["last_name"].notna()]
    pairings = pairings[pairings["last_name"].str.startswith(cut)]
    pairings = pairings.sort_values(by="last_name", key=lambda x: x.str.len(), ascending=False)

    best_score = 0
    best_name = None

    # --- first pass: prefix-filtered ---
    for last_name in pairings["last_name"]:
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
        for last_name in df_death_reg_unacc["last_name"].dropna().sort_values(
                key=lambda x: x.str.len(), ascending=False):
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

def alt_algorithm(row_, df_death_reg_unacc, dirty_last_names_list):
    """Run the full last-name matching cascade (A1→A2→A3/A4)."""
    # A1: non-occupation word
    if any(row_["line"].startswith(word) for word in NO_OCC_LIST):
        row_["matched"] = True
        row_["index"] = "A1"
        return row_

    # A2: perfect match
    if not row_["matched"]:
        row_ = perf_match(row_, df_death_reg_unacc)

    # A3 / A4: fuzzy match
    if not row_["matched"]:
        row_ = fuzzy_alt(row_, df_death_reg_unacc, dirty_last_names_list)

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
