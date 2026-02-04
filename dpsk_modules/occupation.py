# -*- coding: utf-8 -*-
"""
occupation.py – Occupation extraction, fuzzy matching, and adjustments.
"""

import re
import string
import pandas as pd
from rapidfuzz import fuzz

from .utils import fuzzy_match_rapidfuzz


# ── Exact occupation extraction ─────────────────────────────────────────

def extract_occ(row, occ_list):
    """Try to find an exact occupation match in the residual line."""
    line = row["residual_line"]
    if isinstance(line, str):
        line = re.sub(r'\s+', ' ', line.strip().lower())
        for word in occ_list["occ_llm"]:
            def clean_token(s):
                return s.strip().lower().translate(str.maketrans('', '', string.punctuation))
            if isinstance(word, str) and word.lower().rstrip() in line:
                if isinstance(word, str) and (
                        any(clean_token(word) == clean_token(lw) for lw in line.split()) or
                        any(clean_token(word) == clean_token(lw) for lw in line.split(","))):
                    row["occ_reg"] = word
                    break
    row["occ_reg"] = "" if len(row["occ_reg"]) < 3 or row["occ_reg"] == "hustru" else row["occ_reg"].strip()
    row["occ_reg"] = row["occ_reg"].lower()
    return row


# ── Fuzzy occupation matching ───────────────────────────────────────────

def occ_fuzz(row, occ_list):
    """Fuzzy-match lowercase words in the line against the occupation list."""
    line = str(row["line_complete"])
    if (row["split"] in [1, 3]
            and row["firm_dummy"] == 0
            and row["estate_dummy"] == 0
            and row["index"] != "A1"
            and row["occ_reg"] == ""
            and any(word and word[0].islower() for word in line.split())):
        lower_cases_ = " ".join([w for w in line.split() if w and w[0].islower()])
        lower_cases_ = lower_cases_.replace(",", " ").strip()
        if not lower_cases_:
            return ""
        parts = [p.strip() for p in lower_cases_.split(",") if p.strip()]
        candidate = parts[1] if len(parts) > 1 else parts[0]
        candidate = candidate.strip()
        best_match, score, _idx = fuzzy_match_rapidfuzz(candidate, occ_list["occ_llm"])
        return best_match if score >= 85.5 else ""
    return ""


# ── Secondary occupation extraction ─────────────────────────────────────

def sec_occup(row, occ_list):
    """Extract a second occupation from the line (if any)."""
    line = str(row["line_complete"])
    ln = str(row["last_name"])
    occ_ = str(row["occ_reg"])
    if (row["split"] in [1, 3] and row["firm_dummy"] == 0
            and row["estate_dummy"] == 0 and row["index"] != "A1" and row["occ_reg"] != ""):
        line_split = line.split(",")
        line_split = [w.strip() for w in line_split if w and w.strip() and w.strip()[0].islower()]
        line_split = [x for x in line_split if fuzz.token_sort_ratio(x, occ_) < 82 and x not in ln]
        if not line_split:
            return ""
        parts = [p.strip() for p in line_split if p.strip()]
        candidate = parts[1] if len(parts) > 1 and line.startswith(parts[0]) else parts[0]
        candidate = candidate.strip()
        best_match, score, _idx = fuzzy_match_rapidfuzz(candidate, occ_list["occ_llm"])
        return (candidate if score >= 87 and occ_ not in candidate
                and candidate not in occ_ and not line.startswith(candidate)
                and candidate.lower() != occ_.lower() else "")
    return None


# ── Suspect-occupation adjustment ───────────────────────────────────────

def adj_suspect_occ(row, occ_list):
    """Re-scan for occupations on rows that have none yet."""
    line = row["line_complete"]
    if (row["occ_reg"] == "" and row["firm_dummy"] == 0
            and row["split"] in [1, 3] and row["index"] != "A1"
            and not re.search(r'froken|ankefru|\bfru', line)):
        line_split = [x.strip() for x in line.split(",")]
        for word in line_split:
            if any(word == occ_ for occ_ in occ_list["occ_llm"].values):
                row["occ_reg"] = word
    return row
