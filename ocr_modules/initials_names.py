# -*- coding: utf-8 -*-
"""
initials_names.py – Extraction of initials, first names, and second last names.
"""

import re
import pandas as pd
from collections import defaultdict

from .config import FIRM_PATTERN, INITIALS_PATTERN


# ── Initials extraction ─────────────────────────────────────────────────

def get_initials(row):
    """Extract initials (e.g. ``A. B.``) from the residual line."""
    line = str(row["residual_line"])
    line = re.sub(r"\s*,\s*", ", ", line)
    line_split = line.split()

    initials = []
    start_ = False

    for i, token in enumerate(line_split):
        if re.search(INITIALS_PATTERN, token):
            initials.append(token)
            start_ = True
            next_token = line_split[i + 1] if i + 1 < len(line_split) else ""
            if "," in token or (next_token and len(next_token) > 4 and re.search(r'[a-z]', next_token)):
                break
        elif start_:
            break

    row["initials"] = ' '.join(initials).replace(",", "")
    if re.search(r'A\s*\.?\s*-\s*B', row["initials"]):
        row["initials"] = re.sub(r'A\s*\.?\s*-\s*B\.?', "", row["initials"])
    return row


# ── First-name detection ────────────────────────────────────────────────

def build_prefix_dict(first_names, prefix_len=2):
    prefix_dict = defaultdict(list)
    for name in first_names:
        prefix_dict[name[:prefix_len]].append(name)
    return prefix_dict


def first_name(df, first_names, prefix_dict):
    """Detect first names among words not yet captured as initials."""
    idx_list = df.index.to_list()
    for pos, idx in enumerate(idx_list):
        initials = df.at[idx, "initials"]
        line = df.at[idx, "line"].replace(",", "")
        last_name = df.at[idx, "last_name"]

        if initials == "" and not re.search(FIRM_PATTERN, line):
            for word in line.split()[:-1]:
                candidates = prefix_dict.get(word[:2], [])
                if word in candidates and word != last_name:
                    df.at[idx, "initials"] = word
                    break
    return df


# ── Second last-name detection ──────────────────────────────────────────

def second_last_name(row):
    """Detect a second last name (token with ':') in the residual line."""
    remaining_line = row["residual_line"]
    remaining_line_split = remaining_line.split()

    if remaining_line_split:
        first_token = remaining_line_split[0].replace(",", "")
        if ":" in first_token and re.search(r'[A-Z]', first_token) and not re.search(FIRM_PATTERN, first_token):
            row["second_last_name"] = first_token

    return row


# ── Duplicate-initial adjustment ────────────────────────────────────────

def adj_initials_dupl(row):
    """Remove duplicated initials separated by a comma in the source line."""
    initials = str(row["initials"])
    line = str(row["line_complete"])

    for w in initials.split():
        if initials.split().count(w) > 1:
            positions = [m.start() for m in re.finditer(w, line)]
            if len(positions) > 1:
                line_cut = line[positions[0]:positions[-1]]
                if "," in line_cut:
                    last_occ = initials.rfind(w)
                    row["initials"] = initials[:last_occ].strip()
    return row
