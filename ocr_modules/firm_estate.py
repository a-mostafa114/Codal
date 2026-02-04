# -*- coding: utf-8 -*-
"""
firm_estate.py – Firm-dummy and estate-dummy token detection.
"""

import re
import pandas as pd

from .config import FIRM_PATTERN, ESTATE_PATTERN


# ── Firm token ──────────────────────────────────────────────────────────

def firm_token(row):
    """Set ``firm_dummy = 1`` when a firm pattern is found in the line."""
    line = row["line_complete"]
    if pd.notna(line) and re.search(FIRM_PATTERN, line):
        row["firm_dummy"] = 1
    if "(" in line:
        new_complete_line = re.sub(r'\([^)]*\)', '', line)
        if not re.search(FIRM_PATTERN, new_complete_line):
            row["firm_dummy"] = 0
    if ")" in line and "(" not in line:
        def strip_parentheses_fragments(s):
            s2 = re.sub(r'\([^)]*\)', '', s)
            if ")" in s2 and "(" not in s2:
                s2 = s2.split(")")[-1].strip()
            if "(" in s2 and ")" not in s2:
                s2 = s2.split("(")[0].strip()
            return s2
        new_complete_line_2 = strip_parentheses_fragments(line)
        if not re.search(FIRM_PATTERN, new_complete_line_2):
            row["firm_dummy"] = 0
    return row


# ── Individual + firm-token adjustment ──────────────────────────────────

def _ind_FT(df, df_death_reg_unacc, surname_list):
    """Resolve cases where a row has both an occupation and a firm flag."""
    df["change"] = 0
    df_copy = df[
        (df["occ_reg"] != "")
        & (df["firm_dummy"] == 1)
        & (df["last_name"] != "")
        & (df["line"].str.contains(r'[A-Z]\w*,\s*\b(?:[A-Z]\.)'))
    ].copy()

    df_copy["line"] = df_copy["line"].str.replace("A.-B.", "", regex=False)

    df_copy = df_copy[
        (df_copy["occ_reg"] != "")
        & (df_copy["firm_dummy"] == 1)
        & (df_copy["last_name"] != "")
        & (df_copy["line"].str.contains(r'\w+,\s*\b(?:[A-Z]\.)'))
    ]

    def get_ind_with_FT(row):
        last_name = row["last_name"]
        line = row["line"]
        line_complete = row["line_complete"]

        if "(" in line_complete:
            match_original = re.search(FIRM_PATTERN, line_complete)
            new_complete_line = re.sub(r'\([^)]*\)', '', line_complete)
            match_clean = re.search(FIRM_PATTERN, new_complete_line)
            if match_original and not match_clean:
                row["firm_dummy"] = 0
                row["change"] = 1
            return row

        if last_name.endswith("s"):
            last_name_upd = last_name[:-1]
            if last_name_upd not in df_death_reg_unacc["last_name"].values:
                row["firm_dummy"] = 0
                row["change"] = 1
                return row

        if re.search(r'dir|kontorist', line):
            row["firm_dummy"] = 0
            row["change"] = 1
            return row

        if line.startswith("Bank"):
            row["firm_dummy"] = 0
            row["change"] = 1
            return row

        line_low = line_complete.lower()
        start_pos_occ = line_low.find(row["occ_reg"])
        if start_pos_occ != -1:
            end_pos_occ = start_pos_occ + len(row["occ_reg"]) - 1
            list_next_word = [line_low[end_pos_occ + 1:x]
                              for x in range(end_pos_occ + 2, len(line_complete))]
            if any(re.search(FIRM_PATTERN, x) for x in list_next_word):
                row["firm_dummy"] = 0
                row["change"] = 1
            return row

        return row

    df_copy = df_copy.apply(get_ind_with_FT, axis=1)
    df.loc[df_copy.index, ["firm_dummy", "change"]] = df_copy[["firm_dummy", "change"]]

    idx_list_ = df.index.to_list()
    for pos, idx in enumerate(idx_list_[:-1]):
        nxt = idx_list_[pos + 1]
        if (df.at[idx, "split"] == 1 and df.at[idx, "change"] == 1
                and df.at[nxt, "split"] == 2 and df.at[nxt, "firm_dummy"] == 1):
            df.at[nxt, "firm_dummy"] = 0

    return df


# ── Estate token ────────────────────────────────────────────────────────

def estate_token(row):
    """Set ``estate_dummy = 1`` when an estate pattern is found."""
    line = row["line_complete"]
    if pd.notna(line) and re.search(ESTATE_PATTERN, line) and not re.search(r'starbhusnot\.', line):
        row["estate_dummy"] = 1
    return row
