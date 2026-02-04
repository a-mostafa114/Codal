# -*- coding: utf-8 -*-
"""
income.py – Income extraction, splitting, and line-unification.
"""

import re
import pandas as pd


# ── Unite separated lines ───────────────────────────────────────────────

def unite_lines(df_):
    """Combine first-half (split=1) with its continuation line."""
    df_["line_complete"] = df_["line"].fillna("")
    for i in range(len(df_) - 1):
        if df_.iloc[i]["split"] == 1:
            current = str(df_.iloc[i]["line"]).rstrip()
            if str(df_.iloc[i + 1]["line"]) != "-":
                next_line = str(df_.iloc[i + 1]["line"]).lstrip()
            else:
                next_line = str(df_.iloc[i + 2]["line"]).lstrip()
            df_.at[df_.index[i], "line_complete"] = current + " " + next_line
    return df_


# ── Extract raw income string ───────────────────────────────────────────

def extr_inc(row):
    """Walk backwards from end of ``line_complete`` to extract income digits."""
    def income_until_punct(s):
        i = len(s) - 1
        income_ = []
        started = False
        while i >= 0:
            ch = s[i]
            if ch.isdigit():
                income_.insert(0, ch)
                started = True
            elif ch in [',', '.'] and i != len(s) - 1:
                break
            elif not ch.isalpha() and started:
                income_.insert(0, ch)
            elif ch.isalpha():
                break
            i -= 1
        return ''.join(income_) if income_ else None

    if row.get("split") in [0, 1, 3] and isinstance(row.get("line_complete"), str):
        row["income"] = income_until_punct(row["line_complete"])
    else:
        row["income"] = None
    row["income"] = row["income"].lstrip() if isinstance(row["income"], str) else row["income"]
    return row


# ── Split income into two parts ─────────────────────────────────────────

def split_income(row):
    """Split a raw income string into ``income_1`` and ``income_2``."""
    income = row.get("income")
    if not isinstance(income, str):
        income = ""

    income_1 = ""
    income_2 = ""
    buffer = []
    start = False
    first_end = False
    i = 0

    while i < len(income):
        ch = income[i]
        if ch.isdigit():
            buffer.append(ch)
            start = True
            i += 1
        else:
            if start:
                if not first_end:
                    income_1 = ''.join(buffer)
                    buffer = []
                    first_end = True
                    start = False
                else:
                    income_2 = ''.join(buffer)
                    break
            i += 1

    if not first_end and buffer:
        income_1 = ''.join(buffer)
    elif first_end and buffer:
        income_2 = ''.join(buffer)

    row["income_1"] = income_1
    row["income_2"] = income_2
    return row


# ── Combined income pipeline ────────────────────────────────────────────

def find_income(df_, third_line_func, occ_list):
    """Run the full income-extraction pipeline on a dataframe.

    Parameters
    ----------
    df_ : DataFrame
    third_line_func : callable
        ``line_processing.third_line``
    occ_list : DataFrame
    """
    if df_ is not None and not df_.empty:
        df_ = unite_lines(df_)
        df_ = third_line_func(df_, occ_list)
        df_["income"] = 0
        df_ = df_.apply(extr_inc, axis=1)
        df_["income_1"] = ""
        df_["income_2"] = ""
        df_ = df_.apply(split_income, axis=1)
        df_["income"] = df_["income"].apply(
            lambda x: x if bool(re.search(r'\d', str(x))) else "")
    return df_
