# -*- coding: utf-8 -*-
"""
line_processing.py – Line cleaning, splitting, and residual extraction.

Covers:
  - Comma/dot number cleaning
  - Initial "0" → "O" correction
  - Double-dot fix
  - The ``split_line`` algorithm (conditions 1 / 1-bis / 2 / 2-bis / 2-extra)
  - Third-line adjustment
  - Secondary-line adjustments
"""

import re
import string
import pandas as pd

from .config import FIRM_PATTERN, INITIALS_PATTERN


# ── Number / punctuation cleaning ───────────────────────────────────────

def clean_comma_num(row):
    line = str(row["line_complete"])
    if (re.search(r'\d,\d\d', line)
        and (not any(x > 100 for x in [int(n) for n in re.findall(r'\d+', line)])
             or any(x < 10 for x in [int(n) for n in re.findall(r'\d+', line)]))):
        row["line"] = re.sub(r'(\d)\s*,\s*(\d\d)', r'\1\2', row["line"])
    return row


def clean_dot_num(row):
    line = str(row["line_complete"])
    if (re.search(r'\d,\d\d', line)
        and (not any(x > 100 for x in [int(n) for n in re.findall(r'\d+', line)])
             or any(x < 10 for x in [int(n) for n in re.findall(r'\d+', line)]))):
        row["line"] = re.sub(r'(\d)\s*,\s*(\d\d)', r'\1\2', row["line"])
    return row


def fix_initials_and_dots(surname_list):
    """Fix '0.' → 'O.', '..' → '., ', 'X.. y' → 'X., y'."""
    for col in ["line", "line_complete"]:
        surname_list[col] = surname_list[col].apply(lambda x: re.sub(r"\s0\.,", " O.,", x))
        surname_list[col] = surname_list.apply(
            lambda x: re.sub(r"\.\.\s(\d)", r"., \1", x[col]), axis=1)
        surname_list[col] = surname_list.apply(
            lambda x: re.sub(r"\.\.\s-", r"., -", x[col]), axis=1)
        surname_list[col] = surname_list[col].apply(
            lambda x: re.sub(r'([A-Z])\.\.\s([a-z])', r'\1., \2', str(x)))
    return surname_list


# ── Residual-line extraction ────────────────────────────────────────────

def get_the_residual_line(row):
    """Remove the last_name from the line and return the residual."""
    last_name = row["last_name"]
    residual_line = row["line_complete"]
    if isinstance(last_name, str) and last_name.strip() != "":
        residual_line = residual_line.replace(last_name, "", 1)
        for i, ch in enumerate(residual_line):
            if not ch.isalpha():
                residual_line = residual_line[i:]
                break
        residual_line = residual_line.strip()
    row["residual_line"] = residual_line
    return row


def update_residual_after_initials(row):
    """Remove the initials from the residual line."""
    initials = row["initials"]
    residual_line = row["residual_line"]
    if isinstance(initials, str) and initials.strip() != "":
        residual_line = residual_line.replace(initials, "", 1)
        for i, ch in enumerate(residual_line):
            if not ch.isalpha():
                residual_line = residual_line[i:]
                break
        residual_line = residual_line.strip()
    row["residual_line"] = residual_line
    if initials == ".":
        row["initials"] = ""
    if initials and initials[0] == ".":
        row["initials"] = initials[0:].strip()
    return row


def update_residual_after_second_last_name(row):
    """Remove the second_last_name from the residual line."""
    second_last_name = row["second_last_name"]
    residual_line = row["residual_line"]
    if isinstance(second_last_name, str) and second_last_name.strip() != "":
        residual_line = residual_line.replace(second_last_name, "", 1)
        for i, ch in enumerate(residual_line):
            if not ch.isalpha():
                residual_line = residual_line[i:]
                break
        residual_line = residual_line.strip()
    row["residual_line"] = residual_line
    return row


def update_residual_after_occupation(row):
    """Remove the occupation from the residual line."""
    if row["occ_reg"] != "":
        occ_reg = row["occ_reg"].strip()
    else:
        if row["residual_line"].split(",")[0].strip().islower():
            occ_reg = row["residual_line"].split(",")[0].strip()
        else:
            return row

    row["residual_line"] = row["residual_line"].strip()
    residual_line = row["residual_line"]

    if isinstance(occ_reg, str) and occ_reg.strip() != "" and occ_reg.lower() in residual_line.lower():
        index_fin = residual_line.lower().index(occ_reg.lower()) + len(occ_reg)
        residual_line = residual_line[index_fin:]
        for i, ch in enumerate(residual_line):
            if not ch.isalpha():
                residual_line = residual_line[i:]
                break
        residual_line = residual_line.strip()

    row["residual_line"] = residual_line
    return row


# ── Line splitting ──────────────────────────────────────────────────────

def _only_firm_occup_pattern_hyphens(line, occ_list):
    """Return True if every '-' in the line belongs to a firm/occ pattern."""
    firm_pattern_list = (
        [re.sub(r'\\', '', p) for p in FIRM_PATTERN.split('|')]
        + ["fabrik", "sverk", "Bank", "bank ", "bank,", "Jarnvag ", "Jarnvag,", "jarnvag,", "jarnvag "]
    )
    firm_pattern_list = [w for w in firm_pattern_list if "-" in w]
    occ_with_line = occ_list[occ_list["occ_llm"].str.contains("-")]
    occ_with_line = occ_with_line.sort_values(by="occ_llm", key=lambda x: x.str.len(), ascending=False)

    hyphen_positions = [m.start() for m in re.finditer(r'-', line)]
    if not hyphen_positions:
        return True

    for word in firm_pattern_list:
        if word in line:
            control_ = line.replace(word, "")
            return "-" not in control_

    for word in occ_with_line["occ_llm"]:
        if word in line:
            control_ = line.replace(word, "")
            return "-" not in control_

    if re.search(r'\b[a-zA-Z]+(\s*-\s*)[a-zA-Z]+\b', line):
        return True

    return False  # fallback


def split_line(df, occ_list):
    """Assign split codes (0=solo, 1=first-half, 2=second-half, 3=complete)."""
    df = df.copy()
    df["split"] = 0

    idx_list = df.index.to_list()

    for pos, idx in enumerate(idx_list):
        line = str(df.at[idx, "line"]) if pd.notna(df.at[idx, "line"]) else ""
        line = line.rstrip()

        if df.at[idx, "index"] == "A1" and re.search(r'\d+', line):
            df.at[idx, "split"] = 3
            continue

        if df.at[idx, "split"] != 0:
            continue

        if df.at[idx, "line"].startswith("Paul U. Bergstroms A.-B.") and df.at[idx, "row"] == 1:
            continue

        # ---------- Condition 1 ----------
        if (re.search(r'[A-Z]', line) and re.search(r'[a-z]', line)
                and any(n > 1000 for n in [int(x) for x in re.findall(r'\d+', line)])
                and len(re.findall(r'\d', line)) > 1
                and re.search(r'-\s*\d+', line) and len(line) > 5
                and len(max(max(line.split(), key=len, default=""),
                            max(line.split(","), key=len, default=""), key=len)) > 4):
            if pos + 1 < len(idx_list):
                nxt = idx_list[pos + 1]
                next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
                combined = line + next_line
            if (not re.search(r'[A-Za-z]', next_line)
                    and len(re.findall(r'\d+', next_line)) == 1
                    and all(n > 1000 for n in [int(x) for x in re.findall(r'\d+', next_line)])
                    and (re.search(r'\d+\s*-\s*\d+\s*-\s*\d+', combined)
                         or re.search(r'\d+\s*-\s*\d+\s\d+', combined)
                         and all(n > 15000 for n in [int(x) for x in re.findall(r'\d+', next_line)]))):
                df.at[idx, "split"] = 1
                df.at[nxt, "split"] = 2
            else:
                df.at[idx, "split"] = 3
            continue

        # ---------- Condition 1-bis ----------
        if (line and re.search(r'[A-Z]', line) and re.search(r'[a-z]', line)
                and re.search(r'\d+', line) and len(re.findall(r'\d', line)) > 1
                and (df.at[idx, "initials"] != "" or re.search(FIRM_PATTERN, line))
                and not line[0].isdigit()
                and (line.endswith('-') or line[-1].isalpha() or line.endswith(',')
                     or ((df.at[idx, "initials"] != "" or re.search(FIRM_PATTERN, line))
                         and line[-1].isdigit()))):
            if pos + 1 < len(idx_list):
                nxt = idx_list[pos + 1]
                next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
                if next_line and (next_line[0].isupper()
                                  or (next_line.startswith("de ")
                                      and any(n > 1000 for n in [int(x) for x in re.findall(r'\d+', line)]))):
                    df.at[idx, "split"] = 3
                    continue

        if line and line.endswith(("->", "-.", ">", "<", "/")):
            line = line[:-1]

        # ---------- Condition 2 ----------
        if (line and not line[0].isnumeric()
                and (re.search(r'[A-Za-z]', line.split(",")[0]) or re.search(r'[A-Za-z]', line.split()[0]))
                and (line[-1].isdigit() or line[-1] in ['-', '.', ',', ')', ";", ">"] or line[-1].isalpha())
                and re.search(r'[A-Z]', line) and re.search(r'[a-z]', line)
                and len(line) > 10 and not re.search(r'\d+\s*inv\.\)', line)
                and pos + 1 < len(idx_list)):
            nxt = idx_list[pos + 1]
            next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
            combined = line + next_line
            if (df.at[nxt, "index"] != "A1"
                    and (re.search(r'-\s*\d+', combined) or re.search(r'\s*\d+-', combined)
                         or re.search(r'\b\d+\s+\d+\b', next_line)
                         or (re.search(r'\d+', next_line)
                             and all(n > 1000 for n in [int(x) for x in re.findall(r'\d+', next_line)])))
                    and (((not next_line or (not next_line[0].isupper()
                                            or (next_line[0].isupper() and next_line[1] in [".", ","])
                                            or (next_line[0].isupper() and next_line[2] in [".", ","])))
                          and any(num > 1000 for num in [int(x) for x in re.findall(r'\d+', next_line)]))
                         or (re.search(FIRM_PATTERN, line)
                             and any(num > 1000 for num in [int(x) for x in re.findall(r'\d+', next_line)])
                             and (not next_line or (not next_line[0].isupper()
                                                    or (next_line[0].isupper() and not next_line.startswith("A.-B") and next_line[1] in [".", ","])
                                                    or (next_line[0].isupper() and next_line[2] in [".", ","])))))):
                df.at[idx, "split"] = 1
                df.at[nxt, "split"] = 2
                continue

        # ---------- Condition 2-bis ----------
        if (line
                and (re.search(r'[A-Za-z]', line.split(",")[0]) or re.search(r'[A-Za-z]', line.split()[0]))
                and _only_firm_occup_pattern_hyphens(line, occ_list)
                and len(line) > 10
                and not re.search(r'\d+(?:\s+|-)\d+', line)
                and not re.search(r'\d+\s*inv\.\)', line)
                and ((re.search(r'[a-z]', line) and re.search(r'[A-Z]', line) and line[0].isupper())
                     or re.search(FIRM_PATTERN, line))):
            if pos + 1 < len(idx_list):
                nxt = idx_list[pos + 1]
                next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
                nums = re.findall(r'\d+', next_line)
                if (df.at[nxt, "index"] != "A1" and nums and "-" not in next_line
                        and not re.search(r'[A-Za-z]', next_line)):
                    nums_int = [int(n) for n in nums]
                    if (len(nums_int) == 1 and nums_int[0] > 1000) or (len(nums_int) == 2 and min(nums_int) > 1000):
                        next_line = "-" + next_line
                        combined = line + next_line
                        if ((re.search(r'-\s*\d+', combined) and not next_line[0].isupper())
                                or ((re.search(r'\s*\d+-', combined) or re.search(r'^-\s*\d+\s+\d+$', next_line))
                                    and re.search(FIRM_PATTERN, line)
                                    and (not next_line[0].isupper()
                                         or (next_line[0].isupper() and not next_line.startswith("A.-B") and next_line[1] in [".", ","])
                                         or (next_line[0].isupper() and next_line[2] in [".", ","])))):
                            df.at[idx, "split"] = 1
                            df.at[nxt, "split"] = 2
                            continue

        # ---------- Condition 2-extra ----------
        if (line and not re.search(r'\d', line) and len(line) > 10
                and not re.search(r'\d+\s*inv\.\)', line)
                and ((line[0].isupper() and re.search(r'[a-z]', line) and re.search(r'[A-Z]', line)
                      and "," in line and df.at[idx, "initials"] != "")
                     or re.search(FIRM_PATTERN, line))):
            if pos + 1 < len(idx_list):
                nxt = idx_list[pos + 1]
                next_line = str(df.at[nxt, "line"]) if pd.notna(df.at[nxt, "line"]) else ""
                nums_ = re.findall(r'\d+\s*-', next_line)
                nums = re.findall(r'\d+(?=\s*-)', next_line)
                if (df.at[nxt, "index"] != "A1" and nums_ and nums
                        and len(nums) == 1 and int(nums[0]) > 1000
                        and not re.search(r'[A-Za-z]', next_line)):
                    combined_line = line + next_line
                    if re.search(r'\d+\s*-', combined_line):
                        df.at[idx, "split"] = 1
                        df.at[nxt, "split"] = 2
                        continue

    return df


# ── Third-line adjustment ───────────────────────────────────────────────

def third_line(df, occ_list):
    """Detect lines that are a *third* part (split=4) of a multi-row entry."""
    idx_list = df.index.to_list()
    occ__ = occ_list[occ_list["occ_llm"].str.len() > 3]
    for pos, idx in enumerate(idx_list):
        line = df.at[idx, "line"]
        if df.at[idx, "split"] == 2 and (len(line) > 25 or (not re.search(r'[A-Za-z]', line))):
            if pos + 1 < len(idx_list):
                idx_next = idx_list[pos + 1]
                next_line = df.at[idx_next, "line"]
                if pos - 1 >= 0:
                    idx_prev = idx_list[pos - 1]
                    prev_compl_line = df.at[idx_prev, "line_complete"]
                    prev_line = df.at[idx_prev, "line"]
                    combined_ = prev_compl_line + next_line
                    if (all(n > 6000 for n in [int(x) for x in re.findall(r'\d+', next_line)])
                            and next_line != "-"
                            and (all(n > 1000 for n in [int(x) for x in re.findall(r'\d+', line)]) or len(line) > 35)
                            and not re.search(r'[A-Za-z]', next_line)
                            and df.at[idx_next, "split"] == 0
                            and len(re.findall(r'\d+', combined_)) <= 3
                            and not any(word in df.at[idx, "line"] for word in occ__)
                            and (re.search(FIRM_PATTERN, prev_line) or df.at[idx_prev, "initials"] != "")
                            and not any(w in re.findall(FIRM_PATTERN, line) for w in re.findall(FIRM_PATTERN, prev_line))):
                        df.at[idx_next, "split"] = 4
                        df.at[idx_prev, "line_complete"] = prev_compl_line + " " + next_line
    return df


# ── Secondary lowercase last-name adjustment ────────────────────────────

def adj_sec_lowercase_LN(df):
    """Fix second-line entries that start with a lowercase last name."""
    idx_list = df.index.to_list()
    for pos, idx in enumerate(idx_list):
        line = df.at[idx, "line"]
        prv_line = None
        nxt_line = None
        last_name = str(df.at[idx, "last_name"])
        if pos - 1 >= 0:
            prv = idx_list[pos - 1]
            prv_line = df.at[prv, "line"]
        if pos + 1 < len(df):
            nxt = idx_list[pos + 1]
            next_line = df.at[nxt, "line"]
        if (prv_line is not None and prv_line[-1] != "-"
                and df.at[idx, "split"] == 2 and line.startswith(last_name)
                and not re.search(r"\(", line) and len(last_name) > 1
                and (last_name[0].islower() or (last_name[0].isupper() and last_name[1] == "."))):
            df.at[idx, "split"] = 0
            df.at[prv, "split"] = 0
            if (not re.search(r'[A-Za-z]', next_line)
                    and any(x > 1000 for x in [int(n) for n in re.findall(r'\d+', next_line)])
                    and df.at[nxt, "split"] == 0):
                df.at[idx, "split"] = 1
                df.at[nxt, "split"] = 2
                df.at[idx, "line_complete"] = df.at[idx, "line_complete"] + df.at[nxt, "line_complete"]
            else:
                df.at[idx, "split"] = 3
            if (len(prv_line.split(",")) > 2
                    and (re.search(FIRM_PATTERN, prv_line) or re.search(INITIALS_PATTERN, prv_line))
                    and any(x > 1000 for x in [int(n) for n in re.findall(r'\d+', next_line)])):
                df.at[prv, "split"] = 3
    return df


# ── Extra first-half adjustment ─────────────────────────────────────────

def adj_extra_FH(df):
    idx_list = df.index.to_list()
    for pos, idx in enumerate(idx_list):
        line = df.at[idx, "line"]
        if pos + 2 < len(df):
            nxt = idx_list[pos + 1]
            nxt_nxt = idx_list[pos + 2]
            if (df.at[idx, "split"] == 1 and df.at[nxt, "split"] != 2
                    and df.at[nxt, "line"] != "-" and df.at[nxt_nxt, "split"] != 2):
                if (df.at[idx, "initials"] != "" and re.search(r'\d+', line)) or df.at[idx, "occ_reg"] != "":
                    df.at[idx, "split"] = 3
                else:
                    df.at[idx, "split"] = 0
    return df
