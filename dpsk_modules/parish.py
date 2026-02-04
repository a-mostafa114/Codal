# -*- coding: utf-8 -*-
"""
parish.py – Parish extraction, cleaning, mapping, and quality checking.

This is one of the largest modules because parish handling spans several
passes:
  1. ``extract_parish``             – initial extraction (initials-based)
  2. ``extract_parish_no_init``     – extraction without initials
  3. ``extra_parish_residual_cases``– residual / edge-case extraction
  4. ``cleaned_parish``             – abbreviation → full-name mapping
  5. ``parish_map``                 – reference-table mapping
  6. ``parish_adjustment``          – fill parishes for rows still missing one
  7. ``firms_parishes_``            – assign parishes to firm rows
  8. Parish quality check (``run_parish_quality_check``)
"""

import re
import pandas as pd
from rapidfuzz import process, fuzz

from .config import FIRM_PATTERN, INITIALS_PATTERN, PARISH_DICT_KNOWN
from .utils import fuzzy_match_rapidfuzz


# ── 1. Extract parish (initials-based) ──────────────────────────────────

def extract_parish(row):
    parish_pattern = r'\b(?:[A-Z]{1,3}\.?|[A-Z]:\w+|[A-Z]:\s|[A-Z]:,|[A-Z][a-z]\.?|[A-Z][a-z]{2}\.?)'
    line = row["line_complete"]

    line_no_comma = line.replace(",", " ").replace("-", "")
    line_no_comma = re.sub(r'\s+', ' ', line_no_comma).strip()
    line_no_comma = re.sub(r'(\d+)[A-Za-z]+', r'\1', line_no_comma)
    line_no_comma_split = line_no_comma.split()

    inter_ = [x for x in line_no_comma_split if not re.search(r'[A-Za-z]', x) and re.search(r'\d', x)]
    if not inter_:
        return row

    pos_inc = line_no_comma_split.index(inter_[0])
    if pos_inc == 0:
        return row

    found_ = False
    candidate = ""
    j = 1
    while not found_ and pos_inc - j > 0:
        string_ = line_no_comma_split[pos_inc - j]
        if re.search(r'[A-Za-z]', string_):
            candidate = string_
            break
        else:
            j += 1

    if candidate and (
            (re.search(INITIALS_PATTERN, candidate) or re.search(parish_pattern, candidate)
             or re.fullmatch(r"[A-Z][a-z]", candidate))
            and row["split"] in [1, 3]):
        pos_cand = line.find(candidate)
        if pos_cand == -1:
            return row
        row["parish"] = candidate

        def comma_betw_init(s, line_2=line):
            tokens = s.split()
            for j_ in range(len(tokens) - 1):
                w, next_init = tokens[j_], tokens[j_ + 1]
                try:
                    index_1 = line_2.index(w)
                    index_2 = line_2.index(next_init)
                except ValueError:
                    continue
                if "," in line_2[index_1:index_2]:
                    return True
            return False

        initials_str = str(row["initials"])
        if (candidate in initials_str) and comma_betw_init(initials_str):
            if initials_str.split()[-1] == candidate:
                row["initials"] = " ".join(initials_str.split()[:-1])
            else:
                row["initials"] = initials_str.replace(candidate, "")
    return row


# ── 2. Extract parish (no initials) ────────────────────────────────────

def extract_parish_no_init(row):
    line = row["line_complete"]
    line_sec = re.sub(r'\s+', ' ', line).strip()
    line_sec = re.sub(r'(\d+)[A-Za-z]+', r'\1', line_sec)
    line_split = line_sec.split(",")
    for h, token in enumerate(line_split):
        if re.search(r'\d+', token):
            token_clean = token.replace("-", " ")
            parts = token_clean.split()
            if len(parts) > 1:
                line_split = line_split[:h] + parts + line_split[h + 1:]

    inter_ = [x for x in line_split if not re.search(r'[A-Za-z]', x) and re.search(r'\d', x)]
    if not inter_:
        return row

    pos_inc = line_split.index(inter_[0])
    if pos_inc == 0:
        return row

    candidate = ""
    j = 1
    while pos_inc - j > 0:
        string_ = line_split[pos_inc - j]
        if re.search(r'[A-Za-z]', string_):
            candidate = string_
            break
        else:
            j += 1

    candidate = candidate.strip()

    if ((re.search("-", candidate) or re.search(":", candidate)
         or any(word.islower() for word in candidate.split()))
            and row["split"] in [1, 3] and row["parish"] == ""
            and any(re.search(r'[a-z]', word) for word in candidate.split("-"))
            and any(re.search(r'[A-Z]', word) for word in candidate.split("-"))):
        pos_cand = line.find(candidate)
        if pos_cand == -1 and re.search(r'\d+', candidate):
            return row
        if candidate != row["occ_reg"]:
            row["parish"] = candidate
    return row


# ── 3. Extra parish residual cases ──────────────────────────────────────

def extra_parish_residual_cases(row):
    line = row["line_complete"]
    line_sec = re.sub(r'\s+', ' ', line).strip()
    line_sec = re.sub(r'(\d+)[A-Za-z]+', r'\1', line_sec)
    line_split = line_sec.split(",")
    for h, token in enumerate(line_split):
        if re.search(r'\d+\s*-\s*\d+', token):
            token_clean = token.replace("-", " ")
            parts = token_clean.split()
            if len(parts) > 1:
                line_split = line_split[:h] + parts + line_split[h + 1:]

    inter_ = [x for x in line_split if not re.search(r'[A-Za-z]', x) and re.search(r'\d', x)]
    if not inter_:
        return row

    pos_inc = line_split.index(inter_[0])
    if pos_inc == 0:
        return row

    candidate = ""
    j = 1
    while pos_inc - j > 0:
        string_ = line_split[pos_inc - j]
        if re.search(r'[A-Za-z]', string_):
            candidate = string_
            break
        else:
            j += 1

    candidate = candidate.strip()

    if (((re.search("-", candidate) or re.search(":", candidate)
          or any(word.islower() for word in candidate.split()))
         and row["split"] in [1, 3] and row["parish"] == ""
         and any(re.search(r'[a-z]', word) for word in candidate.split("-"))
         and any(re.search(r'[A-Z]', word) for word in candidate.split("-")))
            or re.fullmatch(r"[a-z]\.?", candidate)
            or re.fullmatch(r"[A-Z]][a-z]\.?", candidate)):
        pos_cand = line.find(candidate)
        if pos_cand == -1 and re.search(r'\d+', candidate):
            return row
        if candidate != row["occ_reg"]:
            row["parish"] = candidate
    return row


# ── 4. Clean parish via abbreviation dictionary ─────────────────────────

def cleaned_parish(row, dict_=None):
    if dict_ is None:
        dict_ = PARISH_DICT_KNOWN
    parish = str(row["parish"])
    if parish in dict_:
        row["parish_cleaned_"] = dict_[parish]
        return row
    parish_letters = "".join(re.findall(r"[A-Za-z]", parish))
    for key, value in dict_.items():
        key_letters = "".join(re.findall(r"[A-Za-z]", key))
        if abs(len(key) - len(parish)) <= 1 and parish_letters == key_letters:
            row["parish_cleaned_"] = value
            return row
    return row


# ── 5. Map parish against reference table ───────────────────────────────

def parish_map(row, proper_parish):
    parish = row["parish"]
    if row["parish_cleaned_"] != "":
        return row
    row_ = proper_parish.loc[proper_parish["parish"] == parish]
    if not row_.empty:
        mapped = row_["mapped_parish"].values[0]
        cleaned = row_["parish_cleaned"].values[0]
        row["parish_cleaned_"] = mapped if mapped != "" else cleaned
    return row


# ── 6. Parish adjustment (fill missing) ────────────────────────────────

def parish_adjustment(row, proper_parish, comma=True):
    line = row["line_complete"]
    if (row["parish"] == "" and row["split"] in [1, 3]
            and row["initials"] != "" and row["index"] != "A1"):
        line_split = line.split(",") if comma else line.split()
        number = re.findall(r'\d+', line) if re.findall(r'\d+', line) else ""
        number = [x for x in number if int(x) > 50]
        number = number[0] if number else ""
        idx = next((i for i, part in enumerate(line_split) if number in part), None)
        if idx is not None and idx > 0:
            candidate = line_split[idx - 1].strip()
            if candidate == "" and idx - 1 > 0:
                candidate = line_split[idx - 2].strip()
            if candidate in proper_parish["parish"].values or re.search(r'[A-Z]', candidate):
                row["parish"] = candidate
                row_ = proper_parish.loc[proper_parish["parish"] == candidate]
                if not row_.empty:
                    mapped = row_["mapped_parish"].values[0]
                    cleaned = row_["parish_cleaned"].values[0]
                    row["parish_cleaned_"] = mapped if mapped != "" else cleaned
    return row


# ── 7. Firm parishes ───────────────────────────────────────────────────

def firms_parishes_(row, initials=True, comma=True):
    line = row["line_complete"]
    if row["firm_dummy"] == 1 and row["parish"] == "" and row["split"] in [1, 3]:
        line_split = line.split(",") if comma else line.split()
        numbers = re.findall(r'\d+', line)
        numbers = [x for x in numbers if int(x) > 50]
        number = numbers[0] if numbers else ""
        part = [w for w in line_split if number in w]
        part = part[0] if part else ""
        pos = line_split.index(part) if part in line_split else 0
        candidate = line_split[pos - 1] if pos > 0 else ""
        candidate = candidate.strip()
        if initials:
            if (re.search(INITIALS_PATTERN, candidate) and not re.search(FIRM_PATTERN, candidate)) \
                    or re.fullmatch(r'[A-Za-z]{1,2}', candidate):
                row["parish"] = candidate
        else:
            if (re.search(r'[A-Z]', candidate) and re.search(r'[a-z]', candidate)
                    and not re.search(FIRM_PATTERN, candidate) and len(candidate) <= 25):
                row["parish"] = candidate
    return row


# ── 8. Spot wrong occupation / parish collision ─────────────────────────

def spot_wrong_occ(row, occ_list):
    occ_reg = str(row["occ_reg"]).strip()
    parish = str(row["parish"]).strip()
    if parish and occ_reg and parish == occ_reg:
        if not any(occ_reg.lower() == str(word).lower().strip() for word in occ_list["occ_llm"]):
            row["change_occ"] = 1
            row["occ_reg"] = ""
        else:
            row["change_occ"] = 1
            row["parish"] = ""
    return row


# ── 9. Remove firm patterns leaked into parish ─────────────────────────

def remove_firms_from_parish(row, parish_num, parish_firm):
    parish = row["parish"]
    if (any(word == parish for word in parish_firm[0].values)
            or (row["occ_reg"] in parish and len(row["occ_reg"]) > 4)
            or len(parish) > 30):
        row["parish"] = ""
        candidate = parish.split()[-1]
        if any(w == candidate for w in parish_num[0].values) and not re.search(FIRM_PATTERN, candidate):
            row["parish"] = candidate
            return row
    return row


# ── 10. Parish quality check ───────────────────────────────────────────

def run_parish_quality_check(surname_list, parish_mapped, parish_only_matched,
                             df_parish_added_year_by_year):
    """Full parish quality-check pipeline (Section 10 of original code).

    Parameters
    ----------
    surname_list : DataFrame
    parish_mapped : DataFrame – clean parish / county reference
    parish_only_matched : DataFrame – previously matched parish list
    df_parish_added_year_by_year : DataFrame – iteratively improved list

    Returns
    -------
    surname_list : DataFrame with ``matched_parish`` column added.
    """

    # -- Update known Stockholm parishes --
    stockholm_known_par = surname_list[
        (surname_list["municipality"] == "Stockholm")
        & (surname_list["parish"] != "")
        & (surname_list["split"].isin([1, 3]))
        & (surname_list["firm_dummy"] == 0)
        & (surname_list["estate_dummy"] == 0)
        & (surname_list["index"] != "A1")
    ][["parish", "municipality"]].drop_duplicates()

    stockholm_known_par["parish_old"] = stockholm_known_par["parish"]
    stockholm_known_par["parish"] = stockholm_known_par["parish"].apply(
        lambda x: re.sub(r'\b\.\s', "", x).strip())
    stockholm_known_par["parish"] = stockholm_known_par["parish"].apply(
        lambda x: re.sub(r'\(|\)', "", x))
    stockholm_known_par["parish"] = (
        stockholm_known_par["parish"].fillna("")
        .str.replace(r"^[^A-Za-zÀ-ÖØ-öø-ÿ]+", "", regex=True)
    )

    def _match_stk_parish(row):
        parish = str(row["parish"])
        parish_old = str(row["parish_old"])
        if "Stockholm" in row["municipality"] and parish_old not in parish_only_matched["parish_old"].values:
            parish_letters = "".join(re.findall(r"[A-Za-z]", parish))
            for key, value in PARISH_DICT_KNOWN.items():
                key_letters = "".join(re.findall(r"[A-Za-z]", key))
                if abs(len(key) - len(parish)) <= 1 and parish_letters == key_letters:
                    return value
        return ""

    stockholm_known_par["matched_parish"] = stockholm_known_par.apply(_match_stk_parish, axis=1)
    stockholm_known_par = stockholm_known_par[stockholm_known_par["matched_parish"] != ""]
    parish_only_matched = pd.concat([parish_only_matched, stockholm_known_par], axis=0)

    # -- Compare group --
    compare_group = parish_only_matched[["parish_old", "matched_parish", "municipality"]].copy()
    compare_group = compare_group.rename(columns={"parish_old": "parish"})
    compare_group = compare_group.drop_duplicates()

    # -- Analyse unmatched parishes --
    parish_analyzed = surname_list[["municipality", "parish"]].drop_duplicates()
    parish_analyzed["parish_old"] = parish_analyzed["parish"]
    parish_analyzed = parish_analyzed[parish_analyzed["parish"].apply(lambda x: not re.search(r'\d', x))]
    parish_analyzed["parish"] = parish_analyzed["parish"].apply(
        lambda x: re.sub(r'\b\.\s', "", x).strip())
    parish_analyzed["parish"] = parish_analyzed["parish"].apply(lambda x: re.sub(r'\(|\)', "", x))
    parish_analyzed["parish"] = (
        parish_analyzed["parish"].fillna("")
        .str.replace(r"^[^A-Za-zÀ-ÖØ-öø-ÿ]+", "", regex=True)
    )
    parish_analyzed = parish_analyzed[
        (~parish_analyzed["parish"].isin(parish_only_matched["parish_old"]))
        & (~parish_analyzed["parish"].isin(df_parish_added_year_by_year["parish_old"]))
        & (parish_analyzed["parish"] != "")
        & (parish_analyzed["parish"].apply(
            lambda s: (
                (letters := ''.join(re.findall(r'[A-Za-z]', str(s))))
                and not any(letters == ''.join(re.findall(r'[A-Za-z]', str(key)))
                            for key in PARISH_DICT_KNOWN.keys())
                and letters not in parish_only_matched["parish_old"].values
                and not any(letters == ''.join(re.findall(r'[A-Za-z]', str(word)))
                            for word in df_parish_added_year_by_year["parish"].values)
            )
        ))
    ]

    def _check_on_parishes(row):
        parish_ = str(row["parish"])
        municip_, score, _ = fuzzy_match_rapidfuzz(row["municipality"], parish_mapped["municipality"])
        municip_ = municip_ if score >= 85.5 else row["municipality"]
        subgroup = parish_mapped[parish_mapped["municipality"] == municip_]
        if not subgroup.empty:
            match = fuzzy_match_rapidfuzz(parish_, subgroup["Parish"])
            if match and len(match) == 3:
                best_match, score, _ = match
                if score >= 85.5 and best_match is not None:
                    row["matched_parish"] = best_match
                    return row
        match = fuzzy_match_rapidfuzz(parish_, parish_mapped["Parish"])
        if match and len(match) == 3:
            best_match, score, _ = match
            if score > 85.5 and best_match is not None:
                row["matched_parish"] = best_match
                return row
        parish_2 = re.sub(r'(\w+)\s*-\s*(\w+)', r'\1\2', parish_)
        match = fuzzy_match_rapidfuzz(parish_2, parish_mapped["Parish"])
        if match and len(match) == 3:
            best_match, score, _ = match
            if score > 85.5 and best_match is not None:
                row["matched_parish"] = best_match
                return row
        row["matched_parish"] = ""
        return row

    parish_analyzed["matched_parish"] = ""
    parish_analyzed = parish_analyzed.apply(_check_on_parishes, axis=1)
    parish_analyzed = parish_analyzed[parish_analyzed["matched_parish"] != ""]

    # -- Apply quality filter --
    surname_list["parish"] = surname_list["parish"].apply(
        lambda s: s if (
            s in parish_only_matched["parish_old"].values
            or s in df_parish_added_year_by_year["parish_old"].values
            or s in parish_analyzed["parish_old"].values
            or any(''.join(re.findall(r'[A-Za-z]', s)) == ''.join(re.findall(r'[A-Za-z]', word))
                   and abs(len(s) - len(word)) <= 2
                   for word in df_parish_added_year_by_year["parish"].values)
            or any(''.join(re.findall(r'[A-Za-z]', s)) in word
                   and abs(len(s) - len(word)) <= 2
                   for word in parish_only_matched["parish"].values)
        ) else ""
    )

    if "parish_cleaned_" in surname_list.columns:
        surname_list = surname_list.drop(columns=["parish_cleaned_"])
    surname_list = pd.merge(surname_list, compare_group, on=["parish", "municipality"], how='left')
    surname_list["matched_parish"] = surname_list["matched_parish"].fillna("")
    surname_list["matched_parish"] = surname_list.apply(
        lambda s: s["parish"] if s["parish"] != "" and s["matched_parish"] == "" else s["matched_parish"],
        axis=1)

    return surname_list
