# -*- coding: utf-8 -*-
"""
classification.py – Classify rows into certain categories and detect
potential second / first lines.

Categories
----------
- pages_to_cut, certain_estate, certain_locations_inv, certain_Tel_int,
  only_dash, certain_ind_A1_complete, IT_1, IT_2, certain_firms,
  certain_noise_A, certain_noise_B
- potential_sec_line_A/B/C/D
- pot_first_line
"""

import re
import string
import pandas as pd

from .config import FIRM_PATTERN, INITIALS_PATTERN, CITIES_PAR


# ── Determine pages to cut ──────────────────────────────────────────────

def find_pages_to_cut(surname_list):
    pages_to_cut = surname_list.groupby("page").filter(
        lambda g: (
            ((g["last_name"].str.strip() != "").sum() < 5)
            and ((g["occ_reg"].str.strip() != "").sum() < 5)
            and ((g["firm_dummy"] != 0).sum() < 10)
        ) or ((g["line"].str.len() > 60).sum() > 3)
        or ((g["occ_reg"].str.strip() != "").sum() < 1)
    )
    return pages_to_cut["page"].unique()


# ── Main certain-lines classification ───────────────────────────────────

def certain_lines(df, pages_to_cut, occ_list):
    """Classify every row into one of the 'certain' buckets."""

    df["pages_to_cut"] = 0
    df["certain_estate"] = 0
    df["certain_locations_inv"] = 0
    df["certain_Tel_int"] = 0
    df["only_dash"] = 0
    df["certain_ind_A1_complete"] = 0
    df["IT_1"] = 0
    df["IT_2"] = 0
    df["certain_firms"] = 0
    df["certain_noise_A"] = 0
    df["certain_noise_B"] = 0

    df["pages_to_cut"] = df["page"].apply(lambda x: 1 if x in pages_to_cut else 0)

    df["certain_locations_inv"] = df.apply(
        lambda x: 1 if (pd.notna(x["line"]) and re.search(r'inv\.\)', str(x["line"]))
                        and x["pages_to_cut"] == 0) else 0, axis=1)

    df["certain_Tel_int"] = df.apply(
        lambda x: 1 if (
            pd.notna(x["line_complete"]) and (
                re.search(r'[Tt]el\.\s*\d', x["line_complete"])
                or re.search(r'[Tt]el\s*\d', x["line_complete"])
                or re.search(r'[Tt]el\.\s', x["line_complete"])
                or re.search(r'Allm\.\s*[Tt]el', x["line_complete"])
            ) and x["pages_to_cut"] == 0
        ) else 0, axis=1)

    df["only_dash"] = df.apply(
        lambda x: 1 if (not re.search(r'[A-Za-z]', str(x["line"]))
                        and not re.search(r'\d+', str(x["line"]))
                        and x["pages_to_cut"] == 0) else 0, axis=1)

    certain = {
        "certain_only_dash": df[df["only_dash"] == 1],
        "certain_locations_inv": df[df["certain_locations_inv"] == 1],
        "certain_Tel_int": df[df["certain_Tel_int"] == 1],
        "pages_to_cut": df[df["pages_to_cut"] == 1],
    }

    df_clean = df[(df["only_dash"] == 0) & (df["certain_locations_inv"] == 0)
                  & (df["certain_Tel_int"] == 0)].copy()
    idx_list_ = df_clean.index.to_list()

    for pos, idx in enumerate(idx_list_):
        line_compact = "".join(
            str(v) if pd.notna(v) else ""
            for v in [df_clean.at[idx, "last_name"], df_clean.at[idx, "initials"],
                      df_clean.at[idx, "occ_reg"]])
        line_compact_strip = re.sub(rf"[{string.punctuation}\s]+", "", line_compact)

        initials_2 = df_clean.at[idx, "initials"].split()[:-1]
        initials_2_str = "".join(initials_2)
        line_compact_2 = "".join(
            str(v) if pd.notna(v) else ""
            for v in [df_clean.at[idx, "last_name"], initials_2_str, df_clean.at[idx, "occ_reg"]])
        line_compact_strip_2 = re.sub(rf"[{string.punctuation}\s]+", "", line_compact_2)

        line_complete = str(df_clean.at[idx, "line_complete"]) if pd.notna(df_clean.at[idx, "line_complete"]) else ""
        line_complete_strip = re.sub(rf"[{string.punctuation}\s]+", "", line_complete)

        line = str(df_clean.at[idx, "line"]) if pd.notna(df_clean.at[idx, "line"]) else ""

        # Skip already-classified
        if any(df_clean.at[idx, c] != 0 for c in [
            "pages_to_cut", "certain_estate", "certain_locations_inv",
            "certain_Tel_int", "only_dash", "certain_ind_A1_complete",
            "IT_1", "IT_2", "certain_firms", "certain_noise_A", "certain_noise_B"]):
            continue

        next_line = ""
        nxt = None
        if pos + 1 < len(idx_list_):
            nxt = idx_list_[pos + 1]
            next_line = str(df_clean.at[nxt, "line"]) if pd.notna(df_clean.at[nxt, "line"]) else ""

        # Estate
        if df_clean.at[idx, "estate_dummy"] == 1:
            df_clean.at[idx, "certain_estate"] = 1
            if df_clean.at[idx, "split"] == 1 and nxt is not None and df_clean.at[nxt, "split"] == 2:
                df_clean.at[nxt, "certain_estate"] = 1
            continue

        # A1
        if df_clean.at[idx, "index"] == "A1":
            df_clean.at[idx, "certain_ind_A1_complete"] = 1
            continue

        # IT_1: LN + initials + occupation
        if (df_clean.at[idx, "split"]
                and df_clean.at[idx, "occ_reg"] != "" and df_clean.at[idx, "last_name"] != ""
                and df_clean.at[idx, "initials"] != ""
                and (line_complete_strip.lower().startswith(line_compact_strip.lower())
                     or (line_complete_strip.lower().startswith(line_compact_strip_2.lower())
                         and df.at[idx, "initials"] != ""))
                and df_clean.at[idx, "split"] != 2 and df_clean.at[idx, "firm_dummy"] == 0
                and ((nxt is not None and df_clean.at[nxt, "firm_dummy"] == 0
                      and df_clean.at[idx, "split"] == 1)
                     or df_clean.at[idx, "split"] in [0, 2, 3])):
            df_clean.at[idx, "IT_1"] = 1
            if df_clean.at[idx, "split"] == 1 and nxt is not None and df_clean.at[nxt, "split"] == 2:
                df_clean.at[nxt, "IT_1"] = 1
            continue

        # IT_2
        if (df_clean.at[idx, "firm_dummy"] == 0 and df_clean.at[idx, "split"] != 2
                and (df_clean.at[idx, "occ_reg"] != ""
                     or (df_clean.at[idx, "initials"] != ""
                         and re.search(r'\d+', line_complete)))
                and not (df_clean.at[idx, "split"] == 0 and df_clean.at[idx, "index"] == "A5")):
            df_clean.at[idx, "IT_2"] = 1
            if df_clean.at[idx, "split"] == 1 and nxt is not None and df_clean.at[nxt, "split"] == 2:
                df_clean.at[nxt, "IT_2"] = 1
            continue

        # Certain firms
        if (df_clean.at[idx, "firm_dummy"] == 1
                and not (df_clean.at[idx, "occ_reg"] != ""
                         and df_clean.at[idx, "initials"] != ""
                         and df_clean.at[idx, "last_name"] != "")):
            df_clean.at[idx, "certain_firms"] = 1
            if df_clean.at[idx, "split"] == 1 and nxt is not None and df_clean.at[nxt, "split"] == 2:
                df_clean.at[nxt, "certain_firms"] = 1
            continue

        # Certain noises
        if (df_clean.at[idx, "index"] == "A5" and df_clean.at[idx, "split"] == 0
                and df_clean.at[idx, "firm_dummy"] == 0
                and df_clean["line"].duplicated(keep=False).loc[idx]):
            first_word = line.split()[0] if line else ""
            first_word_comma = line.split(",")[0] if "," in line else ""
            if not (first_word in CITIES_PAR or first_word_comma in CITIES_PAR):
                if len(re.findall(r'\w+', line)) in [1, 2] and re.search(r'\d+', line):
                    df_clean.at[idx, "certain_noise_B"] = 1
                else:
                    df_clean.at[idx, "certain_noise_A"] = 1
                continue

    # Populate the certain dictionary
    certain["certain_estate_complete"] = df_clean[(df_clean["certain_estate"] == 1) & (df_clean["split"] == 3)]
    certain["certain_estate_FH_SH"] = df_clean[(df_clean["certain_estate"] == 1) & (df_clean["split"].isin([1, 2]))]
    certain["certain_estate_noise"] = df_clean[(df_clean["certain_estate"] == 1) & (df_clean["split"] == 0)]
    certain["df_A1"] = df_clean[df_clean["certain_ind_A1_complete"] == 1]
    certain["df_IT_1_complete"] = df_clean[(df_clean["IT_1"] == 1) & (df_clean["split"] == 3)]
    certain["df_IT_1_noise"] = df_clean[(df_clean["IT_1"] == 1) & (df_clean["split"] == 0)]
    certain["df_IT_1_FH_SH"] = df_clean[(df_clean["IT_1"] == 1) & (df_clean["split"].isin([1, 2]))]
    certain["df_IT_2_complete"] = df_clean[(df_clean["IT_2"] == 1) & (df_clean["split"] == 3)]
    certain["df_IT_2_noise"] = df_clean[(df_clean["IT_2"] == 1) & (df_clean["split"] == 0)]
    certain["df_IT_2_FH_SH"] = df_clean[(df_clean["IT_2"] == 1) & (df_clean["split"].isin([1, 2]))]
    certain["df_CF_complete"] = df_clean[(df_clean["certain_firms"] == 1) & (df_clean["split"] == 3)]
    certain["df_CF_noise"] = df_clean[(df_clean["certain_firms"] == 1) & (df_clean["split"] == 0)]
    certain["df_CF_FH_SH"] = df_clean[(df_clean["certain_firms"] == 1) & (df_clean["split"].isin([1, 2]))]
    certain["df_certain_noise_A"] = df_clean[df_clean["certain_noise_A"] == 1]
    certain["df_certain_noise_B"] = df_clean[df_clean["certain_noise_B"] == 1]

    return df, certain


# ── Potential second lines ──────────────────────────────────────────────

def potential_sec_lines(df, certain):
    for col in ["potential_sec_line_A", "potential_sec_line_B",
                "potential_sec_line_C", "potential_sec_line_D"]:
        df[col] = 0

    all_certain_lines = pd.concat(
        [v["unique_key"] for v in certain.values() if isinstance(v, pd.DataFrame) and "unique_key" in v],
        ignore_index=True)
    df_filt = df[~df["unique_key"].isin(all_certain_lines)].copy()

    # A – only integers
    def first_pot(row):
        if not re.search(r'[A-Za-z]', row["line"]) and row["split"] in [0, 2]:
            row["potential_sec_line_A"] = 1
        return row
    df_filt = df_filt.apply(first_pot, axis=1)
    df.update(df_filt)
    certain["potential_sec_line_A"] = df[df["potential_sec_line_A"] == 1]

    # B – occ + integer
    all_certain_lines = pd.concat(
        [v["unique_key"] for v in certain.values() if isinstance(v, pd.DataFrame) and "unique_key" in v],
        ignore_index=True)
    df_filt = df[~df["unique_key"].isin(all_certain_lines)].copy()

    def sec_pot(row):
        if row["split"] in [0, 2] and row["occ_reg"] != "" and re.search(r'\d+', row["line"]):
            row["potential_sec_line_B"] = 1
        return row
    df_filt = df_filt.apply(sec_pot, axis=1)
    df.update(df_filt)
    certain["potential_sec_line_B"] = df[df["potential_sec_line_B"] == 1]

    # C – municipality + integer
    all_certain_lines = pd.concat(
        [v["unique_key"] for v in certain.values() if isinstance(v, pd.DataFrame) and "unique_key" in v],
        ignore_index=True)
    df_filt = df[~df["unique_key"].isin(all_certain_lines)].copy()

    def third_pot(row):
        line__ = row["line"]
        if (row["split"] in [0, 2, 3] and row["firm_dummy"] == 0
                and not re.search(r'inv\.\)', line__)
                and (line__.split()[0] in CITIES_PAR or line__.split(",")[0] in CITIES_PAR)
                and re.search(r'\d+', line__)
                and any(int(n) > 1000 for n in re.findall(r'\d+', line__))):
            row["potential_sec_line_C"] = 1
        return row
    df_filt = df_filt.apply(third_pot, axis=1)
    df.update(df_filt)
    certain["potential_sec_line_C"] = df[df["potential_sec_line_C"] == 1]

    # D – letters + integers > 1000
    all_certain_lines = pd.concat(
        [v["unique_key"] for v in certain.values() if isinstance(v, pd.DataFrame) and "unique_key" in v],
        ignore_index=True)
    df_filt = df[~df["unique_key"].isin(all_certain_lines)].copy()

    def fourth_pot(row):
        line__ = row["line"]
        if (row["split"] in [0, 2, 3]
                and re.search(r'[A-Za-z]', line__)
                and any(n > 1000 for n in [int(x) for x in re.findall(r'\d+', line__)])):
            row["potential_sec_line_D"] = 1
        return row
    df_filt = df_filt.apply(fourth_pot, axis=1)
    df.update(df_filt)
    certain["potential_sec_line_D"] = df[df["potential_sec_line_D"] == 1]

    return df, certain


# ── Adjust second lines ────────────────────────────────────────────────

def adj_sec_lines(df, certain):
    potential_sec_lines_df = pd.concat([
        certain["potential_sec_line_C"],
        certain["potential_sec_line_A"],
        certain["potential_sec_line_B"],
        certain["potential_sec_line_D"],
    ], axis=0)
    idx_list_ = df.index.to_list()

    for pos, idx in enumerate(idx_list_):
        if df.at[idx, "unique_key"] in potential_sec_lines_df["unique_key"].values:
            prv = idx_list_[pos - 1]
            line_prev = df.at[prv, "line"]
            split_prec = df.at[prv, "split"]
            line = df.at[idx, "line"]
            split_act = df.at[idx, "split"]
            if (split_act != 2
                    and (split_prec == 0
                         or (split_prec == 3 and not re.search(r'[A-Za-z]', line)))
                    and (line_prev[0].isupper() or line_prev.startswith("von ")
                         or line_prev.startswith("de ") or line_prev.startswith("af. ")
                         or line_prev.startswith("af "))
                    and (re.search(INITIALS_PATTERN, line_prev)
                         or df.at[prv, "firm_dummy"] == 1 or df.at[prv, "initials"] != "")
                    and len(line_prev) > 10 and re.search(r'[a-z]', line_prev)
                    and df.at[prv, "unique_key"] not in certain["certain_Tel_int"]["unique_key"].values):
                df.at[prv, "split"] = 1
                df.at[idx, "split"] = 2
    return df


# ── Potential first lines ───────────────────────────────────────────────

def potential_FH(df, surname_list, certain):
    df = df.sort_values(by=["page", "column", "row"])
    index_list_ = df.index.to_list()
    df["pot_first_line"] = 0

    for pos, idx in enumerate(index_list_):
        line = str(df.at[idx, "line"])
        split = df.at[idx, "split"]

        next_line_series = surname_list.loc[
            (surname_list["page"] == df.at[idx, "page"])
            & (surname_list["column"] == df.at[idx, "column"])
            & (surname_list["row"] == int(df.at[idx, "row"]) + 1),
            "line"
        ]
        if next_line_series.empty:
            continue
        next_line = str(next_line_series.iloc[0])

        if ((line[0].isupper() or re.match(r"(von\s|de\s)", line))
                and re.search(r'[A-Za-z]', line)
                and (re.search(INITIALS_PATTERN, line) or re.search(FIRM_PATTERN, line))
                and any(int(n) > 1000 for n in re.findall(r'\d+', next_line))
                and line[0:2] != next_line[0:2]
                and split not in [2] and len(line) > 8):
            df.at[idx, "pot_first_line"] = 1

    certain["pot_first_line"] = df[df["pot_first_line"] == 1]
    df = df[df["pot_first_line"] == 0]
    return df


def _adj_pot_FH(df, certain, surname_list):
    df["pot_first_line"] = df["unique_key"].isin(
        certain["pot_first_line"]["unique_key"]).astype(int)
    index_list_ = df.index.to_list()
    for pos, idx in enumerate(index_list_):
        line = df.at[idx, "line"]
        split = df.at[idx, "split"]
        if pos + 1 < len(df):
            nxt = index_list_[pos + 1]
            next_line = df.at[nxt, "line"]
        if df.at[idx, "pot_first_line"] == 1 and df.at[idx, "split"] not in [1]:
            if (df.at[nxt, "split"] not in [1, 2]
                    and re.search(r'\d+', next_line)
                    and line[0:2] != next_line[0:2]
                    and any(n > 1000 for n in [int(x) for x in re.findall(r'\d+', next_line)])
                    and ((split == 3 and not re.search(r'[A-Za-z]', next_line)) or split == 0)
                    and df.at[nxt, "index"] != "A1"):
                df.at[idx, "split"] = 1
                df.at[nxt, "split"] = 2
    return df


# ── Adjust telephone split lines ────────────────────────────────────────

def adj_tell_split(row, certain):
    if row["split"] == 3 and row["unique_key"] in certain["certain_Tel_int"]["unique_key"].values:
        row["split"] = 0
    return row


# ── Take out fake second lines ──────────────────────────────────────────

def take_out_fake_sec_lines(df):
    idx_list = df.index.to_list()
    for pos, idx in enumerate(idx_list):
        split_ = df.at[idx, "split"]
        if split_ in [0, 1, 3]:
            continue
        last_name_ = df.at[idx, "last_name"]
        line = df.at[idx, "line"]
        if (split_ == 2 and df.at[idx, "initials"] != "" and last_name_
                and df.at[idx, "occ_reg"] != "" and df.at[idx, "parish"] != ""
                and df.at[idx, "line"].startswith(last_name_)
                and (not last_name_[0].islower()
                     or last_name_[0:3] in ["von", "af ", "de "])):
            prv = idx_list[pos - 1]
            df.at[prv, "split"] = 0
            df.at[idx, "split"] = 0
            nxt = idx_list[pos + 1]
            next_line = df.at[nxt, "line"]
            if re.search(r'-\s*\d+', line) and re.search(r'[A-Za-z]', next_line):
                df.at[idx, "split"] = 3
    return df


# ── Spellout helpers ────────────────────────────────────────────────────

def df_FH_SH_FUNCT(df):
    """Split a certain-category frame into (all, no-A5, A5-only)."""
    idx_list = df.index.to_list()
    df["idx_true"] = df["index"]
    for pos, idx in enumerate(idx_list):
        split_act = df.at[idx, "split"]
        if pos + 1 < len(df):
            nxt = idx_list[pos + 1]
            split_nxt = df.at[nxt, "split"]
        if split_act == 2:
            continue
        if split_act == 1 and split_nxt == 2:
            df.at[nxt, "idx_true"] = df.at[idx, "idx_true"]
        continue
    df_no_A5 = df[df["idx_true"] != "A5"]
    df_A5 = df[df["idx_true"] == "A5"]
    df = df.drop(columns={"idx_true"})
    df_A5 = df_A5.drop(columns={"idx_true"})
    df_no_A5 = df_no_A5.drop(columns={"idx_true"})
    return df, df_no_A5, df_A5
