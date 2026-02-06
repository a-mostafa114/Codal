# -*- coding: utf-8 -*-
"""
data_loader.py – Load all input data files and build the initial dataframe.

Functions
---------
load_dirty_last_names(path)   – Excel file with last-name corrections.
load_death_register(path)     – Updated death-register CSV.
load_first_names(path)        – Stata file with burial first names.
load_main_dataframe(path)     – Main OCR CSV (``ocr_input.csv``).
build_surname_list(main_df)   – Construct the initial ``surname_list`` frame.
load_occupation_list(path)    – Occupation reference CSV.
"""

import os
import pandas as pd
import pyreadstat

from .utils import remove_accents


# ── Dirty last names ────────────────────────────────────────────────────
def load_dirty_last_names(path="Last_names_to_update_DR.xlsx"):
    df = pd.read_excel(path, sheet_name="Sheet2")
    df["last_name"] = df["line"].str.split(",", n=1).str[0].str.strip()
    df = df[["last_name", "last_name_clean"]].drop_duplicates(subset="last_name")
    dirty_dict = df.set_index("last_name")["last_name_clean"].to_dict()
    return df, dirty_dict


# ── Death register ──────────────────────────────────────────────────────
def load_death_register(path="Updated_DR.csv"):
    df = pd.read_csv(path)
    df = df.sort_values(by="last_name", key=lambda x: x.str.len(), ascending=False)
    df = df[~df["last_name"].str.contains("hustru")]
    return df


# ── First names ─────────────────────────────────────────────────────────
def load_first_names(path="Burial_names.dta"):
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Missing required file: {path}. "
            "Place Burial_names.dta in the repo root (or pass a custom path)."
        )
    first_names, _meta = pyreadstat.read_dta(path)
    first_names = first_names["firstname"].drop_duplicates()
    replacements = {
        "ö": "o", "ä": "a", "à": "a", "å": "a",
        "Ö": "O", "Ä": "A", "Å": "A", "Ü": "U",
    }
    for old, new in replacements.items():
        first_names = first_names.str.replace(old, new)
    return first_names


# ── Main dataframe ──────────────────────────────────────────────────────
def load_main_dataframe(path="ocr_input.csv"):
    return pd.read_csv(path)


# ── Build the initial surname_list ──────────────────────────────────────
def build_surname_list(main_df):
    """Create the working ``surname_list`` dataframe from the raw import."""
    import re

    main_df["no_occ"] = 0
    surname_list = main_df[["page", "column", "row", "line"]].copy()
    surname_list["line"] = surname_list["line"].apply(remove_accents)
    surname_list["matched"] = False
    surname_list = surname_list[surname_list["line"].apply(lambda x: isinstance(x, str))]
    surname_list["last_name"] = ""

    # Adjust "0, " → "O., "
    clean_O = surname_list[surname_list["line"].str.contains(r'\b0,\s', regex=True)].copy()
    clean_O["line"] = clean_O["line"].apply(lambda x: re.sub(r'0,\s', r'O., ', x))
    surname_list.update(clean_O)

    return surname_list


# ── Occupation list ─────────────────────────────────────────────────────
def load_occupation_list(path="occ_list_for_alg.csv"):
    occ_list = pd.read_csv(path, index_col=0)
    occ_list = occ_list[["occ_llm"]]
    occ_list["occ_llm"] = remove_accents(occ_list["occ_llm"])
    occ_list = occ_list.sort_values(by="occ_llm", key=lambda x: x.str.len(), ascending=False)
    return occ_list
