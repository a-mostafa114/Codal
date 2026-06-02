# -*- coding: utf-8 -*-
"""
data_loader.py – Load all input data files and build the initial dataframe.

Functions
---------
load_dirty_last_names(path)        – Excel file with last-name corrections.
load_death_register(path)          – Updated death-register CSV.
load_first_names(path)             – Stata file with burial first names.
load_main_dataframe(path)          – Main OCR CSV (``ocr_input.csv``).
build_surname_list(main_df)        – Construct the initial ``surname_list`` frame.
fill_em_dash_lastnames(df)         – Pre-fill surnames into em-dash continuation lines.
load_occupation_list(path)         – Occupation reference CSV.
"""

import os
import re
import pandas as pd
import pyreadstat

from .utils import remove_accents


# ── Em-dash continuation helpers ────────────────────────────────────────────

# Words that start an A1 (NO_OCC) line — never fill these with a surname.
# Compared after remove_accents, so Swedish diacritics already stripped.
_NO_OCC_FILL = frozenset(["hustru", "fru", "froken", "ankefru"])


def _strip_dash_prefix(line):
    """Strip a leading em-dash / double-dash continuation prefix.

    Returns ``(stripped_line, had_dash)``.
    Handles: ``—``, ``--``, ``- -``, single ``-`` before initials, and their
    ``*:s`` possessive variants.
    """
    # em-dash possessive (—:s, …)
    m = re.match(r'^—:s,?\s*', line)
    if m:
        return line[m.end():], True
    # em-dash (—)
    m = re.match(r'^—\s*', line)
    if m:
        return line[m.end():], True
    # double-hyphen possessive (--:s, …)
    m = re.match(r'^--:s,?\s*', line)
    if m:
        return line[m.end():], True
    # double-hyphen (--)
    m = re.match(r'^--\s*', line)
    if m:
        return line[m.end():], True
    # spaced double-hyphen (- -) OCR variant — only if followed by initials
    m = re.match(r'^-\s+-\s*(?=[A-ZÅÄÖ][a-z]?\.[A-ZÅÄÖ])', line)
    if m:
        return line[m.end():], True
    # single hyphen immediately before initials (e.g. -O.V., not -Adling)
    m = re.match(r'^-(?=[A-ZÅÄÖ][a-z]?\.[A-ZÅÄÖ])', line)
    if m:
        return line[m.end():], True
    return line, False


def _looks_like_missed_dash(line):
    """True when a line is a continuation entry whose leading dash was dropped by OCR.

    Pattern: first comma-separated token consists entirely of initials
    (e.g. ``H.J.C.``, ``A.M.``) — no word-like component, no sentence structure.
    At least 3 comma-separated segments are required.
    """
    if not line or not line[0].isupper():
        return False
    parts = line.split(',')
    if len(parts) < 3:
        return False
    first = parts[0].strip()
    # Must start with Capital.Capital (two-initial pattern minimum)
    if not re.match(r'^[A-ZÅÄÖ][a-z]?\.[A-ZÅÄÖ]', first):
        return False
    # No word-like component: no letter followed by 2+ lowercase letters
    if re.search(r'[A-Za-z][a-z]{2,}', first.replace('.', '')):
        return False
    return True


def _extract_surname_from_line(line):
    """Return the surname (first comma-token) from a full-name line, or ''."""
    # Strip ALL leading dashes and spaces (handles '- -Adling' → 'Adling')
    stripped = re.sub(r'^[-— ]+', '', line.strip())
    if not stripped:
        return ''
    first = stripped.split(',')[0].strip()
    if not first or first.endswith('.'):
        return ''
    if first[0].islower():
        return ''
    if any(first.lower().startswith(w) for w in _NO_OCC_FILL):
        return ''
    # All-uppercase → advertisement header, not a surname
    if first.isupper() and len(first) > 3:
        return ''
    if re.search(r'\d', first):
        return ''
    if len(first.split()) > 4:
        return ''
    return first


def fill_em_dash_lastnames(df):
    """Pre-fill surnames into em-dash continuation lines (1920/1921-style books).

    Activates when >5 % of lines start with ``—`` or ``--``.  For each
    continuation line the most recently seen surname (within global reading
    order: page → column → row) is prepended so the normal last-name matching
    pipeline (A1-A5) can handle the result unchanged.

    ``hustru`` / ``fru`` / ``fröken`` / ``änkefru`` lines are never modified.
    """
    lines = df['line'].astype(str)
    n_em = (lines.str.startswith('—') | lines.str.startswith('--')).sum()
    if n_em / max(len(df), 1) < 0.05:
        return df

    df = df.sort_values(['page', 'column', 'row']).reset_index(drop=True)
    raw = df['line'].tolist()
    new_lines = list(raw)
    last_surname = ''

    for pos in range(len(raw)):
        line = str(raw[pos])
        stripped, had_dash = _strip_dash_prefix(line)

        # A1-type lines — leave untouched
        if any(stripped.lower().startswith(w) for w in _NO_OCC_FILL):
            continue

        is_continuation = had_dash or _looks_like_missed_dash(stripped)

        if is_continuation:
            if last_surname:
                new_lines[pos] = last_surname + ', ' + stripped
        else:
            # Strip leading OCR-artifact dashes from proper entry lines
            # (e.g. '- -Adling, C. A., ...' → 'Adling, C. A., ...')
            clean = re.sub(r'^[-— ]+', '', line).strip()
            if clean and clean != line:
                new_lines[pos] = clean
            s = _extract_surname_from_line(clean or line)
            if s:
                last_surname = s

    df = df.copy()
    df['line'] = new_lines
    return df


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
    base_cols = ["page", "column", "row", "line"]
    if "source" in main_df.columns:
        base_cols.append("source")
    surname_list = main_df[base_cols].copy()
    if "source" not in surname_list.columns:
        surname_list["source"] = ""
    surname_list["line"] = surname_list["line"].apply(remove_accents)
    surname_list["matched"] = False
    surname_list = surname_list[surname_list["line"].apply(lambda x: isinstance(x, str))]
    surname_list["last_name"] = ""

    # Adjust "0, " → "O., "
    clean_O = surname_list[surname_list["line"].str.contains(r'\b0,\s', regex=True)].copy()
    clean_O["line"] = clean_O["line"].apply(lambda x: re.sub(r'0,\s', r'O., ', x))
    surname_list.update(clean_O)

    # Forward-fill surnames into em-dash continuation lines (1920/1921 books)
    surname_list = fill_em_dash_lastnames(surname_list)

    return surname_list


# ── Occupation list ─────────────────────────────────────────────────────
def load_occupation_list(path="occ_list_for_alg.csv"):
    occ_list = pd.read_csv(path, index_col=0)
    occ_list = occ_list[["occ_llm"]]
    occ_list["occ_llm"] = remove_accents(occ_list["occ_llm"])
    occ_list = occ_list.sort_values(by="occ_llm", key=lambda x: x.str.len(), ascending=False)
    return occ_list
