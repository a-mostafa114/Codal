# -*- coding: utf-8 -*-
"""
utils.py – Shared utility functions used across multiple modules.
"""

import re
import unicodedata
import regex as regex_mod
import pandas as pd
from rapidfuzz import process, fuzz


def remove_accents(s):
    """Remove diacritical marks (accents) from a string."""
    if isinstance(s, str):
        return ''.join(
            c for c in unicodedata.normalize('NFD', s)
            if unicodedata.category(c) != 'Mn'
        )
    return s


def fuzzy_match_rapidfuzz(x, choices):
    """Fuzzy-match *x* against *choices* using token-sort ratio.

    Returns ``(best_match, score, index)`` or ``(None, 0, None)`` on failure.
    """
    try:
        match = process.extractOne(x, choices, scorer=fuzz.token_sort_ratio)
        if match is None:
            return (None, 0, None)
        return match
    except Exception:
        return (None, 0, None)


def clean_edges(txt: str) -> str:
    """Strip leading/trailing punctuation and whitespace (Unicode-aware)."""
    return regex_mod.sub(r'^[\p{P}\p{Zs}]+|[\p{P}\p{Zs}]+$', '', txt)


def complete_first_word(partial, full_line):
    """Return the first complete word that starts with *partial* in *full_line*."""
    match = re.search(r'\b' + re.escape(partial) + r'[^\s,\.]*', full_line)
    if match:
        return match.group()
    return partial
