# -*- coding: utf-8 -*-
"""
book_profile.py – Per-book layout/era awareness.

The Taxeringskalender changes physical layout over the decades, so a single
parsing flowchart cannot fit every year. Valerio handled this by maintaining
separate codebases per era; here we make the pipeline *book-aware* instead:
each run resolves a :class:`BookProfile` (year + layout + extraction strategy),
and the steps branch on it.

Layout (the structural delimiters the parser relies on):

- ``COMMA_DASH``      surname/occupation separated by commas, the income pair
                      written ``X—Y``. Covers ~1911–1959. This is the layout
                      the current pipeline was built for.
- ``DASH_NO_COMMA``   income still ``X—Y`` but fields no longer comma-delimited.
                      ~1960–1969 (Valerio confirmed on 1961).
- ``NO_DASH_NO_COMMA`` neither field commas nor a dash income separator.
                      1970s (Valerio confirmed on 1974).

``ln_strategy`` is the finer extraction regime within a layout (e.g. the
1911–1917 "Step A" surname logic vs the 1918+ "Step 0 / new format" logic),
so the flowchart can differ by *year* even when the layout is identical.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Layout(str, Enum):
    COMMA_DASH = "comma_dash"
    DASH_NO_COMMA = "dash_no_comma"
    NO_DASH_NO_COMMA = "no_dash_no_comma"


@dataclass(frozen=True)
class BookProfile:
    year: int
    layout: Layout
    ln_strategy: str          # 'step_a' | 'step_0' | 'dash_no_comma' | 'no_dash_no_comma'
    note: str = ""

    @property
    def has_field_commas(self) -> bool:
        return self.layout == Layout.COMMA_DASH

    @property
    def has_dash_income(self) -> bool:
        return self.layout in (Layout.COMMA_DASH, Layout.DASH_NO_COMMA)

    def __str__(self) -> str:
        return (f"BookProfile(year={self.year}, layout={self.layout.value}, "
                f"ln_strategy={self.ln_strategy})")


# Era boundaries. Only a few years were ground-truthed by Valerio (1961, 1974);
# the rest are inferred and should be confirmed by detect_layout() when an OCR
# frame is available. Keep boundaries here so they are easy to correct.
def profile_for_year(year: int) -> BookProfile:
    """Resolve the best-known profile for a publication year."""
    y = int(year)
    if y <= 1917:
        return BookProfile(y, Layout.COMMA_DASH, "step_a",
                           "1911–1917 Step-A surname extraction")
    if y <= 1959:
        return BookProfile(y, Layout.COMMA_DASH, "step_0",
                           "1918–1959 new-format (comma+dash)")
    if y <= 1969:
        return BookProfile(y, Layout.DASH_NO_COMMA, "dash_no_comma",
                           "~1960s dash income, no field commas")
    return BookProfile(y, Layout.NO_DASH_NO_COMMA, "no_dash_no_comma",
                       "1970s no dash, no comma")


def detect_layout(df, sample: int = 4000) -> Layout:
    """Sniff the structural layout from a raw OCR frame's ``line`` column.

    Used to validate (or override) the year-based prior, since the era
    boundaries are fuzzy and Valerio warns the layout must be checked per book.
    Looks at how often person-like lines carry a field comma and a dash income.
    """
    import re

    lines = (df["line"].dropna().astype(str))
    lines = lines[lines.str.contains(r"\d", na=False)]
    if len(lines) == 0:
        return Layout.COMMA_DASH
    if len(lines) > sample:
        lines = lines.sample(sample, random_state=0)

    dash_income = lines.str.contains(r"\d\s*[-—–]\s*\d", regex=True, na=False).mean()
    field_comma = lines.str.contains(r"[A-Za-z],\s", regex=True, na=False).mean()

    if field_comma >= 0.30:
        return Layout.COMMA_DASH
    if dash_income >= 0.30:
        return Layout.DASH_NO_COMMA
    return Layout.NO_DASH_NO_COMMA


def resolve_profile(year, df=None) -> BookProfile:
    """Profile from year prior, validated against a sniff of ``df`` if given.

    On disagreement the *detected* layout wins (the book is what it is), but we
    keep the year's ln_strategy and surface the mismatch to the caller's log.
    """
    prof = profile_for_year(year)
    if df is None or "line" not in getattr(df, "columns", []):
        return prof
    detected = detect_layout(df)
    if detected != prof.layout:
        print(f"    [book_profile] year {prof.year} prior layout "
              f"{prof.layout.value!r} but OCR looks {detected.value!r}; "
              f"using detected.")
        return BookProfile(prof.year, detected, prof.ln_strategy,
                           prof.note + " [layout overridden by detection]")
    return prof
