# -*- coding: utf-8 -*-
"""
location.py – Location finding and municipality assignment.
"""

import glob
import json
import os
import re
import unicodedata
from functools import lru_cache

import pandas as pd

try:
    from rapidfuzz import fuzz, process as _rf_process
    _HAS_RF = True
except Exception:  # pragma: no cover
    _HAS_RF = False
    import difflib


# ── Gazetteer gate (reject garbage headers like "Pyro", ad slogans) ──────

_GAZETTEER_PATH = os.path.join(os.path.dirname(__file__), "known_locations.csv")


@lru_cache(maxsize=1)
def _load_gazetteer():
    """Known real municipalities/counties (ASCII, lowercased)."""
    try:
        col = pd.read_csv(_GAZETTEER_PATH)["location"].dropna().astype(str)
        return tuple(sorted({s.strip().lower() for s in col if s.strip()}))
    except Exception:
        return tuple()


def _is_known_location(candidate, threshold=87):
    """True if ``candidate`` exactly or fuzzily matches a known real location.

    Tolerates OCR noise (e.g. "Vasterbottens. lan", "Stockkolm") so real
    headers survive, while one-off garbage ("Pyro", "Alltid nyheter") is
    rejected. Fails open if the gazetteer file is missing.
    """
    c = str(candidate).strip().lower()
    if not c:
        return False
    gaz = _load_gazetteer()
    if not gaz:
        return True
    if c in gaz:
        return True
    if _HAS_RF:
        m = _rf_process.extractOne(c, gaz, scorer=fuzz.ratio)
        return m is not None and m[1] >= threshold
    return bool(difflib.get_close_matches(c, gaz, n=1, cutoff=threshold / 100.0))


# ── Find locations (inv.) ───────────────────────────────────────────────

def find_locations(df):
    """Mark location markers in the DataFrame, setting a ``location`` column.

    Pattern 1 – lines containing ``inv.)`` (classic city/county headers).
    Pattern 2 – source=``title`` lines that look like bare city/county names,
                restricted to pages >= the first Pattern-1 page so that
                advertisement titles on early pages are never picked up.
    """
    df = df.copy()
    df["location"] = ""
    lines = df["line"].astype(str)

    # ── Pattern 1: classic "City (N,NNN inv.)" form ──────────────────────
    def _city_from_inv(line):
        city = ""
        for ch in line:
            if ch in "()":
                break
            city += ch
        city = re.sub(r'\d+', "", city)
        city = re.sub(r'inv\.', "", city)
        city = re.sub(r',', "", city)
        return city.strip()

    mask_inv = lines.str.contains(r'inv\.?\)', regex=True, na=False)
    df.loc[mask_inv, "location"] = lines[mask_inv].apply(_city_from_inv)

    # Pattern 1 fallback: when the city name was on the previous row
    # (e.g. "Goteborg" on row N, "(170,173 inv.)" on row N+1).
    # Look up previous row for inv.) entries that got an empty city.
    empty_inv = mask_inv & (df["location"] == "")
    if empty_inv.any():
        page_col_row = df[["page", "column", "row"]].copy()
        for idx in df.index[empty_inv]:
            p, c, r = df.at[idx, "page"], df.at[idx, "column"], df.at[idx, "row"]
            prev = df[(df["page"] == p) & (df["column"] == c) & (df["row"] == r - 1)]
            if not prev.empty:
                candidate = str(prev.iloc[0]["line"]).strip()
                if (candidate
                        and candidate[0].isupper()
                        and not re.search(r'[\d,\(\)]', candidate)
                        and len(candidate.split()) <= 3):
                    df.at[idx, "location"] = candidate

    # ── Pattern 2: bare city/county title lines ───────────────────────────
    # Only active on pages >= the first inv.) page to avoid picking up
    # advertisement titles that appear earlier in the document.
    if mask_inv.any() and "source" in df.columns:
        first_inv_page = int(df.loc[mask_inv, "page"].min())

        def _city_from_title(line):
            stripped = line.strip()
            if not stripped or stripped.isupper():
                return ""
            # Starts with lowercase → sentence fragment or ad copy, not a location
            if stripped[0].islower():
                return ""
            # Any all-uppercase word longer than 3 chars → advertisement, not a location
            if any(
                w.isupper() and len(w) > 3
                for w in re.split(r'[\s—–\-]+', stripped)
                if w.isalpha()
            ):
                return ""
            # "CityA—CityB" or "CityA--CityB" divider → take the last segment
            if re.search(r'[—–]|--', stripped) and not re.search(r'\d', stripped):
                city = re.split(r'[—–]|--', stripped)[-1].strip()
            else:
                city = re.split(r'\s*\(', stripped)[0].strip()
            city = re.sub(r'[,\.]+$', '', city).strip()
            # Internal comma → redirect note (e.g. "Cimbrishamn, se Simrishamn")
            if ',' in city:
                return ""
            # Digits → book/price text, not a location
            if re.search(r'\d', city):
                return ""
            # " och " → ad copy; valid "och" names are caught by Pattern 1
            if ' och ' in city.lower():
                return ""
            # Trailing hyphen → broken wrapped word from advertisement
            if city.endswith('-'):
                return ""
            # Swedish street suffix → address, not a municipality
            if re.search(r'gatan$', city, re.IGNORECASE):
                return ""
            # 1-2 words: bare city names ("Goteborg") or county names ("Stockholms lan")
            if city and len(city.split()) <= 2 and re.search(r'[A-Za-z]', city):
                return city
            return ""

        # row == 1 only: city section headers always start at the top of a column
        mask_title = (
            (df["source"] == "title")
            & (df["page"] >= first_inv_page)
            & (df["row"] == 1)
            & ~mask_inv
            & (df["location"] == "")
        )
        # Locations already confirmed by the reliable Pattern 1 (inv.) form.
        p1_confirmed = set(
            df.loc[df["location"].str.strip() != "", "location"].str.strip().str.lower()
        )

        def _gated_title(line):
            city = _city_from_title(line)
            if not city:
                return ""
            # Keep only if independently confirmed by an inv.) header or a known place.
            if city.strip().lower() in p1_confirmed or _is_known_location(city):
                return city
            return ""

        df.loc[mask_title, "location"] = lines[mask_title].apply(_gated_title)

    return df


# ── Build the location list ─────────────────────────────────────────────

def build_location_list(surname_list):
    """Build the reference ``location_list`` from extracted locations."""
    location_list = surname_list[surname_list["location"] != ""].copy()
    location_list = location_list[["page", "column", "row", "line", "line_complete", "split", "location"]]

    def resolve_location(x):
        loc = x["location"]
        # Empty or space → city name was on the previous row (e.g. "(170,173 inv.)")
        if loc in ("", " "):
            try:
                prev = surname_list.loc[
                    (surname_list["page"] == x["page"])
                    & (surname_list["column"] == x["column"])
                    & (surname_list["row"] == int(x["row"]) - 1),
                    "line",
                ]
                candidate = str(prev.values[0]).strip()
                # Accept only if it looks like a city name: short, starts uppercase,
                # no digits, no commas
                if (candidate
                        and candidate[0].isupper()
                        and not re.search(r'[\d,]', candidate)
                        and len(candidate.split()) <= 3):
                    return candidate
            except (IndexError, ValueError):
                pass
        return loc

    location_list["location"] = location_list.apply(resolve_location, axis=1)
    # Coerce to string: resolve_location can yield NaN/float, and an all-empty
    # result infers float dtype, both of which break the .str accessor below.
    location_list["location"] = location_list["location"].fillna("").astype(str)
    # Drop entries that still have empty location (inv.) line with no usable city name)
    location_list = location_list[location_list["location"].str.strip() != ""]

    header = pd.DataFrame({
        "page": [0], "column": [0], "row": [0],
        "line": ["Stockholm"], "line_complete": ["Stockholm"],
        "split": [0], "location": ["Stockholm"],
    })
    location_list = pd.concat([header, location_list], axis=0)
    return location_list


# ── Assign municipality ─────────────────────────────────────────────────

def extract_location(df, location_list):
    """Assign a ``municipality`` to every row using binary search on location markers."""
    import numpy as np

    df = df.copy()

    loc = location_list[
        location_list["location"].notna() & (location_list["location"] != "")
    ].sort_values(["page", "column", "row"])

    # Composite key respecting physical reading order: page → column → row.
    # Rows are numbered per-column (each column restarts at 1), so the key must
    # include column; otherwise a header at the top of column 2 wrongly captures
    # the tail of column 1 (the boundary-bleed bug).
    def _reading_key(frame):
        return (
            (frame["page"].astype(int) * 1_000 + frame["column"].astype(int))
            * 100_000
            + frame["row"].astype(int)
        ).values

    loc_keys = _reading_key(loc)
    loc_vals = loc["location"].tolist()

    df_keys = _reading_key(df)

    # For each data row find the last location marker at or before (page, row)
    idx = np.searchsorted(loc_keys, df_keys, side="right") - 1
    idx = np.clip(idx, 0, len(loc_vals) - 1)
    df["municipality"] = [loc_vals[i] for i in idx]

    return df


# ── Limit-case municipality ─────────────────────────────────────────────

def location_limit_case(df):
    """Fill municipality for rows that still have none."""
    idx_list = df.index.to_list()
    for pos, idx in enumerate(idx_list):
        line = df.at[idx, "line"]
        prv = idx_list[pos - 1]
        next_mun = ""
        if pos + 3 < len(df):
            nxt = idx_list[pos + 3]
            next_mun = df.at[nxt, "municipality"]
        prev_mun = df.at[prv, "municipality"]
        if df.at[idx, "location"] == "" and df.at[idx, "municipality"] == "":
            if re.search(r'[A-G]', line[0]):
                df.at[idx, "municipality"] = next_mun
                continue
            if re.search(r'[O-Z]', line[0]):
                df.at[idx, "municipality"] = prev_mun
    return df


# ── Municipality normalization ──────────────────────────────────────────

def normalize_municipality(df):
    """Unify municipality spelling: trim/collapse whitespace, capitalize.

    OCR renders some section headers in lowercase, splitting one
    municipality into two spellings ('Ostergotlands lan' 13 rows /
    'ostergotlands lan' 654 rows in the 1913 book) that downstream
    matching treats as different places.
    """
    mun = df["municipality"].astype(str)
    mun = mun.str.strip().str.replace(r"\s+", " ", regex=True)
    nonempty = mun != ""
    mun.loc[nonempty] = mun[nonempty].str[0].str.upper() + mun[nonempty].str[1:]
    df["municipality"] = mun
    return df


# ── MinerU header-based municipality detection ──────────────────────────────

def _remove_accents_simple(s):
    """Remove diacritical marks using NFD decomposition."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
    )


def load_mineru_headers(mineru_dir):
    """Read the first ``type=header`` element from each MinerU JSON file.

    Returns ``{page_num: [mun1]}`` for single-municipality pages and
    ``{page_num: [mun1, mun2]}`` for range pages (e.g. "Gävleborgs—Göteborgs").
    Municipality names are accent-stripped for consistency with the pipeline.
    """
    def _parse_header(content):
        # Apply accent stripping first so downstream fixes work on ASCII
        content = _remove_accents_simple(content)
        # Fix OCR artifact: capital-I mistaken for lowercase-l (e.g. "Ian" was "Iän")
        content = re.sub(r'\bIan\b', 'lan', content)
        # Split on em-dash, en-dash, or double-hyphen for range headers
        if re.search(r'[—–]|--', content):
            parts = re.split(r'\s*[—–]\s*|\s*--\s*', content)
        else:
            parts = [content]
        result = []
        for part in parts:
            name = re.sub(r'[,\.]+$', '', part.strip()).strip()
            if name:
                result.append(name)
        return result

    headers = {}
    pattern = os.path.join(mineru_dir, '*_extracted.json')
    for filepath in sorted(glob.glob(pattern)):
        m = re.search(r'_(\d+)_extracted', os.path.basename(filepath))
        if not m:
            continue
        page_num = int(m.group(1))
        try:
            with open(filepath, encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue
        for entry in data:
            if entry.get('type') == 'header':
                content = entry.get('content', '').strip()
                if content:
                    names = _parse_header(content)
                    if names:
                        headers[page_num] = names
                break  # only first header per page
    return headers


def _resolve_range_first_mun(page_num, first_mun, all_headers, lookback=10):
    """Normalize the first municipality of a range header using nearby pages.

    Range headers sometimes omit "lan" from the first part (e.g. "Jämtlands—
    Jönköpings lan" where the first part is just "Jämtlands"). The preceding
    single-municipality pages always have the full name ("Jämtlands lan").
    This function looks back up to ``lookback`` pages for a matching single-page
    header and returns the full name.
    """
    base = re.sub(r'\s+lan$', '', first_mun, flags=re.IGNORECASE).strip().lower()
    for pg in range(page_num - 1, max(0, page_num - lookback - 1), -1):
        if pg not in all_headers:
            continue
        prev = all_headers[pg]
        if len(prev) != 1:
            continue
        prev_name = prev[0]
        prev_base = re.sub(r'\s+lan$', '', prev_name, flags=re.IGNORECASE).strip().lower()
        if base == prev_base:
            return prev_name
    return first_mun


_NOBLE_PREFIX_RE = re.compile(
    r'^(af|von|v\.|de|van|ter|zu|du|le|la)\s+', re.IGNORECASE
)


def _alpha_transitions_by_col(df_page, threshold=8, pos_limit=2):
    """Detect the last section-boundary alphabet drop per column on a page.

    Processes rows in ascending row order within each column. Tracks a running
    maximum first letter. A drop of ``threshold`` or more positions is treated
    as a boundary only when it lands in the A–``pos_limit`` range, which
    distinguishes genuine municipality transitions (drop to A/B/C) from within-
    section noise (e.g. Å→Ö rendered as A→O after ``remove_accents``).

    Drops that land outside the accepted range are silently skipped without
    resetting the maximum, so that a later true drop (to A) can still be
    detected.

    Noble particles ("af", "von", "de", …) are stripped before the alphabetical
    comparison so "af Klintberg" sorts as K, not A — preventing false drops in
    the middle of a single-municipality K-section.

    Entries whose last_name contains digits are skipped (ad text such as
    "Allm Tel. 9269" that bleeds into the last_name column).

    Returns the LAST qualifying drop per column: ``{col: row}``.  The "last"
    semantics correctly handles county-level pages where Swedish Å/Ä/Ö names
    (A/O after accent-stripping) cause a spurious early A-drop before the real
    section boundary.
    """
    transitions = {}
    if 'last_name' not in df_page.columns:
        return transitions

    for col, group in df_page.groupby('column'):
        group = group.sort_values('row')
        current_max = -1
        last_drop_row = None

        for _, r in group.iterrows():
            name = str(r.get('last_name', '')).strip()
            if not name:
                continue
            # Skip ad-text entries that contain digits (e.g. "Allm Tel. 9269")
            if re.search(r'\d', name):
                continue
            # Strip noble particles so "af Klintberg" sorts as K, not A
            sort_name = _NOBLE_PREFIX_RE.sub('', name).strip()
            if not sort_name or not sort_name[0].isalpha():
                continue
            pos = ord(sort_name[0].upper()) - ord('A')  # 0–25
            if current_max < 0:
                current_max = pos
            elif current_max - pos >= threshold:
                if pos <= pos_limit:
                    # Real section boundary: drop lands in A–pos_limit
                    last_drop_row = int(r['row'])
                    current_max = pos
                # else: within-section noise; ignore and keep current_max
            elif pos > current_max:
                current_max = pos

        if last_drop_row is not None:
            transitions[int(col)] = last_drop_row

    return transitions


def _city_from_inv_line(line):
    """Extract city name from a raw inv.) line (e.g. 'Göteborg (170173 inv.)')."""
    city = ""
    for ch in line:
        if ch in "()":
            break
        city += ch
    city = re.sub(r'\d+', "", city)
    city = re.sub(r'inv\.', "", city, flags=re.IGNORECASE)
    city = re.sub(r',', "", city)
    return _remove_accents_simple(city.strip())


def load_inv_from_mineru_json(mineru_dir, df):
    """Read ALL MinerU JSON types for inv.) content missed by the pipeline.

    The standard pipeline filters to types=['text','ref_text','title'].
    Some inv.) city headers are emitted as 'table_caption' or other types
    and are silently dropped.  This function scans all types, keeps only
    entries whose content matches an inv.) pattern, and returns a DataFrame
    of synthetic location rows that can be merged into the base location_list.

    Column and row are estimated from the bbox and the actual row distribution
    already present in ``df`` for each (page, column) pair.
    """
    _INV_RE = re.compile(r'inv\.?\)', re.IGNORECASE)
    _PIPELINE_TYPES = {'text', 'ref_text', 'title'}

    rows = []
    pattern = os.path.join(mineru_dir, '*_extracted.json')
    for filepath in sorted(glob.glob(pattern)):
        m = re.search(r'_(\d+)_extracted', os.path.basename(filepath))
        if not m:
            continue
        page_num = int(m.group(1))
        try:
            with open(filepath, encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue

        for entry in data:
            if entry.get('type', '') in _PIPELINE_TYPES:
                continue  # already handled by the main pipeline
            content = (entry.get('content') or '').strip()
            if not content or not _INV_RE.search(content):
                continue

            city = _city_from_inv_line(content)
            if not city:
                continue

            bbox = entry.get('bbox', [])
            if len(bbox) >= 4:
                # City-section headers span full page width; use col=1 so the
                # marker propagates into col=2 via binary-search key ordering.
                y_center = (bbox[1] + bbox[3]) / 2
                col = 1
            else:
                y_center = 0.5
                col = 1

            # Map y_center (0–1) to an approximate row using the actual row
            # distribution for this (page, col) in the pipeline data.
            page_col_rows = df[
                (df['page'] == page_num) & (df['column'] == col)
            ]['row']
            if not page_col_rows.empty:
                row = max(1, int(y_center * int(page_col_rows.max())))
            else:
                row = max(1, int(y_center * 100))

            rows.append({
                'page': page_num, 'column': col, 'row': row,
                'line': city, 'line_complete': city, 'split': 0,
                'location': city,
            })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def build_location_list_with_headers(df, mineru_dir=None, threshold=8, pos_limit=2):
    """Build location list augmented with MinerU page-header and alphabet-
    transition municipality data.

    Calls ``build_location_list`` first (existing ``inv.)`` detection), then
    injects additional location markers in two passes:

    Pass 1 – single-mun MinerU headers (1920/1921 only): inserts a start-of-
    page marker for pages beyond the last ``inv.)``-covered page.

    Pass 2 – alphabet transitions (ALL books): for each page that has no
    existing ``inv.)`` marker, ``_alpha_transitions_by_col`` is run to detect
    mid-page municipality boundaries.  The municipality name after the
    transition comes from:

    * MinerU range headers (1920/1921) when the page header lists two names, or
    * The next distinct ``inv.)`` entry (1911-1914 and fallback for 1920/1921).

    Range-header pages always get a start-of-page marker for the first
    municipality even when alphabet detection finds no transition.
    """
    import numpy as np

    location_list = build_location_list(df)

    # Supplement with inv.) entries that were filtered out by type in the
    # pipeline (e.g. table_caption city headers in 1920/1921).
    # A single page can carry more than one transition, so we add all
    # supplementary entries regardless of whether the page already has an
    # inv.) marker from the pipeline.
    if mineru_dir:
        extra_inv = load_inv_from_mineru_json(mineru_dir, df)
        if not extra_inv.empty:
            location_list = pd.concat(
                [location_list, extra_inv], axis=0, ignore_index=True
            ).sort_values(['page', 'column', 'row']).reset_index(drop=True)

    page_headers = load_mineru_headers(mineru_dir) if mineru_dir else {}

    # Pages with existing inv.) markers (exclude synthetic page-0 header)
    inv_pages = location_list.loc[location_list['page'] > 0, 'page']
    max_covered_page = int(inv_pages.max()) if len(inv_pages) > 0 else 0
    # First real content page: skip everything before (ad / front-matter pages)
    first_inv_page = int(inv_pages.min()) if len(inv_pages) > 0 else 0
    pages_with_markers = set(inv_pages.astype(int))

    # Binary-search arrays built from the BASE location_list for municipality
    # look-ups at arbitrary (page, col, row) positions.
    loc_sorted = location_list.sort_values(['page', 'column', 'row'])

    def _read_key(frame):
        return (
            (frame['page'].astype(int) * 1_000 + frame['column'].astype(int))
            * 100_000 + frame['row'].astype(int)
        ).values

    loc_keys = _read_key(loc_sorted)
    loc_vals = loc_sorted['location'].tolist()

    def _mun_at(page, col=0, row=0):
        key = int((int(page) * 1_000 + int(col)) * 100_000 + int(row))
        idx = int(np.searchsorted(loc_keys, key, side='right')) - 1
        return loc_vals[max(0, min(idx, len(loc_vals) - 1))]

    _fwd_pages = [int(p) for p in loc_sorted['page'].values]

    # Maximum number of pages between a detected alpha transition and the next
    # inv.) marker for the transition to be considered real.  Transitions where
    # the next municipality is further away are almost certainly within-
    # municipality alphabet noise and are silently skipped.
    _MAX_ALPHA_DIST = 3

    def _next_mun_after(page_num, current_mun):
        """First inv.) municipality distinct from current_mun after page_num.

        Returns ``(municipality, distance)`` where distance is the number of
        pages between page_num and the inv.) entry.  Returns ``(None, 9999)``
        when no such entry exists.
        """
        cur = current_mun.strip().lower()
        for pg, mun in zip(_fwd_pages, loc_vals):
            if pg > page_num and mun.strip().lower() != cur:
                return mun, pg - page_num
        return None, 9999

    new_rows = []

    def _make_row(page, col, row, mun):
        return {
            'page': page, 'column': col, 'row': row,
            'line': mun, 'line_complete': mun, 'split': 0, 'location': mun,
        }

    # ── Pass 1: single-mun MinerU headers beyond inv.) coverage ─────────
    for page_num, mun_names in sorted(page_headers.items()):
        if len(mun_names) == 1 and page_num > max_covered_page:
            new_rows.append(_make_row(page_num, 0, 0, mun_names[0]))

    # ── Pass 2: range-header anchors (Method 3) ──────────────────────────
    # For pages whose MinerU header lists two municipalities, insert a
    # start-of-page marker for the first municipality.  This covers any
    # remaining gap where the inv.) entry and the range page are not on
    # the same page (rare after the type-filter and regex fixes above).
    #
    # ── Method 2 (alpha-transitions) — DISABLED ──────────────────────────
    # _alpha_transitions_by_col detects Z→A alphabet drops in last_name to
    # locate municipality boundaries mid-page.  Grid-search on 1920/1921
    # MinerU ground truth (threshold=8, pos_limit=2) achieved recall=0.978,
    # precision=0.587, F1=0.734.  However, enabling it on pages that already
    # carry inv.) markers caused a regression (alpha can fire before the inv.)
    # row, moving the boundary earlier than it should be).  Left disabled
    # until Method 1 quality is fully validated on all years.
    for page_num in sorted(page_headers.keys()):
        page_num = int(page_num)
        if page_num < first_inv_page:
            continue
        if len(page_headers[page_num]) < 2:
            continue  # single-mun pages handled by Pass 1
        if page_num in pages_with_markers:
            continue  # inv.) already anchors this page
        first_mun = _resolve_range_first_mun(
            page_num, page_headers[page_num][0], page_headers
        )
        new_rows.append(_make_row(page_num, 0, 0, first_mun))

    if not new_rows:
        return location_list

    extra = pd.DataFrame(new_rows)
    location_list = pd.concat([location_list, extra], axis=0, ignore_index=True)
    location_list = location_list.sort_values(
        ['page', 'column', 'row']
    ).reset_index(drop=True)
    return location_list
