"""
Filename parser for slide files.

The parser accepts one or more regex patterns and tries each in order. Each
pattern must use these named groups (any may be absent — the parser fills
missing fields with empty strings):

    accession       (required) full accession identifier
    year            (required) 2-digit or 4-digit year
    block           block id (A, A1, SMA, ...)
    slide           slide number (1, 2, ...)
    stain           stain type (HE, IHC-CD3, ...)
    random          random / secondary identifier

The default pattern matches the DFCI Ligon Lab format:

    BS-25-F12345_A1-1_HE_7f3a2b.svs
    BS25-12345_A1-2_IHC-CD3_8c4d1e.svs
    BS22-W29575_SMA_HNE_121259.svs   (multi-letter block, no slide number)
    BS23-F12345_1_HNE.svs            (no block, slide number only)
    BS11-K54919_C1-7_194606.svs      (no stain, trailing token is random)

Deploying institutions can override the pattern list via the
`PARSER_PATTERNS` environment variable (see backend/app/config.py).
"""
import re
from dataclasses import dataclass
from typing import Iterable, List, Optional


@dataclass
class NamedPattern:
    """A regex pattern with a human-readable name and description."""
    name: str
    description: str
    regex: re.Pattern


@dataclass
class ParsedFilename:
    """Structured representation of slide filename components."""
    accession: str          # BS-25-F12345 or BS25-12345 (PHI - surgical accession number)
    block_id: str           # A1, B2, SMA, etc. (empty string if absent)
    slide_number: str       # 1, 2, 3, etc. (empty string if absent)
    stain_type: str         # HE, IHC-CD3, etc.
    random_id: str          # 7f3a2b (empty string if absent)
    year: int               # 2025 (extracted from accession)
    pattern_name: str = ''  # Which configured pattern matched

    @property
    def full_stem(self) -> str:
        """Reconstruct filename without extension."""
        if self.block_id and self.slide_number:
            block_part = f"{self.block_id}-{self.slide_number}"
        elif self.block_id:
            block_part = self.block_id
        else:
            block_part = self.slide_number  # digits only — no leading dash
        parts = [self.accession, block_part]
        if self.stain_type:
            parts.append(self.stain_type)
            if self.random_id:
                parts.append(self.random_id)
        elif self.random_id:
            # No stain — random_id occupies the same regex slot in the filename
            parts.append(self.random_id)
        return '_'.join(parts)

    @property
    def deidentified_name(self) -> str:
        """Filename suitable for sharing with collaborators."""
        identifier = self.random_id or self.slide_number or self.block_id or 'unknown'
        return f"{identifier}.svs"


# ── Default DFCI pattern (named groups) ────────────────────────────────
# Same structure as the legacy positional pattern, just with names.

_DEFAULT_PATTERN_REGEX = (
    r'^(?P<accession>(?:BS|BN)-?(?P<year>\d{2})-[A-Z]?\d{5,6})_'
    r'(?:(?P<block>[A-Z]+(?:\d+)?)(?:-(?P<slide>\d+))?|-?(?P<slide_only>\d+))_'
    r'(?P<stain>[A-Za-z0-9-]+)(?:_(?P<random>[A-Za-z0-9]+))?\.svs$'
)


def _default_patterns() -> list[NamedPattern]:
    return [
        NamedPattern(
            name="DFCI Ligon Lab",
            description=(
                "Default — BS/BN accession (BS25-12345 or BS-25-F12345), optional "
                "block (A1, SMA), optional slide number, stain or trailing random ID."
            ),
            regex=re.compile(_DEFAULT_PATTERN_REGEX, re.IGNORECASE),
        ),
    ]


def patterns_from_config(raw: str) -> list[NamedPattern]:
    """
    Build a list of NamedPattern from a JSON string (PARSER_PATTERNS env var).

    Empty / whitespace input returns the built-in DFCI default. Invalid JSON
    or invalid regex prints a warning and falls back to the default — never
    raises, so a bad config can't take the backend down.
    """
    import json
    raw = (raw or '').strip()
    if not raw:
        return _default_patterns()
    try:
        items = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[parser] PARSER_PATTERNS is not valid JSON ({e}). Falling back to default.")
        return _default_patterns()
    if not isinstance(items, list) or not items:
        print(f"[parser] PARSER_PATTERNS must be a non-empty JSON list. Falling back to default.")
        return _default_patterns()

    out: list[NamedPattern] = []
    for i, entry in enumerate(items):
        if not isinstance(entry, dict):
            print(f"[parser] PARSER_PATTERNS[{i}] is not an object — skipping.")
            continue
        name = str(entry.get('name') or f"Pattern {i + 1}")
        description = str(entry.get('description') or '')
        regex_str = entry.get('regex')
        if not regex_str:
            print(f"[parser] PARSER_PATTERNS[{i}] missing 'regex' — skipping.")
            continue
        try:
            compiled = re.compile(regex_str, re.IGNORECASE)
        except re.error as e:
            print(f"[parser] PARSER_PATTERNS[{i}] regex invalid ({e}) — skipping.")
            continue
        out.append(NamedPattern(name=name, description=description, regex=compiled))

    if not out:
        print(f"[parser] PARSER_PATTERNS yielded zero valid patterns. Falling back to default.")
        return _default_patterns()
    return out


def default_pattern_regex() -> str:
    """The string form of the built-in DFCI pattern (for the settings UI)."""
    return _DEFAULT_PATTERN_REGEX


class FilenameParser:
    """
    Parses slide filenames into structured components.

    Accepts a list of `NamedPattern`s — tries each in order, first match wins.
    Falls back to the bundled DFCI pattern if no list is provided.
    """

    def __init__(self, patterns: Optional[Iterable[NamedPattern]] = None):
        self.patterns: List[NamedPattern] = list(patterns) if patterns else _default_patterns()

    # Convenience for old callers that read `.PATTERN` directly
    @property
    def PATTERN(self) -> re.Pattern:
        return self.patterns[0].regex if self.patterns else re.compile(_DEFAULT_PATTERN_REGEX, re.IGNORECASE)

    def parse(self, filename: str) -> Optional[ParsedFilename]:
        """Parse a slide filename. Returns None if no configured pattern matches."""
        for pat in self.patterns:
            match = pat.regex.match(filename)
            if match:
                return self._build_parsed(match, pat.name)
        return None

    def _build_parsed(self, match: re.Match, pattern_name: str) -> ParsedFilename:
        gd = match.groupdict()

        # Accession is required; year is required
        accession = (gd.get('accession') or '').strip()
        # Normalize accession: BS-22-W29575 → BS22-W29575
        accession_norm = re.sub(r'^BS-(\d{2})-', r'BS\1-', accession.upper())

        year_str = gd.get('year') or '0'
        try:
            year_int = int(year_str)
        except ValueError:
            year_int = 0
        if len(year_str) <= 2:
            year = 2000 + year_int if year_int < 50 else 1900 + year_int
        else:
            year = year_int

        # Slide number can come from one of two named groups
        slide_number = (gd.get('slide') or gd.get('slide_only') or '').strip()
        block_id = (gd.get('block') or '').strip()
        stain_type = (gd.get('stain') or '').strip()
        random_id = (gd.get('random') or '').strip()

        # Legacy filename heuristic: if "stain" is purely digits (4+ chars)
        # and there's no separate random_id, treat the digits as the random
        # id and leave stain_type empty. See full_stem for the round-trip.
        if not random_id and stain_type and stain_type.isdigit() and len(stain_type) >= 4:
            random_id = stain_type
            stain_type = ''

        return ParsedFilename(
            accession=accession_norm,
            block_id=block_id.upper() if block_id else '',
            slide_number=slide_number,
            stain_type=stain_type,
            random_id=random_id.lower() if random_id else '',
            year=year,
            pattern_name=pattern_name,
        )

    def extract_accession(self, filename: str) -> Optional[str]:
        """Quick extraction of just the accession number."""
        parsed = self.parse(filename)
        return parsed.accession if parsed else None

    def is_valid_filename(self, filename: str) -> bool:
        """Check if a filename matches any configured pattern."""
        return self.parse(filename) is not None


# Quick test
if __name__ == "__main__":
    parser = FilenameParser()

    test_files = [
        "BS-25-F12345_A1-1_HE_7f3a2b.svs",
        "BS25-12345_A1-2_IHC-CD3_8c4d1e.svs",
        "BS25-123456_B2-1_HE_9e5f2a.svs",
        "BS-24-A99999_C1-3_PAS_abc123.svs",
        "BS22-W29575_SMA_HNE_121259.svs",
        "BS22-W29575_FSA_HNE_121259.svs",
        "BS23-F12345_1_HNE.svs",
        "BS23-F12345_3_IHC-CD3.svs",
        "BS11-K54919_C1-7_194606.svs",   # no stain — trailing random id
        "invalid_filename.svs",
        "BS25-12345.svs",
    ]

    print("Filename Parser Test")
    print("=" * 60)
    for f in test_files:
        result = parser.parse(f)
        if result:
            print(f"\n{f}  [{result.pattern_name}]")
            print(f"  Accession:  {result.accession}")
            print(f"  Block:      {result.block_id!r}")
            print(f"  Slide #:    {result.slide_number!r}")
            print(f"  Stain:      {result.stain_type!r}")
            print(f"  Random ID:  {result.random_id!r}")
            print(f"  Year:       {result.year}")
            print(f"  Full stem:  {result.full_stem}")
        else:
            print(f"\n{f} - Could not parse")
