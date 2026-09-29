"""
Which scanner produced a slide.

Grundium Ocus writes Aperio-compatible SVS, so openslide reports vendor 'aperio'
for both the lab's Aperio and the Grundium -- vendor alone cannot tell them
apart. The field that does is the Aperio ScanScope ID in the TIFF
ImageDescription ('SS12340' vs 'Grundium Ocus'), which Grundium fills in with
its own name.

This matters because scanner is a batch variable: in the UNI patch-embedding
atlas, Aperio and Grundium slides were separable at AUC 1.00 with case-grouped
CV, i.e. a model could tell the scanner apart perfectly from the embeddings
alone. Being able to filter a cohort by scanner before submitting it is how you
keep that out of a result.

Reading is header-only -- no pixel decode, no label image, no accession -- so the
cost is O(1) in slide size and safe to run over a whole library. It is still a
file open, which is why indexing never calls it (see Slide.scanner).
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

# The Aperio ImageDescription field both makes populate.
SCANSCOPE_ID = "aperio.ScanScope ID"

# Recognised values, for display and for the "is this the Grundium?" question.
# Anything not listed is still stored verbatim -- this map only drives labels.
KNOWN_SCANNERS = {
    "SS12340": "Aperio",
    "Grundium Ocus": "Grundium",
}


def label_for(scanner: Optional[str]) -> str:
    """Human-facing name for a stored scanner string."""
    if not scanner:
        return "Unknown"
    return KNOWN_SCANNERS.get(scanner, scanner)


def read_scanner(filepath: Path) -> tuple[Optional[str], Optional[str]]:
    """
    Read the scanner identifier from a slide header.

    Returns (scanner, error). A successful read with nothing to report gives
    (None, None) -- the caller should still stamp scanner_checked_at so the
    slide isn't probed again on every backfill.
    """
    try:
        import openslide
    except Exception as e:  # pragma: no cover - import guard
        return None, f"openslide unavailable: {e}"

    try:
        props = openslide.OpenSlide(str(filepath)).properties
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"

    scanner = props.get(SCANSCOPE_ID)
    if scanner and scanner.strip():
        return scanner.strip(), None

    # No ScanScope ID: not an Aperio-family file at all. The vendor string is
    # the next most specific thing we have (hamamatsu, leica, generic-tiff...).
    vendor = props.get("openslide.vendor")
    if vendor and vendor.strip():
        return vendor.strip(), None

    return None, None
