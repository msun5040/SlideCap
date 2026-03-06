"""
Filename parser for slide files.

Expected format: surgicalAccession_block-slide_stainType_randomIdentifier.svs
Examples:
    BS-25-F12345_A1-1_HE_7f3a2b.svs
    BS25-12345_A1-2_IHC-CD3_8c4d1e.svs
    BS25-123456_B2-1_HE_9e5f2a.svs
    BS23-F12345_1_HNE.svs           (slide number only, no block, no random_id)
    BS22-W29575_SMA_HNE_121259.svs  (multi-letter block type)
"""
import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedFilename:
    """Structured representation of slide filename components."""
    accession: str          # BS-25-F12345 or BS25-12345 (PHI - surgical accession number)
    block_id: str           # A1, B2, SMA, etc. (empty string if absent)
    slide_number: str       # 1, 2, 3, etc. (empty string if absent)
    stain_type: str         # HE, IHC-CD3, etc.
    random_id: str          # 7f3a2b (empty string if absent)
    year: int               # 2025 (extracted from accession)

    @property
    def full_stem(self) -> str:
        """Reconstruct filename without extension."""
        if self.block_id and self.slide_number:
            block_part = f"{self.block_id}-{self.slide_number}"
        elif self.block_id:
            block_part = self.block_id
        else:
            block_part = self.slide_number  # digits only — no leading dash
        parts = [self.accession, block_part, self.stain_type]
        if self.random_id:
            parts.append(self.random_id)
        return '_'.join(parts)

    @property
    def deidentified_name(self) -> str:
        """Filename suitable for sharing with collaborators."""
        identifier = self.random_id or self.slide_number or self.block_id or 'unknown'
        return f"{identifier}.svs"


class FilenameParser:
    """
    Parses slide filenames into structured components.

    Handles formats:
        BS-25-F12345_A1-1_HE_7f3a2b.svs  (with dashes and letter prefix)
        BS25-12345_A1-2_HE_7f3a2b.svs    (no dash after BS)
        BS25-123456_B2-1_HE_9e5f2a.svs   (6-digit case number)
        BS23-F12345_1_HNE.svs            (slide number only, no block, no random_id)
        BS22-W29575_SMA_HNE_121259.svs   (multi-letter block type)
    """

    # Pattern breakdown:
    # ((?:BS|BN)-?(\d{2})-[A-Z]?\d{5,6})  - Accession: BS/BN + optional dash + 2-digit year + dash + optional letter + 5-6 digits
    # ([A-Z]+(?:\d+)?)             - Block ID: one or more letters + optional digits (A, A1, B2, SMA, FSA, ...)
    # (?:-(\d+))?                  - Slide number: optional digits after a dash
    # ([A-Za-z0-9-]+)              - Stain type: alphanumeric with possible dashes
    # (?:_([A-Za-z0-9]+))?         - Random ID: optional trailing alphanumeric segment
    PATTERN = re.compile(
        r'^((?:BS|BN)-?(\d{2})-[A-Z]?\d{5,6})_(?:([A-Z]+(?:\d+)?)(?:-(\d+))?|-?(\d+))_([A-Za-z0-9-]+)(?:_([A-Za-z0-9]+))?\.svs$',
        re.IGNORECASE
    )

    def parse(self, filename: str) -> Optional[ParsedFilename]:
        """
        Parse a slide filename into components.

        Args:
            filename: The filename to parse (e.g., "BS25-12345_A1-1_HE_7f3a2b.svs")

        Returns:
            ParsedFilename if successful, None if filename doesn't match pattern
        """
        match = self.PATTERN.match(filename)
        if not match:
            return None

        accession, year_str, block_id, slide_from_block, slide_only, stain_type, random_id = match.groups()

        year_short = int(year_str)
        year = 2000 + year_short if year_short < 50 else 1900 + year_short

        # Normalize accession: BS-22-W29575 → BS22-W29575 (drop the dash between BS and year digits)
        accession_norm = re.sub(r'^BS-(\d{2})-', r'BS\1-', accession.upper())

        slide_number = slide_from_block or slide_only or ''
        block_id = block_id or ''

        return ParsedFilename(
            accession=accession_norm,
            block_id=block_id.upper() if block_id else '',
            slide_number=slide_number or '',
            stain_type=stain_type,
            random_id=random_id.lower() if random_id else '',
            year=year
        )

    def extract_accession(self, filename: str) -> Optional[str]:
        """Quick extraction of just the accession number."""
        parsed = self.parse(filename)
        return parsed.accession if parsed else None

    def is_valid_filename(self, filename: str) -> bool:
        """Check if a filename matches the expected pattern."""
        return self.PATTERN.match(filename) is not None


# Quick test
if __name__ == "__main__":
    parser = FilenameParser()

    test_files = [
        "BS-25-F12345_A1-1_HE_7f3a2b.svs",      # Format 1: BS-YY-Lnnnnn
        "BS25-12345_A1-2_IHC-CD3_8c4d1e.svs",   # Format 2: BSYY-nnnnn
        "BS25-123456_B2-1_HE_9e5f2a.svs",       # Format 3: BSYY-nnnnnn (6 digits)
        "BS-24-A99999_C1-3_PAS_abc123.svs",     # With letter prefix
        "BS22-W29575_SMA_HNE_121259.svs",        # Multi-letter block type (SMA)
        "BS22-W29575_FSA_HNE_121259.svs",        # Multi-letter block type (FSA)
        "BS23-F12345_1_HNE.svs",                 # Slide number only, no block, no random_id
        "BS23-F12345_3_IHC-CD3.svs",             # Slide number only, IHC stain, no random_id
        "invalid_filename.svs",
        "BS25-12345.svs",                        # Missing components
    ]

    print("Filename Parser Test")
    print("=" * 60)

    for f in test_files:
        result = parser.parse(f)
        if result:
            print(f"\n{f}")
            print(f"  Accession:  {result.accession}")
            print(f"  Block:      {result.block_id!r}")
            print(f"  Slide #:    {result.slide_number!r}")
            print(f"  Stain:      {result.stain_type}")
            print(f"  Random ID:  {result.random_id!r}")
            print(f"  Year:       {result.year}")
            print(f"  Full stem:  {result.full_stem}")
            print(f"  De-id name: {result.deidentified_name}")
        else:
            print(f"\n{f} - Could not parse")
