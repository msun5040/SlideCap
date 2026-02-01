"""
Filename parser for slide files.

Expected format: surgicalAccession_block-slide_stainType_randomIdentifier.svs
Examples:
    BS-25-F12345_A1-1_HE_7f3a2b.svs
    BS25-12345_A1-2_IHC-CD3_8c4d1e.svs
    BS25-123456_B2-1_HE_9e5f2a.svs
"""
import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedFilename:
    """Structured representation of slide filename components."""
    accession: str          # BS-25-F12345 or BS25-12345 (PHI - surgical accession number)
    block_id: str           # A1, B2, etc.
    slide_number: str       # 1, 2, 3, etc.
    stain_type: str         # HE, IHC-CD3, etc.
    random_id: str          # 7f3a2b (de-identification friendly)
    year: int               # 2025 (extracted from accession)

    @property
    def full_stem(self) -> str:
        """Reconstruct filename without extension."""
        return f"{self.accession}_{self.block_id}-{self.slide_number}_{self.stain_type}_{self.random_id}"

    @property
    def deidentified_name(self) -> str:
        """Filename suitable for sharing with collaborators."""
        return f"{self.random_id}.svs"


class FilenameParser:
    """
    Parses slide filenames into structured components.

    Handles formats:
        BS-25-F12345_A1-1_HE_7f3a2b.svs  (with dashes and letter prefix)
        BS25-12345_A1-2_HE_7f3a2b.svs    (no dash after BS)
        BS25-123456_B2-1_HE_7f3a2b.svs   (6-digit case number)
    """

    # Pattern breakdown:
    # (BS-?(\d{2})-[A-Z]?\d{5,6})  - Accession: BS + optional dash + 2-digit year + dash + optional letter + 5-6 digits
    # ([A-Z]\d+)                   - Block ID: letter + number(s) like A1, B2
    # (?:-(\d+))?                  - Slide number: optional digits after the dash
    # ([A-Za-z0-9-]+)              - Stain type: alphanumeric with possible dashes
    # ([A-Za-z0-9]+)               - Random ID: alphanumeric string
    PATTERN = re.compile(
        r'^(BS-?(\d{2})-[A-Z]?\d{5,6})_([A-Z]\d+)(?:-(\d+))?_([A-Za-z0-9-]+)_([A-Za-z0-9]+)\.svs$',
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

        accession, year_str, block_id, slide_number, stain_type, random_id = match.groups()

        # Year is captured directly by the regex group
        year_short = int(year_str)
        year = 2000 + year_short if year_short < 50 else 1900 + year_short

        return ParsedFilename(
            accession=accession.upper(),
            block_id=block_id.upper(),
            slide_number=slide_number or '',  # May be None if no dash in block-slide
            stain_type=stain_type,
            random_id=random_id.lower(),
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
        "invalid_filename.svs",
        "BS25-12345.svs",  # Missing components
    ]

    print("Filename Parser Test")
    print("=" * 60)

    for f in test_files:
        result = parser.parse(f)
        if result:
            print(f"\n{f}")
            print(f"  Accession: {result.accession}")
            print(f"  Block: {result.block_id}")
            print(f"  Slide #: {result.slide_number}")
            print(f"  Stain: {result.stain_type}")
            print(f"  Random ID: {result.random_id}")
            print(f"  Year: {result.year}")
            print(f"  De-identified: {result.deidentified_name}")
        else:
            print(f"\n{f} - Could not parse")
