"""
Filename parser for slide files.

Expected format: surgicalAccession_block+slide_stainType_randomIdentifier.svs
Examples:
    S24-12345_A1_HE_7f3a2b.svs
    S24-12345_A1_IHC-CD3_8c4d1e.svs
    S23-00042_B2_HE_9e5f2a.svs
"""
import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class ParsedFilename:
    """Structured representation of slide filename components."""
    accession: str          # S24-12345 (PHI - surgical accession number)
    block_id: str           # A1, B2, etc.
    stain_type: str         # HE, IHC-CD3, etc.
    random_id: str          # 7f3a2b (de-identification friendly)
    year: int               # 2024 (extracted from accession)
    
    @property
    def full_stem(self) -> str:
        """Reconstruct filename without extension."""
        return f"{self.accession}_{self.block_id}_{self.stain_type}_{self.random_id}"
    
    @property 
    def deidentified_name(self) -> str:
        """Filename suitable for sharing with collaborators."""
        return f"{self.random_id}.svs"


class FilenameParser:
    """
    Parses slide filenames into structured components.
    
    Handles format: surgicalAccession_block+slide_stainType_randomIdentifier.svs
    """
    
    # Pattern breakdown:
    # (S\d{2}-\d+)      - Accession: S + 2-digit year + dash + case number
    # ([A-Z]\d+)        - Block ID: letter + number(s)  
    # ([A-Za-z0-9-]+)   - Stain type: alphanumeric with possible dashes
    # ([A-Za-z0-9]+)    - Random ID: alphanumeric string
    PATTERN = re.compile(
        r'^(S\d{2}-\d+)_([A-Z]\d+)_([A-Za-z0-9-]+)_([A-Za-z0-9]+)\.svs$',
        re.IGNORECASE
    )
    
    def parse(self, filename: str) -> Optional[ParsedFilename]:
        """
        Parse a slide filename into components.
        
        Args:
            filename: The filename to parse (e.g., "S24-12345_A1_HE_7f3a2b.svs")
            
        Returns:
            ParsedFilename if successful, None if filename doesn't match pattern
        """
        match = self.PATTERN.match(filename)
        if not match:
            return None
        
        accession, block_id, stain_type, random_id = match.groups()
        
        # Extract year from accession (S24-xxxxx -> 2024)
        year_short = int(accession[1:3])
        year = 2000 + year_short if year_short < 50 else 1900 + year_short
        
        return ParsedFilename(
            accession=accession.upper(),
            block_id=block_id.upper(),
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
        "S24-12345_A1_HE_7f3a2b.svs",
        "S24-12345_A1_IHC-CD3_8c4d1e.svs",
        "S23-00042_B2_HE_9e5f2a.svs",
        "S24-00001_C1_PAS_abc123.svs",
        "invalid_filename.svs",
        "S24-12345.svs",  # Missing components
    ]
    
    print("Filename Parser Test")
    print("=" * 60)
    
    for f in test_files:
        result = parser.parse(f)
        if result:
            print(f"\n✓ {f}")
            print(f"  Accession: {result.accession}")
            print(f"  Block: {result.block_id}")
            print(f"  Stain: {result.stain_type}")
            print(f"  Random ID: {result.random_id}")
            print(f"  Year: {result.year}")
            print(f"  De-identified: {result.deidentified_name}")
        else:
            print(f"\n✗ {f} - Could not parse")
