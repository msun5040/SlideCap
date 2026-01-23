#!/usr/bin/env python3
"""
Setup Test Directory

Creates a test directory structure with sample slide files (empty files with correct names).
Use this to test the indexer before pointing at your real network drive.

Usage:
    python scripts/setup_test_dir.py /path/to/test/directory

This will create:
    /path/to/test/directory/
    ├── 2025/
    │   ├── BS-25-F12345_A1_HE_abc123.svs
    │   ├── BS25-12345_A1_IHC-CD3_def456.svs
    │   ├── BS25-123456_B1_HE_789abc.svs
    │   └── ...
    ├── 2024/
    │   └── ...
    └── .slidecap/
        └── (app data will go here)
"""
import sys
import random
from pathlib import Path


def random_hex(length: int = 6) -> str:
    """Generate a random hex string."""
    return ''.join(random.choices('0123456789abcdef', k=length))


def generate_accession(year: int, case_num: int) -> str:
    """
    Generate an accession number in one of the supported formats:
    - BS-25-F12345  (with dashes and letter prefix)
    - BS25-12345    (no dash after BS)
    - BS25-123456   (6-digit case number)
    """
    year_short = str(year)[2:]  # 2025 -> 25
    format_choice = random.randint(1, 3)

    if format_choice == 1:
        # BS-25-F12345 format
        letter = random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        return f"BS-{year_short}-{letter}{case_num:05d}"
    elif format_choice == 2:
        # BS25-12345 format (5 digits)
        return f"BS{year_short}-{case_num:05d}"
    else:
        # BS25-123456 format (6 digits)
        return f"BS{year_short}-{case_num:06d}"


def create_test_slides(root_dir: str, slides_per_year: int = 10):
    """
    Create a test directory with sample slide files.

    Args:
        root_dir: Root directory for test slides
        slides_per_year: Number of slides to create per year
    """
    root = Path(root_dir)

    # Years to create
    years = [2025, 2024, 2023]

    # Stain types
    stains = ['HE', 'IHC-CD3', 'IHC-CD20', 'PAS', 'Trichrome']

    # Blocks
    blocks = ['A1', 'A2', 'B1', 'B2', 'C1']

    print(f"Creating test directory at: {root}")
    print("=" * 60)

    total_created = 0

    for year in years:
        year_dir = root / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        # Create slides
        case_num = 1
        slides_created = 0

        while slides_created < slides_per_year:
            # Each case can have multiple slides
            num_slides_for_case = random.randint(1, 3)

            for _ in range(num_slides_for_case):
                if slides_created >= slides_per_year:
                    break

                accession = generate_accession(year, case_num)
                block = random.choice(blocks)
                stain = random.choice(stains)
                random_id = random_hex(6)

                filename = f"{accession}_{block}_{stain}_{random_id}.svs"
                filepath = year_dir / filename

                # Create empty file (or small file to simulate)
                filepath.touch()

                slides_created += 1
                total_created += 1

            case_num += 1

        print(f"  {year}: Created {slides_created} slides ({case_num - 1} cases)")
    
    # Create app data directory
    app_data_dir = root / ".slidecap"
    app_data_dir.mkdir(exist_ok=True)
    (app_data_dir / "thumbnails").mkdir(exist_ok=True)
    
    print("=" * 60)
    print(f"Total slides created: {total_created}")
    print(f"App data directory: {app_data_dir}")
    print()
    print("Next steps:")
    print(f"  1. Update NETWORK_ROOT in backend/app/config.py to: {root}")
    print("  2. Run: cd backend && pip install -r requirements.txt")
    print("  3. Run: cd backend && python -m uvicorn app.main:app --reload")
    print("  4. Visit: http://localhost:8000/docs to see the API")
    print("  5. POST to /index/full to index all slides")
    print("  6. GET /search?q=BS25 to search for slides")


def main():
    if len(sys.argv) < 2:
        print("Usage: python setup_test_dir.py /path/to/test/directory")
        print()
        print("Example:")
        print("  python setup_test_dir.py ~/slide-test")
        print("  python setup_test_dir.py /tmp/slide-test")
        sys.exit(1)
    
    test_dir = sys.argv[1]
    slides_per_year = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    
    create_test_slides(test_dir, slides_per_year)


if __name__ == "__main__":
    main()
