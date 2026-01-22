#!/usr/bin/env python3
"""
Test Script

Verifies that all components work correctly.
Run this after setting up your test directory.

Usage:
    cd backend
    python -m scripts.test_components
"""
import sys
import tempfile
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / 'backend'))

def test_filename_parser():
    """Test the filename parser."""
    print("\n1. Testing Filename Parser")
    print("-" * 40)
    
    from app.services.filename_parser import FilenameParser
    
    parser = FilenameParser()
    
    test_cases = [
        ("S24-12345_A1_HE_7f3a2b.svs", True),
        ("S24-12345_A1_IHC-CD3_8c4d1e.svs", True),
        ("S23-00042_B2_PAS_9e5f2a.svs", True),
        ("invalid.svs", False),
        ("S24-12345.svs", False),
    ]
    
    all_passed = True
    for filename, should_parse in test_cases:
        result = parser.parse(filename)
        parsed = result is not None
        status = "✓" if parsed == should_parse else "✗"
        
        if parsed != should_parse:
            all_passed = False
        
        if result:
            print(f"  {status} {filename}")
            print(f"      Accession: {result.accession}, Block: {result.block_id}, "
                  f"Stain: {result.stain_type}, Year: {result.year}")
        else:
            print(f"  {status} {filename} -> Not parseable (expected: {should_parse})")
    
    return all_passed


def test_hasher():
    """Test the hashing service."""
    print("\n2. Testing Hasher")
    print("-" * 40)
    
    from app.services.hasher import SlideHasher
    
    with tempfile.TemporaryDirectory() as tmpdir:
        salt_path = Path(tmpdir) / ".salt"
        hasher = SlideHasher(salt_path)
        
        # Test consistency
        hash1 = hasher.hash_accession("S24-12345")
        hash2 = hasher.hash_accession("S24-12345")
        
        print(f"  Salt created: {salt_path.exists()}")
        print(f"  Hash length: {len(hash1)}")
        print(f"  Consistent: {hash1 == hash2}")
        
        # Test different inputs give different outputs
        hash3 = hasher.hash_accession("S24-12346")
        print(f"  Different inputs differ: {hash1 != hash3}")
        
        return hash1 == hash2 and hash1 != hash3


def test_database():
    """Test database models."""
    print("\n3. Testing Database")
    print("-" * 40)
    
    from app.db.models import init_db, Case, Slide, Tag, Project
    
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.sqlite"
        session = init_db(db_path)
        
        # Create test data
        case = Case(accession_hash="test_hash_123", year=2024)
        session.add(case)
        session.flush()
        
        slide = Slide(
            case_id=case.id,
            slide_hash="slide_hash_456",
            block_id="A1",
            stain_type="HE",
            random_id="abc123"
        )
        session.add(slide)
        
        tag = Tag(name="test-tag")
        session.add(tag)
        session.flush()
        
        case.tags.append(tag)
        slide.tags.append(tag)
        
        project = Project(name="Test Project")
        project.cases.append(case)
        session.add(project)
        
        session.commit()
        
        # Verify
        print(f"  Database created: {db_path.exists()}")
        print(f"  Case created: {case.id is not None}")
        print(f"  Slide linked to case: {slide.case_id == case.id}")
        print(f"  Tag applied: {len(case.tags) == 1}")
        print(f"  Project has case: {len(project.cases) == 1}")
        print(f"  Project slide count: {project.slide_count}")
        
        session.close()
        return True


def test_indexer():
    """Test the indexer with a temporary directory."""
    print("\n4. Testing Indexer")
    print("-" * 40)
    
    from app.services.hasher import SlideHasher
    from app.services.indexer import SlideIndexer
    from app.db.models import init_db
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create test structure
        (tmpdir / "2024").mkdir()
        (tmpdir / "2023").mkdir()
        
        # Create test files
        test_files = [
            "2024/S24-00001_A1_HE_abc123.svs",
            "2024/S24-00001_B1_IHC-CD3_def456.svs",
            "2024/S24-00002_A1_HE_789abc.svs",
            "2023/S23-00001_A1_HE_a1b2c3.svs",
        ]
        
        for f in test_files:
            (tmpdir / f).touch()
        
        # Initialize components
        salt_path = tmpdir / ".salt"
        db_path = tmpdir / "test.sqlite"
        
        hasher = SlideHasher(salt_path)
        session = init_db(db_path)
        indexer = SlideIndexer(session, hasher, str(tmpdir))
        
        # Run index
        print("  Running full index...")
        stats = indexer.build_full_index()
        
        print(f"  Years processed: {stats['years_processed']}")
        print(f"  Slides indexed: {stats['slides_indexed']}")
        print(f"  Cases created: {stats['cases_created']}")
        
        # Build cache
        cache_count = indexer.build_path_cache()
        print(f"  Cache built: {cache_count} slides")
        
        # Test search
        results = indexer.search("S24")
        print(f"  Search 'S24': {len(results)} results")
        
        results = indexer.search("", year=2023)
        print(f"  Search year=2023: {len(results)} results")
        
        session.close()
        return stats['slides_indexed'] == 4


def main():
    print("=" * 60)
    print("Slide Organizer Component Tests")
    print("=" * 60)
    
    results = []
    
    try:
        results.append(("Filename Parser", test_filename_parser()))
    except Exception as e:
        print(f"  ERROR: {e}")
        results.append(("Filename Parser", False))
    
    try:
        results.append(("Hasher", test_hasher()))
    except Exception as e:
        print(f"  ERROR: {e}")
        results.append(("Hasher", False))
    
    try:
        results.append(("Database", test_database()))
    except Exception as e:
        print(f"  ERROR: {e}")
        results.append(("Database", False))
    
    try:
        results.append(("Indexer", test_indexer()))
    except Exception as e:
        print(f"  ERROR: {e}")
        results.append(("Indexer", False))
    
    print("\n" + "=" * 60)
    print("Results")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {name}")
        if not passed:
            all_passed = False
    
    print()
    if all_passed:
        print("All tests passed! Ready to run the API server.")
    else:
        print("Some tests failed. Check the output above.")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
