"""
Slide indexer service.

Builds and maintains the index of slides from the network drive.
Handles both full reindexing and incremental updates.
"""
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Callable
from sqlalchemy.orm import Session

from .filename_parser import FilenameParser, ParsedFilename
from .hasher import SlideHasher
from ..db.models import Case, Slide


class SlideIndexer:
    """
    Indexes slides from the network drive into the database.
    
    Maintains two types of indexes:
    1. Database (persistent): Case and slide metadata, tags, projects
    2. In-memory cache (runtime): Hash -> filepath mappings for instant lookup
    """
    
    def __init__(
        self,
        db_session: Session,
        hasher: SlideHasher,
        network_root: str
    ):
        self.db = db_session
        self.hasher = hasher
        self.root = Path(network_root)
        self.parser = FilenameParser()
        
        # In-memory caches for fast lookup (built on startup)
        self.slide_hash_to_path: dict[str, Path] = {}
        self.accession_hash_to_paths: dict[str, list[Path]] = {}
    
    def index_file(self, filepath: Path) -> tuple[Case, Slide] | None:
        """
        Index a single slide file.
        
        Creates Case if it doesn't exist, then creates or updates Slide.
        
        Args:
            filepath: Path to the SVS file
            
        Returns:
            (case, slide) tuple, or None if file can't be parsed
        """
        parsed = self.parser.parse(filepath.name)
        if not parsed:
            return None
        
        # Hash the accession for case-level lookup
        accession_hash = self.hasher.hash_accession(parsed.accession)
        
        # Hash the full filename for slide-level lookup
        slide_hash = self.hasher.hash_slide_stem(parsed.full_stem)
        
        # Get or create case
        case = self.db.query(Case).filter_by(accession_hash=accession_hash).first()
        if not case:
            case = Case(
                accession_hash=accession_hash,
                year=parsed.year,
                indexed_at=datetime.utcnow()
            )
            self.db.add(case)
            self.db.flush()  # Get the ID
        
        # Check if slide already exists
        existing_slide = self.db.query(Slide).filter_by(slide_hash=slide_hash).first()
        if existing_slide:
            # Update file_exists flag if re-indexing
            existing_slide.file_exists = 1
            return case, existing_slide
        
        # Create new slide
        try:
            file_size = filepath.stat().st_size
        except OSError:
            file_size = None
            
        slide = Slide(
            case_id=case.id,
            slide_hash=slide_hash,
            block_id=parsed.block_id,
            stain_type=parsed.stain_type,
            random_id=parsed.random_id,
            file_size_bytes=file_size,
            indexed_at=datetime.utcnow()
        )
        self.db.add(slide)
        
        return case, slide
    
    def build_full_index(
        self,
        progress_callback: Optional[Callable[[dict], None]] = None
    ) -> dict:
        """
        Index all slides in the network drive.
        
        Args:
            progress_callback: Optional function called after each year with progress info
            
        Returns:
            Summary statistics dict
        """
        stats = {
            'years_processed': [],
            'cases_created': 0,
            'slides_indexed': 0,
            'slides_updated': 0,
            'files_skipped': 0,
            'errors': []
        }
        
        # Mark all existing slides as potentially missing
        self.db.query(Slide).update({Slide.file_exists: 0})
        self.db.commit()
        
        # Find year directories
        year_dirs = sorted([
            d for d in self.root.iterdir()
            if d.is_dir() and d.name.isdigit()
        ])
        
        if not year_dirs:
            print(f"Warning: No year directories found in {self.root}")
            print("Expected structure: {root}/2024/, {root}/2023/, etc.")
            return stats
        
        cases_before = self.db.query(Case).count()
        
        for year_dir in year_dirs:
            year = int(year_dir.name)
            svs_files = list(year_dir.glob('*.svs'))
            year_indexed = 0
            year_skipped = 0
            
            for filepath in svs_files:
                try:
                    result = self.index_file(filepath)
                    if result:
                        year_indexed += 1
                        stats['slides_indexed'] += 1
                    else:
                        year_skipped += 1
                        stats['files_skipped'] += 1
                except Exception as e:
                    stats['errors'].append({
                        'file': str(filepath),
                        'error': str(e)
                    })
            
            # Commit after each year
            self.db.commit()
            stats['years_processed'].append(year)
            
            if progress_callback:
                progress_callback({
                    'year': year,
                    'slides_in_year': len(svs_files),
                    'indexed': year_indexed,
                    'skipped': year_skipped,
                    'total_slides': stats['slides_indexed']
                })
            else:
                print(f"  Year {year}: {year_indexed} slides indexed, {year_skipped} skipped")
        
        # Count new cases
        cases_after = self.db.query(Case).count()
        stats['cases_created'] = cases_after - cases_before
        
        return stats
    
    def build_path_cache(self) -> int:
        """
        Build in-memory hash -> filepath mappings for instant lookup.
        
        Call this on application startup after database is ready.
        
        Returns:
            Number of slides cached
        """
        self.slide_hash_to_path.clear()
        self.accession_hash_to_paths.clear()
        
        count = 0
        
        # Scan filesystem and build mappings
        for year_dir in self.root.iterdir():
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            
            for filepath in year_dir.glob('*.svs'):
                parsed = self.parser.parse(filepath.name)
                if not parsed:
                    continue
                
                # Slide hash -> path
                slide_hash = self.hasher.hash_slide_stem(parsed.full_stem)
                self.slide_hash_to_path[slide_hash] = filepath
                
                # Accession hash -> paths (multiple slides per case)
                accession_hash = self.hasher.hash_accession(parsed.accession)
                if accession_hash not in self.accession_hash_to_paths:
                    self.accession_hash_to_paths[accession_hash] = []
                self.accession_hash_to_paths[accession_hash].append(filepath)
                
                count += 1
        
        return count
    
    def get_filepath(self, slide_hash: str) -> Optional[Path]:
        """Get the actual filesystem path for a slide hash."""
        return self.slide_hash_to_path.get(slide_hash)
    
    def get_case_filepaths(self, accession_hash: str) -> list[Path]:
        """Get all slide filepaths for a case (accession hash)."""
        return self.accession_hash_to_paths.get(accession_hash, [])
    
    def search(
        self,
        query: str,
        year: Optional[int] = None,
        stain_type: Optional[str] = None,
        tags: Optional[list[str]] = None,
        limit: int = 100
    ) -> list[dict]:
        """
        Search for slides by accession number (partial match supported).
        
        Since we can't search hashes by partial match, we search the
        in-memory path cache which has the actual filenames.
        
        Args:
            query: Search string (matches against filename)
            year: Optional year filter
            stain_type: Optional stain type filter (e.g., "HE")
            tags: Optional list of tag names to filter by
            limit: Maximum results to return
            
        Returns:
            List of slide info dicts
        """
        results = []
        query_lower = query.lower().strip()
        
        for slide_hash, filepath in self.slide_hash_to_path.items():
            filename = filepath.name.lower()
            
            # Check if query matches filename
            if query_lower and query_lower not in filename:
                continue
            
            # Parse for filtering
            parsed = self.parser.parse(filepath.name)
            if not parsed:
                continue
            
            # Apply filters
            if year and parsed.year != year:
                continue
            
            if stain_type and parsed.stain_type.lower() != stain_type.lower():
                continue
            
            # Get database record for tags and other metadata
            slide = self.db.query(Slide).filter_by(slide_hash=slide_hash).first()
            
            # Tag filter (if specified)
            if tags and slide:
                slide_tag_names = {t.name.lower() for t in slide.tags}
                case_tag_names = {t.name.lower() for t in slide.case.tags}
                all_tags = slide_tag_names | case_tag_names
                
                if not any(t.lower() in all_tags for t in tags):
                    continue
            
            # Build result
            result = {
                'slide_hash': slide_hash,
                'year': parsed.year,
                'block_id': parsed.block_id,
                'stain_type': parsed.stain_type,
                'random_id': parsed.random_id,
            }
            
            if slide:
                result['case_hash'] = slide.case.accession_hash
                result['slide_tags'] = [t.name for t in slide.tags]
                result['case_tags'] = [t.name for t in slide.case.tags]
                result['projects'] = [p.name for p in slide.case.projects]
                result['file_size_bytes'] = slide.file_size_bytes
            
            results.append(result)
            
            if len(results) >= limit:
                break
        
        return results
    
    def get_stats(self) -> dict:
        """Get current index statistics."""
        return {
            'total_cases': self.db.query(Case).count(),
            'total_slides': self.db.query(Slide).count(),
            'slides_in_cache': len(self.slide_hash_to_path),
            'years': sorted(set(
                int(p.parent.name) 
                for p in self.slide_hash_to_path.values()
            )),
            'stain_types': sorted(set(
                self.parser.parse(p.name).stain_type
                for p in self.slide_hash_to_path.values()
                if self.parser.parse(p.name)
            )),
        }


# Quick test
if __name__ == "__main__":
    print("Indexer module loaded successfully")
    print("Run the test script to test with actual files")
