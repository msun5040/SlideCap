from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from io import BytesIO
import os
import json
import zipfile
import queue
import threading
import re
from datetime import datetime
from sqlalchemy.orm import Session, joinedload

from .config import settings
from .db import init_db, get_db, get_session, Case, Slide, Tag, Project, Cohort, Analysis, AnalysisJob, JobSlide, init_lock, get_lock
from .services import SlideHasher, SlideIndexer, ClusterService, JobStatusPoller


# Request models for bulk operations
class BulkTagRequest(BaseModel):
    slide_hashes: List[str]
    tags: List[str]
    color: Optional[str] = None  # Color for new tags


# Global instances (initialized on startup)
hasher = None
indexer = None
db_lock = None
cluster_service: Optional[ClusterService] = None
job_poller: Optional[JobStatusPoller] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global hasher, indexer, db_lock, cluster_service, job_poller

    print("=" * 60)
    print("Starting Slide Organizer API")
    print("=" * 60)

    # Validate configuration
    if not os.path.exists(settings.NETWORK_ROOT):
        print(f"ERROR: Network root does not exist: {settings.NETWORK_ROOT}")
        print("Please update NETWORK_ROOT in config.py or set via environment variable")
        raise RuntimeError(f"Network root not found: {settings.NETWORK_ROOT}")

    if not settings.slides_path.exists():
        print(f"Creating slides directory: {settings.slides_path}")
        settings.slides_path.mkdir(parents=True, exist_ok=True)

    print(f"Network root: {settings.NETWORK_ROOT}")
    print(f"Slides path: {settings.slides_path}")
    print(f"Database: {settings.db_path}")

    # Initialize database lock for multi-user safety
    print("Initializing database lock...")
    db_lock = init_lock(settings.app_data_path)

    # Initialize database (creates session factory)
    print("Initializing database...")
    init_db(settings.db_path)

    # Initialize hasher
    print("Initializing hasher...")
    hasher = SlideHasher(settings.salt_path)

    # Initialize indexer (scans slides/ subdirectory for year folders)
    print("Initializing indexer...")
    indexer = SlideIndexer(hasher, str(settings.slides_path))

    # Build path cache for fast lookups
    print("Building path cache...")
    cache_count = indexer.build_path_cache()
    print(f"Cached {cache_count} slide paths")

    # Auto-run incremental indexing to catch new files
    # Use a dedicated session for startup operations
    print("Running incremental index...")
    startup_db = get_session()
    try:
        index_stats = indexer.build_incremental_index(startup_db)
        startup_db.commit()
        if index_stats['new_slides_indexed'] > 0:
            print(f"Indexed {index_stats['new_slides_indexed']} new slides")
        else:
            print("No new slides found")
    except Exception:
        startup_db.rollback()
        raise
    finally:
        startup_db.close()

    # Initialize cluster service (connection is per-session via UI)
    cluster_service = ClusterService(
        host=settings.CLUSTER_HOST,
        port=settings.CLUSTER_PORT,
    )
    job_poller = JobStatusPoller(
        cluster_service, interval=15,
        indexer=indexer, analyses_path=settings.analyses_path,
    )
    job_poller.start()
    print("Cluster service initialized (connect via UI to enable job submission)")

    print("=" * 60)
    print(f"API ready at http://{settings.HOST}:{settings.PORT}")
    print("=" * 60)

    yield  # Application runs here

    # Cleanup
    if job_poller:
        job_poller.stop()
    if cluster_service:
        cluster_service.disconnect()
    print("Shutting down...")


# Create FastAPI app
app = FastAPI(
    title="Slide Organizer API",
    description="Backend API for organizing and searching pathology slides",
    version="0.1.0",
    lifespan=lifespan
)

# CORS middleware (allows frontend to connect)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Health & Status Endpoints
# ============================================================

@app.get("/")
def root():
    """API root - basic info."""
    return {
        "name": "Slide Organizer API",
        "version": "0.1.0",
        "status": "running"
    }


@app.get("/health")
def health():
    """Health check endpoint."""
    return {
        "status": "ok",
        "network_root": settings.NETWORK_ROOT,
        "network_accessible": os.path.exists(settings.NETWORK_ROOT),
    }


@app.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """Get index statistics."""
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")
    return indexer.get_stats(db)


@app.get("/thumbnails/stats")
def get_thumbnail_stats():
    """Get thumbnail cache statistics."""
    cache_dir = settings.thumbnail_cache_path

    if not cache_dir.exists():
        return {
            "cached_count": 0,
            "total_slides": len(indexer.slide_hash_to_path),
            "cache_size_mb": 0
        }

    # Count cached thumbnails
    cached_files = list(cache_dir.glob("*.jpg"))
    # Exclude label files
    thumbnail_files = [f for f in cached_files if "_label" not in f.name]
    total_size = sum(f.stat().st_size for f in cached_files)

    return {
        "cached_count": len(thumbnail_files),
        "total_slides": len(indexer.slide_hash_to_path),
        "cache_size_mb": round(total_size / (1024 * 1024), 2)
    }


# ============================================================
# Indexing Endpoints
# ============================================================

@app.post("/index/full")
def run_full_index(db: Session = Depends(get_db)):
    """
    Run a full index of all slides.

    This scans the entire network drive and updates the database.
    May take several minutes for large collections.
    """
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")

    print("Starting full index...")
    stats = indexer.build_full_index(db)

    # Rebuild path cache
    cache_count = indexer.build_path_cache()
    stats['cache_rebuilt'] = cache_count

    print(f"Index complete: {stats}")
    return stats


@app.post("/index/incremental")
def run_incremental_index(db: Session = Depends(get_db)):
    """
    Index only new slides that aren't already in the database.

    Much faster than full index - use this to catch newly added files.
    """
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")

    print("Starting incremental index...")
    stats = indexer.build_incremental_index(db)
    print(f"Incremental index complete: {stats['new_slides_indexed']} new slides")
    return stats


@app.post("/index/refresh-cache")
def refresh_cache():
    """Refresh the in-memory path cache without re-indexing database."""
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")

    count = indexer.build_path_cache()
    return {"cached_slides": count}


# ============================================================
# Search Endpoints
# ============================================================

@app.get("/search")
def search_slides(
    db: Session = Depends(get_db),
    q: Optional[str] = Query(None, description="Search query (matches against filename). If empty, returns all slides matching filters."),
    year: Optional[int] = Query(None, description="Filter by year"),
    stain: Optional[str] = Query(None, description="Filter by stain type: HE (exact), IHC (prefix match), Special (not HE or IHC)"),
    tag: Optional[str] = Query(None, description="Filter by tag name"),
    limit: int = Query(500, le=500, description="Maximum results")
):
    """
    Search for slides by accession number or other filename components.

    Supports partial matching (e.g., "S24-123" will match "S24-12345").
    If no query is provided, returns all slides matching the filters (up to limit).
    """
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")

    # Use empty string if no query provided - indexer will return all slides
    search_query = q or ""

    results = indexer.search(
        db=db,
        query=search_query,
        year=year,
        stain_type=stain,
        limit=limit
    )

    # Filter by tag if specified
    if tag:
        tag_obj = db.query(Tag).filter_by(name=tag).first()
        if tag_obj:
            # Get slide hashes that have this tag
            tagged_hashes = set()
            for slide in tag_obj.slides:
                tagged_hashes.add(slide.slide_hash)
            # Filter results to only include slides with this tag
            results = [r for r in results if r.get('slide_hash') in tagged_hashes]
        else:
            # Tag doesn't exist, return empty results
            results = []

    return {
        "query": q,
        "filters": {"year": year, "stain": stain, "tag": tag},
        "count": len(results),
        "truncated": len(results) == limit,
        "results": results
    }


# ============================================================
# Bulk Tag Endpoints (must be before /slides/{slide_hash} routes)
# ============================================================

@app.post("/slides/bulk/tags/add")
def bulk_add_tags(request: BulkTagRequest, db: Session = Depends(get_db)):
    """Add tags to multiple slides at once."""
    updated = []
    not_found = []

    with get_lock().write_lock():
        for slide_hash in request.slide_hashes:
            slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
            if not slide:
                not_found.append(slide_hash)
                continue

            for tag_name in request.tags:
                tag = db.query(Tag).filter_by(name=tag_name).first()
                if not tag:
                    tag = Tag(name=tag_name, color=request.color)
                    db.add(tag)

                if tag not in slide.tags:
                    slide.tags.append(tag)

            updated.append(slide_hash)

        db.commit()  # Commit while holding lock to ensure data is written

    return {
        "status": "ok",
        "updated": len(updated),
        "not_found": not_found
    }


@app.post("/slides/bulk/tags/remove")
def bulk_remove_tags(request: BulkTagRequest, db: Session = Depends(get_db)):
    """Remove tags from multiple slides at once."""
    updated = []
    not_found = []

    with get_lock().write_lock():
        for slide_hash in request.slide_hashes:
            slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
            if not slide:
                not_found.append(slide_hash)
                continue

            for tag_name in request.tags:
                tag = db.query(Tag).filter_by(name=tag_name).first()
                if tag and tag in slide.tags:
                    slide.tags.remove(tag)

            updated.append(slide_hash)

        db.commit()  # Commit while holding lock

    return {
        "status": "ok",
        "updated": len(updated),
        "not_found": not_found
    }


@app.put("/slides/bulk/tags")
def bulk_set_tags(request: BulkTagRequest, db: Session = Depends(get_db)):
    """Replace all tags on multiple slides (removes existing tags first)."""
    updated = []
    not_found = []

    with get_lock().write_lock():
        tags_to_set = []
        for tag_name in request.tags:
            tag = db.query(Tag).filter_by(name=tag_name).first()
            if not tag:
                tag = Tag(name=tag_name)
                db.add(tag)
            tags_to_set.append(tag)

        for slide_hash in request.slide_hashes:
            slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
            if not slide:
                not_found.append(slide_hash)
                continue

            slide.tags = tags_to_set.copy()
            updated.append(slide_hash)

        db.commit()  # Commit while holding lock

    return {
        "status": "ok",
        "updated": len(updated),
        "not_found": not_found
    }


@app.get("/slides/{slide_hash}")
def get_slide(slide_hash: str, db: Session = Depends(get_db)):
    """Get details for a specific slide by its hash."""
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")

    slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
    if not slide:
        raise HTTPException(status_code=404, detail="Slide not found")

    filepath = indexer.get_filepath(slide_hash)

    return {
        "slide_hash": slide.slide_hash,
        "case_hash": slide.case.accession_hash,
        "year": slide.case.year,
        "block_id": slide.block_id,
        "stain_type": slide.stain_type,
        "random_id": slide.random_id,
        "file_exists": bool(slide.file_exists),
        "file_size_bytes": slide.file_size_bytes,
        "filepath_available": filepath is not None,
        "slide_tags": [t.name for t in slide.tags],
        "case_tags": [t.name for t in slide.case.tags],
        "projects": [{"id": p.id, "name": p.name} for p in slide.case.projects],
        "analysis_jobs": [
            {
                "id": js.job.id,
                "model": js.job.model_name,
                "status": js.status,
                "submitted_at": js.job.submitted_at.isoformat() if js.job.submitted_at else None
            }
            for js in slide.job_slides if js.job
        ]
    }


# ============================================================
# Slide Viewer Endpoints (OpenSlide)
# ============================================================

from openslide import open_slide
from openslide.deepzoom import DeepZoomGenerator

# Cache for DeepZoom generators (handles all tile math correctly)
_dz_cache: dict[str, DeepZoomGenerator] = {}

TILE_SIZE = 254
TILE_OVERLAP = 1
TILE_FORMAT = "jpeg"

def _get_dz(slide_hash: str) -> DeepZoomGenerator:
    """Get or create a DeepZoomGenerator for a slide."""
    if slide_hash in _dz_cache:
        return _dz_cache[slide_hash]

    filepath = indexer.get_filepath(slide_hash)
    if not filepath or not filepath.exists():
        raise HTTPException(status_code=404, detail="Slide file not found")

    try:
        slide = open_slide(str(filepath))
        dz = DeepZoomGenerator(slide, tile_size=TILE_SIZE, overlap=TILE_OVERLAP)
        _dz_cache[slide_hash] = dz
        return dz
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open slide: {e}")


def _get_and_cache_embedded_thumbnail(slide_hash: str, filepath: Path) -> bytes:
    """
    Extract and cache the embedded thumbnail from the SVS file.
    Only stores one small file (~20-30KB) per slide.
    Returns JPEG bytes.
    """
    cache_dir = settings.thumbnail_cache_path
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{slide_hash}.jpg"

    try:
        slide = open_slide(str(filepath))

        # Get the embedded thumbnail (these are pre-stored in SVS files)
        thumb = None
        if 'thumbnail' in slide.associated_images:
            thumb = slide.associated_images['thumbnail']
        elif 'macro' in slide.associated_images:
            thumb = slide.associated_images['macro']

        if thumb is None:
            # Fallback: get from lowest pyramid level (rare)
            level = slide.level_count - 1
            dims = slide.level_dimensions[level]
            thumb = slide.read_region((0, 0), level, dims)

        slide.close()

        # Convert and cache
        thumb = thumb.convert('RGB')
        buffer = BytesIO()
        thumb.save(buffer, format='JPEG', quality=85)
        image_bytes = buffer.getvalue()

        cache_file.write_bytes(image_bytes)
        return image_bytes

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to extract thumbnail: {e}")


@app.get("/slides/{slide_hash}/thumbnail.jpeg")
def get_slide_thumbnail(slide_hash: str, max_size: int = Query(1024, le=2048)):
    """
    Get the embedded thumbnail from the slide.
    Caches only the original embedded thumbnail (~20-30KB per slide).
    Resizes on-the-fly if a smaller size is requested.
    """
    from PIL import Image

    cache_dir = settings.thumbnail_cache_path
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{slide_hash}.jpg"

    # Check cache first
    if cache_file.exists():
        image_bytes = cache_file.read_bytes()
    else:
        # Not cached - extract from SVS file
        filepath = indexer.get_filepath(slide_hash)
        if not filepath or not filepath.exists():
            raise HTTPException(status_code=404, detail="Slide file not found")
        image_bytes = _get_and_cache_embedded_thumbnail(slide_hash, filepath)

    # Resize on-the-fly if requested size is smaller than cached
    thumb = Image.open(BytesIO(image_bytes))
    if max(thumb.size) > max_size:
        ratio = max_size / max(thumb.size)
        new_size = (int(thumb.size[0] * ratio), int(thumb.size[1] * ratio))
        thumb = thumb.resize(new_size)
        buffer = BytesIO()
        thumb.save(buffer, format='JPEG', quality=85)
        image_bytes = buffer.getvalue()

    return Response(
        content=image_bytes,
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=86400"}
    )


@app.get("/slides/{slide_hash}/label.jpeg")
def get_slide_label(slide_hash: str, max_size: int = Query(256, le=512)):
    """
    Get the slide label image (the paper label on the physical slide).
    Returns 404 if no label is available.
    """
    # Check cache first
    cache_dir = settings.thumbnail_cache_path
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f"{slide_hash}_label.jpg"

    if cache_file.exists():
        return Response(
            content=cache_file.read_bytes(),
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"}
        )

    filepath = indexer.get_filepath(slide_hash)
    if not filepath or not filepath.exists():
        raise HTTPException(status_code=404, detail="Slide file not found")

    try:
        slide = open_slide(str(filepath))

        # Try to get label image
        label = None
        if 'label' in slide.associated_images:
            label = slide.associated_images['label']

        slide.close()

        if label is None:
            raise HTTPException(status_code=404, detail="No label image available")

        # Resize if needed
        if max(label.size) > max_size:
            ratio = max_size / max(label.size)
            new_size = (int(label.size[0] * ratio), int(label.size[1] * ratio))
            label = label.resize(new_size)

        # Convert and cache
        label = label.convert('RGB')
        buffer = BytesIO()
        label.save(buffer, format='JPEG', quality=90)
        image_bytes = buffer.getvalue()

        cache_file.write_bytes(image_bytes)

        return Response(
            content=image_bytes,
            media_type="image/jpeg",
            headers={"Cache-Control": "public, max-age=86400"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get label: {e}")


@app.get("/slides/{slide_hash}/dzi.json")
def get_slide_dzi(slide_hash: str):
    """
    Get Deep Zoom Image metadata for a slide.
    Returns tile size, format, and dimensions for OpenSeadragon.
    """
    dz = _get_dz(slide_hash)

    width, height = dz.level_dimensions[-1]  # Highest resolution level

    return {
        "Image": {
            "xmlns": "http://schemas.microsoft.com/deepzoom/2008",
            "Format": TILE_FORMAT,
            "Overlap": str(TILE_OVERLAP),
            "TileSize": str(TILE_SIZE),
            "Size": {
                "Width": str(width),
                "Height": str(height)
            }
        }
    }


@app.get("/slides/{slide_hash}/tiles/{level}/{col}_{row}.jpeg")
def get_slide_tile(slide_hash: str, level: int, col: int, row: int):
    """
    Get a single tile from the slide at the specified level and position.
    Uses OpenSlide's DeepZoomGenerator for correct tile calculation.
    """
    dz = _get_dz(slide_hash)

    # Validate level
    if level < 0 or level >= dz.level_count:
        raise HTTPException(status_code=404, detail=f"Invalid level: {level}")

    # Validate tile coordinates
    tiles_x, tiles_y = dz.level_tiles[level]
    if col < 0 or col >= tiles_x or row < 0 or row >= tiles_y:
        raise HTTPException(status_code=404, detail="Tile out of bounds")

    try:
        # Get tile using DeepZoomGenerator (handles all the math)
        tile = dz.get_tile(level, (col, row))

        # Convert RGBA to RGB
        tile = tile.convert('RGB')

        # Encode as JPEG
        buffer = BytesIO()
        tile.save(buffer, format='JPEG', quality=80)
        buffer.seek(0)

        return Response(
            content=buffer.read(),
            media_type="image/jpeg",
            headers={
                "Cache-Control": "public, max-age=86400",  # Cache for 24 hours
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read tile: {e}")


# ============================================================
# Thumbnail Pre-generation
# ============================================================

# ============================================================
# Tag Endpoints
# ============================================================

class TagCreate(BaseModel):
    name: str
    color: Optional[str] = None  # Hex color like "#FF5733"
    category: Optional[str] = None


class TagUpdate(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None
    category: Optional[str] = None


@app.get("/tags")
def list_tags(db: Session = Depends(get_db)):
    """List all tags with usage counts."""
    tags = db.query(Tag).order_by(Tag.name).all()
    return [
        {
            "id": t.id,
            "name": t.name,
            "color": t.color,
            "category": t.category,
            "slide_count": len(t.slides),
            "case_count": len(t.cases)
        }
        for t in tags
    ]


@app.get("/tags/search")
def search_tags(db: Session = Depends(get_db), q: str = Query(..., min_length=1)):
    """Search/autocomplete tags by name prefix."""
    tags = db.query(Tag).filter(
        Tag.name.ilike(f"{q}%")
    ).order_by(Tag.name).limit(10).all()

    return [
        {
            "id": t.id,
            "name": t.name,
            "color": t.color,
            "category": t.category
        }
        for t in tags
    ]


@app.post("/tags")
def create_tag(tag_data: TagCreate, db: Session = Depends(get_db)):
    """Create a new tag."""
    existing = db.query(Tag).filter_by(name=tag_data.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Tag already exists")

    tag = Tag(name=tag_data.name, color=tag_data.color, category=tag_data.category)
    db.add(tag)
    db.commit()

    return {"id": tag.id, "name": tag.name, "color": tag.color, "category": tag.category}


@app.patch("/tags/{tag_id}")
def update_tag(tag_id: int, tag_data: TagUpdate, db: Session = Depends(get_db)):
    """Update a tag's name, color, or category."""
    tag = db.query(Tag).filter_by(id=tag_id).first()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    if tag_data.name is not None:
        # Check for name conflict
        existing = db.query(Tag).filter(Tag.name == tag_data.name, Tag.id != tag_id).first()
        if existing:
            raise HTTPException(status_code=400, detail="Tag name already exists")
        tag.name = tag_data.name

    if tag_data.color is not None:
        tag.color = tag_data.color

    if tag_data.category is not None:
        tag.category = tag_data.category

    db.commit()

    return {"id": tag.id, "name": tag.name, "color": tag.color, "category": tag.category}


@app.delete("/tags/{tag_id}")
def delete_tag(tag_id: int, db: Session = Depends(get_db)):
    """Delete a tag."""
    tag = db.query(Tag).filter_by(id=tag_id).first()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    db.delete(tag)
    db.commit()

    return {"status": "ok"}


@app.get("/tags/{tag_name}/slides")
def get_slides_by_tag(tag_name: str, db: Session = Depends(get_db), limit: int = Query(100, le=500)):
    """Get all slides with a given tag."""
    tag = db.query(Tag).filter_by(name=tag_name).first()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    slides = tag.slides[:limit]

    return {
        "tag": tag_name,
        "color": tag.color,
        "count": len(slides),
        "slides": [
            {
                "slide_hash": s.slide_hash,
                "case_hash": s.case.accession_hash,
                "year": s.case.year,
                "block_id": s.block_id,
                "stain_type": s.stain_type,
                "file_exists": bool(s.file_exists),
            }
            for s in slides
        ]
    }


@app.get("/slides/{slide_hash}/tags")
def get_slide_tags(slide_hash: str, db: Session = Depends(get_db)):
    """Get all tags for a slide."""
    slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
    if not slide:
        raise HTTPException(status_code=404, detail="Slide not found")

    return [
        {
            "id": t.id,
            "name": t.name,
            "color": t.color,
            "category": t.category
        }
        for t in slide.tags
    ]


class AddTagRequest(BaseModel):
    name: str
    color: Optional[str] = None  # Only used if creating new tag


@app.post("/slides/{slide_hash}/tags")
def add_tag_to_slide(slide_hash: str, tag_data: AddTagRequest, db: Session = Depends(get_db)):
    """Add a tag to a slide. Creates the tag if it doesn't exist."""
    slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
    if not slide:
        raise HTTPException(status_code=404, detail="Slide not found")

    with get_lock().write_lock():
        tag = db.query(Tag).filter_by(name=tag_data.name).first()
        if not tag:
            # Auto-create the tag with color
            tag = Tag(name=tag_data.name, color=tag_data.color)
            db.add(tag)
            db.flush()  # Get the ID

        if tag not in slide.tags:
            slide.tags.append(tag)

        db.commit()

    return {
        "status": "ok",
        "tag": {
            "id": tag.id,
            "name": tag.name,
            "color": tag.color,
            "category": tag.category
        }
    }


@app.post("/slides/{slide_hash}/tags/{tag_name}")
def add_tag_to_slide_by_name(slide_hash: str, tag_name: str, db: Session = Depends(get_db)):
    """Add a tag to a slide by name (legacy endpoint)."""
    slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
    if not slide:
        raise HTTPException(status_code=404, detail="Slide not found")

    with get_lock().write_lock():
        tag = db.query(Tag).filter_by(name=tag_name).first()
        if not tag:
            # Auto-create the tag
            tag = Tag(name=tag_name)
            db.add(tag)

        if tag not in slide.tags:
            slide.tags.append(tag)
            db.commit()

    return {"status": "ok", "slide_hash": slide_hash, "tag": tag_name}


@app.delete("/slides/{slide_hash}/tags/{tag_name}")
def remove_tag_from_slide(slide_hash: str, tag_name: str, db: Session = Depends(get_db)):
    """Remove a tag from a slide."""
    slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
    if not slide:
        raise HTTPException(status_code=404, detail="Slide not found")

    with get_lock().write_lock():
        tag = db.query(Tag).filter_by(name=tag_name).first()
        if tag and tag in slide.tags:
            slide.tags.remove(tag)
            db.commit()

    return {"status": "ok"}


# ============================================================
# Project Endpoints
# ============================================================

@app.get("/projects")
def list_projects(db: Session = Depends(get_db)):
    """List all projects."""
    projects = db.query(Project).order_by(Project.created_at.desc()).all()
    return [
        {
            "id": p.id,
            "name": p.name,
            "description": p.description,
            "case_count": p.case_count,
            "slide_count": p.slide_count,
            "created_at": p.created_at.isoformat()
        }
        for p in projects
    ]


@app.post("/projects")
def create_project(db: Session = Depends(get_db), name: str = Query(...), description: Optional[str] = None, created_by: Optional[str] = None):
    """Create a new project."""
    with get_lock().write_lock():
        project = Project(name=name, description=description, created_by=created_by)
        db.add(project)
        db.commit()

    return {"id": project.id, "name": project.name}


@app.get("/projects/{project_id}")
def get_project(project_id: int, db: Session = Depends(get_db)):
    """Get project details including all cases."""
    project = db.query(Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    return {
        "id": project.id,
        "name": project.name,
        "description": project.description,
        "created_by": project.created_by,
        "created_at": project.created_at.isoformat(),
        "case_count": project.case_count,
        "slide_count": project.slide_count,
        "cases": [
            {
                "case_hash": c.accession_hash,
                "year": c.year,
                "slide_count": len(c.slides),
                "tags": [t.name for t in c.tags]
            }
            for c in project.cases
        ]
    }


@app.post("/projects/{project_id}/cases/{case_hash}")
def add_case_to_project(project_id: int, case_hash: str, db: Session = Depends(get_db)):
    """Add a case to a project."""
    project = db.query(Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    case = db.query(Case).filter_by(accession_hash=case_hash).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    with get_lock().write_lock():
        if case not in project.cases:
            project.cases.append(case)
            db.commit()

    return {"status": "ok", "project_id": project_id, "case_hash": case_hash}


# ============================================================
# Cohort Endpoints
# ============================================================

class CohortCreate(BaseModel):
    name: str
    description: Optional[str] = None
    created_by: Optional[str] = None


class CohortFromTag(BaseModel):
    name: str
    description: Optional[str] = None
    tag_name: str
    created_by: Optional[str] = None


class CohortAddSlides(BaseModel):
    slide_hashes: List[str]


@app.get("/cohorts")
def list_cohorts(db: Session = Depends(get_db)):
    """List all cohorts."""
    cohorts = db.query(Cohort).order_by(Cohort.created_at.desc()).all()
    return [
        {
            "id": c.id,
            "name": c.name,
            "description": c.description,
            "source_type": c.source_type,
            "slide_count": c.slide_count,
            "case_count": c.case_count,
            "created_by": c.created_by,
            "created_at": c.created_at.isoformat() if c.created_at else None,
            "updated_at": c.updated_at.isoformat() if c.updated_at else None
        }
        for c in cohorts
    ]


@app.post("/cohorts")
def create_cohort(cohort_data: CohortCreate, db: Session = Depends(get_db)):
    """Create an empty cohort."""
    with get_lock().write_lock():
        cohort = Cohort(
            name=cohort_data.name,
            description=cohort_data.description,
            source_type='manual',
            created_by=cohort_data.created_by
        )
        db.add(cohort)
        db.commit()

    return {
        "id": cohort.id,
        "name": cohort.name,
        "slide_count": 0
    }


@app.get("/cohorts/{cohort_id}")
def get_cohort(cohort_id: int, db: Session = Depends(get_db)):
    """Get cohort details including all slides with enriched data."""
    cohort = db.query(Cohort).options(
        joinedload(Cohort.slides).joinedload(Slide.case),
        joinedload(Cohort.slides).joinedload(Slide.tags),
    ).filter_by(id=cohort_id).first()
    if not cohort:
        raise HTTPException(status_code=404, detail="Cohort not found")

    slides_data = []
    for s in cohort.slides:
        accession_number = None
        slide_number = None
        filepath = indexer.get_filepath(s.slide_hash) if indexer else None
        if filepath:
            parsed = indexer.parser.parse(filepath.name)
            if parsed:
                accession_number = parsed.accession
                slide_number = parsed.slide_number

        slides_data.append({
            "slide_hash": s.slide_hash,
            "accession_number": accession_number,
            "block_id": s.block_id,
            "slide_number": slide_number,
            "stain_type": s.stain_type,
            "random_id": s.random_id,
            "year": s.case.year if s.case else None,
            "case_hash": s.case.accession_hash if s.case else None,
            "tags": [t.name for t in s.tags],
            "file_size_bytes": s.file_size_bytes,
        })

    return {
        "id": cohort.id,
        "name": cohort.name,
        "description": cohort.description,
        "source_type": cohort.source_type,
        "source_details": cohort.source_details,
        "created_by": cohort.created_by,
        "created_at": cohort.created_at.isoformat() if cohort.created_at else None,
        "updated_at": cohort.updated_at.isoformat() if cohort.updated_at else None,
        "slide_count": cohort.slide_count,
        "case_count": cohort.case_count,
        "slides": slides_data
    }


@app.delete("/cohorts/{cohort_id}")
def delete_cohort(cohort_id: int, db: Session = Depends(get_db)):
    """Delete a cohort."""
    cohort = db.query(Cohort).filter_by(id=cohort_id).first()
    if not cohort:
        raise HTTPException(status_code=404, detail="Cohort not found")

    with get_lock().write_lock():
        db.delete(cohort)
        db.commit()

    return {"status": "ok"}


@app.post("/cohorts/{cohort_id}/slides")
def add_slides_to_cohort(cohort_id: int, data: CohortAddSlides, db: Session = Depends(get_db)):
    """Add slides to a cohort."""
    cohort = db.query(Cohort).filter_by(id=cohort_id).first()
    if not cohort:
        raise HTTPException(status_code=404, detail="Cohort not found")

    added = []
    not_found = []

    with get_lock().write_lock():
        for slide_hash in data.slide_hashes:
            slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
            if not slide:
                not_found.append(slide_hash)
                continue

            if slide not in cohort.slides:
                cohort.slides.append(slide)
                added.append(slide_hash)

        db.commit()

    return {
        "status": "ok",
        "added": len(added),
        "added_hashes": added,
        "not_found": not_found,
        "total_slides": cohort.slide_count,
        "total_cases": cohort.case_count
    }


@app.delete("/cohorts/{cohort_id}/slides")
def remove_slides_from_cohort(cohort_id: int, data: CohortAddSlides, db: Session = Depends(get_db)):
    """Remove slides from a cohort."""
    cohort = db.query(Cohort).filter_by(id=cohort_id).first()
    if not cohort:
        raise HTTPException(status_code=404, detail="Cohort not found")

    removed = []

    with get_lock().write_lock():
        for slide_hash in data.slide_hashes:
            slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
            if slide and slide in cohort.slides:
                cohort.slides.remove(slide)
                removed.append(slide_hash)

        db.commit()

    return {
        "status": "ok",
        "removed": len(removed),
        "removed_hashes": removed,
        "total_slides": cohort.slide_count,
        "total_cases": cohort.case_count
    }


class _ZipStreamWriter:
    """File-like object that feeds zip data to a queue in ~1 MB chunks."""

    def __init__(self, q: queue.Queue, chunk_size: int = 1024 * 1024):
        self._q = q
        self._chunk_size = chunk_size
        self._buf = bytearray()
        self._pos = 0

    def write(self, data: bytes) -> int:
        self._buf.extend(data)
        self._pos += len(data)
        while len(self._buf) >= self._chunk_size:
            self._q.put(bytes(self._buf[:self._chunk_size]))
            self._buf = self._buf[self._chunk_size:]
        return len(data)

    def tell(self) -> int:
        return self._pos

    def flush(self):
        if self._buf:
            self._q.put(bytes(self._buf))
            self._buf.clear()

    def close(self):
        self.flush()


@app.get("/cohorts/{cohort_id}/export")
def export_cohort(cohort_id: int, db: Session = Depends(get_db)):
    """Stream cohort slides as a ZIP file, organized by accession number."""
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")

    cohort = db.query(Cohort).options(
        joinedload(Cohort.slides).joinedload(Slide.case),
    ).filter_by(id=cohort_id).first()
    if not cohort:
        raise HTTPException(status_code=404, detail="Cohort not found")

    # Gather (filesystem_path, archive_name) pairs, skip missing files
    slides_info: list[tuple[str, str]] = []
    for s in cohort.slides:
        filepath = indexer.get_filepath(s.slide_hash)
        if not filepath or not filepath.exists():
            continue
        parsed = indexer.parser.parse(filepath.name)
        folder = parsed.accession if parsed else s.slide_hash[:12]
        arcname = f"{folder}/{filepath.name}"
        slides_info.append((str(filepath), arcname))

    if not slides_info:
        raise HTTPException(status_code=404, detail="No slide files available for export")

    def generate():
        q: queue.Queue = queue.Queue(maxsize=32)

        def writer():
            try:
                stream = _ZipStreamWriter(q)
                with zipfile.ZipFile(stream, 'w', zipfile.ZIP_STORED, allowZip64=True) as zf:
                    for filepath, arcname in slides_info:
                        zf.write(filepath, arcname)
                stream.flush()
            except Exception as e:
                print(f"ZIP export error: {e}")
            finally:
                q.put(None)  # sentinel

        t = threading.Thread(target=writer, daemon=True)
        t.start()

        while True:
            chunk = q.get()
            if chunk is None:
                break
            yield chunk

        t.join(timeout=5)

    # Sanitize cohort name for filename
    safe_name = re.sub(r'[^\w\s-]', '', cohort.name).strip().replace(' ', '_') or 'cohort'
    return StreamingResponse(
        generate(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}.zip"',
        },
    )


@app.post("/cohorts/{cohort_id}/export-analyses")
def export_cohort_analyses(
    cohort_id: int,
    analysis_name: Optional[str] = Query(None, description="Filter by analysis name"),
    db: Session = Depends(get_db),
):
    """
    Export post-processed analysis results for a cohort as a ZIP.
    Structure: {accession_number}/{analysis_name}/{files}
    """
    import subprocess
    import tempfile

    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")

    cohort = db.query(Cohort).options(
        joinedload(Cohort.slides).joinedload(Slide.case),
        joinedload(Cohort.slides).joinedload(Slide.job_slides).joinedload(JobSlide.job).joinedload(AnalysisJob.analysis),
    ).filter_by(id=cohort_id).first()
    if not cohort:
        raise HTTPException(status_code=404, detail="Cohort not found")

    # Collect (output_dir, archive_prefix, postprocess_template) for each completed slide analysis
    export_items: list[tuple[Path, str, str | None]] = []

    for slide in cohort.slides:
        # Resolve accession number for folder name
        filepath = indexer.get_filepath(slide.slide_hash)
        if filepath:
            parsed = indexer.parser.parse(filepath.name)
            folder = parsed.accession if parsed else slide.slide_hash[:12]
        else:
            folder = slide.slide_hash[:12]

        for js in slide.job_slides:
            if js.status != "completed":
                continue
            if not js.local_output_path:
                continue
            local_out = Path(js.local_output_path)
            if not local_out.is_dir():
                continue

            job = js.job
            if not job:
                continue

            a_name = job.model_name
            if analysis_name and a_name != analysis_name:
                continue

            pp_template = job.analysis.postprocess_template if job.analysis else None
            arc_prefix = f"{folder}/{a_name}"
            export_items.append((local_out, arc_prefix, pp_template))

    if not export_items:
        raise HTTPException(status_code=404, detail="No completed analysis results available for export")

    def generate():
        q: queue.Queue = queue.Queue(maxsize=32)

        def writer():
            try:
                stream = _ZipStreamWriter(q)
                with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
                    for output_dir, arc_prefix, pp_template in export_items:
                        if pp_template:
                            # Run postprocess into a temp dir
                            with tempfile.TemporaryDirectory() as tmpdir:
                                cmd = pp_template.format(
                                    input_dir=str(output_dir),
                                    output_dir=tmpdir,
                                    filename_stem=output_dir.name,
                                )
                                try:
                                    subprocess.run(cmd, shell=True, check=True, timeout=300)
                                    src_dir = Path(tmpdir)
                                except Exception as e:
                                    print(f"Postprocess failed for {arc_prefix}: {e}, using raw files")
                                    src_dir = output_dir

                                for f in sorted(src_dir.iterdir()):
                                    if f.is_file():
                                        zf.write(str(f), f"{arc_prefix}/{f.name}")
                        else:
                            # No postprocessing — copy raw files
                            for f in sorted(output_dir.iterdir()):
                                if f.is_file():
                                    zf.write(str(f), f"{arc_prefix}/{f.name}")
                stream.flush()
            except Exception as e:
                print(f"Analysis export error: {e}")
            finally:
                q.put(None)

        t = threading.Thread(target=writer, daemon=True)
        t.start()

        while True:
            chunk = q.get()
            if chunk is None:
                break
            yield chunk

        t.join(timeout=5)

    safe_name = re.sub(r'[^\w\s-]', '', cohort.name).strip().replace(' ', '_') or 'cohort'
    return StreamingResponse(
        generate(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_name}_analyses.zip"',
        },
    )


@app.post("/cohorts/from-tag")
def create_cohort_from_tag(data: CohortFromTag, db: Session = Depends(get_db)):
    """Create a cohort from all slides with a specific tag."""
    tag = db.query(Tag).filter_by(name=data.tag_name).first()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    with get_lock().write_lock():
        cohort = Cohort(
            name=data.name,
            description=data.description,
            source_type='tag',
            source_details=f'{{"tag": "{data.tag_name}"}}',
            created_by=data.created_by
        )
        cohort.slides = list(tag.slides)
        db.add(cohort)
        db.commit()

    return {
        "id": cohort.id,
        "name": cohort.name,
        "slide_count": cohort.slide_count,
        "case_count": cohort.case_count
    }


from fastapi import UploadFile, File


@app.post("/cohorts/from-file")
async def create_cohort_from_file(
    name: str,
    file: UploadFile = File(...),
    description: Optional[str] = None,
    created_by: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Create a cohort by uploading a file with accession numbers or a manifest.
    Supports .txt (one per line), .csv, .xlsx.

    Single column: matches all slides for each accession number.
    Three columns (accession, block, stain): matches specific slides by all three fields.
    """
    # Read file content (strip BOM if present)
    content = await file.read()
    if content.startswith(b'\xef\xbb\xbf'):
        content = content[3:]
    filename = file.filename or ""

    # Parse rows — each row is a list of column values
    raw_rows = []

    if filename.endswith('.xlsx'):
        try:
            import openpyxl
            from io import BytesIO
            wb = openpyxl.load_workbook(BytesIO(content), read_only=True)
            ws = wb.active
            for row in ws.iter_rows(values_only=True):
                cells = [str(c).strip() if c is not None else "" for c in row]
                if any(cells):
                    raw_rows.append(cells)
        except ImportError:
            raise HTTPException(status_code=400, detail="openpyxl not installed for Excel support")
    elif filename.endswith('.csv'):
        import csv
        from io import StringIO
        text = content.decode('utf-8')
        reader = csv.reader(StringIO(text))
        for row in reader:
            cells = [c.strip() for c in row]
            if any(cells):
                raw_rows.append(cells)
    else:
        # Text file — split each line by tab, comma, or whitespace
        import re as _re
        text = content.decode('utf-8')
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if '\t' in line:
                cells = [c.strip() for c in line.split('\t')]
            elif ',' in line:
                cells = [c.strip() for c in line.split(',')]
            else:
                # Split by whitespace (handles multiple spaces)
                cells = _re.split(r'\s+', line)
            raw_rows.append(cells)

    if not raw_rows:
        raise HTTPException(status_code=400, detail="No data found in file")

    # Detect mode: manifest (3 cols) vs accession list (1 col)
    is_manifest = len(raw_rows[0]) >= 3 and all(len(r) >= 3 for r in raw_rows)

    import re as _re2

    def normalize_accession(acc: str) -> str:
        """Normalize BS-?YY- prefix so BS18- and BS-18- both match."""
        return _re2.sub(r'^BS-?', 'BS', acc.upper())

    matching_slides = []
    matching_slide_ids = set()  # track by id to avoid duplicates

    if is_manifest:
        # Manifest mode: each row is (accession, block, stain)
        manifest_rows = [(r[0], r[1], r[2]) for r in raw_rows]
        rows_matched = []
        rows_not_matched = []

        for acc, block, stain in manifest_rows:
            acc_norm = normalize_accession(acc)
            block_upper = block.upper()
            stain_lower = stain.lower()
            found = False
            near_misses = []

            for slide_hash, filepath in indexer.slide_hash_to_path.items():
                parsed = indexer.parser.parse(filepath.name)
                if not parsed:
                    continue
                if normalize_accession(parsed.accession) == acc_norm:
                    # Accession matched — check block + stain
                    if (parsed.block_id.upper() == block_upper
                            and parsed.stain_type.lower() == stain_lower):
                        slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
                        if slide and slide.id not in matching_slide_ids:
                            matching_slides.append(slide)
                            matching_slide_ids.add(slide.id)
                        found = True
                    else:
                        near_misses.append(f"block={parsed.block_id},stain={parsed.stain_type}")

            if found:
                rows_matched.append(f"{acc},{block},{stain}")
            else:
                detail = f"{acc},{block},{stain}"
                if near_misses:
                    detail += f" (accession found but slides have: {'; '.join(near_misses[:5])})"
                rows_not_matched.append(detail)

        # Create cohort
        with get_lock().write_lock():
            cohort = Cohort(
                name=name,
                description=description,
                source_type='upload',
                source_details=json.dumps({
                    "filename": filename,
                    "mode": "manifest",
                    "rows_requested": len(manifest_rows),
                    "rows_matched": len(rows_matched),
                    "rows_not_matched": rows_not_matched[:50]
                }),
                created_by=created_by
            )
            cohort.slides = matching_slides
            db.add(cohort)
            db.commit()

        return {
            "id": cohort.id,
            "name": cohort.name,
            "slide_count": cohort.slide_count,
            "case_count": cohort.case_count,
            "rows_requested": len(manifest_rows),
            "rows_matched": len(rows_matched),
            "rows_not_matched": rows_not_matched
        }
    else:
        # Accession list mode (original behavior)
        accessions = [r[0] for r in raw_rows if r[0]]

        if not accessions:
            raise HTTPException(status_code=400, detail="No accession numbers found in file")

        found_accessions = set()
        not_found_accessions = []

        for accession in accessions:
            acc_norm = normalize_accession(accession)
            found = False

            for slide_hash, filepath in indexer.slide_hash_to_path.items():
                parsed = indexer.parser.parse(filepath.name)
                if parsed and normalize_accession(parsed.accession) == acc_norm:
                    slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
                    if slide and slide.id not in matching_slide_ids:
                        matching_slides.append(slide)
                        matching_slide_ids.add(slide.id)
                        found_accessions.add(acc_norm)
                        found = True

            if not found and acc_norm not in found_accessions:
                not_found_accessions.append(accession)

        # Create cohort
        with get_lock().write_lock():
            cohort = Cohort(
                name=name,
                description=description,
                source_type='upload',
                source_details=json.dumps({
                    "filename": filename,
                    "mode": "accession_list",
                    "accessions_requested": len(accessions),
                    "accessions_found": len(found_accessions),
                    "accessions_not_found": not_found_accessions[:50]
                }),
                created_by=created_by
            )
            cohort.slides = matching_slides
            db.add(cohort)
            db.commit()

        return {
            "id": cohort.id,
            "name": cohort.name,
            "slide_count": cohort.slide_count,
            "case_count": cohort.case_count,
            "accessions_requested": len(accessions),
            "accessions_found": len(found_accessions),
            "accessions_not_found": not_found_accessions
        }


@app.post("/cohorts/from-filter")
def create_cohort_from_filter(
    db: Session = Depends(get_db),
    name: str = Query(...),
    description: Optional[str] = None,
    created_by: Optional[str] = None,
    query: Optional[str] = None,
    year: Optional[int] = None,
    stain: Optional[str] = None,
    tag: Optional[str] = None
):
    """Create a cohort from slides matching filter criteria."""
    # Use the search function to find matching slides
    results = indexer.search(
        db=db,
        query=query or "",
        year=year,
        stain_type=stain,
        tags=[tag] if tag else None,
        limit=10000  # Higher limit for cohort building
    )

    if not results:
        raise HTTPException(status_code=400, detail="No slides match the filter criteria")

    # Get slide objects
    slide_hashes = [r['slide_hash'] for r in results]
    slides = db.query(Slide).filter(Slide.slide_hash.in_(slide_hashes)).all()

    # Create cohort
    with get_lock().write_lock():
        cohort = Cohort(
            name=name,
            description=description,
            source_type='filter',
            source_details=json.dumps({
                "query": query,
                "year": year,
                "stain": stain,
                "tag": tag
            }),
            created_by=created_by
        )
        cohort.slides = slides
        db.add(cohort)
        db.commit()

    return {
        "id": cohort.id,
        "name": cohort.name,
        "slide_count": cohort.slide_count,
        "case_count": cohort.case_count
    }


# ============================================================
# Analysis Registry Endpoints
# ============================================================

class AnalysisCreate(BaseModel):
    name: str
    version: str = '1.0'
    description: Optional[str] = None
    script_path: Optional[str] = None
    working_directory: Optional[str] = None
    env_setup: Optional[str] = None
    command_template: Optional[str] = None
    postprocess_template: Optional[str] = None
    parameters_schema: Optional[str] = None
    default_parameters: Optional[str] = None
    gpu_required: bool = True
    estimated_runtime_minutes: int = 60


class AnalysisUpdate(BaseModel):
    name: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None
    script_path: Optional[str] = None
    working_directory: Optional[str] = None
    env_setup: Optional[str] = None
    command_template: Optional[str] = None
    postprocess_template: Optional[str] = None
    parameters_schema: Optional[str] = None
    default_parameters: Optional[str] = None
    gpu_required: Optional[bool] = None
    estimated_runtime_minutes: Optional[int] = None
    active: Optional[bool] = None


@app.get("/analyses")
def list_analyses(
    db: Session = Depends(get_db),
    active_only: bool = Query(False, description="Only return active analyses"),
):
    """List all registered analyses."""
    query = db.query(Analysis)
    if active_only:
        query = query.filter(Analysis.active == True)
    analyses = query.order_by(Analysis.name).all()
    return [
        {
            "id": a.id,
            "name": a.name,
            "version": a.version,
            "description": a.description,
            "script_path": a.script_path,
            "working_directory": a.working_directory,
            "env_setup": a.env_setup,
            "command_template": a.command_template,
            "postprocess_template": a.postprocess_template,
            "parameters_schema": a.parameters_schema,
            "default_parameters": a.default_parameters,
            "gpu_required": a.gpu_required,
            "estimated_runtime_minutes": a.estimated_runtime_minutes,
            "active": a.active,
            "created_at": a.created_at.isoformat() if a.created_at else None,
            "job_count": len(a.jobs),
        }
        for a in analyses
    ]


@app.post("/analyses")
def create_analysis(data: AnalysisCreate, db: Session = Depends(get_db)):
    """Register a new analysis pipeline."""
    existing = db.query(Analysis).filter_by(name=data.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Analysis with this name already exists")

    analysis = Analysis(
        name=data.name,
        version=data.version,
        description=data.description,
        script_path=data.script_path,
        working_directory=data.working_directory,
        env_setup=data.env_setup,
        command_template=data.command_template,
        postprocess_template=data.postprocess_template,
        parameters_schema=data.parameters_schema,
        default_parameters=data.default_parameters,
        gpu_required=data.gpu_required,
        estimated_runtime_minutes=data.estimated_runtime_minutes,
    )
    db.add(analysis)
    db.commit()

    return {
        "id": analysis.id,
        "name": analysis.name,
        "version": analysis.version,
    }


@app.get("/analyses/{analysis_id}")
def get_analysis(analysis_id: int, db: Session = Depends(get_db)):
    """Get a single analysis by ID."""
    analysis = db.query(Analysis).filter_by(id=analysis_id).first()
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")

    return {
        "id": analysis.id,
        "name": analysis.name,
        "version": analysis.version,
        "description": analysis.description,
        "script_path": analysis.script_path,
        "working_directory": analysis.working_directory,
        "env_setup": analysis.env_setup,
        "command_template": analysis.command_template,
        "postprocess_template": analysis.postprocess_template,
        "parameters_schema": analysis.parameters_schema,
        "default_parameters": analysis.default_parameters,
        "gpu_required": analysis.gpu_required,
        "estimated_runtime_minutes": analysis.estimated_runtime_minutes,
        "active": analysis.active,
        "created_at": analysis.created_at.isoformat() if analysis.created_at else None,
        "job_count": len(analysis.jobs),
    }


@app.patch("/analyses/{analysis_id}")
def update_analysis(analysis_id: int, data: AnalysisUpdate, db: Session = Depends(get_db)):
    """Update an analysis pipeline."""
    analysis = db.query(Analysis).filter_by(id=analysis_id).first()
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")

    if data.name is not None:
        existing = db.query(Analysis).filter(Analysis.name == data.name, Analysis.id != analysis_id).first()
        if existing:
            raise HTTPException(status_code=400, detail="Analysis name already exists")
        analysis.name = data.name
    if data.version is not None:
        analysis.version = data.version
    if data.description is not None:
        analysis.description = data.description
    if data.script_path is not None:
        analysis.script_path = data.script_path
    if data.working_directory is not None:
        analysis.working_directory = data.working_directory
    if data.env_setup is not None:
        analysis.env_setup = data.env_setup
    if data.command_template is not None:
        analysis.command_template = data.command_template
    if data.postprocess_template is not None:
        analysis.postprocess_template = data.postprocess_template
    if data.parameters_schema is not None:
        analysis.parameters_schema = data.parameters_schema
    if data.default_parameters is not None:
        analysis.default_parameters = data.default_parameters
    if data.gpu_required is not None:
        analysis.gpu_required = data.gpu_required
    if data.estimated_runtime_minutes is not None:
        analysis.estimated_runtime_minutes = data.estimated_runtime_minutes
    if data.active is not None:
        analysis.active = data.active

    db.commit()

    return {
        "id": analysis.id,
        "name": analysis.name,
        "version": analysis.version,
        "active": analysis.active,
    }


@app.delete("/analyses/{analysis_id}")
def delete_analysis(analysis_id: int, db: Session = Depends(get_db)):
    """Soft-delete an analysis (sets active=False)."""
    analysis = db.query(Analysis).filter_by(id=analysis_id).first()
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found")

    analysis.active = False
    db.commit()

    return {"status": "ok", "id": analysis_id, "active": False}


# ============================================================
# Job Submission & Management Endpoints
# ============================================================

class JobSubmitRequest(BaseModel):
    analysis_id: int
    slide_hashes: List[str]
    gpu_index: int = 0
    remote_wsi_dir: str = "/tmp/slidecap_wsi"
    remote_output_dir: str = "/tmp/slidecap_output"
    parameters: Optional[str] = None
    submitted_by: Optional[str] = None


class CohortJobSubmitRequest(BaseModel):
    analysis_id: int
    gpu_index: int = 0
    remote_wsi_dir: str = "/tmp/slidecap_wsi"
    remote_output_dir: str = "/tmp/slidecap_output"
    parameters: Optional[str] = None
    submitted_by: Optional[str] = None


class JobCancelRequest(BaseModel):
    job_ids: List[int]


@app.post("/jobs/submit")
def submit_jobs(data: JobSubmitRequest, db: Session = Depends(get_db)):
    """
    Submit a multi-slide analysis job.

    Creates DB records immediately (pending), then spawns a background thread
    to do rsync + tmux start. This avoids holding the DB lock during long transfers.
    """
    if not cluster_service or not cluster_service.is_connected:
        raise HTTPException(status_code=503, detail="Not connected to cluster. Connect first via /cluster/connect")

    analysis = db.query(Analysis).filter_by(id=data.analysis_id, active=True).first()
    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis not found or inactive")

    params = None
    if data.parameters:
        try:
            params = json.loads(data.parameters)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in parameters")

    # Snapshot analysis config for the background thread (detached from session)
    analysis_snapshot = {
        "id": analysis.id,
        "name": analysis.name,
        "version": analysis.version,
        "script_path": analysis.script_path,
        "working_directory": analysis.working_directory,
        "env_setup": analysis.env_setup,
        "command_template": analysis.command_template,
        "gpu_required": analysis.gpu_required,
    }

    # --- Phase 1: Create all DB records and commit (fast, releases DB) ---
    job = AnalysisJob(
        analysis_id=analysis.id,
        model_name=analysis.name,
        model_version=analysis.version,
        parameters=data.parameters,
        gpu_index=data.gpu_index,
        remote_wsi_dir=data.remote_wsi_dir,
        remote_output_dir=data.remote_output_dir,
        output_path=str(settings.analyses_path),
        status="pending",
        submitted_by=data.submitted_by,
    )
    db.add(job)
    db.flush()

    errors = []
    slides_created = 0
    old_records_cleaned = 0

    # List of (job_slide_id, slide_hash, local_path, remote_wsi_path, remote_out) for bg thread
    slides_to_process: list[tuple[int, str, str, str, str]] = []

    for slide_hash in data.slide_hashes:
        slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
        if not slide:
            errors.append(f"Slide not found: {slide_hash[:12]}")
            continue

        slide_path = indexer.get_filepath(slide_hash) if indexer else None
        if not slide_path:
            errors.append(f"No filepath for {slide_hash[:12]}")
            continue

        # Clean up old completed/failed JobSlide records for same analysis
        old_job_slides = (
            db.query(JobSlide)
            .join(AnalysisJob)
            .filter(
                JobSlide.slide_id == slide.id,
                AnalysisJob.analysis_id == analysis.id,
                JobSlide.status.in_(["completed", "failed"]),
            )
            .all()
        )
        for old_js in old_job_slides:
            db.delete(old_js)
            old_records_cleaned += 1
            remaining = db.query(JobSlide).filter(
                JobSlide.job_id == old_js.job_id, JobSlide.id != old_js.id
            ).count()
            if remaining == 0:
                old_job = db.query(AnalysisJob).filter_by(id=old_js.job_id).first()
                if old_job:
                    db.delete(old_job)

        remote_wsi_path = f"{data.remote_wsi_dir}/{slide.slide_hash}/{slide_path.name}"
        remote_out = f"{data.remote_output_dir}/{slide.slide_hash}/{analysis.name}_v{analysis.version}"

        job_slide = JobSlide(
            job_id=job.id,
            slide_id=slide.id,
            remote_wsi_path=remote_wsi_path,
            remote_output_path=remote_out,
            status="pending",
        )
        db.add(job_slide)
        db.flush()

        slides_to_process.append((job_slide.id, slide_hash, str(slide_path), remote_wsi_path, remote_out))
        slides_created += 1

    db.commit()  # Commit and release DB immediately

    # --- Phase 2: Background thread for rsync + tmux start ---
    job_id = job.id
    gpu_index = data.gpu_index
    remote_wsi_dir = data.remote_wsi_dir

    def _run_submissions():
        # --- Phase A: Transfer ALL slides first ---
        transfer_ok: list[tuple[int, str, str, str]] = []  # (js_id, slide_hash, remote_wsi_path, remote_out)

        for i, (js_id, slide_hash, local_path_str, remote_wsi_path, remote_out) in enumerate(slides_to_process):
            bg_db = get_session()
            try:
                js = bg_db.query(JobSlide).filter_by(id=js_id).first()
                if not js:
                    continue

                local_path = Path(local_path_str)
                if not local_path.exists():
                    js.status = "failed"
                    js.error_message = f"Local file not found: {local_path}"
                    bg_db.commit()
                    continue

                js.status = "transferring"
                bg_db.commit()

                # rsync to per-slide subdirectory so CellViT only sees this one slide
                per_slide_wsi_dir = str(Path(remote_wsi_path).parent)
                print(f"[Job {job_id}/Transfer {i+1}/{len(slides_to_process)}] Rsyncing {local_path.name} ({local_path.stat().st_size / 1e6:.0f} MB)")
                cluster_service.rsync_slide(local_path, per_slide_wsi_dir)
                print(f"[Job {job_id}/Transfer {i+1}/{len(slides_to_process)}] Done: {local_path.name}")
                transfer_ok.append((js_id, slide_hash, remote_wsi_path, remote_out))

            except Exception as e:
                import traceback
                traceback.print_exc()
                try:
                    js = bg_db.query(JobSlide).filter_by(id=js_id).first()
                    if js:
                        js.status = "failed"
                        js.error_message = f"Transfer failed: {e}"
                        bg_db.commit()
                except Exception:
                    bg_db.rollback()
            finally:
                bg_db.close()

        def _recompute_and_commit():
            _db = get_session()
            try:
                parent = _db.query(AnalysisJob).options(
                    joinedload(AnalysisJob.slides)
                ).filter_by(id=job_id).first()
                if parent:
                    _recompute_job_status(parent)
                    _db.commit()
            except Exception:
                _db.rollback()
            finally:
                _db.close()

        if not transfer_ok:
            print(f"[Job {job_id}] All transfers failed.")
            _recompute_and_commit()
            return

        print(f"[Job {job_id}] All transfers complete ({len(transfer_ok)}/{len(slides_to_process)}). Starting analysis...")

        # --- Phase B: Start all jobs (now that all slides are on the cluster) ---
        for js_id, slide_hash, remote_wsi_path, remote_out in transfer_ok:
            bg_db = get_session()
            try:
                js = bg_db.query(JobSlide).filter_by(id=js_id).first()
                if not js:
                    continue

                _analysis = bg_db.query(Analysis).filter_by(id=analysis_snapshot["id"]).first()
                if not _analysis:
                    js.status = "failed"
                    js.error_message = "Analysis not found"
                    bg_db.commit()
                    continue

                session_name = cluster_service.start_job(
                    analysis=_analysis,
                    slide_hash=slide_hash,
                    remote_wsi_path=remote_wsi_path,
                    remote_output_dir=remote_out,
                    gpu_index=gpu_index,
                    parameters=params,
                )
                js.cluster_job_id = session_name
                js.status = "running"
                js.started_at = datetime.utcnow()
                bg_db.commit()
                print(f"[Job {job_id}/Slide {js_id}] Started tmux session: {session_name}")

            except Exception as e:
                import traceback
                traceback.print_exc()
                try:
                    js = bg_db.query(JobSlide).filter_by(id=js_id).first()
                    if js:
                        js.status = "failed"
                        js.error_message = f"Job start failed: {e}"
                        bg_db.commit()
                except Exception:
                    bg_db.rollback()
            finally:
                bg_db.close()

        _recompute_and_commit()

    t = threading.Thread(target=_run_submissions, daemon=True)
    t.start()

    return {
        "job_id": job_id,
        "slides_created": slides_created,
        "old_records_cleaned": old_records_cleaned,
        "errors": errors,
        "cluster_connected": cluster_service.is_connected,
    }


def _recompute_job_status(job: AnalysisJob):
    """Derive parent job status from its child JobSlides."""
    if not job.slides:
        return
    statuses = [js.status for js in job.slides]
    if any(s in ("running", "transferring") for s in statuses):
        job.status = "running"
        if not job.started_at:
            job.started_at = datetime.utcnow()
    elif all(s == "completed" for s in statuses):
        job.status = "completed"
        job.completed_at = datetime.utcnow()
    elif any(s == "failed" for s in statuses) and not any(s in ("running", "transferring", "pending") for s in statuses):
        job.status = "failed"
        job.completed_at = datetime.utcnow()
    elif all(s == "pending" for s in statuses):
        job.status = "pending"


@app.post("/jobs/submit-cohort/{cohort_id}")
def submit_cohort_jobs(cohort_id: int, data: CohortJobSubmitRequest, db: Session = Depends(get_db)):
    """Submit a multi-slide analysis job for all slides in a cohort."""
    cohort = db.query(Cohort).filter_by(id=cohort_id).first()
    if not cohort:
        raise HTTPException(status_code=404, detail="Cohort not found")

    slide_hashes = [s.slide_hash for s in cohort.slides]
    submit_data = JobSubmitRequest(
        analysis_id=data.analysis_id,
        slide_hashes=slide_hashes,
        gpu_index=data.gpu_index,
        remote_wsi_dir=data.remote_wsi_dir,
        remote_output_dir=data.remote_output_dir,
        parameters=data.parameters,
        submitted_by=data.submitted_by,
    )
    return submit_jobs(submit_data, db)


@app.get("/jobs")
def list_jobs(
    db: Session = Depends(get_db),
    status: Optional[str] = Query(None),
    analysis_id: Optional[int] = Query(None),
    slide_hashes: Optional[str] = Query(None, description="Comma-separated slide hashes to filter jobs containing these slides"),
    limit: int = Query(200, le=1000),
):
    """List analysis jobs with slide progress counts."""
    query = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides).joinedload(JobSlide.slide)
    )
    if status:
        query = query.filter(AnalysisJob.status == status)
    if analysis_id:
        query = query.filter(AnalysisJob.analysis_id == analysis_id)

    jobs = query.order_by(AnalysisJob.submitted_at.desc()).limit(limit).all()

    # Filter to only jobs that contain at least one of the requested slides
    if slide_hashes:
        wanted = set(h.strip() for h in slide_hashes.split(",") if h.strip())
        jobs = [
            j for j in jobs
            if any(js.slide and js.slide.slide_hash in wanted for js in j.slides)
        ]

    def _job_dict(j):
        slide_count = len(j.slides)
        completed_count = sum(1 for js in j.slides if js.status == "completed")
        failed_count = sum(1 for js in j.slides if js.status == "failed")
        return {
            "id": j.id,
            "analysis_id": j.analysis_id,
            "model_name": j.model_name,
            "model_version": j.model_version,
            "parameters": j.parameters,
            "gpu_index": j.gpu_index,
            "status": j.status,
            "submitted_by": j.submitted_by,
            "submitted_at": j.submitted_at.isoformat() if j.submitted_at else None,
            "started_at": j.started_at.isoformat() if j.started_at else None,
            "completed_at": j.completed_at.isoformat() if j.completed_at else None,
            "error_message": j.error_message,
            "slide_count": slide_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
        }

    return [_job_dict(j) for j in jobs]


@app.get("/jobs/{job_id}")
def get_job(job_id: int, db: Session = Depends(get_db)):
    """Get a single job with nested slides detail."""
    job = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides).joinedload(JobSlide.slide)
    ).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    slide_count = len(job.slides)
    completed_count = sum(1 for js in job.slides if js.status == "completed")
    failed_count = sum(1 for js in job.slides if js.status == "failed")

    return {
        "id": job.id,
        "analysis_id": job.analysis_id,
        "model_name": job.model_name,
        "model_version": job.model_version,
        "parameters": job.parameters,
        "gpu_index": job.gpu_index,
        "status": job.status,
        "submitted_by": job.submitted_by,
        "submitted_at": job.submitted_at.isoformat() if job.submitted_at else None,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "error_message": job.error_message,
        "slide_count": slide_count,
        "completed_count": completed_count,
        "failed_count": failed_count,
        "slides": [
            {
                "id": js.id,
                "slide_hash": js.slide.slide_hash if js.slide else None,
                "cluster_job_id": js.cluster_job_id,
                "status": js.status,
                "started_at": js.started_at.isoformat() if js.started_at else None,
                "completed_at": js.completed_at.isoformat() if js.completed_at else None,
                "error_message": js.error_message,
                "log_tail": js.log_tail,
                "remote_output_path": js.remote_output_path,
            }
            for js in job.slides
        ],
    }


@app.get("/jobs/{job_id}/log")
def get_job_log(
    job_id: int,
    slide_id: Optional[int] = Query(None, description="Optional JobSlide ID for specific slide log"),
    db: Session = Depends(get_db),
):
    """Fetch log from the cluster for a job or specific slide within it."""
    job = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides)
    ).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # If slide_id specified, get that specific slide's log
    if slide_id is not None:
        job_slide = next((js for js in job.slides if js.id == slide_id), None)
        if not job_slide:
            raise HTTPException(status_code=404, detail="JobSlide not found in this job")
        slides_to_check = [job_slide]
    else:
        slides_to_check = job.slides

    if not cluster_service or not cluster_service.is_connected:
        logs = []
        for js in slides_to_check:
            logs.append({"slide_id": js.id, "log": js.log_tail or "(not connected)", "source": "cached"})
        return {"job_id": job_id, "slides": logs}

    results = []
    for js in slides_to_check:
        remote_out = js.remote_output_path
        if not remote_out:
            results.append({"slide_id": js.id, "log": "(no remote output path)", "source": "none"})
            continue
        try:
            stdout, stderr, exit_code = cluster_service.run_command(f"cat {remote_out}/run.log 2>&1")
            _, _, tmux_code = cluster_service.run_command(
                f"tmux has-session -t {js.cluster_job_id} 2>/dev/null" if js.cluster_job_id else "false"
            )
            results.append({
                "slide_id": js.id,
                "log": stdout if exit_code == 0 else f"(no run.log found)\nstderr: {stderr}",
                "tmux_alive": tmux_code == 0,
                "cluster_job_id": js.cluster_job_id,
                "source": "live",
            })
        except Exception as e:
            results.append({"slide_id": js.id, "log": f"Error fetching log: {e}", "source": "error"})

    return {"job_id": job_id, "slides": results}


@app.post("/jobs/cancel")
def cancel_jobs(data: JobCancelRequest, db: Session = Depends(get_db)):
    """Cancel jobs by their IDs. Cancels all active child slides."""
    cancelled = 0
    errors = []

    for job_id in data.job_ids:
        job = db.query(AnalysisJob).options(
            joinedload(AnalysisJob.slides)
        ).filter_by(id=job_id).first()
        if not job:
            errors.append(f"Job {job_id} not found")
            continue

        if job.status not in ("queued", "running", "pending", "transferring"):
            errors.append(f"Job {job_id} is already {job.status}")
            continue

        # Cancel all active child slides
        for js in job.slides:
            if js.status in ("pending", "transferring", "running", "queued"):
                if cluster_service and cluster_service.is_connected and js.cluster_job_id:
                    try:
                        cluster_service.cancel_job(js.cluster_job_id)
                    except Exception:
                        pass
                js.status = "failed"
                js.error_message = "Cancelled by user"
                js.completed_at = datetime.utcnow()

        job.status = "failed"
        job.error_message = "Cancelled by user"
        job.completed_at = datetime.utcnow()
        cancelled += 1

    db.commit()

    return {"cancelled": cancelled, "errors": errors}


@app.delete("/jobs/{job_id}")
def delete_job(job_id: int, db: Session = Depends(get_db)):
    """Hard-delete a job and all its JobSlide records from the database."""
    job = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides)
    ).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in ("running", "transferring"):
        raise HTTPException(status_code=400, detail="Cannot delete a running job. Cancel it first.")

    with get_lock().write_lock():
        # Delete child JobSlides first (cascade should handle this, but be explicit)
        for js in job.slides:
            db.delete(js)
        db.delete(job)
        db.commit()

    return {"status": "ok", "deleted_job_id": job_id}


class JobDeleteBulkRequest(BaseModel):
    job_ids: List[int]


@app.post("/jobs/delete-bulk")
def delete_jobs_bulk(data: JobDeleteBulkRequest, db: Session = Depends(get_db)):
    """Hard-delete multiple jobs and their JobSlide records."""
    deleted = 0
    errors = []

    with get_lock().write_lock():
        for job_id in data.job_ids:
            job = db.query(AnalysisJob).options(
                joinedload(AnalysisJob.slides)
            ).filter_by(id=job_id).first()
            if not job:
                errors.append(f"Job {job_id} not found")
                continue
            if job.status in ("running", "transferring"):
                errors.append(f"Job {job_id} is still {job.status}")
                continue
            for js in job.slides:
                db.delete(js)
            db.delete(job)
            deleted += 1

        db.commit()

    return {"deleted": deleted, "errors": errors}


@app.post("/jobs/refresh")
def refresh_job_statuses():
    """Trigger an immediate job status refresh from the cluster."""
    if not job_poller:
        raise HTTPException(status_code=503, detail="Job poller not running")
    if not cluster_service or not cluster_service.is_connected:
        raise HTTPException(status_code=503, detail="Not connected to cluster")

    job_poller.poll_now()
    return {"status": "ok", "message": "Status refresh triggered"}


@app.post("/jobs/{job_id}/transfer-results")
def transfer_job_results(job_id: int, db: Session = Depends(get_db)):
    """Manually trigger result transfer from cluster for completed slides in a job."""
    if not job_poller:
        raise HTTPException(status_code=503, detail="Job poller not running")
    if not cluster_service or not cluster_service.is_connected:
        raise HTTPException(status_code=503, detail="Not connected to cluster")

    job = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides).joinedload(JobSlide.slide)
    ).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    transferred = 0
    errors = []
    for js in job.slides:
        if js.status != "completed":
            continue
        if js.local_output_path:
            local_dir = Path(js.local_output_path)
            if local_dir.exists() and any(local_dir.iterdir()):
                # Already transferred and files exist
                continue
        try:
            local_path = job_poller._transfer_results(js)
            if local_path:
                js.local_output_path = local_path
                transferred += 1
            else:
                errors.append(f"Slide {js.id}: transfer returned None")
        except Exception as e:
            errors.append(f"Slide {js.id}: {e}")

    db.commit()
    return {"transferred": transferred, "errors": errors}


# ============================================================
# Cluster Connection Endpoints
# ============================================================

class ClusterConnectRequest(BaseModel):
    host: str
    port: int = 22
    username: str
    password: str


@app.post("/cluster/connect")
def cluster_connect(data: ClusterConnectRequest):
    """Connect to the GPU cluster via SSH."""
    if not cluster_service:
        raise HTTPException(status_code=503, detail="Cluster service not initialized")

    try:
        result = cluster_service.connect(data.host, data.port, data.username, data.password)
        # Immediately query GPU status
        try:
            gpus = cluster_service.get_gpu_status()
            result["gpus"] = gpus
        except Exception as e:
            result["gpus"] = []
            result["gpu_error"] = str(e)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/cluster/disconnect")
def cluster_disconnect():
    """Disconnect from the GPU cluster."""
    if cluster_service:
        cluster_service.disconnect()
    return {"status": "ok", "connected": False}


@app.get("/cluster/status")
def cluster_status():
    """Get cluster connection status."""
    if not cluster_service:
        return {"connected": False}

    info = cluster_service.connection_info
    if info["connected"]:
        try:
            info["gpus"] = cluster_service.get_gpu_status()
        except Exception:
            info["gpus"] = []
    return info


@app.get("/cluster/gpus")
def cluster_gpus():
    """Get current GPU status from the cluster."""
    if not cluster_service or not cluster_service.is_connected:
        raise HTTPException(status_code=503, detail="Not connected to cluster")

    try:
        return cluster_service.get_gpu_status()
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to query GPUs: {e}")


# ============================================================
# Seed / Pre-configuration Endpoints
# ============================================================

@app.post("/analyses/seed-cellvit")
def seed_cellvit(db: Session = Depends(get_db)):
    """Pre-populate registry with the CellViT analysis pipeline."""
    existing = db.query(Analysis).filter_by(name="CellViT").first()
    if existing:
        return {"status": "already_exists", "id": existing.id}

    # Resolve postprocess script path relative to this file
    scripts_dir = Path(__file__).resolve().parent.parent / "scripts"
    postprocess_cmd = f"python {scripts_dir / 'postprocess_cellvit.py'} --input-dir {{input_dir}} --output-dir {{output_dir}}"

    analysis = Analysis(
        name="CellViT",
        version="SAM-H-x40",
        description="Cell detection and segmentation using CellViT with SAM-H backbone at 40x magnification",
        script_path="/ligonlab/Prem/CellViT_CCNU/CellViT-plus-plus/run_cellvit_resume.sh",
        working_directory="/ligonlab/Prem/CellViT_CCNU/CellViT-plus-plus",
        env_setup="source cellvit_env/bin/activate && export TMPDIR=/ligonlab/Prem/CellViT_CCNU/tmp && export RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1",
        command_template="./run_cellvit_resume.sh {wsi_dir} {outdir} ./checkpoints/CellViT-SAM-H-x40-AMP.pth {gpu} {batch_size}",
        postprocess_template=postprocess_cmd,
        default_parameters='{"batch_size": 4}',
        gpu_required=True,
        estimated_runtime_minutes=120,
    )
    db.add(analysis)
    db.commit()

    return {"status": "created", "id": analysis.id, "name": analysis.name}


# ============================================================
# Results Endpoints
# ============================================================

@app.get("/results/search")
def search_results(
    q: str = Query(..., min_length=1, description="Accession number search query"),
    db: Session = Depends(get_db),
):
    """
    Search for analysis results by accession number.
    Returns slides matching the query with their completed analyses.
    """
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")

    query_upper = q.strip().upper()

    # Find matching slides via the path cache (same pattern as /search)
    matching: list[dict] = []
    seen_hashes: set[str] = set()

    for slide_hash, filepath in indexer.slide_hash_to_path.items():
        parsed = indexer.parser.parse(filepath.name)
        if not parsed:
            continue
        if query_upper not in parsed.accession.upper():
            continue
        if slide_hash in seen_hashes:
            continue
        seen_hashes.add(slide_hash)

        slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
        if not slide:
            continue

        # Get completed analyses for this slide
        completed = (
            db.query(JobSlide)
            .join(AnalysisJob)
            .filter(
                JobSlide.slide_id == slide.id,
                JobSlide.status == "completed",
            )
            .order_by(JobSlide.completed_at.desc())
            .all()
        )

        if not completed:
            continue

        matching.append({
            "slide_hash": slide.slide_hash,
            "accession_number": parsed.accession,
            "block_id": slide.block_id,
            "stain_type": slide.stain_type,
            "year": slide.case.year if slide.case else None,
            "results": [
                {
                    "job_id": js.job_id,
                    "job_slide_id": js.id,
                    "analysis_name": js.job.model_name,
                    "version": js.job.model_version or "",
                    "status": js.status,
                    "completed_at": js.completed_at.isoformat() if js.completed_at else None,
                    "output_path": js.local_output_path or js.remote_output_path,
                }
                for js in completed
            ],
        })

    return {"query": q, "count": len(matching), "results": matching}


@app.get("/slides/{slide_hash}/results")
def get_slide_results(slide_hash: str, db: Session = Depends(get_db)):
    """List completed analysis results for a slide."""
    slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
    if not slide:
        raise HTTPException(status_code=404, detail="Slide not found")

    completed_job_slides = (
        db.query(JobSlide)
        .join(AnalysisJob)
        .filter(
            JobSlide.slide_id == slide.id,
            JobSlide.status == "completed",
        )
        .order_by(JobSlide.completed_at.desc())
        .all()
    )

    return [
        {
            "job_id": js.job_id,
            "job_slide_id": js.id,
            "analysis_name": js.job.model_name,
            "version": js.job.model_version or "",
            "status": js.status,
            "completed_at": js.completed_at.isoformat() if js.completed_at else None,
            "output_path": js.local_output_path or js.remote_output_path,
        }
        for js in completed_job_slides
    ]


@app.get("/results/{job_id}/files")
def list_result_files(
    job_id: int,
    slide_hash: Optional[str] = Query(None, description="Slide hash to get per-slide output"),
    db: Session = Depends(get_db),
):
    """List output files for a completed job. Use slide_hash for per-slide results."""
    job = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides).joinedload(JobSlide.slide)
    ).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Determine output directory (prefer local path if available)
    output_dir = None
    if slide_hash:
        js = next((js for js in job.slides if js.slide and js.slide.slide_hash == slide_hash), None)
        if js:
            if js.local_output_path:
                output_dir = Path(js.local_output_path)
            elif js.remote_output_path:
                output_dir = Path(js.remote_output_path)
    elif job.output_path:
        output_dir = Path(job.output_path)

    if not output_dir or not output_dir.exists():
        return []

    IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp", ".gif", ".svg"}

    files = []
    for f in sorted(output_dir.iterdir()):
        if f.is_file():
            files.append({
                "name": f.name,
                "size": f.stat().st_size,
                "is_image": f.suffix.lower() in IMAGE_EXTS,
            })

    return files


from fastapi.responses import FileResponse

import snappy as snappy_module
import sys as _sys
_scripts_dir = str(Path(__file__).resolve().parent.parent / "scripts")
if _scripts_dir not in _sys.path:
    _sys.path.insert(0, _scripts_dir)
from postprocess_cellvit import fix_geometry


def _decompress_snappy_file(file_path: Path) -> tuple[bytes, str]:
    """Decompress a .snappy file and optionally fix geometries.
    Returns (content_bytes, download_filename)."""
    compressed = file_path.read_bytes()
    raw = snappy_module.decompress(compressed)
    download_name = file_path.name

    if file_path.name.endswith(".geojson.snappy"):
        # Decompress + fix geometry
        raw = fix_geometry(raw)
        download_name = file_path.name.removesuffix(".snappy")
    elif file_path.name.endswith(".snappy"):
        # Decompress only (e.g. .json.snappy)
        download_name = file_path.name.removesuffix(".snappy")

    return raw, download_name


@app.get("/results/{job_id}/file/{filename}")
def get_result_file(
    job_id: int,
    filename: str,
    slide_hash: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Serve a single result file for download or preview. Decompresses .snappy on-the-fly."""
    job = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides).joinedload(JobSlide.slide)
    ).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Determine output directory (prefer local path if available)
    output_dir = None
    if slide_hash:
        js = next((js for js in job.slides if js.slide and js.slide.slide_hash == slide_hash), None)
        if js:
            if js.local_output_path:
                output_dir = Path(js.local_output_path)
            elif js.remote_output_path:
                output_dir = Path(js.remote_output_path)
    elif job.output_path:
        output_dir = Path(job.output_path)

    if not output_dir:
        raise HTTPException(status_code=404, detail="No output path for this job")

    # Prevent path traversal
    safe_name = Path(filename).name
    file_path = output_dir / safe_name

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    # Decompress .snappy files on-the-fly
    if safe_name.endswith(".snappy"):
        content, download_name = _decompress_snappy_file(file_path)
        media_type = "application/geo+json" if download_name.endswith(".geojson") else "application/octet-stream"
        return Response(
            content=content,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{download_name}"'},
        )

    return FileResponse(str(file_path), filename=safe_name)


def _resolve_job_slide_output(js) -> Optional[Path]:
    """Get the output directory for a JobSlide, if it exists locally."""
    for attr in ("local_output_path", "remote_output_path"):
        val = getattr(js, attr, None)
        if val:
            p = Path(val)
            if p.is_dir():
                return p
    return None


def _add_files_to_zip(zf: zipfile.ZipFile, output_dir: Path, arc_prefix: str):
    """Add all files from output_dir to the ZIP, decompressing .snappy on-the-fly."""
    for f in sorted(output_dir.iterdir()):
        if not f.is_file():
            continue
        if f.name.endswith(".snappy"):
            content, download_name = _decompress_snappy_file(f)
            zf.writestr(f"{arc_prefix}/{download_name}", content)
        else:
            zf.write(str(f), f"{arc_prefix}/{f.name}")


@app.get("/jobs/{job_id}/download-zip")
def download_job_zip(job_id: int, db: Session = Depends(get_db)):
    """Stream a ZIP of all completed slides' output files for a job, decompressing .snappy."""
    job = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides).joinedload(JobSlide.slide)
    ).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Collect slides with local output
    slides_with_output: list[tuple[str, Path]] = []
    for js in job.slides:
        output_dir = _resolve_job_slide_output(js)
        if output_dir and js.slide:
            slides_with_output.append((js.slide.slide_hash, output_dir))

    if not slides_with_output:
        raise HTTPException(status_code=404, detail="No local output files available")

    def generate():
        q: queue.Queue = queue.Queue(maxsize=32)

        def writer():
            try:
                stream = _ZipStreamWriter(q)
                with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
                    for slide_hash, output_dir in slides_with_output:
                        _add_files_to_zip(zf, output_dir, slide_hash[:12])
                stream.flush()
            except Exception as e:
                print(f"Job ZIP export error: {e}")
            finally:
                q.put(None)

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        while True:
            chunk = q.get()
            if chunk is None:
                break
            yield chunk
        t.join(timeout=5)

    return StreamingResponse(
        generate(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="job_{job_id}_results.zip"',
        },
    )


class CartItem(BaseModel):
    job_id: int
    slide_hash: str
    filename: str


class CartDownloadRequest(BaseModel):
    items: List[CartItem]


@app.post("/results/download-cart")
def download_cart(data: CartDownloadRequest, db: Session = Depends(get_db)):
    """Stream a ZIP of selected files from the download cart, decompressing .snappy."""
    if not data.items:
        raise HTTPException(status_code=400, detail="Cart is empty")

    # Resolve each item to a file path
    resolved: list[tuple[Path, str]] = []  # (file_path, arcname)
    for item in data.items:
        job = db.query(AnalysisJob).options(
            joinedload(AnalysisJob.slides).joinedload(JobSlide.slide)
        ).filter_by(id=item.job_id).first()
        if not job:
            continue

        js = next((js for js in job.slides if js.slide and js.slide.slide_hash == item.slide_hash), None)
        if not js:
            continue

        output_dir = _resolve_job_slide_output(js)
        if not output_dir:
            continue

        safe_name = Path(item.filename).name
        file_path = output_dir / safe_name
        if not file_path.exists() or not file_path.is_file():
            continue

        arc_prefix = item.slide_hash[:12]
        resolved.append((file_path, f"{arc_prefix}/{safe_name}"))

    if not resolved:
        raise HTTPException(status_code=404, detail="No files found for the selected items")

    def generate():
        q: queue.Queue = queue.Queue(maxsize=32)

        def writer():
            try:
                stream = _ZipStreamWriter(q)
                with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
                    for file_path, arcname in resolved:
                        if file_path.name.endswith(".snappy"):
                            content, download_name = _decompress_snappy_file(file_path)
                            # Replace the .snappy arcname with decompressed name
                            arc_dir = str(Path(arcname).parent)
                            zf.writestr(f"{arc_dir}/{download_name}", content)
                        else:
                            zf.write(str(file_path), arcname)
                stream.flush()
            except Exception as e:
                print(f"Cart ZIP export error: {e}")
            finally:
                q.put(None)

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        while True:
            chunk = q.get()
            if chunk is None:
                break
            yield chunk
        t.join(timeout=5)

    return StreamingResponse(
        generate(),
        media_type="application/zip",
        headers={
            "Content-Disposition": 'attachment; filename="selected_results.zip"',
        },
    )


@app.get("/jobs/{job_id}/output-filenames")
def get_job_output_filenames(
    job_id: int,
    slide_hashes: Optional[str] = Query(None, description="Comma-separated slide hashes to restrict to"),
    db: Session = Depends(get_db),
):
    """Return the unique set of output filenames for specific slides in a job."""
    job = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides).joinedload(JobSlide.slide)
    ).filter_by(id=job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    wanted = None
    if slide_hashes:
        wanted = set(h.strip() for h in slide_hashes.split(",") if h.strip())

    filenames: set[str] = set()
    for js in job.slides:
        if wanted and (not js.slide or js.slide.slide_hash not in wanted):
            continue
        output_dir = _resolve_job_slide_output(js)
        if not output_dir:
            continue
        for f in output_dir.iterdir():
            if f.is_file():
                filenames.add(f.name)

    return sorted(filenames)


class DownloadBundleRequest(BaseModel):
    slide_hashes: List[str]
    job_id: int
    include_filenames: List[str]
    include_wsi: bool = False


@app.post("/download-bundle")
def download_bundle(data: DownloadBundleRequest, db: Session = Depends(get_db)):
    """
    Stream a ZIP with selected output files (decompressed) and optionally the
    original WSI for each requested slide.

    ZIP structure: {accession_or_hash}/{filename}
    """
    job = db.query(AnalysisJob).options(
        joinedload(AnalysisJob.slides).joinedload(JobSlide.slide).joinedload(Slide.case)
    ).filter_by(id=data.job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    requested = set(data.slide_hashes)
    include_set = set(data.include_filenames)

    # Resolve per-slide: (folder_name, output_dir, [wsi_path])
    items: list[tuple[str, Optional[Path], Optional[Path]]] = []
    for js in job.slides:
        if not js.slide or js.slide.slide_hash not in requested:
            continue

        slide_hash = js.slide.slide_hash
        output_dir = _resolve_job_slide_output(js)

        # Resolve folder name (accession if possible, else hash prefix)
        wsi_path = None
        folder = slide_hash[:12]
        if indexer:
            fp = indexer.get_filepath(slide_hash)
            if fp:
                parsed = indexer.parser.parse(fp.name)
                if parsed:
                    folder = parsed.accession
                if data.include_wsi and fp.exists():
                    wsi_path = fp

        items.append((folder, output_dir, wsi_path))

    if not items:
        raise HTTPException(status_code=404, detail="No matching slides found")

    def generate():
        q: queue.Queue = queue.Queue(maxsize=32)

        def writer():
            try:
                stream = _ZipStreamWriter(q)
                with zipfile.ZipFile(stream, 'w', zipfile.ZIP_STORED, allowZip64=True) as zf:
                    for folder, output_dir, wsi_path in items:
                        # Add selected output files
                        if output_dir:
                            for f in sorted(output_dir.iterdir()):
                                if not f.is_file() or f.name not in include_set:
                                    continue
                                if f.name.endswith(".snappy"):
                                    content, dl_name = _decompress_snappy_file(f)
                                    zf.writestr(f"{folder}/{dl_name}", content)
                                else:
                                    zf.write(str(f), f"{folder}/{f.name}")
                        # Add WSI
                        if wsi_path:
                            zf.write(str(wsi_path), f"{folder}/{wsi_path.name}")
                stream.flush()
            except Exception as e:
                print(f"Bundle ZIP error: {e}")
            finally:
                q.put(None)

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        while True:
            chunk = q.get()
            if chunk is None:
                break
            yield chunk
        t.join(timeout=5)

    return StreamingResponse(
        generate(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="job_{data.job_id}_bundle.zip"',
        },
    )


# ============================================================
# Run with uvicorn
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=True  # Auto-reload on code changes
    )
