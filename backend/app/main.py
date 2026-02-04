from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from io import BytesIO
import os

from .config import settings
from .db import init_db, Case, Slide, Tag, Project, Cohort, init_lock, get_lock
from .services import SlideHasher, SlideIndexer


# Request models for bulk operations
class BulkTagRequest(BaseModel):
    slide_hashes: List[str]
    tags: List[str]
    color: Optional[str] = None  # Color for new tags


# Global instances (initialized on startup)
db_session = None
hasher = None
indexer = None
db_lock = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_session, hasher, indexer, db_lock

    print("=" * 60)
    print("Starting Slide Organizer API")
    print("=" * 60)

    # Validate configuration
    if not os.path.exists(settings.NETWORK_ROOT):
        print(f"ERROR: Network root does not exist: {settings.NETWORK_ROOT}")
        print("Please update NETWORK_ROOT in config.py or set via environment variable")
        raise RuntimeError(f"Network root not found: {settings.NETWORK_ROOT}")

    print(f"Network root: {settings.NETWORK_ROOT}")
    print(f"Database: {settings.db_path}")

    # Initialize database lock for multi-user safety
    print("Initializing database lock...")
    db_lock = init_lock(settings.app_data_path)

    # Initialize database
    print("Initializing database...")
    db_session = init_db(settings.db_path)
    
    # Initialize hasher
    print("Initializing hasher...")
    hasher = SlideHasher(settings.salt_path)
    
    # Initialize indexer
    print("Initializing indexer...")
    indexer = SlideIndexer(db_session, hasher, settings.NETWORK_ROOT)
    
    # Build path cache for fast lookups
    print("Building path cache...")
    cache_count = indexer.build_path_cache()
    print(f"Cached {cache_count} slide paths")

    # Auto-run incremental indexing to catch new files
    print("Running incremental index...")
    index_stats = indexer.build_incremental_index()
    if index_stats['new_slides_indexed'] > 0:
        print(f"Indexed {index_stats['new_slides_indexed']} new slides")
    else:
        print("No new slides found")

    print("=" * 60)
    print(f"API ready at http://{settings.HOST}:{settings.PORT}")
    print("=" * 60)

    yield  # Application runs here
    
    # Cleanup
    print("Shutting down...")
    if db_session:
        db_session.close()


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


# Session cleanup middleware - prevents "prepared state" errors
@app.middleware("http")
async def db_session_cleanup(request, call_next):
    """Clean up database session after each request to prevent stale state."""
    response = await call_next(request)
    # Always rollback and expire after request to ensure clean state
    try:
        if db_session is not None:
            db_session.rollback()
            db_session.expire_all()
    except Exception:
        pass  # Ignore cleanup errors
    return response


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
def get_stats():
    """Get index statistics."""
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")
    return indexer.get_stats()


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
def run_full_index():
    """
    Run a full index of all slides.
    
    This scans the entire network drive and updates the database.
    May take several minutes for large collections.
    """
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")
    
    print("Starting full index...")
    stats = indexer.build_full_index()
    
    # Rebuild path cache
    cache_count = indexer.build_path_cache()
    stats['cache_rebuilt'] = cache_count
    
    print(f"Index complete: {stats}")
    return stats


@app.post("/index/incremental")
def run_incremental_index():
    """
    Index only new slides that aren't already in the database.

    Much faster than full index - use this to catch newly added files.
    """
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")

    print("Starting incremental index...")
    stats = indexer.build_incremental_index()
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
    q: str = Query(..., description="Search query (matches against filename)"),
    year: Optional[int] = Query(None, description="Filter by year"),
    stain: Optional[str] = Query(None, description="Filter by stain type: HE (exact), IHC (prefix match), Special (not HE or IHC)"),
    limit: int = Query(100, le=500, description="Maximum results")
):
    """
    Search for slides by accession number or other filename components.
    
    Supports partial matching (e.g., "S24-123" will match "S24-12345").
    """
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")
    
    results = indexer.search(
        query=q,
        year=year,
        stain_type=stain,
        limit=limit
    )
    
    return {
        "query": q,
        "filters": {"year": year, "stain": stain},
        "count": len(results),
        "results": results
    }


# ============================================================
# Bulk Tag Endpoints (must be before /slides/{slide_hash} routes)
# ============================================================

@app.post("/slides/bulk/tags/add")
def bulk_add_tags(request: BulkTagRequest):
    """Add tags to multiple slides at once."""
    try:
        db_session.rollback()
        updated = []
        not_found = []

        with get_lock().write_lock():
            for slide_hash in request.slide_hashes:
                slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
                if not slide:
                    not_found.append(slide_hash)
                    continue

                for tag_name in request.tags:
                    tag = db_session.query(Tag).filter_by(name=tag_name).first()
                    if not tag:
                        tag = Tag(name=tag_name, color=request.color)
                        db_session.add(tag)

                    if tag not in slide.tags:
                        slide.tags.append(tag)

                updated.append(slide_hash)

            db_session.commit()

        return {
            "status": "ok",
            "updated": len(updated),
            "not_found": not_found
        }
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.post("/slides/bulk/tags/remove")
def bulk_remove_tags(request: BulkTagRequest):
    """Remove tags from multiple slides at once."""
    updated = []
    not_found = []

    with get_lock().write_lock():
        for slide_hash in request.slide_hashes:
            slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
            if not slide:
                not_found.append(slide_hash)
                continue

            for tag_name in request.tags:
                tag = db_session.query(Tag).filter_by(name=tag_name).first()
                if tag and tag in slide.tags:
                    slide.tags.remove(tag)

            updated.append(slide_hash)

        db_session.commit()

    return {
        "status": "ok",
        "updated": len(updated),
        "not_found": not_found
    }


@app.put("/slides/bulk/tags")
def bulk_set_tags(request: BulkTagRequest):
    """Replace all tags on multiple slides (removes existing tags first)."""
    updated = []
    not_found = []

    with get_lock().write_lock():
        tags_to_set = []
        for tag_name in request.tags:
            tag = db_session.query(Tag).filter_by(name=tag_name).first()
            if not tag:
                tag = Tag(name=tag_name)
                db_session.add(tag)
            tags_to_set.append(tag)

        for slide_hash in request.slide_hashes:
            slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
            if not slide:
                not_found.append(slide_hash)
                continue

            slide.tags = tags_to_set.copy()
            updated.append(slide_hash)

        db_session.commit()

    return {
        "status": "ok",
        "updated": len(updated),
        "not_found": not_found
    }


@app.get("/slides/{slide_hash}")
def get_slide(slide_hash: str):
    """Get details for a specific slide by its hash."""
    if not indexer:
        raise HTTPException(status_code=503, detail="Indexer not initialized")
    
    slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
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
                "id": j.id,
                "model": j.model_name,
                "status": j.status,
                "submitted_at": j.submitted_at.isoformat() if j.submitted_at else None
            }
            for j in slide.analysis_jobs
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
def list_tags():
    """List all tags with usage counts."""
    tags = db_session.query(Tag).order_by(Tag.name).all()
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
def search_tags(q: str = Query(..., min_length=1)):
    """Search/autocomplete tags by name prefix."""
    try:
        db_session.rollback()
        db_session.expire_all()

        tags = db_session.query(Tag).filter(
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
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.post("/tags")
def create_tag(tag_data: TagCreate):
    """Create a new tag."""
    existing = db_session.query(Tag).filter_by(name=tag_data.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Tag already exists")

    tag = Tag(name=tag_data.name, color=tag_data.color, category=tag_data.category)
    db_session.add(tag)
    db_session.commit()

    return {"id": tag.id, "name": tag.name, "color": tag.color, "category": tag.category}


@app.patch("/tags/{tag_id}")
def update_tag(tag_id: int, tag_data: TagUpdate):
    """Update a tag's name, color, or category."""
    tag = db_session.query(Tag).filter_by(id=tag_id).first()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    if tag_data.name is not None:
        # Check for name conflict
        existing = db_session.query(Tag).filter(Tag.name == tag_data.name, Tag.id != tag_id).first()
        if existing:
            raise HTTPException(status_code=400, detail="Tag name already exists")
        tag.name = tag_data.name

    if tag_data.color is not None:
        tag.color = tag_data.color

    if tag_data.category is not None:
        tag.category = tag_data.category

    db_session.commit()

    return {"id": tag.id, "name": tag.name, "color": tag.color, "category": tag.category}


@app.delete("/tags/{tag_id}")
def delete_tag(tag_id: int):
    """Delete a tag."""
    tag = db_session.query(Tag).filter_by(id=tag_id).first()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    db_session.delete(tag)
    db_session.commit()

    return {"status": "ok"}


@app.get("/tags/{tag_name}/slides")
def get_slides_by_tag(tag_name: str, limit: int = Query(100, le=500)):
    """Get all slides with a given tag."""
    tag = db_session.query(Tag).filter_by(name=tag_name).first()
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
def get_slide_tags(slide_hash: str):
    """Get all tags for a slide."""
    try:
        db_session.rollback()
        db_session.expire_all()  # Clear cached data to get fresh results

        slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
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
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


class AddTagRequest(BaseModel):
    name: str
    color: Optional[str] = None  # Only used if creating new tag


@app.post("/slides/{slide_hash}/tags")
def add_tag_to_slide(slide_hash: str, tag_data: AddTagRequest):
    """Add a tag to a slide. Creates the tag if it doesn't exist."""
    try:
        # Rollback any pending transaction first
        db_session.rollback()

        slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
        if not slide:
            raise HTTPException(status_code=404, detail="Slide not found")

        with get_lock().write_lock():
            tag = db_session.query(Tag).filter_by(name=tag_data.name).first()
            if not tag:
                # Auto-create the tag with color
                tag = Tag(name=tag_data.name, color=tag_data.color)
                db_session.add(tag)
                db_session.flush()  # Get the ID

            if tag not in slide.tags:
                slide.tags.append(tag)

            db_session.commit()

        return {
            "status": "ok",
            "tag": {
                "id": tag.id,
                "name": tag.name,
                "color": tag.color,
                "category": tag.category
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.post("/slides/{slide_hash}/tags/{tag_name}")
def add_tag_to_slide_by_name(slide_hash: str, tag_name: str):
    """Add a tag to a slide by name (legacy endpoint)."""
    slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
    if not slide:
        raise HTTPException(status_code=404, detail="Slide not found")

    with get_lock().write_lock():
        tag = db_session.query(Tag).filter_by(name=tag_name).first()
        if not tag:
            # Auto-create the tag
            tag = Tag(name=tag_name)
            db_session.add(tag)

        if tag not in slide.tags:
            slide.tags.append(tag)
            db_session.commit()

    return {"status": "ok", "slide_hash": slide_hash, "tag": tag_name}


@app.delete("/slides/{slide_hash}/tags/{tag_name}")
def remove_tag_from_slide(slide_hash: str, tag_name: str):
    """Remove a tag from a slide."""
    try:
        db_session.rollback()

        slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
        if not slide:
            raise HTTPException(status_code=404, detail="Slide not found")

        with get_lock().write_lock():
            tag = db_session.query(Tag).filter_by(name=tag_name).first()
            if tag and tag in slide.tags:
                slide.tags.remove(tag)
                db_session.commit()

        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================================
# Project Endpoints
# ============================================================

@app.get("/projects")
def list_projects():
    """List all projects."""
    projects = db_session.query(Project).order_by(Project.created_at.desc()).all()
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
def create_project(name: str, description: Optional[str] = None, created_by: Optional[str] = None):
    """Create a new project."""
    with get_lock().write_lock():
        project = Project(name=name, description=description, created_by=created_by)
        db_session.add(project)
        db_session.commit()

    return {"id": project.id, "name": project.name}


@app.get("/projects/{project_id}")
def get_project(project_id: int):
    """Get project details including all cases."""
    project = db_session.query(Project).filter_by(id=project_id).first()
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
def add_case_to_project(project_id: int, case_hash: str):
    """Add a case to a project."""
    project = db_session.query(Project).filter_by(id=project_id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    case = db_session.query(Case).filter_by(accession_hash=case_hash).first()
    if not case:
        raise HTTPException(status_code=404, detail="Case not found")

    with get_lock().write_lock():
        if case not in project.cases:
            project.cases.append(case)
            db_session.commit()

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
def list_cohorts():
    """List all cohorts."""
    try:
        db_session.rollback()
        db_session.expire_all()
        cohorts = db_session.query(Cohort).order_by(Cohort.created_at.desc()).all()
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
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.post("/cohorts")
def create_cohort(cohort_data: CohortCreate):
    """Create an empty cohort."""
    try:
        db_session.rollback()
        with get_lock().write_lock():
            cohort = Cohort(
                name=cohort_data.name,
                description=cohort_data.description,
                source_type='manual',
                created_by=cohort_data.created_by
            )
            db_session.add(cohort)
            db_session.commit()

        return {
            "id": cohort.id,
            "name": cohort.name,
            "slide_count": 0
        }
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/cohorts/{cohort_id}")
def get_cohort(cohort_id: int):
    """Get cohort details including all slides."""
    try:
        db_session.rollback()
        db_session.expire_all()

        cohort = db_session.query(Cohort).filter_by(id=cohort_id).first()
        if not cohort:
            raise HTTPException(status_code=404, detail="Cohort not found")

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
            "slides": [
                {
                    "slide_hash": s.slide_hash,
                    "block_id": s.block_id,
                    "stain_type": s.stain_type,
                    "year": s.case.year if s.case else None,
                    "tags": [t.name for t in s.tags]
                }
                for s in cohort.slides
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.delete("/cohorts/{cohort_id}")
def delete_cohort(cohort_id: int):
    """Delete a cohort."""
    try:
        db_session.rollback()
        cohort = db_session.query(Cohort).filter_by(id=cohort_id).first()
        if not cohort:
            raise HTTPException(status_code=404, detail="Cohort not found")

        with get_lock().write_lock():
            db_session.delete(cohort)
            db_session.commit()

        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.post("/cohorts/{cohort_id}/slides")
def add_slides_to_cohort(cohort_id: int, data: CohortAddSlides):
    """Add slides to a cohort."""
    try:
        db_session.rollback()
        cohort = db_session.query(Cohort).filter_by(id=cohort_id).first()
        if not cohort:
            raise HTTPException(status_code=404, detail="Cohort not found")

        added = []
        not_found = []

        with get_lock().write_lock():
            for slide_hash in data.slide_hashes:
                slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
                if not slide:
                    not_found.append(slide_hash)
                    continue

                if slide not in cohort.slides:
                    cohort.slides.append(slide)
                    added.append(slide_hash)

            db_session.commit()

        return {
            "status": "ok",
            "added": len(added),
            "not_found": not_found,
            "total_slides": cohort.slide_count
        }
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.delete("/cohorts/{cohort_id}/slides")
def remove_slides_from_cohort(cohort_id: int, data: CohortAddSlides):
    """Remove slides from a cohort."""
    try:
        db_session.rollback()
        cohort = db_session.query(Cohort).filter_by(id=cohort_id).first()
        if not cohort:
            raise HTTPException(status_code=404, detail="Cohort not found")

        removed = []

        with get_lock().write_lock():
            for slide_hash in data.slide_hashes:
                slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
                if slide and slide in cohort.slides:
                    cohort.slides.remove(slide)
                    removed.append(slide_hash)

            db_session.commit()

        return {
            "status": "ok",
            "removed": len(removed),
            "total_slides": cohort.slide_count
        }
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.post("/cohorts/from-tag")
def create_cohort_from_tag(data: CohortFromTag):
    """Create a cohort from all slides with a specific tag."""
    try:
        db_session.rollback()
        tag = db_session.query(Tag).filter_by(name=data.tag_name).first()
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
            db_session.add(cohort)
            db_session.commit()

        return {
            "id": cohort.id,
            "name": cohort.name,
            "slide_count": cohort.slide_count,
            "case_count": cohort.case_count
        }
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


from fastapi import UploadFile, File
import json


@app.post("/cohorts/from-file")
async def create_cohort_from_file(
    name: str,
    file: UploadFile = File(...),
    description: Optional[str] = None,
    created_by: Optional[str] = None
):
    """
    Create a cohort by uploading a file with accession numbers.
    Supports .txt (one per line), .csv (first column), .xlsx (first column).
    """
    try:
        db_session.rollback()

        # Read file content
        content = await file.read()
        filename = file.filename or ""

        accessions = []

        if filename.endswith('.xlsx'):
            # Excel file
            try:
                import openpyxl
                from io import BytesIO
                wb = openpyxl.load_workbook(BytesIO(content), read_only=True)
                ws = wb.active
                for row in ws.iter_rows(min_row=1, max_col=1, values_only=True):
                    if row[0]:
                        accessions.append(str(row[0]).strip())
            except ImportError:
                raise HTTPException(status_code=400, detail="openpyxl not installed for Excel support")
        elif filename.endswith('.csv'):
            # CSV file
            import csv
            from io import StringIO
            text = content.decode('utf-8')
            reader = csv.reader(StringIO(text))
            for row in reader:
                if row and row[0].strip():
                    accessions.append(row[0].strip())
        else:
            # Assume text file (one accession per line)
            text = content.decode('utf-8')
            accessions = [line.strip() for line in text.splitlines() if line.strip()]

        if not accessions:
            raise HTTPException(status_code=400, detail="No accession numbers found in file")

        # Find slides matching these accessions
        # We need to search the path cache since accession numbers are hashed
        matching_slides = []
        found_accessions = set()
        not_found_accessions = []

        for accession in accessions:
            accession_upper = accession.upper()
            found = False

            for slide_hash, filepath in indexer.slide_hash_to_path.items():
                parsed = indexer.parser.parse(filepath.name)
                if parsed and parsed.accession.upper() == accession_upper:
                    slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
                    if slide and slide not in matching_slides:
                        matching_slides.append(slide)
                        found_accessions.add(accession_upper)
                        found = True

            if not found and accession_upper not in found_accessions:
                not_found_accessions.append(accession)

        # Create cohort
        with get_lock().write_lock():
            cohort = Cohort(
                name=name,
                description=description,
                source_type='upload',
                source_details=json.dumps({
                    "filename": filename,
                    "accessions_requested": len(accessions),
                    "accessions_found": len(found_accessions),
                    "accessions_not_found": not_found_accessions[:50]  # Limit stored
                }),
                created_by=created_by
            )
            cohort.slides = matching_slides
            db_session.add(cohort)
            db_session.commit()

        return {
            "id": cohort.id,
            "name": cohort.name,
            "slide_count": cohort.slide_count,
            "case_count": cohort.case_count,
            "accessions_requested": len(accessions),
            "accessions_found": len(found_accessions),
            "accessions_not_found": not_found_accessions
        }
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.post("/cohorts/from-filter")
def create_cohort_from_filter(
    name: str,
    description: Optional[str] = None,
    created_by: Optional[str] = None,
    query: Optional[str] = None,
    year: Optional[int] = None,
    stain: Optional[str] = None,
    tag: Optional[str] = None
):
    """Create a cohort from slides matching filter criteria."""
    try:
        db_session.rollback()

        # Use the search function to find matching slides
        results = indexer.search(
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
        slides = db_session.query(Slide).filter(Slide.slide_hash.in_(slide_hashes)).all()

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
            db_session.add(cohort)
            db_session.commit()

        return {
            "id": cohort.id,
            "name": cohort.name,
            "slide_count": cohort.slide_count,
            "case_count": cohort.case_count
        }
    except HTTPException:
        raise
    except Exception as e:
        db_session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


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
