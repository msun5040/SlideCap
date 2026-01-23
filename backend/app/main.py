from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import os

from .config import settings
from .db import init_db, Case, Slide, Tag, Project
from .services import SlideHasher, SlideIndexer


# Request models for bulk operations
class BulkTagRequest(BaseModel):
    slide_hashes: List[str]
    tags: List[str]


# Global instances (initialized on startup)
db_session = None
hasher = None
indexer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_session, hasher, indexer
    
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
    stain: Optional[str] = Query(None, description="Filter by stain type (e.g., HE)"),
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
# Tag Endpoints
# ============================================================

@app.get("/tags")
def list_tags():
    """List all tags."""
    tags = db_session.query(Tag).order_by(Tag.name).all()
    return [{"id": t.id, "name": t.name, "category": t.category} for t in tags]


@app.post("/tags")
def create_tag(name: str, category: Optional[str] = None):
    """Create a new tag."""
    existing = db_session.query(Tag).filter_by(name=name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Tag already exists")

    tag = Tag(name=name, category=category)
    db_session.add(tag)
    db_session.commit()

    return {"id": tag.id, "name": tag.name, "category": tag.category}


@app.get("/tags/{tag_name}/slides")
def get_slides_by_tag(tag_name: str, limit: int = Query(100, le=500)):
    """Get all slides with a given tag."""
    tag = db_session.query(Tag).filter_by(name=tag_name).first()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    slides = tag.slides[:limit]

    return {
        "tag": tag_name,
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


@app.post("/slides/{slide_hash}/tags/{tag_name}")
def add_tag_to_slide(slide_hash: str, tag_name: str):
    """Add a tag to a slide."""
    slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
    if not slide:
        raise HTTPException(status_code=404, detail="Slide not found")
    
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
    slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
    if not slide:
        raise HTTPException(status_code=404, detail="Slide not found")
    
    tag = db_session.query(Tag).filter_by(name=tag_name).first()
    if tag and tag in slide.tags:
        slide.tags.remove(tag)
        db_session.commit()
    
    return {"status": "ok"}


# ============================================================
# Bulk Tag Endpoints
# ============================================================

@app.post("/slides/bulk/tags/add")
def bulk_add_tags(request: BulkTagRequest):
    """Add tags to multiple slides at once."""
    updated = []
    not_found = []

    for slide_hash in request.slide_hashes:
        slide = db_session.query(Slide).filter_by(slide_hash=slide_hash).first()
        if not slide:
            not_found.append(slide_hash)
            continue

        for tag_name in request.tags:
            tag = db_session.query(Tag).filter_by(name=tag_name).first()
            if not tag:
                tag = Tag(name=tag_name)
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


@app.post("/slides/bulk/tags/remove")
def bulk_remove_tags(request: BulkTagRequest):
    """Remove tags from multiple slides at once."""
    updated = []
    not_found = []

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
    
    if case not in project.cases:
        project.cases.append(case)
        db_session.commit()
    
    return {"status": "ok", "project_id": project_id, "case_hash": case_hash}


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
