"""
Database models for Slide Organizer.

Key design decisions:
- No PHI stored in database (only hashed identifiers)
- Case = surgical accession (one patient visit)
- Slide = individual SVS file (multiple per case)
- Tags can be applied to both cases and slides
- Projects contain cases (and by extension, their slides)
"""
from datetime import datetime
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Table,
    Index,
    event,
)
from sqlalchemy.orm import (
    declarative_base,
    relationship,
    sessionmaker,
    Session,
)
from pathlib import Path

Base = declarative_base()


# ============================================================
# Association Tables (many-to-many relationships)
# ============================================================

case_tags = Table(
    'case_tags', Base.metadata,
    Column('case_id', Integer, ForeignKey('cases.id', ondelete='CASCADE'), primary_key=True),
    Column('tag_id', Integer, ForeignKey('tags.id', ondelete='CASCADE'), primary_key=True),
    Column('tagged_by', String(100)),
    Column('tagged_at', DateTime, default=datetime.utcnow)
)

slide_tags = Table(
    'slide_tags', Base.metadata,
    Column('slide_id', Integer, ForeignKey('slides.id', ondelete='CASCADE'), primary_key=True),
    Column('tag_id', Integer, ForeignKey('tags.id', ondelete='CASCADE'), primary_key=True),
    Column('tagged_by', String(100)),
    Column('tagged_at', DateTime, default=datetime.utcnow)
)

project_cases = Table(
    'project_cases', Base.metadata,
    Column('project_id', Integer, ForeignKey('projects.id', ondelete='CASCADE'), primary_key=True),
    Column('case_id', Integer, ForeignKey('cases.id', ondelete='CASCADE'), primary_key=True),
    Column('added_by', String(100)),
    Column('added_at', DateTime, default=datetime.utcnow)
)

cohort_slides = Table(
    'cohort_slides', Base.metadata,
    Column('cohort_id', Integer, ForeignKey('cohorts.id', ondelete='CASCADE'), primary_key=True),
    Column('slide_id', Integer, ForeignKey('slides.id', ondelete='CASCADE'), primary_key=True),
    Column('added_at', DateTime, default=datetime.utcnow)
)


# ============================================================
# Core Models
# ============================================================

class Case(Base):
    """
    Represents a surgical case (accession number).
    
    No PHI stored - only the hashed accession identifier.
    A case can have multiple slides (different blocks, stains).
    """
    __tablename__ = 'cases'
    
    id = Column(Integer, primary_key=True)
    accession_hash = Column(String(64), unique=True, nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    indexed_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    slides = relationship('Slide', back_populates='case', cascade='all, delete-orphan')
    tags = relationship('Tag', secondary=case_tags, back_populates='cases')
    projects = relationship('Project', secondary=project_cases, back_populates='cases')
    
    def __repr__(self):
        return f"<Case(id={self.id}, hash={self.accession_hash[:8]}..., year={self.year})>"


class Slide(Base):
    """
    Individual slide file (SVS).
    
    Stores non-PHI components extracted from filename.
    The slide_hash is derived from the full filename stem (minus extension).
    """
    __tablename__ = 'slides'
    
    id = Column(Integer, primary_key=True)
    case_id = Column(Integer, ForeignKey('cases.id', ondelete='CASCADE'), nullable=False)
    
    # Hashed full filename for lookups
    slide_hash = Column(String(64), unique=True, nullable=False, index=True)
    
    # Non-PHI components extracted from filename
    block_id = Column(String(20))       # A1, B2, etc.
    stain_type = Column(String(50))     # HE, IHC-CD3, etc.
    random_id = Column(String(20))      # 7f3a2b (the de-id friendly part)
    
    # Metadata
    indexed_at = Column(DateTime, default=datetime.utcnow)
    file_exists = Column(Integer, default=1)  # 1 = exists, 0 = missing
    file_size_bytes = Column(Integer)
    
    # Relationships
    case = relationship('Case', back_populates='slides')
    tags = relationship('Tag', secondary=slide_tags, back_populates='slides')
    analysis_jobs = relationship('AnalysisJob', back_populates='slide', cascade='all, delete-orphan')
    
    def __repr__(self):
        return f"<Slide(id={self.id}, block={self.block_id}, stain={self.stain_type})>"


class Tag(Base):
    """
    Tags for organizing cases and slides.
    
    Examples: "melanoma", "margin-positive", "needs-review", "training-set"
    """
    __tablename__ = 'tags'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    category = Column(String(50))  # Optional: 'diagnosis', 'quality', 'workflow', etc.
    color = Column(String(7))      # Optional: hex color for UI (e.g., "#FF5733")
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    cases = relationship('Case', secondary=case_tags, back_populates='tags')
    slides = relationship('Slide', secondary=slide_tags, back_populates='tags')
    
    def __repr__(self):
        return f"<Tag(name={self.name})>"


class Project(Base):
    """
    Projects for grouping cases into cohorts.
    
    Examples: "Melanoma Study 2024", "AI Training Set v2", "Dr. Smith Review"
    """
    __tablename__ = 'projects'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(String(1000))
    created_by = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    cases = relationship('Case', secondary=project_cases, back_populates='projects')
    
    @property
    def case_count(self) -> int:
        return len(self.cases)
    
    @property
    def slide_count(self) -> int:
        return sum(len(case.slides) for case in self.cases)
    
    def __repr__(self):
        return f"<Project(name={self.name})>"


class Cohort(Base):
    """
    A cohort is a collection of slides for analysis or export.

    Cohorts can be created by:
    - Uploading a list of accession numbers
    - Filtering slides by criteria
    - Selecting slides with specific tags
    """
    __tablename__ = 'cohorts'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(String(1000))

    # How the cohort was created
    source_type = Column(String(50))  # 'upload', 'filter', 'tag', 'manual'
    source_details = Column(String(2000))  # JSON with filter criteria, tag names, etc.

    # Metadata
    created_by = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    slides = relationship('Slide', secondary=cohort_slides, backref='cohorts')

    @property
    def slide_count(self) -> int:
        return len(self.slides)

    @property
    def case_count(self) -> int:
        return len(set(s.case_id for s in self.slides))

    def __repr__(self):
        return f"<Cohort(name={self.name}, slides={self.slide_count})>"


class AnalysisJob(Base):
    """
    Track AI model runs on slides.
    
    Stores job metadata and paths to results (results stay on network drive).
    """
    __tablename__ = 'analysis_jobs'
    
    id = Column(Integer, primary_key=True)
    slide_id = Column(Integer, ForeignKey('slides.id', ondelete='CASCADE'), nullable=False)
    
    # Job info
    model_name = Column(String(100), nullable=False)
    model_version = Column(String(50))
    
    # Status tracking
    status = Column(String(20), default='pending')  # pending, queued, running, completed, failed
    submitted_by = Column(String(100))
    submitted_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
    # Results
    error_message = Column(String(1000))
    result_path = Column(String(500))  # Relative path to results on network drive
    
    # Relationships
    slide = relationship('Slide', back_populates='analysis_jobs')
    
    def __repr__(self):
        return f"<AnalysisJob(model={self.model_name}, status={self.status})>"


# ============================================================
# Indexes for common queries
# ============================================================

Index('idx_slides_case_stain', Slide.case_id, Slide.stain_type)
Index('idx_slides_block_stain', Slide.block_id, Slide.stain_type)
Index('idx_cases_year', Case.year)
Index('idx_jobs_status', AnalysisJob.status)
Index('idx_jobs_model_status', AnalysisJob.model_name, AnalysisJob.status)


# ============================================================
# Database Connection (Hardened for Network Drives)
# ============================================================

def get_engine(db_path: Path):
    """Create SQLite engine hardened for network drive usage."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(
        f"sqlite:///{db_path}",
        echo=False,  # Set to True for SQL debugging
        connect_args={
            "timeout": 60.0,  # Wait up to 60s for locks
            "check_same_thread": False,  # Allow multi-threaded access
        },
        pool_pre_ping=True,  # Verify connections before use
        pool_recycle=300,  # Recycle connections every 5 minutes (helps with network drives)
    )

    # Configure SQLite for network drive compatibility
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        # Use DELETE journal mode (not WAL) - more compatible with network drives
        cursor.execute("PRAGMA journal_mode=DELETE")
        # Long timeout for busy database (60 seconds)
        cursor.execute("PRAGMA busy_timeout=60000")
        # FULL synchronous for data safety on network drives
        cursor.execute("PRAGMA synchronous=FULL")
        # Smaller cache for network drives
        cursor.execute("PRAGMA cache_size=-16000")  # 16MB cache
        # Use NORMAL locking (not EXCLUSIVE) to allow multiple readers
        cursor.execute("PRAGMA locking_mode=NORMAL")
        cursor.close()

    return engine


_SessionLocal = None
_engine = None


def init_db(db_path: Path):
    """
    Initialize database schema and session factory.
    Call this once at startup before handling requests.
    """
    global _SessionLocal, _engine
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _engine = get_engine(db_path)
    Base.metadata.create_all(_engine)
    _SessionLocal = sessionmaker(bind=_engine)


import time as _time


def _create_session_with_retry(max_retries: int = 3, retry_delay: float = 0.5) -> Session:
    """Create a session with retry logic for transient network failures."""
    from sqlalchemy import text

    for attempt in range(max_retries):
        db = _SessionLocal()
        try:
            # Test the connection with a simple query
            db.execute(text("SELECT 1"))
            return db
        except Exception as e:
            db.close()

            # Check if it's a retryable error (network/file access issues)
            error_str = str(e).lower()
            is_retryable = (
                "unable to open database" in error_str or
                "database is locked" in error_str or
                "disk i/o error" in error_str
            )

            if is_retryable and attempt < max_retries - 1:
                print(f"[DB] Retrying connection (attempt {attempt + 1}/{max_retries}): {e}")
                _time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                continue
            raise

    raise RuntimeError("Failed to create database session after retries")


def get_db():
    """
    FastAPI dependency that provides a database session per request.

    Usage:
        @app.get("/items")
        def get_items(db: Session = Depends(get_db)):
            return db.query(Item).all()

    Each request gets its own session that is:
    - Committed on success
    - Rolled back on exception
    - Closed when the request completes

    Includes retry logic for transient network drive failures.
    """
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")

    db = _create_session_with_retry()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_session() -> Session:
    """
    Get a database session for use outside of FastAPI request handling.

    Use this for startup operations, background tasks, etc.
    Caller is responsible for committing, rolling back, and closing the session.

    Usage:
        db = get_session()
        try:
            # do work
            db.commit()
        except:
            db.rollback()
            raise
        finally:
            db.close()
    """
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _SessionLocal()


# Quick test
if __name__ == "__main__":
    from pathlib import Path
    import tempfile
    
    # Create a test database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.sqlite"
        session = init_db(db_path)
        
        # Create a test case
        case = Case(accession_hash="abc123", year=2024)
        session.add(case)
        session.flush()
        
        # Create a test slide
        slide = Slide(
            case_id=case.id,
            slide_hash="def456",
            block_id="A1",
            stain_type="HE",
            random_id="7f3a2b"
        )
        session.add(slide)
        
        # Create a tag
        tag = Tag(name="melanoma", category="diagnosis")
        session.add(tag)
        session.flush()
        
        # Tag the case
        case.tags.append(tag)
        
        # Create a project
        project = Project(name="Test Project", created_by="test_user")
        project.cases.append(case)
        session.add(project)
        
        session.commit()
        
        print("Database test successful!")
        print(f"  Case: {case}")
        print(f"  Slide: {slide}")
        print(f"  Tag: {tag}")
        print(f"  Project: {project}")
        print(f"  Project has {project.case_count} cases, {project.slide_count} slides")
