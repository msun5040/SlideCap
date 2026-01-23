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
# Database Connection
# ============================================================

def get_engine(db_path: Path):
    """Create SQLite engine with network drive optimizations."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    engine = create_engine(
        f"sqlite:///{db_path}",
        echo=False,  # Set to True for SQL debugging
        connect_args={
            "timeout": 30.0,  # Wait for locks
        }
    )
    
    # Configure SQLite for better concurrency on network drives
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")      # Better concurrency
        cursor.execute("PRAGMA busy_timeout=30000")    # 30s timeout
        cursor.execute("PRAGMA synchronous=NORMAL")    # Balance speed/safety
        cursor.execute("PRAGMA cache_size=-64000")     # 64MB cache
        cursor.close()
    
    return engine


def init_db(db_path: Path) -> Session:
    """Initialize database and return a session."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = get_engine(db_path)
    Base.metadata.create_all(engine)
    
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


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
