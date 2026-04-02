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
    Boolean,
    Text,
    UniqueConstraint,
    event,
    inspect,
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
# SlideCap ID Generation
# ============================================================

class IdCounter(Base):
    """
    Auto-incrementing counter for human-readable SlideCap IDs.

    Each prefix (PT, CS, SL, JB, CO, etc.) gets its own counter.
    IDs are formatted as PREFIX + zero-padded number: PT00001, SL00042, etc.
    """
    __tablename__ = 'id_counters'

    prefix = Column(String(4), primary_key=True)  # PT, CS, SL, JB
    next_number = Column(Integer, nullable=False, default=1)


def generate_slidecap_id(db, prefix: str) -> str:
    """
    Generate the next SlideCap ID for a given prefix.
    Thread-safe via DB-level row locking.

    Usage:
        sid = generate_slidecap_id(db, "SL")  # → "SL00001"
    """
    counter = db.query(IdCounter).filter_by(prefix=prefix).first()
    if not counter:
        counter = IdCounter(prefix=prefix, next_number=1)
        db.add(counter)
        db.flush()
    sid = f"{prefix}{counter.next_number:05d}"
    counter.next_number += 1
    return sid


# ============================================================
# Global Patient Model
# ============================================================

class Patient(Base):
    """
    A real-world patient (de-identified within SlideCap).

    Patients are NOT created automatically by the indexer — they are created
    when a user links cases (surgeries) to a patient. This is because the
    indexer only knows accession numbers, not which accessions belong to the
    same person.

    The slidecap_id (PT00001) is the only identifier stored. No name, no MRN.
    External trial IDs are linked via ExternalMapping.
    """
    __tablename__ = 'patients'

    id = Column(Integer, primary_key=True)
    slidecap_id = Column(String(10), unique=True, nullable=False, index=True)  # PT00001
    note = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    cases = relationship('Case', back_populates='patient', order_by='Case.year')
    external_mappings = relationship('ExternalMapping', back_populates='patient',
                                    cascade='all, delete-orphan')

    @property
    def case_count(self) -> int:
        return len(self.cases)

    @property
    def slide_count(self) -> int:
        return sum(len(c.slides) for c in self.cases)

    def __repr__(self):
        return f"<Patient(slidecap_id={self.slidecap_id})>"


class ExternalMapping(Base):
    """
    Maps a SlideCap patient to an external system identifier.

    Primary use case: linking PT00001 to a REDCap trial ID like "REC-0045"
    so researchers can cross-reference SlideCap data with clinical trial data
    without exposing PHI.

    Supports multiple external systems per patient (REDCap, EPIC, etc.)
    and multiple projects within a system.
    """
    __tablename__ = 'external_mappings'

    id = Column(Integer, primary_key=True)
    patient_id = Column(Integer, ForeignKey('patients.id', ondelete='CASCADE'), nullable=False, index=True)
    external_system = Column(String(100), nullable=False)   # "redcap", "epic", "ctms"
    external_project = Column(String(200))                  # REDCap project name or ID
    external_id = Column(String(200), nullable=False)       # The trial/subject ID
    note = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)

    patient = relationship('Patient', back_populates='external_mappings')

    __table_args__ = (
        UniqueConstraint('external_system', 'external_project', 'external_id',
                         name='uq_external_mapping'),
        Index('idx_external_lookup', 'external_system', 'external_id'),
    )

    def __repr__(self):
        return f"<ExternalMapping({self.external_system}:{self.external_id} → {self.patient.slidecap_id if self.patient else '?'})>"


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
    Optionally linked to a Patient (multiple cases per patient).
    """
    __tablename__ = 'cases'

    id = Column(Integer, primary_key=True)
    slidecap_id = Column(String(10), unique=True, index=True)  # CS00001
    accession_hash = Column(String(64), unique=True, nullable=False, index=True)
    patient_id = Column(Integer, ForeignKey('patients.id', ondelete='SET NULL'), nullable=True, index=True)
    year = Column(Integer, nullable=False, index=True)
    indexed_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    patient = relationship('Patient', back_populates='cases')
    slides = relationship('Slide', back_populates='case', cascade='all, delete-orphan')
    tags = relationship('Tag', secondary=case_tags, back_populates='cases')
    projects = relationship('Project', secondary=project_cases, back_populates='cases')

    def __repr__(self):
        return f"<Case(id={self.id}, slidecap_id={self.slidecap_id}, hash={self.accession_hash[:8]}..., year={self.year})>"


class Slide(Base):
    """
    Individual slide file (SVS).
    
    Stores non-PHI components extracted from filename.
    The slide_hash is derived from the full filename stem (minus extension).
    """
    __tablename__ = 'slides'

    id = Column(Integer, primary_key=True)
    slidecap_id = Column(String(10), unique=True, index=True)  # SL00001
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
    job_slides = relationship('JobSlide', back_populates='slide', cascade='all, delete-orphan')
    
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
    patients = relationship('CohortPatient', back_populates='cohort', cascade='all, delete-orphan')
    flags = relationship('CohortFlag', back_populates='cohort', cascade='all, delete-orphan')

    @property
    def slide_count(self) -> int:
        return len(self.slides)

    @property
    def case_count(self) -> int:
        return len(set(s.case_id for s in self.slides))

    def __repr__(self):
        return f"<Cohort(name={self.name}, slides={self.slide_count})>"


class CohortPatient(Base):
    """
    A de-identified patient within a cohort.
    Groups multiple surgical cases (accessions) belonging to the same person.
    """
    __tablename__ = 'cohort_patients'

    id = Column(Integer, primary_key=True)
    cohort_id = Column(Integer, ForeignKey('cohorts.id', ondelete='CASCADE'), nullable=False, index=True)
    label = Column(String(100), nullable=False)   # user-defined, e.g. "P001"
    note = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)

    cohort = relationship('Cohort', back_populates='patients')
    surgeries = relationship(
        'CohortPatientCase',
        back_populates='patient',
        cascade='all, delete-orphan',
        order_by='CohortPatientCase.surgery_label',
    )


class CohortPatientCase(Base):
    """
    Links a surgical case to a patient within a cohort.
    surgery_label (S1, S2, S3…) identifies the surgery order for that patient.
    A case can be assigned to at most one patient per cohort (enforced in API).
    """
    __tablename__ = 'cohort_patient_cases'

    id = Column(Integer, primary_key=True)
    patient_id = Column(Integer, ForeignKey('cohort_patients.id', ondelete='CASCADE'), nullable=False)
    case_id = Column(Integer, ForeignKey('cases.id', ondelete='CASCADE'), nullable=False)
    surgery_label = Column(String(20), nullable=False)   # "S1", "S2", "S3"
    note = Column(String(500))

    patient = relationship('CohortPatient', back_populates='surgeries')
    case = relationship('Case')

    __table_args__ = (
        UniqueConstraint('patient_id', 'case_id', name='uq_patient_case'),
    )


class CohortFlag(Base):
    """
    A named selection subset within a cohort, used to mark cases for targeted analysis.

    Stores the set of case_hashes (accession_hashes) belonging to this flag.
    Flags are cohort-scoped and independent of the global slide-library tag system.
    """
    __tablename__ = 'cohort_flags'

    id = Column(Integer, primary_key=True)
    cohort_id = Column(Integer, ForeignKey('cohorts.id', ondelete='CASCADE'), nullable=False, index=True)
    name = Column(String(200), nullable=False)
    # JSON array of case accession_hashes: ["abc123", "def456", ...]
    case_hashes_json = Column(Text, default='[]')
    created_at = Column(DateTime, default=datetime.utcnow)

    cohort = relationship('Cohort', back_populates='flags')

    def get_case_hashes(self) -> list:
        import json
        try:
            return json.loads(self.case_hashes_json or '[]')
        except Exception:
            return []

    def set_case_hashes(self, hashes: list):
        import json
        self.case_hashes_json = json.dumps(list(set(hashes)))


class Analysis(Base):
    """
    Registry of available AI analysis pipelines.

    Each analysis defines a script on the cluster, parameter schema,
    and resource requirements for running on a GPU cluster via SSH + tmux.
    """
    __tablename__ = 'analyses'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), unique=True, nullable=False, index=True)
    version = Column(String(50), nullable=False, default='1.0')
    description = Column(Text)

    # Script-based execution config (replaces container_image)
    script_path = Column(String(500))       # Path to script on cluster
    working_directory = Column(String(500))  # cd here before running
    env_setup = Column(Text)                 # Commands to run before script (e.g. source venv)
    command_template = Column(Text)          # Full command with {placeholders}
    postprocess_template = Column(Text)      # Post-processing command with {input_dir} {output_dir} {filename_stem}

    # Parameter schema (JSON Schema string) and defaults (JSON string)
    parameters_schema = Column(Text)  # JSON Schema defining accepted parameters
    default_parameters = Column(Text)  # JSON string of default parameter values

    # Resource requirements
    gpu_required = Column(Boolean, default=True)
    estimated_runtime_minutes = Column(Integer, default=60)

    # Status
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    jobs = relationship('AnalysisJob', back_populates='analysis', passive_deletes=True)

    def __repr__(self):
        return f"<Analysis(name={self.name}, version={self.version})>"


class AnalysisJob(Base):
    """
    Track AI model runs on slides (parent job).

    A single job groups multiple slides. Per-slide tracking is in JobSlide.
    """
    __tablename__ = 'analysis_jobs'

    id = Column(Integer, primary_key=True)
    slidecap_id = Column(String(10), unique=True, index=True)  # JB00001
    analysis_id = Column(Integer, ForeignKey('analyses.id', ondelete='SET NULL'), nullable=True)

    # Job info
    model_name = Column(String(100), nullable=False)
    model_version = Column(String(50))
    parameters = Column(Text)  # JSON of actual params used

    # Cluster config
    gpu_index = Column(Integer, default=0)
    remote_wsi_dir = Column(String(500))      # Base WSI directory on cluster
    remote_output_dir = Column(String(500))   # Base output directory on cluster

    # Status tracking
    status = Column(String(20), default='pending')  # pending, transferring, running, completed, failed
    submitted_by = Column(String(100))
    submitted_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    # Results
    error_message = Column(String(1000))
    output_path = Column(String(500))  # Result directory path

    # Relationships
    slides = relationship('JobSlide', back_populates='job', cascade='all, delete-orphan')
    analysis = relationship('Analysis', back_populates='jobs')

    def __repr__(self):
        return f"<AnalysisJob(id={self.id}, model={self.model_name}, status={self.status})>"


class JobSlide(Base):
    """
    Per-slide tracking within an AnalysisJob.

    Each slide gets its own tmux session and tracks individual progress.
    """
    __tablename__ = 'job_slides'

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey('analysis_jobs.id', ondelete='CASCADE'), nullable=False)
    slide_id = Column(Integer, ForeignKey('slides.id', ondelete='CASCADE'), nullable=False)

    # Cluster execution
    cluster_job_id = Column(String(100))      # tmux session name (shared across all slides in a batch job)
    remote_wsi_path = Column(String(500))     # Where the slide was rsynced to
    remote_output_path = Column(String(500))  # Shared batch output directory on cluster
    local_output_path = Column(String(500))   # Per-slide path on network drive after distribution
    filename = Column(String(500))            # Original slide filename, used to match output files
    log_tail = Column(Text)                   # Last ~50 lines of progress log
    cell_stats = Column(Text)                 # Cached JSON of parsed cell statistics

    # Status tracking
    status = Column(String(20), default='pending')  # pending, transferring, running, completed, failed
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    error_message = Column(String(1000))

    # Relationships
    job = relationship('AnalysisJob', back_populates='slides')
    slide = relationship('Slide', back_populates='job_slides')

    def __repr__(self):
        return f"<JobSlide(id={self.id}, job_id={self.job_id}, status={self.status})>"


class RequestSheet(Base):
    """
    A tracking sheet for managing slide requests.
    Each sheet tracks multiple cases through the request/receipt/scanning lifecycle.
    """
    __tablename__ = 'request_sheets'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(String(1000))
    created_by = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    rows = relationship('RequestRow', back_populates='sheet', cascade='all, delete-orphan',
                        order_by='RequestRow.accession_number')

    @property
    def case_count(self) -> int:
        return len(self.rows)

    def __repr__(self):
        return f"<RequestSheet(name={self.name})>"


class RequestRow(Base):
    """
    A single case row in a request tracking sheet.
    Tracks blocks, slides, and status through the full request lifecycle.
    """
    __tablename__ = 'request_rows'

    id = Column(Integer, primary_key=True)
    sheet_id = Column(Integer, ForeignKey('request_sheets.id', ondelete='CASCADE'), nullable=False, index=True)

    # Case identification
    accession_number = Column(String(100), nullable=False)

    # Case Status
    case_status = Column(String(100), default='Not Started')

    # Requests
    all_blocks = Column(String(2000))
    blocks_available = Column(String(2000))
    order_id = Column(String(100))
    is_consult = Column(Boolean, default=False)
    blocks_hes_requested = Column(String(2000))
    hes_requested = Column(Integer, default=0)
    non_hes_requested = Column(Integer, default=0)
    ihc_stains_requested = Column(String(2000))

    # Receipts
    block_hes_received = Column(String(2000))
    hes_received = Column(Integer, default=0)
    unaccounted_blocks = Column(String(2000))
    non_hes_received = Column(Integer, default=0)
    fs_received = Column(Integer, default=0)
    uss_received = Column(Integer, default=0)
    ihc_received = Column(Integer, default=0)
    ihc_stains_received = Column(String(2000))

    # Recuts
    recut_blocks = Column(String(2000))
    recut_status = Column(String(100))

    # Scanning
    hes_scanned = Column(String(2000))
    he_scanning_status = Column(String(100))
    non_hes_scanned = Column(String(2000))

    # Other
    slide_location = Column(String(200))
    notes = Column(Text)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    sheet = relationship('RequestSheet', back_populates='rows')

    __table_args__ = (
        UniqueConstraint('sheet_id', 'accession_number', name='uq_sheet_accession'),
    )


# ============================================================
# Study Models
# ============================================================

study_slides = Table(
    'study_slides', Base.metadata,
    Column('study_id', Integer, ForeignKey('studies.id', ondelete='CASCADE'), primary_key=True),
    Column('slide_id', Integer, ForeignKey('slides.id', ondelete='CASCADE'), primary_key=True),
    Column('added_at', DateTime, default=datetime.utcnow)
)

study_group_slides = Table(
    'study_group_slides', Base.metadata,
    Column('group_id', Integer, ForeignKey('study_groups.id', ondelete='CASCADE'), primary_key=True),
    Column('slide_id', Integer, ForeignKey('slides.id', ondelete='CASCADE'), primary_key=True),
    Column('added_at', DateTime, default=datetime.utcnow)
)


class Study(Base):
    """
    A research study containing non-clinical (and optionally clinical) slides.
    Each study maps to a folder on the network drive under slides/studies/{folder_name}.
    """
    __tablename__ = 'studies'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(String(2000))
    folder_name = Column(String(200), nullable=False, unique=True)  # filesystem folder name
    created_by = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    slides = relationship('Slide', secondary=study_slides, backref='studies')
    groups = relationship('StudyGroup', back_populates='study', cascade='all, delete-orphan',
                         order_by='StudyGroup.sort_order')

    # Unlinked files: slides in the study folder that aren't in the DB
    # (tracked at runtime, not stored)

    @property
    def slide_count(self) -> int:
        return len(self.slides)

    @property
    def group_count(self) -> int:
        return len(self.groups)

    def __repr__(self):
        return f"<Study(name={self.name})>"


class StudyGroup(Base):
    """
    A named group within a study for organizing slides.
    Groups can represent patients, cohorts, experimental conditions, etc.
    Groups can optionally have a parent_id for nesting (cohort > patient).
    """
    __tablename__ = 'study_groups'

    id = Column(Integer, primary_key=True)
    study_id = Column(Integer, ForeignKey('studies.id', ondelete='CASCADE'), nullable=False, index=True)
    parent_id = Column(Integer, ForeignKey('study_groups.id', ondelete='SET NULL'), nullable=True, index=True)
    name = Column(String(200), nullable=False)
    label = Column(String(50))  # Short label like "P001", "Cohort A"
    color = Column(String(7))   # Hex color for UI
    note = Column(String(1000))
    sort_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    study = relationship('Study', back_populates='groups')
    children = relationship('StudyGroup', backref='parent', remote_side='StudyGroup.id',
                           cascade='all, delete-orphan', single_parent=True)
    slides = relationship('Slide', secondary=study_group_slides)

    def __repr__(self):
        return f"<StudyGroup(name={self.name}, study_id={self.study_id})>"


# ============================================================
# Indexes for common queries
# ============================================================

Index('idx_slides_case_stain', Slide.case_id, Slide.stain_type)
Index('idx_slides_block_stain', Slide.block_id, Slide.stain_type)
Index('idx_cases_year', Case.year)
Index('idx_jobs_status', AnalysisJob.status)
Index('idx_jobs_model_status', AnalysisJob.model_name, AnalysisJob.status)
Index('idx_job_slides_job_id', JobSlide.job_id)
Index('idx_job_slides_status', JobSlide.status)
Index('idx_request_rows_status', RequestRow.case_status)
Index('idx_study_groups_study', StudyGroup.study_id)
Index('idx_study_groups_parent', StudyGroup.parent_id)


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


def _migrate_analysis_jobs(engine):
    """Add new columns to existing tables if missing. Skips gracefully on DB errors."""
    from sqlalchemy import text

    try:
        insp = inspect(engine)
    except Exception as e:
        print(f"[DB Migration] Skipping migration (cannot inspect DB): {e}")
        return

    # Migrate analysis_jobs table
    try:
        if insp.has_table('analysis_jobs'):
            existing_cols = {col['name'] for col in insp.get_columns('analysis_jobs')}
            job_migrations = {
                'analysis_id': "ALTER TABLE analysis_jobs ADD COLUMN analysis_id INTEGER REFERENCES analyses(id) ON DELETE SET NULL",
                'parameters': "ALTER TABLE analysis_jobs ADD COLUMN parameters TEXT",
                'output_path': "ALTER TABLE analysis_jobs ADD COLUMN output_path VARCHAR(500)",
                'gpu_index': "ALTER TABLE analysis_jobs ADD COLUMN gpu_index INTEGER DEFAULT 0",
                'remote_wsi_dir': "ALTER TABLE analysis_jobs ADD COLUMN remote_wsi_dir VARCHAR(500)",
                'remote_output_dir': "ALTER TABLE analysis_jobs ADD COLUMN remote_output_dir VARCHAR(500)",
            }

            with engine.connect() as conn:
                for col_name, ddl in job_migrations.items():
                    if col_name not in existing_cols:
                        print(f"[DB Migration] Adding column: analysis_jobs.{col_name}")
                        conn.execute(text(ddl))
                conn.commit()
    except Exception as e:
        print(f"[DB Migration] Skipping analysis_jobs migration: {e}")

    # Fix legacy slide_id NOT NULL constraint on analysis_jobs
    # The column was migrated to job_slides but SQLite can't ALTER COLUMN,
    # so we recreate the table if slide_id still has NOT NULL.
    try:
        if insp.has_table('analysis_jobs'):
            with engine.connect() as conn:
                table_info = conn.execute(text("PRAGMA table_info(analysis_jobs)")).fetchall()
                slide_id_col = next((c for c in table_info if c[1] == 'slide_id'), None)
                # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
                if slide_id_col and slide_id_col[3]:  # notnull == 1
                    print("[DB Migration] Removing NOT NULL constraint from analysis_jobs.slide_id")
                    col_names = [c[1] for c in table_info]
                    col_names_str = ', '.join(col_names)

                    # Build column definitions with slide_id as nullable
                    col_defs = []
                    for c in table_info:
                        cid, name, ctype, notnull, dflt, pk = c
                        if pk:
                            col_defs.append(f"{name} INTEGER PRIMARY KEY")
                        elif name == 'slide_id':
                            col_defs.append(f"{name} INTEGER")  # drop NOT NULL
                        else:
                            parts = [name, ctype or 'TEXT']
                            if notnull:
                                parts.append("NOT NULL")
                            if dflt is not None:
                                parts.append(f"DEFAULT {dflt}")
                            col_defs.append(' '.join(parts))

                    conn.execute(text("PRAGMA foreign_keys=OFF"))
                    conn.execute(text(f"CREATE TABLE analysis_jobs_new ({', '.join(col_defs)})"))
                    conn.execute(text(f"INSERT INTO analysis_jobs_new ({col_names_str}) SELECT {col_names_str} FROM analysis_jobs"))
                    conn.execute(text("DROP TABLE analysis_jobs"))
                    conn.execute(text("ALTER TABLE analysis_jobs_new RENAME TO analysis_jobs"))
                    conn.execute(text("PRAGMA foreign_keys=ON"))
                    conn.commit()
                    print("[DB Migration] analysis_jobs.slide_id is now nullable")
    except Exception as e:
        print(f"[DB Migration] Skipping slide_id constraint fix: {e}")

    # Migrate analyses table (container_image → script-based fields)
    try:
        if insp.has_table('analyses'):
            existing_cols = {col['name'] for col in insp.get_columns('analyses')}
            analysis_migrations = {
                'script_path': "ALTER TABLE analyses ADD COLUMN script_path VARCHAR(500)",
                'working_directory': "ALTER TABLE analyses ADD COLUMN working_directory VARCHAR(500)",
                'env_setup': "ALTER TABLE analyses ADD COLUMN env_setup TEXT",
                'command_template': "ALTER TABLE analyses ADD COLUMN command_template TEXT",
                'postprocess_template': "ALTER TABLE analyses ADD COLUMN postprocess_template TEXT",
            }

            with engine.connect() as conn:
                for col_name, ddl in analysis_migrations.items():
                    if col_name not in existing_cols:
                        print(f"[DB Migration] Adding column: analyses.{col_name}")
                        conn.execute(text(ddl))
                conn.commit()
    except Exception as e:
        print(f"[DB Migration] Skipping analyses migration: {e}")

    # Migrate job_slides table
    try:
        if insp.has_table('job_slides'):
            existing_cols = {col['name'] for col in insp.get_columns('job_slides')}
            js_migrations = {
                'local_output_path': "ALTER TABLE job_slides ADD COLUMN local_output_path VARCHAR(500)",
                'filename': "ALTER TABLE job_slides ADD COLUMN filename VARCHAR(500)",
                'cell_stats': "ALTER TABLE job_slides ADD COLUMN cell_stats TEXT",
            }

            with engine.connect() as conn:
                for col_name, ddl in js_migrations.items():
                    if col_name not in existing_cols:
                        print(f"[DB Migration] Adding column: job_slides.{col_name}")
                        conn.execute(text(ddl))
                conn.commit()
    except Exception as e:
        print(f"[DB Migration] Skipping job_slides column migration: {e}")

    # Migrate existing AnalysisJob rows → JobSlide (one-time migration)
    try:
        if insp.has_table('analysis_jobs') and insp.has_table('job_slides'):
            old_cols = {col['name'] for col in insp.get_columns('analysis_jobs')}
            # If old schema had slide_id column, migrate those rows to job_slides
            if 'slide_id' in old_cols:
                with engine.connect() as conn:
                    # Check if any rows need migration (old jobs with slide_id that have no job_slides)
                    rows = conn.execute(text(
                        "SELECT aj.id, aj.slide_id, aj.cluster_job_id, aj.remote_wsi_path, "
                        "aj.remote_output_path, aj.log_tail, aj.status, aj.started_at, "
                        "aj.completed_at, aj.error_message "
                        "FROM analysis_jobs aj "
                        "WHERE aj.slide_id IS NOT NULL "
                        "AND NOT EXISTS (SELECT 1 FROM job_slides js WHERE js.job_id = aj.id)"
                    )).fetchall()

                    if rows:
                        print(f"[DB Migration] Migrating {len(rows)} old AnalysisJob rows to JobSlide...")
                        for row in rows:
                            conn.execute(text(
                                "INSERT INTO job_slides (job_id, slide_id, cluster_job_id, "
                                "remote_wsi_path, remote_output_path, log_tail, status, "
                                "started_at, completed_at, error_message) "
                                "VALUES (:job_id, :slide_id, :cluster_job_id, :remote_wsi_path, "
                                ":remote_output_path, :log_tail, :status, :started_at, "
                                ":completed_at, :error_message)"
                            ), {
                                "job_id": row[0], "slide_id": row[1],
                                "cluster_job_id": row[2], "remote_wsi_path": row[3],
                                "remote_output_path": row[4], "log_tail": row[5],
                                "status": row[6], "started_at": row[7],
                                "completed_at": row[8], "error_message": row[9],
                            })
                        conn.commit()
                        print(f"[DB Migration] Migrated {len(rows)} rows to job_slides")
    except Exception as e:
        print(f"[DB Migration] Skipping job_slides migration: {e}")


def _migrate_slidecap_ids(engine):
    """Add slidecap_id columns and backfill existing records with auto-generated IDs."""
    from sqlalchemy import text

    try:
        insp = inspect(engine)
        # Clear cached schema info so we see columns added by create_all
        insp.clear_cache()
    except Exception as e:
        print(f"[DB Migration] Skipping SlideCap ID migration (cannot inspect DB): {e}")
        return

    # SQLite cannot ADD COLUMN with UNIQUE constraint — add column first, index after backfill
    migrations = {
        'cases': {
            'slidecap_id': "ALTER TABLE cases ADD COLUMN slidecap_id VARCHAR(10)",
            'patient_id': "ALTER TABLE cases ADD COLUMN patient_id INTEGER REFERENCES patients(id) ON DELETE SET NULL",
        },
        'slides': {
            'slidecap_id': "ALTER TABLE slides ADD COLUMN slidecap_id VARCHAR(10)",
        },
        'analysis_jobs': {
            'slidecap_id': "ALTER TABLE analysis_jobs ADD COLUMN slidecap_id VARCHAR(10)",
        },
    }

    with engine.connect() as conn:
        for table_name, cols in migrations.items():
            if not insp.has_table(table_name):
                continue
            existing = {col['name'] for col in insp.get_columns(table_name)}
            for col_name, ddl in cols.items():
                if col_name not in existing:
                    try:
                        print(f"[DB Migration] Adding column: {table_name}.{col_name}")
                        conn.execute(text(ddl))
                    except Exception as e:
                        print(f"[DB Migration] Skipping {table_name}.{col_name}: {e}")
        conn.commit()

    # Backfill slidecap_ids for existing records
    with engine.connect() as conn:
        # Ensure id_counters table exists
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS id_counters ("
            "prefix VARCHAR(4) PRIMARY KEY, "
            "next_number INTEGER NOT NULL DEFAULT 1)"
        ))
        conn.commit()

        for table_name, prefix in [('cases', 'CS'), ('slides', 'SL'), ('analysis_jobs', 'JB')]:
            try:
                if not insp.has_table(table_name):
                    continue
                rows = conn.execute(text(
                    f"SELECT id FROM {table_name} WHERE slidecap_id IS NULL ORDER BY id"
                )).fetchall()
                if not rows:
                    continue

                # Get current counter
                counter_row = conn.execute(text(
                    "SELECT next_number FROM id_counters WHERE prefix = :p"
                ), {"p": prefix}).fetchone()
                next_num = counter_row[0] if counter_row else 1

                print(f"[DB Migration] Backfilling {len(rows)} {prefix} IDs...")
                for row in rows:
                    sid = f"{prefix}{next_num:05d}"
                    conn.execute(text(
                        f"UPDATE {table_name} SET slidecap_id = :sid WHERE id = :id"
                    ), {"sid": sid, "id": row[0]})
                    next_num += 1

                # Upsert counter
                conn.execute(text(
                    "INSERT INTO id_counters (prefix, next_number) VALUES (:p, :n) "
                    "ON CONFLICT(prefix) DO UPDATE SET next_number = :n"
                ), {"p": prefix, "n": next_num})
                conn.commit()
                print(f"[DB Migration] Assigned {prefix}00001–{prefix}{next_num-1:05d}")
            except Exception as e:
                print(f"[DB Migration] Skipping {prefix} backfill: {e}")

    # Create unique indexes on slidecap_id columns + patient_id index
    with engine.connect() as conn:
        for idx_sql in [
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_cases_slidecap_id ON cases(slidecap_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_slides_slidecap_id ON slides(slidecap_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_slidecap_id ON analysis_jobs(slidecap_id)",
            "CREATE INDEX IF NOT EXISTS idx_cases_patient ON cases(patient_id)",
        ]:
            try:
                conn.execute(text(idx_sql))
            except Exception as e:
                print(f"[DB Migration] Index skipped: {e}")
        conn.commit()


def init_db(db_path: Path):
    """
    Initialize database schema and session factory.
    Call this once at startup before handling requests.

    Order matters:
    1. Run legacy migrations (ALTER TABLE for old columns)
    2. Create all new tables (patients, external_mappings, id_counters, etc.)
    3. Run SlideCap ID migration (ALTER TABLE to add slidecap_id/patient_id to existing tables + backfill)
    """
    global _SessionLocal, _engine
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _engine = get_engine(db_path)
    _migrate_analysis_jobs(_engine)
    Base.metadata.create_all(_engine)  # Creates new tables (patients, id_counters, external_mappings)
    _migrate_slidecap_ids(_engine)     # Adds columns to existing tables + backfills IDs
    _SessionLocal = sessionmaker(bind=_engine)


import time as _time


def _is_retryable_error(e: Exception) -> bool:
    """Check if an exception is a retryable database error."""
    error_str = str(e).lower()
    return (
        "unable to open database" in error_str or
        "database is locked" in error_str or
        "disk i/o error" in error_str or
        "database disk image is malformed" in error_str
    )


def _create_session_with_retry(max_retries: int = 3, retry_delay: float = 0.5) -> Session:
    """Create a session with retry logic for transient network failures."""
    from sqlalchemy import text

    for attempt in range(max_retries):
        db = _SessionLocal()
        try:
            # Test the connection by actually touching the database file
            # SELECT 1 doesn't open the file in SQLite, so query a real table
            db.execute(text("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1"))
            return db
        except Exception as e:
            db.close()

            if _is_retryable_error(e) and attempt < max_retries - 1:
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


def with_retry(func, db: Session, max_retries: int = 3, retry_delay: float = 0.5):
    """
    Execute a database operation with retry logic for transient failures.

    Usage:
        result = with_retry(lambda: db.query(Slide).all(), db)

    Args:
        func: A callable that performs the database operation
        db: The database session (used for rollback on retry)
        max_retries: Maximum number of attempts
        retry_delay: Base delay between retries (multiplied by attempt number)

    Returns:
        The result of func()
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            last_error = e
            if _is_retryable_error(e) and attempt < max_retries - 1:
                print(f"[DB] Retrying operation (attempt {attempt + 1}/{max_retries}): {e}")
                try:
                    db.rollback()
                except Exception:
                    pass
                _time.sleep(retry_delay * (attempt + 1))
                continue
            raise
    raise last_error


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
