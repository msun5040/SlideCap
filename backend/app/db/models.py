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
    hes_scanned = Column(String(50))
    he_scanning_status = Column(String(100))
    non_hes_scanned = Column(String(50))

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


def init_db(db_path: Path):
    """
    Initialize database schema and session factory.
    Call this once at startup before handling requests.
    """
    global _SessionLocal, _engine
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _engine = get_engine(db_path)
    _migrate_analysis_jobs(_engine)
    Base.metadata.create_all(_engine)
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
