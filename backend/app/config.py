"""
Configuration for Slide Organizer.
Update NETWORK_ROOT to point to your network drive.
"""
import secrets
from pathlib import Path
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # ============================================================
    # UPDATE THIS PATH to your network drive location
    # ============================================================
    # Windows example: "Z:/slides" or "//server/share/slides"
    # macOS example: "/Volumes/SharedDrive/slides"
    # Linux example: "/mnt/network/slides"

    # NETWORK_ROOT: str = '/Volumes/DFCI-LIGONLAB/Ligon Lab/test_directory'
    NETWORK_ROOT: str = r'L:\Ligon Lab\test_directory_pt_slides'

    # App data directory on NETWORK (for shared assets like salt)
    APP_DATA_DIR: str = ".slidecap"

    # Local data directory for SQLite DB (MUST be local disk, not network)
    # SQLite does not work reliably over SMB/NFS due to file locking issues.
    LOCAL_DATA_DIR: str = "~/.slidecap"

    # App mode: "prod" (live deployment) or "demo" (test instance with PHI redacted).
    # Demo mode swaps accession numbers for slide/case IDs in the UI and routes
    # cluster submissions to a mock pipeline. Set via APP_MODE env var.
    APP_MODE: str = "prod"

    # ── Filename parser ────────────────────────────────────────────────
    # Deploying institutions can override the parser pattern list to match
    # their accession scheme without code changes. JSON env var
    # PARSER_PATTERNS must decode to a list of {name, description, regex}
    # objects. The regex must use these named groups:
    #
    #   accession (required), year (required), block, slide / slide_only,
    #   stain, random
    #
    # Default = "" which means "use the built-in DFCI Ligon Lab pattern."
    PARSER_PATTERNS: str = ""

    @property
    def app_data_path(self) -> Path:
        return Path(self.NETWORK_ROOT) / self.APP_DATA_DIR

    @property
    def local_data_path(self) -> Path:
        p = Path(self.LOCAL_DATA_DIR).expanduser()
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def db_path(self) -> Path:
        return self.local_data_path / "database.sqlite"

    @property
    def salt_path(self) -> Path:
        return self.app_data_path / ".salt"

    @property
    def thumbnail_cache_path(self) -> Path:
        # Thumbnails include the slide *label* and *macro* images, which are
        # photographs of the printed slide label (accession / MRN / name) and
        # therefore contain PHI that cannot be hashed or obscured. Keep them on
        # the access-controlled network drive (next to .salt), NOT scattered on
        # each workstation's local disk. Bonus: the cache is then shared across
        # all users of the multi-user server instead of regenerated per machine.
        p = self.app_data_path / "thumbnails"
        p.mkdir(parents=True, exist_ok=True)
        return p
    
    # Server settings
    # HOST: str = "127.0.0.1"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    
    # Authentication settings
    AUTH_SECRET_KEY: Optional[str] = None  # Auto-generated if not set
    AUTH_TOKEN_EXPIRY_DAYS: int = 30
    AUTH_CHALLENGE_EXPIRY_MINUTES: int = 5

    # Cluster settings (SSH + tmux, no Slurm)
    CLUSTER_HOST: Optional[str] = None   # Default hostname, overridable in UI
    CLUSTER_PORT: int = 22

    # If the cluster can access the same network drive as the slides, set this
    # to the cluster's mount path to skip rsync transfers entirely.
    # e.g. if slides are at "/Volumes/DFCI-LIGONLAB/..." on the server and
    # mounted at "/mnt/dfci-ligonlab/..." on the cluster, set this to that path.
    # Leave as None to use rsync transfers (default).
    CLUSTER_NETWORK_MOUNT: Optional[str] = None

    # Base directory on the cluster for per-job scratch/TMPDIR. Default is local
    # /tmp, but on clusters where / is small/full, point this at a bigger mount
    # (e.g. "/mnt/scratch/slidecap_tmp"). Per-job dirs are created under it and
    # removed after each run; stale ones (>6h) are swept at the next job start.
    CLUSTER_TMPDIR: str = "/ligonlab/michael/slidecap_tmp"

    # Ray's temp/spill dir. Unlike everything else, Ray puts Unix domain SOCKETS
    # here, which do NOT work on NFS — so this must be a LOCAL filesystem, not the
    # NFS CLUSTER_TMPDIR. /dev/shm is local (tmpfs, RAM-backed) and usually large,
    # so it's the safe default when the local root disk is full. Set to "" to leave
    # Ray on its default (/tmp/ray). Exported as RAY_TMPDIR per job.
    CLUSTER_RAY_TMPDIR: str = "/dev/shm/slidecap_ray"

    # tmux keeps its control SOCKET in $TMUX_TMPDIR/tmux-<uid>/ (default /tmp).
    # On clusters where / (and thus /tmp) is full, tmux can't create/hold that
    # socket and every session dies instantly ("error connecting to
    # /tmp/tmux-<uid>/default"). Point it at a local fs with space + socket
    # support (/dev/shm = tmpfs). Applied to ALL tmux commands SlideCap runs.
    CLUSTER_TMUX_TMPDIR: str = "/dev/shm"

    # Minimum free space (MB) required on the cluster output volume before a job
    # is allowed to start. Below this, the submit fails fast with a clear
    # "disk full" message instead of dying with an empty log. 0 disables the check.
    CLUSTER_MIN_FREE_MB: int = 512

    @property
    def staging_path(self) -> Path:
        return Path(self.NETWORK_ROOT) / "slide_staging"

    @property
    def slides_path(self) -> Path:
        return Path(self.NETWORK_ROOT) / "slides"

    @property
    def studies_path(self) -> Path:
        return Path(self.NETWORK_ROOT) / "slides" / "studies"

    @property
    def external_path(self) -> Path:
        # Non-clinical / outside-hospital scans (no accession). Registered manually.
        return Path(self.NETWORK_ROOT) / "slides" / "external"

    # Microns per pixel to stamp on converted pyramids whose source TIFF says
    # nothing about resolution. Analyses like UNI refuse to run without MPP.
    # Leave unset to convert without it (and get a warning in the log).
    DEFAULT_MPP: Optional[float] = None

    @property
    def pyramid_path(self) -> Path:
        """
        Where converted pyramidal copies of plain TIFFs live.

        On the network drive, NOT in local data: the cluster mounts this drive
        (CLUSTER_NETWORK_MOUNT), so an analysis can symlink straight to the
        converted file. A server-local copy would be invisible to the cluster
        and would have to be re-transferred for every job. `.pyramids` sits
        beside the year folders and is skipped by the indexer (not a year dir).
        """
        return Path(self.NETWORK_ROOT) / "slides" / ".pyramids"

    @property
    def local_pyramid_path(self) -> Path:
        """Fallback pyramid store, used when the network drive isn't writable."""
        return self.local_data_path / "pyramid-cache"

    @property
    def analyses_path(self) -> Path:
        return Path(self.NETWORK_ROOT) / "analyses"

    @property
    def annotations_path(self) -> Path:
        return Path(self.NETWORK_ROOT) / "annotations"

    @property
    def secret_key_path(self) -> Path:
        return self.local_data_path / ".secret_key"

    @property
    def auth_challenges_path(self) -> Path:
        return self.app_data_path / ".auth_challenges"

    def get_secret_key(self) -> str:
        """Get or auto-generate a secret key for JWT signing."""
        if self.AUTH_SECRET_KEY:
            return self.AUTH_SECRET_KEY
        path = self.secret_key_path
        if path.exists():
            return path.read_text().strip()
        key = secrets.token_hex(32)
        path.write_text(key)
        return key

    @property
    def ssh_configured(self) -> bool:
        return bool(self.CLUSTER_HOST)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
