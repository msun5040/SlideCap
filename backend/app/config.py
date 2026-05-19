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
    NETWORK_ROOT: str = '/Volumes/DFCI-LIGONLAB/Ligon Lab/test_directory_pt_slides'

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
        return self.local_data_path / "thumbnails"
    
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
