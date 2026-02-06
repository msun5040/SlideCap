"""
Configuration for Slide Organizer.
Update NETWORK_ROOT to point to your network drive.
"""
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

    # App data directory (will be created inside NETWORK_ROOT)
    APP_DATA_DIR: str = ".slidecap"

    # Database and salt paths (derived from above)
    @property
    def app_data_path(self) -> Path:
        return Path(self.NETWORK_ROOT) / self.APP_DATA_DIR

    @property
    def db_path(self) -> Path:
        return self.app_data_path / "database.sqlite"

    @property
    def salt_path(self) -> Path:
        return self.app_data_path / ".salt"

    @property
    def thumbnail_cache_path(self) -> Path:
        return self.app_data_path / "thumbnails"
    
    # Server settings
    HOST: str = "127.0.0.1"
    PORT: int = 8000
    
    # SSH settings for GPU cluster (configure later)
    SSH_HOST: Optional[str] = None
    SSH_USER: Optional[str] = None
    SSH_KEY_PATH: Optional[str] = None
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
