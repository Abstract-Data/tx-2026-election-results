"""Configuration settings."""
import os
from pathlib import Path
from typing import Optional


class Config:
    """Application configuration."""

    # Database settings
    DATABASE_TYPE: str = os.getenv("DATABASE_TYPE", "sqlite")
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")

    # API settings
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    CORS_ORIGINS: list[str] = os.getenv("CORS_ORIGINS", "*").split(",")

    # Data paths
    DATA_DIR: Path = Path(os.getenv("DATA_DIR", "./"))
    PROCESSED_VOTERFILE: Path = DATA_DIR / "processed_voterfile.parquet"
    PROCESSED_EARLY_VOTING: Path = DATA_DIR / "processed_early_voting.parquet"
    MERGED_DATA: Path = DATA_DIR / "early_voting_merged.parquet"
    TURNOUT_DIR: Path = DATA_DIR

    @classmethod
    def get_database_url(cls) -> Optional[str]:
        """
        Get database connection URL.

        Returns:
            Database connection string or None
        """
        return cls.DATABASE_URL

