"""Cloudflare D1 connection strategy."""
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import sessionmaker

from .base_connection_strategy import BaseConnectionStrategy


class CloudflareD1ConnectionStrategy(BaseConnectionStrategy):
    """Strategy for Cloudflare D1 database connections."""

    def create_engine(self, connection_string: Optional[str] = None) -> AsyncEngine:
        """
        Create Cloudflare D1 async engine.

        Note: Cloudflare D1 requires special handling in Workers environment.
        This is a placeholder - actual implementation depends on deployment environment.

        Args:
            connection_string: D1 database path (e.g., "sqlite+aiosqlite:///./data.db")

        Returns:
            SQLAlchemy AsyncEngine instance
        """
        # For local development, use SQLite with aiosqlite
        # In Cloudflare Workers, this would use the D1 API
        if connection_string is None:
            connection_string = "sqlite+aiosqlite:///./d1_data.db"

        from sqlalchemy.ext.asyncio import create_async_engine

        return create_async_engine(
            connection_string,
            echo=False,
            future=True,
        )

    def create_session_factory(self, engine: AsyncEngine) -> sessionmaker:
        """
        Create session factory for Cloudflare D1.

        Args:
            engine: SQLAlchemy engine

        Returns:
            Session factory
        """
        from sqlalchemy.ext.asyncio import AsyncSession

        return sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

