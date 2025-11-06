"""SQLite connection strategy."""
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker

from .base_connection_strategy import BaseConnectionStrategy


class SQLiteConnectionStrategy(BaseConnectionStrategy):
    """Strategy for SQLite database connections."""

    def create_engine(self, connection_string: Optional[str] = None) -> AsyncEngine:
        """
        Create SQLite async engine.

        Args:
            connection_string: SQLite database path (e.g., "sqlite+aiosqlite:///./data.db")

        Returns:
            SQLAlchemy AsyncEngine instance
        """
        if connection_string is None:
            connection_string = "sqlite+aiosqlite:///./election_data.db"

        return create_async_engine(
            connection_string,
            echo=False,
            future=True,
        )

    def create_session_factory(self, engine: AsyncEngine) -> sessionmaker:
        """
        Create session factory for SQLite.

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

