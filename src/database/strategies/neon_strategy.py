"""Neon PostgreSQL connection strategy."""
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import sessionmaker

from .base_connection_strategy import BaseConnectionStrategy


class NeonConnectionStrategy(BaseConnectionStrategy):
    """Strategy for Neon PostgreSQL database connections."""

    def create_engine(self, connection_string: Optional[str] = None) -> AsyncEngine:
        """
        Create Neon PostgreSQL async engine.

        Args:
            connection_string: PostgreSQL connection string (e.g., "postgresql+asyncpg://...")

        Returns:
            SQLAlchemy AsyncEngine instance
        """
        if connection_string is None:
            raise ValueError("Neon connection string is required")

        return create_async_engine(
            connection_string,
            echo=False,
            future=True,
            pool_pre_ping=True,
        )

    def create_session_factory(self, engine: AsyncEngine) -> sessionmaker:
        """
        Create session factory for Neon PostgreSQL.

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

