"""Base connection strategy interface."""
from abc import ABC, abstractmethod
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker


class BaseConnectionStrategy(ABC):
    """Base interface for database connection strategies."""

    @abstractmethod
    def create_engine(self, connection_string: Optional[str] = None) -> AsyncEngine:
        """
        Create database engine based on connection string.

        Args:
            connection_string: Database connection string

        Returns:
            SQLAlchemy AsyncEngine instance
        """
        pass

    @abstractmethod
    def create_session_factory(self, engine: AsyncEngine) -> sessionmaker:
        """
        Create session factory for database sessions.

        Args:
            engine: SQLAlchemy engine

        Returns:
            Session factory
        """
        pass

