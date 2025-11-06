"""Factory for creating database connections."""
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import sessionmaker

from .strategies import (
    CloudflareD1ConnectionStrategy,
    NeonConnectionStrategy,
    SQLiteConnectionStrategy,
)


class DatabaseConnectionFactory:
    """Factory for creating database connections using Strategy pattern."""

    @staticmethod
    def create_connection(
        db_type: str = "sqlite", connection_string: Optional[str] = None
    ) -> tuple:
        """
        Create database connection based on type.

        Args:
            db_type: Database type ("sqlite", "neon", "d1")
            connection_string: Optional connection string

        Returns:
            Tuple of (engine, session_factory)
        """
        strategy = DatabaseConnectionFactory._get_strategy(db_type)
        engine = strategy.create_engine(connection_string)
        session_factory = strategy.create_session_factory(engine)

        return engine, session_factory

    @staticmethod
    def _get_strategy(db_type: str):
        """
        Get connection strategy based on database type.

        Args:
            db_type: Database type ("sqlite", "neon", "d1")

        Returns:
            Connection strategy instance
        """
        strategies = {
            "sqlite": SQLiteConnectionStrategy,
            "neon": NeonConnectionStrategy,
            "d1": CloudflareD1ConnectionStrategy,
        }

        strategy_class = strategies.get(db_type.lower())
        if strategy_class is None:
            raise ValueError(f"Unknown database type: {db_type}. Supported types: {list(strategies.keys())}")

        return strategy_class()

