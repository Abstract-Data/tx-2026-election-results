"""Database initialization."""
from sqlalchemy.ext.asyncio import AsyncEngine

from sqlmodel import SQLModel

from ..models import EarlyVoting, TurnoutMetrics, Voter


async def init_db(engine: AsyncEngine) -> None:
    """
    Initialize database by creating all tables.

    Args:
        engine: SQLAlchemy async engine
    """
    async with engine.begin() as conn:
        # Create all tables
        await conn.run_sync(SQLModel.metadata.create_all)

