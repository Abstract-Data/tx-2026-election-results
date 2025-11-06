"""Early voting model for SQLModel."""
from datetime import date
from typing import Optional

from sqlmodel import Field, SQLModel


class EarlyVoting(SQLModel, table=True):
    """Early voting record model."""

    __tablename__ = "early_voting"

    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True, description="Record ID")

    # Foreign key to voter
    vuid: int = Field(foreign_key="voters.vuid", index=True, description="Voter unique identifier")

    # Voting information
    tx_name: Optional[str] = Field(default=None, description="County name")
    voting_method: Optional[str] = Field(default=None, description="Voting method")
    precinct: Optional[str] = Field(default=None, description="Precinct")
    source_file: Optional[str] = Field(default=None, description="Source filename")
    early_vote_date: Optional[date] = Field(
        default=None, description="Date of early vote (extracted from filename)"
    )

