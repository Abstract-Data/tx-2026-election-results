"""Turnout metrics model for SQLModel."""
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlmodel import Field, SQLModel


class DistrictType(str, Enum):
    """District type enumeration."""

    CONGRESSIONAL_2022 = "congressional_2022"
    SENATE_2022 = "senate_2022"
    SENATE_2026 = "senate_2026"


class TurnoutMetrics(SQLModel, table=True):
    """Turnout metrics by district model."""

    __tablename__ = "turnout_metrics"

    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True, description="Record ID")

    # District information
    district_type: DistrictType = Field(description="Type of district")
    district_id: str = Field(index=True, description="District identifier")
    district_name: Optional[str] = Field(default=None, description="District name")

    # Turnout metrics
    total_voters: int = Field(description="Total registered voters in district")
    early_voters: int = Field(description="Number of early voters")
    turnout_rate: float = Field(description="Turnout rate as percentage")

    # Metadata
    calculated_at: datetime = Field(
        default_factory=datetime.utcnow, description="Timestamp when metrics were calculated"
    )

