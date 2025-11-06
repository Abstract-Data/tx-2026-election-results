"""Voter model for SQLModel."""
from datetime import date
from enum import Enum
from typing import Optional

from sqlmodel import Field, SQLModel


class GeneralLikelihood(str, Enum):
    """General election likelihood classification."""

    LIKELY_REPUBLICAN = "Likely Republican"
    LEAN_REPUBLICAN = "Lean Republican"
    TOSS_UP = "Toss-Up"
    LEAN_DEMOCRAT = "Lean Democrat"
    LIKELY_DEMOCRAT = "Likely Democrat"


class PredictionAccuracy(str, Enum):
    """Prediction accuracy classification."""

    CORRECT_EARLY = "Correct-Early"
    CORRECT_ELECTION_DAY = "Correct-ElectionDay"
    PREDICTED_EARLY_BUT_DIDNT = "Predicted-Early-But-Didnt"
    PREDICTED_ELECTION_DAY_BUT_VOTED_EARLY = "Predicted-ElectionDay-But-VotedEarly"
    UNKNOWN = "Unknown"


class Voter(SQLModel, table=True):
    """Voter model with likelihood scores and prediction accuracy."""

    __tablename__ = "voters"

    # Primary key
    vuid: int = Field(primary_key=True, description="Voter unique identifier")

    # Basic demographics
    county: Optional[str] = Field(default=None, description="County name")
    age: Optional[int] = Field(default=None, description="Age in years")
    age_bracket: Optional[str] = Field(default=None, description="Age bracket (e.g., '18-24')")
    dob: Optional[str] = Field(default=None, description="Date of birth (YYYYMMDD format)")

    # District assignments
    newcd: Optional[int] = Field(default=None, description="New Congressional District")
    newsd: Optional[int] = Field(default=None, description="New State Senate District")
    newhd: Optional[int] = Field(default=None, description="New State House District")

    # Party codes and derived party names
    pri24: Optional[str] = Field(default=None, description="2024 Primary party code")
    pri22: Optional[str] = Field(default=None, description="2022 Primary party code")
    party_2024: Optional[str] = Field(default=None, description="Derived 2024 party name")
    party_2022: Optional[str] = Field(default=None, description="Derived 2022 party name")
    party: Optional[str] = Field(default=None, description="Primary party affiliation")

    # Likelihood scores
    primary_likelihood_r: Optional[float] = Field(
        default=None, ge=0.0, le=1.0, description="Likelihood to vote in Republican primary (0.0-1.0)"
    )
    primary_likelihood_d: Optional[float] = Field(
        default=None, ge=0.0, le=1.0, description="Likelihood to vote in Democratic primary (0.0-1.0)"
    )
    voting_method_likelihood_early: Optional[float] = Field(
        default=None, ge=0.0, le=1.0, description="Likelihood to vote early (0.0-1.0)"
    )
    voting_method_likelihood_election_day: Optional[float] = Field(
        default=None, ge=0.0, le=1.0, description="Likelihood to vote on election day (0.0-1.0)"
    )
    general_likelihood: Optional[GeneralLikelihood] = Field(
        default=None, description="General election likelihood classification"
    )
    turnout_score: Optional[float] = Field(
        default=None, ge=0.0, le=1.0, description="Overall likelihood to vote in general election (0.0-1.0)"
    )

    # Actual voting behavior
    actual_voted_early: Optional[bool] = Field(
        default=None, description="Whether voter actually voted early (from early voting data)"
    )
    prediction_accuracy: Optional[PredictionAccuracy] = Field(
        default=None, description="Prediction accuracy classification"
    )

