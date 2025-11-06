"""Voter endpoints."""
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from src.api.main import get_db
from src.models import Voter
from src.models.voter import GeneralLikelihood, PredictionAccuracy

router = APIRouter()


@router.get("/", response_model=List[Voter])
async def list_voters(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    party: Optional[str] = None,
    district_type: Optional[str] = None,
    district_id: Optional[int] = None,
    general_likelihood: Optional[GeneralLikelihood] = None,
    prediction_accuracy: Optional[PredictionAccuracy] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    List voters with pagination and filters.

    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        party: Filter by party affiliation
        district_type: Filter by district type (newcd, newsd, newhd)
        district_id: Filter by district ID
        general_likelihood: Filter by general election likelihood
        prediction_accuracy: Filter by prediction accuracy
        db: Database session

    Returns:
        List of voter records
    """
    query = select(Voter)

    # Apply filters
    if party:
        query = query.where(Voter.party == party)
    if district_type == "newcd" and district_id:
        query = query.where(Voter.newcd == district_id)
    elif district_type == "newsd" and district_id:
        query = query.where(Voter.newsd == district_id)
    elif district_type == "newhd" and district_id:
        query = query.where(Voter.newhd == district_id)
    if general_likelihood:
        query = query.where(Voter.general_likelihood == general_likelihood)
    if prediction_accuracy:
        query = query.where(Voter.prediction_accuracy == prediction_accuracy)

    # Apply pagination
    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    voters = result.scalars().all()

    return voters


@router.get("/{vuid}", response_model=Voter)
async def get_voter(vuid: int, db: AsyncSession = Depends(get_db)):
    """
    Get voter by ID with all likelihood scores and prediction accuracy.

    Args:
        vuid: Voter unique identifier
        db: Database session

    Returns:
        Voter record
    """
    result = await db.execute(select(Voter).where(Voter.vuid == vuid))
    voter = result.scalar_one_or_none()

    if voter is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Voter not found")

    return voter


@router.get("/stats/summary")
async def get_voter_stats(db: AsyncSession = Depends(get_db)):
    """
    Get aggregate statistics about voters.

    Args:
        db: Database session

    Returns:
        Dictionary with voter statistics
    """
    from sqlmodel import func

    result = await db.execute(select(func.count(Voter.vuid)))
    total_voters = result.scalar()

    result = await db.execute(select(func.count(Voter.vuid)).where(Voter.actual_voted_early == True))
    early_voters = result.scalar()

    result = await db.execute(select(func.avg(Voter.turnout_score)))
    avg_turnout_score = result.scalar()

    return {
        "total_voters": total_voters,
        "early_voters": early_voters,
        "early_voting_rate": (early_voters / total_voters * 100) if total_voters > 0 else 0,
        "avg_turnout_score": avg_turnout_score,
    }


@router.get("/likelihood/primary")
async def get_voters_by_primary_likelihood(
    party: Optional[str] = Query(None, description="Filter by party (R or D)"),
    min_likelihood: float = Query(0.0, ge=0.0, le=1.0),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    Get voters by primary likelihood.

    Args:
        party: Filter by party (R or D)
        min_likelihood: Minimum likelihood threshold
        skip: Number of records to skip
        limit: Maximum number of records to return
        db: Database session

    Returns:
        List of voter records
    """
    query = select(Voter)

    if party == "R":
        query = query.where(Voter.primary_likelihood_r >= min_likelihood)
    elif party == "D":
        query = query.where(Voter.primary_likelihood_d >= min_likelihood)

    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    voters = result.scalars().all()

    return voters


@router.get("/likelihood/general")
async def get_voters_by_general_likelihood(
    general_likelihood: GeneralLikelihood,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    Get voters by general election likelihood classification.

    Args:
        general_likelihood: General election likelihood classification
        skip: Number of records to skip
        limit: Maximum number of records to return
        db: Database session

    Returns:
        List of voter records
    """
    query = select(Voter).where(Voter.general_likelihood == general_likelihood)
    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    voters = result.scalars().all()

    return voters


@router.get("/likelihood/voting-method")
async def get_voters_by_voting_method_likelihood(
    method: str = Query(..., description="Voting method (early or election_day)"),
    min_likelihood: float = Query(0.0, ge=0.0, le=1.0),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    Get voters by voting method likelihood.

    Args:
        method: Voting method (early or election_day)
        min_likelihood: Minimum likelihood threshold
        skip: Number of records to skip
        limit: Maximum number of records to return
        db: Database session

    Returns:
        List of voter records
    """
    query = select(Voter)

    if method == "early":
        query = query.where(Voter.voting_method_likelihood_early >= min_likelihood)
    elif method == "election_day":
        query = query.where(Voter.voting_method_likelihood_election_day >= min_likelihood)

    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    voters = result.scalars().all()

    return voters


@router.get("/prediction-accuracy")
async def get_voters_by_prediction_accuracy(
    prediction_accuracy: PredictionAccuracy,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    Get voters by prediction accuracy status.

    Args:
        prediction_accuracy: Prediction accuracy classification
        skip: Number of records to skip
        limit: Maximum number of records to return
        db: Database session

    Returns:
        List of voter records
    """
    query = select(Voter).where(Voter.prediction_accuracy == prediction_accuracy)
    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    voters = result.scalars().all()

    return voters


@router.get("/prediction-stats/summary")
async def get_prediction_stats(db: AsyncSession = Depends(get_db)):
    """
    Get statistics on prediction accuracy.

    Args:
        db: Database session

    Returns:
        Dictionary with prediction accuracy statistics
    """
    from sqlmodel import func

    result = await db.execute(
        select(func.count(Voter.vuid)).where(
            Voter.prediction_accuracy == PredictionAccuracy.CORRECT_EARLY
        )
    )
    correct_early = result.scalar()

    result = await db.execute(
        select(func.count(Voter.vuid)).where(
            Voter.prediction_accuracy == PredictionAccuracy.CORRECT_ELECTION_DAY
        )
    )
    correct_election_day = result.scalar()

    result = await db.execute(
        select(func.count(Voter.vuid)).where(
            Voter.prediction_accuracy == PredictionAccuracy.PREDICTED_EARLY_BUT_DIDNT
        )
    )
    predicted_early_but_didnt = result.scalar()

    result = await db.execute(
        select(func.count(Voter.vuid)).where(
            Voter.prediction_accuracy == PredictionAccuracy.PREDICTED_ELECTION_DAY_BUT_VOTED_EARLY
        )
    )
    predicted_election_day_but_voted_early = result.scalar()

    result = await db.execute(select(func.count(Voter.vuid)))
    total = result.scalar()

    return {
        "total_voters": total,
        "correct_early": correct_early,
        "correct_election_day": correct_election_day,
        "predicted_early_but_didnt": predicted_early_but_didnt,
        "predicted_election_day_but_voted_early": predicted_election_day_but_voted_early,
        "accuracy_rate": (
            (correct_early + correct_election_day) / total * 100 if total > 0 else 0
        ),
    }

