"""Turnout endpoints."""
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from src.api.main import get_db
from src.models import TurnoutMetrics
from src.models.turnout import DistrictType

router = APIRouter()


@router.get("/", response_model=List[TurnoutMetrics])
async def list_turnout_metrics(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    district_type: Optional[DistrictType] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    List all turnout metrics.

    Args:
        skip: Number of records to skip
        limit: Maximum number of records to return
        district_type: Filter by district type
        db: Database session

    Returns:
        List of turnout metrics
    """
    query = select(TurnoutMetrics)

    if district_type:
        query = query.where(TurnoutMetrics.district_type == district_type)

    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    metrics = result.scalars().all()

    return metrics


@router.get("/{district_type}", response_model=List[TurnoutMetrics])
async def get_turnout_by_district_type(
    district_type: DistrictType,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    Get turnout metrics by district type.

    Args:
        district_type: District type
        skip: Number of records to skip
        limit: Maximum number of records to return
        db: Database session

    Returns:
        List of turnout metrics
    """
    query = select(TurnoutMetrics).where(TurnoutMetrics.district_type == district_type)
    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    metrics = result.scalars().all()

    return metrics


@router.get("/{district_type}/{district_id}", response_model=TurnoutMetrics)
async def get_turnout_by_district(
    district_type: DistrictType,
    district_id: str,
    db: AsyncSession = Depends(get_db),
):
    """
    Get turnout metrics for specific district.

    Args:
        district_type: District type
        district_id: District identifier
        db: Database session

    Returns:
        Turnout metrics for district
    """
    result = await db.execute(
        select(TurnoutMetrics).where(
            TurnoutMetrics.district_type == district_type,
            TurnoutMetrics.district_id == district_id,
        )
    )
    metric = result.scalar_one_or_none()

    if metric is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Turnout metrics not found")

    return metric


@router.get("/comparison/2022-vs-2026")
async def compare_turnout_2022_vs_2026(db: AsyncSession = Depends(get_db)):
    """
    Compare turnout metrics between 2022 and 2026 districts.

    Args:
        db: Database session

    Returns:
        Comparison data
    """
    from sqlmodel import func

    # Get 2022 senate metrics
    result = await db.execute(
        select(func.avg(TurnoutMetrics.turnout_rate)).where(
            TurnoutMetrics.district_type == DistrictType.SENATE_2022
        )
    )
    avg_turnout_2022 = result.scalar()

    # Get 2026 senate metrics
    result = await db.execute(
        select(func.avg(TurnoutMetrics.turnout_rate)).where(
            TurnoutMetrics.district_type == DistrictType.SENATE_2026
        )
    )
    avg_turnout_2026 = result.scalar()

    # Get totals
    result = await db.execute(
        select(func.sum(TurnoutMetrics.total_voters)).where(
            TurnoutMetrics.district_type == DistrictType.SENATE_2022
        )
    )
    total_voters_2022 = result.scalar()

    result = await db.execute(
        select(func.sum(TurnoutMetrics.total_voters)).where(
            TurnoutMetrics.district_type == DistrictType.SENATE_2026
        )
    )
    total_voters_2026 = result.scalar()

    return {
        "2022": {
            "avg_turnout_rate": avg_turnout_2022,
            "total_voters": total_voters_2022,
        },
        "2026": {
            "avg_turnout_rate": avg_turnout_2026,
            "total_voters": total_voters_2026,
        },
        "difference": {
            "turnout_rate": (avg_turnout_2026 - avg_turnout_2022) if avg_turnout_2022 else None,
            "total_voters": (total_voters_2026 - total_voters_2022) if total_voters_2022 else None,
        },
    }

