"""District endpoints."""
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from src.api.main import get_db
from src.models import TurnoutMetrics, Voter
from src.models.turnout import DistrictType

router = APIRouter()


@router.get("/")
async def list_districts(
    district_type: Optional[DistrictType] = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    List all districts.

    Args:
        district_type: Filter by district type
        skip: Number of records to skip
        limit: Maximum number of records to return
        db: Database session

    Returns:
        List of districts
    """
    query = select(TurnoutMetrics).distinct(TurnoutMetrics.district_id, TurnoutMetrics.district_type)

    if district_type:
        query = query.where(TurnoutMetrics.district_type == district_type)

    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    districts = result.scalars().all()

    return [
        {
            "district_type": d.district_type,
            "district_id": d.district_id,
            "district_name": d.district_name,
        }
        for d in districts
    ]


@router.get("/{district_type}")
async def get_districts_by_type(
    district_type: DistrictType,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    Get districts by type.

    Args:
        district_type: District type
        skip: Number of records to skip
        limit: Maximum number of records to return
        db: Database session

    Returns:
        List of districts
    """
    query = (
        select(TurnoutMetrics)
        .where(TurnoutMetrics.district_type == district_type)
        .distinct(TurnoutMetrics.district_id)
    )
    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    districts = result.scalars().all()

    return [
        {
            "district_type": d.district_type,
            "district_id": d.district_id,
            "district_name": d.district_name,
        }
        for d in districts
    ]


@router.get("/{district_type}/{district_id}/voters")
async def get_voters_in_district(
    district_type: DistrictType,
    district_id: str,
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    Get voters in district.

    Args:
        district_type: District type
        district_id: District identifier
        skip: Number of records to skip
        limit: Maximum number of records to return
        db: Database session

    Returns:
        List of voters in district
    """
    query = select(Voter)

    # Map district type to voter field
    try:
        district_id_int = int(district_id)
    except ValueError:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail="Invalid district ID format")

    if district_type == DistrictType.CONGRESSIONAL_2022:
        # For 2022 districts, we'd need to map differently
        # For now, we'll use the 2026 fields
        query = query.where(Voter.newcd == district_id_int)
    elif district_type == DistrictType.SENATE_2022:
        query = query.where(Voter.newsd == district_id_int)
    elif district_type == DistrictType.SENATE_2026:
        query = query.where(Voter.newsd == district_id_int)

    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    voters = result.scalars().all()

    return voters

