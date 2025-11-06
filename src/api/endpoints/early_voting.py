"""Early voting analytics endpoints."""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import func, select

from src.api.main import get_db
from src.models import EarlyVoting, Voter

router = APIRouter()


@router.get("/by-date")
async def get_early_voting_by_date(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    party: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Get early voting turnout by date (all parties or specific party).

    Args:
        start_date: Optional start date filter
        end_date: Optional end date filter
        party: Optional party filter
        db: Database session

    Returns:
        List of early voting records aggregated by date
    """
    query = select(
        EarlyVoting.early_vote_date,
        func.count(EarlyVoting.vuid).label("count"),
    ).group_by(EarlyVoting.early_vote_date)

    if start_date:
        query = query.where(EarlyVoting.early_vote_date >= start_date)
    if end_date:
        query = query.where(EarlyVoting.early_vote_date <= end_date)

    result = await db.execute(query)
    records = result.all()

    # If party filter, join with voters
    if party:
        party_records = []
        for record in records:
            date_val = record[0]
            # Get count for this date and party
            party_query = select(func.count(EarlyVoting.vuid)).join(Voter).where(
                EarlyVoting.early_vote_date == date_val, Voter.party == party
            )
            party_result = await db.execute(party_query)
            party_count = party_result.scalar()
            party_records.append({"date": date_val, "count": party_count, "party": party})
        return party_records

    return [{"date": record[0], "count": record[1]} for record in records]


@router.get("/by-date/{party}")
async def get_early_voting_by_date_for_party(
    party: str,
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Get early voting turnout by date for specific party.

    Args:
        party: Party name
        start_date: Optional start date filter
        end_date: Optional end date filter
        db: Database session

    Returns:
        List of early voting records aggregated by date for party
    """
    query = (
        select(EarlyVoting.early_vote_date, func.count(EarlyVoting.vuid).label("count"))
        .join(Voter, EarlyVoting.vuid == Voter.vuid)
        .where(Voter.party == party)
        .group_by(EarlyVoting.early_vote_date)
    )

    if start_date:
        query = query.where(EarlyVoting.early_vote_date >= start_date)
    if end_date:
        query = query.where(EarlyVoting.early_vote_date <= end_date)

    result = await db.execute(query)
    records = result.all()

    return [{"date": record[0], "count": record[1], "party": party} for record in records]


@router.get("/by-date/{start_date}/{end_date}")
async def get_early_voting_in_date_range(
    start_date: date,
    end_date: date,
    party: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Get early voting within date range.

    Args:
        start_date: Start date
        end_date: End date
        party: Optional party filter
        db: Database session

    Returns:
        List of early voting records in date range
    """
    query = select(EarlyVoting).where(
        EarlyVoting.early_vote_date >= start_date, EarlyVoting.early_vote_date <= end_date
    )

    if party:
        query = query.join(Voter).where(Voter.party == party)

    result = await db.execute(query)
    records = result.scalars().all()

    return records


@router.get("/party-comparison")
async def compare_early_voting_by_party(db: AsyncSession = Depends(get_db)):
    """
    Compare early voting patterns by party over time.

    Args:
        db: Database session

    Returns:
        Comparison data by party
    """
    # Get early voting by date and party
    query = (
        select(
            EarlyVoting.early_vote_date,
            Voter.party,
            func.count(EarlyVoting.vuid).label("count"),
        )
        .join(Voter, EarlyVoting.vuid == Voter.vuid)
        .where(EarlyVoting.early_vote_date.isnot(None))
        .group_by(EarlyVoting.early_vote_date, Voter.party)
        .order_by(EarlyVoting.early_vote_date, Voter.party)
    )

    result = await db.execute(query)
    records = result.all()

    # Organize by party
    party_data = {}
    for record in records:
        party = record[1] or "Unknown"
        if party not in party_data:
            party_data[party] = []
        party_data[party].append({"date": record[0], "count": record[2]})

    return party_data

