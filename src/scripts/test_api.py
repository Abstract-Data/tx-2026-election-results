"""Quick test script for the API."""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, func

from src.database import DatabaseConnectionFactory, init_db
from src.config import Config
from src.models import Voter, TurnoutMetrics, EarlyVoting


async def test_database():
    """Test database connection and queries."""
    print("Testing database connection...")
    
    engine, session_factory = DatabaseConnectionFactory.create_connection(
        db_type=Config.DATABASE_TYPE, connection_string=Config.get_database_url()
    )
    
    async with session_factory() as session:
        # Test voter count
        result = await session.execute(select(func.count(Voter.vuid)))
        voter_count = result.scalar()
        print(f"✓ Total voters in database: {voter_count}")
        
        # Test early voting count
        result = await session.execute(select(func.count(EarlyVoting.id)))
        early_voting_count = result.scalar()
        print(f"✓ Total early voting records: {early_voting_count}")
        
        # Test turnout metrics count
        result = await session.execute(select(func.count(TurnoutMetrics.id)))
        turnout_count = result.scalar()
        print(f"✓ Total turnout metrics: {turnout_count}")
        
        # Test a sample voter
        result = await session.execute(select(Voter).limit(1))
        voter = result.scalar_one_or_none()
        if voter:
            print(f"✓ Sample voter: VUID={voter.vuid}, Party={voter.party}, General Likelihood={voter.general_likelihood}")
            print(f"  - Primary Likelihood R: {voter.primary_likelihood_r}, D: {voter.primary_likelihood_d}")
            print(f"  - Voting Method Likelihood Early: {voter.voting_method_likelihood_early}")
            print(f"  - Turnout Score: {voter.turnout_score}")
            print(f"  - Prediction Accuracy: {voter.prediction_accuracy}")
        
        # Test early voting with date
        result = await session.execute(select(EarlyVoting).where(EarlyVoting.early_vote_date.isnot(None)).limit(1))
        ev = result.scalar_one_or_none()
        if ev:
            print(f"✓ Sample early voting: VUID={ev.vuid}, Date={ev.early_vote_date}, County={ev.tx_name}")
        
        # Test turnout metrics
        result = await session.execute(select(TurnoutMetrics).limit(1))
        metric = result.scalar_one_or_none()
        if metric:
            print(f"✓ Sample turnout metric: {metric.district_type} District {metric.district_id}")
            print(f"  - Total Voters: {metric.total_voters}, Early Voters: {metric.early_voters}")
            print(f"  - Turnout Rate: {metric.turnout_rate:.2f}%")
    
    await engine.dispose()
    print("\n✓ All database tests passed!")


if __name__ == "__main__":
    asyncio.run(test_database())

