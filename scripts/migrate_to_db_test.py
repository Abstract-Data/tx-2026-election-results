"""Test data migration script - processes only a small sample."""
import asyncio
from pathlib import Path
from typing import List

import polars as pl
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import Config
from src.database import DatabaseConnectionFactory, init_db
from src.models import EarlyVoting, TurnoutMetrics, Voter
from src.models.scoring import LikelihoodScorerFactory
from tx_election_results.utils.observers import ErrorObserver, ProgressObserver, StatisticsObserver
from scripts.utils.date_extractor import extract_date_from_filename


def map_party_code(party_code: str) -> str:
    """Map party codes to full party names."""
    if not party_code or party_code.strip() == "":
        return "Unknown"

    party_code = party_code.strip().upper()

    party_mapping = {
        "RE": "Republican",
        "DE": "Democrat",
        "DE/RE": "Democrat/Republican",
        "RE/DE": "Republican/Democrat",
        "LI": "Libertarian",
        "GR": "Green",
        "UN": "Unaffiliated",
        "": "Unknown",
    }

    return party_mapping.get(party_code, "Unknown")


async def migrate_voters(
    session: AsyncSession, voterfile_path: Path, observers: List, sample_size: int = 10000
) -> None:
    """Migrate a sample of voters from parquet to database."""
    print(f"Loading voterfile (sample: {sample_size} voters)...")
    df = pl.read_parquet(voterfile_path)
    
    # Take only a sample
    df = df.head(sample_size)
    
    total = len(df)
    print(f"Processing {total} voters...")

    scorer = LikelihoodScorerFactory.create_scorer()
    batch_size = 1000
    processed = 0

    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch_df = df[batch_start:batch_end]

        voters = []
        for row in batch_df.iter_rows(named=True):
            try:
                party_2024 = map_party_code(row.get("PRI24", ""))
                party_2022 = map_party_code(row.get("PRI22", ""))

                party = party_2024 if party_2024 != "Unknown" else party_2022

                voter_data = {
                    "pri24": row.get("PRI24", ""),
                    "pri22": row.get("PRI22", ""),
                    "age": row.get("age"),
                }

                scores = scorer.score_voter(voter_data)

                voter = Voter(
                    vuid=row["VUID"],
                    county=row.get("COUNTY"),
                    age=row.get("age"),
                    age_bracket=row.get("age_bracket"),
                    dob=row.get("DOB"),
                    newcd=row.get("NEWCD"),
                    newsd=row.get("NEWSD"),
                    newhd=row.get("NEWHD"),
                    pri24=row.get("PRI24"),
                    pri22=row.get("PRI22"),
                    party_2024=party_2024,
                    party_2022=party_2022,
                    party=party,
                    **scores,
                )

                voters.append(voter)

            except Exception as e:
                for observer in observers:
                    observer.on_error(e, {"row": row})

        session.add_all(voters)
        await session.commit()

        processed = batch_end
        for observer in observers:
            observer.on_progress(processed, total, f"Processed {processed} voters")

    print(f"Migrated {processed} voters")


async def migrate_early_voting(
    session: AsyncSession, early_voting_path: Path, observers: List, sample_size: int = 5000
) -> None:
    """Migrate a sample of early voting data."""
    print(f"Loading early voting data (sample: {sample_size} records)...")
    df = pl.read_parquet(early_voting_path)
    
    # Take only a sample
    df = df.head(sample_size)
    
    total = len(df)
    print(f"Processing {total} early voting records...")

    batch_size = 1000
    processed = 0

    for batch_start in range(0, total, batch_size):
        batch_end = min(batch_start + batch_size, total)
        batch_df = df[batch_start:batch_end]

        early_voting_records = []
        for row in batch_df.iter_rows(named=True):
            try:
                source_file = row.get("source_file", "")
                early_vote_date = extract_date_from_filename(source_file) if source_file else None

                # Handle both VUID and id_voter column names
                vuid = row.get("VUID") or row.get("id_voter")
                if not vuid:
                    continue

                ev_record = EarlyVoting(
                    vuid=vuid,
                    tx_name=row.get("tx_name"),
                    voting_method=row.get("voting_method"),
                    precinct=row.get("precinct"),
                    source_file=source_file,
                    early_vote_date=early_vote_date,
                )

                early_voting_records.append(ev_record)

            except Exception as e:
                for observer in observers:
                    observer.on_error(e, {"row": row})

        session.add_all(early_voting_records)
        await session.commit()

        processed = batch_end
        for observer in observers:
            observer.on_progress(processed, total, f"Processed {processed} early voting records")

    print(f"Migrated {processed} early voting records")

    # Update voter records
    print("Updating voter records with early voting status...")
    result = await session.execute(select(EarlyVoting.vuid))
    early_voter_uids = {row[0] for row in result.all()}

    batch_size = 1000
    for i in range(0, len(early_voter_uids), batch_size):
        batch_uids = list(early_voter_uids)[i : i + batch_size]
        result = await session.execute(select(Voter).where(Voter.vuid.in_(batch_uids)))
        voters = result.scalars().all()

        for voter in voters:
            voter.actual_voted_early = True

        await session.commit()

    print("Updated voter records with early voting status")

    # Calculate prediction accuracy
    print("Calculating prediction accuracy...")
    result = await session.execute(select(Voter))
    voters = result.scalars().all()

    scorer = LikelihoodScorerFactory.create_scorer()
    updated = 0

    for voter in voters:
        voter_data = {
            "voting_method_likelihood_early": voter.voting_method_likelihood_early or 0.5,
            "actual_voted_early": voter.actual_voted_early,
        }

        accuracy = scorer.prediction_accuracy_strategy.calculate(voter_data)
        voter.prediction_accuracy = accuracy.get("prediction_accuracy")

        updated += 1
        if updated % 1000 == 0:
            await session.commit()

    await session.commit()
    print(f"Calculated prediction accuracy for {updated} voters")


async def migrate_turnout_metrics(
    session: AsyncSession, turnout_dir: Path, observers: List
) -> None:
    """Migrate turnout metrics from CSV to database."""
    print("Loading turnout metrics...")

    turnout_files = {
        "congressional_2022": turnout_dir / "turnout_by_district_2022_congressional.csv",
        "senate_2022": turnout_dir / "turnout_by_district_2022_senate.csv",
        "senate_2026": turnout_dir / "turnout_by_district_2026.csv",
    }

    from src.models.turnout import DistrictType

    district_type_map = {
        "congressional_2022": DistrictType.CONGRESSIONAL_2022,
        "senate_2022": DistrictType.SENATE_2022,
        "senate_2026": DistrictType.SENATE_2026,
    }

    total_processed = 0

    for file_key, file_path in turnout_files.items():
        if not file_path.exists():
            print(f"Warning: {file_path} not found, skipping...")
            continue

        print(f"Processing {file_path.name}...")
        df = pl.read_csv(file_path)

        district_type = district_type_map[file_key]
        records = []

        for row in df.iter_rows(named=True):
            try:
                metric = TurnoutMetrics(
                    district_type=district_type,
                    district_id=str(row.get("district_id", "")),
                    district_name=row.get("district_name"),
                    total_voters=int(row.get("total_voters", 0)),
                    early_voters=int(row.get("early_voters", 0)),
                    turnout_rate=float(row.get("turnout_rate", 0.0)),
                )

                records.append(metric)

            except Exception as e:
                for observer in observers:
                    observer.on_error(e, {"row": row, "file": file_path.name})

        session.add_all(records)
        await session.commit()

        total_processed += len(records)
        print(f"Migrated {len(records)} turnout metrics from {file_path.name}")

    for observer in observers:
        observer.on_progress(total_processed, total_processed, "Turnout metrics migration complete")


async def main():
    """Main migration function."""
    print("=" * 80)
    print("Starting TEST database migration (small sample)...")
    print("=" * 80)

    observers = [
        ProgressObserver(),
        ErrorObserver(),
        StatisticsObserver(),
    ]

    engine, session_factory = DatabaseConnectionFactory.create_connection(
        db_type=Config.DATABASE_TYPE, connection_string=Config.get_database_url()
    )

    await init_db(engine)

    async with session_factory() as session:
        try:
            await migrate_voters(session, Config.PROCESSED_VOTERFILE, observers, sample_size=10000)
            await migrate_early_voting(session, Config.PROCESSED_EARLY_VOTING, observers, sample_size=5000)
            await migrate_turnout_metrics(session, Config.TURNOUT_DIR, observers)

            stats = {}
            for observer in observers:
                if isinstance(observer, StatisticsObserver):
                    stats = observer.get_statistics()

            for observer in observers:
                observer.on_complete(stats)

            print("=" * 80)
            print("TEST Migration completed successfully!")
            print("=" * 80)

        except Exception as e:
            for observer in observers:
                observer.on_error(e)
            raise

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

