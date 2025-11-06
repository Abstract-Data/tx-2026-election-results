"""
Merge voterfile data with early voting data.
Join on voter ID and append voting status and party information.
"""
import polars as pl
from pathlib import Path


def map_party_code(party_code: str) -> str:
    """
    Map party codes to full party names.
    
    Args:
        party_code: Party code from PRI columns (e.g., "RE", "DE", etc.)
    
    Returns:
        Full party name or "Unknown" if code is not recognized
    """
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


def merge_voter_data(
    voterfile_df: pl.DataFrame,
    early_voting_df: pl.DataFrame,
    output_path: str = None
) -> pl.DataFrame:
    """
    Merge voterfile with early voting data on voter ID.
    
    Args:
        voterfile_df: Processed voterfile DataFrame
        early_voting_df: Processed early voting DataFrame
        output_path: Optional path to save merged data as parquet
    
    Returns:
        Merged DataFrame with voter info and early voting status
    """
    print("Merging voterfile with early voting data...")
    
    # Rename columns for join
    # Voterfile uses VUID, early voting uses id_voter
    early_voting_df = early_voting_df.rename({"id_voter": "VUID"})
    
    # Ensure VUID is same type in both dataframes
    voterfile_df = voterfile_df.with_columns([
        pl.col("VUID").cast(pl.Int64)
    ])
    early_voting_df = early_voting_df.with_columns([
        pl.col("VUID").cast(pl.Int64)
    ])
    
    # Left join to keep all voters, add early voting info where available
    merged_df = voterfile_df.join(
        early_voting_df,
        on="VUID",
        how="left"
    )
    
    # Create voted_early boolean column
    merged_df = merged_df.with_columns([
        pl.col("tx_name").is_not_null().alias("voted_early")
    ])
    
    # Map party codes to full names for all available PRI columns
    print("Mapping party codes from last 4 primaries...")
    
    # Check which PRI columns exist (PRI24, PRI22, PRI20, PRI18)
    pri_cols_available = []
    for col in ["PRI24", "PRI22", "PRI20", "PRI18"]:
        if col in merged_df.columns:
            pri_cols_available.append(col)
            merged_df = merged_df.with_columns([
                pl.col(col).map_elements(
                    map_party_code,
                    return_dtype=pl.Utf8
                ).alias(f"party_{col}")
            ])
    
    print(f"Found primary columns: {pri_cols_available}")
    
    # Calculate party affiliation based on last 4 primaries
    # Strategy: Count Republican and Democrat votes across ALL available primaries
    # Classify as:
    # - "Republican": If R votes > D votes (and at least 1 R vote)
    # - "Democrat": If D votes > R votes (and at least 1 D vote)
    # - "Swing": If R == D, or mixed patterns, or only 1 vote total
    # - "Unknown": No votes in any primary
    
    # Count Republican and Democrat votes across last 4 primaries
    rep_votes = pl.lit(0)
    dem_votes = pl.lit(0)
    
    for col in pri_cols_available:
        party_col = f"party_{col}"
        rep_votes = rep_votes + (pl.col(party_col) == "Republican").cast(pl.Int32)
        dem_votes = dem_votes + (pl.col(party_col) == "Democrat").cast(pl.Int32)
    
    # Calculate total votes
    total_primary_votes = rep_votes + dem_votes
    
    # Determine party affiliation
    merged_df = merged_df.with_columns([
        rep_votes.alias("rep_primary_votes"),
        dem_votes.alias("dem_primary_votes"),
        total_primary_votes.alias("total_primary_votes"),
    ])
    
    # Classify based on voting pattern across all available primaries
    # Strategy:
    # - If someone has ONLY Republican votes (D=0) → Republican
    # - If someone has ONLY Democrat votes (R=0) → Democrat
    # - If someone has BOTH R and D votes (mixed pattern) → Swing
    # - If no votes → Unknown
    # Examples:
    #   2 R's, 0 D's → Republican
    #   1 R, 2 D's → Swing (mixed pattern)
    #   2 R's, 1 D → Swing (mixed pattern)
    #   1 R, 1 D → Swing (mixed pattern)
    merged_df = merged_df.with_columns([
        pl.when(pl.col("total_primary_votes") == 0)
        .then(pl.lit("Unknown"))
        # If has BOTH R and D votes → Swing (mixed pattern)
        .when((pl.col("rep_primary_votes") > 0) & (pl.col("dem_primary_votes") > 0))
        .then(pl.lit("Swing"))
        # If ONLY Republican votes (D=0, R>0) → Republican
        .when((pl.col("rep_primary_votes") > 0) & (pl.col("dem_primary_votes") == 0))
        .then(pl.lit("Republican"))
        # If ONLY Democrat votes (R=0, D>0) → Democrat
        .when((pl.col("dem_primary_votes") > 0) & (pl.col("rep_primary_votes") == 0))
        .then(pl.lit("Democrat"))
        # Default fallback (shouldn't happen, but just in case)
        .otherwise(pl.lit("Unknown"))
        .alias("party")
    ])
    
    # For backward compatibility, also create party_2024 and party_2022
    if "party_PRI24" in merged_df.columns:
        merged_df = merged_df.with_columns([
            pl.col("party_PRI24").alias("party_2024")
        ])
    if "party_PRI22" in merged_df.columns:
        merged_df = merged_df.with_columns([
            pl.col("party_PRI22").alias("party_2022")
        ])
    
    # Show summary statistics
    print(f"\nMerged data summary:")
    print(f"Total voters: {len(merged_df)}")
    print(f"Early voters: {merged_df['voted_early'].sum()}")
    print(f"Early voting rate: {merged_df['voted_early'].mean() * 100:.2f}%")
    
    print(f"\nParty distribution:")
    print(merged_df.group_by("party").agg(pl.count()).sort("party"))
    
    print(f"\nEarly voting by party:")
    if "party" in merged_df.columns:
        print(
            merged_df.group_by("party")
            .agg([
                pl.count().alias("total"),
                pl.col("voted_early").sum().alias("early_voters"),
                (pl.col("voted_early").mean() * 100).alias("early_vote_rate")
            ])
            .sort("party")
        )
    
    # Save to parquet if output path provided
    if output_path:
        print(f"\nSaving merged data to {output_path}...")
        merged_df.write_parquet(output_path)
        print("Saved!")
    
    return merged_df


if __name__ == "__main__":
    # Load processed data
    voterfile_path = "processed_voterfile.parquet"
    early_voting_path = "processed_early_voting.parquet"
    output_path = "early_voting_merged.parquet"
    
    if not Path(voterfile_path).exists():
        print(f"Error: {voterfile_path} not found. Please run process_voterfile.py first.")
        exit(1)
    
    if not Path(early_voting_path).exists():
        print(f"Error: {early_voting_path} not found. Please run process_early_voting.py first.")
        exit(1)
    
    voterfile_df = pl.read_parquet(voterfile_path)
    early_voting_df = pl.read_parquet(early_voting_path)
    
    merged_df = merge_voter_data(voterfile_df, early_voting_df, output_path)
    print("\nFirst few rows:")
    print(merged_df.head())

