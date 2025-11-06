"""
Classify voters with primary history as Republican, Democrat, or Swing
based on their voting patterns across the last 4 primaries.
"""
import polars as pl
from typing import Literal


def classify_primary_voters(df: pl.DataFrame) -> pl.DataFrame:
    """
    Classify voters with primary history as R/D/Swing based on last 4 primaries.
    
    Classification logic:
    - Republican: Only Republican votes (no Democrat votes)
    - Democrat: Only Democrat votes (no Republican votes)
    - Swing: Mixed R/D votes (has both Republican and Democrat votes)
    - Unknown: No primary votes
    
    Args:
        df: DataFrame with PRI24, PRI22, PRI20, PRI18 columns and party_* columns
        
    Returns:
        DataFrame with primary_classification column added
    """
    print("=" * 80)
    print("CLASSIFYING PRIMARY VOTERS")
    print("=" * 80)
    print()
    
    # Check which PRI columns exist
    pri_cols = ["PRI24", "PRI22", "PRI20", "PRI18"]
    pri_cols_available = [col for col in pri_cols if col in df.columns]
    
    if not pri_cols_available:
        print("⚠️  No PRI columns found. Cannot classify primary voters.")
        return df.with_columns([pl.lit("Unknown").alias("primary_classification")])
    
    print(f"Found primary columns: {pri_cols_available}")
    
    # Check if party_* columns exist (from merge.py processing)
    party_cols = [f"party_{col}" for col in pri_cols_available]
    party_cols_available = [col for col in party_cols if col in df.columns]
    
    if not party_cols_available:
        print("⚠️  No party_* columns found. Primary classification may not work correctly.")
        print("   Make sure merge_voter_data has been run first.")
        return df.with_columns([pl.lit("Unknown").alias("primary_classification")])
    
    # Count Republican and Democrat votes across last 4 primaries
    # Use existing rep_primary_votes and dem_primary_votes if available
    if "rep_primary_votes" in df.columns and "dem_primary_votes" in df.columns:
        rep_votes = pl.col("rep_primary_votes")
        dem_votes = pl.col("dem_primary_votes")
        total_votes = pl.col("total_primary_votes")
    else:
        # Calculate from party_* columns
        rep_votes = pl.lit(0)
        dem_votes = pl.lit(0)
        
        for col in party_cols_available:
            rep_votes = rep_votes + (pl.col(col) == "Republican").cast(pl.Int32)
            dem_votes = dem_votes + (pl.col(col) == "Democrat").cast(pl.Int32)
        
        total_votes = rep_votes + dem_votes
    
    # Classify based on voting pattern
    # - If has BOTH R and D votes → Swing (mixed pattern)
    # - If ONLY Republican votes (D=0, R>0) → Republican
    # - If ONLY Democrat votes (R=0, D>0) → Democrat
    # - If no votes → Unknown
    df_classified = df.with_columns([
        pl.when(total_votes == 0)
        .then(pl.lit("Unknown"))
        # If has BOTH R and D votes → Swing (mixed pattern)
        .when((rep_votes > 0) & (dem_votes > 0))
        .then(pl.lit("Swing"))
        # If ONLY Republican votes (D=0, R>0) → Republican
        .when((rep_votes > 0) & (dem_votes == 0))
        .then(pl.lit("Republican"))
        # If ONLY Democrat votes (R=0, D>0) → Democrat
        .when((dem_votes > 0) & (rep_votes == 0))
        .then(pl.lit("Democrat"))
        # Default fallback
        .otherwise(pl.lit("Unknown"))
        .alias("primary_classification")
    ])
    
    # Add detailed breakdown columns for analysis
    df_classified = df_classified.with_columns([
        rep_votes.alias("rep_primary_votes"),
        dem_votes.alias("dem_primary_votes"),
        total_votes.alias("total_primary_votes"),
    ])
    
    # Summary statistics
    print("\nPrimary Voter Classification Summary:")
    print("-" * 80)
    classification_counts = df_classified.group_by("primary_classification").agg([
        pl.count().alias("count"),
        (pl.count() / pl.len() * 100).alias("percentage")
    ]).sort("primary_classification")
    
    print(classification_counts)
    
    # Detailed breakdown by primary history
    print("\nDetailed Breakdown:")
    print("-" * 80)
    detailed = df_classified.filter(
        pl.col("primary_classification").is_in(["Republican", "Democrat", "Swing"])
    ).group_by([
        "primary_classification",
        "rep_primary_votes",
        "dem_primary_votes"
    ]).agg(pl.count().alias("count")).sort([
        "primary_classification",
        "rep_primary_votes",
        "dem_primary_votes"
    ])
    
    print(detailed.head(20))
    
    print()
    print("=" * 80)
    print("Classification complete!")
    print("=" * 80)
    print()
    
    return df_classified


if __name__ == "__main__":
    # Test with sample data
    import sys
    from pathlib import Path
    
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        from tx_election_results.config import config
        input_path = str(config.MERGED_DATA)
    
    if not Path(input_path).exists():
        print(f"Error: {input_path} not found.")
        print("Please run the merge step first.")
        sys.exit(1)
    
    print(f"Loading data from {input_path}...")
    df = pl.read_parquet(input_path)
    print(f"Loaded {len(df):,} voters")
    
    df_classified = classify_primary_voters(df)
    
    print(f"\nClassification added to {len(df_classified):,} voters")
    print("\nFirst few rows:")
    print(df_classified.select([
        "VUID",
        "primary_classification",
        "rep_primary_votes",
        "dem_primary_votes",
        "total_primary_votes"
    ]).head(10))

