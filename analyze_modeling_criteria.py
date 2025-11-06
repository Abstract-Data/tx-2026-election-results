"""
Analyze how the modeling system determines which voters to model.
"""

import polars as pl
from pathlib import Path

def analyze_modeling_criteria():
    """Analyze the current modeling criteria and show voter breakdown."""
    
    # Check if we have the modeled file
    modeled_path = Path("voters_with_party_modeling.parquet")
    merged_path = Path("early_voting_merged.parquet")
    
    if not modeled_path.exists() and not merged_path.exists():
        print("❌ No data files found. Please run merge_voter_data.py first.")
        return
    
    # Load the file (use lazy scan to avoid memory issues)
    print("=" * 80)
    print("ANALYZING MODELING CRITERIA")
    print("=" * 80)
    print()
    
    # Try to load modeled file first, then fallback to merged
    if modeled_path.exists():
        print(f"Loading {modeled_path}...")
        df_scan = pl.scan_parquet(str(modeled_path))
    else:
        print(f"Loading {merged_path}...")
        df_scan = pl.scan_parquet(str(merged_path))
    
    # Get schema to see available columns
    schema = df_scan.collect_schema()
    schema_names = schema.names()
    
    print(f"\nTotal voters: {df_scan.select(pl.count()).collect().item():,}")
    print(f"\nAvailable columns: {len(schema_names)}")
    
    # Check for key columns
    has_party = "party" in schema_names
    has_primary_votes = "total_primary_votes" in schema_names
    has_gen_cols = any(col.upper().startswith("GEN") for col in schema_names)
    has_voted_early = "voted_early" in schema_names
    has_predicted = "predicted_party_score" in schema_names
    
    print(f"\nKey columns:")
    print(f"  - party: {has_party}")
    print(f"  - total_primary_votes: {has_primary_votes}")
    print(f"  - GEN columns: {has_gen_cols}")
    print(f"  - voted_early: {has_voted_early}")
    print(f"  - predicted_party_score: {has_predicted}")
    
    if has_gen_cols:
        gen_cols = [col for col in schema_names if col.upper().startswith("GEN")]
        print(f"  - GEN columns found: {gen_cols[:5]}..." if len(gen_cols) > 5 else f"  - GEN columns found: {gen_cols}")
    
    print("\n" + "=" * 80)
    print("CURRENT MODELING CRITERIA")
    print("=" * 80)
    print()
    
    # Analyze party distribution
    if has_party:
        party_counts = (
            df_scan
            .select("party")
            .group_by("party")
            .agg(pl.count().alias("count"))
            .sort("count", descending=True)
            .collect()
        )
        print("Party Distribution:")
        for row in party_counts.iter_rows(named=True):
            pct = (row["count"] / df_scan.select(pl.count()).collect().item()) * 100
            print(f"  {row['party']:15s}: {row['count']:>12,} ({pct:5.2f}%)")
    
    print("\n" + "=" * 80)
    print("CURRENT MODELING LOGIC (from model_party_affiliation.py)")
    print("=" * 80)
    print()
    print("Voters are modeled if:")
    print("  party == 'Unknown' OR")
    print("  party == 'Other' OR")
    print("  party is null")
    print()
    print("This means ALL voters with no primary history get modeled,")
    print("regardless of whether they have general election history.")
    print()
    
    # Check how many "Unknown" voters have general election history
    if has_party and has_primary_votes:
        print("=" * 80)
        print("BREAKDOWN OF 'Unknown' VOTERS")
        print("=" * 80)
        print()
        
        # Count unknown voters
        unknown_total = (
            df_scan
            .filter(pl.col("party") == "Unknown")
            .select(pl.count())
            .collect()
            .item()
        )
        print(f"Total 'Unknown' voters: {unknown_total:,}")
        
        # Check primary votes
        unknown_no_primary = (
            df_scan
            .filter(
                (pl.col("party") == "Unknown") &
                ((pl.col("total_primary_votes") == 0) | pl.col("total_primary_votes").is_null())
            )
            .select(pl.count())
            .collect()
            .item()
        )
        print(f"  - With no primary votes: {unknown_no_primary:,}")
        
        # Check general election history
        if has_gen_cols:
            gen_cols_list = [col for col in schema_names if col.upper().startswith("GEN")]
            
            # Build condition: has at least one GEN column with a value
            has_gen_condition = (
                pl.col(gen_cols_list[0]).is_not_null() &
                (pl.col(gen_cols_list[0]) != "")
            )
            for gen_col in gen_cols_list[1:]:
                has_gen_condition = has_gen_condition | (
                    pl.col(gen_col).is_not_null() &
                    (pl.col(gen_col) != "")
                )
            
            unknown_with_gen = (
                df_scan
                .filter(
                    (pl.col("party") == "Unknown") &
                    ((pl.col("total_primary_votes") == 0) | pl.col("total_primary_votes").is_null()) &
                    has_gen_condition
                )
                .select(pl.count())
                .collect()
                .item()
            )
            print(f"  - With GEN history but no primary: {unknown_with_gen:,}")
            
            unknown_no_gen = unknown_no_primary - unknown_with_gen
            print(f"  - With no GEN history and no primary: {unknown_no_gen:,}")
        
        elif has_voted_early:
            unknown_with_early = (
                df_scan
                .filter(
                    (pl.col("party") == "Unknown") &
                    ((pl.col("total_primary_votes") == 0) | pl.col("total_primary_votes").is_null()) &
                    (pl.col("voted_early") == True)
                )
                .select(pl.count())
                .collect()
                .item()
            )
            print(f"  - With voted_early=True but no primary: {unknown_with_early:,}")
    
    print("\n" + "=" * 80)
    print("DESIRED MODELING CRITERIA (for 'secret' voters)")
    print("=" * 80)
    print()
    print("Should only model voters who:")
    print("  1. Have general election history (GEN columns or voted_early)")
    print("  2. Have NO primary history (total_primary_votes == 0)")
    print()
    print("This excludes:")
    print("  - Voters with no voting history at all")
    print("  - Voters who have voted in primaries")
    print()
    
    # Show what the new criteria would select
    if has_party and has_primary_votes:
        print("=" * 80)
        print("VOTERS THAT WOULD BE MODELED WITH NEW CRITERIA")
        print("=" * 80)
        print()
        
        if has_gen_cols:
            gen_cols_list = [col for col in schema_names if col.upper().startswith("GEN")]
            
            # Build condition: has at least one GEN column with a value
            has_gen_condition = (
                pl.col(gen_cols_list[0]).is_not_null() &
                (pl.col(gen_cols_list[0]) != "")
            )
            for gen_col in gen_cols_list[1:]:
                has_gen_condition = has_gen_condition | (
                    pl.col(gen_col).is_not_null() &
                    (pl.col(gen_col) != "")
                )
            
            secret_voters = (
                df_scan
                .filter(
                    (pl.col("party") == "Unknown") &
                    ((pl.col("total_primary_votes") == 0) | pl.col("total_primary_votes").is_null()) &
                    has_gen_condition
                )
                .select(pl.count())
                .collect()
                .item()
            )
            
        elif has_voted_early:
            secret_voters = (
                df_scan
                .filter(
                    (pl.col("party") == "Unknown") &
                    ((pl.col("total_primary_votes") == 0) | pl.col("total_primary_votes").is_null()) &
                    (pl.col("voted_early") == True)
                )
                .select(pl.count())
                .collect()
                .item()
            )
        else:
            secret_voters = 0
        
        print(f"Secret voters (GEN history but no primary): {secret_voters:,}")
        
        if has_predicted:
            currently_modeled = (
                df_scan
                .filter(pl.col("predicted_party_score").is_not_null())
                .select(pl.count())
                .collect()
                .item()
            )
            print(f"Currently modeled voters: {currently_modeled:,}")
            print(f"Difference: {currently_modeled - secret_voters:+,} voters")
    
    print("\n" + "=" * 80)
    print("RECOMMENDATION")
    print("=" * 80)
    print()
    print("Update model_party_affiliation.py to filter unknown_voters to only include:")
    print("  - party == 'Unknown'")
    print("  - total_primary_votes == 0 (or null)")
    print("  - Has general election history (GEN columns or voted_early)")
    print()

if __name__ == "__main__":
    analyze_modeling_criteria()

