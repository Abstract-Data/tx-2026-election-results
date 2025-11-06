"""
Analyze voter differences between 2022 and 2026 districts by district type.
Shows voter counts, gains/losses, and transitions between old and new districts.
"""
import polars as pl
import pandas as pd
from pathlib import Path


def analyze_voter_differences_by_district_type():
    """Analyze voter differences between 2022 and 2026 for HD, CD, and SD."""
    print("=" * 80)
    print("VOTER DIFFERENCES: 2022 vs 2026 DISTRICTS BY TYPE")
    print("=" * 80)
    print()
    
    # Load modeled voter data
    modeled_data_path = Path("voters_with_party_modeling.parquet")
    if not modeled_data_path.exists():
        print(f"Error: {modeled_data_path} not found. Please run main.py first.")
        return
    
    df = pl.read_parquet(str(modeled_data_path))
    print(f"Loaded {len(df):,} voters")
    print()
    
    output_dir = Path("voter_differences_2022_2026")
    output_dir.mkdir(exist_ok=True)
    
    # Analyze each district type
    for dist_type, old_col, new_col in [
        ("HD", "NEWHD", "2026_HD"),
        ("CD", "NEWCD", "2026_CD"),
        ("SD", "NEWSD", "2026_SD"),
    ]:
        print("=" * 80)
        print(f"{dist_type} DISTRICT ANALYSIS")
        print("=" * 80)
        print()
        
        # Filter to voters with both old and new districts (excluding district 0)
        df_dist = df.filter(
            pl.col(old_col).is_not_null() &
            pl.col(new_col).is_not_null() &
            (pl.col(old_col) != 0) &
            (pl.col(new_col) != 0)
        )
        
        print(f"Voters with both old and new districts: {len(df_dist):,}")
        print()
        
        # 1. Voter counts by old districts (2022)
        print("1. VOTER COUNTS BY 2022 DISTRICTS:")
        old_district_counts = df_dist.group_by(old_col).agg([
            pl.len().alias("voter_count")
        ]).sort(old_col)
        
        # For HD, ensure all districts 1-150 are included
        if dist_type == "HD":
            all_old_districts = pl.DataFrame({old_col: range(1, 151)})
            old_district_counts = all_old_districts.join(
                old_district_counts, on=old_col, how="left"
            ).with_columns(pl.col("voter_count").fill_null(0))
        
        old_total = old_district_counts["voter_count"].sum()
        old_unique = old_district_counts.filter(pl.col("voter_count") > 0).height
        print(f"  Total districts in 2022: {len(old_district_counts)}")
        print(f"  Districts with voters: {old_unique}")
        print(f"  Total voters: {old_total:,}")
        print(f"  Average voters per district: {old_total / len(old_district_counts):,.0f}")
        print(f"  Min voters per district: {old_district_counts['voter_count'].min():,}")
        print(f"  Max voters per district: {old_district_counts['voter_count'].max():,}")
        print()
        
        # 2. Voter counts by new districts (2026)
        print("2. VOTER COUNTS BY 2026 DISTRICTS:")
        new_district_counts = df_dist.group_by(new_col).agg([
            pl.len().alias("voter_count")
        ]).sort(new_col)
        
        # For HD, ensure all districts 1-150 are included
        if dist_type == "HD":
            all_new_districts = pl.DataFrame({new_col: range(1, 151)})
            new_district_counts = all_new_districts.join(
                new_district_counts, on=new_col, how="left"
            ).with_columns(pl.col("voter_count").fill_null(0))
        
        new_total = new_district_counts["voter_count"].sum()
        new_unique = new_district_counts.filter(pl.col("voter_count") > 0).height
        print(f"  Total districts in 2026: {len(new_district_counts)}")
        print(f"  Districts with voters: {new_unique}")
        print(f"  Total voters: {new_total:,}")
        print(f"  Average voters per district: {new_total / len(new_district_counts):,.0f}")
        print(f"  Min voters per district: {new_district_counts['voter_count'].min():,}")
        print(f"  Max voters per district: {new_district_counts['voter_count'].max():,}")
        print()
        
        # 3. District-by-district comparison
        print("3. DISTRICT-BY-DISTRICT COMPARISON:")
        old_counts_pd = old_district_counts.rename({old_col: "district"})
        new_counts_pd = new_district_counts.rename({new_col: "district"})
        
        comparison = old_counts_pd.join(
            new_counts_pd,
            on="district",
            how="full",
            suffix="_2026"
        ).with_columns([
            pl.col("voter_count").fill_null(0).cast(pl.Int64).alias("voters_2022"),
            pl.col("voter_count_2026").fill_null(0).cast(pl.Int64).alias("voters_2026"),
        ]).with_columns([
            (pl.col("voters_2026") - pl.col("voters_2022")).alias("net_change"),
            pl.when(pl.col("voters_2022") > 0)
            .then(((pl.col("voters_2026") - pl.col("voters_2022")).cast(pl.Float64) / pl.col("voters_2022").cast(pl.Float64) * 100))
            .otherwise(pl.lit(0.0))
            .alias("pct_change")
        ]).drop(["voter_count", "voter_count_2026"]).sort("district")
        
        comparison_pd = comparison.to_pandas()
        
        # Calculate summary statistics
        districts_with_increase = len(comparison_pd[comparison_pd["net_change"] > 0])
        districts_with_decrease = len(comparison_pd[comparison_pd["net_change"] < 0])
        districts_unchanged = len(comparison_pd[comparison_pd["net_change"] == 0])
        
        print(f"  Districts with voter increase: {districts_with_increase}")
        print(f"  Districts with voter decrease: {districts_with_decrease}")
        print(f"  Districts unchanged: {districts_unchanged}")
        print(f"  Largest increase: {comparison_pd['net_change'].max():,.0f} voters")
        print(f"  Largest decrease: {comparison_pd['net_change'].min():,.0f} voters")
        print()
        
        # 4. Top districts by change
        print("4. TOP 10 DISTRICTS WITH LARGEST VOTER INCREASES (2022 → 2026):")
        top_increases = comparison_pd.nlargest(10, "net_change")[
            ["district", "voters_2022", "voters_2026", "net_change", "pct_change"]
        ]
        print(top_increases.to_string(index=False))
        print()
        
        print("5. TOP 10 DISTRICTS WITH LARGEST VOTER DECREASES (2022 → 2026):")
        top_decreases = comparison_pd.nsmallest(10, "net_change")[
            ["district", "voters_2022", "voters_2026", "net_change", "pct_change"]
        ]
        print(top_decreases.to_string(index=False))
        print()
        
        # 5. Voter transitions (how many voters moved from old district to new district)
        print("6. VOTER TRANSITIONS (OLD → NEW DISTRICTS):")
        transitions = df_dist.group_by([old_col, new_col]).agg([
            pl.len().alias("voter_count")
        ]).sort([old_col, new_col])
        
        # Calculate total transitions
        total_transitions = len(transitions)
        voters_who_stayed = transitions.filter(pl.col(old_col) == pl.col(new_col))["voter_count"].sum()
        voters_who_moved = transitions.filter(pl.col(old_col) != pl.col(new_col))["voter_count"].sum()
        
        print(f"  Total district transitions: {total_transitions:,}")
        print(f"  Voters who stayed in same district: {voters_who_stayed:,} ({voters_who_stayed/len(df_dist)*100:.2f}%)")
        print(f"  Voters who moved to different district: {voters_who_moved:,} ({voters_who_moved/len(df_dist)*100:.2f}%)")
        print()
        
        # Show top transitions (voters moving from one district to another)
        print("7. TOP 20 DISTRICT TRANSITIONS (Most voters moving):")
        top_transitions = transitions.filter(
            pl.col(old_col) != pl.col(new_col)
        ).sort("voter_count", descending=True).head(20)
        
        top_transitions_pd = top_transitions.rename({
            old_col: "old_district",
            new_col: "new_district"
        }).to_pandas()
        print(top_transitions_pd.to_string(index=False))
        print()
        
        # Save detailed reports
        output_file_comparison = output_dir / f"{dist_type.lower()}_district_comparison.csv"
        comparison_pd.to_csv(output_file_comparison, index=False)
        print(f"✅ Saved district comparison to {output_file_comparison}")
        
        output_file_transitions = output_dir / f"{dist_type.lower()}_voter_transitions.csv"
        transitions_pd = transitions.rename({
            old_col: "old_district",
            new_col: "new_district"
        }).to_pandas()
        transitions_pd.to_csv(output_file_transitions, index=False)
        print(f"✅ Saved voter transitions to {output_file_transitions}")
        
        # Summary statistics
        summary_stats = {
            "district_type": dist_type,
            "total_voters": len(df_dist),
            "old_districts_total": len(old_district_counts),
            "old_districts_with_voters": old_unique,
            "new_districts_total": len(new_district_counts),
            "new_districts_with_voters": new_unique,
            "districts_with_increase": districts_with_increase,
            "districts_with_decrease": districts_with_decrease,
            "districts_unchanged": districts_unchanged,
            "voters_who_stayed": voters_who_stayed,
            "voters_who_moved": voters_who_moved,
            "pct_voters_stayed": voters_who_stayed / len(df_dist) * 100,
            "pct_voters_moved": voters_who_moved / len(df_dist) * 100,
            "avg_voters_2022": old_total / len(old_district_counts),
            "avg_voters_2026": new_total / len(new_district_counts),
        }
        
        # Save summary
        summary_file = output_dir / f"{dist_type.lower()}_summary.csv"
        pd.DataFrame([summary_stats]).to_csv(summary_file, index=False)
        print(f"✅ Saved summary statistics to {summary_file}")
        print()
    
    # Create comprehensive summary across all district types
    print("=" * 80)
    print("COMPREHENSIVE SUMMARY ACROSS ALL DISTRICT TYPES")
    print("=" * 80)
    print()
    
    # Load all summaries
    summaries = []
    for dist_type in ["hd", "cd", "sd"]:
        summary_file = output_dir / f"{dist_type}_summary.csv"
        if summary_file.exists():
            summary = pd.read_csv(summary_file)
            summaries.append(summary)
    
    if summaries:
        comprehensive_summary = pd.concat(summaries, ignore_index=True)
        comprehensive_summary_file = output_dir / "comprehensive_summary.csv"
        comprehensive_summary.to_csv(comprehensive_summary_file, index=False)
        print(comprehensive_summary.to_string(index=False))
        print()
        print(f"✅ Saved comprehensive summary to {comprehensive_summary_file}")
    
    print()
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nAll results saved to: {output_dir}/")


if __name__ == "__main__":
    analyze_voter_differences_by_district_type()

