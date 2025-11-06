"""
Analyze modeled voters broken out by district type (HD, CD, SD).
Shows party distribution, voter counts, and party advantage by district.
"""
import polars as pl
import pandas as pd
from pathlib import Path


def analyze_modeled_by_district_type():
    """Break out modeled voters by district type."""
    print("=" * 80)
    print("MODELED VOTERS ANALYSIS BY DISTRICT TYPE")
    print("=" * 80)
    print()
    
    # Load modeled data
    print("Loading modeled voter data...")
    modeled_df = pl.read_parquet("voters_with_party_modeling.parquet")
    print(f"Loaded {len(modeled_df):,} voters")
    print()
    
    # Create output directory
    output_dir = Path("modeled_voters_by_district_type")
    output_dir.mkdir(exist_ok=True)
    
    # Analyze each district type
    district_types = [
        ("HD", "2026_HD", "House District"),
        ("CD", "2026_CD", "Congressional District"),
        ("SD", "2026_SD", "State Senate District")
    ]
    
    all_summaries = []
    
    for dist_type, dist_col, dist_name in district_types:
        print("=" * 80)
        print(f"{dist_name.upper()} ({dist_type}) ANALYSIS")
        print("=" * 80)
        print()
        
        # Filter to voters with district assignments
        df = modeled_df.filter(pl.col(dist_col).is_not_null())
        
        # Map party to R/D for analysis
        df = df.with_columns([
            pl.when(pl.col("party_final") == "Republican")
            .then(pl.lit("Republican"))
            .when(pl.col("party_final") == "Democrat")
            .then(pl.lit("Democrat"))
            .when(pl.col("party_final").str.contains("Republican"))
            .then(pl.lit("Republican"))
            .when(pl.col("party_final").str.contains("Democrat"))
            .then(pl.lit("Democrat"))
            .when(pl.col("party_final") == "Swing")
            .then(pl.lit("Swing"))
            .otherwise(pl.lit("Unknown"))
            .alias("party_simple")
        ])
        
        # Determine voter type based on party vs party_final
        df = df.with_columns([
            pl.when(pl.col("party").is_in(["Republican", "Democrat", "Swing"]))
            .then(pl.lit("Known_Primary"))
            .when(pl.col("predicted_party_score").is_not_null())
            .then(pl.lit("Modeled"))
            .otherwise(pl.lit("Unknown"))
            .alias("voter_type")
        ])
        
        # Calculate party composition by district
        district_party = df.group_by([dist_col, "party_simple"]).agg([
            pl.len().alias("voter_count"),
            (pl.col("voter_type") == "Known_Primary").sum().alias("known_voters"),
            (pl.col("voter_type") == "Modeled").sum().alias("modeled_voters"),
        ]).sort([dist_col, "party_simple"])
        
        # Pivot to get R/D counts per district
        district_party_pd = district_party.to_pandas()
        district_summary = district_party_pd.pivot_table(
            index=dist_col,
            columns="party_simple",
            values="voter_count",
            fill_value=0
        ).reset_index()
        
        # Calculate known vs modeled breakdown
        district_known_modeled = district_party_pd.pivot_table(
            index=dist_col,
            columns="party_simple",
            values=["known_voters", "modeled_voters"],
            fill_value=0
        )
        
        # Flatten column names
        if isinstance(district_known_modeled.columns, pd.MultiIndex):
            district_known_modeled.columns = [
                f"{col[0]}_{col[1]}" for col in district_known_modeled.columns
            ]
            district_known_modeled = district_known_modeled.reset_index()
        else:
            district_known_modeled = district_known_modeled.reset_index()
        
        # Ensure we have R and D columns
        for party in ["Republican", "Democrat", "Swing", "Unknown"]:
            if party not in district_summary.columns:
                district_summary[party] = 0
        
        # Calculate totals and net advantage
        district_summary["total_voters"] = (
            district_summary.get("Republican", 0) +
            district_summary.get("Democrat", 0) +
            district_summary.get("Swing", 0) +
            district_summary.get("Unknown", 0)
        )
        
        district_summary["republican_voters"] = district_summary.get("Republican", 0)
        district_summary["democrat_voters"] = district_summary.get("Democrat", 0)
        district_summary["swing_voters"] = district_summary.get("Swing", 0)
        
        district_summary["net_advantage"] = (
            district_summary["republican_voters"] - district_summary["democrat_voters"]
        )
        district_summary["net_advantage_pct"] = (
            district_summary["net_advantage"] / district_summary["total_voters"] * 100
        ).fillna(0)
        
        district_summary["rep_pct"] = (
            district_summary["republican_voters"] / district_summary["total_voters"] * 100
        ).fillna(0)
        district_summary["dem_pct"] = (
            district_summary["democrat_voters"] / district_summary["total_voters"] * 100
        ).fillna(0)
        
        # Merge known/modeled breakdown
        rep_known_col = f"known_voters_Republican"
        rep_modeled_col = f"modeled_voters_Republican"
        dem_known_col = f"known_voters_Democrat"
        dem_modeled_col = f"modeled_voters_Democrat"
        
        if rep_known_col in district_known_modeled.columns:
            district_summary = district_summary.merge(
                district_known_modeled[[dist_col, rep_known_col, rep_modeled_col, dem_known_col, dem_modeled_col]],
                on=dist_col,
                how="left"
            ).fillna(0)
        else:
            district_summary[rep_known_col] = 0
            district_summary[rep_modeled_col] = 0
            district_summary[dem_known_col] = 0
            district_summary[dem_modeled_col] = 0
        
        # Rename district column
        district_summary = district_summary.rename(columns={dist_col: "district"})
        
        # Sort by net advantage
        district_summary = district_summary.sort_values("net_advantage", ascending=False)
        
        # Save detailed breakdown
        output_file = output_dir / f"{dist_type.lower()}_modeled_voters_by_district.csv"
        district_summary.to_csv(output_file, index=False)
        print(f"✅ Saved detailed breakdown to {output_file}")
        
        # Summary statistics
        total_districts = len(district_summary)
        total_voters = district_summary["total_voters"].sum()
        total_rep = district_summary["republican_voters"].sum()
        total_dem = district_summary["democrat_voters"].sum()
        total_swing = district_summary["swing_voters"].sum()
        net_advantage = total_rep - total_dem
        
        # Classify districts
        threshold = 1000
        rep_advantage = len(district_summary[district_summary["net_advantage"] > threshold])
        dem_advantage = len(district_summary[district_summary["net_advantage"] < -threshold])
        competitive = total_districts - rep_advantage - dem_advantage
        
        print()
        print(f"SUMMARY STATISTICS:")
        print(f"  Total Districts: {total_districts}")
        print(f"  Total Voters: {total_voters:,}")
        print()
        print(f"Party Composition:")
        print(f"  Republican: {total_rep:,} ({total_rep/total_voters*100:.2f}%)")
        print(f"  Democrat: {total_dem:,} ({total_dem/total_voters*100:.2f}%)")
        print(f"  Swing: {total_swing:,} ({total_swing/total_voters*100:.2f}%)")
        print(f"  Net Advantage: {net_advantage:+,} (Republican)")
        print()
        print(f"District Classification:")
        print(f"  Republican Advantage: {rep_advantage} districts ({rep_advantage/total_districts*100:.1f}%)")
        print(f"  Democrat Advantage: {dem_advantage} districts ({dem_advantage/total_districts*100:.1f}%)")
        print(f"  Competitive/Swing: {competitive} districts ({competitive/total_districts*100:.1f}%)")
        print()
        
        # Top 10 districts by net advantage
        print("Top 10 Districts by Republican Advantage:")
        top_rep = district_summary.head(10)[["district", "republican_voters", "democrat_voters", "net_advantage", "net_advantage_pct"]]
        print(top_rep.to_string(index=False))
        print()
        
        print("Top 10 Districts by Democrat Advantage:")
        top_dem = district_summary.tail(10)[["district", "republican_voters", "democrat_voters", "net_advantage", "net_advantage_pct"]].sort_values("net_advantage")
        print(top_dem.to_string(index=False))
        print()
        
        # Create summary row for overall comparison
        summary_row = {
            "district_type": dist_type,
            "total_districts": total_districts,
            "total_voters": total_voters,
            "republican_voters": total_rep,
            "democrat_voters": total_dem,
            "swing_voters": total_swing,
            "net_advantage": net_advantage,
            "rep_advantage_districts": rep_advantage,
            "dem_advantage_districts": dem_advantage,
            "competitive_districts": competitive,
            "rep_pct": total_rep / total_voters * 100,
            "dem_pct": total_dem / total_voters * 100,
        }
        all_summaries.append(summary_row)
    
    # Create comprehensive summary
    print()
    print("=" * 80)
    print("COMPREHENSIVE SUMMARY - ALL DISTRICT TYPES")
    print("=" * 80)
    print()
    
    summary_df = pd.DataFrame(all_summaries)
    summary_file = output_dir / "modeled_voters_summary_by_district_type.csv"
    summary_df.to_csv(summary_file, index=False)
    print(summary_df.to_string(index=False))
    print()
    print(f"✅ Summary saved to {summary_file}")
    print()
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nAll results saved to: {output_dir}/")


if __name__ == "__main__":
    analyze_modeled_by_district_type()

