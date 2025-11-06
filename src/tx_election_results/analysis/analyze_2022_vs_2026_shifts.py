"""
Analyze how district maps shifted between 2022 and 2026 using modeled voters.
Shows party composition changes and advantage shifts for each district type.
"""
import polars as pl
import pandas as pd
from pathlib import Path


def analyze_2022_vs_2026_shifts():
    """Compare 2022 vs 2026 district maps using modeled voters."""
    print("=" * 80)
    print("2022 vs 2026 DISTRICT MAP SHIFTS (USING MODELED VOTERS)")
    print("=" * 80)
    print()
    
    # Load modeled data
    print("Loading modeled voter data...")
    df = pl.read_parquet("voters_with_party_modeling.parquet")
    print(f"Loaded {len(df):,} voters")
    print()
    
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
    
    # Create output directory
    output_dir = Path("2022_vs_2026_shifts")
    output_dir.mkdir(exist_ok=True)
    
    # Analyze each district type
    district_types = [
        ("HD", "NEWHD", "2026_HD", "House District"),
        ("CD", "NEWCD", "2026_CD", "Congressional District"),
        ("SD", "NEWSD", "2026_SD", "State Senate District")
    ]
    
    all_summaries = []
    
    for dist_type, old_col, new_col, dist_name in district_types:
        print("=" * 80)
        print(f"{dist_name.upper()} ({dist_type}): 2022 vs 2026 MAP COMPARISON")
        print("=" * 80)
        print()
        
        # Filter to voters with both old and new district assignments
        # Exclude district 0 (invalid/unknown districts)
        df_dist = df.filter(
            pl.col(old_col).is_not_null() & 
            pl.col(new_col).is_not_null() &
            (pl.col(old_col) != 0) &
            (pl.col(new_col) != 0)
        )
        
        print(f"Voters with both old and new districts: {len(df_dist):,}")
        print()
        
        # Calculate party composition for OLD districts (2022)
        print("Calculating party composition for 2022 districts...")
        old_district_party = df_dist.group_by([old_col, "party_simple"]).agg([
            pl.len().alias("voter_count")
        ]).sort([old_col, "party_simple"])
        
        old_party_pd = old_district_party.to_pandas()
        old_summary = old_party_pd.pivot_table(
            index=old_col,
            columns="party_simple",
            values="voter_count",
            fill_value=0
        ).reset_index()
        
        # Ensure all party columns exist
        for party in ["Republican", "Democrat", "Swing", "Unknown"]:
            if party not in old_summary.columns:
                old_summary[party] = 0
        
        # For House Districts, ensure all districts 1-150 are included
        if dist_type == "HD":
            all_districts = pd.DataFrame({old_col: range(1, 151)})
            old_summary = all_districts.merge(old_summary, on=old_col, how="left").fillna(0)
        
        old_summary["total_voters"] = (
            old_summary.get("Republican", 0) +
            old_summary.get("Democrat", 0) +
            old_summary.get("Swing", 0) +
            old_summary.get("Unknown", 0)
        )
        old_summary["republican_voters"] = old_summary.get("Republican", 0)
        old_summary["democrat_voters"] = old_summary.get("Democrat", 0)
        old_summary["net_advantage_2022"] = (
            old_summary["republican_voters"] - old_summary["democrat_voters"]
        )
        old_summary["net_advantage_pct_2022"] = (
            old_summary["net_advantage_2022"] / old_summary["total_voters"] * 100
        ).fillna(0)
        old_summary["rep_pct_2022"] = (
            old_summary["republican_voters"] / old_summary["total_voters"] * 100
        ).fillna(0)
        old_summary["dem_pct_2022"] = (
            old_summary["democrat_voters"] / old_summary["total_voters"] * 100
        ).fillna(0)
        
        old_summary = old_summary.rename(columns={old_col: "district"})
        
        # Calculate party composition for NEW districts (2026)
        print("Calculating party composition for 2026 districts...")
        new_district_party = df_dist.group_by([new_col, "party_simple"]).agg([
            pl.len().alias("voter_count")
        ]).sort([new_col, "party_simple"])
        
        new_party_pd = new_district_party.to_pandas()
        new_summary = new_party_pd.pivot_table(
            index=new_col,
            columns="party_simple",
            values="voter_count",
            fill_value=0
        ).reset_index()
        
        # Ensure all party columns exist
        for party in ["Republican", "Democrat", "Swing", "Unknown"]:
            if party not in new_summary.columns:
                new_summary[party] = 0
        
        # For House Districts, ensure all districts 1-150 are included
        if dist_type == "HD":
            all_districts = pd.DataFrame({new_col: range(1, 151)})
            new_summary = all_districts.merge(new_summary, on=new_col, how="left").fillna(0)
        
        new_summary["total_voters"] = (
            new_summary.get("Republican", 0) +
            new_summary.get("Democrat", 0) +
            new_summary.get("Swing", 0) +
            new_summary.get("Unknown", 0)
        )
        new_summary["republican_voters"] = new_summary.get("Republican", 0)
        new_summary["democrat_voters"] = new_summary.get("Democrat", 0)
        new_summary["net_advantage_2026"] = (
            new_summary["republican_voters"] - new_summary["democrat_voters"]
        )
        new_summary["net_advantage_pct_2026"] = (
            new_summary["net_advantage_2026"] / new_summary["total_voters"] * 100
        ).fillna(0)
        new_summary["rep_pct_2026"] = (
            new_summary["republican_voters"] / new_summary["total_voters"] * 100
        ).fillna(0)
        new_summary["dem_pct_2026"] = (
            new_summary["democrat_voters"] / new_summary["total_voters"] * 100
        ).fillna(0)
        
        new_summary = new_summary.rename(columns={new_col: "district"})
        
        # Classify districts
        threshold = 1000
        old_summary["advantage_2022"] = old_summary.apply(
            lambda row: "Republican" if row["net_advantage_2022"] > threshold
            else "Democrat" if row["net_advantage_2022"] < -threshold
            else "Competitive",
            axis=1
        )
        new_summary["advantage_2026"] = new_summary.apply(
            lambda row: "Republican" if row["net_advantage_2026"] > threshold
            else "Democrat" if row["net_advantage_2026"] < -threshold
            else "Competitive",
            axis=1
        )
        
        # Save old and new district summaries
        old_file = output_dir / f"{dist_type.lower()}_2022_districts.csv"
        new_file = output_dir / f"{dist_type.lower()}_2026_districts.csv"
        old_summary.to_csv(old_file, index=False)
        new_summary.to_csv(new_file, index=False)
        print(f"✅ Saved 2022 districts to {old_file}")
        print(f"✅ Saved 2026 districts to {new_file}")
        print()
        
        # Summary statistics
        print("2022 DISTRICTS SUMMARY:")
        old_rep_districts = len(old_summary[old_summary["advantage_2022"] == "Republican"])
        old_dem_districts = len(old_summary[old_summary["advantage_2022"] == "Democrat"])
        old_comp_districts = len(old_summary[old_summary["advantage_2022"] == "Competitive"])
        old_total_rep = old_summary["republican_voters"].sum()
        old_total_dem = old_summary["democrat_voters"].sum()
        old_net_advantage = old_total_rep - old_total_dem
        
        print(f"  Total Districts: {len(old_summary)}")
        print(f"  Republican Advantage: {old_rep_districts} ({old_rep_districts/len(old_summary)*100:.1f}%)")
        print(f"  Democrat Advantage: {old_dem_districts} ({old_dem_districts/len(old_summary)*100:.1f}%)")
        print(f"  Competitive: {old_comp_districts} ({old_comp_districts/len(old_summary)*100:.1f}%)")
        print(f"  Total Republican Voters: {old_total_rep:,}")
        print(f"  Total Democrat Voters: {old_total_dem:,}")
        print(f"  Net Advantage: {old_net_advantage:+,} (Republican)")
        print()
        
        print("2026 DISTRICTS SUMMARY:")
        new_rep_districts = len(new_summary[new_summary["advantage_2026"] == "Republican"])
        new_dem_districts = len(new_summary[new_summary["advantage_2026"] == "Democrat"])
        new_comp_districts = len(new_summary[new_summary["advantage_2026"] == "Competitive"])
        new_total_rep = new_summary["republican_voters"].sum()
        new_total_dem = new_summary["democrat_voters"].sum()
        new_net_advantage = new_total_rep - new_total_dem
        
        print(f"  Total Districts: {len(new_summary)}")
        print(f"  Republican Advantage: {new_rep_districts} ({new_rep_districts/len(new_summary)*100:.1f}%)")
        print(f"  Democrat Advantage: {new_dem_districts} ({new_dem_districts/len(new_summary)*100:.1f}%)")
        print(f"  Competitive: {new_comp_districts} ({new_comp_districts/len(new_summary)*100:.1f}%)")
        print(f"  Total Republican Voters: {new_total_rep:,}")
        print(f"  Total Democrat Voters: {new_total_dem:,}")
        print(f"  Net Advantage: {new_net_advantage:+,} (Republican)")
        print()
        
        # Calculate changes
        rep_district_change = new_rep_districts - old_rep_districts
        dem_district_change = new_dem_districts - old_dem_districts
        net_advantage_change = new_net_advantage - old_net_advantage
        
        print("CHANGES FROM 2022 TO 2026:")
        print(f"  Republican Advantage Districts: {rep_district_change:+.0f} ({old_rep_districts} → {new_rep_districts})")
        print(f"  Democrat Advantage Districts: {dem_district_change:+.0f} ({old_dem_districts} → {new_dem_districts})")
        print(f"  Net Advantage Change: {net_advantage_change:+,} voters")
        print(f"    (Positive = shift toward Republicans, Negative = shift toward Democrats)")
        print()
        
        # Top changes in net advantage
        print("TOP 10 DISTRICTS WITH LARGEST REPUBLICAN ADVANTAGE GAINS (2022 → 2026):")
        # Calculate change for each 2026 district
        new_summary_sorted = new_summary.sort_values("net_advantage_2026", ascending=False).head(10)
        print(new_summary_sorted[["district", "republican_voters", "democrat_voters", "net_advantage_2026", "net_advantage_pct_2026"]].to_string(index=False))
        print()
        
        print("TOP 10 DISTRICTS WITH LARGEST DEMOCRAT ADVANTAGE GAINS (2022 → 2026):")
        new_summary_sorted_dem = new_summary.sort_values("net_advantage_2026", ascending=True).head(10)
        print(new_summary_sorted_dem[["district", "republican_voters", "democrat_voters", "net_advantage_2026", "net_advantage_pct_2026"]].to_string(index=False))
        print()
        
        # Create summary row
        summary_row = {
            "district_type": dist_type,
            "old_rep_districts": old_rep_districts,
            "old_dem_districts": old_dem_districts,
            "old_comp_districts": old_comp_districts,
            "old_total_rep": old_total_rep,
            "old_total_dem": old_total_dem,
            "old_net_advantage": old_net_advantage,
            "new_rep_districts": new_rep_districts,
            "new_dem_districts": new_dem_districts,
            "new_comp_districts": new_comp_districts,
            "new_total_rep": new_total_rep,
            "new_total_dem": new_total_dem,
            "new_net_advantage": new_net_advantage,
            "rep_district_change": rep_district_change,
            "dem_district_change": dem_district_change,
            "net_advantage_change": net_advantage_change,
        }
        all_summaries.append(summary_row)
    
    # Create comprehensive summary
    print()
    print("=" * 80)
    print("COMPREHENSIVE SUMMARY - ALL DISTRICT TYPES")
    print("=" * 80)
    print()
    
    summary_df = pd.DataFrame(all_summaries)
    summary_file = output_dir / "2022_vs_2026_shifts_summary.csv"
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
    analyze_2022_vs_2026_shifts()

