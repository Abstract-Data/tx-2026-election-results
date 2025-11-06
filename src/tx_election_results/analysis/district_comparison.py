"""
Compare turnout and voting results between old (2022/2024) and new (2026) districts.
Shows what turnout would have been in new districts based on the same voters.
"""
import polars as pl
import pandas as pd
import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from typing import Optional


def calculate_district_transition_matrix(
    merged_voter_df: pl.DataFrame,
    old_district_col: str = "NEWSD",
    new_district_col: str = "2026_District"
) -> pd.DataFrame:
    """
    Calculate transition matrix showing how voters moved from old to new districts.
    
    Args:
        merged_voter_df: Voter dataframe with both old and new district assignments
        old_district_col: Column name for old district (e.g., "NEWSD")
        new_district_col: Column name for new district (e.g., "2026_District")
    
    Returns:
        DataFrame with transition matrix showing voter counts moving from old to new districts
    """
    print("Calculating district transition matrix...")
    
    # Filter to voters with both old and new district assignments
    voters_with_both = merged_voter_df.filter(
        pl.col(old_district_col).is_not_null() &
        pl.col(new_district_col).is_not_null()
    )
    
    # Group by old and new districts
    transition = (
        voters_with_both
        .group_by([old_district_col, new_district_col])
        .agg([
            pl.count().alias("voter_count"),
            pl.col("voted_early").sum().alias("early_voters")
        ])
        .sort([old_district_col, new_district_col])
    )
    
    transition_pd = transition.to_pandas()
    
    # Create pivot table for easier visualization
    pivot_voters = transition_pd.pivot_table(
        index=old_district_col,
        columns=new_district_col,
        values="voter_count",
        fill_value=0
    )
    
    pivot_voters.index.name = "Old_District"
    pivot_voters.columns.name = "New_District"
    
    print(f"Transition matrix: {len(pivot_voters)} old districts → {len(pivot_voters.columns)} new districts")
    
    return transition_pd, pivot_voters


from tx_election_results.utils.helpers import map_modeled_party_to_r_d


def calculate_party_gains_losses(
    merged_voter_df: pl.DataFrame,
    old_district_col: str = "NEWSD",
    new_district_col: str = "2026_District",
    output_dir: str = "data/exports",
    use_modeled: bool = True
) -> pd.DataFrame:
    """
    Calculate Republican/Democrat gains and losses for each new district.
    
    Shows how redistricting affects party composition in each district.
    Now includes modeled party predictions for non-primary voters.
    
    Args:
        merged_voter_df: Voter dataframe with both old and new district assignments
        old_district_col: Column name for old district (e.g., "NEWSD")
        new_district_col: Column name for new district (e.g., "2026_District")
        output_dir: Directory to save results
        use_modeled: Whether to include modeled party predictions (default: True)
    
    Returns:
        DataFrame with party gains/losses for each new district
    """
    print("\n" + "=" * 80)
    print("PARTY GAINS/LOSSES: OLD vs NEW DISTRICTS" + (" (WITH MODELED VOTERS)" if use_modeled else ""))
    print("=" * 80)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Create unified party column that includes modeled predictions
    # Note: Swing voters from last 4 primaries are considered "known" but not included in R/D counts
    if use_modeled and "predicted_party_score" in merged_voter_df.columns:
        print("Including modeled party predictions for non-primary voters...")
        merged_voter_df = merged_voter_df.with_columns([
            # Use known party if available (R/D/Swing), otherwise map modeled score
            pl.when(pl.col("party").is_in(["Republican", "Democrat", "Swing"]))
            .then(pl.col("party"))
            .when(pl.col("predicted_party_score").is_not_null())
            .then(pl.col("predicted_party_score").map_elements(
                lambda x: map_modeled_party_to_r_d(x) if x else None,
                return_dtype=pl.Utf8
            ))
            .otherwise(pl.lit(None))
            .alias("party_all")
        ])

        # Track voter type (known primary voter vs modeled)
        merged_voter_df = merged_voter_df.with_columns([
            pl.when(pl.col("party").is_in(["Republican", "Democrat", "Swing"]))
            .then(pl.lit("Known_Primary"))
            .when(pl.col("predicted_party_score").is_not_null())
            .then(pl.lit("Modeled"))
            .otherwise(pl.lit("Unknown"))
            .alias("voter_type")
        ])
        party_col = "party_all"
    else:
        party_col = "party"
        merged_voter_df = merged_voter_df.with_columns([
            pl.when(pl.col("party").is_in(["Republican", "Democrat", "Swing"]))
            .then(pl.lit("Known_Primary"))
            .otherwise(pl.lit("Unknown"))
            .alias("voter_type")
        ])
    
    # Calculate party composition for OLD districts
    print(f"\nCalculating party composition in OLD districts...")
    old_district_party = (
        merged_voter_df
        .filter(pl.col(old_district_col).is_not_null())
        .group_by([old_district_col, party_col])
        .agg([
            pl.count().alias("voter_count"),
            (pl.col("voter_type") == "Known_Primary").sum().alias("known_voters"),
            (pl.col("voter_type") == "Modeled").sum().alias("modeled_voters"),
        ])
        .sort([old_district_col, party_col])
    )
    
    # Calculate party composition for NEW districts
    print("Calculating party composition in NEW districts...")
    new_district_party = (
        merged_voter_df
        .filter(pl.col(new_district_col).is_not_null())
        .group_by([new_district_col, party_col])
        .agg([
            pl.count().alias("voter_count"),
            (pl.col("voter_type") == "Known_Primary").sum().alias("known_voters"),
            (pl.col("voter_type") == "Modeled").sum().alias("modeled_voters"),
        ])
        .sort([new_district_col, party_col])
    )
    
    # Convert to pandas for easier pivot operations
    old_district_party_pd = old_district_party.to_pandas()
    new_district_party_pd = new_district_party.to_pandas()
    
    # Rename party column for pivot
    old_district_party_pd = old_district_party_pd.rename(columns={party_col: "party"})
    new_district_party_pd = new_district_party_pd.rename(columns={party_col: "party"})
    
    # Pivot to get party columns for old districts
    old_pivot_pd = old_district_party_pd.pivot_table(
        index=old_district_col,
        columns="party",
        values=["voter_count", "known_voters", "modeled_voters"],
        aggfunc="sum",
        fill_value=0
    )
    
    # Flatten column names
    old_pivot_pd.columns = ['_'.join(col).strip() if col[1] else col[0] for col in old_pivot_pd.columns.values]
    old_pivot_pd = old_pivot_pd.reset_index()
    
    # Pivot to get party columns for new districts
    new_pivot_pd = new_district_party_pd.pivot_table(
        index=new_district_col,
        columns="party",
        values=["voter_count", "known_voters", "modeled_voters"],
        aggfunc="sum",
        fill_value=0
    )
    
    # Flatten column names
    new_pivot_pd.columns = ['_'.join(col).strip() if col[1] else col[0] for col in new_pivot_pd.columns.values]
    new_pivot_pd = new_pivot_pd.reset_index()
    
    # Rename district columns
    old_pivot_pd = old_pivot_pd.rename(columns={old_district_col: "district"})
    new_pivot_pd = new_pivot_pd.rename(columns={new_district_col: "district"})
    
    # Calculate totals and percentages for old districts (with known/modeled breakdown)
    old_summary = calculate_party_summary_with_modeling(old_pivot_pd, "old")
    
    # Calculate totals and percentages for new districts (with known/modeled breakdown)
    new_summary = calculate_party_summary_with_modeling(new_pivot_pd, "new")
    
    # Calculate net gains/losses for each new district
    print("\nCalculating net party gains/losses...")
    
    # For each new district, we need to see what voters it gained/lost
    # This is the difference between what voters are in the new district
    # vs what was in the old districts that contributed to it
    
    # Create a transition analysis: for each new district, show where voters came from
    transition = (
        merged_voter_df
        .filter(
            pl.col(old_district_col).is_not_null() &
            pl.col(new_district_col).is_not_null()
        )
        .group_by([new_district_col, old_district_col, party_col])
        .agg(pl.count().alias("voter_count"))
        .sort([new_district_col, old_district_col, party_col])
    )
    
    # Calculate net change: voters in new district minus what was in contributing old districts
    # For each new district, sum up all the voters from old districts
    transition_pd = transition.to_pandas()
    
    # For each new district, calculate what it gained/lost
    party_changes = []
    
    # Ensure transition_pd has the right column names
    transition_pd = transition_pd.rename(columns={
        new_district_col: "new_district",
        old_district_col: "old_district"
    })
    
    for new_dist in sorted(new_summary["district"].unique()):
        new_row = new_summary[new_summary["district"] == new_dist].iloc[0]
        
        # Find all old districts that contributed to this new district
        # and sum up ONLY the voters from those old districts that ended up in this new district
        voters_from_old_districts = transition_pd[
            transition_pd["new_district"] == new_dist
        ]
        
        # Calculate party composition of voters that came from old districts to this new district
        # This is the same as what's in the new district (same voters), so we compare to
        # the weighted average party composition of the old districts that contributed
        
        # Get the old districts that contributed and their voter counts
        old_dist_voter_counts = voters_from_old_districts.groupby("old_district")["voter_count"].sum()
        
        # Calculate weighted average party composition from contributing old districts
        # Weight by the proportion of voters from each old district
        if len(old_dist_voter_counts) > 0:
            total_voters_from_old = old_dist_voter_counts.sum()
            old_districts_list = old_dist_voter_counts.index.tolist()
            
            # Get party composition from old districts
            old_dist_comp = old_summary[old_summary["district"].isin(old_districts_list)]
            
            # Calculate weighted averages
            weights = old_dist_voter_counts / total_voters_from_old
            old_weighted_rep_pct = (old_dist_comp.set_index("district")["republican_pct"] * weights).sum()
            old_weighted_dem_pct = (old_dist_comp.set_index("district")["democrat_pct"] * weights).sum()
            
            # Apply to total voters in new district to get expected counts
            total_in_new = new_row["total_voters"]
            old_expected_republican = old_weighted_rep_pct / 100 * total_in_new
            old_expected_democrat = old_weighted_dem_pct / 100 * total_in_new
            old_expected_other = total_in_new - old_expected_republican - old_expected_democrat
        else:
            old_expected_republican = 0
            old_expected_democrat = 0
            old_expected_other = 0
        
        # Calculate net change (actual in new district vs expected from weighted average of old)
        net_republican = new_row["republican_voters"] - old_expected_republican
        net_democrat = new_row["democrat_voters"] - old_expected_democrat
        net_other = new_row["other_voters"] - old_expected_other
        
        # Calculate percentage changes
        pct_change_republican = (net_republican / old_expected_republican * 100) if old_expected_republican > 0 else 0
        pct_change_democrat = (net_democrat / old_expected_democrat * 100) if old_expected_democrat > 0 else 0
        
        party_changes.append({
            "district": new_dist,
            "old_expected_republican_voters": old_expected_republican,
            "new_republican_voters": new_row["republican_voters"],
            "net_republican_change": net_republican,
            "pct_republican_change": pct_change_republican,
            "old_expected_democrat_voters": old_expected_democrat,
            "new_democrat_voters": new_row["democrat_voters"],
            "net_democrat_change": net_democrat,
            "pct_democrat_change": pct_change_democrat,
            "old_expected_other_voters": old_expected_other,
            "new_other_voters": new_row["other_voters"],
            "net_other_change": net_other,
            # Add breakdown columns for known vs modeled voters
            "new_rep_known": new_row.get("republican_known", 0),
            "new_rep_modeled": new_row.get("republican_modeled", 0),
            "new_dem_known": new_row.get("democrat_known", 0),
            "new_dem_modeled": new_row.get("democrat_modeled", 0),
            "old_expected_republican_pct": old_weighted_rep_pct if len(old_dist_voter_counts) > 0 else 0,
            "new_republican_pct": new_row["republican_pct"],
            "old_expected_democrat_pct": old_weighted_dem_pct if len(old_dist_voter_counts) > 0 else 0,
            "new_democrat_pct": new_row["democrat_pct"],
        })
    
    changes_df = pd.DataFrame(party_changes)
    changes_df = changes_df.sort_values("district")
    
    # Save results
    old_summary.to_csv(output_path / "party_composition_old_districts.csv", index=False)
    new_summary.to_csv(output_path / "party_composition_new_districts.csv", index=False)
    changes_df.to_csv(output_path / "party_gains_losses_by_district.csv", index=False)
    
    # Print summary
    print("\n" + "-" * 80)
    print("PARTY GAINS/LOSSES SUMMARY")
    print("-" * 80)
    print(f"\nNet Republican Change: {changes_df['net_republican_change'].sum():+,} voters")
    print(f"Net Democrat Change: {changes_df['net_democrat_change'].sum():+,} voters")
    
    if use_modeled and "new_rep_known" in changes_df.columns:
        total_known = changes_df['new_rep_known'].sum() + changes_df['new_dem_known'].sum()
        total_modeled = changes_df['new_rep_modeled'].sum() + changes_df['new_dem_modeled'].sum()
        print(f"\nVoter Breakdown:")
        print(f"  Known Primary Voters: {total_known:+,} ({total_known/(total_known+total_modeled)*100:.1f}%)")
        print(f"  Modeled Voters: {total_modeled:+,} ({total_modeled/(total_known+total_modeled)*100:.1f}%)")
    
    print(f"\nDistricts with largest Republican gains:")
    top_rep_gain = changes_df.nlargest(5, "net_republican_change")[["district", "net_republican_change", "pct_republican_change"]]
    print(top_rep_gain.to_string(index=False))
    
    print(f"\nDistricts with largest Democrat gains:")
    top_dem_gain = changes_df.nlargest(5, "net_democrat_change")[["district", "net_democrat_change", "pct_democrat_change"]]
    print(top_dem_gain.to_string(index=False))
    
    print(f"\nDistricts with largest Republican losses:")
    top_rep_loss = changes_df.nsmallest(5, "net_republican_change")[["district", "net_republican_change", "pct_republican_change"]]
    print(top_rep_loss.to_string(index=False))
    
    print(f"\nDistricts with largest Democrat losses:")
    top_dem_loss = changes_df.nsmallest(5, "net_democrat_change")[["district", "net_democrat_change", "pct_democrat_change"]]
    print(top_dem_loss.to_string(index=False))
    
    return {
        "old_composition": old_summary,
        "new_composition": new_summary,
        "gains_losses": changes_df
    }


def calculate_party_summary(pivot_df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Calculate party summary from pivot table (backward compatibility)."""
    return calculate_party_summary_with_modeling(pivot_df, prefix)


def calculate_party_summary_with_modeling(pivot_df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Calculate party summary including known vs modeled breakdown."""
    # Get district column
    district_col = [col for col in pivot_df.columns if "district" in col.lower() or "NEWSD" in col or "2026" in col]
    if not district_col:
        district_col = [pivot_df.columns[0]]
    district_col = district_col[0]
    
    # Get party voter count columns
    rep_cols = [col for col in pivot_df.columns if "Republican" in str(col) and "voter_count" in str(col)]
    dem_cols = [col for col in pivot_df.columns if "Democrat" in str(col) and "voter_count" in str(col)]
    other_cols = [col for col in pivot_df.columns if "voter_count" in str(col) and "Republican" not in str(col) and "Democrat" not in str(col)]
    
    # Get known/modeled breakdown columns
    rep_known_cols = [col for col in pivot_df.columns if "Republican" in str(col) and "known_voters" in str(col)]
    rep_modeled_cols = [col for col in pivot_df.columns if "Republican" in str(col) and "modeled_voters" in str(col)]
    dem_known_cols = [col for col in pivot_df.columns if "Democrat" in str(col) and "known_voters" in str(col)]
    dem_modeled_cols = [col for col in pivot_df.columns if "Democrat" in str(col) and "modeled_voters" in str(col)]
    
    # Sum up values
    republican_voters = sum([pivot_df[col].fillna(0) for col in rep_cols]) if rep_cols else pd.Series([0] * len(pivot_df))
    democrat_voters = sum([pivot_df[col].fillna(0) for col in dem_cols]) if dem_cols else pd.Series([0] * len(pivot_df))
    other_voters = sum([pivot_df[col].fillna(0) for col in other_cols]) if other_cols else pd.Series([0] * len(pivot_df))
    
    republican_known = sum([pivot_df[col].fillna(0) for col in rep_known_cols]) if rep_known_cols else pd.Series([0] * len(pivot_df))
    republican_modeled = sum([pivot_df[col].fillna(0) for col in rep_modeled_cols]) if rep_modeled_cols else pd.Series([0] * len(pivot_df))
    democrat_known = sum([pivot_df[col].fillna(0) for col in dem_known_cols]) if dem_known_cols else pd.Series([0] * len(pivot_df))
    democrat_modeled = sum([pivot_df[col].fillna(0) for col in dem_modeled_cols]) if dem_modeled_cols else pd.Series([0] * len(pivot_df))
    
    districts = pivot_df[district_col]
    total_voters = republican_voters + democrat_voters + other_voters
    
    summary = pd.DataFrame({
        "district": districts,
        "republican_voters": republican_voters,
        "democrat_voters": democrat_voters,
        "other_voters": other_voters,
        "total_voters": total_voters,
        "republican_pct": (republican_voters / total_voters * 100).fillna(0),
        "democrat_pct": (democrat_voters / total_voters * 100).fillna(0),
        "other_pct": (other_voters / total_voters * 100).fillna(0),
        "republican_known": republican_known,
        "republican_modeled": republican_modeled,
        "democrat_known": democrat_known,
        "democrat_modeled": democrat_modeled,
    })
    
    return summary


def compare_old_vs_new_turnout(
    merged_voter_df: pl.DataFrame,
    old_district_col: str = "NEWSD",
    new_district_col: str = "2026_District",
    output_dir: str = "data/exports"
) -> pd.DataFrame:
    """
    Compare turnout metrics between old and new districts for the same set of voters.
    
    Shows what turnout would have been in new districts based on actual voting behavior.
    
    Args:
        merged_voter_df: Voter dataframe with both old and new district assignments
        old_district_col: Column name for old district
        new_district_col: Column name for new district
        output_dir: Directory to save comparison results
    
    Returns:
        DataFrame with comparison metrics
    """
    print("\n" + "=" * 80)
    print("OLD vs NEW DISTRICT TURNOUT COMPARISON")
    print("=" * 80)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Calculate turnout in OLD districts
    print("\nCalculating turnout in OLD districts...")
    old_turnout = (
        merged_voter_df
        .filter(pl.col(old_district_col).is_not_null())
        .group_by(old_district_col)
        .agg([
            pl.count().alias("old_total_voters"),
            pl.col("voted_early").sum().alias("old_early_voters"),
            pl.col("party").value_counts().alias("old_party_counts")
        ])
        .with_columns([
            (pl.col("old_early_voters") / pl.col("old_total_voters") * 100).alias("old_turnout_rate")
        ])
        .sort(old_district_col)
    )
    
    # Calculate turnout in NEW districts
    print("Calculating turnout in NEW districts...")
    new_turnout = (
        merged_voter_df
        .filter(pl.col(new_district_col).is_not_null())
        .group_by(new_district_col)
        .agg([
            pl.count().alias("new_total_voters"),
            pl.col("voted_early").sum().alias("new_early_voters"),
            pl.col("party").value_counts().alias("new_party_counts")
        ])
        .with_columns([
            (pl.col("new_early_voters") / pl.col("new_total_voters") * 100).alias("new_turnout_rate")
        ])
        .sort(new_district_col)
    )
    
    # Convert to pandas for easier manipulation
    old_turnout_pd = old_turnout.to_pandas()
    new_turnout_pd = new_turnout.to_pandas()
    
    # Rename district columns for clarity
    old_turnout_pd = old_turnout_pd.rename(columns={old_district_col: "district"})
    new_turnout_pd = new_turnout_pd.rename(columns={new_district_col: "district"})
    
    # Calculate party composition for old districts
    print("Calculating party composition...")
    old_party_comp = calculate_party_composition(merged_voter_df, old_district_col)
    new_party_comp = calculate_party_composition(merged_voter_df, new_district_col)
    
    # Merge party composition
    old_turnout_pd = old_turnout_pd.merge(old_party_comp, on="district", how="left")
    new_turnout_pd = new_turnout_pd.merge(new_party_comp, on="district", how="left")
    
    # Save individual results
    old_turnout_pd.to_csv(output_path / "turnout_old_districts.csv", index=False)
    new_turnout_pd.to_csv(output_path / "turnout_new_districts.csv", index=False)
    
    # Print summary statistics
    print("\n" + "-" * 80)
    print("OLD DISTRICTS SUMMARY")
    print("-" * 80)
    print(f"Number of districts: {len(old_turnout_pd)}")
    print(f"Average turnout: {old_turnout_pd['old_turnout_rate'].mean():.2f}%")
    print(f"Median turnout: {old_turnout_pd['old_turnout_rate'].median():.2f}%")
    print(f"Min turnout: {old_turnout_pd['old_turnout_rate'].min():.2f}%")
    print(f"Max turnout: {old_turnout_pd['old_turnout_rate'].max():.2f}%")
    
    print("\n" + "-" * 80)
    print("NEW DISTRICTS SUMMARY")
    print("-" * 80)
    print(f"Number of districts: {len(new_turnout_pd)}")
    print(f"Average turnout: {new_turnout_pd['new_turnout_rate'].mean():.2f}%")
    print(f"Median turnout: {new_turnout_pd['new_turnout_rate'].median():.2f}%")
    print(f"Min turnout: {new_turnout_pd['new_turnout_rate'].min():.2f}%")
    print(f"Max turnout: {new_turnout_pd['new_turnout_rate'].max():.2f}%")
    
    print("\n" + "-" * 80)
    print("CHANGE SUMMARY")
    print("-" * 80)
    avg_change = new_turnout_pd['new_turnout_rate'].mean() - old_turnout_pd['old_turnout_rate'].mean()
    print(f"Average turnout change: {avg_change:+.2f} percentage points")
    print(f"Median turnout change: {new_turnout_pd['new_turnout_rate'].median() - old_turnout_pd['old_turnout_rate'].median():+.2f} percentage points")
    
    return {
        "old_districts": old_turnout_pd,
        "new_districts": new_turnout_pd
    }


def calculate_party_composition(
    merged_voter_df: pl.DataFrame,
    district_col: str
) -> pd.DataFrame:
    """Calculate party composition by district."""
    party_stats = (
        merged_voter_df
        .filter(pl.col(district_col).is_not_null())
        .group_by([district_col, "party"])
        .agg(pl.count().alias("count"))
        .pivot(
            index=district_col,
            columns="party",
            values="count",
            aggregate_function="sum"
        )
        .fill_null(0)
    )
    
    # Calculate percentages
    total_by_district = party_stats.select(pl.exclude(district_col)).sum(axis=1)
    
    party_pct = party_stats.clone()
    for col in party_pct.columns:
        if col != district_col:
            party_pct = party_pct.with_columns([
                (pl.col(col) / total_by_district * 100).alias(f"{col}_pct")
            ])
    
    # Select percentage columns
    pct_cols = [col for col in party_pct.columns if col.endswith("_pct") or col == district_col]
    party_pct = party_pct.select(pct_cols)
    
    # Rename district column
    party_pct = party_pct.rename({district_col: "district"})
    
    return party_pct.to_pandas()


def create_party_gains_losses_visualizations(
    party_results: dict,
    shapefile_2026: gpd.GeoDataFrame,
    output_dir: str = "data/exports"
):
    """
    Create visualizations showing party gains/losses from redistricting.
    
    Args:
        party_results: Dictionary with 'old_composition', 'new_composition', and 'gains_losses' DataFrames
        shapefile_2026: GeoDataFrame for new districts
        output_dir: Directory to save visualizations
    """
    print("\nCreating party gains/losses visualizations...")
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    gains_losses = party_results["gains_losses"]
    
    # Determine district type from number of districts
    num_districts = len(shapefile_2026)
    if num_districts == 150:
        district_type = "House Districts (HD)"
    elif num_districts == 31:
        district_type = "State Senate Districts (SD)"
    elif num_districts == 38:
        district_type = "Congressional Districts (CD)"
    else:
        district_type = "Districts"
    
    # 1. Map showing net Republican change
    create_party_change_map(
        gains_losses, shapefile_2026, "net_republican_change", 
        f"Net Republican Change - {district_type}", "Reds", output_path / "republican_gains_losses_map.png"
    )
    
    # 2. Map showing net Democrat change
    create_party_change_map(
        gains_losses, shapefile_2026, "net_democrat_change",
        f"Net Democrat Change - {district_type}", "Blues", output_path / "democrat_gains_losses_map.png"
    )
    
    # 3. Bar chart showing top gains/losses
    create_party_change_barchart(gains_losses, output_path, district_type)
    
    # 4. Scatter plot: Republican vs Democrat change
    create_party_change_scatter(gains_losses, output_path, district_type)
    
    # 5. Party composition comparison
    create_party_composition_comparison(party_results, output_path, district_type)
    
    print(f"\nParty gains/losses visualizations saved to {output_path}")


def create_party_change_map(
    changes_df: pd.DataFrame,
    gdf: gpd.GeoDataFrame,
    column: str,
    title: str,
    cmap: str,
    output_path: Path
):
    """Create choropleth map showing party changes."""
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Merge with shapefile
    merged = gdf.merge(
        changes_df,
        left_on="District",
        right_on="district",
        how="left"
    )
    
    # Plot
    merged.plot(
        column=column,
        ax=ax,
        cmap=cmap,
        legend=True,
        missing_kwds={"color": "lightgrey"},
        edgecolor="black",
        linewidth=0.5
    )
    
    # Determine district type from number of districts if not already in title
    if "House Districts" not in title and "Senate Districts" not in title and "Congressional Districts" not in title:
        num_districts = len(gdf)
        if num_districts == 150:
            district_type = " - House Districts (HD)"
        elif num_districts == 31:
            district_type = " - State Senate Districts (SD)"
        elif num_districts == 38:
            district_type = " - Congressional Districts (CD)"
        else:
            district_type = ""
        title = title + district_type
    
    ax.set_title(title + "\n(Redistricting Impact)", fontsize=16, fontweight="bold")
    ax.axis("off")
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    
    print(f"  - {title} map")


def create_party_change_barchart(
    changes_df: pd.DataFrame,
    output_path: Path,
    district_type: str = "Districts"
):
    """Create bar chart showing top gains and losses."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Top Republican gains and losses
    top_rep = changes_df.nlargest(10, "net_republican_change")
    bottom_rep = changes_df.nsmallest(10, "net_republican_change")
    
    ax1.barh(range(len(top_rep)), top_rep["net_republican_change"], color="red", alpha=0.7, label="Gains")
    ax1.barh(range(len(top_rep), len(top_rep) + len(bottom_rep)), 
             bottom_rep["net_republican_change"], color="darkred", alpha=0.7, label="Losses")
    ax1.set_yticks(range(len(top_rep) + len(bottom_rep)))
    ax1.set_yticklabels([f"District {d}" for d in list(top_rep["district"]) + list(bottom_rep["district"])])
    ax1.set_xlabel("Net Change in Republican Voters", fontsize=12)
    ax1.set_title(f"Top Republican Gains and Losses - {district_type}", fontsize=14, fontweight="bold")
    ax1.axvline(0, color="black", linestyle="--", linewidth=1)
    ax1.legend()
    ax1.grid(alpha=0.3, axis="x")
    
    # Top Democrat gains and losses
    top_dem = changes_df.nlargest(10, "net_democrat_change")
    bottom_dem = changes_df.nsmallest(10, "net_democrat_change")
    
    ax2.barh(range(len(top_dem)), top_dem["net_democrat_change"], color="blue", alpha=0.7, label="Gains")
    ax2.barh(range(len(top_dem), len(top_dem) + len(bottom_dem)), 
             bottom_dem["net_democrat_change"], color="darkblue", alpha=0.7, label="Losses")
    ax2.set_yticks(range(len(top_dem) + len(bottom_dem)))
    ax2.set_yticklabels([f"District {d}" for d in list(top_dem["district"]) + list(bottom_dem["district"])])
    ax2.set_xlabel("Net Change in Democrat Voters", fontsize=12)
    ax2.set_title(f"Top Democrat Gains and Losses - {district_type}", fontsize=14, fontweight="bold")
    ax2.axvline(0, color="black", linestyle="--", linewidth=1)
    ax2.legend()
    ax2.grid(alpha=0.3, axis="x")
    
    plt.tight_layout()
    plt.savefig(output_path / "party_gains_losses_barchart.png", dpi=300, bbox_inches="tight")
    plt.close()
    
    print("  - Party gains/losses bar chart")


def create_party_change_scatter(
    changes_df: pd.DataFrame,
    output_path: Path,
    district_type: str = "Districts"
):
    """Create scatter plot showing Republican vs Democrat changes."""
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Color by net change direction
    colors = []
    for _, row in changes_df.iterrows():
        if row["net_republican_change"] > 0 and row["net_democrat_change"] > 0:
            colors.append("purple")  # Both gained
        elif row["net_republican_change"] > 0:
            colors.append("red")  # Republican gained
        elif row["net_democrat_change"] > 0:
            colors.append("blue")  # Democrat gained
        else:
            colors.append("grey")  # Both lost or neutral
    
    scatter = ax.scatter(
        changes_df["net_republican_change"],
        changes_df["net_democrat_change"],
        c=colors,
        s=100,
        alpha=0.6,
        edgecolors="black",
        linewidth=1
    )
    
    # Add district labels
    for _, row in changes_df.iterrows():
        ax.annotate(
            f"Dist {int(row['district'])}",
            (row["net_republican_change"], row["net_democrat_change"]),
            fontsize=8,
            alpha=0.7
        )
    
    ax.axhline(0, color="black", linestyle="--", linewidth=1)
    ax.axvline(0, color="black", linestyle="--", linewidth=1)
    ax.set_xlabel("Net Republican Change (voters)", fontsize=12)
    ax.set_ylabel("Net Democrat Change (voters)", fontsize=12)
    ax.set_title(f"Party Gains/Losses: Republican vs Democrat Change - {district_type}", fontsize=14, fontweight="bold")
    ax.grid(alpha=0.3)
    
    # Add legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor="red", label="Republican Gain"),
        Patch(facecolor="blue", label="Democrat Gain"),
        Patch(facecolor="purple", label="Both Gained"),
        Patch(facecolor="grey", label="Net Loss/Neutral")
    ]
    ax.legend(handles=legend_elements, loc="upper right")
    
    plt.tight_layout()
    plt.savefig(output_path / "party_change_scatter.png", dpi=300, bbox_inches="tight")
    plt.close()
    
    print("  - Party change scatter plot")


def create_party_composition_comparison(
    party_results: dict,
    output_path: Path,
    district_type: str = "Districts"
):
    """Create comparison of party composition before and after redistricting."""
    old_comp = party_results["old_composition"]
    new_comp = party_results["new_composition"]
    changes = party_results["gains_losses"]
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
    
    # Before redistricting (old districts)
    districts_sorted = sorted(old_comp["district"].unique())
    x = np.arange(len(districts_sorted))
    width = 0.35
    
    old_rep = [old_comp[old_comp["district"] == d]["republican_pct"].values[0] if len(old_comp[old_comp["district"] == d]) > 0 else 0 for d in districts_sorted]
    old_dem = [old_comp[old_comp["district"] == d]["democrat_pct"].values[0] if len(old_comp[old_comp["district"] == d]) > 0 else 0 for d in districts_sorted]
    
    ax1.bar(x - width/2, old_rep, width, label="Republican %", color="red", alpha=0.7)
    ax1.bar(x + width/2, old_dem, width, label="Democrat %", color="blue", alpha=0.7)
    ax1.set_xlabel("District", fontsize=12)
    ax1.set_ylabel("Party Percentage", fontsize=12)
    ax1.set_title(f"OLD Districts (2022/2024): Party Composition - {district_type}", fontsize=14, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"D{d}" for d in districts_sorted], rotation=45, ha="right")
    ax1.legend()
    ax1.grid(alpha=0.3, axis="y")
    ax1.set_ylim(0, 100)
    
    # After redistricting (new districts)
    districts_sorted_new = sorted(new_comp["district"].unique())
    x_new = np.arange(len(districts_sorted_new))
    
    new_rep = [new_comp[new_comp["district"] == d]["republican_pct"].values[0] if len(new_comp[new_comp["district"] == d]) > 0 else 0 for d in districts_sorted_new]
    new_dem = [new_comp[new_comp["district"] == d]["democrat_pct"].values[0] if len(new_comp[new_comp["district"] == d]) > 0 else 0 for d in districts_sorted_new]
    
    ax2.bar(x_new - width/2, new_rep, width, label="Republican %", color="red", alpha=0.7)
    ax2.bar(x_new + width/2, new_dem, width, label="Democrat %", color="blue", alpha=0.7)
    ax2.set_xlabel("District", fontsize=12)
    ax2.set_ylabel("Party Percentage", fontsize=12)
    ax2.set_title(f"NEW Districts (2026): Party Composition - {district_type}", fontsize=14, fontweight="bold")
    ax2.set_xticks(x_new)
    ax2.set_xticklabels([f"D{d}" for d in districts_sorted_new], rotation=45, ha="right")
    ax2.legend()
    ax2.grid(alpha=0.3, axis="y")
    ax2.set_ylim(0, 100)
    
    plt.tight_layout()
    plt.savefig(output_path / "party_composition_comparison.png", dpi=300, bbox_inches="tight")
    plt.close()
    
    print("  - Party composition comparison")


def create_comparison_visualizations(
    comparison_results: dict,
    shapefile_2022_sd: gpd.GeoDataFrame,
    shapefile_2026: gpd.GeoDataFrame,
    output_dir: str = "data/exports"
):
    """
    Create visualizations comparing old vs new district turnout.
    
    Args:
        comparison_results: Dictionary with 'old_districts' and 'new_districts' DataFrames
        shapefile_2022_sd: GeoDataFrame for old districts
        shapefile_2026: GeoDataFrame for new districts
        output_dir: Directory to save visualizations
    """
    print("\nCreating comparison visualizations...")
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    old_df = comparison_results["old_districts"]
    new_df = comparison_results["new_districts"]
    
    # 1. Side-by-side comparison map
    create_side_by_side_comparison_map(
        old_df, new_df, shapefile_2022_sd, shapefile_2026, output_path
    )
    
    # 2. Turnout change histogram
    create_turnout_change_histogram(old_df, new_df, output_path)
    
    # 3. Scatter plot: old vs new turnout
    create_turnout_scatter(old_df, new_df, output_path)
    
    # 4. District transition visualization
    create_transition_heatmap(old_df, new_df, output_path)
    
    print(f"\nComparison visualizations saved to {output_path}")


def create_side_by_side_comparison_map(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    gdf_old: gpd.GeoDataFrame,
    gdf_new: gpd.GeoDataFrame,
    output_path: Path
):
    """Create side-by-side maps showing old vs new district turnout."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
    
    # Old districts map
    old_merged = gdf_old.merge(
        old_df,
        left_on="SLDUST",
        right_on="district",
        how="left"
    )
    old_merged.plot(
        column="old_turnout_rate",
        ax=ax1,
        cmap="YlOrRd",
        legend=True,
        missing_kwds={"color": "lightgrey"},
        edgecolor="black",
        linewidth=0.5
    )
    # Determine district types from number of districts
    num_old = len(gdf_old)
    num_new = len(gdf_new)
    
    if num_old == 150:
        type_old = "State House Districts (HD)"
    elif num_old == 31:
        type_old = "State Senate Districts (SD)"
    elif num_old == 38:
        type_old = "Congressional Districts (CD)"
    else:
        type_old = "Districts"
    
    if num_new == 150:
        type_new = "State House Districts (HD)"
    elif num_new == 31:
        type_new = "State Senate Districts (SD)"
    elif num_new == 38:
        type_new = "Congressional Districts (CD)"
    else:
        type_new = "Districts"
    
    ax1.set_title(f"OLD Districts (2022/2024) - {type_old}\nTurnout Rate (%)", fontsize=14, fontweight="bold")
    ax1.axis("off")
    
    # New districts map
    new_merged = gdf_new.merge(
        new_df,
        left_on="District",
        right_on="district",
        how="left"
    )
    new_merged.plot(
        column="new_turnout_rate",
        ax=ax2,
        cmap="YlOrRd",
        legend=True,
        missing_kwds={"color": "lightgrey"},
        edgecolor="black",
        linewidth=0.5
    )
    ax2.set_title(f"NEW Districts (2026) - {type_new}\nTurnout Rate (%)", fontsize=14, fontweight="bold")
    ax2.axis("off")
    
    plt.tight_layout()
    plt.savefig(output_path / "turnout_comparison_old_vs_new.png", dpi=300, bbox_inches="tight")
    plt.close()
    
    print("  - Side-by-side comparison map")


def create_turnout_change_histogram(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    output_path: Path
):
    """Create histogram showing distribution of turnout rates."""
    # Determine district types from number of districts
    num_old = len(old_df)
    num_new = len(new_df)
    
    if num_old == 150:
        type_old = "State House Districts (HD)"
    elif num_old == 31:
        type_old = "State Senate Districts (SD)"
    elif num_old == 38:
        type_old = "Congressional Districts (CD)"
    else:
        type_old = "Districts"
    
    if num_new == 150:
        type_new = "State House Districts (HD)"
    elif num_new == 31:
        type_new = "State Senate Districts (SD)"
    elif num_new == 38:
        type_new = "Congressional Districts (CD)"
    else:
        type_new = "Districts"
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Old districts histogram
    ax1.hist(old_df["old_turnout_rate"], bins=20, edgecolor="black", alpha=0.7, color="steelblue")
    ax1.axvline(old_df["old_turnout_rate"].mean(), color="red", linestyle="--", linewidth=2, label=f"Mean: {old_df['old_turnout_rate'].mean():.2f}%")
    ax1.set_xlabel("Turnout Rate (%)", fontsize=12)
    ax1.set_ylabel("Number of Districts", fontsize=12)
    ax1.set_title(f"OLD Districts (2022/2024) Turnout Distribution - {type_old}", fontsize=14, fontweight="bold")
    ax1.legend()
    ax1.grid(alpha=0.3)
    
    # New districts histogram
    ax2.hist(new_df["new_turnout_rate"], bins=20, edgecolor="black", alpha=0.7, color="orange")
    ax2.axvline(new_df["new_turnout_rate"].mean(), color="red", linestyle="--", linewidth=2, label=f"Mean: {new_df['new_turnout_rate'].mean():.2f}%")
    ax2.set_xlabel("Turnout Rate (%)", fontsize=12)
    ax2.set_ylabel("Number of Districts", fontsize=12)
    ax2.set_title(f"NEW Districts (2026) Turnout Distribution - {type_new}", fontsize=14, fontweight="bold")
    ax2.legend()
    ax2.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path / "turnout_distribution_comparison.png", dpi=300, bbox_inches="tight")
    plt.close()
    
    print("  - Turnout distribution comparison")


def create_turnout_scatter(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    output_path: Path
):
    """Create scatter plot comparing old vs new turnout (if districts can be matched)."""
    # Determine district types from number of districts
    num_old = len(old_df)
    num_new = len(new_df)
    
    if num_old == 150:
        type_old = "State House Districts (HD)"
    elif num_old == 31:
        type_old = "State Senate Districts (SD)"
    elif num_old == 38:
        type_old = "Congressional Districts (CD)"
    else:
        type_old = "Districts"
    
    if num_new == 150:
        type_new = "State House Districts (HD)"
    elif num_new == 31:
        type_new = "State Senate Districts (SD)"
    elif num_new == 38:
        type_new = "Congressional Districts (CD)"
    else:
        type_new = "Districts"
    
    # This is a simplified version - in practice, you'd match districts by geography
    # For now, just show the distributions
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Create bins for comparison
    bins = np.linspace(0, max(old_df["old_turnout_rate"].max(), new_df["new_turnout_rate"].max()), 20)
    
    ax.hist(old_df["old_turnout_rate"], bins=bins, alpha=0.5, label=f"OLD {type_old}", color="steelblue", edgecolor="black")
    ax.hist(new_df["new_turnout_rate"], bins=bins, alpha=0.5, label=f"NEW {type_new}", color="orange", edgecolor="black")
    
    ax.set_xlabel("Turnout Rate (%)", fontsize=12)
    ax.set_ylabel("Number of Districts", fontsize=12)
    ax.set_title(f"Turnout Rate Distribution: OLD ({type_old}) vs NEW ({type_new})", fontsize=14, fontweight="bold")
    ax.legend(fontsize=12)
    ax.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path / "turnout_overlay_comparison.png", dpi=300, bbox_inches="tight")
    plt.close()
    
    print("  - Turnout overlay comparison")


def create_transition_heatmap(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    output_path: Path
):
    """Create a summary table showing key statistics."""
    # Create summary comparison table
    summary_data = {
        "Metric": [
            "Number of Districts",
            "Average Turnout (%)",
            "Median Turnout (%)",
            "Min Turnout (%)",
            "Max Turnout (%)",
            "Std Dev Turnout (%)",
            "Total Voters",
            "Total Early Voters"
        ],
        "OLD Districts": [
            len(old_df),
            f"{old_df['old_turnout_rate'].mean():.2f}",
            f"{old_df['old_turnout_rate'].median():.2f}",
            f"{old_df['old_turnout_rate'].min():.2f}",
            f"{old_df['old_turnout_rate'].max():.2f}",
            f"{old_df['old_turnout_rate'].std():.2f}",
            f"{old_df['old_total_voters'].sum():,}",
            f"{old_df['old_early_voters'].sum():,}"
        ],
        "NEW Districts": [
            len(new_df),
            f"{new_df['new_turnout_rate'].mean():.2f}",
            f"{new_df['new_turnout_rate'].median():.2f}",
            f"{new_df['new_turnout_rate'].min():.2f}",
            f"{new_df['new_turnout_rate'].max():.2f}",
            f"{new_df['new_turnout_rate'].std():.2f}",
            f"{new_df['new_total_voters'].sum():,}",
            f"{new_df['new_early_voters'].sum():,}"
        ],
        "Change": [
            f"{len(new_df) - len(old_df):+d}",
            f"{new_df['new_turnout_rate'].mean() - old_df['old_turnout_rate'].mean():+.2f}",
            f"{new_df['new_turnout_rate'].median() - old_df['old_turnout_rate'].median():+.2f}",
            f"{new_df['new_turnout_rate'].min() - old_df['old_turnout_rate'].min():+.2f}",
            f"{new_df['new_turnout_rate'].max() - old_df['old_turnout_rate'].max():+.2f}",
            f"{new_df['new_turnout_rate'].std() - old_df['old_turnout_rate'].std():+.2f}",
            f"{new_df['new_total_voters'].sum() - old_df['old_total_voters'].sum():+,}",
            f"{new_df['new_early_voters'].sum() - old_df['old_early_voters'].sum():+,}"
        ]
    }
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(output_path / "district_comparison_summary.csv", index=False)
    
    # Create visualization of summary table
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis('tight')
    ax.axis('off')
    
    table = ax.table(
        cellText=summary_df.values,
        colLabels=summary_df.columns,
        cellLoc='center',
        loc='center',
        bbox=[0, 0, 1, 1]
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)
    
    # Style header row
    for i in range(len(summary_df.columns)):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    # Determine district types from number of districts
    num_old = len(old_df)
    num_new = len(new_df)
    
    if num_old == 150:
        type_old = "State House Districts (HD)"
    elif num_old == 31:
        type_old = "State Senate Districts (SD)"
    elif num_old == 38:
        type_old = "Congressional Districts (CD)"
    else:
        type_old = "Districts"
    
    if num_new == 150:
        type_new = "State House Districts (HD)"
    elif num_new == 31:
        type_new = "State Senate Districts (SD)"
    elif num_new == 38:
        type_new = "Congressional Districts (CD)"
    else:
        type_new = "Districts"
    
    plt.title(f"OLD ({type_old}) vs NEW ({type_new}): Summary Comparison", fontsize=14, fontweight="bold", pad=20)
    plt.savefig(output_path / "district_comparison_summary_table.png", dpi=300, bbox_inches="tight")
    plt.close()
    
    print("  - Summary comparison table")
    
    return summary_df

