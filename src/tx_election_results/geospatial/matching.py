"""
Geospatial matching: Calculate turnout metrics by district.
Match voters to districts using voterfile district assignments.
"""
from pathlib import Path
import geopandas as gpd
import polars as pl
import pandas as pd


def calculate_turnout_by_district(
    merged_voter_df: pl.DataFrame,
    district_type: str,
    shapefile_gdf: gpd.GeoDataFrame,
    district_id_col: str,
    district_name_col: str = None,
    voter_district_col: str = None  # Optional: specify which column to use from voterfile
) -> pd.DataFrame:
    """
    Calculate turnout metrics by district.
    
    Args:
        merged_voter_df: Merged voter data with district assignments
        district_type: Type of district ('congressional' or 'senate')
        shapefile_gdf: GeoDataFrame with district boundaries
        district_id_col: Column name in shapefile for district ID
        district_name_col: Optional column name for district name
    
    Returns:
        DataFrame with turnout metrics by district
    """
    print(f"\nCalculating turnout for {district_type} districts...")
    
    # Determine which district column to use from voterfile
    # If not explicitly provided, use default based on district type
    if voter_district_col is None:
        if district_type == "congressional":
            voter_district_col = "NEWCD"  # OLD Congressional District
        elif district_type == "senate":
            voter_district_col = "NEWSD"  # OLD State Senate District
        else:
            raise ValueError(f"Unknown district_type: {district_type}")
    
    # Check if the column exists
    if voter_district_col not in merged_voter_df.columns:
        raise ValueError(f"Column '{voter_district_col}' not found in voter dataframe. Available columns: {merged_voter_df.columns}")
    
    # Convert voter district numbers to format matching shapefile
    # 2026 shapefile uses numeric District column, 2022 shapefiles use zero-padded strings
    if district_type == "senate" and district_id_col == "District":
        # 2026 shapefile uses numeric District column
        voter_df = merged_voter_df.with_columns([
            pl.col(voter_district_col).cast(pl.Int64).alias("district_id_str")
        ])
    else:
        # 2022 shapefiles use zero-padded strings
        # State Senate uses 3 digits (e.g., "004", "007"), Congressional uses 2 digits (e.g., "01", "02")
        zfill_width = 3 if district_type == "senate" else 2
        voter_df = merged_voter_df.with_columns([
            pl.col(voter_district_col).cast(pl.Utf8).str.zfill(zfill_width).alias("district_id_str")
        ])
    
    # Convert to pandas for easier aggregation
    voter_pd = voter_df.to_pandas()
    
    # Group by district and calculate metrics
    district_stats = voter_pd.groupby("district_id_str").agg({
        "VUID": "count",  # Total registered voters
        "voted_early": "sum",  # Early voters
    }).reset_index()
    
    district_stats.columns = ["district_id_str", "total_voters", "early_voters"]
    district_stats["turnout_rate"] = (district_stats["early_voters"] / district_stats["total_voters"]) * 100
    
    # Merge with shapefile to get district names and ensure all districts are included
    # Handle case where district_name_col might be the same as district_id_col
    if district_name_col and district_name_col != district_id_col:
        cols_to_select = [district_id_col, district_name_col]
    else:
        cols_to_select = [district_id_col]
    
    shapefile_pd = shapefile_gdf[cols_to_select].copy()
    
    # Convert district_id_col to appropriate type for merging
    if district_type == "senate" and district_id_col == "District":
        # 2026 shapefile uses numeric
        shapefile_pd[district_id_col] = shapefile_pd[district_id_col].astype(int)
        district_stats["district_id_str"] = district_stats["district_id_str"].astype(int)
    else:
        # 2022 shapefiles use strings
        shapefile_pd[district_id_col] = shapefile_pd[district_id_col].astype(str)
        district_stats["district_id_str"] = district_stats["district_id_str"].astype(str)
    
    # Merge
    result = shapefile_pd.merge(
        district_stats,
        left_on=district_id_col,
        right_on="district_id_str",
        how="left"
    )
    
    # Fill NaN values for districts with no voters
    result["total_voters"] = result["total_voters"].fillna(0).astype(int)
    result["early_voters"] = result["early_voters"].fillna(0).astype(int)
    result["turnout_rate"] = result["turnout_rate"].fillna(0.0)
    
    # Rename district_id_str to district_id
    result = result.rename(columns={"district_id_str": "district_id"})
    
    print(f"Calculated turnout for {len(result)} districts")
    print(f"Average turnout: {result['turnout_rate'].mean():.2f}%")
    
    return result


def calculate_turnout_metrics(
    merged_voter_df: pl.DataFrame,
    shapefile_2022_cd: gpd.GeoDataFrame,
    shapefile_2022_sd: gpd.GeoDataFrame,
    shapefile_2026: gpd.GeoDataFrame,
    output_dir: str = "data/exports"
) -> dict:
    """
    Calculate turnout metrics for all district types.
    
    Returns:
        Dictionary with turnout DataFrames for each district type
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    (output_path / "csv").mkdir(exist_ok=True, parents=True)
    
    # Calculate turnout for 2022 Congressional districts
    turnout_2022_cd = calculate_turnout_by_district(
        merged_voter_df,
        "congressional",
        shapefile_2022_cd,
        "CD118FP",
        "NAMELSAD20"
    )
    
    # Calculate turnout for 2022 State Senate districts
    turnout_2022_sd = calculate_turnout_by_district(
        merged_voter_df,
        "senate",
        shapefile_2022_sd,
        "SLDUST",
        "NAMELSAD"
    )
    
    # For 2026, we need to determine NEW districts using precinct lookup
    # NEWCD/NEWSD are OLD districts, so we can't use them for 2026 shapefiles
    # We need to match using precinct-to-district lookup
    # Check if 2026_SD column exists (from precinct lookup)
    if "2026_SD" not in merged_voter_df.columns:
        print("\nWARNING: 2026_SD column not found.")
        print("For 2026 districts, we need to build a precinct-to-district lookup.")
        print("This requires spatial matching with precinct shapefiles.")
        print("Skipping 2026 district calculation for now.")
        print("Please run precinct_to_district_lookup.py first to create the lookup.")
        turnout_2026 = None
    else:
        # Use 2026_SD column for 2026 shapefile matching (State Senate)
        turnout_2026 = calculate_turnout_by_district(
            merged_voter_df,
            "senate",  # State Senate district
            shapefile_2026,
            "District",
            "District",
            voter_district_col="2026_SD"  # Use NEW State Senate district from precinct lookup
        )
    
    # Save results
    turnout_2022_cd.to_csv(output_path / "csv" / "turnout_by_district_2022_congressional.csv", index=False)
    turnout_2022_sd.to_csv(output_path / "csv" / "turnout_by_district_2022_senate.csv", index=False)
    
    results = {
        "2022_congressional": turnout_2022_cd,
        "2022_senate": turnout_2022_sd,
        "2026": turnout_2026,
    }
    
    if turnout_2026 is not None:
        turnout_2026.to_csv(output_path / "csv" / "turnout_by_district_2026.csv", index=False)
        print(f"\nSaved turnout metrics to {output_path}")
    else:
        print(f"\nSaved 2022 turnout metrics to {output_path}")
        print("2026 metrics not calculated - precinct lookup required")
    
    return results


def create_geodataframes_with_turnout(
    shapefile_2022_cd: gpd.GeoDataFrame,
    shapefile_2022_sd: gpd.GeoDataFrame,
    shapefile_2026: gpd.GeoDataFrame,
    turnout_metrics: dict
) -> dict:
    """
    Merge turnout metrics back into GeoDataFrames for visualization.
    
    Returns:
        Dictionary with GeoDataFrames containing turnout data
    """
    # Merge turnout data with geometry
    gdf_2022_cd = shapefile_2022_cd.merge(
        turnout_metrics["2022_congressional"][["district_id", "total_voters", "early_voters", "turnout_rate"]],
        left_on="CD118FP",
        right_on="district_id",
        how="left"
    )
    
    gdf_2022_sd = shapefile_2022_sd.merge(
        turnout_metrics["2022_senate"][["district_id", "total_voters", "early_voters", "turnout_rate"]],
        left_on="SLDUST",
        right_on="district_id",
        how="left"
    )
    
    gdf_2026 = shapefile_2026.merge(
        turnout_metrics["2026"][["district_id", "total_voters", "early_voters", "turnout_rate"]],
        left_on="District",
        right_on="district_id",
        how="left"
    )
    
    return {
        "2022_congressional": gdf_2022_cd,
        "2022_senate": gdf_2022_sd,
        "2026": gdf_2026,
    }


