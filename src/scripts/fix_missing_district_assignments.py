"""
Fix missing district assignments by using spatial fallback for voters without precinct matches.
For districts with 0 voters, attempts to assign voters using old district boundaries and spatial relationships.
"""
import polars as pl
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pathlib import Path
import numpy as np


def fix_missing_district_assignments():
    """Fix districts with 0 voters by using spatial fallback."""
    print("=" * 80)
    print("FIXING MISSING DISTRICT ASSIGNMENTS")
    print("=" * 80)
    print()
    
    # Load data
    print("Loading data...")
    df = pl.read_parquet("early_voting_merged.parquet")
    print(f"Loaded {len(df):,} voters")
    
    # Load shapefiles
    gdf_2026_hd = gpd.read_file("/Users/johneakin/Downloads/data/shapefiles/2024/texas_house/PLANH2316.shp")
    gdf_2022_hd = gpd.read_file("/Users/johneakin/Downloads/data/shapefiles/2022/state_house/tl_2022_48_sldl.shp")
    
    # Check which districts have 0 voters
    hd_districts = df.filter(
        pl.col("2026_HD").is_not_null() & (pl.col("2026_HD") != 0)
    ).group_by("2026_HD").agg(pl.len().alias("voter_count"))
    
    all_districts = set(range(1, 151))
    districts_with_voters = set(hd_districts["2026_HD"].to_list())
    districts_without_voters = sorted(all_districts - districts_with_voters)
    
    print(f"Districts with voters: {len(districts_with_voters)}")
    print(f"Districts without voters: {len(districts_without_voters)}")
    print(f"  Missing districts: {districts_without_voters}")
    print()
    
    if len(districts_without_voters) == 0:
        print("✅ All districts have voters assigned!")
        return df
    
    # For each missing district, find voters that should be assigned
    # Strategy: Use voters from neighboring old districts that overlap with the new district
    print("Attempting to assign voters to missing districts...")
    print("Using spatial relationship between 2022 and 2026 districts...")
    
    # Build a mapping: old HD → new HD based on spatial overlap
    # This will help us assign voters to missing districts
    print("\nBuilding old-to-new district spatial mapping...")
    
    # Ensure both shapefiles are in the same CRS
    if gdf_2022_hd.crs != gdf_2026_hd.crs:
        gdf_2022_hd = gdf_2022_hd.to_crs(gdf_2026_hd.crs)
    
    # Find which old districts overlap with missing new districts
    missing_dist_gdf = gdf_2026_hd[gdf_2026_hd["District"].isin(districts_without_voters)]
    
    # Spatial join to find overlapping old districts
    overlaps = gpd.sjoin(
        gdf_2022_hd,
        missing_dist_gdf,
        how="inner",
        predicate="intersects"
    )
    
    if len(overlaps) > 0:
        # Create mapping: old district → new district
        old_to_new_mapping = overlaps.groupby("SLDLST").agg({
            "District": lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None
        }).reset_index()
        old_to_new_mapping.columns = ["old_hd", "new_hd_fallback"]
        
        print(f"Found {len(old_to_new_mapping)} old districts that overlap with missing new districts")
        
        # Apply fallback: For voters in old districts that overlap with missing new districts,
        # assign them to the new district if they don't already have one
        old_to_new_dict = dict(zip(
            old_to_new_mapping["old_hd"].astype(int),
            old_to_new_mapping["new_hd_fallback"].astype(int)
        ))
        
        # For voters without 2026_HD, check if their old HD maps to a missing district
        df = df.with_columns([
            pl.when(pl.col("2026_HD").is_null())
            .then(
                pl.col("NEWHD").map_elements(
                    lambda x: old_to_new_dict.get(int(x), None) if x is not None else None,
                    return_dtype=pl.Int64
                )
            )
            .otherwise(pl.col("2026_HD"))
            .alias("2026_HD_fallback")
        ])
        
        # Update 2026_HD for voters that got a fallback assignment
        df = df.with_columns([
            pl.when(pl.col("2026_HD").is_null() & pl.col("2026_HD_fallback").is_not_null())
            .then(pl.col("2026_HD_fallback"))
            .otherwise(pl.col("2026_HD"))
            .alias("2026_HD")
        ])
        
        # Drop temporary column
        df = df.drop("2026_HD_fallback")
        
        # Check results
        print(f"\nAfter fallback assignment:")
        hd_districts_after = df.filter(
            pl.col("2026_HD").is_not_null() & (pl.col("2026_HD") != 0)
        ).group_by("2026_HD").agg(pl.len().alias("voter_count"))
        
        districts_with_voters_after = set(hd_districts_after["2026_HD"].to_list())
        districts_without_voters_after = sorted(all_districts - districts_with_voters_after)
        
        print(f"Districts with voters: {len(districts_with_voters_after)}")
        print(f"Districts still without voters: {len(districts_without_voters_after)}")
        if len(districts_without_voters_after) > 0:
            print(f"  Still missing: {districts_without_voters_after}")
    else:
        print("⚠️  No spatial overlaps found between 2022 and missing 2026 districts")
        print("These districts may be genuinely new/empty districts")
    
    # For any remaining missing districts, try a different approach:
    # Assign voters proportionally from neighboring districts or use centroid-based assignment
    if len(districts_without_voters_after) > 0:
        print(f"\nAttempting alternative assignment for remaining {len(districts_without_voters_after)} districts...")
        
        # For each missing district, find the closest district with voters and proportionally assign
        # This is a heuristic approach - assign voters from old districts that are geographically closest
        for missing_dist in districts_without_voters_after:
            # Get the district geometry
            missing_dist_geom = gdf_2026_hd[gdf_2026_hd["District"] == missing_dist].geometry.iloc[0]
            
            # Find old districts that overlap with this new district
            old_districts_overlap = gdf_2022_hd[gdf_2022_hd.geometry.intersects(missing_dist_geom)]
            
            if len(old_districts_overlap) > 0:
                # Get voters from these old districts who don't have a 2026_HD yet
                old_dist_nums = old_districts_overlap["SLDLST"].astype(int).tolist()
                
                unassigned_voters = df.filter(
                    pl.col("NEWHD").is_in(old_dist_nums) &
                    (pl.col("2026_HD").is_null() | (pl.col("2026_HD") == 0))
                )
                
                if len(unassigned_voters) > 0:
                    # Assign these voters to the missing district
                    print(f"  Assigning {len(unassigned_voters):,} voters from old districts {old_dist_nums} to new district {missing_dist}")
                    df = df.with_columns([
                        pl.when(
                            pl.col("VUID").is_in(unassigned_voters["VUID"].to_list())
                        )
                        .then(pl.lit(missing_dist))
                        .otherwise(pl.col("2026_HD"))
                        .alias("2026_HD")
                    ])
    
    # Final check
    print(f"\nFinal check:")
    hd_districts_final = df.filter(
        pl.col("2026_HD").is_not_null() & (pl.col("2026_HD") != 0)
    ).group_by("2026_HD").agg(pl.len().alias("voter_count"))
    
    districts_with_voters_final = set(hd_districts_final["2026_HD"].to_list())
    districts_without_voters_final = sorted(all_districts - districts_with_voters_final)
    
    print(f"Districts with voters: {len(districts_with_voters_final)}")
    print(f"Districts without voters: {len(districts_without_voters_final)}")
    if len(districts_without_voters_final) > 0:
        print(f"  Still missing: {districts_without_voters_final}")
        print(f"  These districts may be genuinely new/empty or have no registered voters")
    
    # Save updated data
    print(f"\nSaving updated data...")
    df.write_parquet("early_voting_merged.parquet")
    print("✅ Updated merged data saved!")
    
    return df


if __name__ == "__main__":
    fix_missing_district_assignments()

