"""
Create precinct-to-2026-district lookup table using spatial intersection.
Uses 2024 precinct shapefiles to determine which precincts are in which 2026 districts.
"""
from pathlib import Path
import polars as pl
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from tqdm import tqdm


def build_county_code_to_name_mapping(voterfile_df: pl.DataFrame) -> dict:
    """
    Build a mapping from county code to county name.
    We'll need to infer this from the voterfile and precinct shapefile.
    
    For now, we'll create a mapping by matching precinct codes between
    voterfile and shapefile, then extracting county names.
    
    Args:
        voterfile_df: Voter dataframe with COUNTY and PCT columns
    
    Returns:
        Dictionary mapping county code (int) to county name (str)
    """
    # This is a placeholder - in practice, we'd need a proper Texas county code lookup
    # For now, we'll build it from the data by matching precincts
    print("Building county code to name mapping...")
    
    # Get unique county+precinct combinations from voterfile
    voter_precincts = voterfile_df.select(["COUNTY", "PCT"]).unique()
    
    # We'll need to match this with the shapefile to build the mapping
    # For now, return empty dict - we'll handle county matching in apply_precinct_lookup
    return {}


def build_precinct_to_district_lookup_spatial(
    precinct_shapefile_path: str,
    district_shapefile: gpd.GeoDataFrame,
    district_col_name: str = "District",
    output_col_name: str = "2026_District",
    output_path: str = None,
    use_cached: bool = True
) -> pd.DataFrame:
    """
    Build a lookup table mapping CNTY+PREC to district using spatial intersection.
    
    Strategy:
    1. Load 2024 precinct shapefile (with CNTY + PREC codes)
    2. Load district shapefile (could be CD, SD, or HD)
    3. Ensure both are in same CRS
    4. Do spatial intersection to find which precincts overlap with which districts
    5. For precincts that span multiple districts, use the district with largest overlap
    6. Create CNTY+PREC → district mapping
    
    Args:
        precinct_shapefile_path: Path to 2024 precinct shapefile
        district_shapefile: GeoDataFrame with district boundaries
        district_col_name: Name of the district column in the shapefile (default: "District")
        output_col_name: Name of the output column (default: "2026_District")
        output_path: Optional path to save lookup table
        use_cached: If True, load existing lookup if available
    
    Returns:
        DataFrame with CNTY, PREC, and output_col_name columns
    """
    # Check for cached lookup
    if use_cached and output_path and Path(output_path).exists():
        print(f"Loading cached lookup from {output_path}...")
        return pd.read_csv(output_path)
    
    print(f"Building precinct-to-district lookup using spatial intersection...")
    print(f"Loading precinct shapefile: {precinct_shapefile_path}")
    
    # Load precinct shapefile
    gdf_precincts = gpd.read_file(precinct_shapefile_path)
    gdf_districts = district_shapefile.copy()
    
    print(f"Loaded {len(gdf_precincts)} precincts")
    print(f"Loaded {len(gdf_districts)} districts")
    
    # Ensure both are in the same CRS
    if gdf_precincts.crs != gdf_districts.crs:
        print(f"Converting precincts CRS from {gdf_precincts.crs} to {gdf_districts.crs}")
        gdf_precincts = gdf_precincts.to_crs(gdf_districts.crs)
    
    print("Performing spatial intersection...")
    print("This may take a few minutes for ~9,000 precincts...")
    
    # Spatial intersection: find which precincts overlap with which districts
    # Use spatial join to find which precincts intersect with which districts
    joined = gpd.sjoin(gdf_precincts, gdf_districts, how='left', predicate='intersects')
    
    print(f"Spatial join completed: {len(joined)} intersections found")
    
    # For precincts that intersect multiple districts, we need to find the one with largest overlap
    print("Calculating overlap areas for split precincts...")
    
    # Group by precinct and find the district with largest overlap
    precincts_with_districts = []
    
    # Create a lookup for precinct geometries
    precinct_geoms_dict = {}
    for idx, row in gdf_precincts.iterrows():
        precinct_geoms_dict[(row['CNTY'], row['PREC'])] = row.geometry
    
    for (cnty, prec), group in tqdm(joined.groupby(['CNTY', 'PREC']), desc="Processing precincts", total=len(gdf_precincts)):
        if len(group) == 1:
            # Single district match
            precincts_with_districts.append({
                'CNTY': cnty,
                'PREC': prec,
                output_col_name: group.iloc[0][district_col_name]
            })
        else:
            # Multiple districts - find the one with largest overlap
            precinct_geom = precinct_geoms_dict[(cnty, prec)]
            max_overlap = 0
            best_district = None
            
            for idx, row in group.iterrows():
                if pd.notna(row[district_col_name]) and pd.notna(row['index_right']):
                    # Get district geometry from the original districts dataframe
                    district_idx = row['index_right']  # Index from right dataframe
                    district_geom = gdf_districts.loc[district_idx, 'geometry']
                    intersection = precinct_geom.intersection(district_geom)
                    overlap_area = intersection.area
                    
                    if overlap_area > max_overlap:
                        max_overlap = overlap_area
                        best_district = row[district_col_name]
            
            precincts_with_districts.append({
                'CNTY': cnty,
                'PREC': prec,
                output_col_name: best_district
            })
    
    # Create lookup DataFrame
    lookup = pd.DataFrame(precincts_with_districts)
    
    # Count how many precincts got matched
    matched = lookup[output_col_name].notna().sum()
    print(f"\nMatched {matched} out of {len(lookup)} precincts to districts ({matched/len(lookup)*100:.2f}%)")
    
    # Save lookup
    if output_path:
        lookup.to_csv(output_path, index=False)
        print(f"Saved lookup table to {output_path}")
    
    return lookup


def build_precinct_to_district_lookup(
    merged_voter_df: pl.DataFrame,
    district_shapefile: gpd.GeoDataFrame,
    district_col_name: str = "District",
    output_col_name: str = "2026_District",
    precinct_shapefile_path: str = None,
    output_path: str = None,
    use_cached: bool = True
) -> pd.DataFrame:
    """
    Build a lookup table mapping COUNTY+PCT to district.
    
    This function now uses spatial intersection with precinct shapefiles
    instead of geocoding addresses.
    
    Args:
        merged_voter_df: Merged voter data (used to determine which precinct shapefile to use)
        district_shapefile: GeoDataFrame with district boundaries (CD, SD, or HD)
        district_col_name: Name of the district column in the shapefile (default: "District")
        output_col_name: Name of the output column (default: "2026_District")
        precinct_shapefile_path: Path to 2024 precinct shapefile (defaults to general)
        output_path: Optional path to save lookup table
        use_cached: If True, load existing lookup if available
    
    Returns:
        DataFrame with CNTY, PREC, and output_col_name columns
        (County codes, not names - will be mapped in apply_precinct_lookup)
    """
    if precinct_shapefile_path is None:
        # Default to general precincts
        precinct_shapefile_path = "/Users/johneakin/Downloads/data/shapefiles/2024/general_precincts/Precincts24G.shp"
    
    # Build lookup using spatial intersection
    # This returns CNTY (code) + PREC → district
    lookup_spatial = build_precinct_to_district_lookup_spatial(
        precinct_shapefile_path,
        district_shapefile,
        district_col_name=district_col_name,
        output_col_name=output_col_name,
        output_path=output_path,
        use_cached=use_cached
    )
    
    return lookup_spatial


def apply_precinct_lookup(
    voter_df: pl.DataFrame,
    lookup_df: pd.DataFrame,
    output_col_name: str = "2026_District"
) -> pl.DataFrame:
    """
    Apply precinct-to-district lookup to voter dataframe.
    
    This function handles the mapping between:
    - Voterfile: COUNTY (name) + PCT (string)
    - Lookup: CNTY (code) + PREC (string)
    
    Strategy:
    1. Build county name → code mapping by matching precinct codes
    2. Join lookup on both county code and precinct code
    
    Args:
        voter_df: Voter dataframe with COUNTY and PCT columns
        lookup_df: Lookup table with CNTY, PREC, and output_col_name columns
        output_col_name: Name of the output column to add (default: "2026_District")
    
    Returns:
        Voter dataframe with output_col_name column added
    """
    print(f"Applying precinct-to-district lookup (output column: {output_col_name})...")
    print("Building county name to code mapping...")
    
    # Convert lookup to polars for easier matching
    lookup_pl = pl.from_pandas(lookup_df)
    
    # Build county name to code mapping
    # Strategy: Match precinct codes between voterfile and lookup
    # Since precinct codes might repeat across counties, we'll match on PCT first
    # then use the county information to disambiguate
    
    # Get unique precinct codes from voterfile
    voter_precincts = voter_df.select(["COUNTY", "PCT"]).unique()
    
    # Get unique precinct codes from lookup
    lookup_precincts = lookup_pl.select(["CNTY", "PREC"]).unique()
    
    # Create a mapping: (COUNTY, PCT) → CNTY code
    # We'll do this by matching PCT codes and seeing which CNTY codes appear
    # for precincts that likely belong to each county
    
    # For each unique COUNTY+PCT in voterfile, find matching CNTY+PREC in lookup
    # Match on PCT == PREC, then use county name to disambiguate if needed
    
    # Rename PREC to PCT for matching
    lookup_pl_renamed = lookup_pl.with_columns([
        pl.col("PREC").alias("PCT")
    ])
    
    # Strategy: First, try to build a county name → code mapping
    # by matching precinct codes that are unique across counties
    # Then use that mapping for the final join
    
    # Get unique COUNTY+PCT from voterfile
    voter_unique = voter_df.select(["COUNTY", "PCT"]).unique()
    
    # Get unique CNTY+PREC from lookup
    lookup_unique = lookup_pl.select(["CNTY", "PREC"]).unique()
    
    # Try matching on PCT/PREC to find county mappings
    # Join on precinct code to see which CNTY codes match which COUNTY names
    temp_join = voter_unique.join(
        lookup_pl_renamed.select(["CNTY", "PCT"]).unique(),
        on="PCT",
        how="inner"
    )
    
    # Build county mapping: for each COUNTY, find the most common CNTY code
    county_mapping = (
        temp_join
        .group_by(["COUNTY", "CNTY"])
        .agg(pl.count().alias("count"))
        .sort(["COUNTY", "count"], descending=[False, True])
        .group_by("COUNTY")
        .agg(pl.col("CNTY").first().alias("CNTY"))
    )
    
    # Create dictionary for mapping
    county_map_dict = dict(zip(
        county_mapping["COUNTY"].to_list(),
        county_mapping["CNTY"].to_list()
    ))
    
    print(f"Built county mapping for {len(county_map_dict)} counties")
    
    # Add CNTY code to voter_df (if not already present)
    if "CNTY" not in voter_df.columns:
        voter_df = voter_df.with_columns([
            pl.col("COUNTY").map_elements(
                lambda x: county_map_dict.get(x, None),
                return_dtype=pl.Int64
            ).alias("CNTY")
        ])
    else:
        # Update existing CNTY column if needed
        voter_df = voter_df.with_columns([
            pl.col("COUNTY").map_elements(
                lambda x: county_map_dict.get(x, None),
                return_dtype=pl.Int64
            ).alias("CNTY")
        ])
    
    # First, try joining on both CNTY and PCT (exact match)
    voter_df = voter_df.join(
        lookup_pl_renamed.select(["CNTY", "PCT", output_col_name]),
        on=["CNTY", "PCT"],
        how="left"
    )
    
    matched_exact = voter_df[output_col_name].is_not_null().sum()
    total = len(voter_df)
    print(f"Matched {matched_exact:,} out of {total:,} voters to {output_col_name} ({matched_exact/total*100:.2f}%)")
    
    # Fallback: For voters still unmatched, try matching on PCT code only
    # This handles cases where precinct codes repeat across counties
    # and the county mapping might be slightly off
    unmatched = voter_df.filter(pl.col(output_col_name).is_null())
    matched_count = matched_exact
    
    if len(unmatched) > 0:
        print(f"Attempting fallback matching for {len(unmatched):,} unmatched voters...")
        print("Using PCT code only (precinct codes may repeat across counties)...")
        
        # Get unique precinct-to-district mappings from lookup
        # For precincts that appear in multiple counties, we'll use the most common district
        precinct_district_mapping = (
            lookup_pl_renamed
            .group_by(["PCT", output_col_name])
            .agg(pl.count().alias("count"))
            .sort(["PCT", "count"], descending=[False, True])
            .group_by("PCT")
            .agg(pl.col(output_col_name).first().alias(output_col_name))
        )
        
        # For unmatched voters, join on PCT code only to get district
        unmatched_with_district = unmatched.join(
            precinct_district_mapping.select(["PCT", output_col_name]),
            on="PCT",
            how="left",
            suffix="_fallback"
        )
        
        # Update voter_df: use fallback district if original is null
        fallback_col = f"{output_col_name}_fallback"
        voter_df = voter_df.join(
            unmatched_with_district.select(["VUID", fallback_col]),
            on="VUID",
            how="left"
        )
        
        voter_df = voter_df.with_columns([
            pl.when(pl.col(output_col_name).is_null())
            .then(pl.col(fallback_col))
            .otherwise(pl.col(output_col_name))
            .alias(output_col_name)
        ]).drop(fallback_col)
        
        matched_fallback = voter_df.filter(
            pl.col(output_col_name).is_not_null()
        ).filter(
            pl.col("VUID").is_in(unmatched["VUID"].to_list())
        ).height - matched_exact
        
        matched_count = voter_df[output_col_name].is_not_null().sum()
        if matched_fallback > 0:
            print(f"Fallback matched {matched_fallback:,} additional voters")
    
    print(f"Total matched: {matched_count:,} out of {total:,} voters ({matched_count/total*100:.2f}%)")
    
    return voter_df


if __name__ == "__main__":
    print("Precinct-to-district lookup module")
    print("This module uses spatial intersection with precinct shapefiles")
