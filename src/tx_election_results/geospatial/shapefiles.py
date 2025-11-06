"""
Load shapefiles for geospatial operations.
"""
import geopandas as gpd


def load_shapefiles(
    shapefile_2022_congressional: str,
    shapefile_2022_senate: str,
    shapefile_2026: str
):
    """
    Load all shapefiles.
    
    Returns:
        Tuple of (gdf_2022_cd, gdf_2022_sd, gdf_2026)
    """
    print("Loading shapefiles...")
    
    gdf_2022_cd = gpd.read_file(shapefile_2022_congressional)
    print(f"Loaded 2022 Congressional districts: {len(gdf_2022_cd)} districts")
    
    gdf_2022_sd = gpd.read_file(shapefile_2022_senate)
    print(f"Loaded 2022 State Senate districts: {len(gdf_2022_sd)} districts")
    
    gdf_2026 = gpd.read_file(shapefile_2026)
    print(f"Loaded 2026 districts: {len(gdf_2026)} districts")
    
    return gdf_2022_cd, gdf_2022_sd, gdf_2026


