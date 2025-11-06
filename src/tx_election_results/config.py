"""
Configuration for Texas 2026 Election Results analysis.
"""
from pathlib import Path
from typing import Optional


class Config:
    """Configuration class for data paths and settings."""
    
    # Data paths
    VF_2024: str = "/Users/johneakin/PyCharmProjects/vep-2024/data/voterfiles/texas/texasnovember2024.csv"
    EV_DATA_DIR: str = "/Users/johneakin/Downloads/data"
    
    # Shapefile paths - OLD districts (2022/2024 boundaries)
    SHAPEFILE_2022_CD: str = "/Users/johneakin/Downloads/data/shapefiles/2022/congressional/tl_2022_48_cd118.shp"
    SHAPEFILE_2022_SD: str = "/Users/johneakin/Downloads/data/shapefiles/2022/state_senate/tl_2022_48_sldu.shp"
    
    # NEW districts (2024/2026 boundaries - 2023-2026)
    SHAPEFILE_2024_CD: str = "/Users/johneakin/Downloads/data/shapefiles/2024/congressional/PLANC2193.shp"
    SHAPEFILE_2024_SD: str = "/Users/johneakin/Downloads/data/shapefiles/2024/texas_senate/PLANS2168.shp"
    SHAPEFILE_2024_HD: str = "/Users/johneakin/Downloads/data/shapefiles/2024/texas_house/PLANH2316.shp"
    SHAPEFILE_2026: str = "/Users/johneakin/Downloads/data/shapefiles/2026/PLANC2333.shp"  # Keep for backward compatibility
    
    PRECINCT_SHAPEFILE_2024: str = "/Users/johneakin/Downloads/data/shapefiles/2024/general_precincts/Precincts24G.shp"
    
    # Output paths
    OUTPUT_DIR: Path = Path("data/exports")
    PROCESSED_VOTERFILE: Path = OUTPUT_DIR / "parquet" / "processed_voterfile.parquet"
    PROCESSED_EARLY_VOTING: Path = OUTPUT_DIR / "parquet" / "processed_early_voting.parquet"
    MERGED_DATA: Path = OUTPUT_DIR / "parquet" / "early_voting_merged.parquet"
    MODELED_DATA: Path = OUTPUT_DIR / "parquet" / "voters_with_party_modeling.parquet"
    PRECINCT_LOOKUP_SD: Path = OUTPUT_DIR / "lookups" / "precinct_to_2026_sd_lookup.csv"
    PRECINCT_LOOKUP_CD: Path = OUTPUT_DIR / "lookups" / "precinct_to_2026_cd_lookup.csv"
    PRECINCT_LOOKUP_HD: Path = OUTPUT_DIR / "lookups" / "precinct_to_2026_hd_lookup.csv"
    VISUALIZATIONS_DIR: Path = OUTPUT_DIR / "visualizations"


# Global config instance
config = Config()

