"""
Create visualizations comparing turnout across 2022 and 2026 district boundaries.
Generate choropleth maps and difference visualizations.
"""
from pathlib import Path
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
import pandas as pd


def create_turnout_choropleth(
    gdf: gpd.GeoDataFrame,
    title: str,
    output_path: str,
    column: str = "turnout_rate",
    vmin: float = None,
    vmax: float = None,
    figsize: tuple = (12, 8)
):
    """
    Create a choropleth map of turnout rates.
    
    Args:
        gdf: GeoDataFrame with district boundaries and turnout data
        title: Title for the map
        output_path: Path to save the figure
        column: Column name to visualize
        vmin: Minimum value for color scale
        vmax: Maximum value for color scale
    """
    fig, ax = plt.subplots(figsize=figsize)
    
    # Set color scale limits if not provided
    if vmin is None:
        vmin = gdf[column].min()
    if vmax is None:
        vmax = gdf[column].max()
    
    # Create choropleth
    gdf.plot(
        column=column,
        ax=ax,
        legend=True,
        cmap="YlOrRd",  # Yellow-Orange-Red colormap
        edgecolor="black",
        linewidth=0.5,
        vmin=vmin,
        vmax=vmax,
        legend_kwds={
            "label": "Turnout Rate (%)",
            "shrink": 0.8,
            "orientation": "horizontal",
            "pad": 0.05
        }
    )
    
    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    ax.axis("off")
    
    # Add statistics text
    stats_text = (
        f"Avg Turnout: {gdf[column].mean():.2f}%\n"
        f"Min: {gdf[column].min():.2f}% | Max: {gdf[column].max():.2f}%\n"
        f"Total Districts: {len(gdf)}"
    )
    ax.text(
        0.02, 0.98, stats_text,
        transform=ax.transAxes,
        fontsize=10,
        verticalalignment="top",
        bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5)
    )
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


def create_comparison_map(
    gdf_2022: gpd.GeoDataFrame,
    gdf_2026: gpd.GeoDataFrame,
    title: str,
    output_path: str,
    column: str = "turnout_rate"
):
    """
    Create side-by-side comparison maps of 2022 vs 2026 districts.
    
    Args:
        gdf_2022: GeoDataFrame for 2022 districts
        gdf_2026: GeoDataFrame for 2026 districts
        title: Title for the comparison
        output_path: Path to save the figure
        column: Column name to visualize
    """
    # Set common color scale
    vmin = min(gdf_2022[column].min(), gdf_2026[column].min())
    vmax = max(gdf_2022[column].max(), gdf_2026[column].max())
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10))
    
    # 2022 map
    gdf_2022.plot(
        column=column,
        ax=ax1,
        legend=True,
        cmap="YlOrRd",
        edgecolor="black",
        linewidth=0.5,
        vmin=vmin,
        vmax=vmax,
        legend_kwds={
            "label": "Turnout Rate (%)",
            "shrink": 0.8,
            "orientation": "horizontal",
            "pad": 0.05
        }
    )
    # Determine district types from number of districts
    num_2022 = len(gdf_2022)
    num_2026 = len(gdf_2026)
    
    if num_2022 == 150:
        type_2022 = "House Districts (HD)"
    elif num_2022 == 31:
        type_2022 = "State Senate Districts (SD)"
    elif num_2022 == 38:
        type_2022 = "Congressional Districts (CD)"
    else:
        type_2022 = "Districts"
    
    if num_2026 == 150:
        type_2026 = "House Districts (HD)"
    elif num_2026 == 31:
        type_2026 = "State Senate Districts (SD)"
    elif num_2026 == 38:
        type_2026 = "Congressional Districts (CD)"
    else:
        type_2026 = "Districts"
    
    ax1.set_title(f"2022 {type_2022}", fontsize=14, fontweight="bold")
    ax1.axis("off")
    
    # 2026 map
    gdf_2026.plot(
        column=column,
        ax=ax2,
        legend=True,
        cmap="YlOrRd",
        edgecolor="black",
        linewidth=0.5,
        vmin=vmin,
        vmax=vmax,
        legend_kwds={
            "label": "Turnout Rate (%)",
            "shrink": 0.8,
            "orientation": "horizontal",
            "pad": 0.05
        }
    )
    ax2.set_title(f"2026 {type_2026}", fontsize=14, fontweight="bold")
    ax2.axis("off")
    
    fig.suptitle(title, fontsize=16, fontweight="bold", y=0.98)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


def create_age_bracket_visualization(
    voter_df_path: str,
    output_path: str
):
    """
    Create visualization of age bracket distribution by district.
    
    Args:
        voter_df_path: Path to merged voter data parquet file
        output_path: Path to save the figure
    """
    import polars as pl
    
    df = pl.read_parquet(voter_df_path)
    
    # Group by district and age bracket, calculate turnout
    age_stats = (
        df.group_by(["NEWSD", "age_bracket"])
        .agg([
            pl.count().alias("total"),
            pl.col("voted_early").sum().alias("early_voters"),
            (pl.col("voted_early").mean() * 100).alias("turnout_rate")
        ])
        .sort(["NEWSD", "age_bracket"])
    )
    
    # Convert to pandas for plotting
    age_stats_pd = age_stats.to_pandas()
    
    # Pivot for easier plotting
    pivot_df = age_stats_pd.pivot(
        index="NEWSD",
        columns="age_bracket",
        values="turnout_rate"
    )
    
    # Create heatmap
    fig, ax = plt.subplots(figsize=(14, 10))
    
    # Sort age brackets
    age_order = ["18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "75+"]
    available_columns = [col for col in age_order if col in pivot_df.columns]
    pivot_df = pivot_df[available_columns]
    
    im = ax.imshow(pivot_df.values, cmap="YlOrRd", aspect="auto")
    
    # Set labels
    ax.set_xticks(range(len(pivot_df.columns)))
    ax.set_xticklabels(pivot_df.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(pivot_df.index)))
    ax.set_yticklabels(pivot_df.index)
    ax.set_xlabel("Age Bracket", fontsize=12)
    ax.set_ylabel("District (State Senate - SD)", fontsize=12)
    ax.set_title("Early Voting Turnout Rate by Age Bracket - State Senate Districts (SD)", fontsize=14, fontweight="bold")
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label("Turnout Rate (%)", fontsize=12)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_path}")


def create_all_visualizations(
    shapefile_2022_cd_path: str,
    shapefile_2022_sd_path: str,
    shapefile_2026_path: str,
    turnout_2022_cd_path: str,
    turnout_2022_sd_path: str,
    turnout_2026_path: str,
    merged_voter_path: str,
    output_dir: str = "data/exports/visualizations"
):
    """
    Create all visualizations.
    
    Args:
        shapefile_*_path: Paths to shapefiles
        turnout_*_path: Paths to turnout CSV files
        merged_voter_path: Path to merged voter data
        output_dir: Directory to save visualizations
    """
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    
    print("Creating visualizations...")
    
    # Load shapefiles
    gdf_2022_cd = gpd.read_file(shapefile_2022_cd_path)
    gdf_2022_sd = gpd.read_file(shapefile_2022_sd_path)
    gdf_2026 = gpd.read_file(shapefile_2026_path)
    
    # Load turnout data
    turnout_2022_cd = pd.read_csv(turnout_2022_cd_path)
    turnout_2022_sd = pd.read_csv(turnout_2022_sd_path)
    turnout_2026 = pd.read_csv(turnout_2026_path)
    
    # Merge turnout with shapefiles
    # Ensure types match for merging
    turnout_2022_cd["district_id"] = turnout_2022_cd["district_id"].astype(str)
    gdf_2022_cd["CD118FP"] = gdf_2022_cd["CD118FP"].astype(str)
    gdf_2022_cd = gdf_2022_cd.merge(
        turnout_2022_cd[["district_id", "turnout_rate"]],
        left_on="CD118FP",
        right_on="district_id",
        how="left"
    )
    
    turnout_2022_sd["district_id"] = turnout_2022_sd["district_id"].astype(str)
    gdf_2022_sd["SLDUST"] = gdf_2022_sd["SLDUST"].astype(str)
    gdf_2022_sd = gdf_2022_sd.merge(
        turnout_2022_sd[["district_id", "turnout_rate"]],
        left_on="SLDUST",
        right_on="district_id",
        how="left"
    )
    
    # 2026 uses numeric District column
    # Fill NaN values before converting to int
    turnout_2026["district_id"] = pd.to_numeric(turnout_2026["district_id"], errors="coerce").fillna(0).astype(int)
    gdf_2026["District"] = gdf_2026["District"].astype(int)
    gdf_2026 = gdf_2026.merge(
        turnout_2026[["district_id", "turnout_rate"]],
        left_on="District",
        right_on="district_id",
        how="left"
    )
    
    # Fill NaN values
    for gdf in [gdf_2022_cd, gdf_2022_sd, gdf_2026]:
        gdf["turnout_rate"] = gdf["turnout_rate"].fillna(0.0)
    
    # Determine 2026 district type from number of districts
    num_2026_districts = len(gdf_2026)
    if num_2026_districts == 150:
        district_type_2026 = "House Districts (HD)"
    elif num_2026_districts == 31:
        district_type_2026 = "State Senate Districts (SD)"
    elif num_2026_districts == 38:
        district_type_2026 = "Congressional Districts (CD)"
    else:
        district_type_2026 = "Districts"
    
    # Create individual maps
    create_turnout_choropleth(
        gdf_2022_cd,
        "Early Voting Turnout - 2022 Congressional Districts (CD)",
        output_path / "turnout_2022_congressional.png"
    )
    
    create_turnout_choropleth(
        gdf_2022_sd,
        "Early Voting Turnout - 2022 State Senate Districts (SD)",
        output_path / "turnout_2022_senate.png"
    )
    
    create_turnout_choropleth(
        gdf_2026,
        f"Early Voting Turnout - 2026 {district_type_2026}",
        output_path / "turnout_2026.png"
    )
    
    # Create comparison maps (2022 Senate vs 2026)
    create_comparison_map(
        gdf_2022_sd,
        gdf_2026,
        f"Turnout Comparison: 2022 State Senate Districts (SD) vs 2026 {district_type_2026}",
        output_path / "turnout_comparison_2022_vs_2026.png"
    )
    
    # Create age bracket visualization
    create_age_bracket_visualization(
        merged_voter_path,
        output_path / "turnout_by_age_bracket.png"
    )
    
    print(f"\nAll visualizations saved to {output_path}")


if __name__ == "__main__":
    base_dir = Path(".")
    
    create_all_visualizations(
        shapefile_2022_cd_path="/Users/johneakin/Downloads/data/shapefiles/2022/congressional/tl_2022_48_cd118.shp",
        shapefile_2022_sd_path="/Users/johneakin/Downloads/data/shapefiles/2022/state_senate/tl_2022_48_sldu.shp",
        shapefile_2026_path="/Users/johneakin/Downloads/data/shapefiles/2026/PLANC2333.shp",
        turnout_2022_cd_path="turnout_by_district_2022_congressional.csv",
        turnout_2022_sd_path="turnout_by_district_2022_senate.csv",
        turnout_2026_path="turnout_by_district_2026.csv",
        merged_voter_path="early_voting_merged.parquet",
        output_dir="visualizations"
    )

