"""
Create visualizations for redistricting analysis: maps, charts, and heatmaps.
"""
import polars as pl
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, Optional
import numpy as np

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)


def create_party_composition_map(
    gdf: gpd.GeoDataFrame,
    composition_df: pl.DataFrame,
    district_col: str,
    party_pct_col: str,
    title: str,
    output_path: str
) -> None:
    """
    Create a map showing party composition by district.
    
    Args:
        gdf: GeoDataFrame with district geometries
        composition_df: DataFrame with party composition data
        district_col: Column name for district ID
        party_pct_col: Column name for party percentage
        title: Map title
        output_path: Path to save map
    """
    print(f"Creating party composition map: {title}...")
    
    # Merge composition data with geometries
    composition_pd = composition_df.to_pandas()
    gdf_merged = gdf.merge(
        composition_pd,
        left_on='District',
        right_on=district_col,
        how='left'
    )
    
    # Create figure
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    
    # Plot districts colored by party percentage
    gdf_merged.plot(
        column=party_pct_col,
        ax=ax,
        cmap='RdBu_r',
        legend=True,
        edgecolor='black',
        linewidth=0.5,
        missing_kwds={'color': 'lightgray'}
    )
    
    ax.set_title(title, fontsize=16, fontweight='bold')
    ax.axis('off')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved: {output_path}")


def create_competitiveness_map(
    gdf: gpd.GeoDataFrame,
    competitiveness_df: pl.DataFrame,
    district_col: str,
    competitiveness_col: str,
    title: str,
    output_path: str
) -> None:
    """
    Create a map showing district competitiveness.
    
    Args:
        gdf: GeoDataFrame with district geometries
        competitiveness_df: DataFrame with competitiveness data
        district_col: Column name for district ID
        competitiveness_col: Column name for competitiveness classification
        title: Map title
        output_path: Path to save map
    """
    print(f"Creating competitiveness map: {title}...")
    
    # Merge competitiveness data with geometries
    comp_pd = competitiveness_df.to_pandas()
    gdf_merged = gdf.merge(
        comp_pd,
        left_on='District',
        right_on=district_col,
        how='left'
    )
    
    # Map competitiveness to colors
    color_map = {
        'Solidly Republican': '#d62728',  # Red
        'Solidly Democrat': '#2ca02c',    # Green
        'Competitive': '#ff7f0e',         # Orange
    }
    
    gdf_merged['color'] = gdf_merged[competitiveness_col].map(color_map)
    gdf_merged['color'] = gdf_merged['color'].fillna('lightgray')
    
    # Create figure
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    
    # Plot districts
    gdf_merged.plot(
        color=gdf_merged['color'],
        ax=ax,
        edgecolor='black',
        linewidth=0.5
    )
    
    # Create legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=color_map['Solidly Republican'], label='Solidly Republican'),
        Patch(facecolor=color_map['Solidly Democrat'], label='Solidly Democrat'),
        Patch(facecolor=color_map['Competitive'], label='Competitive'),
    ]
    ax.legend(handles=legend_elements, loc='upper right')
    
    ax.set_title(title, fontsize=16, fontweight='bold')
    ax.axis('off')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved: {output_path}")


def create_redistricting_shifts_chart(
    shifts_df: pl.DataFrame,
    district_type: str,
    output_path: str
) -> None:
    """
    Create a bar chart showing redistricting shifts (gains/losses).
    
    Args:
        shifts_df: DataFrame with redistricting shifts
        district_type: Type of district ('CD', 'SD', 'HD')
        output_path: Path to save chart
    """
    print(f"Creating redistricting shifts chart for {district_type}...")
    
    shifts_pd = shifts_df.to_pandas()
    
    # Select top shifts
    if 'rep_pct_shift' in shifts_pd.columns:
        shifts_pd = shifts_pd.sort_values('rep_pct_shift', ascending=False)
        top_shifts = shifts_pd.head(20)
        
        fig, ax = plt.subplots(1, 1, figsize=(14, 8))
        
        x = np.arange(len(top_shifts))
        width = 0.35
        
        ax.bar(x - width/2, top_shifts['rep_pct_shift'], width, label='Republican', color='#d62728')
        ax.bar(x + width/2, top_shifts['dem_pct_shift'], width, label='Democrat', color='#2ca02c')
        
        ax.set_xlabel('District', fontsize=12)
        ax.set_ylabel('Percentage Point Shift', fontsize=12)
        ax.set_title(f'Top 20 Redistricting Shifts - {district_type}', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(top_shifts['new_district'], rotation=45, ha='right')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  Saved: {output_path}")


def create_party_composition_scatter(
    old_df: pl.DataFrame,
    new_df: pl.DataFrame,
    district_type: str,
    output_path: str
) -> None:
    """
    Create a scatter plot comparing 2022 vs 2026 party composition.
    
    Args:
        old_df: DataFrame with 2022 district composition
        new_df: DataFrame with 2026 district composition
        district_type: Type of district ('CD', 'SD', 'HD')
        output_path: Path to save chart
    """
    print(f"Creating party composition scatter plot for {district_type}...")
    
    old_pd = old_df.to_pandas()
    new_pd = new_df.to_pandas()
    
    # Merge on district (assuming same district numbers)
    if 'old_rep_pct' in old_pd.columns and 'new_rep_pct' in new_pd.columns:
        merged = old_pd.merge(
            new_pd,
            left_on='old_district',
            right_on='new_district',
            how='inner'
        )
        
        fig, ax = plt.subplots(1, 1, figsize=(10, 10))
        
        ax.scatter(merged['old_rep_pct'], merged['new_rep_pct'], alpha=0.6, s=100)
        
        # Add diagonal line
        min_val = min(merged['old_rep_pct'].min(), merged['new_rep_pct'].min())
        max_val = max(merged['old_rep_pct'].max(), merged['new_rep_pct'].max())
        ax.plot([min_val, max_val], [min_val, max_val], 'r--', alpha=0.5, label='No Change')
        
        ax.set_xlabel('2022 Republican %', fontsize=12)
        ax.set_ylabel('2026 Republican %', fontsize=12)
        ax.set_title(f'Party Composition: 2022 vs 2026 - {district_type}', fontsize=14, fontweight='bold')
        ax.legend()
        ax.grid(alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  Saved: {output_path}")


def create_competitiveness_changes_chart(
    old_comp_df: pl.DataFrame,
    new_comp_df: pl.DataFrame,
    district_type: str,
    output_path: str
) -> None:
    """
    Create a bar chart showing competitiveness changes.
    
    Args:
        old_comp_df: DataFrame with 2022 competitiveness
        new_comp_df: DataFrame with 2026 competitiveness
        district_type: Type of district ('CD', 'SD', 'HD')
        output_path: Path to save chart
    """
    print(f"Creating competitiveness changes chart for {district_type}...")
    
    old_pd = old_comp_df.to_pandas()
    new_pd = new_comp_df.to_pandas()
    
    # Count districts by competitiveness
    old_counts = old_pd['old_competitiveness'].value_counts()
    new_counts = new_pd['new_competitiveness'].value_counts()
    
    categories = ['Solidly Republican', 'Solidly Democrat', 'Competitive']
    
    old_values = [old_counts.get(cat, 0) for cat in categories]
    new_values = [new_counts.get(cat, 0) for cat in categories]
    
    fig, ax = plt.subplots(1, 1, figsize=(10, 6))
    
    x = np.arange(len(categories))
    width = 0.35
    
    ax.bar(x - width/2, old_values, width, label='2022', color='#1f77b4')
    ax.bar(x + width/2, new_values, width, label='2026', color='#ff7f0e')
    
    ax.set_xlabel('Competitiveness', fontsize=12)
    ax.set_ylabel('Number of Districts', fontsize=12)
    ax.set_title(f'Competitiveness Changes: 2022 vs 2026 - {district_type}', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"  Saved: {output_path}")


def create_transition_heatmap(
    transition_df: pl.DataFrame,
    district_type: str,
    output_path: str
) -> None:
    """
    Create a heatmap showing voter movement between districts.
    
    Args:
        transition_df: DataFrame with transition matrix
        district_type: Type of district ('CD', 'SD', 'HD')
        output_path: Path to save heatmap
    """
    print(f"Creating transition heatmap for {district_type}...")
    
    transition_pd = transition_df.to_pandas()
    
    # Pivot to create matrix
    if 'old_district' in transition_pd.columns and 'new_district' in transition_pd.columns:
        pivot = transition_pd.pivot_table(
            values='total_moved',
            index='old_district',
            columns='new_district',
            aggfunc='sum',
            fill_value=0
        )
        
        fig, ax = plt.subplots(1, 1, figsize=(14, 12))
        
        sns.heatmap(
            pivot,
            cmap='YlOrRd',
            annot=False,
            fmt='.0f',
            cbar_kws={'label': 'Voters Moved'},
            ax=ax
        )
        
        ax.set_xlabel('2026 District', fontsize=12)
        ax.set_ylabel('2022 District', fontsize=12)
        ax.set_title(f'Voter Movement: 2022 → 2026 - {district_type}', fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  Saved: {output_path}")


def create_all_redistricting_visualizations(
    redistricting_results: Dict,
    competitiveness_results: Dict,
    shapefile_paths: Dict[str, str],
    output_dir: str
) -> None:
    """
    Create all redistricting visualizations.
    
    Args:
        redistricting_results: Dict with redistricting analysis results
        competitiveness_results: Dict with competitiveness analysis results
        shapefile_paths: Dict mapping district types to shapefile paths
        output_dir: Directory to save visualizations
    """
    print("=" * 80)
    print("CREATING REDISTRICTING VISUALIZATIONS")
    print("=" * 80)
    print()
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for district_type in ['CD', 'SD', 'HD']:
        if district_type not in redistricting_results:
            continue
        
        print(f"\nCreating visualizations for {district_type}...")
        
        # Load shapefile
        shapefile_key = f'2026_{district_type}'
        if shapefile_key not in shapefile_paths:
            print(f"  ⚠️  Shapefile not found for {district_type}, skipping maps...")
            continue
        
        try:
            gdf = gpd.read_file(shapefile_paths[shapefile_key])
        except Exception as e:
            print(f"  ⚠️  Error loading shapefile: {e}")
            continue
        
        redist_data = redistricting_results[district_type]
        comp_data = competitiveness_results.get(district_type, {})
        
        # 1. Party composition maps
        if 'new_composition' in redist_data:
            new_comp = redist_data['new_composition']
            if len(new_comp) > 0:
                create_party_composition_map(
                    gdf,
                    new_comp,
                    'new_district',
                    'new_rep_pct',
                    f'2026 Republican % by District - {district_type}',
                    str(output_path / f'party_composition_2026_{district_type.lower()}.png')
                )
        
        if 'old_composition' in redist_data:
            old_comp = redist_data['old_composition']
            if len(old_comp) > 0:
                # Need old shapefile for this
                old_shapefile_key = f'2022_{district_type}'
                if old_shapefile_key in shapefile_paths:
                    try:
                        gdf_old = gpd.read_file(shapefile_paths[old_shapefile_key])
                        create_party_composition_map(
                            gdf_old,
                            old_comp,
                            'old_district',
                            'old_rep_pct',
                            f'2022 Republican % by District - {district_type}',
                            str(output_path / f'party_composition_2022_{district_type.lower()}.png')
                        )
                    except Exception as e:
                        print(f"  ⚠️  Error loading old shapefile: {e}")
        
        # 2. Competitiveness maps
        if 'new_competitiveness' in comp_data:
            new_comp = comp_data['new_competitiveness']
            if len(new_comp) > 0:
                create_competitiveness_map(
                    gdf,
                    new_comp,
                    'new_district',
                    'new_competitiveness',
                    f'2026 Competitiveness - {district_type}',
                    str(output_path / f'competitiveness_2026_{district_type.lower()}.png')
                )
        
        # 3. Redistricting shifts chart
        if 'shifts' in redist_data:
            shifts = redist_data['shifts']
            if len(shifts) > 0:
                create_redistricting_shifts_chart(
                    shifts,
                    district_type,
                    str(output_path / f'redistricting_shifts_{district_type.lower()}.png')
                )
        
        # 4. Party composition scatter
        if 'old_composition' in redist_data and 'new_composition' in redist_data:
            create_party_composition_scatter(
                redist_data['old_composition'],
                redist_data['new_composition'],
                district_type,
                str(output_path / f'party_composition_scatter_{district_type.lower()}.png')
            )
        
        # 5. Competitiveness changes chart
        if 'old_competitiveness' in comp_data and 'new_competitiveness' in comp_data:
            create_competitiveness_changes_chart(
                comp_data['old_competitiveness'],
                comp_data['new_competitiveness'],
                district_type,
                str(output_path / f'competitiveness_changes_{district_type.lower()}.png')
            )
        
        # 6. Transition heatmap
        if 'transition_matrix' in redist_data:
            transition = redist_data['transition_matrix']
            if len(transition) > 0:
                create_transition_heatmap(
                    transition,
                    district_type,
                    str(output_path / f'transition_heatmap_{district_type.lower()}.png')
                )
    
    print()
    print("=" * 80)
    print("All visualizations created!")
    print("=" * 80)
    print()


if __name__ == "__main__":
    # Test visualizations
    import sys
    from pathlib import Path
    
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "data/exports/visualizations"
    else:
        from tx_election_results.config import config
        input_path = str(config.MODELED_DATA)
        output_dir = str(config.VISUALIZATIONS_DIR)
    
    if not Path(input_path).exists():
        print(f"Error: {input_path} not found.")
        print("Please run the analysis step first.")
        sys.exit(1)
    
    # This would typically be called from the main pipeline with pre-computed results
    print("Visualizations should be created from the main pipeline with analysis results.")

