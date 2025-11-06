"""
Common utility functions for analysis and data processing.
Extracted common patterns from analysis scripts.
"""
from pathlib import Path
from typing import Optional
import polars as pl
import pandas as pd


def map_modeled_party_to_r_d(party_score: str) -> Optional[str]:
    """
    Map modeled party scores to Republican/Democrat for aggregation.
    
    Args:
        party_score: Party score string (e.g., "Republican-leaning", "Democrat-leaning")
    
    Returns:
        "Republican", "Democrat", or None
    """
    if not party_score:
        return None
    party_score = str(party_score).strip()
    if "Republican" in party_score:
        return "Republican"
    elif "Democrat" in party_score:
        return "Democrat"
    return None


def calculate_party_composition(
    voter_df: pl.DataFrame,
    district_col: str,
    party_col: str = "party",
    use_modeled: bool = True
) -> pd.DataFrame:
    """
    Calculate party composition (Republican/Democrat counts) by district.
    
    Args:
        voter_df: Voter dataframe with district assignments
        district_col: Column name for district ID
        party_col: Column name for party affiliation
        use_modeled: Whether to include modeled party predictions
    
    Returns:
        DataFrame with party composition by district
    """
    # Filter to voters with district assignments
    voters_with_district = voter_df.filter(pl.col(district_col).is_not_null())
    
    # If using modeled data, ensure we have the unified party column
    if use_modeled and "predicted_party_score" in voter_df.columns:
        # Use known party if available, otherwise map modeled score
        voters_with_district = voters_with_district.with_columns([
            pl.when(pl.col(party_col).is_in(["Republican", "Democrat", "Swing"]))
            .then(pl.col(party_col))
            .otherwise(pl.col("predicted_party_score"))
            .alias("unified_party")
        ])
        party_col = "unified_party"
    
    # Count by district and party
    composition = (
        voters_with_district
        .group_by([district_col, party_col])
        .agg(pl.count().alias("voter_count"))
        .sort([district_col, party_col])
    )
    
    # Convert to pandas for easier manipulation
    composition_pd = composition.to_pandas()
    
    # Create summary by district
    summary = composition_pd.groupby(district_col).agg({
        "voter_count": "sum"
    }).reset_index()
    
    # Add Republican and Democrat counts
    rep_counts = composition_pd[composition_pd[party_col] == "Republican"].groupby(district_col)["voter_count"].sum()
    dem_counts = composition_pd[composition_pd[party_col] == "Democrat"].groupby(district_col)["voter_count"].sum()
    
    summary = summary.merge(
        rep_counts.rename("republican_voters").reset_index(),
        on=district_col,
        how="left"
    ).merge(
        dem_counts.rename("democrat_voters").reset_index(),
        on=district_col,
        how="left"
    )
    
    summary["republican_voters"] = summary["republican_voters"].fillna(0).astype(int)
    summary["democrat_voters"] = summary["democrat_voters"].fillna(0).astype(int)
    summary["total_voters"] = summary["voter_count"]
    summary = summary.drop(columns=["voter_count"])
    
    return summary


def save_analysis_csv(
    df: pd.DataFrame,
    output_path: Path,
    filename: str,
    create_subdir: bool = True
) -> Path:
    """
    Save DataFrame to CSV with standardized path handling.
    
    Args:
        df: DataFrame to save
        output_path: Base output directory
        filename: CSV filename
        create_subdir: Whether to create csv/ subdirectory
    
    Returns:
        Full path to saved file
    """
    output_path = Path(output_path)
    output_path.mkdir(exist_ok=True, parents=True)
    
    if create_subdir:
        csv_dir = output_path / "csv"
        csv_dir.mkdir(exist_ok=True, parents=True)
        file_path = csv_dir / filename
    else:
        file_path = output_path / filename
    
    df.to_csv(file_path, index=False)
    return file_path


def format_district_summary(
    summary_df: pd.DataFrame,
    district_type: str,
    comparison_label: str
) -> pd.DataFrame:
    """
    Format district summary with standardized columns and statistics.
    
    Args:
        summary_df: Summary DataFrame with district statistics
        district_type: Type of district (e.g., "State Senate Districts (SD)")
        comparison_label: Label for comparison (e.g., "NEWSD to 2026_SD")
    
    Returns:
        Formatted summary DataFrame
    """
    total_districts = len(summary_df)
    
    # Count districts where Dem change > Rep change
    if "net_democrat_change" in summary_df.columns and "net_republican_change" in summary_df.columns:
        dem_larger = len(summary_df[summary_df["net_democrat_change"] > summary_df["net_republican_change"]])
        rep_larger = len(summary_df[summary_df["net_republican_change"] > summary_df["net_democrat_change"]])
        equal = len(summary_df[summary_df["net_republican_change"] == summary_df["net_democrat_change"]])
        
        # Calculate totals
        total_net_rep_change = summary_df["net_republican_change"].sum()
        total_net_dem_change = summary_df["net_democrat_change"].sum()
        
        formatted = pd.DataFrame([{
            "District_Type": district_type,
            "Comparison": comparison_label,
            "Total_Districts": total_districts,
            "Districts_Where_Dem_Change_Larger": dem_larger,
            "Districts_Where_Rep_Change_Larger": rep_larger,
            "Districts_Where_Equal": equal,
            "Total_Net_Republican_Change": total_net_rep_change,
            "Total_Net_Democrat_Change": total_net_dem_change,
            "Pct_Districts_Dem_Larger": (dem_larger / total_districts * 100) if total_districts > 0 else 0,
            "Pct_Districts_Rep_Larger": (rep_larger / total_districts * 100) if total_districts > 0 else 0,
        }])
        
        return formatted
    
    return summary_df


def create_district_type_aggregation(
    voter_df: pl.DataFrame,
    district_col: str,
    groupby_cols: Optional[list] = None
) -> pd.DataFrame:
    """
    Create aggregation by district type with common metrics.
    
    Args:
        voter_df: Voter dataframe
        district_col: Column name for district ID
        groupby_cols: Additional columns to group by
    
    Returns:
        Aggregated DataFrame with common metrics
    """
    if groupby_cols is None:
        groupby_cols = [district_col]
    else:
        groupby_cols = [district_col] + groupby_cols
    
    # Common aggregations
    aggregation = (
        voter_df
        .filter(pl.col(district_col).is_not_null())
        .group_by(groupby_cols)
        .agg([
            pl.count().alias("total_voters"),
            pl.col("voted_early").sum().alias("early_voters"),
            (pl.col("voted_early").sum() / pl.count() * 100).alias("turnout_rate"),
        ])
        .sort(groupby_cols)
    )
    
    return aggregation.to_pandas()


