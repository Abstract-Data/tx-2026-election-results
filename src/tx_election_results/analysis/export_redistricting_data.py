"""
Export comprehensive redistricting analysis data to CSV files.
"""
import polars as pl
from pathlib import Path
from typing import Dict, Optional

from tx_election_results.analysis.redistricting_impact import analyze_all_district_types
from tx_election_results.analysis.competitiveness import assess_all_district_types


def export_voter_classifications(
    df: pl.DataFrame,
    output_path: str
) -> None:
    """
    Export voter-level data with classifications and district assignments.
    
    Args:
        df: DataFrame with voter classifications
        output_path: Path to save CSV
    """
    print(f"Exporting voter classifications to {output_path}...")
    
    # Select key columns for export
    export_cols = [
        'VUID',
        'COUNTY',
        'PCT',
        'age',
        'age_bracket',
        'RCITY',
        'primary_classification',
        'party_final',
        'NEWCD', 'NEWSD', 'NEWHD',  # 2022 districts
        '2026_CD', '2026_SD', '2026_HD',  # 2026 districts
    ]
    
    # Add prediction columns if they exist
    if 'predicted_party' in df.columns:
        export_cols.extend(['predicted_party', 'predicted_party_prob_rep', 'predicted_party_prob_dem'])
    
    # Filter to available columns
    available_cols = [col for col in export_cols if col in df.columns]
    
    df_export = df.select(available_cols)
    df_export.write_csv(output_path)
    print(f"  Exported {len(df_export):,} voters")


def export_redistricting_analysis(
    df: pl.DataFrame,
    output_dir: str,
    party_col: str = 'party_final'
) -> Dict[str, Dict[str, pl.DataFrame]]:
    """
    Export redistricting impact analysis results.
    
    Args:
        df: DataFrame with voters and classifications
        output_dir: Directory to save CSV files
        party_col: Column name for party classification
        
    Returns:
        Dict with redistricting analysis results
    """
    print("=" * 80)
    print("EXPORTING REDISTRICTING ANALYSIS")
    print("=" * 80)
    print()
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Run redistricting analysis
    redistricting_results = analyze_all_district_types(df, party_col, output_dir=str(output_path))
    
    return redistricting_results


def export_competitiveness_analysis(
    df: pl.DataFrame,
    output_dir: str,
    party_col: str = 'party_final',
    threshold: float = 57.0
) -> Dict[str, Dict[str, pl.DataFrame]]:
    """
    Export competitiveness analysis results.
    
    Args:
        df: DataFrame with voters and classifications
        output_dir: Directory to save CSV files
        party_col: Column name for party classification
        threshold: Threshold percentage for solid classification
        
    Returns:
        Dict with competitiveness analysis results
    """
    print("=" * 80)
    print("EXPORTING COMPETITIVENESS ANALYSIS")
    print("=" * 80)
    print()
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Run competitiveness assessment
    competitiveness_results = assess_all_district_types(
        df, party_col, threshold, output_dir=str(output_path)
    )
    
    return competitiveness_results


def export_all_redistricting_data(
    df: pl.DataFrame,
    output_dir: str,
    party_col: str = 'party_final',
    threshold: float = 57.0
) -> Dict:
    """
    Export all redistricting analysis data to CSV files.
    
    Args:
        df: DataFrame with voters and classifications
        output_dir: Base directory to save all CSV files
        party_col: Column name for party classification
        threshold: Threshold percentage for solid classification
        
    Returns:
        Dict with all analysis results
    """
    print("=" * 80)
    print("EXPORTING ALL REDISTRICTING DATA")
    print("=" * 80)
    print()
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Create subdirectories
    csv_dir = output_path / "csv"
    redistricting_dir = output_path / "analysis" / "redistricting_impact"
    competitiveness_dir = output_path / "analysis" / "competitiveness"
    
    csv_dir.mkdir(parents=True, exist_ok=True)
    redistricting_dir.mkdir(parents=True, exist_ok=True)
    competitiveness_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Export voter classifications
    print("\n1. Exporting voter classifications...")
    voter_csv = csv_dir / "voter_classifications.csv"
    export_voter_classifications(df, str(voter_csv))
    
    # 2. Export redistricting analysis
    print("\n2. Exporting redistricting impact analysis...")
    redistricting_results = export_redistricting_analysis(df, str(redistricting_dir), party_col)
    
    # 3. Export competitiveness analysis
    print("\n3. Exporting competitiveness analysis...")
    competitiveness_results = export_competitiveness_analysis(
        df, str(competitiveness_dir), party_col, threshold
    )
    
    # 4. Create summary exports
    print("\n4. Creating summary exports...")
    
    # Summary by district type
    for district_type in ['CD', 'SD', 'HD']:
        if district_type in redistricting_results:
            # Export shifts summary
            shifts = redistricting_results[district_type].get('shifts')
            if shifts is not None and len(shifts) > 0:
                shifts_path = csv_dir / f"redistricting_shifts_{district_type.lower()}.csv"
                shifts.write_csv(str(shifts_path))
                print(f"  Saved: {shifts_path}")
            
            # Export transition matrix
            transition = redistricting_results[district_type].get('transition_matrix')
            if transition is not None and len(transition) > 0:
                transition_path = csv_dir / f"transition_matrix_{district_type.lower()}.csv"
                transition.write_csv(str(transition_path))
                print(f"  Saved: {transition_path}")
        
        if district_type in competitiveness_results:
            # Export competitiveness summary
            new_comp = competitiveness_results[district_type].get('new_competitiveness')
            if new_comp is not None and len(new_comp) > 0:
                comp_path = csv_dir / f"competitiveness_2026_{district_type.lower()}.csv"
                new_comp.write_csv(str(comp_path))
                print(f"  Saved: {comp_path}")
            
            old_comp = competitiveness_results[district_type].get('old_competitiveness')
            if old_comp is not None and len(old_comp) > 0:
                comp_path = csv_dir / f"competitiveness_2022_{district_type.lower()}.csv"
                old_comp.write_csv(str(comp_path))
                print(f"  Saved: {comp_path}")
    
    print()
    print("=" * 80)
    print("All data exported successfully!")
    print("=" * 80)
    print()
    
    return {
        'redistricting': redistricting_results,
        'competitiveness': competitiveness_results,
    }


if __name__ == "__main__":
    # Test export
    import sys
    from pathlib import Path
    
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "data/exports"
    else:
        from tx_election_results.config import config
        input_path = str(config.MODELED_DATA)
        output_dir = str(config.OUTPUT_DIR)
    
    if not Path(input_path).exists():
        print(f"Error: {input_path} not found.")
        print("Please run the prediction step first.")
        sys.exit(1)
    
    print(f"Loading data from {input_path}...")
    df = pl.read_parquet(input_path)
    print(f"Loaded {len(df):,} voters")
    
    # Ensure party_final exists
    if 'party_final' not in df.columns:
        if 'party' in df.columns:
            df = df.with_columns([
                pl.col('party').alias('party_final')
            ])
        else:
            print("Error: No party classification found. Please run prediction step first.")
            sys.exit(1)
    
    # Export all data
    results = export_all_redistricting_data(df, output_dir, threshold=57.0)
    
    print(f"\n✅ Export complete!")
    print(f"Results saved to: {output_dir}")

