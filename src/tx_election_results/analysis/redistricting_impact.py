"""
Analyze redistricting impacts: calculate party composition shifts between 2022 and 2026 districts.
"""
import polars as pl
import pandas as pd
from pathlib import Path
from typing import Literal, Dict, Tuple


def calculate_district_party_composition(
    df: pl.DataFrame,
    district_col: str,
    party_col: str = 'party_final',
    district_type: str = 'CD'
) -> pl.DataFrame:
    """
    Calculate party composition for each district.
    
    Args:
        df: DataFrame with voters and party classifications
        district_col: Column name for district (e.g., 'NEWCD', '2026_CD')
        party_col: Column name for party classification
        district_type: Type of district ('CD', 'SD', 'HD')
        
    Returns:
        DataFrame with party composition by district
    """
    print(f"Calculating party composition for {district_type} districts ({district_col})...")
    
    # Filter to voters with valid district and party
    valid_voters = df.filter(
        pl.col(district_col).is_not_null() &
        (pl.col(district_col) != 0) &
        pl.col(party_col).is_not_null()
    )
    
    # Calculate party composition
    composition = valid_voters.group_by([district_col, party_col]).agg([
        pl.count().alias('voter_count')
    ])
    
    # Pivot to get counts by party
    composition_pivot = composition.pivot(
        values='voter_count',
        index=district_col,
        columns=party_col,
        aggregate_function='sum'
    )
    
    # Calculate totals and percentages
    party_cols = ['Republican', 'Democrat', 'Swing', 'Unknown']
    available_party_cols = [col for col in party_cols if col in composition_pivot.columns]
    
    # Fill missing party columns with 0
    for col in party_cols:
        if col not in composition_pivot.columns:
            composition_pivot = composition_pivot.with_columns([
                pl.lit(0).alias(col)
            ])
    
    # Calculate totals
    composition_pivot = composition_pivot.with_columns([
        (
            pl.col('Republican') + pl.col('Democrat') + pl.col('Swing') + pl.col('Unknown')
        ).alias('total_voters')
    ])
    
    # Calculate percentages
    composition_pivot = composition_pivot.with_columns([
        (pl.col('Republican') / pl.col('total_voters') * 100).alias('rep_pct'),
        (pl.col('Democrat') / pl.col('total_voters') * 100).alias('dem_pct'),
        (pl.col('Swing') / pl.col('total_voters') * 100).alias('swing_pct'),
        (pl.col('Unknown') / pl.col('total_voters') * 100).alias('unknown_pct'),
    ])
    
    # Rename district column
    composition_pivot = composition_pivot.rename({district_col: 'district'})
    
    return composition_pivot


def calculate_redistricting_shifts(
    df: pl.DataFrame,
    old_district_col: str,
    new_district_col: str,
    party_col: str = 'party_final',
    district_type: str = 'CD'
) -> Dict[str, pl.DataFrame]:
    """
    Calculate redistricting shifts between old and new districts.
    
    Args:
        df: DataFrame with voters and both old and new district assignments
        old_district_col: Column name for old district (e.g., 'NEWCD')
        new_district_col: Column name for new district (e.g., '2026_CD')
        party_col: Column name for party classification
        district_type: Type of district ('CD', 'SD', 'HD')
        
    Returns:
        Dict with multiple DataFrames:
        - 'old_composition': Party composition in old districts
        - 'new_composition': Party composition in new districts
        - 'shifts': Gains/losses by district
        - 'transition_matrix': Voter movement between districts
    """
    print("=" * 80)
    print(f"REDISTRICTING IMPACT ANALYSIS: {district_type}")
    print("=" * 80)
    print()
    
    # Calculate party composition for old districts
    print(f"1. Calculating party composition for 2022 districts ({old_district_col})...")
    old_composition = calculate_district_party_composition(
        df, old_district_col, party_col, district_type
    )
    old_composition = old_composition.rename({
        'district': 'old_district',
        'Republican': 'old_rep_count',
        'Democrat': 'old_dem_count',
        'Swing': 'old_swing_count',
        'Unknown': 'old_unknown_count',
        'total_voters': 'old_total_voters',
        'rep_pct': 'old_rep_pct',
        'dem_pct': 'old_dem_pct',
        'swing_pct': 'old_swing_pct',
        'unknown_pct': 'old_unknown_pct',
    })
    
    # Calculate party composition for new districts
    print(f"2. Calculating party composition for 2026 districts ({new_district_col})...")
    new_composition = calculate_district_party_composition(
        df, new_district_col, party_col, district_type
    )
    new_composition = new_composition.rename({
        'district': 'new_district',
        'Republican': 'new_rep_count',
        'Democrat': 'new_dem_count',
        'Swing': 'new_swing_count',
        'Unknown': 'new_unknown_count',
        'total_voters': 'new_total_voters',
        'rep_pct': 'new_rep_pct',
        'dem_pct': 'new_dem_pct',
        'swing_pct': 'new_swing_pct',
        'unknown_pct': 'new_unknown_pct',
    })
    
    # Calculate shifts (gains/losses) for each new district
    print("3. Calculating redistricting shifts...")
    shifts = new_composition.select([
        'new_district',
        'new_rep_count', 'new_dem_count', 'new_swing_count',
        'new_total_voters',
        'new_rep_pct', 'new_dem_pct', 'new_swing_pct',
    ]).with_columns([
        (pl.col('new_rep_count') - pl.col('new_rep_count')).alias('rep_shift'),  # Will be calculated properly
        (pl.col('new_dem_count') - pl.col('new_dem_count')).alias('dem_shift'),
        (pl.col('new_swing_count') - pl.col('new_swing_count')).alias('swing_shift'),
    ])
    
    # Actually calculate shifts by comparing to weighted average from old districts
    # For each new district, find which old districts contributed voters
    print("4. Calculating voter movement between districts...")
    
    # Create transition matrix: old_district -> new_district
    valid_voters = df.filter(
        pl.col(old_district_col).is_not_null() &
        (pl.col(old_district_col) != 0) &
        pl.col(new_district_col).is_not_null() &
        (pl.col(new_district_col) != 0) &
        pl.col(party_col).is_not_null()
    )
    
    transition = valid_voters.group_by([
        old_district_col, new_district_col, party_col
    ]).agg([
        pl.count().alias('voter_count')
    ])
    
    # Pivot transition matrix
    transition_pivot = transition.pivot(
        values='voter_count',
        index=[old_district_col, new_district_col],
        columns=party_col,
        aggregate_function='sum'
    )
    
    # Rename columns
    transition_pivot = transition_pivot.rename({
        old_district_col: 'old_district',
        new_district_col: 'new_district',
    })
    
    # Fill missing party columns
    for col in ['Republican', 'Democrat', 'Swing', 'Unknown']:
        if col not in transition_pivot.columns:
            transition_pivot = transition_pivot.with_columns([
                pl.lit(0).alias(col)
            ])
    
    transition_pivot = transition_pivot.with_columns([
        (
            pl.col('Republican') + pl.col('Democrat') + pl.col('Swing') + pl.col('Unknown')
        ).alias('total_moved')
    ])
    
    # Calculate expected composition for new districts based on old districts
    # For each new district, calculate weighted average from contributing old districts
    print("5. Calculating expected vs actual composition...")
    
    # For each new district, find contributing old districts and their weights
    new_district_contributions = transition_pivot.group_by('new_district').agg([
        pl.sum('Republican').alias('expected_rep'),
        pl.sum('Democrat').alias('expected_dem'),
        pl.sum('Swing').alias('expected_swing'),
        pl.sum('total_moved').alias('expected_total'),
    ])
    
    # Calculate expected percentages
    new_district_contributions = new_district_contributions.with_columns([
        (pl.col('expected_rep') / pl.col('expected_total') * 100).alias('expected_rep_pct'),
        (pl.col('expected_dem') / pl.col('expected_total') * 100).alias('expected_dem_pct'),
        (pl.col('expected_swing') / pl.col('expected_total') * 100).alias('expected_swing_pct'),
    ])
    
    # Join with actual composition to calculate shifts
    shifts = new_composition.join(
        new_district_contributions,
        left_on='new_district',
        right_on='new_district',
        how='left'
    ).with_columns([
        (pl.col('new_rep_pct') - pl.col('expected_rep_pct')).alias('rep_pct_shift'),
        (pl.col('new_dem_pct') - pl.col('expected_dem_pct')).alias('dem_pct_shift'),
        (pl.col('new_swing_pct') - pl.col('expected_swing_pct')).alias('swing_pct_shift'),
        (pl.col('new_rep_count') - pl.col('expected_rep')).alias('rep_count_shift'),
        (pl.col('new_dem_count') - pl.col('expected_dem')).alias('dem_count_shift'),
        (pl.col('new_swing_count') - pl.col('expected_swing')).alias('swing_count_shift'),
    ])
    
    # Sort by district
    shifts = shifts.sort('new_district')
    old_composition = old_composition.sort('old_district')
    new_composition = new_composition.sort('new_district')
    transition_pivot = transition_pivot.sort(['old_district', 'new_district'])
    
    print()
    print("=" * 80)
    print(f"Redistricting analysis complete for {district_type}!")
    print("=" * 80)
    print()
    
    return {
        'old_composition': old_composition,
        'new_composition': new_composition,
        'shifts': shifts,
        'transition_matrix': transition_pivot,
    }


def analyze_all_district_types(
    df: pl.DataFrame,
    party_col: str = 'party_final',
    output_dir: str = None
) -> Dict[str, Dict[str, pl.DataFrame]]:
    """
    Analyze redistricting impacts for all district types (CD, SD, HD).
    
    Args:
        df: DataFrame with voters and district assignments
        party_col: Column name for party classification
        output_dir: Optional directory to save results
        
    Returns:
        Dict with results for each district type
    """
    print("=" * 80)
    print("REDISTRICTING IMPACT ANALYSIS - ALL DISTRICT TYPES")
    print("=" * 80)
    print()
    
    results = {}
    
    # Congressional Districts (CD)
    if 'NEWCD' in df.columns and '2026_CD' in df.columns:
        print("\n" + "=" * 80)
        results['CD'] = calculate_redistricting_shifts(
            df, 'NEWCD', '2026_CD', party_col, 'CD'
        )
    
    # State Senate Districts (SD)
    if 'NEWSD' in df.columns and '2026_SD' in df.columns:
        print("\n" + "=" * 80)
        results['SD'] = calculate_redistricting_shifts(
            df, 'NEWSD', '2026_SD', party_col, 'SD'
        )
    
    # House Districts (HD)
    if 'NEWHD' in df.columns and '2026_HD' in df.columns:
        print("\n" + "=" * 80)
        results['HD'] = calculate_redistricting_shifts(
            df, 'NEWHD', '2026_HD', party_col, 'HD'
        )
    
    # Save results if output directory provided
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for district_type, district_results in results.items():
            # Save each result DataFrame
            for result_name, result_df in district_results.items():
                csv_path = output_path / f"redistricting_{result_name}_{district_type.lower()}.csv"
                result_df.write_csv(str(csv_path))
                print(f"Saved: {csv_path}")
    
    return results


if __name__ == "__main__":
    # Test analysis
    import sys
    from pathlib import Path
    
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "data/exports/analysis/redistricting_impact"
    else:
        from tx_election_results.config import config
        input_path = str(config.MODELED_DATA)
        output_dir = str(config.OUTPUT_DIR / "analysis" / "redistricting_impact")
    
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
    
    # Analyze all district types
    results = analyze_all_district_types(df, output_dir=output_dir)
    
    print(f"\n✅ Redistricting analysis complete!")
    print(f"Results saved to: {output_dir}")

