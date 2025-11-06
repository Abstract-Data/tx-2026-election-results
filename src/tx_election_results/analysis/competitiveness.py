"""
Assess district competitiveness based on party composition.
Classifies districts as solidly Republican (≥57%), solidly Democrat (≥57%), or competitive.
"""
import polars as pl
from pathlib import Path
from typing import Literal, Dict


def classify_competitiveness(
    df: pl.DataFrame,
    district_col: str,
    rep_pct_col: str = 'rep_pct',
    dem_pct_col: str = 'dem_pct',
    threshold: float = 57.0
) -> pl.DataFrame:
    """
    Classify districts by competitiveness.
    
    Classification:
    - Solidly Republican: ≥57% Republican
    - Solidly Democrat: ≥57% Democrat
    - Competitive: <57% for both parties
    
    Args:
        df: DataFrame with district party composition
        district_col: Column name for district
        rep_pct_col: Column name for Republican percentage
        dem_pct_col: Column name for Democrat percentage
        threshold: Threshold percentage for solid classification (default 57%)
        
    Returns:
        DataFrame with competitiveness classification added
    """
    print(f"Classifying competitiveness for districts ({district_col})...")
    print(f"  Threshold: ≥{threshold}% for solid classification")
    
    df = df.with_columns([
        pl.when(pl.col(rep_pct_col) >= threshold)
        .then(pl.lit('Solidly Republican'))
        .when(pl.col(dem_pct_col) >= threshold)
        .then(pl.lit('Solidly Democrat'))
        .otherwise(pl.lit('Competitive'))
        .alias('competitiveness')
    ])
    
    return df


def assess_competitiveness_2022_2026(
    df: pl.DataFrame,
    old_district_col: str,
    new_district_col: str,
    party_col: str = 'party_final',
    district_type: str = 'CD',
    threshold: float = 57.0
) -> Dict[str, pl.DataFrame]:
    """
    Assess competitiveness for both 2022 and 2026 districts.
    
    Args:
        df: DataFrame with voters and district assignments
        old_district_col: Column name for old district (2022)
        new_district_col: Column name for new district (2026)
        party_col: Column name for party classification
        district_type: Type of district ('CD', 'SD', 'HD')
        threshold: Threshold percentage for solid classification
        
    Returns:
        Dict with competitiveness DataFrames for old and new districts
    """
    print("=" * 80)
    print(f"COMPETITIVENESS ASSESSMENT: {district_type}")
    print("=" * 80)
    print()
    
    # Calculate party composition for old districts
    print(f"1. Calculating competitiveness for 2022 districts ({old_district_col})...")
    old_valid = df.filter(
        pl.col(old_district_col).is_not_null() &
        (pl.col(old_district_col) != 0) &
        pl.col(party_col).is_not_null()
    )
    
    old_composition = old_valid.group_by([old_district_col, party_col]).agg([
        pl.count().alias('voter_count')
    ]).pivot(
        values='voter_count',
        index=old_district_col,
        columns=party_col,
        aggregate_function='sum'
    )
    
    # Fill missing party columns
    for col in ['Republican', 'Democrat', 'Swing', 'Unknown']:
        if col not in old_composition.columns:
            old_composition = old_composition.with_columns([
                pl.lit(0).alias(col)
            ])
    
    old_composition = old_composition.with_columns([
        (
            pl.col('Republican') + pl.col('Democrat') + pl.col('Swing') + pl.col('Unknown')
        ).alias('total_voters'),
        # Calculate percentages based on known party voters (R+D only, excluding Swing and Unknown)
        # This gives the percentage of known party voters that are Republican/Democrat
        (pl.col('Republican') / (
            pl.col('Republican') + pl.col('Democrat')
        ) * 100).alias('rep_pct'),
        (pl.col('Democrat') / (
            pl.col('Republican') + pl.col('Democrat')
        ) * 100).alias('dem_pct'),
    ]).fill_nan(0.0)
    
    old_composition = old_composition.rename({old_district_col: 'district'})
    old_competitiveness = classify_competitiveness(
        old_composition,
        'district',
        'rep_pct',
        'dem_pct',
        threshold
    )
    old_competitiveness = old_competitiveness.rename({
        'district': 'old_district',
        'rep_pct': 'old_rep_pct',
        'dem_pct': 'old_dem_pct',
        'competitiveness': 'old_competitiveness',
    })
    
    # Calculate party composition for new districts
    print(f"2. Calculating competitiveness for 2026 districts ({new_district_col})...")
    new_valid = df.filter(
        pl.col(new_district_col).is_not_null() &
        (pl.col(new_district_col) != 0) &
        pl.col(party_col).is_not_null()
    )
    
    new_composition = new_valid.group_by([new_district_col, party_col]).agg([
        pl.count().alias('voter_count')
    ]).pivot(
        values='voter_count',
        index=new_district_col,
        columns=party_col,
        aggregate_function='sum'
    )
    
    # Fill missing party columns
    for col in ['Republican', 'Democrat', 'Swing', 'Unknown']:
        if col not in new_composition.columns:
            new_composition = new_composition.with_columns([
                pl.lit(0).alias(col)
            ])
    
    new_composition = new_composition.with_columns([
        (
            pl.col('Republican') + pl.col('Democrat') + pl.col('Swing') + pl.col('Unknown')
        ).alias('total_voters'),
        # Calculate percentages based on known party voters (R+D only, excluding Swing and Unknown)
        # This gives the percentage of known party voters that are Republican/Democrat
        (pl.col('Republican') / (
            pl.col('Republican') + pl.col('Democrat')
        ) * 100).alias('rep_pct'),
        (pl.col('Democrat') / (
            pl.col('Republican') + pl.col('Democrat')
        ) * 100).alias('dem_pct'),
    ]).fill_nan(0.0)
    
    new_composition = new_composition.rename({new_district_col: 'district'})
    new_competitiveness = classify_competitiveness(
        new_composition,
        'district',
        'rep_pct',
        'dem_pct',
        threshold
    )
    new_competitiveness = new_competitiveness.rename({
        'district': 'new_district',
        'rep_pct': 'new_rep_pct',
        'dem_pct': 'new_dem_pct',
        'competitiveness': 'new_competitiveness',
    })
    
    # Compare competitiveness changes
    print("3. Comparing competitiveness changes...")
    
    # Create comparison by joining old and new
    # Note: This is a simplified comparison - actual comparison would need to track
    # which old districts contributed to which new districts
    old_summary = old_competitiveness.group_by('old_competitiveness').agg([
        pl.count().alias('old_count')
    ])
    
    new_summary = new_competitiveness.group_by('new_competitiveness').agg([
        pl.count().alias('new_count')
    ])
    
    comparison = old_summary.join(
        new_summary,
        left_on='old_competitiveness',
        right_on='new_competitiveness',
        how='outer'
    ).with_columns([
        (pl.col('new_count') - pl.col('old_count')).alias('change')
    ])
    
    print("\nCompetitiveness Summary:")
    print("-" * 80)
    print("2022 Districts:")
    print(old_summary)
    print("\n2026 Districts:")
    print(new_summary)
    print("\nChanges:")
    print(comparison)
    
    print()
    print("=" * 80)
    print(f"Competitiveness assessment complete for {district_type}!")
    print("=" * 80)
    print()
    
    return {
        'old_competitiveness': old_competitiveness,
        'new_competitiveness': new_competitiveness,
        'comparison': comparison,
    }


def assess_all_district_types(
    df: pl.DataFrame,
    party_col: str = 'party_final',
    threshold: float = 57.0,
    output_dir: str = None
) -> Dict[str, Dict[str, pl.DataFrame]]:
    """
    Assess competitiveness for all district types (CD, SD, HD).
    
    Args:
        df: DataFrame with voters and district assignments
        party_col: Column name for party classification
        threshold: Threshold percentage for solid classification
        output_dir: Optional directory to save results
        
    Returns:
        Dict with results for each district type
    """
    print("=" * 80)
    print("COMPETITIVENESS ASSESSMENT - ALL DISTRICT TYPES")
    print("=" * 80)
    print()
    
    results = {}
    
    # Congressional Districts (CD)
    if 'NEWCD' in df.columns and '2026_CD' in df.columns:
        print("\n" + "=" * 80)
        results['CD'] = assess_competitiveness_2022_2026(
            df, 'NEWCD', '2026_CD', party_col, 'CD', threshold
        )
    
    # State Senate Districts (SD)
    if 'NEWSD' in df.columns and '2026_SD' in df.columns:
        print("\n" + "=" * 80)
        results['SD'] = assess_competitiveness_2022_2026(
            df, 'NEWSD', '2026_SD', party_col, 'SD', threshold
        )
    
    # House Districts (HD)
    if 'NEWHD' in df.columns and '2026_HD' in df.columns:
        print("\n" + "=" * 80)
        results['HD'] = assess_competitiveness_2022_2026(
            df, 'NEWHD', '2026_HD', party_col, 'HD', threshold
        )
    
    # Save results if output directory provided
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        for district_type, district_results in results.items():
            # Save each result DataFrame
            for result_name, result_df in district_results.items():
                csv_path = output_path / f"competitiveness_{result_name}_{district_type.lower()}.csv"
                result_df.write_csv(str(csv_path))
                print(f"Saved: {csv_path}")
    
    return results


if __name__ == "__main__":
    # Test assessment
    import sys
    from pathlib import Path
    
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "data/exports/analysis/competitiveness"
    else:
        from tx_election_results.config import config
        input_path = str(config.MODELED_DATA)
        output_dir = str(config.OUTPUT_DIR / "analysis" / "competitiveness")
    
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
    
    # Assess all district types
    results = assess_all_district_types(df, threshold=57.0, output_dir=output_dir)
    
    print(f"\n✅ Competitiveness assessment complete!")
    print(f"Results saved to: {output_dir}")

