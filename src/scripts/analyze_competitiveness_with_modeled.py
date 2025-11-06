#!/usr/bin/env python3
"""
Analyze competitiveness for each district type with modeled data included.
Shows comparison: Known-only vs Known + Modeled (general-election-only voters).
"""
import polars as pl
import pandas as pd
from pathlib import Path
from tx_election_results.config import config

def analyze_competitiveness_with_modeled(district_type: str, district_col: str, threshold: float = 57.0):
    """
    Analyze competitiveness with and without modeled data for a district type.
    
    Args:
        district_type: 'CD', 'SD', or 'HD'
        district_col: Column name for district (e.g., '2026_CD')
        threshold: Competitiveness threshold (default 57.0)
    """
    print(f"\n{'='*80}")
    print(f"COMPETITIVENESS ANALYSIS WITH MODELED DATA: {district_type}")
    print(f"{'='*80}")
    
    # Load modeled data if available
    if config.MODELED_DATA.exists():
        df = pl.read_parquet(str(config.MODELED_DATA))
        print(f"✓ Loaded modeled data: {len(df):,} voters")
    else:
        # Fallback to merged data
        df = pl.read_parquet(str(config.MERGED_DATA))
        print(f"⚠️  No modeled data found. Using merged data (no ML predictions).")
        print(f"   Loaded merged data: {len(df):,} voters")
    
    # Filter to voters with district assignments
    df = df.filter(pl.col(district_col).is_not_null())
    
    # Identify general-election-only voters (those with GEN history but no primary)
    gen_cols = [col for col in df.columns if col.upper().startswith("GEN")]
    if gen_cols:
        has_gen_history = (
            pl.col(gen_cols[0]).is_not_null() &
            (pl.col(gen_cols[0]) != "")
        )
        for gen_col in gen_cols[1:]:
            has_gen_history = has_gen_history | (
                pl.col(gen_col).is_not_null() &
                (pl.col(gen_col) != "")
            )
        
        df = df.with_columns([
            has_gen_history.alias("has_gen_history"),
            (
                has_gen_history &
                ((pl.col('total_primary_votes') == 0) | pl.col('total_primary_votes').is_null()) &
                (pl.col('party').is_in(['Unknown', 'Other']) | pl.col('party').is_null())
            ).alias("is_general_only")
        ])
    else:
        df = df.with_columns([
            pl.lit(False).alias("has_gen_history"),
            pl.lit(False).alias("is_general_only")
        ])
    
    # Count general-election-only voters
    general_only_count = df.filter(pl.col("is_general_only") == True).select(pl.count()).item()
    print(f"✓ General-election-only voters: {general_only_count:,}")
    
    # Analysis 1: Known primary voters only (current analysis)
    known_voters = df.filter(
        pl.col('party').is_in(['Republican', 'Democrat'])
    )
    
    known_by_district = (
        known_voters
        .group_by([district_col, 'party'])
        .agg(pl.count().alias('count'))
        .pivot(index=district_col, columns='party', values='count')
        .fill_null(0)
    )
    
    # Calculate competitiveness for known-only
    known_competitiveness = known_by_district.with_columns([
        (pl.col('Republican') / (pl.col('Republican') + pl.col('Democrat')) * 100).alias('rep_pct_known'),
        (pl.col('Democrat') / (pl.col('Republican') + pl.col('Democrat')) * 100).alias('dem_pct_known'),
    ]).with_columns([
        pl.when(pl.col('rep_pct_known') >= threshold)
        .then(pl.lit('Solidly Republican'))
        .when(pl.col('dem_pct_known') >= threshold)
        .then(pl.lit('Solidly Democrat'))
        .otherwise(pl.lit('Competitive'))
        .alias('competitiveness_known_only')
    ])
    
    # Analysis 2: Known + Modeled (if available)
    if 'predicted_party_score' in df.columns or 'party_final' in df.columns:
        # Use party_final if available (includes predictions), otherwise use party
        party_col = 'party_final' if 'party_final' in df.columns else 'party'
        
        # Map party_final to R/D for analysis
        all_voters = df.with_columns([
            pl.when(pl.col(party_col).str.contains('Republican', literal=True))
            .then(pl.lit('Republican'))
            .when(pl.col(party_col).str.contains('Democrat', literal=True))
            .then(pl.lit('Democrat'))
            .otherwise(pl.lit('Other'))
            .alias('party_classified')
        ])
        
        all_by_district = (
            all_voters
            .filter(pl.col('party_classified').is_in(['Republican', 'Democrat']))
            .group_by([district_col, 'party_classified'])
            .agg(pl.count().alias('count'))
            .pivot(index=district_col, columns='party_classified', values='count')
            .fill_null(0)
        )
        
        # Calculate competitiveness with modeled data
        all_competitiveness = all_by_district.with_columns([
            (pl.col('Republican') / (pl.col('Republican') + pl.col('Democrat')) * 100).alias('rep_pct_with_modeled'),
            (pl.col('Democrat') / (pl.col('Republican') + pl.col('Democrat')) * 100).alias('dem_pct_with_modeled'),
        ]).with_columns([
            pl.when(pl.col('rep_pct_with_modeled') >= threshold)
            .then(pl.lit('Solidly Republican'))
            .when(pl.col('dem_pct_with_modeled') >= threshold)
            .then(pl.lit('Solidly Democrat'))
            .otherwise(pl.lit('Competitive'))
            .alias('competitiveness_with_modeled')
        ])
        
        # Merge for comparison
        comparison = (
            known_competitiveness
            .join(
                all_competitiveness.select([
                    district_col,
                    'Republican',
                    'Democrat',
                    'rep_pct_with_modeled',
                    'dem_pct_with_modeled',
                    'competitiveness_with_modeled'
                ]),
                on=district_col,
                how='outer'
            )
            .sort(district_col)
        )
        
        # Count changes
        changes = comparison.with_columns([
            (pl.col('competitiveness_known_only') != pl.col('competitiveness_with_modeled')).alias('changed')
        ])
        
        num_changed = changes.filter(pl.col('changed') == True).select(pl.count()).item()
        total_districts = len(comparison)
        
        print(f"\n{'='*80}")
        print(f"COMPETITIVENESS COMPARISON: {district_type}")
        print(f"{'='*80}")
        print(f"\nTotal Districts: {total_districts}")
        print(f"Districts that changed category: {num_changed} ({num_changed/total_districts*100:.1f}%)")
        
        # Count by category
        known_counts = known_competitiveness['competitiveness_known_only'].value_counts().sort('competitiveness_known_only')
        modeled_counts = all_competitiveness['competitiveness_with_modeled'].value_counts().sort('competitiveness_with_modeled')
        
        print(f"\nKnown-Only Analysis:")
        for row in known_counts.iter_rows(named=True):
            print(f"  {row['competitiveness_known_only']}: {row['count']} districts")
        
        print(f"\nWith Modeled Data:")
        for row in modeled_counts.iter_rows(named=True):
            print(f"  {row['competitiveness_with_modeled']}: {row['count']} districts")
        
        # Save results
        output_dir = Path(config.OUTPUT_DIR) / "analysis" / "competitiveness_with_modeled"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        comparison_pd = comparison.to_pandas()
        comparison_pd.to_csv(output_dir / f"{district_type.lower()}_competitiveness_comparison.csv", index=False)
        
        return comparison_pd
    else:
        print("\n⚠️  No modeled predictions available. Cannot compare with modeled data.")
        print("   Run the ML pipeline (steps 12-15) to generate predictions.")
        
        # Still save known-only analysis
        output_dir = Path(config.OUTPUT_DIR) / "analysis" / "competitiveness_with_modeled"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        known_pd = known_competitiveness.to_pandas()
        known_pd.to_csv(output_dir / f"{district_type.lower()}_known_only.csv", index=False)
        
        return known_pd


def main():
    """Run competitiveness analysis for all district types."""
    results = {}
    
    # Congressional Districts
    results['CD'] = analyze_competitiveness_with_modeled('CD', '2026_CD')
    
    # State Senate Districts
    results['SD'] = analyze_competitiveness_with_modeled('SD', '2026_SD')
    
    # House Districts
    results['HD'] = analyze_competitiveness_with_modeled('HD', '2026_HD')
    
    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*80}")
    print(f"\nResults saved to: {config.OUTPUT_DIR / 'analysis' / 'competitiveness_with_modeled'}")


if __name__ == "__main__":
    main()

