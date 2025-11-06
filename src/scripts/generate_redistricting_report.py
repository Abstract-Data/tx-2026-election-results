#!/usr/bin/env python3
"""
Generate comprehensive redistricting analysis report with district-by-district breakdowns.
"""
import polars as pl
import pandas as pd
from pathlib import Path
from tx_election_results.config import config

def calculate_competitiveness_from_composition(composition_df: pl.DataFrame, threshold: float = 57.0) -> pl.DataFrame:
    """
    Calculate competitiveness from party composition data.
    Uses percentage of known party voters (R+D only).
    """
    # Calculate percentages based on known party voters (R+D only)
    composition_df = composition_df.with_columns([
        (pl.col('republican_voters') / (pl.col('republican_voters') + pl.col('democrat_voters')) * 100).alias('rep_pct_of_known'),
        (pl.col('democrat_voters') / (pl.col('republican_voters') + pl.col('democrat_voters')) * 100).alias('dem_pct_of_known'),
    ]).fill_nan(0.0)
    
    # Classify competitiveness
    composition_df = composition_df.with_columns([
        pl.when(pl.col('rep_pct_of_known') >= threshold)
        .then(pl.lit('Solidly Republican'))
        .when(pl.col('dem_pct_of_known') >= threshold)
        .then(pl.lit('Solidly Democrat'))
        .otherwise(pl.lit('Competitive'))
        .alias('competitiveness')
    ])
    
    return composition_df

def generate_report():
    """Generate comprehensive redistricting analysis report."""
    print("=" * 80)
    print("GENERATING REDISTRICTING ANALYSIS REPORT")
    print("=" * 80)
    print()
    
    report_lines = []
    report_lines.append("# Redistricting Analysis Report - By District Type\n")
    report_lines.append("## Executive Summary\n")
    report_lines.append("This report provides a comprehensive analysis of redistricting impacts across three district types: Congressional Districts (CD), State Senate Districts (SD), and House Districts (HD), comparing 2022 boundaries to 2026 boundaries.\n")
    report_lines.append(f"**Total Voters Analyzed:** 18,663,050\n")
    report_lines.append("---\n\n")
    
    # Process each district type
    for district_type, district_name in [('CD', 'Congressional Districts'), ('SD', 'State Senate Districts'), ('HD', 'House Districts')]:
        print(f"\nProcessing {district_type}...")
        
        # Load composition data
        new_comp_path = config.OUTPUT_DIR / "districts" / f"{district_type.lower()}_districts" / "party_composition_new_districts.csv"
        old_comp_path = config.OUTPUT_DIR / "districts" / f"{district_type.lower()}_districts" / "party_composition_old_districts.csv"
        gains_losses_path = config.OUTPUT_DIR / "districts" / f"{district_type.lower()}_districts" / "party_gains_losses_by_district.csv"
        
        if not new_comp_path.exists() or not gains_losses_path.exists():
            print(f"  ⚠️  Missing data files for {district_type}, skipping...")
            continue
        
        new_comp = pl.read_csv(str(new_comp_path))
        gains_losses = pl.read_csv(str(gains_losses_path))
        
        # Calculate competitiveness for new districts
        new_comp = calculate_competitiveness_from_composition(new_comp, threshold=57.0)
        
        # Calculate old competitiveness if available
        if old_comp_path.exists():
            old_comp = pl.read_csv(str(old_comp_path))
            old_comp = calculate_competitiveness_from_composition(old_comp, threshold=57.0)
        else:
            old_comp = None
        
        # Summary statistics
        total_districts = len(new_comp)
        total_voters = new_comp['total_voters'].sum()
        
        # Competitiveness breakdown
        comp_breakdown = new_comp.group_by('competitiveness').agg([
            pl.count().alias('count'),
            (pl.count() / pl.len() * 100).alias('percentage')
        ]).sort('competitiveness')
        
        solid_rep = comp_breakdown.filter(pl.col('competitiveness') == 'Solidly Republican')['count'].sum()
        solid_dem = comp_breakdown.filter(pl.col('competitiveness') == 'Solidly Democrat')['count'].sum()
        competitive = comp_breakdown.filter(pl.col('competitiveness') == 'Competitive')['count'].sum()
        
        # Party composition summary
        total_rep = new_comp['republican_voters'].sum()
        total_dem = new_comp['democrat_voters'].sum()
        total_known = total_rep + total_dem
        rep_pct = (total_rep / total_known * 100) if total_known > 0 else 0
        dem_pct = (total_dem / total_known * 100) if total_known > 0 else 0
        
        # Add section to report
        report_lines.append(f"## {district_type}: {district_name} - {total_districts} Districts\n")
        report_lines.append(f"### Overall Statistics\n")
        report_lines.append(f"- **Total Districts:** {total_districts}\n")
        report_lines.append(f"- **Total Voters:** {total_voters:,}\n")
        report_lines.append(f"- **Total Known Party Voters:** {total_known:,}\n")
        report_lines.append(f"- **Republican Voters:** {total_rep:,} ({rep_pct:.2f}% of known)\n")
        report_lines.append(f"- **Democrat Voters:** {total_dem:,} ({dem_pct:.2f}% of known)\n")
        report_lines.append(f"\n### Competitiveness (2026 Districts)\n")
        report_lines.append(f"- **Solidly Republican (≥57% R):** {solid_rep} ({solid_rep/total_districts*100:.1f}%)\n")
        report_lines.append(f"- **Solidly Democrat (≥57% D):** {solid_dem} ({solid_dem/total_districts*100:.1f}%)\n")
        report_lines.append(f"- **Competitive (<57% for both):** {competitive} ({competitive/total_districts*100:.1f}%)\n")
        
        # Gains/losses summary
        if 'net_republican_change' in gains_losses.columns:
            avg_rep_change = gains_losses['net_republican_change'].mean()
            avg_dem_change = gains_losses['net_democrat_change'].mean()
            total_rep_change = gains_losses['net_republican_change'].sum()
            total_dem_change = gains_losses['net_democrat_change'].sum()
            
            report_lines.append(f"\n### Redistricting Shifts Summary\n")
            report_lines.append(f"- **Average Republican Change per District:** {avg_rep_change:+.1f} voters\n")
            report_lines.append(f"- **Average Democrat Change per District:** {avg_dem_change:+.1f} voters\n")
            report_lines.append(f"- **Total Republican Change:** {total_rep_change:+,.0f} voters\n")
            report_lines.append(f"- **Total Democrat Change:** {total_dem_change:+,.0f} voters\n")
        
        # District-by-district breakdown table
        report_lines.append(f"\n### District-by-District Breakdown (2026 Districts)\n")
        report_lines.append(f"\n| District | Rep Voters | Dem Voters | Rep % | Dem % | Competitiveness | Net Rep Change | Net Dem Change |\n")
        report_lines.append(f"|----------|------------|------------|-------|-------|-----------------|----------------|----------------|\n")
        
        # Merge composition with gains/losses
        breakdown = new_comp.join(
            gains_losses.select([
                'district',
                'net_republican_change',
                'net_democrat_change'
            ]),
            on='district',
            how='left'
        ).sort('district')
        
        for row in breakdown.iter_rows(named=True):
            district = row['district']
            rep_voters = int(row['republican_voters'])
            dem_voters = int(row['democrat_voters'])
            rep_pct_val = row.get('rep_pct_of_known', 0)
            dem_pct_val = row.get('dem_pct_of_known', 0)
            competitiveness = row.get('competitiveness', 'Unknown')
            net_rep_change = row.get('net_republican_change', 0)
            net_dem_change = row.get('net_democrat_change', 0)
            
            report_lines.append(
                f"| {district} | {rep_voters:,} | {dem_voters:,} | {rep_pct_val:.1f}% | {dem_pct_val:.1f}% | {competitiveness} | {net_rep_change:+.0f} | {net_dem_change:+.0f} |\n"
            )
        
        report_lines.append("\n---\n\n")
    
    # Write report
    report_path = config.OUTPUT_DIR / "analysis" / "redistricting_impact" / "REDISTRICTING_ANALYSIS_REPORT.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(report_path, 'w') as f:
        f.writelines(report_lines)
    
    print(f"\n✅ Report generated: {report_path}")
    return report_path

if __name__ == "__main__":
    generate_report()

