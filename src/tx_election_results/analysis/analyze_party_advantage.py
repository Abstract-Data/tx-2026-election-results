"""
Analyze party advantage breakdown by district type (HD, CD, SD).

Shows which districts favor Republicans vs Democrats based on voter composition.
"""
import pandas as pd
import numpy as np
from pathlib import Path


def analyze_party_advantage(district_type: str, file_path: str, threshold: int = 1000):
    """
    Analyze party advantage for a district type.
    
    Args:
        district_type: Type of district (e.g., "HD", "CD", "SD")
        file_path: Path to party_gains_losses_by_district.csv
        threshold: Minimum voter difference to be considered "advantaged"
    
    Returns:
        Dictionary with analysis results
    """
    print(f"\n{'='*100}")
    print(f"{district_type} DISTRICTS - PARTY ADVANTAGE ANALYSIS")
    print(f"{'='*100}")
    
    # Load data
    df = pd.read_csv(file_path)
    
    # Calculate total voters if not present
    if 'total_voters' not in df.columns:
        df['total_voters'] = df['new_republican_voters'] + df['new_democrat_voters'] + df.get('new_other_voters', 0)
    
    # Calculate net advantage (Republican voters - Democrat voters)
    df['net_advantage'] = df['new_republican_voters'] - df['new_democrat_voters']
    df['net_advantage_pct'] = ((df['new_republican_voters'] - df['new_democrat_voters']) / df['total_voters'] * 100).fillna(0)
    
    # Classify districts
    df['advantage_party'] = df['net_advantage'].apply(
        lambda x: 'Republican' if x > threshold 
        else ('Democrat' if x < -threshold else 'Competitive/Swing')
    )
    
    # Count districts by advantage
    advantage_counts = df['advantage_party'].value_counts()
    total_districts = len(df)
    
    rep_districts = len(df[df['net_advantage'] > threshold])
    dem_districts = len(df[df['net_advantage'] < -threshold])
    competitive_districts = len(df[(df['net_advantage'] >= -threshold) & (df['net_advantage'] <= threshold)])
    
    print(f"\nTotal Districts: {total_districts}")
    print(f"\nParty Advantage Breakdown:")
    print(f"  Republican Advantage: {rep_districts} districts ({rep_districts/total_districts*100:.1f}%)")
    print(f"  Democrat Advantage: {dem_districts} districts ({dem_districts/total_districts*100:.1f}%)")
    print(f"  Competitive/Swing: {competitive_districts} districts ({competitive_districts/total_districts*100:.1f}%)")
    
    # Calculate totals
    total_rep_voters = df['new_republican_voters'].sum()
    total_dem_voters = df['new_democrat_voters'].sum()
    total_voters = df['total_voters'].sum()
    
    print(f"\nTotal Voter Composition:")
    print(f"  Republican Voters: {total_rep_voters:+,} ({total_rep_voters/total_voters*100:.1f}%)")
    print(f"  Democrat Voters: {total_dem_voters:+,} ({total_dem_voters/total_voters*100:.1f}%)")
    print(f"  Other/Unknown: {total_voters - total_rep_voters - total_dem_voters:+,} ({(total_voters - total_rep_voters - total_dem_voters)/total_voters*100:.1f}%)")
    print(f"  Net Advantage: {total_rep_voters - total_dem_voters:+,} voters (Republican)")
    
    # Top districts by advantage
    print(f"\nTop 10 Districts by Republican Advantage:")
    top_rep = df.nlargest(10, 'net_advantage')[['district', 'new_republican_voters', 'new_democrat_voters', 'net_advantage', 'net_advantage_pct']]
    print(top_rep.to_string(index=False))
    
    print(f"\nTop 10 Districts by Democrat Advantage:")
    top_dem = df.nsmallest(10, 'net_advantage')[['district', 'new_republican_voters', 'new_democrat_voters', 'net_advantage', 'net_advantage_pct']]
    print(top_dem.to_string(index=False))
    
    # If modeled data columns exist, show breakdown
    if 'new_rep_known' in df.columns:
        print(f"\nVoter Type Breakdown:")
        total_rep_known = df['new_rep_known'].sum()
        total_rep_modeled = df['new_rep_modeled'].sum()
        total_dem_known = df['new_dem_known'].sum()
        total_dem_modeled = df['new_dem_modeled'].sum()
        
        print(f"  Republican - Known Primary: {total_rep_known:+,}")
        print(f"  Republican - Modeled: {total_rep_modeled:+,}")
        print(f"  Democrat - Known Primary: {total_dem_known:+,}")
        print(f"  Democrat - Modeled: {total_dem_modeled:+,}")
    
    return {
        'district_type': district_type,
        'total_districts': total_districts,
        'republican_advantage': rep_districts,
        'democrat_advantage': dem_districts,
        'competitive': competitive_districts,
        'total_rep_voters': total_rep_voters,
        'total_dem_voters': total_dem_voters,
        'net_advantage': total_rep_voters - total_dem_voters,
        'df': df
    }


def main():
    """Main analysis function."""
    print("="*100)
    print("PARTY ADVANTAGE BREAKDOWN BY DISTRICT TYPE")
    print("="*100)
    print("\nAnalyzing party composition and advantage for HD, CD, and SD districts")
    print("Based on voter composition (known + modeled voters)")
    
    results = {}
    
    # Analyze each district type
    district_types = [
        ('HD', 'hd_districts/party_gains_losses_by_district.csv'),
        ('CD', 'cd_districts/party_gains_losses_by_district.csv'),
        ('SD', 'sd_districts/party_gains_losses_by_district.csv')
    ]
    
    for dist_type, file_path in district_types:
        path = Path(file_path)
        if path.exists():
            try:
                result = analyze_party_advantage(dist_type, str(path))
                results[dist_type] = result
            except Exception as e:
                print(f"\n❌ Error analyzing {dist_type}: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"\n⚠️  File not found: {file_path}")
            print(f"   Run main.py to generate district analysis files")
    
    # Create comprehensive summary
    if results:
        print(f"\n{'='*100}")
        print("COMPREHENSIVE SUMMARY - ALL DISTRICT TYPES")
        print(f"{'='*100}")
        
        summary_data = []
        for dist_type in ['HD', 'CD', 'SD']:
            if dist_type in results:
                r = results[dist_type]
                summary_data.append({
                    'District Type': dist_type,
                    'Total Districts': r['total_districts'],
                    'Rep Advantage Districts': f"{r['republican_advantage']} ({r['republican_advantage']/r['total_districts']*100:.1f}%)",
                    'Dem Advantage Districts': f"{r['democrat_advantage']} ({r['democrat_advantage']/r['total_districts']*100:.1f}%)",
                    'Competitive Districts': f"{r['competitive']} ({r['competitive']/r['total_districts']*100:.1f}%)",
                    'Total Rep Voters': f"{r['total_rep_voters']:+,}",
                    'Total Dem Voters': f"{r['total_dem_voters']:+,}",
                    'Net Advantage (Rep-Dem)': f"{r['net_advantage']:+,}",
                })
        
        summary_df = pd.DataFrame(summary_data)
        print("\n" + summary_df.to_string(index=False))
        
        # Save summary
        summary_df.to_csv('party_advantage_summary.csv', index=False)
        print(f"\n✅ Summary saved to: party_advantage_summary.csv")
        
        # Save detailed breakdowns
        output_dir = Path('party_advantage_breakdowns')
        output_dir.mkdir(exist_ok=True)
        
        for dist_type in ['HD', 'CD', 'SD']:
            if dist_type in results:
                df = results[dist_type]['df']
                # Select relevant columns
                output_cols = ['district', 'new_republican_voters', 'new_democrat_voters', 
                              'net_advantage', 'net_advantage_pct', 'advantage_party']
                if 'new_rep_known' in df.columns:
                    output_cols.extend(['new_rep_known', 'new_rep_modeled', 'new_dem_known', 'new_dem_modeled'])
                
                output_df = df[output_cols].sort_values('net_advantage', ascending=False)
                output_file = output_dir / f'{dist_type.lower()}_party_advantage_breakdown.csv'
                output_df.to_csv(output_file, index=False)
                print(f"✅ {dist_type} breakdown saved to: {output_file}")
        
        print(f"\n{'='*100}")
        print("ANALYSIS COMPLETE")
        print(f"{'='*100}")


if __name__ == "__main__":
    main()

