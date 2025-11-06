"""
Compare party advantage changes between 2022 (old districts) and 2026 (new districts).

Shows how redistricting affected party advantage in each district type.
"""
import pandas as pd
import numpy as np
from pathlib import Path


def compare_district_advantage(district_type: str, old_file: str, new_file: str, threshold: int = 1000):
    """
    Compare party advantage between old (2022) and new (2026) districts.
    
    Args:
        district_type: Type of district (e.g., "HD", "CD", "SD")
        old_file: Path to party_composition_old_districts.csv
        new_file: Path to party_composition_new_districts.csv
        threshold: Minimum voter difference to be considered "advantaged"
    
    Returns:
        Dictionary with comparison results
    """
    print(f"\n{'='*100}")
    print(f"{district_type} DISTRICTS - 2022 vs 2026 PARTY ADVANTAGE COMPARISON")
    print(f"{'='*100}")
    
    # Load data
    old_df = pd.read_csv(old_file)
    new_df = pd.read_csv(new_file)
    
    # Calculate net advantage for old districts
    old_df['net_advantage'] = old_df['republican_voters'] - old_df['democrat_voters']
    old_df['net_advantage_pct'] = ((old_df['republican_voters'] - old_df['democrat_voters']) / old_df['total_voters'] * 100).fillna(0)
    old_df['advantage_party_2022'] = old_df['net_advantage'].apply(
        lambda x: 'Republican' if x > threshold 
        else ('Democrat' if x < -threshold else 'Competitive/Swing')
    )
    
    # Calculate net advantage for new districts
    new_df['net_advantage'] = new_df['republican_voters'] - new_df['democrat_voters']
    new_df['net_advantage_pct'] = ((new_df['republican_voters'] - new_df['democrat_voters']) / new_df['total_voters'] * 100).fillna(0)
    new_df['advantage_party_2026'] = new_df['net_advantage'].apply(
        lambda x: 'Republican' if x > threshold 
        else ('Democrat' if x < -threshold else 'Competitive/Swing')
    )
    
    # Merge old and new by district
    # Note: District numbers may not align perfectly, so we compare overall composition
    # For a proper comparison, we'd need transition data, but we can compare aggregate stats
    
    # 2022 Summary
    old_total_rep = old_df['republican_voters'].sum()
    old_total_dem = old_df['democrat_voters'].sum()
    old_total_voters = old_df['total_voters'].sum()
    old_net_advantage = old_total_rep - old_total_dem
    
    old_rep_districts = len(old_df[old_df['net_advantage'] > threshold])
    old_dem_districts = len(old_df[old_df['net_advantage'] < -threshold])
    old_comp_districts = len(old_df[(old_df['net_advantage'] >= -threshold) & (old_df['net_advantage'] <= threshold)])
    
    # 2026 Summary
    new_total_rep = new_df['republican_voters'].sum()
    new_total_dem = new_df['democrat_voters'].sum()
    new_total_voters = new_df['total_voters'].sum()
    new_net_advantage = new_total_rep - new_total_dem
    
    new_rep_districts = len(new_df[new_df['net_advantage'] > threshold])
    new_dem_districts = len(new_df[new_df['net_advantage'] < -threshold])
    new_comp_districts = len(new_df[(new_df['net_advantage'] >= -threshold) & (new_df['net_advantage'] <= threshold)])
    
    print(f"\n2022 MAPS (OLD DISTRICTS):")
    print(f"  Total Districts: {len(old_df)}")
    print(f"  Republican Advantage: {old_rep_districts} districts ({old_rep_districts/len(old_df)*100:.1f}%)")
    print(f"  Democrat Advantage: {old_dem_districts} districts ({old_dem_districts/len(old_df)*100:.1f}%)")
    print(f"  Competitive/Swing: {old_comp_districts} districts ({old_comp_districts/len(old_df)*100:.1f}%)")
    print(f"  Total Republican Voters: {old_total_rep:+,} ({old_total_rep/old_total_voters*100:.1f}%)")
    print(f"  Total Democrat Voters: {old_total_dem:+,} ({old_total_dem/old_total_voters*100:.1f}%)")
    print(f"  Net Advantage: {old_net_advantage:+,} voters (Republican)")
    
    print(f"\n2026 MAPS (NEW DISTRICTS):")
    print(f"  Total Districts: {len(new_df)}")
    print(f"  Republican Advantage: {new_rep_districts} districts ({new_rep_districts/len(new_df)*100:.1f}%)")
    print(f"  Democrat Advantage: {new_dem_districts} districts ({new_dem_districts/len(new_df)*100:.1f}%)")
    print(f"  Competitive/Swing: {new_comp_districts} districts ({new_comp_districts/len(new_df)*100:.1f}%)")
    print(f"  Total Republican Voters: {new_total_rep:+,} ({new_total_rep/new_total_voters*100:.1f}%)")
    print(f"  Total Democrat Voters: {new_total_dem:+,} ({new_total_dem/new_total_voters*100:.1f}%)")
    print(f"  Net Advantage: {new_net_advantage:+,} voters (Republican)")
    
    print(f"\nCHANGE SUMMARY:")
    print(f"  District Count Change: {len(new_df) - len(old_df):+d} districts")
    print(f"  Rep Advantage Districts: {new_rep_districts - old_rep_districts:+d} ({new_rep_districts - old_rep_districts:+d} districts)")
    print(f"  Dem Advantage Districts: {new_dem_districts - old_dem_districts:+d} ({new_dem_districts - old_dem_districts:+d} districts)")
    print(f"  Competitive Districts: {new_comp_districts - old_comp_districts:+d} ({new_comp_districts - old_comp_districts:+d} districts)")
    print(f"  Net Advantage Change: {new_net_advantage - old_net_advantage:+,} voters")
    print(f"    (Positive = shift toward Republicans, Negative = shift toward Democrats)")
    
    # Calculate percentage point changes
    rep_pct_change = (new_total_rep/new_total_voters - old_total_rep/old_total_voters) * 100
    dem_pct_change = (new_total_dem/new_total_voters - old_total_dem/old_total_voters) * 100
    
    print(f"\n  Republican Share Change: {rep_pct_change:+.2f} percentage points")
    print(f"  Democrat Share Change: {dem_pct_change:+.2f} percentage points")
    
    # Create transition analysis (if we can match districts)
    # For now, show top districts by advantage in each period
    print(f"\nTop 10 Districts by Republican Advantage - 2022:")
    top_old_rep = old_df.nlargest(10, 'net_advantage')[['district', 'republican_voters', 'democrat_voters', 'net_advantage', 'net_advantage_pct']]
    print(top_old_rep.to_string(index=False))
    
    print(f"\nTop 10 Districts by Republican Advantage - 2026:")
    top_new_rep = new_df.nlargest(10, 'net_advantage')[['district', 'republican_voters', 'democrat_voters', 'net_advantage', 'net_advantage_pct']]
    print(top_new_rep.to_string(index=False))
    
    print(f"\nTop 10 Districts by Democrat Advantage - 2022:")
    top_old_dem = old_df.nsmallest(10, 'net_advantage')[['district', 'republican_voters', 'democrat_voters', 'net_advantage', 'net_advantage_pct']]
    print(top_old_dem.to_string(index=False))
    
    print(f"\nTop 10 Districts by Democrat Advantage - 2026:")
    top_new_dem = new_df.nsmallest(10, 'net_advantage')[['district', 'republican_voters', 'democrat_voters', 'net_advantage', 'net_advantage_pct']]
    print(top_new_dem.to_string(index=False))
    
    return {
        'district_type': district_type,
        'old': {
            'total_districts': len(old_df),
            'rep_districts': old_rep_districts,
            'dem_districts': old_dem_districts,
            'comp_districts': old_comp_districts,
            'total_rep': old_total_rep,
            'total_dem': old_total_dem,
            'net_advantage': old_net_advantage,
            'df': old_df
        },
        'new': {
            'total_districts': len(new_df),
            'rep_districts': new_rep_districts,
            'dem_districts': new_dem_districts,
            'comp_districts': new_comp_districts,
            'total_rep': new_total_rep,
            'total_dem': new_total_dem,
            'net_advantage': new_net_advantage,
            'df': new_df
        },
        'change': {
            'net_advantage_change': new_net_advantage - old_net_advantage,
            'rep_districts_change': new_rep_districts - old_rep_districts,
            'dem_districts_change': new_dem_districts - old_dem_districts,
            'rep_pct_change': rep_pct_change,
            'dem_pct_change': dem_pct_change
        }
    }


def main():
    """Main comparison function."""
    print("="*100)
    print("2022 vs 2026 PARTY ADVANTAGE COMPARISON")
    print("="*100)
    print("\nComparing party advantage in OLD districts (2022/2024 boundaries) vs NEW districts (2026 boundaries)")
    
    results = {}
    
    # Compare each district type
    district_types = [
        ('HD', 'hd_districts/party_composition_old_districts.csv', 'hd_districts/party_composition_new_districts.csv'),
        ('CD', 'cd_districts/party_composition_old_districts.csv', 'cd_districts/party_composition_new_districts.csv'),
        ('SD', 'sd_districts/party_composition_old_districts.csv', 'sd_districts/party_composition_new_districts.csv')
    ]
    
    for dist_type, old_file, new_file in district_types:
        old_path = Path(old_file)
        new_path = Path(new_file)
        if old_path.exists() and new_path.exists():
            try:
                result = compare_district_advantage(dist_type, str(old_path), str(new_path))
                results[dist_type] = result
            except Exception as e:
                print(f"\n❌ Error comparing {dist_type}: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"\n⚠️  Files not found for {dist_type}:")
            if not old_path.exists():
                print(f"   Missing: {old_file}")
            if not new_path.exists():
                print(f"   Missing: {new_file}")
    
    # Create comprehensive summary
    if results:
        print(f"\n{'='*100}")
        print("COMPREHENSIVE COMPARISON SUMMARY - ALL DISTRICT TYPES")
        print(f"{'='*100}")
        
        summary_data = []
        for dist_type in ['HD', 'CD', 'SD']:
            if dist_type in results:
                r = results[dist_type]
                summary_data.append({
                    'District Type': dist_type,
                    '2022 Rep Districts': r['old']['rep_districts'],
                    '2026 Rep Districts': r['new']['rep_districts'],
                    'Rep Districts Change': r['change']['rep_districts_change'],
                    '2022 Dem Districts': r['old']['dem_districts'],
                    '2026 Dem Districts': r['new']['dem_districts'],
                    'Dem Districts Change': r['change']['dem_districts_change'],
                    '2022 Net Advantage': f"{r['old']['net_advantage']:+,}",
                    '2026 Net Advantage': f"{r['new']['net_advantage']:+,}",
                    'Net Advantage Change': f"{r['change']['net_advantage_change']:+,}",
                    'Rep % Point Change': f"{r['change']['rep_pct_change']:+.2f}",
                })
        
        summary_df = pd.DataFrame(summary_data)
        print("\n" + summary_df.to_string(index=False))
        
        # Save summary
        summary_df.to_csv('2022_vs_2026_advantage_comparison_summary.csv', index=False)
        print(f"\n✅ Summary saved to: 2022_vs_2026_advantage_comparison_summary.csv")
        
        # Save detailed comparisons
        output_dir = Path('2022_vs_2026_comparisons')
        output_dir.mkdir(exist_ok=True)
        
        for dist_type in ['HD', 'CD', 'SD']:
            if dist_type in results:
                r = results[dist_type]
                
                # Combine old and new data
                old_df = r['old']['df'].copy()
                new_df = r['new']['df'].copy()
                
                old_df['period'] = '2022'
                new_df['period'] = '2026'
                
                # Select common columns
                common_cols = ['district', 'republican_voters', 'democrat_voters', 'total_voters', 
                              'republican_pct', 'democrat_pct', 'net_advantage', 'net_advantage_pct', 
                              'advantage_party_2022', 'advantage_party_2026']
                
                # Rename columns for clarity
                old_df = old_df.rename(columns={'advantage_party_2022': 'advantage_party'})
                new_df = new_df.rename(columns={'advantage_party_2026': 'advantage_party'})
                
                # Combine
                comparison_df = pd.concat([
                    old_df[['district', 'republican_voters', 'democrat_voters', 'total_voters', 
                           'republican_pct', 'democrat_pct', 'net_advantage', 'net_advantage_pct', 'advantage_party']].assign(period='2022'),
                    new_df[['district', 'republican_voters', 'democrat_voters', 'total_voters', 
                           'republican_pct', 'democrat_pct', 'net_advantage', 'net_advantage_pct', 'advantage_party']].assign(period='2026')
                ], ignore_index=True)
                
                output_file = output_dir / f'{dist_type.lower()}_2022_vs_2026_comparison.csv'
                comparison_df.to_csv(output_file, index=False)
                print(f"✅ {dist_type} comparison saved to: {output_file}")
        
        print(f"\n{'='*100}")
        print("COMPARISON COMPLETE")
        print(f"{'='*100}")


if __name__ == "__main__":
    main()

