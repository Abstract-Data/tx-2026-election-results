#!/usr/bin/env python3
"""
Calculate competitiveness comparison between 2022 and 2026 districts.
"""
import pandas as pd
from pathlib import Path

def calculate_competitiveness(composition_df, threshold=57.0):
    """
    Calculate competitiveness based on percentage of known party voters (R+D only).
    """
    # Calculate percentages based on known party voters (R+D only)
    composition_df = composition_df.copy()
    composition_df['rep_pct_of_known'] = (
        composition_df['republican_voters'] / 
        (composition_df['republican_voters'] + composition_df['democrat_voters']) * 100
    )
    composition_df['dem_pct_of_known'] = (
        composition_df['democrat_voters'] / 
        (composition_df['republican_voters'] + composition_df['democrat_voters']) * 100
    )
    
    # Fill NaN values (where R+D = 0) with 0
    composition_df['rep_pct_of_known'] = composition_df['rep_pct_of_known'].fillna(0)
    composition_df['dem_pct_of_known'] = composition_df['dem_pct_of_known'].fillna(0)
    
    # Classify competitiveness
    composition_df['competitiveness'] = 'Competitive'
    composition_df.loc[composition_df['rep_pct_of_known'] >= threshold, 'competitiveness'] = 'Solidly Republican'
    composition_df.loc[composition_df['dem_pct_of_known'] >= threshold, 'competitiveness'] = 'Solidly Democrat'
    
    return composition_df

def compare_competitiveness():
    """Compare competitiveness between 2022 and 2026 districts."""
    base_dir = Path("data/exports/districts")
    threshold = 57.0
    
    results = {}
    
    for district_type in ['cd', 'sd', 'hd']:
        print(f"\n{'='*80}")
        print(f"Processing {district_type.upper()} districts...")
        print(f"{'='*80}")
        
        # Load old and new composition files
        old_file = base_dir / f"{district_type}_districts" / "party_composition_old_districts.csv"
        new_file = base_dir / f"{district_type}_districts" / "party_composition_new_districts.csv"
        
        if not old_file.exists() or not new_file.exists():
            print(f"⚠️  Missing files for {district_type}")
            continue
        
        old_df = pd.read_csv(old_file)
        new_df = pd.read_csv(new_file)
        
        # Calculate competitiveness
        old_df = calculate_competitiveness(old_df, threshold)
        new_df = calculate_competitiveness(new_df, threshold)
        
        # Count by category for 2022
        old_counts = old_df['competitiveness'].value_counts().to_dict()
        old_solid_r = old_counts.get('Solidly Republican', 0)
        old_solid_d = old_counts.get('Solidly Democrat', 0)
        old_competitive = old_counts.get('Competitive', 0)
        old_total = len(old_df)
        
        # Count by category for 2026
        new_counts = new_df['competitiveness'].value_counts().to_dict()
        new_solid_r = new_counts.get('Solidly Republican', 0)
        new_solid_d = new_counts.get('Solidly Democrat', 0)
        new_competitive = new_counts.get('Competitive', 0)
        new_total = len(new_df)
        
        # Calculate net changes
        net_solid_r = new_solid_r - old_solid_r
        net_solid_d = new_solid_d - old_solid_d
        net_competitive = new_competitive - old_competitive
        
        results[district_type.upper()] = {
            '2022': {
                'Solidly Republican': old_solid_r,
                'Solidly Democrat': old_solid_d,
                'Competitive': old_competitive,
                'Total': old_total
            },
            '2026': {
                'Solidly Republican': new_solid_r,
                'Solidly Democrat': new_solid_d,
                'Competitive': new_competitive,
                'Total': new_total
            },
            'Net Change': {
                'Solidly Republican': net_solid_r,
                'Solidly Democrat': net_solid_d,
                'Competitive': net_competitive
            }
        }
        
        print(f"\n2022 Districts:")
        print(f"  Solidly Republican: {old_solid_r} ({old_solid_r/old_total*100:.1f}%)")
        print(f"  Solidly Democrat: {old_solid_d} ({old_solid_d/old_total*100:.1f}%)")
        print(f"  Competitive: {old_competitive} ({old_competitive/old_total*100:.1f}%)")
        
        print(f"\n2026 Districts:")
        print(f"  Solidly Republican: {new_solid_r} ({new_solid_r/new_total*100:.1f}%)")
        print(f"  Solidly Democrat: {new_solid_d} ({new_solid_d/new_total*100:.1f}%)")
        print(f"  Competitive: {new_competitive} ({new_competitive/new_total*100:.1f}%)")
        
        print(f"\nNet Change:")
        print(f"  Solidly Republican: {net_solid_r:+d}")
        print(f"  Solidly Democrat: {net_solid_d:+d}")
        print(f"  Competitive: {net_competitive:+d}")
    
    return results

if __name__ == "__main__":
    results = compare_competitiveness()
    
    # Save results to CSV
    output_file = Path("data/exports/analysis/competitiveness_2022_vs_2026_comparison.csv")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Create comparison DataFrame
    comparison_data = []
    for district_type, data in results.items():
        comparison_data.append({
            'District_Type': district_type,
            'Year': '2022',
            'Solidly_Republican': data['2022']['Solidly Republican'],
            'Solidly_Democrat': data['2022']['Solidly Democrat'],
            'Competitive': data['2022']['Competitive'],
            'Total': data['2022']['Total']
        })
        comparison_data.append({
            'District_Type': district_type,
            'Year': '2026',
            'Solidly_Republican': data['2026']['Solidly Republican'],
            'Solidly_Democrat': data['2026']['Solidly Democrat'],
            'Competitive': data['2026']['Competitive'],
            'Total': data['2026']['Total']
        })
        comparison_data.append({
            'District_Type': district_type,
            'Year': 'Net_Change',
            'Solidly_Republican': data['Net Change']['Solidly Republican'],
            'Solidly_Democrat': data['Net Change']['Solidly Democrat'],
            'Competitive': data['Net Change']['Competitive'],
            'Total': 0
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    comparison_df.to_csv(output_file, index=False)
    print(f"\n✅ Saved comparison to {output_file}")
    
    print("\n" + "="*80)
    print("COMPETITIVENESS COMPARISON SUMMARY")
    print("="*80)
    print(comparison_df.to_string(index=False))

