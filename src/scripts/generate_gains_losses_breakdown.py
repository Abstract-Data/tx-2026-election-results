"""
Generate a comprehensive breakdown of party gains/losses due to redistricting.
"""
import pandas as pd
from pathlib import Path


def generate_gains_losses_breakdown(output_dir: str = "."):
    """
    Generate a comprehensive breakdown of party gains/losses by district type.
    """
    output_path = Path(output_dir)
    
    print("=" * 100)
    print("PARTY GAINS/LOSSES BREAKDOWN - REDISTRICTING ANALYSIS")
    print("=" * 100)
    print("\nThis analysis compares party composition in OLD districts vs NEW districts")
    print("showing how redistricting affected Republican and Democrat voter counts.\n")
    
    # Load data for each district type
    district_types = {
        "State Senate (SD)": {
            "file": output_path / "sd_districts" / "party_gains_losses_by_district.csv",
            "summary": output_path / "sd_gains_losses_summary.csv"
        },
        "Congressional (CD)": {
            "file": output_path / "cd_districts" / "party_gains_losses_by_district.csv",
            "summary": output_path / "cd_gains_losses_summary.csv"
        },
        "House (HD)": {
            "file": output_path / "hd_districts" / "party_gains_losses_by_district.csv",
            "summary": output_path / "hd_gains_losses_summary.csv"
        }
    }
    
    all_results = {}
    
    for district_type, paths in district_types.items():
        if not paths["file"].exists():
            print(f"\n⚠️  Warning: {district_type} file not found: {paths['file']}")
            continue
        
        print("\n" + "=" * 100)
        print(f"{district_type.upper()} DISTRICTS")
        print("=" * 100)
        
        df = pd.read_csv(paths["file"])
        
        # Calculate summary statistics
        total_districts = len(df)
        total_rep_change = df["net_republican_change"].sum()
        total_dem_change = df["net_democrat_change"].sum()
        
        # Districts where each party gained
        rep_gain_districts = len(df[df["net_republican_change"] > 0])
        dem_gain_districts = len(df[df["net_democrat_change"] > 0])
        
        # Districts where net change favors one party
        rep_favored = len(df[df["net_republican_change"] > df["net_democrat_change"]])
        dem_favored = len(df[df["net_democrat_change"] > df["net_republican_change"]])
        
        print(f"\n📊 SUMMARY STATISTICS:")
        print(f"   Total Districts: {total_districts}")
        print(f"   Total Net Republican Change: {total_rep_change:+,.0f} voters")
        print(f"   Total Net Democrat Change: {total_dem_change:+,.0f} voters")
        print(f"   Net Difference (Rep - Dem): {(total_rep_change - total_dem_change):+,.0f} voters")
        
        print(f"\n📈 DISTRICTS WITH GAINS:")
        print(f"   Districts where Republicans gained: {rep_gain_districts} ({rep_gain_districts/total_districts*100:.1f}%)")
        print(f"   Districts where Democrats gained: {dem_gain_districts} ({dem_gain_districts/total_districts*100:.1f}%)")
        
        print(f"\n🎯 DISTRICTS FAVORED BY REDISTRICTING:")
        print(f"   Districts where Rep change > Dem change: {rep_favored} ({rep_favored/total_districts*100:.1f}%)")
        print(f"   Districts where Dem change > Rep change: {dem_favored} ({dem_favored/total_districts*100:.1f}%)")
        
        # Top gaining/losing districts
        print(f"\n🏆 TOP 5 DISTRICTS - REPUBLICAN GAINS:")
        top_rep = df.nlargest(5, "net_republican_change")[
            ["district", "net_republican_change", "net_democrat_change", 
             "new_republican_pct", "new_democrat_pct"]
        ]
        print(top_rep.to_string(index=False))
        
        print(f"\n📉 TOP 5 DISTRICTS - REPUBLICAN LOSSES:")
        top_rep_loss = df.nsmallest(5, "net_republican_change")[
            ["district", "net_republican_change", "net_democrat_change",
             "new_republican_pct", "new_democrat_pct"]
        ]
        print(top_rep_loss.to_string(index=False))
        
        print(f"\n🏆 TOP 5 DISTRICTS - DEMOCRAT GAINS:")
        top_dem = df.nlargest(5, "net_democrat_change")[
            ["district", "net_republican_change", "net_democrat_change",
             "new_republican_pct", "new_democrat_pct"]
        ]
        print(top_dem.to_string(index=False))
        
        print(f"\n📉 TOP 5 DISTRICTS - DEMOCRAT LOSSES:")
        top_dem_loss = df.nsmallest(5, "net_democrat_change")[
            ["district", "net_republican_change", "net_democrat_change",
             "new_republican_pct", "new_democrat_pct"]
        ]
        print(top_dem_loss.to_string(index=False))
        
        # Districts where parties swapped advantages
        print(f"\n🔄 DISTRICTS WITH SIGNIFICANT SHIFTS:")
        # Districts where Rep gained more than Dem by a significant margin
        significant_rep_favored = df[
            (df["net_republican_change"] > df["net_democrat_change"]) &
            (df["net_republican_change"] > 1000)  # Significant threshold
        ]
        print(f"   Districts with Rep advantage > 1,000 voters: {len(significant_rep_favored)}")
        if len(significant_rep_favored) > 0:
            print(f"   Total Rep advantage in these districts: {significant_rep_favored['net_republican_change'].sum():,.0f}")
            print(f"   Total Dem loss in these districts: {significant_rep_favored['net_democrat_change'].sum():,.0f}")
        
        significant_dem_favored = df[
            (df["net_democrat_change"] > df["net_republican_change"]) &
            (df["net_democrat_change"] > 1000)  # Significant threshold
        ]
        print(f"   Districts with Dem advantage > 1,000 voters: {len(significant_dem_favored)}")
        if len(significant_dem_favored) > 0:
            print(f"   Total Dem advantage in these districts: {significant_dem_favored['net_democrat_change'].sum():,.0f}")
            print(f"   Total Rep loss in these districts: {significant_dem_favored['net_republican_change'].sum():,.0f}")
        
        all_results[district_type] = {
            "total_districts": total_districts,
            "total_rep_change": total_rep_change,
            "total_dem_change": total_dem_change,
            "rep_favored": rep_favored,
            "dem_favored": dem_favored,
            "dataframe": df
        }
    
    # Create comprehensive summary table
    print("\n" + "=" * 100)
    print("COMPREHENSIVE SUMMARY - ALL DISTRICT TYPES")
    print("=" * 100)
    
    summary_data = []
    for district_type, results in all_results.items():
        summary_data.append({
            "District Type": district_type,
            "Total Districts": results["total_districts"],
            "Total Rep Change": f"{results['total_rep_change']:+,.0f}",
            "Total Dem Change": f"{results['total_dem_change']:+,.0f}",
            "Net Advantage (Rep-Dem)": f"{(results['total_rep_change'] - results['total_dem_change']):+,.0f}",
            "Rep Favored Districts": f"{results['rep_favored']} ({results['rep_favored']/results['total_districts']*100:.1f}%)",
            "Dem Favored Districts": f"{results['dem_favored']} ({results['dem_favored']/results['total_districts']*100:.1f}%)"
        })
    
    summary_df = pd.DataFrame(summary_data)
    print("\n" + summary_df.to_string(index=False))
    
    # Save comprehensive breakdown
    breakdown_file = output_path / "comprehensive_gains_losses_breakdown.csv"
    summary_df.to_csv(breakdown_file, index=False)
    print(f"\n✅ Comprehensive breakdown saved to: {breakdown_file}")
    
    # Create detailed breakdown by district
    print("\n" + "=" * 100)
    print("DETAILED BREAKDOWN BY DISTRICT")
    print("=" * 100)
    
    detailed_breakdown = []
    for district_type, results in all_results.items():
        df = results["dataframe"]
        for _, row in df.iterrows():
            detailed_breakdown.append({
                "District Type": district_type,
                "District": row["district"],
                "Rep Change": row["net_republican_change"],
                "Dem Change": row["net_democrat_change"],
                "Net Advantage": row["net_republican_change"] - row["net_democrat_change"],
                "New Rep %": f"{row['new_republican_pct']:.1f}%",
                "New Dem %": f"{row['new_democrat_pct']:.1f}%",
                "Favors": "Republican" if row["net_republican_change"] > row["net_democrat_change"] else "Democrat"
            })
    
    detailed_df = pd.DataFrame(detailed_breakdown)
    detailed_file = output_path / "detailed_gains_losses_by_district.csv"
    detailed_df.to_csv(detailed_file, index=False)
    print(f"✅ Detailed breakdown saved to: {detailed_file}")
    
    print("\n" + "=" * 100)
    print("ANALYSIS COMPLETE")
    print("=" * 100)
    
    return summary_df, detailed_df


if __name__ == "__main__":
    summary, detailed = generate_gains_losses_breakdown()

