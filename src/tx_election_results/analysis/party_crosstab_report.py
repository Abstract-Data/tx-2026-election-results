"""
Generate crosstab reports showing party gains/losses by County, CD, SD, HD.
"""
import polars as pl
import pandas as pd
from pathlib import Path


def generate_party_crosstab_report(
    merged_voter_df: pl.DataFrame,
    output_dir: str = "data/exports"
) -> dict:
    """
    Generate crosstab reports showing party gains/losses broken down by:
    - County
    - Congressional District (CD)
    - State Senate District (SD)
    - State House District (HD)
    
    Shows Democrat/Republican losses for each combination.
    
    Args:
        merged_voter_df: Voter dataframe with district assignments
        output_dir: Directory to save reports
    
    Returns:
        Dictionary with multiple crosstab DataFrames
    """
    print("\n" + "=" * 80)
    print("GENERATING PARTY CROSSTAB REPORTS")
    print("=" * 80)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    (output_path / "csv").mkdir(exist_ok=True, parents=True)
    
    # Filter to voters with party information and district assignments
    voters_with_party = merged_voter_df.filter(
        pl.col("party").is_not_null() &
        pl.col("COUNTY").is_not_null()
    )
    
    print(f"\nAnalyzing {len(voters_with_party):,} voters with party and county information...")
    
    # Convert to pandas for easier crosstab operations
    df_pd = voters_with_party.to_pandas()
    
    # Ensure we have the right columns
    # OLD districts: NEWCD (Congressional), NEWSD (Senate), NEWHD (House)
    # NEW districts: 2026_CD (Congressional), 2026_SD (Senate), 2026_HD (House) for 2026
    
    # 1. By County - Party Composition
    print("\n1. Generating crosstab by County...")
    county_party = pd.crosstab(
        df_pd["COUNTY"],
        df_pd["party"],
        margins=True
    )
    county_party.to_csv(output_path / "csv" / "party_by_county.csv")
    
    # 2. By County and OLD Congressional District
    print("2. Generating crosstab by County and OLD Congressional District...")
    county_cd_old = pd.crosstab(
        [df_pd["COUNTY"], df_pd["NEWCD"]],
        df_pd["party"],
        margins=True
    )
    county_cd_old.to_csv(output_path / "csv" / "party_by_county_cd_old.csv")
    
    # 3. By County and OLD State Senate District
    print("3. Generating crosstab by County and OLD State Senate District...")
    county_sd_old = pd.crosstab(
        [df_pd["COUNTY"], df_pd["NEWSD"]],
        df_pd["party"],
        margins=True
    )
    county_sd_old.to_csv(output_path / "csv" / "party_by_county_sd_old.csv")
    
    # 4. By County and OLD State House District
    print("4. Generating crosstab by County and OLD State House District...")
    county_hd_old = pd.crosstab(
        [df_pd["COUNTY"], df_pd["NEWHD"]],
        df_pd["party"],
        margins=True
    )
    county_hd_old.to_csv(output_path / "csv" / "party_by_county_hd_old.csv")
    
    # 5. By County and NEW State Senate District (2026)
    print("5. Generating crosstab by County and NEW State Senate District (2026)...")
    county_sd_new = pd.crosstab(
        [df_pd["COUNTY"], df_pd["2026_SD"]],
        df_pd["party"],
        margins=True
    )
    county_sd_new.to_csv(output_path / "csv" / "party_by_county_sd_new.csv")
    
    # 5b. By County and NEW Congressional District (2026)
    print("5b. Generating crosstab by County and NEW Congressional District (2026)...")
    county_cd_new = pd.crosstab(
        [df_pd["COUNTY"], df_pd["2026_CD"]],
        df_pd["party"],
        margins=True
    )
    county_cd_new.to_csv(output_path / "csv" / "party_by_county_cd_new.csv")
    
    # 5c. By County and NEW House District (2026)
    print("5c. Generating crosstab by County and NEW House District (2026)...")
    county_hd_new = pd.crosstab(
        [df_pd["COUNTY"], df_pd["2026_HD"]],
        df_pd["party"],
        margins=True
    )
    county_hd_new.to_csv(output_path / "csv" / "party_by_county_hd_new.csv")
    
    # 6. Calculate gains/losses by County
    print("\n6. Calculating party gains/losses by County...")
    county_gains_losses = calculate_county_gains_losses(df_pd)
    county_gains_losses.to_csv(output_path / "csv" / "party_gains_losses_by_county.csv", index=False)
    
    # 7. Calculate gains/losses by County and OLD CD
    print("7. Calculating party gains/losses by County and OLD CD...")
    county_cd_gains_losses = calculate_gains_losses_by_geography(
        df_pd, groupby_cols=["COUNTY", "NEWCD"], label="County_CD_Old"
    )
    county_cd_gains_losses.to_csv(output_path / "csv" / "party_gains_losses_by_county_cd_old.csv", index=False)
    
    # 8. Calculate gains/losses by County and OLD SD
    print("8. Calculating party gains/losses by County and OLD SD...")
    county_sd_old_gains_losses = calculate_gains_losses_by_geography(
        df_pd, groupby_cols=["COUNTY", "NEWSD"], label="County_SD_Old"
    )
    county_sd_old_gains_losses.to_csv(output_path / "csv" / "party_gains_losses_by_county_sd_old.csv", index=False)
    
    # 9. Calculate gains/losses by County and OLD HD
    print("9. Calculating party gains/losses by County and OLD HD...")
    county_hd_old_gains_losses = calculate_gains_losses_by_geography(
        df_pd, groupby_cols=["COUNTY", "NEWHD"], label="County_HD_Old"
    )
    county_hd_old_gains_losses.to_csv(output_path / "csv" / "party_gains_losses_by_county_hd_old.csv", index=False)
    
    # 10. Calculate gains/losses by County and NEW SD (2026)
    print("10. Calculating party gains/losses by County and NEW SD (2026)...")
    county_sd_new_gains_losses = calculate_gains_losses_by_geography(
        df_pd, groupby_cols=["COUNTY", "2026_SD"], label="County_SD_New", use_new_districts=True
    )
    county_sd_new_gains_losses.to_csv(output_path / "csv" / "party_gains_losses_by_county_sd_new.csv", index=False)
    
    # 10b. Calculate gains/losses by County and NEW CD (2026)
    print("10b. Calculating party gains/losses by County and NEW CD (2026)...")
    county_cd_new_gains_losses = calculate_gains_losses_by_geography(
        df_pd, groupby_cols=["COUNTY", "2026_CD"], label="County_CD_New", use_new_districts=True
    )
    county_cd_new_gains_losses.to_csv(output_path / "csv" / "party_gains_losses_by_county_cd_new.csv", index=False)
    
    # 10c. Calculate gains/losses by County and NEW HD (2026)
    print("10c. Calculating party gains/losses by County and NEW HD (2026)...")
    county_hd_new_gains_losses = calculate_gains_losses_by_geography(
        df_pd, groupby_cols=["COUNTY", "2026_HD"], label="County_HD_New", use_new_districts=True
    )
    county_hd_new_gains_losses.to_csv(output_path / "csv" / "party_gains_losses_by_county_hd_new.csv", index=False)
    
    # 11. Create comprehensive crosstab showing OLD vs NEW districts
    print("\n11. Creating comprehensive OLD vs NEW district comparison...")
    old_vs_new_comparison = create_old_vs_new_comparison(df_pd)
    old_vs_new_comparison.to_csv(output_path / "csv" / "party_old_vs_new_districts_comparison.csv", index=False)
    
    print("\n" + "-" * 80)
    print("CROSSTAB REPORTS SUMMARY")
    print("-" * 80)
    print(f"\nAll reports saved to: {output_path}")
    print("\nGenerated files:")
    print("  - party_by_county.csv")
    print("  - party_by_county_cd_old.csv")
    print("  - party_by_county_sd_old.csv")
    print("  - party_by_county_hd_old.csv")
    print("  - party_by_county_sd_new.csv")
    print("  - party_gains_losses_by_county.csv")
    print("  - party_gains_losses_by_county_cd_old.csv")
    print("  - party_gains_losses_by_county_sd_old.csv")
    print("  - party_gains_losses_by_county_hd_old.csv")
    print("  - party_gains_losses_by_county_sd_new.csv")
    print("  - party_old_vs_new_districts_comparison.csv")
    
    return {
        "county_party": county_party,
        "county_cd_old": county_cd_old,
        "county_sd_old": county_sd_old,
        "county_hd_old": county_hd_old,
        "county_sd_new": county_sd_new,
        "county_gains_losses": county_gains_losses,
        "county_cd_gains_losses": county_cd_gains_losses,
        "county_sd_old_gains_losses": county_sd_old_gains_losses,
        "county_hd_old_gains_losses": county_hd_old_gains_losses,
        "county_sd_new_gains_losses": county_sd_new_gains_losses,
        "old_vs_new_comparison": old_vs_new_comparison
    }


def calculate_county_gains_losses(df_pd: pd.DataFrame) -> pd.DataFrame:
    """Calculate party gains/losses by county comparing old vs new districts."""
    results = []
    
    for county in sorted(df_pd["COUNTY"].unique()):
        county_df = df_pd[df_pd["COUNTY"] == county]
        
        # OLD districts (what voters were in before)
        old_with_party = county_df[
            county_df["NEWSD"].notna() & county_df["party"].notna()
        ]
        old_rep = len(old_with_party[old_with_party["party"] == "Republican"])
        old_dem = len(old_with_party[old_with_party["party"] == "Democrat"])
        old_other = len(old_with_party[~old_with_party["party"].isin(["Republican", "Democrat"])])
        old_total = len(old_with_party)
        
        # NEW districts (where voters are now) - using State Senate as representative
        new_with_party = county_df[
            county_df["2026_SD"].notna() & county_df["party"].notna()
        ]
        new_rep = len(new_with_party[new_with_party["party"] == "Republican"])
        new_dem = len(new_with_party[new_with_party["party"] == "Democrat"])
        new_other = len(new_with_party[~new_with_party["party"].isin(["Republican", "Democrat"])])
        new_total = len(new_with_party)
        
        # Calculate changes
        net_rep = new_rep - old_rep
        net_dem = new_dem - old_dem
        net_other = new_other - old_other
        
        results.append({
            "County": county,
            "Old_Republican": old_rep,
            "Old_Democrat": old_dem,
            "Old_Other": old_other,
            "Old_Total": old_total,
            "New_Republican": new_rep,
            "New_Democrat": new_dem,
            "New_Other": new_other,
            "New_Total": new_total,
            "Net_Republican_Change": net_rep,
            "Net_Democrat_Change": net_dem,
            "Net_Other_Change": net_other,
            "Pct_Republican_Change": (net_rep / old_rep * 100) if old_rep > 0 else 0,
            "Pct_Democrat_Change": (net_dem / old_dem * 100) if old_dem > 0 else 0,
        })
    
    return pd.DataFrame(results)


def calculate_gains_losses_by_geography(
    df_pd: pd.DataFrame,
    groupby_cols: list,
    label: str,
    use_new_districts: bool = False
) -> pd.DataFrame:
    """Calculate party gains/losses grouped by specified geography columns."""
    results = []
    
    # Group by the specified columns
    for group_values, group_df in df_pd.groupby(groupby_cols):
        if isinstance(group_values, tuple):
            group_dict = dict(zip(groupby_cols, group_values))
        else:
            group_dict = {groupby_cols[0]: group_values}
        
        # Filter to voters with party information
        with_party = group_df[group_df["party"].notna()]
        
        # Count by party
        rep_count = len(with_party[with_party["party"] == "Republican"])
        dem_count = len(with_party[with_party["party"] == "Democrat"])
        other_count = len(with_party[~with_party["party"].isin(["Republican", "Democrat"])])
        total_count = len(with_party)
        
        # Calculate percentages
        rep_pct = (rep_count / total_count * 100) if total_count > 0 else 0
        dem_pct = (dem_count / total_count * 100) if total_count > 0 else 0
        other_pct = (other_count / total_count * 100) if total_count > 0 else 0
        
        result_row = group_dict.copy()
        result_row.update({
            "Republican_Count": rep_count,
            "Democrat_Count": dem_count,
            "Other_Count": other_count,
            "Total_Count": total_count,
            "Republican_Pct": rep_pct,
            "Democrat_Pct": dem_pct,
            "Other_Pct": other_pct,
        })
        
        results.append(result_row)
    
    return pd.DataFrame(results)


def create_old_vs_new_comparison(df_pd: pd.DataFrame) -> pd.DataFrame:
    """Create comprehensive comparison showing party composition in old vs new districts by county."""
    results = []
    
    # Group by county and old/new district combinations
    for county in sorted(df_pd["COUNTY"].unique()):
        county_df = df_pd[df_pd["COUNTY"] == county]
        
        # Get unique old and new districts in this county
        old_districts = county_df["NEWSD"].dropna().unique()
        new_districts = county_df["2026_SD"].dropna().unique()
        
        # For each old district, show what it contributed to new districts
        for old_sd in old_districts:
            old_dist_voters = county_df[county_df["NEWSD"] == old_sd]
            
            old_rep = len(old_dist_voters[old_dist_voters["party"] == "Republican"])
            old_dem = len(old_dist_voters[old_dist_voters["party"] == "Democrat"])
            old_other = len(old_dist_voters[~old_dist_voters["party"].isin(["Republican", "Democrat"])])
            old_total = len(old_dist_voters)
            
            # Find where these voters went (new districts)
            for new_sd in new_districts:
                transition_voters = old_dist_voters[old_dist_voters["2026_SD"] == new_sd]
                
                if len(transition_voters) == 0:
                    continue
                
                new_rep = len(transition_voters[transition_voters["party"] == "Republican"])
                new_dem = len(transition_voters[transition_voters["party"] == "Democrat"])
                new_other = len(transition_voters[~transition_voters["party"].isin(["Republican", "Democrat"])])
                new_total = len(transition_voters)
                
                results.append({
                    "County": county,
                    "Old_SD": old_sd,
                    "New_SD": new_sd,
                    "Old_Republican": old_rep,
                    "Old_Democrat": old_dem,
                    "Old_Other": old_other,
                    "Old_Total": old_total,
                    "Transition_Republican": new_rep,
                    "Transition_Democrat": new_dem,
                    "Transition_Other": new_other,
                    "Transition_Total": new_total,
                    "Pct_Old_Republican": (old_rep / old_total * 100) if old_total > 0 else 0,
                    "Pct_Old_Democrat": (old_dem / old_total * 100) if old_total > 0 else 0,
                    "Pct_Transition_Republican": (new_rep / new_total * 100) if new_total > 0 else 0,
                    "Pct_Transition_Democrat": (new_dem / new_total * 100) if new_total > 0 else 0,
                })
    
    return pd.DataFrame(results)

