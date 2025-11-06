"""
Generate party gains/losses tables for all district types: HD, SD, CD.
"""
import polars as pl
import pandas as pd
from pathlib import Path
from tx_election_results.analysis.district_comparison import calculate_party_gains_losses
from tx_election_results.utils.helpers import format_district_summary


def generate_all_districts_gains_losses(
    merged_voter_df: pl.DataFrame,
    output_dir: str = "data/exports"
) -> dict:
    """
    Generate party gains/losses for all district types: HD, SD, CD.
    
    Args:
        merged_voter_df: Voter dataframe with district assignments
        output_dir: Directory to save reports
    
    Returns:
        Dictionary with results for each district type
    """
    print("\n" + "=" * 80)
    print("GENERATING PARTY GAINS/LOSSES FOR ALL DISTRICT TYPES")
    print("=" * 80)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    (output_path / "csv").mkdir(exist_ok=True, parents=True)
    (output_path / "districts").mkdir(exist_ok=True, parents=True)
    
    results = {}
    
    # 1. State Senate Districts (SD) - comparing old NEWSD to new 2026_SD
    print("\n" + "-" * 80)
    print("1. STATE SENATE DISTRICTS (SD)")
    print("-" * 80)
    print("Comparing OLD State Senate Districts (NEWSD) to NEW State Senate Districts (2026_SD)")
    
    # Check if modeled data is available
    use_modeled = "predicted_party_score" in merged_voter_df.columns
    
    sd_results = calculate_party_gains_losses(
        merged_voter_df,
        old_district_col="NEWSD",
        new_district_col="2026_SD",
        output_dir=str(output_path / "districts" / "sd_districts"),
        use_modeled=use_modeled
    )
    
    # Save SD summary
    sd_summary = format_district_summary(
        sd_results["gains_losses"],
        "State Senate Districts (SD)",
        "NEWSD to 2026_SD"
    )
    sd_summary.to_csv(output_path / "csv" / "sd_gains_losses_summary.csv", index=False)
    results["SD"] = {
        "detailed": sd_results,
        "summary": sd_summary
    }
    
    # 2. Congressional Districts (CD) - comparing old NEWCD to new 2026_CD
    print("\n" + "-" * 80)
    print("2. CONGRESSIONAL DISTRICTS (CD)")
    print("-" * 80)
    print("Comparing OLD Congressional Districts (NEWCD) to NEW Congressional Districts (2026_CD)")
    
    cd_results = calculate_party_gains_losses(
        merged_voter_df,
        old_district_col="NEWCD",
        new_district_col="2026_CD",
        output_dir=str(output_path / "districts" / "cd_districts"),
        use_modeled=use_modeled
    )
    
    # Save CD summary
    cd_summary = format_district_summary(
        cd_results["gains_losses"],
        "Congressional Districts (CD)",
        "NEWCD to 2026_CD"
    )
    cd_summary.to_csv(output_path / "csv" / "cd_gains_losses_summary.csv", index=False)
    results["CD"] = {
        "detailed": cd_results,
        "summary": cd_summary
    }
    
    # 3. House Districts (HD) - comparing old NEWHD to new 2026_HD
    print("\n" + "-" * 80)
    print("3. HOUSE DISTRICTS (HD)")
    print("-" * 80)
    print("Comparing OLD House Districts (NEWHD) to NEW House Districts (2026_HD)")
    
    hd_results = calculate_party_gains_losses(
        merged_voter_df,
        old_district_col="NEWHD",
        new_district_col="2026_HD",
        output_dir=str(output_path / "districts" / "hd_districts"),
        use_modeled=use_modeled
    )
    
    # Save HD summary
    hd_summary = format_district_summary(
        hd_results["gains_losses"],
        "House Districts (HD)",
        "NEWHD to 2026_HD"
    )
    hd_summary.to_csv(output_path / "csv" / "hd_gains_losses_summary.csv", index=False)
    results["HD"] = {
        "detailed": hd_results,
        "summary": hd_summary
    }
    
    # 4. Create comprehensive summary table
    print("\n" + "-" * 80)
    print("4. CREATING COMPREHENSIVE SUMMARY")
    print("-" * 80)
    
    comprehensive_summary = create_comprehensive_summary(results, output_path)
    
    print("\n" + "=" * 80)
    print("ALL DISTRICT TYPES ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"\nAll reports saved to: {output_path}")
    
    return results


def analyze_cd_to_new_sd(
    merged_voter_df: pl.DataFrame,
    output_path: Path
) -> dict:
    """Analyze how Congressional Districts map to new State Senate Districts."""
    output_path.mkdir(exist_ok=True, parents=True)
    
    # For each old CD, show party composition and where voters went (new SDs)
    cd_analysis = []
    
    for old_cd in sorted(merged_voter_df.filter(pl.col("NEWCD").is_not_null())["NEWCD"].unique().to_list()):
        cd_voters = merged_voter_df.filter(pl.col("NEWCD") == old_cd)
        
        # Party composition in old CD
        old_rep = len(cd_voters.filter(pl.col("party") == "Republican"))
        old_dem = len(cd_voters.filter(pl.col("party") == "Democrat"))
        old_other = len(cd_voters.filter(~pl.col("party").is_in(["Republican", "Democrat"])))
        old_total = len(cd_voters)
        
        # Where do they go in new SDs?
        new_sd_breakdown = (
            cd_voters
            .filter(pl.col("2026_District").is_not_null())
            .group_by(["2026_District", "party"])
            .agg(pl.count().alias("voter_count"))
            .sort(["2026_District", "party"])
        )
        
        new_sd_pd = new_sd_breakdown.to_pandas()
        
        # For each new SD this CD contributes to, calculate party composition
        for new_sd in new_sd_pd["2026_District"].unique():
            sd_voters = new_sd_pd[new_sd_pd["2026_District"] == new_sd]
            
            new_rep = sd_voters[sd_voters["party"] == "Republican"]["voter_count"].sum() if "Republican" in sd_voters["party"].values else 0
            new_dem = sd_voters[sd_voters["party"] == "Democrat"]["voter_count"].sum() if "Democrat" in sd_voters["party"].values else 0
            new_other = sd_voters[~sd_voters["party"].isin(["Republican", "Democrat"])]["voter_count"].sum()
            new_total = new_rep + new_dem + new_other
            
            # Calculate net change (portion of CD that went to this SD)
            # Weight by proportion of CD voters that went to this SD
            proportion = new_total / old_total if old_total > 0 else 0
            expected_rep = old_rep * proportion
            expected_dem = old_dem * proportion
            expected_other = old_other * proportion
            
            net_rep = new_rep - expected_rep
            net_dem = new_dem - expected_dem
            
            cd_analysis.append({
                "Old_CD": old_cd,
                "New_SD": new_sd,
                "Old_CD_Republican": old_rep,
                "Old_CD_Democrat": old_dem,
                "Old_CD_Other": old_other,
                "Old_CD_Total": old_total,
                "Voters_To_New_SD": new_total,
                "New_SD_Republican": new_rep,
                "New_SD_Democrat": new_dem,
                "New_SD_Other": new_other,
                "Expected_Republican": expected_rep,
                "Expected_Democrat": expected_dem,
                "Net_Republican_Change": net_rep,
                "Net_Democrat_Change": net_dem,
                "Pct_Republican_Change": (net_rep / expected_rep * 100) if expected_rep > 0 else 0,
                "Pct_Democrat_Change": (net_dem / expected_dem * 100) if expected_dem > 0 else 0,
            })
    
    cd_df = pd.DataFrame(cd_analysis)
    cd_df.to_csv(output_path / "cd_to_new_sd_mapping.csv", index=False)
    
    # Create summary by CD
    cd_summary = cd_df.groupby("Old_CD").agg({
        "Net_Republican_Change": "sum",
        "Net_Democrat_Change": "sum",
        "Old_CD_Republican": "first",
        "Old_CD_Democrat": "first",
        "Old_CD_Total": "first"
    }).reset_index()
    
    cd_summary.to_csv(output_path / "cd_gains_losses_summary.csv", index=False)
    
    # Rename columns to match expected format
    cd_summary_for_summary = cd_summary.rename(columns={
        "Old_CD": "district",
        "Net_Republican_Change": "net_republican_change",
        "Net_Democrat_Change": "net_democrat_change"
    })
    
    summary_text = format_district_summary(
        cd_summary_for_summary,
        "Congressional Districts (CD)",
        "NEWCD to 2026_District (SD)"
    )
    summary_text.to_csv(output_path / "cd_summary_table.csv", index=False)
    
    return {
        "mapping": cd_df,
        "summary": cd_summary
    }


def analyze_hd_to_new_sd(
    merged_voter_df: pl.DataFrame,
    output_path: Path
) -> dict:
    """Analyze how House Districts map to new State Senate Districts."""
    output_path.mkdir(exist_ok=True, parents=True)
    
    # Similar to CD analysis
    hd_analysis = []
    
    for old_hd in sorted(merged_voter_df.filter(pl.col("NEWHD").is_not_null())["NEWHD"].unique().to_list()):
        hd_voters = merged_voter_df.filter(pl.col("NEWHD") == old_hd)
        
        # Party composition in old HD
        old_rep = len(hd_voters.filter(pl.col("party") == "Republican"))
        old_dem = len(hd_voters.filter(pl.col("party") == "Democrat"))
        old_other = len(hd_voters.filter(~pl.col("party").is_in(["Republican", "Democrat"])))
        old_total = len(hd_voters)
        
        # Where do they go in new SDs?
        new_sd_breakdown = (
            hd_voters
            .filter(pl.col("2026_District").is_not_null())
            .group_by(["2026_District", "party"])
            .agg(pl.count().alias("voter_count"))
            .sort(["2026_District", "party"])
        )
        
        new_sd_pd = new_sd_breakdown.to_pandas()
        
        for new_sd in new_sd_pd["2026_District"].unique():
            sd_voters = new_sd_pd[new_sd_pd["2026_District"] == new_sd]
            
            new_rep = sd_voters[sd_voters["party"] == "Republican"]["voter_count"].sum() if "Republican" in sd_voters["party"].values else 0
            new_dem = sd_voters[sd_voters["party"] == "Democrat"]["voter_count"].sum() if "Democrat" in sd_voters["party"].values else 0
            new_other = sd_voters[~sd_voters["party"].isin(["Republican", "Democrat"])]["voter_count"].sum()
            new_total = new_rep + new_dem + new_other
            
            proportion = new_total / old_total if old_total > 0 else 0
            expected_rep = old_rep * proportion
            expected_dem = old_dem * proportion
            expected_other = old_other * proportion
            
            net_rep = new_rep - expected_rep
            net_dem = new_dem - expected_dem
            
            hd_analysis.append({
                "Old_HD": old_hd,
                "New_SD": new_sd,
                "Old_HD_Republican": old_rep,
                "Old_HD_Democrat": old_dem,
                "Old_HD_Other": old_other,
                "Old_HD_Total": old_total,
                "Voters_To_New_SD": new_total,
                "New_SD_Republican": new_rep,
                "New_SD_Democrat": new_dem,
                "New_SD_Other": new_other,
                "Expected_Republican": expected_rep,
                "Expected_Democrat": expected_dem,
                "Net_Republican_Change": net_rep,
                "Net_Democrat_Change": net_dem,
                "Pct_Republican_Change": (net_rep / expected_rep * 100) if expected_rep > 0 else 0,
                "Pct_Democrat_Change": (net_dem / expected_dem * 100) if expected_dem > 0 else 0,
            })
    
    hd_df = pd.DataFrame(hd_analysis)
    hd_df.to_csv(output_path / "hd_to_new_sd_mapping.csv", index=False)
    
    # Create summary by HD
    hd_summary = hd_df.groupby("Old_HD").agg({
        "Net_Republican_Change": "sum",
        "Net_Democrat_Change": "sum",
        "Old_HD_Republican": "first",
        "Old_HD_Democrat": "first",
        "Old_HD_Total": "first"
    }).reset_index()
    
    hd_summary.to_csv(output_path / "hd_gains_losses_summary.csv", index=False)
    
    # Rename columns to match expected format
    hd_summary_for_summary = hd_summary.rename(columns={
        "Old_HD": "district",
        "Net_Republican_Change": "net_republican_change",
        "Net_Democrat_Change": "net_democrat_change"
    })
    
    summary_text = format_district_summary(
        hd_summary_for_summary,
        "House Districts (HD)",
        "NEWHD to 2026_District (SD)"
    )
    summary_text.to_csv(output_path / "hd_summary_table.csv", index=False)
    
    return {
        "mapping": hd_df,
        "summary": hd_summary
    }


# create_district_type_summary moved to tx_election_results.utils.helpers.format_district_summary


def create_comprehensive_summary(results: dict, output_path: Path) -> pd.DataFrame:
    """Create a comprehensive summary table for all district types."""
    summaries = []
    
    if "SD" in results:
        summaries.append(results["SD"]["summary"])
    if "CD" in results:
        summaries.append(results["CD"]["summary"])
    if "HD" in results:
        summaries.append(results["HD"]["summary"])
    
    comprehensive = pd.concat(summaries, ignore_index=True)
    comprehensive.to_csv(output_path / "csv" / "all_districts_comprehensive_summary.csv", index=False)
    
    print("\n" + "=" * 80)
    print("COMPREHENSIVE SUMMARY - ALL DISTRICT TYPES")
    print("=" * 80)
    print(comprehensive.to_string(index=False))
    
    return comprehensive

