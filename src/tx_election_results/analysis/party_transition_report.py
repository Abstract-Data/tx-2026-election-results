"""
Generate detailed report showing voter transitions from old to new districts.
Shows how many voters of each party type moved between districts.
"""
import polars as pl
import pandas as pd
from pathlib import Path


def generate_party_transition_report(
    merged_voter_df: pl.DataFrame,
    old_district_col: str = "NEWSD",
    new_district_col: str = "2026_District",
    output_dir: str = "data/exports"
) -> pd.DataFrame:
    """
    Generate detailed report showing voter transitions from old to new districts.
    
    For each new district, shows:
    - Which old districts contributed voters
    - How many voters of each party came from each old district
    - Net gains/losses for each party type
    
    Args:
        merged_voter_df: Voter dataframe with both old and new district assignments
        old_district_col: Column name for old district (e.g., "NEWSD")
        new_district_col: Column name for new district (e.g., "2026_District")
        output_dir: Directory to save report
    
    Returns:
        DataFrame with detailed transition report
    """
    print("\n" + "=" * 80)
    print("GENERATING PARTY TRANSITION REPORT")
    print("=" * 80)
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    (output_path / "csv").mkdir(exist_ok=True, parents=True)
    
    # Filter to voters with both old and new district assignments
    voters_with_both = merged_voter_df.filter(
        pl.col(old_district_col).is_not_null() &
        pl.col(new_district_col).is_not_null()
    )
    
    print(f"\nAnalyzing {len(voters_with_both):,} voters with both old and new district assignments...")
    
    # Create transition matrix: old district -> new district by party
    transition = (
        voters_with_both
        .group_by([old_district_col, new_district_col, "party"])
        .agg(pl.count().alias("voter_count"))
        .sort([new_district_col, old_district_col, "party"])
    )
    
    transition_pd = transition.to_pandas()
    
    # Calculate summary for each new district
    print("\nCalculating voter transitions for each new district...")
    
    report_rows = []
    
    # Get all unique new districts
    new_districts = sorted(voters_with_both.select(new_district_col).unique().to_series().to_list())
    
    for new_dist in new_districts:
        # Get all transitions into this new district
        district_transitions = transition_pd[transition_pd[new_district_col] == new_dist].copy()
        
        if len(district_transitions) == 0:
            continue
        
        # Calculate totals by party in new district
        total_by_party = district_transitions.groupby("party")["voter_count"].sum()
        
        total_republican = total_by_party.get("Republican", 0)
        total_democrat = total_by_party.get("Democrat", 0)
        total_other = total_by_party.sum() - total_republican - total_democrat
        total_voters = total_by_party.sum()
        
        # Calculate which old districts contributed and how many voters from each
        old_dist_contributions = district_transitions.groupby(old_district_col).agg({
            "voter_count": "sum",
            "party": lambda x: dict(zip(*[x, district_transitions.loc[x.index, "voter_count"]]))
        })
        
        # Get old district party compositions (what voters were in before redistricting)
        old_districts_in_new = district_transitions[old_district_col].unique()
        
        # Calculate what party composition was in those old districts (before redistricting)
        old_district_party_totals = {}
        for old_dist in old_districts_in_new:
            old_dist_voters = voters_with_both.filter(pl.col(old_district_col) == old_dist)
            old_dist_party_counts = (
                old_dist_voters
                .group_by("party")
                .agg(pl.count().alias("count"))
                .sort("party")
            )
            old_district_party_totals[old_dist] = old_dist_party_counts.to_pandas().set_index("party")["count"].to_dict()
        
        # For each old district, calculate what portion of its voters came to this new district
        old_dist_breakdown = []
        for old_dist in old_districts_in_new:
            old_to_new = district_transitions[district_transitions[old_district_col] == old_dist]
            
            rep_from_old = old_to_new[old_to_new["party"] == "Republican"]["voter_count"].sum() if "Republican" in old_to_new["party"].values else 0
            dem_from_old = old_to_new[old_to_new["party"] == "Democrat"]["voter_count"].sum() if "Democrat" in old_to_new["party"].values else 0
            other_from_old = old_to_new[~old_to_new["party"].isin(["Republican", "Democrat"])]["voter_count"].sum()
            total_from_old = rep_from_old + dem_from_old + other_from_old
            
            # Get old district totals
            old_totals = old_district_party_totals.get(old_dist, {})
            old_rep_total = old_totals.get("Republican", 0)
            old_dem_total = old_totals.get("Democrat", 0)
            old_other_total = sum([v for k, v in old_totals.items() if k not in ["Republican", "Democrat"]])
            
            old_dist_breakdown.append({
                "old_district": old_dist,
                "voters_to_new_district": total_from_old,
                "republican_voters": rep_from_old,
                "democrat_voters": dem_from_old,
                "other_voters": other_from_old,
                "old_district_rep_total": old_rep_total,
                "old_district_dem_total": old_dem_total,
                "old_district_other_total": old_other_total,
            })
        
        # Calculate weighted expected party composition based on old districts
        total_from_old_districts = sum([b["voters_to_new_district"] for b in old_dist_breakdown])
        
        if total_from_old_districts > 0:
            # Weight by number of voters from each old district
            expected_rep = sum([b["republican_voters"] for b in old_dist_breakdown])
            expected_dem = sum([b["democrat_voters"] for b in old_dist_breakdown])
            expected_other = sum([b["other_voters"] for b in old_dist_breakdown])
        else:
            expected_rep = 0
            expected_dem = 0
            expected_other = 0
        
        # Calculate net gains/losses
        net_rep_change = total_republican - expected_rep
        net_dem_change = total_democrat - expected_dem
        net_other_change = total_other - expected_other
        
        # Calculate percentages
        rep_pct = (total_republican / total_voters * 100) if total_voters > 0 else 0
        dem_pct = (total_democrat / total_voters * 100) if total_voters > 0 else 0
        other_pct = (total_other / total_voters * 100) if total_voters > 0 else 0
        
        expected_rep_pct = (expected_rep / total_voters * 100) if total_voters > 0 else 0
        expected_dem_pct = (expected_dem / total_voters * 100) if total_voters > 0 else 0
        expected_other_pct = (expected_other / total_voters * 100) if total_voters > 0 else 0
        
        # Main summary row
        report_rows.append({
            "new_district": new_dist,
            "total_voters": total_voters,
            "republican_voters": total_republican,
            "democrat_voters": total_democrat,
            "other_voters": total_other,
            "republican_pct": rep_pct,
            "democrat_pct": dem_pct,
            "other_pct": other_pct,
            "expected_republican": expected_rep,
            "expected_democrat": expected_dem,
            "expected_other": expected_other,
            "expected_republican_pct": expected_rep_pct,
            "expected_democrat_pct": expected_dem_pct,
            "expected_other_pct": expected_other_pct,
            "net_republican_change": net_rep_change,
            "net_democrat_change": net_dem_change,
            "net_other_change": net_other_change,
            "pct_republican_change": ((net_rep_change / expected_rep * 100) if expected_rep > 0 else 0),
            "pct_democrat_change": ((net_dem_change / expected_dem * 100) if expected_dem > 0 else 0),
            "pct_other_change": ((net_other_change / expected_other * 100) if expected_other > 0 else 0),
            "contributing_old_districts": ", ".join([str(d) for d in sorted(old_districts_in_new)]),
            "num_contributing_districts": len(old_districts_in_new)
        })
    
    # Create summary report
    summary_report = pd.DataFrame(report_rows)
    summary_report = summary_report.sort_values("new_district")
    
    # Save summary report
    summary_report.to_csv(output_path / "csv" / "party_transition_summary.csv", index=False)
    
    # Create detailed breakdown by old district
    print("\nCreating detailed breakdown by old district...")
    detailed_rows = []
    
    for new_dist in new_districts:
        district_transitions = transition_pd[transition_pd[new_district_col] == new_dist].copy()
        
        if len(district_transitions) == 0:
            continue
        
        old_districts_in_new = district_transitions[old_district_col].unique()
        
        for old_dist in old_districts_in_new:
            old_to_new = district_transitions[district_transitions[old_district_col] == old_dist]
            
            for _, row in old_to_new.iterrows():
                detailed_rows.append({
                    "new_district": new_dist,
                    "old_district": old_dist,
                    "party": row["party"],
                    "voter_count": row["voter_count"]
                })
    
    detailed_report = pd.DataFrame(detailed_rows)
    detailed_report = detailed_report.sort_values(["new_district", "old_district", "party"])
    
    # Create pivot table for easier viewing
    detailed_pivot = detailed_report.pivot_table(
        index=["new_district", "old_district"],
        columns="party",
        values="voter_count",
        aggfunc="sum",
        fill_value=0
    )
    detailed_pivot = detailed_pivot.reset_index()
    
    # Save detailed reports
    detailed_report.to_csv(output_path / "csv" / "party_transition_detailed.csv", index=False)
    detailed_pivot.to_csv(output_path / "csv" / "party_transition_matrix.csv", index=False)
    
    # Print summary statistics
    print("\n" + "-" * 80)
    print("PARTY TRANSITION SUMMARY")
    print("-" * 80)
    print(f"\nTotal new districts: {len(summary_report)}")
    print(f"Total voters analyzed: {summary_report['total_voters'].sum():,}")
    print(f"\nNet Republican Change: {summary_report['net_republican_change'].sum():+,.0f} voters")
    print(f"Net Democrat Change: {summary_report['net_democrat_change'].sum():+,.0f} voters")
    print(f"Net Other Change: {summary_report['net_other_change'].sum():+,.0f} voters")
    
    print("\n" + "-" * 80)
    print("TOP DISTRICTS BY PARTY GAINS/LOSSES")
    print("-" * 80)
    
    print("\nDistricts with largest Republican gains:")
    top_rep = summary_report.nlargest(10, "net_republican_change")[
        ["new_district", "republican_voters", "expected_republican", "net_republican_change", "pct_republican_change"]
    ]
    print(top_rep.to_string(index=False))
    
    print("\nDistricts with largest Democrat gains:")
    top_dem = summary_report.nlargest(10, "net_democrat_change")[
        ["new_district", "democrat_voters", "expected_democrat", "net_democrat_change", "pct_democrat_change"]
    ]
    print(top_dem.to_string(index=False))
    
    print("\nDistricts with largest Republican losses:")
    top_rep_loss = summary_report.nsmallest(10, "net_republican_change")[
        ["new_district", "republican_voters", "expected_republican", "net_republican_change", "pct_republican_change"]
    ]
    print(top_rep_loss.to_string(index=False))
    
    print("\nDistricts with largest Democrat losses:")
    top_dem_loss = summary_report.nsmallest(10, "net_democrat_change")[
        ["new_district", "democrat_voters", "expected_democrat", "net_democrat_change", "pct_democrat_change"]
    ]
    print(top_dem_loss.to_string(index=False))
    
    print(f"\nReports saved to:")
    print(f"  - {output_path / 'csv' / 'party_transition_summary.csv'}")
    print(f"  - {output_path / 'csv' / 'party_transition_detailed.csv'}")
    print(f"  - {output_path / 'csv' / 'party_transition_matrix.csv'}")
    
    return {
        "summary": summary_report,
        "detailed": detailed_report,
        "matrix": detailed_pivot
    }

