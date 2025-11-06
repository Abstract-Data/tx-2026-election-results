import marimo

__generated_with = "0.17.7"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    # Imports
    import polars as pl
    import pandas as pd
    import numpy as np
    from pathlib import Path
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import seaborn as sns

    # Set style
    try:
        plt.style.use('seaborn-v0_8')
    except:
        try:
            plt.style.use('seaborn')
        except:
            plt.style.use('default')
    sns.set_palette("husl")

    return Path, pd, pl


@app.cell
def _(mo):
    mo.md(r"""
    # Texas Redistricting Analysis: District Map Changes 2022 → 2026

    ## Strategic Assessment of Opportunities and Vulnerabilities

    This report analyzes how redistricting in Texas (from 2022/2024 boundaries to 2026 boundaries)
    creates opportunities and vulnerabilities for each party across three district types:
    - **State Senate Districts (SD)**: 31 districts
    - **Congressional Districts (CD)**: 38 districts
    - **House Districts (HD)**: 150 districts

    The analysis is divided into two critical perspectives:
    1. **What We Know**: Analysis based on voters with primary voting history
    2. **What We Don't Know**: Analysis including modeled predictions for non-primary voters
    """)
    return


@app.cell
def _(Path, pl):
    # Load data as LAZY FRAMES - never collect full dataframes, only aggregated results
    # Update paths to point to data/exports/parquet
    modeled_data_path = Path("data/exports/parquet/voters_with_party_modeling.parquet")
    merged_data_path = Path("data/exports/parquet/early_voting_merged.parquet")
    
    # Columns needed for analysis
    district_cols = ["NEWHD", "2026_HD", "NEWCD", "2026_CD", "NEWSD", "2026_SD"]
    
    if modeled_data_path.exists():
        # Create lazy scan - DO NOT COLLECT
        print("Creating lazy scan for modeled voters...")
        _temp_modeled_scan = pl.scan_parquet(str(modeled_data_path))
        available_modeled_cols = _temp_modeled_scan.columns
        # Select only needed columns that exist (including PRI columns for averaging)
        cols_to_select = ["VUID", "party", "predicted_party_score"]
        cols_to_select.extend([col for col in district_cols if col in available_modeled_cols])
        # Include party_PRI columns for computing averages per primary
        party_pri_cols = ["party_PRI24", "party_PRI22", "party_PRI20", "party_PRI18"]
        cols_to_select.extend([col for col in party_pri_cols if col in available_modeled_cols])
        # Include vote count columns for swing voter calculation
        vote_count_cols = ["rep_primary_votes", "dem_primary_votes", "total_primary_votes"]
        cols_to_select.extend([col for col in vote_count_cols if col in available_modeled_cols])
        # Include voted_early to identify general election voters
        if "voted_early" in available_modeled_cols:
            cols_to_select.append("voted_early")
        # Include all GEN columns (general election history)
        gen_cols = [col for col in available_modeled_cols if col.upper().startswith("GEN")]
        cols_to_select.extend(gen_cols)
        
        df_modeled_scan = _temp_modeled_scan.select(cols_to_select)
        print("✓ Lazy scan created for modeled voters (will compute aggregations lazily)")
    else:
        df_modeled_scan = None
        print("⚠️  Modeled data not found")
    
    if merged_data_path.exists():
        # Create lazy scan - DO NOT COLLECT
        print("Creating lazy scan for known voters...")
        _temp_known_scan = pl.scan_parquet(str(merged_data_path))
        available_known_cols = _temp_known_scan.columns
        # Select only needed columns that exist
        cols_to_select = ["VUID", "party"]
        cols_to_select.extend([col for col in district_cols if col in available_known_cols])
        
        df_known_scan = (
            _temp_known_scan
            .filter(
                pl.col("party").is_in(["Republican", "Democrat", "Swing"])
            )
            .select(cols_to_select)
        )
        print("✓ Lazy scan created for known voters (will compute aggregations lazily)")
    else:
        df_known_scan = None
        print("⚠️  Merged data not found")
    
    return df_known_scan, df_modeled_scan, modeled_data_path, merged_data_path


@app.cell
def _(df_known_scan, df_modeled_scan, pl):
    # Compute totals: Unique voters who participated in ANY of the 4 primaries
    # Classified by their voting pattern across all 4 primaries:
    # - Republican: Voters who ONLY voted in Republican primaries (never voted D)
    # - Democrat: Voters who ONLY voted in Democrat primaries (never voted R)
    # - Swing: Voters who voted in BOTH R and D primaries (across different elections)
    if df_modeled_scan is not None:
        schema_names = df_modeled_scan.collect_schema().names()
        
        # Calculate average turnout per primary election across all 4 primaries
        # Check if party_PRI columns exist for counting voters per primary
        has_party_pri_cols = all(col in schema_names for col in ["party_PRI24", "party_PRI22", "party_PRI20", "party_PRI18"])
        
        if has_party_pri_cols:
            # Count voters per primary election (for averaging)
            rep_votes_per_primary = []
            dem_votes_per_primary = []
            
            for pri_col in ["party_PRI24", "party_PRI22", "party_PRI20", "party_PRI18"]:
                # Count Republican primary voters in this election
                rep_count = (
                    df_modeled_scan
                    .filter(pl.col(pri_col) == "Republican")
                    .select(pl.len())
                    .collect()
                    .item()
                )
                rep_votes_per_primary.append(rep_count)
                
                # Count Democrat primary voters in this election
                dem_count = (
                    df_modeled_scan
                    .filter(pl.col(pri_col) == "Democrat")
                    .select(pl.len())
                    .collect()
                    .item()
                )
                dem_votes_per_primary.append(dem_count)
            
            # Average across the 4 primaries
            rep_known = int(sum(rep_votes_per_primary) / len(rep_votes_per_primary)) if rep_votes_per_primary else 0
            dem_known = int(sum(dem_votes_per_primary) / len(dem_votes_per_primary)) if dem_votes_per_primary else 0
            
            # Swing: unique voters who voted in BOTH R and D primaries (across different elections)
            if "rep_primary_votes" in schema_names and "dem_primary_votes" in schema_names:
                swing_known = (
                    df_modeled_scan
                    .filter(
                        (pl.col("rep_primary_votes") > 0) &
                        (pl.col("dem_primary_votes") > 0)
                    )
                    .select(pl.len())
                    .collect()
                    .item()
                )
            else:
                # Build conditions for swing voters using PRI columns
                has_rep_vote = (
                    (pl.col("party_PRI24") == "Republican") |
                    (pl.col("party_PRI22") == "Republican") |
                    (pl.col("party_PRI20") == "Republican") |
                    (pl.col("party_PRI18") == "Republican")
                )
                has_dem_vote = (
                    (pl.col("party_PRI24") == "Democrat") |
                    (pl.col("party_PRI22") == "Democrat") |
                    (pl.col("party_PRI20") == "Democrat") |
                    (pl.col("party_PRI18") == "Democrat")
                )
                swing_known = (
                    df_modeled_scan
                    .filter(has_rep_vote & has_dem_vote)
                    .select(pl.len())
                    .collect()
                    .item()
                )
            
            # Total average = average R + average D turnout per primary
            total_known = rep_known + dem_known
        else:
            # Fallback: use PRI columns if vote count columns don't exist
            pri_cols = ["PRI24", "PRI22", "PRI20", "PRI18"]
            available_pri_cols = [col for col in pri_cols if col in schema_names]
            
            # Build conditions for any R vote and any D vote across all primaries
            rep_conditions = []
            dem_conditions = []
            for pri_col in available_pri_cols:
                rep_conditions.append(
                    pl.col(pri_col).is_not_null() &
                    (pl.col(pri_col).str.contains("R") | pl.col(pri_col).str.contains("r"))
                )
                dem_conditions.append(
                    pl.col(pri_col).is_not_null() &
                    (pl.col(pri_col).str.contains("D") | pl.col(pri_col).str.contains("d"))
                )
            
            # Combine conditions with OR
            has_rep_vote = rep_conditions[0] if rep_conditions else pl.lit(False)
            for condition in rep_conditions[1:]:
                has_rep_vote = has_rep_vote | condition
            
            has_dem_vote = dem_conditions[0] if dem_conditions else pl.lit(False)
            for condition in dem_conditions[1:]:
                has_dem_vote = has_dem_vote | condition
            
            # Count unique voters by classification
            rep_known = (
                df_modeled_scan
                .filter(has_rep_vote & ~has_dem_vote)
                .select(pl.len())
                .collect()
                .item()
            )
            
            dem_known = (
                df_modeled_scan
                .filter(has_dem_vote & ~has_rep_vote)
                .select(pl.len())
                .collect()
                .item()
            )
            
            swing_known = (
                df_modeled_scan
                .filter(has_rep_vote & has_dem_vote)
                .select(pl.len())
                .collect()
                .item()
            )
            
            total_known = rep_known + dem_known + swing_known
        
        # Modeled voters (lazy)
        # Only include voters who have voted in general elections but NOT in primaries (last 10 years)
        # Filter: no primary history AND has general election history (any GEN column) AND has modeling
        gen_cols_available = [col for col in schema_names if col.upper().startswith("GEN")]
        
        # Build condition: voter has voted in at least one general election (any GEN column is not null/empty)
        if gen_cols_available:
            has_gen_history = (
                pl.col(gen_cols_available[0]).is_not_null() &
                (pl.col(gen_cols_available[0]) != "")
            )
            for gen_col in gen_cols_available[1:]:
                has_gen_history = has_gen_history | (
                    pl.col(gen_col).is_not_null() &
                    (pl.col(gen_col) != "")
                )
        else:
            # Fallback to voted_early if no GEN columns found
            has_gen_history = pl.col("voted_early") == True if "voted_early" in schema_names else pl.lit(False)
        
        modeled_voters_scan = df_modeled_scan.filter(
            (pl.col("party") == "Unknown") &
            ((pl.col("total_primary_votes") == 0) | pl.col("total_primary_votes").is_null()) &
            has_gen_history &
            pl.col("predicted_party_score").is_not_null()
        )
        total_modeled = modeled_voters_scan.select(pl.len()).collect().item()
        
        print(f"✓ Computed totals: {total_known:,} avg known voters per primary (R: {rep_known:,}, D: {dem_known:,}, Swing: {swing_known:,} unique), {total_modeled:,} modeled voters")
        
        # For further analysis, use voters with primary history
        # Known voters: those who voted in ANY primary
        if has_party_pri_cols:
            # Use party_PRI columns
            known_voters_scan = df_modeled_scan.filter(
                (pl.col("party_PRI24").is_in(["Republican", "Democrat"])) |
                (pl.col("party_PRI22").is_in(["Republican", "Democrat"])) |
                (pl.col("party_PRI20").is_in(["Republican", "Democrat"])) |
                (pl.col("party_PRI18").is_in(["Republican", "Democrat"]))
            )
        elif "rep_primary_votes" in schema_names and "dem_primary_votes" in schema_names:
            # Use vote count columns
            known_voters_scan = df_modeled_scan.filter(
                (pl.col("rep_primary_votes") > 0) | (pl.col("dem_primary_votes") > 0)
            )
        else:
            # Build PRI column conditions if vote count columns not available
            pri_cols = ["PRI24", "PRI22", "PRI20", "PRI18"]
            available_pri_cols = [col for col in pri_cols if col in schema_names]
            rep_conditions = []
            dem_conditions = []
            for pri_col in available_pri_cols:
                rep_conditions.append(
                    pl.col(pri_col).is_not_null() &
                    (pl.col(pri_col).str.contains("R") | pl.col(pri_col).str.contains("r"))
                )
                dem_conditions.append(
                    pl.col(pri_col).is_not_null() &
                    (pl.col(pri_col).str.contains("D") | pl.col(pri_col).str.contains("d"))
                )
            has_rep_vote_fallback = rep_conditions[0] if rep_conditions else pl.lit(False)
            for condition in rep_conditions[1:]:
                has_rep_vote_fallback = has_rep_vote_fallback | condition
            has_dem_vote_fallback = dem_conditions[0] if dem_conditions else pl.lit(False)
            for condition in dem_conditions[1:]:
                has_dem_vote_fallback = has_dem_vote_fallback | condition
            known_voters_scan = df_modeled_scan.filter(
                has_rep_vote_fallback | has_dem_vote_fallback
            )
        known_scan = known_voters_scan
        modeled_scan = modeled_voters_scan
    else:
        known_scan = None
        modeled_scan = None
        total_known = 0
        rep_known = 0
        dem_known = 0
        swing_known = 0
        total_modeled = 0
    
    # Fallback to df_known_scan if df_modeled_scan is not available
    if df_known_scan is not None and known_scan is None:
        known_scan = df_known_scan
        total_known = df_known_scan.select(pl.count()).collect().item()
        rep_known = df_known_scan.filter(pl.col("party") == "Republican").select(pl.count()).collect().item()
        dem_known = df_known_scan.filter(pl.col("party") == "Democrat").select(pl.count()).collect().item()
        swing_known = df_known_scan.filter(pl.col("party") == "Swing").select(pl.count()).collect().item()
    
    return (
        dem_known,
        known_scan,
        modeled_scan,
        rep_known,
        swing_known,
        total_known,
        total_modeled,
    )


@app.cell
def _(mo):
    mo.md(r"""
    ---

    # SECTION 1: WHAT WE KNOW
    ## Analysis Based on Primary Voting History Across Last 4 Primaries

    This section analyzes voters who have voted in at least one of the last four primary elections (2018, 2020, 2022, 2024).
    These voters have **demonstrated party preference** through their primary voting behavior.

    **Methodology**:
    - Voters are classified based on their voting patterns across the last 4 primaries (PRI24, PRI22, PRI20, PRI18)
    - **Republican**: Voters who only voted in Republican primaries (even if just once, no Democrat primary votes)
    - **Democrat**: Voters who only voted in Democrat primaries (even if just once, no Republican primary votes)
    - **Swing**: Voters who voted in both Republican and Democrat primaries (mixed pattern)
    - District classifications are based on the **percentage** of known primary voters in each district
    """)
    return


@app.cell
def _(dem_known, mo, rep_known, swing_known, total_known):
    if total_known > 0:
        stats = mo.vstack([
            mo.md(f"""
            ### Known Voters Summary
            **Average Turnout Per Primary Election Across Last 4 Primaries (2018, 2020, 2022, 2024)**

            These totals represent **average primary election turnout** across the 4 primaries:

            - **Average Total Primary Voters**: {total_known:,} (average R + average D turnout per primary)
            - **Average Republican Primary Voters**: {rep_known:,} ({rep_known/total_known*100:.1f}% of average total) - Average number of Republican primary voters per election
            - **Average Democrat Primary Voters**: {dem_known:,} ({dem_known/total_known*100:.1f}% of average total) - Average number of Democrat primary voters per election
            - **Swing Voters**: {swing_known:,} - Unique voters who voted in BOTH Republican AND Democrat primaries (across different elections)

            **Calculation Method**: 
            - For each primary election (2018, 2020, 2022, 2024), count how many voters participated in Republican vs Democrat primaries
            - Average those counts across the 4 primaries to get average turnout per election
            - Swing voters are counted separately as unique individuals who voted in both R and D primaries across different elections
            - **Note**: These are averages, representing typical primary election turnout (in the millions)
            """),
        ])
    else:
        stats = mo.md("⚠️  Known voter data not available")

    return stats,


@app.cell
def _(known_scan, pl):
    # Analyze known voters by district type using lazy evaluation
    def analyze_known_by_district_type_lazy(scan, dist_type, old_col, new_col):
        """Analyze known voters' party composition by district - lazy evaluation."""
        if scan is None:
            return None
        
        # Filter to known voters with both old and new districts (lazy)
        df_dist_scan = scan.filter(
            pl.col("party").is_in(["Republican", "Democrat", "Swing"]) &
            pl.col(old_col).is_not_null() &
            pl.col(new_col).is_not_null() &
            (pl.col(old_col) != 0) &
            (pl.col(new_col) != 0)
        )
        
        # Calculate party composition for 2026 districts (lazy aggregation)
        new_comp_scan = (
            df_dist_scan
            .group_by([new_col, "party"])
            .agg(pl.count().alias("count"))
        )
        
        # Only collect the aggregated results (small - just per-district counts)
        new_comp = (
            new_comp_scan
            .collect()
            .pivot(
                index=new_col,
                columns="party",
                values="count",
                aggregate_function="first"
            )
            .fill_null(0)
        )
        
        # Calculate net advantage
        new_comp = new_comp.with_columns([
            (pl.col("Republican") - pl.col("Democrat")).alias("net_advantage_2026")
        ])
        
        return new_comp
    
    # Analyze for each district type (only collect aggregated results)
    if known_scan is not None:
        print("Computing known voter aggregations by district (lazy evaluation)...")
        hd_new_known = analyze_known_by_district_type_lazy(known_scan, "HD", "NEWHD", "2026_HD")
        cd_new_known = analyze_known_by_district_type_lazy(known_scan, "CD", "NEWCD", "2026_CD")
        sd_new_known = analyze_known_by_district_type_lazy(known_scan, "SD", "NEWSD", "2026_SD")
        print("✓ Known voter aggregations computed")
    else:
        hd_new_known, cd_new_known, sd_new_known = None, None, None
    
    return cd_new_known, hd_new_known, sd_new_known


@app.cell
def _(df_modeled_scan, pl):
    # Compute average turnout per primary election for each district type
    def compute_avg_turnout_by_district_type(scan, dist_col, dist_type_name):
        """Compute average turnout per primary election for a specific district type."""
        if scan is None:
            return 0, 0, 0, 0
        
        schema_names = scan.collect_schema().names()
        
        # Filter to voters assigned to districts of this type (exclude district 0)
        filtered_scan = scan.filter(
            pl.col(dist_col).is_not_null() &
            (pl.col(dist_col) != 0)
        )
        
        # Check if party_PRI columns exist
        has_party_pri_cols = all(col in schema_names for col in ["party_PRI24", "party_PRI22", "party_PRI20", "party_PRI18"])
        
        if has_party_pri_cols:
            # Count voters per primary election (for averaging)
            rep_votes_per_primary = []
            dem_votes_per_primary = []
            
            for pri_col in ["party_PRI24", "party_PRI22", "party_PRI20", "party_PRI18"]:
                # Count Republican primary voters in this election
                rep_count = (
                    filtered_scan
                    .filter(pl.col(pri_col) == "Republican")
                    .select(pl.len())
                    .collect()
                    .item()
                )
                rep_votes_per_primary.append(rep_count)
                
                # Count Democrat primary voters in this election
                dem_count = (
                    filtered_scan
                    .filter(pl.col(pri_col) == "Democrat")
                    .select(pl.len())
                    .collect()
                    .item()
                )
                dem_votes_per_primary.append(dem_count)
            
            # Average across the 4 primaries
            rep_avg = int(sum(rep_votes_per_primary) / len(rep_votes_per_primary)) if rep_votes_per_primary else 0
            dem_avg = int(sum(dem_votes_per_primary) / len(dem_votes_per_primary)) if dem_votes_per_primary else 0
            
            # Swing: unique voters who voted in BOTH R and D primaries (across different elections)
            if "rep_primary_votes" in schema_names and "dem_primary_votes" in schema_names:
                swing_unique = (
                    filtered_scan
                    .filter(
                        (pl.col("rep_primary_votes") > 0) &
                        (pl.col("dem_primary_votes") > 0)
                    )
                    .select(pl.len())
                    .collect()
                    .item()
                )
            else:
                # Build conditions for swing voters using PRI columns
                has_rep_vote = (
                    (pl.col("party_PRI24") == "Republican") |
                    (pl.col("party_PRI22") == "Republican") |
                    (pl.col("party_PRI20") == "Republican") |
                    (pl.col("party_PRI18") == "Republican")
                )
                has_dem_vote = (
                    (pl.col("party_PRI24") == "Democrat") |
                    (pl.col("party_PRI22") == "Democrat") |
                    (pl.col("party_PRI20") == "Democrat") |
                    (pl.col("party_PRI18") == "Democrat")
                )
                swing_unique = (
                    filtered_scan
                    .filter(has_rep_vote & has_dem_vote)
                    .select(pl.len())
                    .collect()
                    .item()
                )
            
            total_avg = rep_avg + dem_avg
        else:
            # Fallback: use unique voter counts if party_PRI columns not available
            if "rep_primary_votes" in schema_names and "dem_primary_votes" in schema_names:
                rep_unique = (
                    filtered_scan
                    .filter((pl.col("rep_primary_votes") > 0) & (pl.col("dem_primary_votes") == 0))
                    .select(pl.len())
                    .collect()
                    .item()
                )
                dem_unique = (
                    filtered_scan
                    .filter((pl.col("dem_primary_votes") > 0) & (pl.col("rep_primary_votes") == 0))
                    .select(pl.len())
                    .collect()
                    .item()
                )
                swing_unique = (
                    filtered_scan
                    .filter((pl.col("rep_primary_votes") > 0) & (pl.col("dem_primary_votes") > 0))
                    .select(pl.len())
                    .collect()
                    .item()
                )
                rep_avg = rep_unique
                dem_avg = dem_unique
                total_avg = rep_avg + dem_avg
            else:
                rep_avg, dem_avg, swing_unique, total_avg = 0, 0, 0, 0
        
        return rep_avg, dem_avg, swing_unique, total_avg
    
    # Compute average turnout for each district type
    if df_modeled_scan is not None:
        print("Computing average turnout per primary election by district type...")
        hd_rep_avg, hd_dem_avg, hd_swing_unique, hd_total_avg = compute_avg_turnout_by_district_type(df_modeled_scan, "2026_HD", "HD")
        cd_rep_avg, cd_dem_avg, cd_swing_unique, cd_total_avg = compute_avg_turnout_by_district_type(df_modeled_scan, "2026_CD", "CD")
        sd_rep_avg, sd_dem_avg, sd_swing_unique, sd_total_avg = compute_avg_turnout_by_district_type(df_modeled_scan, "2026_SD", "SD")
        print("✓ Average turnout computed by district type")
    else:
        hd_rep_avg, hd_dem_avg, hd_swing_unique, hd_total_avg = 0, 0, 0, 0
        cd_rep_avg, cd_dem_avg, cd_swing_unique, cd_total_avg = 0, 0, 0, 0
        sd_rep_avg, sd_dem_avg, sd_swing_unique, sd_total_avg = 0, 0, 0, 0
    
    return (
        cd_dem_avg,
        cd_rep_avg,
        cd_swing_unique,
        cd_total_avg,
        hd_dem_avg,
        hd_rep_avg,
        hd_swing_unique,
        hd_total_avg,
        sd_dem_avg,
        sd_rep_avg,
        sd_swing_unique,
        sd_total_avg,
    )


@app.cell
def _(hd_new_known, pd):
    # Analyze House Districts - Known Voters
    # Based on 4-primary average (PRI24, PRI22, PRI20, PRI18)
    if hd_new_known is not None:
        hd_new_pd = hd_new_known.to_pandas()

        # Ensure all districts 1-150 are included
        hd_all_districts = pd.DataFrame({"2026_HD": range(1, 151)})
        hd_new_pd = hd_all_districts.merge(hd_new_pd, on="2026_HD", how="left").fillna(0)

        # Calculate totals and percentages based on 4-primary average
        hd_new_pd["total_known"] = hd_new_pd["Republican"] + hd_new_pd["Democrat"] + hd_new_pd.get("Swing", 0)
        hd_new_pd["net_advantage_2026"] = hd_new_pd["Republican"] - hd_new_pd["Democrat"]

        # Calculate percentages (only for districts with voters)
        hd_new_pd["rep_pct"] = hd_new_pd.apply(
            lambda row: (row["Republican"] / row["total_known"] * 100) if row["total_known"] > 0 else 0,
            axis=1
        )
        hd_new_pd["dem_pct"] = hd_new_pd.apply(
            lambda row: (row["Democrat"] / row["total_known"] * 100) if row["total_known"] > 0 else 0,
            axis=1
        )
        hd_new_pd["swing_pct"] = hd_new_pd.apply(
            lambda row: (row.get("Swing", 0) / row["total_known"] * 100) if row["total_known"] > 0 else 0,
            axis=1
        )

        # Classify districts using percentage-based thresholds
        # Republican advantage: >55% Republican of known voters
        # Democrat advantage: >55% Democrat of known voters
        # Competitive: 45-55% for either party (or within 10% margin)
        hd_new_pd["classification"] = "Competitive"
        # Only classify districts with enough voters (at least 100 known voters)
        hd_has_voters = hd_new_pd["total_known"] >= 100
        hd_new_pd.loc[hd_has_voters & (hd_new_pd["rep_pct"] > 55), "classification"] = "Republican"
        hd_new_pd.loc[hd_has_voters & (hd_new_pd["dem_pct"] > 55), "classification"] = "Democrat"
        # If neither party has >55%, but one has clear advantage (>5% margin), classify accordingly
        hd_new_pd.loc[hd_has_voters & (hd_new_pd["rep_pct"] - hd_new_pd["dem_pct"] > 5) & (hd_new_pd["rep_pct"] <= 55), "classification"] = "Republican"
        hd_new_pd.loc[hd_has_voters & (hd_new_pd["dem_pct"] - hd_new_pd["rep_pct"] > 5) & (hd_new_pd["dem_pct"] <= 55), "classification"] = "Democrat"

        hd_rep_advantage = len(hd_new_pd[hd_new_pd["classification"] == "Republican"])
        hd_dem_advantage = len(hd_new_pd[hd_new_pd["classification"] == "Democrat"])
        hd_competitive = len(hd_new_pd[hd_new_pd["classification"] == "Competitive"])

        hd_total_rep = hd_new_pd["Republican"].sum()
        hd_total_dem = hd_new_pd["Democrat"].sum()
        hd_total_swing = hd_new_pd.get("Swing", pd.Series([0] * len(hd_new_pd))).sum()
        hd_net_advantage = hd_total_rep - hd_total_dem
    else:
        hd_new_pd = None
        hd_rep_advantage = 0
        hd_dem_advantage = 0
        hd_competitive = 0
        hd_total_rep = 0
        hd_total_dem = 0
        hd_total_swing = 0
        hd_net_advantage = 0

    return (
        hd_competitive,
        hd_dem_advantage,
        hd_net_advantage,
        hd_rep_advantage,
        hd_total_dem,
        hd_total_rep,
        hd_total_swing,
    )


@app.cell
def _(
    hd_competitive,
    hd_dem_advantage,
    hd_dem_avg,
    hd_net_advantage,
    hd_rep_advantage,
    hd_rep_avg,
    hd_swing_unique,
    hd_total_avg,
    mo,
):
    # Display HD Known Analysis
    hd_net_avg_advantage = hd_rep_avg - hd_dem_avg
    hd_rep_pct = (hd_rep_avg/hd_total_avg*100) if hd_total_avg > 0 else 0
    hd_dem_pct = (hd_dem_avg/hd_total_avg*100) if hd_total_avg > 0 else 0
    mo.vstack([
        mo.md(f"""
        ### House Districts (HD) - Known Voters Analysis
        **Based on Primary Voting Across Last 4 Primaries (2018, 2020, 2022, 2024)**

        **Party Advantage:**
        - **Republican Advantage Districts**: {hd_rep_advantage} ({hd_rep_advantage/150*100:.1f}%)
        - **Democrat Advantage Districts**: {hd_dem_advantage} ({hd_dem_advantage/150*100:.1f}%)
        - **Competitive Districts**: {hd_competitive} ({hd_competitive/150*100:.1f}%)

        **Total Known Voters (across all House Districts):**
        These totals represent **average primary election turnout** across the 4 primaries (2018, 2020, 2022, 2024) for voters assigned to House Districts:
        - **Average Republican Primary Voters**: {hd_rep_avg:,} ({hd_rep_pct:.1f}% of average total) - Average number of Republican primary voters per election
        - **Average Democrat Primary Voters**: {hd_dem_avg:,} ({hd_dem_pct:.1f}% of average total) - Average number of Democrat primary voters per election
        - **Swing Voters**: {hd_swing_unique:,} - Unique voters assigned to House Districts who voted in BOTH Republican AND Democrat primaries (across different elections)
        - **Net Advantage**: {hd_net_avg_advantage:+,} (Republican)
        
        **Calculation Method**: 
        - For each primary election (2018, 2020, 2022, 2024), count how many voters assigned to House Districts participated in Republican vs Democrat primaries
        - Average those counts across the 4 primaries to get average turnout per election
        - Swing voters are counted separately as unique individuals who voted in both R and D primaries across different elections

        **Classification Method**: Districts are classified based on the percentage of known primary voters:
        - **Republican Advantage**: >55% Republican primary voters OR >5% margin with Republican majority
        - **Democrat Advantage**: >55% Democrat primary voters OR >5% margin with Democrat majority  
        - **Competitive**: All other districts

        **Key Insight**: Based on primary voting history, Republicans hold an advantage in {hd_rep_advantage} districts, while Democrats hold an advantage in {hd_dem_advantage} districts.
        """),
    ])
    return


@app.cell
def _(cd_new_known, pd):
    # Analyze Congressional Districts - Known Voters
    # Based on 4-primary average (PRI24, PRI22, PRI20, PRI18)
    if cd_new_known is not None:
        cd_new_pd = cd_new_known.to_pandas()

        cd_new_pd["total_known"] = cd_new_pd["Republican"] + cd_new_pd["Democrat"] + cd_new_pd.get("Swing", 0)
        cd_new_pd["net_advantage_2026"] = cd_new_pd["Republican"] - cd_new_pd["Democrat"]

        # Calculate percentages
        cd_new_pd["rep_pct"] = cd_new_pd.apply(
            lambda row: (row["Republican"] / row["total_known"] * 100) if row["total_known"] > 0 else 0,
            axis=1
        )
        cd_new_pd["dem_pct"] = cd_new_pd.apply(
            lambda row: (row["Democrat"] / row["total_known"] * 100) if row["total_known"] > 0 else 0,
            axis=1
        )
        cd_new_pd["swing_pct"] = cd_new_pd.apply(
            lambda row: (row.get("Swing", 0) / row["total_known"] * 100) if row["total_known"] > 0 else 0,
            axis=1
        )

        # Classify using percentage-based thresholds
        cd_new_pd["classification"] = "Competitive"
        cd_has_voters = cd_new_pd["total_known"] >= 500
        cd_new_pd.loc[cd_has_voters & (cd_new_pd["rep_pct"] > 55), "classification"] = "Republican"
        cd_new_pd.loc[cd_has_voters & (cd_new_pd["dem_pct"] > 55), "classification"] = "Democrat"
        cd_new_pd.loc[cd_has_voters & (cd_new_pd["rep_pct"] - cd_new_pd["dem_pct"] > 5) & (cd_new_pd["rep_pct"] <= 55), "classification"] = "Republican"
        cd_new_pd.loc[cd_has_voters & (cd_new_pd["dem_pct"] - cd_new_pd["rep_pct"] > 5) & (cd_new_pd["dem_pct"] <= 55), "classification"] = "Democrat"

        cd_rep_advantage = len(cd_new_pd[cd_new_pd["classification"] == "Republican"])
        cd_dem_advantage = len(cd_new_pd[cd_new_pd["classification"] == "Democrat"])
        cd_competitive = len(cd_new_pd[cd_new_pd["classification"] == "Competitive"])

        cd_total_rep = cd_new_pd["Republican"].sum()
        cd_total_dem = cd_new_pd["Democrat"].sum()
        cd_total_swing = cd_new_pd.get("Swing", pd.Series([0] * len(cd_new_pd))).sum()
        cd_net_advantage = cd_total_rep - cd_total_dem
    else:
        cd_new_pd = None
        cd_rep_advantage = 0
        cd_dem_advantage = 0
        cd_competitive = 0
        cd_total_rep = 0
        cd_total_dem = 0
        cd_total_swing = 0
        cd_net_advantage = 0

    return (
        cd_competitive,
        cd_dem_advantage,
        cd_net_advantage,
        cd_rep_advantage,
        cd_total_dem,
        cd_total_rep,
        cd_total_swing,
    )


@app.cell
def _(
    cd_competitive,
    cd_dem_advantage,
    cd_dem_avg,
    cd_net_advantage,
    cd_rep_advantage,
    cd_rep_avg,
    cd_swing_unique,
    cd_total_avg,
    mo,
):
    # Display CD Known Analysis
    cd_net_avg_advantage = cd_rep_avg - cd_dem_avg
    cd_rep_pct = (cd_rep_avg/cd_total_avg*100) if cd_total_avg > 0 else 0
    cd_dem_pct = (cd_dem_avg/cd_total_avg*100) if cd_total_avg > 0 else 0
    mo.vstack([
        mo.md(f"""
        ### Congressional Districts (CD) - Known Voters Analysis
        **Based on Primary Voting Across Last 4 Primaries (2018, 2020, 2022, 2024)**

        **Party Advantage:**
        - **Republican Advantage Districts**: {cd_rep_advantage} ({cd_rep_advantage/38*100:.1f}%)
        - **Democrat Advantage Districts**: {cd_dem_advantage} ({cd_dem_advantage/38*100:.1f}%)
        - **Competitive Districts**: {cd_competitive} ({cd_competitive/38*100:.1f}%)

        **Total Known Voters (across all Congressional Districts):**
        These totals represent **average primary election turnout** across the 4 primaries (2018, 2020, 2022, 2024) for voters assigned to Congressional Districts:
        - **Average Republican Primary Voters**: {cd_rep_avg:,} ({cd_rep_pct:.1f}% of average total) - Average number of Republican primary voters per election
        - **Average Democrat Primary Voters**: {cd_dem_avg:,} ({cd_dem_pct:.1f}% of average total) - Average number of Democrat primary voters per election
        - **Swing Voters**: {cd_swing_unique:,} - Unique voters assigned to Congressional Districts who voted in BOTH Republican AND Democrat primaries (across different elections)
        - **Net Advantage**: {cd_net_avg_advantage:+,} (Republican)
        
        **Calculation Method**: 
        - For each primary election (2018, 2020, 2022, 2024), count how many voters assigned to Congressional Districts participated in Republican vs Democrat primaries
        - Average those counts across the 4 primaries to get average turnout per election
        - Swing voters are counted separately as unique individuals who voted in both R and D primaries across different elections

        **Classification Method**: Districts are classified based on the percentage of known primary voters (>55% for clear advantage, or >5% margin).

        **Key Insight**: Based on primary voting history, Republicans hold an advantage in {cd_rep_advantage} districts, while Democrats hold an advantage in {cd_dem_advantage} districts.
        """),
    ])
    return


@app.cell
def _(pd, sd_new_known):
    # Analyze State Senate Districts - Known Voters
    # Based on 4-primary average (PRI24, PRI22, PRI20, PRI18)
    if sd_new_known is not None:
        sd_new_pd = sd_new_known.to_pandas()

        sd_new_pd["total_known"] = sd_new_pd["Republican"] + sd_new_pd["Democrat"] + sd_new_pd.get("Swing", 0)
        sd_new_pd["net_advantage_2026"] = sd_new_pd["Republican"] - sd_new_pd["Democrat"]

        # Calculate percentages
        sd_new_pd["rep_pct"] = sd_new_pd.apply(
            lambda row: (row["Republican"] / row["total_known"] * 100) if row["total_known"] > 0 else 0,
            axis=1
        )
        sd_new_pd["dem_pct"] = sd_new_pd.apply(
            lambda row: (row["Democrat"] / row["total_known"] * 100) if row["total_known"] > 0 else 0,
            axis=1
        )
        sd_new_pd["swing_pct"] = sd_new_pd.apply(
            lambda row: (row.get("Swing", 0) / row["total_known"] * 100) if row["total_known"] > 0 else 0,
            axis=1
        )

        # Classify using percentage-based thresholds
        sd_new_pd["classification"] = "Competitive"
        sd_has_voters = sd_new_pd["total_known"] >= 1000
        sd_new_pd.loc[sd_has_voters & (sd_new_pd["rep_pct"] > 55), "classification"] = "Republican"
        sd_new_pd.loc[sd_has_voters & (sd_new_pd["dem_pct"] > 55), "classification"] = "Democrat"
        sd_new_pd.loc[sd_has_voters & (sd_new_pd["rep_pct"] - sd_new_pd["dem_pct"] > 5) & (sd_new_pd["rep_pct"] <= 55), "classification"] = "Republican"
        sd_new_pd.loc[sd_has_voters & (sd_new_pd["dem_pct"] - sd_new_pd["rep_pct"] > 5) & (sd_new_pd["dem_pct"] <= 55), "classification"] = "Democrat"

        sd_rep_advantage = len(sd_new_pd[sd_new_pd["classification"] == "Republican"])
        sd_dem_advantage = len(sd_new_pd[sd_new_pd["classification"] == "Democrat"])
        sd_competitive = len(sd_new_pd[sd_new_pd["classification"] == "Competitive"])

        sd_total_rep = sd_new_pd["Republican"].sum()
        sd_total_dem = sd_new_pd["Democrat"].sum()
        sd_total_swing = sd_new_pd.get("Swing", pd.Series([0] * len(sd_new_pd))).sum()
        sd_net_advantage = sd_total_rep - sd_total_dem
    else:
        sd_new_pd = None
        sd_rep_advantage = 0
        sd_dem_advantage = 0
        sd_competitive = 0
        sd_total_rep = 0
        sd_total_dem = 0
        sd_total_swing = 0
        sd_net_advantage = 0

    return (
        sd_competitive,
        sd_dem_advantage,
        sd_net_advantage,
        sd_rep_advantage,
        sd_total_dem,
        sd_total_rep,
        sd_total_swing,
    )


@app.cell
def _(
    mo,
    sd_competitive,
    sd_dem_advantage,
    sd_dem_avg,
    sd_net_advantage,
    sd_rep_advantage,
    sd_rep_avg,
    sd_swing_unique,
    sd_total_avg,
):
    # Display SD Known Analysis
    sd_net_avg_advantage = sd_rep_avg - sd_dem_avg
    sd_rep_pct = (sd_rep_avg/sd_total_avg*100) if sd_total_avg > 0 else 0
    sd_dem_pct = (sd_dem_avg/sd_total_avg*100) if sd_total_avg > 0 else 0
    mo.vstack([
        mo.md(f"""
        ### State Senate Districts (SD) - Known Voters Analysis
        **Based on Primary Voting Across Last 4 Primaries (2018, 2020, 2022, 2024)**

        **Party Advantage:**
        - **Republican Advantage Districts**: {sd_rep_advantage} ({sd_rep_advantage/31*100:.1f}%)
        - **Democrat Advantage Districts**: {sd_dem_advantage} ({sd_dem_advantage/31*100:.1f}%)
        - **Competitive Districts**: {sd_competitive} ({sd_competitive/31*100:.1f}%)

        **Total Known Voters (across all State Senate Districts):**
        These totals represent **average primary election turnout** across the 4 primaries (2018, 2020, 2022, 2024) for voters assigned to State Senate Districts:
        - **Average Republican Primary Voters**: {sd_rep_avg:,} ({sd_rep_pct:.1f}% of average total) - Average number of Republican primary voters per election
        - **Average Democrat Primary Voters**: {sd_dem_avg:,} ({sd_dem_pct:.1f}% of average total) - Average number of Democrat primary voters per election
        - **Swing Voters**: {sd_swing_unique:,} - Unique voters assigned to State Senate Districts who voted in BOTH Republican AND Democrat primaries (across different elections)
        - **Net Advantage**: {sd_net_avg_advantage:+,} (Republican)
        
        **Calculation Method**: 
        - For each primary election (2018, 2020, 2022, 2024), count how many voters assigned to State Senate Districts participated in Republican vs Democrat primaries
        - Average those counts across the 4 primaries to get average turnout per election
        - Swing voters are counted separately as unique individuals who voted in both R and D primaries across different elections

        **Classification Method**: Districts are classified based on the percentage of known primary voters (>55% for clear advantage, or >5% margin).

        **Key Insight**: Based on primary voting history, Republicans hold an advantage in {sd_rep_advantage} districts, while Democrats hold an advantage in {sd_dem_advantage} districts.
        """),
    ])
    return


@app.cell
def _(mo):
    mo.md(r"""
    ---
    
    # SECTION 2: WHAT WE DON'T KNOW
    ## The Hidden Voter Risk: General Election Voters Without Primary History
    
    This section analyzes voters who have **voted in general elections but have never voted in a primary election** (over the last 10 years).
    These "secret" voters represent both **opportunity and risk** for each party, as their party preference is unknown
    but can be modeled based on demographics and geographic patterns.
    """)
    return


@app.cell
def _(modeled_scan, mo, pl, total_modeled):
    if modeled_scan is not None and total_modeled > 0:
        # Compute modeled voter counts lazily
        rep_modeled_count = (
            modeled_scan
            .filter(pl.col("predicted_party_score").str.contains("Republican"))
            .select(pl.count())
            .collect()
            .item()
        )
        dem_modeled_count = (
            modeled_scan
            .filter(pl.col("predicted_party_score").str.contains("Democrat"))
            .select(pl.count())
            .collect()
            .item()
        )
        swing_modeled_count = (
            modeled_scan
            .filter(pl.col("predicted_party_score") == "Swing")
            .select(pl.count())
            .collect()
            .item()
        )

        stats_modeled = mo.vstack([
            mo.md(f"""
            ### Modeled Voters Summary

            - **Total Modeled Voters**: {total_modeled:,} ({total_modeled/18_664_949*100:.1f}% of all registered voters)
            - **Likely Republican**: {rep_modeled_count:,} ({rep_modeled_count/total_modeled*100:.1f}%)
            - **Likely Democrat**: {dem_modeled_count:,} ({dem_modeled_count/total_modeled*100:.1f}%)
            - **Swing/Uncertain**: {swing_modeled_count:,} ({swing_modeled_count/total_modeled*100:.1f}%)

            **Critical Risk**: These voters have **voted in general elections but never demonstrated party preference** through primary voting.
            Their turnout patterns and party preferences are unknown, making them highly unpredictable. If these voters turn out in significant numbers, 
            they could dramatically shift district outcomes, especially in competitive districts.

            **Modeling Methodology**: Party prediction based on:
            - Geographic proximity to known primary voters
            - Age demographics
            - Machine learning model (Random Forest) trained on known voters
            """),
        ])
    else:
        stats_modeled = mo.md("⚠️  Modeled voter data not available")

    return stats_modeled,


@app.cell
def _(modeled_scan, pl):
    # Analyze modeled voters by district type using lazy evaluation
    def analyze_modeled_by_district_type_lazy(scan, dist_type, old_col, new_col):
        """Analyze modeled voters' predicted party composition by district - lazy evaluation."""
        if scan is None:
            return None
        
        # Filter to modeled voters with both old and new districts (lazy)
        df_dist_scan = scan.filter(
            (pl.col("party") == "Unknown") &
            pl.col("predicted_party_score").is_not_null() &
            pl.col(old_col).is_not_null() &
            pl.col(new_col).is_not_null() &
            (pl.col(old_col) != 0) &
            (pl.col(new_col) != 0)
        )
        
        # Simplify party predictions to R/D/Swing (lazy)
        df_dist_scan = df_dist_scan.with_columns([
            pl.when(pl.col("predicted_party_score").str.contains("Republican"))
            .then(pl.lit("Republican"))
            .when(pl.col("predicted_party_score").str.contains("Democrat"))
            .then(pl.lit("Democrat"))
            .otherwise(pl.lit("Swing"))
            .alias("modeled_party")
        ])
        
        # Calculate party composition for 2026 districts (lazy aggregation)
        new_comp_scan = (
            df_dist_scan
            .group_by([new_col, "modeled_party"])
            .agg(pl.count().alias("count"))
        )
        
        # Only collect the aggregated results (small - just per-district counts)
        new_comp = (
            new_comp_scan
            .collect()
            .pivot(
                index=new_col,
                columns="modeled_party",
                values="count",
                aggregate_function="first"
            )
            .fill_null(0)
        )
        
        # Calculate net advantage
        new_comp = new_comp.with_columns([
            (pl.col("Republican") - pl.col("Democrat")).alias("net_advantage_modeled")
        ])
        
        return new_comp
    
    # Analyze for each district type (only collect aggregated results)
    if modeled_scan is not None:
        print("Computing modeled voter aggregations by district (lazy evaluation)...")
        hd_new_modeled = analyze_modeled_by_district_type_lazy(modeled_scan, "HD", "NEWHD", "2026_HD")
        cd_new_modeled = analyze_modeled_by_district_type_lazy(modeled_scan, "CD", "NEWCD", "2026_CD")
        sd_new_modeled = analyze_modeled_by_district_type_lazy(modeled_scan, "SD", "NEWSD", "2026_SD")
        print("✓ Modeled voter aggregations computed")
    else:
        hd_new_modeled, cd_new_modeled, sd_new_modeled = None, None, None
    
    return cd_new_modeled, hd_new_modeled, sd_new_modeled


@app.cell
def _(hd_new_modeled, pd):
    # Analyze House Districts - Modeled Voters
    if hd_new_modeled is not None:
        hd_modeled_pd = hd_new_modeled.to_pandas()

        # Ensure all districts 1-150 are included
        hd_all_districts_modeled = pd.DataFrame({"2026_HD": range(1, 151)})
        hd_modeled_pd = hd_all_districts_modeled.merge(hd_modeled_pd, on="2026_HD", how="left").fillna(0)

        hd_modeled_pd["total_modeled"] = hd_modeled_pd["Republican"] + hd_modeled_pd["Democrat"] + hd_modeled_pd.get("Swing", 0)
        hd_modeled_pd["net_advantage_modeled"] = hd_modeled_pd["Republican"] - hd_modeled_pd["Democrat"]

        # Identify districts with high risk (many modeled voters)
        hd_modeled_pd["risk_level"] = "Low"
        hd_modeled_pd.loc[hd_modeled_pd["total_modeled"] > 50000, "risk_level"] = "High"
        hd_modeled_pd.loc[(hd_modeled_pd["total_modeled"] > 25000) & (hd_modeled_pd["total_modeled"] <= 50000), "risk_level"] = "Medium"

        hd_high_risk_count = len(hd_modeled_pd[hd_modeled_pd["risk_level"] == "High"])
        hd_medium_risk_count = len(hd_modeled_pd[hd_modeled_pd["risk_level"] == "Medium"])

        hd_total_rep_modeled = hd_modeled_pd["Republican"].sum()
        hd_total_dem_modeled = hd_modeled_pd["Democrat"].sum()
        hd_total_swing_modeled = hd_modeled_pd.get("Swing", pd.Series([0] * len(hd_modeled_pd))).sum()
        hd_net_advantage_modeled = hd_total_rep_modeled - hd_total_dem_modeled
    else:
        hd_modeled_pd = None
        hd_high_risk_count = 0
        hd_medium_risk_count = 0
        hd_total_rep_modeled = 0
        hd_total_dem_modeled = 0
        hd_total_swing_modeled = 0
        hd_net_advantage_modeled = 0

    return (
        hd_high_risk_count,
        hd_medium_risk_count,
        hd_net_advantage_modeled,
        hd_total_dem_modeled,
        hd_total_rep_modeled,
        hd_total_swing_modeled,
    )


@app.cell
def _(
    hd_high_risk_count,
    hd_medium_risk_count,
    hd_net_advantage_modeled,
    hd_total_dem_modeled,
    hd_total_rep_modeled,
    hd_total_swing_modeled,
    mo,
):
    # Display HD Modeled Analysis
    mo.vstack([
        mo.md(f"""
        ### House Districts (HD) - Modeled Voters Analysis

        **Hidden Voter Risk:**
        - **High Risk Districts** (>50,000 modeled voters): {hd_high_risk_count} districts
        - **Medium Risk Districts** (25,000-50,000 modeled voters): {hd_medium_risk_count} districts
        - **Low Risk Districts** (<25,000 modeled voters): {150 - hd_high_risk_count - hd_medium_risk_count} districts

        **Modeled Voter Composition:**
        - **Likely Republican**: {hd_total_rep_modeled:,}
        - **Likely Democrat**: {hd_total_dem_modeled:,}
        - **Swing**: {hd_total_swing_modeled:,}
        - **Net Advantage**: {hd_net_advantage_modeled:+,} (Republican)

        **Critical Insight**: {hd_high_risk_count} House districts have over 50,000 "secret" voters who have 
        voted in general elections but never in primaries. Their party preference is unknown, and if they turn out, 
        they could dramatically shift election outcomes, especially in districts currently classified as competitive.
        """),
    ])
    return


@app.cell
def _(cd_new_modeled):
    # Analyze Congressional Districts - Modeled Voters
    if cd_new_modeled is not None:
        cd_modeled_pd = cd_new_modeled.to_pandas()

        cd_modeled_pd["total_modeled"] = cd_modeled_pd["Republican"] + cd_modeled_pd["Democrat"] + cd_modeled_pd.get("Swing", 0)
        cd_modeled_pd["net_advantage_modeled"] = cd_modeled_pd["Republican"] - cd_modeled_pd["Democrat"]

        # Risk levels for CD
        cd_modeled_pd["risk_level"] = "Low"
        cd_modeled_pd.loc[cd_modeled_pd["total_modeled"] > 200000, "risk_level"] = "High"
        cd_modeled_pd.loc[(cd_modeled_pd["total_modeled"] > 100000) & (cd_modeled_pd["total_modeled"] <= 200000), "risk_level"] = "Medium"

        cd_high_risk_count = len(cd_modeled_pd[cd_modeled_pd["risk_level"] == "High"])
        cd_medium_risk_count = len(cd_modeled_pd[cd_modeled_pd["risk_level"] == "Medium"])

        cd_total_rep_modeled = cd_modeled_pd["Republican"].sum()
        cd_total_dem_modeled = cd_modeled_pd["Democrat"].sum()
        cd_total_swing_modeled = cd_modeled_pd.get("Swing", pd.Series([0] * len(cd_modeled_pd))).sum()
        cd_net_advantage_modeled = cd_total_rep_modeled - cd_total_dem_modeled
    else:
        cd_modeled_pd = None
        cd_high_risk_count = 0
        cd_medium_risk_count = 0
        cd_total_rep_modeled = 0
        cd_total_dem_modeled = 0
        cd_total_swing_modeled = 0
        cd_net_advantage_modeled = 0

    return (
        cd_high_risk_count,
        cd_medium_risk_count,
        cd_net_advantage_modeled,
        cd_total_dem_modeled,
        cd_total_rep_modeled,
        cd_total_swing_modeled,
    )


@app.cell
def _(
    cd_high_risk_count,
    cd_medium_risk_count,
    cd_net_advantage_modeled,
    cd_total_dem_modeled,
    cd_total_rep_modeled,
    cd_total_swing_modeled,
    mo,
):
    # Display CD Modeled Analysis
    mo.vstack([
        mo.md(f"""
        ### Congressional Districts (CD) - Modeled Voters Analysis

        **Hidden Voter Risk:**
        - **High Risk Districts** (>200,000 modeled voters): {cd_high_risk_count} districts
        - **Medium Risk Districts** (100,000-200,000 modeled voters): {cd_medium_risk_count} districts
        - **Low Risk Districts** (<100,000 modeled voters): {38 - cd_high_risk_count - cd_medium_risk_count} districts

        **Modeled Voter Composition:**
        - **Likely Republican**: {cd_total_rep_modeled:,}
        - **Likely Democrat**: {cd_total_dem_modeled:,}
        - **Swing**: {cd_total_swing_modeled:,}
        - **Net Advantage**: {cd_net_advantage_modeled:+,} (Republican)

        **Critical Insight**: {cd_high_risk_count} Congressional districts have over 200,000 "secret" voters who have 
        voted in general elections but never in primaries. These districts are highly vulnerable to shifts if these 
        voters turn out in significant numbers.
        """),
    ])
    return


@app.cell
def _(sd_new_modeled):
    # Analyze State Senate Districts - Modeled Voters
    if sd_new_modeled is not None:
        sd_modeled_pd = sd_new_modeled.to_pandas()

        sd_modeled_pd["total_modeled"] = sd_modeled_pd["Republican"] + sd_modeled_pd["Democrat"] + sd_modeled_pd.get("Swing", 0)
        sd_modeled_pd["net_advantage_modeled"] = sd_modeled_pd["Republican"] - sd_modeled_pd["Democrat"]

        # Risk levels for SD
        sd_modeled_pd["risk_level"] = "Low"
        sd_modeled_pd.loc[sd_modeled_pd["total_modeled"] > 300000, "risk_level"] = "High"
        sd_modeled_pd.loc[(sd_modeled_pd["total_modeled"] > 150000) & (sd_modeled_pd["total_modeled"] <= 300000), "risk_level"] = "Medium"

        sd_high_risk_count = len(sd_modeled_pd[sd_modeled_pd["risk_level"] == "High"])
        sd_medium_risk_count = len(sd_modeled_pd[sd_modeled_pd["risk_level"] == "Medium"])

        sd_total_rep_modeled = sd_modeled_pd["Republican"].sum()
        sd_total_dem_modeled = sd_modeled_pd["Democrat"].sum()
        sd_total_swing_modeled = sd_modeled_pd.get("Swing", pd.Series([0] * len(sd_modeled_pd))).sum()
        sd_net_advantage_modeled = sd_total_rep_modeled - sd_total_dem_modeled
    else:
        sd_modeled_pd = None
        sd_high_risk_count = 0
        sd_medium_risk_count = 0
        sd_total_rep_modeled = 0
        sd_total_dem_modeled = 0
        sd_total_swing_modeled = 0
        sd_net_advantage_modeled = 0

    return (
        sd_high_risk_count,
        sd_medium_risk_count,
        sd_net_advantage_modeled,
        sd_total_dem_modeled,
        sd_total_rep_modeled,
        sd_total_swing_modeled,
    )


@app.cell
def _(
    mo,
    sd_high_risk_count,
    sd_medium_risk_count,
    sd_net_advantage_modeled,
    sd_total_dem_modeled,
    sd_total_rep_modeled,
    sd_total_swing_modeled,
):
    # Display SD Modeled Analysis
    mo.vstack([
        mo.md(f"""
        ### State Senate Districts (SD) - Modeled Voters Analysis

        **Hidden Voter Risk:**
        - **High Risk Districts** (>300,000 modeled voters): {sd_high_risk_count} districts
        - **Medium Risk Districts** (150,000-300,000 modeled voters): {sd_medium_risk_count} districts
        - **Low Risk Districts** (<150,000 modeled voters): {31 - sd_high_risk_count - sd_medium_risk_count} districts

        **Modeled Voter Composition:**
        - **Likely Republican**: {sd_total_rep_modeled:,}
        - **Likely Democrat**: {sd_total_dem_modeled:,}
        - **Swing**: {sd_total_swing_modeled:,}
        - **Net Advantage**: {sd_net_advantage_modeled:+,} (Republican)

        **Critical Insight**: {sd_high_risk_count} State Senate districts have over 300,000 "secret" voters who have 
        voted in general elections but never in primaries. These large districts are particularly vulnerable to shifts 
        if these voters mobilize.
        """),
    ])
    return


@app.cell
def _(
    cd_new_known,
    cd_new_modeled,
    hd_new_known,
    hd_new_modeled,
    sd_new_known,
    sd_new_modeled,
):
    # Combined Analysis: Known + Modeled
    # This shows the full picture when both known and modeled voters are considered

    def create_combined_analysis(known_pd, modeled_pd, dist_type, total_districts):
        """Create combined analysis of known + modeled voters."""
        if known_pd is None or modeled_pd is None:
            return None

        # Get district column name
        dist_col_map = {"HD": "2026_HD", "CD": "2026_CD", "SD": "2026_SD"}
        dist_col = dist_col_map.get(dist_type, "2026_HD")

        # Ensure both have the district column
        if dist_col not in known_pd.columns:
            known_pd = known_pd.reset_index()
            if dist_col not in known_pd.columns:
                known_pd.columns = [dist_col] + [c for c in known_pd.columns if c != dist_col]

        if dist_col not in modeled_pd.columns:
            modeled_pd = modeled_pd.reset_index()
            if dist_col not in modeled_pd.columns:
                modeled_pd.columns = [dist_col] + [c for c in modeled_pd.columns if c != dist_col]

        # Merge known and modeled on district
        combined = known_pd.merge(
            modeled_pd,
            on=dist_col,
            how="outer",
            suffixes=("_known", "_modeled")
        ).fillna(0)

        # Calculate combined totals
        rep_known_col = "Republican_known" if "Republican_known" in combined.columns else "Republican"
        dem_known_col = "Democrat_known" if "Democrat_known" in combined.columns else "Democrat"
        rep_modeled_col = "Republican_modeled" if "Republican_modeled" in combined.columns else "Republican"
        dem_modeled_col = "Democrat_modeled" if "Democrat_modeled" in combined.columns else "Democrat"

        combined["rep_total"] = combined.get(rep_known_col, 0) + combined.get(rep_modeled_col, 0)
        combined["dem_total"] = combined.get(dem_known_col, 0) + combined.get(dem_modeled_col, 0)
        combined["net_advantage_combined"] = combined["rep_total"] - combined["dem_total"]

        # Classify districts
        threshold_map = {"HD": 1000, "CD": 5000, "SD": 10000}
        threshold = threshold_map.get(dist_type, 1000)

        combined["classification"] = "Competitive"
        combined.loc[combined["net_advantage_combined"] > threshold, "classification"] = "Republican"
        combined.loc[combined["net_advantage_combined"] < -threshold, "classification"] = "Democrat"

        rep_advantage = len(combined[combined["classification"] == "Republican"])
        dem_advantage = len(combined[combined["classification"] == "Democrat"])
        competitive = len(combined[combined["classification"] == "Competitive"])

        return {
            "rep_advantage": rep_advantage,
            "dem_advantage": dem_advantage,
            "competitive": competitive,
            "combined": combined
        }

    # Create combined analyses
    hd_combined = create_combined_analysis(
        hd_new_known.to_pandas() if hd_new_known is not None else None,
        hd_new_modeled.to_pandas() if hd_new_modeled is not None else None,
        "HD", 150
    )
    cd_combined = create_combined_analysis(
        cd_new_known.to_pandas() if cd_new_known is not None else None,
        cd_new_modeled.to_pandas() if cd_new_modeled is not None else None,
        "CD", 38
    )
    sd_combined = create_combined_analysis(
        sd_new_known.to_pandas() if sd_new_known is not None else None,
        sd_new_modeled.to_pandas() if sd_new_modeled is not None else None,
        "SD", 31
    )

    # Create summary text
    hd_text = ""
    if hd_combined:
        hd_text = f"""
        **House Districts (HD):**
        - Republican Advantage: {hd_combined['rep_advantage']} ({hd_combined['rep_advantage']/150*100:.1f}%)
        - Democrat Advantage: {hd_combined['dem_advantage']} ({hd_combined['dem_advantage']/150*100:.1f}%)
        - Competitive: {hd_combined['competitive']} ({hd_combined['competitive']/150*100:.1f}%)"""
    else:
        hd_text = "\n        **House Districts (HD):** N/A"

    cd_text = ""
    if cd_combined:
        cd_text = f"""
        **Congressional Districts (CD):**
        - Republican Advantage: {cd_combined['rep_advantage']} ({cd_combined['rep_advantage']/38*100:.1f}%)
        - Democrat Advantage: {cd_combined['dem_advantage']} ({cd_combined['dem_advantage']/38*100:.1f}%)
        - Competitive: {cd_combined['competitive']} ({cd_combined['competitive']/38*100:.1f}%)"""
    else:
        cd_text = "\n        **Congressional Districts (CD):** N/A"

    sd_text = ""
    if sd_combined:
        sd_text = f"""
        **State Senate Districts (SD):**
        - Republican Advantage: {sd_combined['rep_advantage']} ({sd_combined['rep_advantage']/31*100:.1f}%)
        - Democrat Advantage: {sd_combined['dem_advantage']} ({sd_combined['dem_advantage']/31*100:.1f}%)
        - Competitive: {sd_combined['competitive']} ({sd_combined['competitive']/31*100:.1f}%)"""
    else:
        sd_text = "\n        **State Senate Districts (SD):** N/A"

    return cd_combined, cd_text, hd_combined, hd_text, sd_combined, sd_text


@app.cell
def _(cd_text, hd_text, mo, sd_text):
    # Display Combined Summary
    mo.vstack([
        mo.md(f"""
        ### Combined Analysis: Known + Modeled Voters
        {hd_text}
        {cd_text}
        {sd_text}
        """),
    ])
    return


@app.cell
def _(
    cd_combined,
    cd_new_known,
    cd_new_modeled,
    hd_combined,
    hd_new_known,
    hd_new_modeled,
    sd_combined,
    sd_new_known,
    sd_new_modeled,
):
    # Identify High-Risk Competitive Districts
    # Districts that are competitive based on known voters but have high numbers of modeled voters

    def identify_high_risk_competitive(known_pd, modeled_pd, combined_dict, dist_type, dist_col_name):
        """Identify competitive districts with high risk from non-primary voters."""
        if known_pd is None or modeled_pd is None or combined_dict is None:
            return None

        combined = combined_dict["combined"]

        # Get modeled voter counts
        if dist_col_name in modeled_pd.columns:
            modeled_counts = modeled_pd[[dist_col_name, "Republican", "Democrat"]].copy()
            modeled_counts["total_modeled"] = modeled_counts["Republican"] + modeled_counts["Democrat"]
        else:
            return None

        # Merge with combined classification
        risk_analysis = combined.merge(
            modeled_counts,
            on=dist_col_name,
            how="left"
        ).fillna(0)

        # Identify competitive districts with high modeled voter counts
        competitive_threshold_map = {"HD": 25000, "CD": 100000, "SD": 150000}
        competitive_threshold = competitive_threshold_map.get(dist_type, 25000)

        high_risk_competitive = risk_analysis[
            (risk_analysis["classification"] == "Competitive") &
            (risk_analysis["total_modeled"] > competitive_threshold)
        ].sort_values("total_modeled", ascending=False)

        return high_risk_competitive

    # Identify high-risk competitive districts
    hd_high_risk = identify_high_risk_competitive(
        hd_new_known.to_pandas() if hd_new_known is not None else None,
        hd_new_modeled.to_pandas() if hd_new_modeled is not None else None,
        hd_combined,
        "HD",
        "2026_HD"
    )

    cd_high_risk = identify_high_risk_competitive(
        cd_new_known.to_pandas() if cd_new_known is not None else None,
        cd_new_modeled.to_pandas() if cd_new_modeled is not None else None,
        cd_combined,
        "CD",
        "2026_CD"
    )

    sd_high_risk = identify_high_risk_competitive(
        sd_new_known.to_pandas() if sd_new_known is not None else None,
        sd_new_modeled.to_pandas() if sd_new_modeled is not None else None,
        sd_combined,
        "SD",
        "2026_SD"
    )

    # Create summary
    hd_risk_text = ""
    if hd_high_risk is not None and len(hd_high_risk) > 0:
        hd_risk_count = len(hd_high_risk)
        hd_risk_text = f"""
        **High-Risk Competitive House Districts**: {hd_risk_count} districts

        These districts are:
        - Currently competitive based on known primary voters
        - Have >25,000 modeled non-primary voters
        - Highly vulnerable to shifts if non-primary voters turn out

        **Top 5 High-Risk Competitive HD Districts:**
        """
        for idx, row in hd_high_risk.head(5).iterrows():
            dist = row["2026_HD"]
            hd_dist_total_modeled = row["total_modeled"]
            hd_risk_text += f"\n        - **HD {int(dist)}**: {hd_dist_total_modeled:,.0f} modeled voters"
    else:
        hd_risk_text = "\n        **High-Risk Competitive Districts**: Analysis not available"

    return (hd_risk_text,)


@app.cell
def _(hd_risk_text, mo):
    # Display High-Risk Competitive Districts
    mo.vstack([
        mo.md(f"""
        ## High-Risk Competitive Districts

        These districts are **competitive based on known primary voters** but have **high numbers of 
        modeled non-primary voters**, making them vulnerable to shifts if these "secret" voters turn out.

        {hd_risk_text}

        **Strategic Implication**: These districts should be priority targets for get-out-the-vote efforts, 
        as mobilizing non-primary voters could tip the balance in either direction.
        """),
    ])
    return


@app.cell
def _(mo):
    mo.md(r"""
    ---

    # Strategic Implications

    ## Opportunities and Vulnerabilities by Party

    ### Republican Opportunities
    1. **Known Primary Voters**: Strong base with majority advantage in most districts
    2. **Modeled Voters**: Large pool of likely Republican voters in high-risk districts
    3. **Turnout Strategy**: Focus on mobilizing modeled voters in competitive districts

    ### Democrat Opportunities
    1. **Hidden Voter Pool**: Large number of modeled Democrat voters who haven't voted in primaries
    2. **Competitive Districts**: Many districts are competitive when modeled voters are included
    3. **Turnout Strategy**: Mobilize non-primary voters, especially in medium-to-high risk districts

    ### Key Vulnerabilities

    **Both Parties Face:**
    - **High-Risk Districts**: Districts with large numbers of non-primary voters are unpredictable
    - **Turnout Uncertainty**: If non-primary voters turn out in significant numbers, current district classifications could shift dramatically
    - **Redistricting Impact**: The new 2026 boundaries create new competitive opportunities that weren't present in the old maps

    **Recommendation**: Focus GOTV efforts on districts with high modeled voter counts, especially those
    currently classified as competitive based on known primary voters.
    """)
    return


if __name__ == "__main__":
    app.run()
