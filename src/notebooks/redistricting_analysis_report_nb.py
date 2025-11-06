import marimo

__generated_with = "0.17.7"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return mo


@app.cell
def _():
    mo.md(r"""
    # Texas Redistricting Analysis - 2026 Election Results
    ## Methodological Report

    This analysis examines how redistricting in Texas (from 2022/2024 boundaries to 2026 boundaries)
    affects party composition across three types of districts: State Senate (SD), Congressional (CD), and House (HD).
    """)


@app.cell
def _():
    # Imports
    import polars as pl
    import pandas as pd
    import geopandas as gpd
    from pathlib import Path
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend for marimo
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
    
    return pl, pd, gpd, Path, plt, sns


@app.cell
def _():
    mo.md(r"""
    ## Purpose
    
    This analysis examines how redistricting in Texas (from 2022/2024 boundaries to 2026 boundaries)
    affects party composition across three types of districts:
    - **State Senate Districts (SD)**: 31 districts
    - **Congressional Districts (CD)**: 38 districts  
    - **House Districts (HD)**: 150 districts

    ### Key Questions
    1. How does redistricting affect Republican vs Democrat voter counts in each district?
    2. Which districts gain or lose Republican/Democrat voters due to boundary changes?
    3. What is the net impact of redistricting on party representation?

    ### Analysis Scope
    - **Time Period**: Comparing OLD districts (2022/2024 boundaries) to NEW districts (2026 boundaries)
    - **Data Coverage**: All registered voters in Texas (18.6+ million voters)
    - **Party Assignment**: Based on primary party registration (PRI24/PRI22), not voting behavior
    """)


@app.cell
def _():
    mo.md(r"""
    ## Data Sources

    ### 1. Voter File (November 2024)
    - **Source**: Texas Secretary of State voter registration file
    - **File**: `texasnovember2024.csv`
    - **Records**: ~18.6 million registered voters
    - **Key Fields**:
      - `VUID`: Unique voter identifier
      - `COUNTY`: County name
      - `PCT`: Precinct code
      - `NEWSD`, `NEWCD`, `NEWHD`: OLD district assignments (2022/2024 boundaries)
      - `PRI24`: 2024 Primary party affiliation
      - `PRI22`: 2022 Primary party affiliation (fallback)

    ### 2. Early Voting Data
    - **Source**: Early voting records from Texas counties
    - **Records**: ~1.4 million early voters (7.37% of all voters)
    - **Note**: Used to mark who voted early but does NOT determine party affiliation

    ### 3. Shapefiles
    - **OLD Districts (2022)**: U.S. Census Bureau shapefiles
    - **NEW Districts (2026)**: Texas Legislative Council shapefiles
    - **Precincts (2024)**: Used for spatial matching
    """)


@app.cell
def _():
    mo.md(r"""
    ## Methodology: Spatial Matching
    
    ### Precinct-to-District Assignment
    1. Load 2024 precinct shapefile and 2026 district shapefile
    2. Perform spatial intersection (spatial join)
    3. For split precincts, assign to district with largest overlap
    4. Map COUNTY (name) + PCT (code) to CNTY (code) + PREC (code)
    5. Assign 2026_SD, 2026_CD, 2026_HD to each voter

    ### Methodology: Party Gains/Losses
    1. Calculate party composition in OLD districts
    2. Calculate party composition in NEW districts
    3. Identify voter transitions between districts
    4. Calculate expected composition using weighted average of contributing OLD districts
    5. Calculate net gains/losses: Actual - Expected
    """)


@app.cell
def _(Path):
    # Check if analysis files exist, run main.py if needed
    import subprocess
    import sys
    import shutil
    
    required_files = [
        "early_voting_merged.parquet",
        "sd_districts/party_gains_losses_by_district.csv",
        "cd_districts/party_gains_losses_by_district.csv",
        "hd_districts/party_gains_losses_by_district.csv",
        "party_gains_losses_by_county.csv"
    ]
    
    missing_files = [f for f in required_files if not Path(f).exists()]
    
    if missing_files:
        print("⚠️  Missing required analysis files:")
        for f in missing_files:
            print(f"   - {f}")
        print("\n🔄 Running main.py to generate analysis files...")
        print("   This may take several minutes...")
        try:
            # Use uv run if available, otherwise use sys.executable
            if shutil.which("uv"):
                cmd = ["uv", "run", "python", "main.py"]
            else:
                cmd = [sys.executable, "main.py"]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            if result.returncode == 0:
                print("✅ Analysis files generated successfully!")
                # Print any output from main.py
                if result.stdout:
                    print("\nOutput from main.py:")
                    print(result.stdout[-500:])  # Last 500 chars
            else:
                print(f"❌ Error running main.py (exit code: {result.returncode})")
                if result.stderr:
                    print("Error output:")
                    print(result.stderr[-1000:])  # Last 1000 chars
                if result.stdout:
                    print("\nStandard output:")
                    print(result.stdout[-1000:])  # Last 1000 chars
                print("\n⚠️  Please run 'uv run python main.py' manually to generate the analysis files.")
        except subprocess.TimeoutExpired:
            print("❌ main.py timed out after 1 hour.")
            print("⚠️  Please run 'uv run python main.py' manually to generate the analysis files.")
        except Exception as e:
            print(f"❌ Error running main.py: {e}")
            print("⚠️  Please run 'uv run python main.py' manually to generate the analysis files.")
    else:
        print("✅ All required analysis files found!")
    
    return


@app.cell
def _():
    mo.md(r"""
    ## State Senate Districts (SD) Analysis
    """)


@app.cell
def _():
    # Load SD results
    sd_file = Path("sd_districts/party_gains_losses_by_district.csv")
    
    if sd_file.exists():
        sd_results = pd.read_csv(sd_file)

    print("=" * 80)
    print("STATE SENATE DISTRICTS - SUMMARY")
    print("=" * 80)
    print(f"\nTotal Districts: {len(sd_results)}")
        print(f"Total Republican Change: {sd_results['net_republican_change'].sum():+,.0f} voters")
        print(f"Total Democrat Change: {sd_results['net_democrat_change'].sum():+,.0f} voters")
        print(f"Net Advantage (Rep - Dem): {(sd_results['net_republican_change'].sum() - sd_results['net_democrat_change'].sum()):+,.0f} voters")

        sd_rep_favored = len(sd_results[sd_results['net_republican_change'] > sd_results['net_democrat_change']])
        sd_dem_favored = len(sd_results[sd_results['net_democrat_change'] > sd_results['net_republican_change']])

    print(f"\nDistricts Favored:")
        print(f"  Republican: {sd_rep_favored} ({sd_rep_favored/len(sd_results)*100:.1f}%)")
        print(f"  Democrat: {sd_dem_favored} ({sd_dem_favored/len(sd_results)*100:.1f}%)")
    else:
        print("⚠️  SD results file not found. Run main.py to generate analysis.")
        sd_results = None
    return sd_file, sd_results


@app.cell
def _():
    # SD Visualization
    if sd_results is not None:
        sd_fig, (sd_ax1, sd_ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # Chart 1: Net changes by district
        sd_sorted_viz = sd_results.sort_values('net_republican_change', ascending=False)
        sd_x_pos = range(len(sd_sorted_viz))
        
        sd_ax1.barh(sd_x_pos, sd_sorted_viz['net_republican_change'], label='Republican Change', color='red', alpha=0.7)
        sd_ax1.barh(sd_x_pos, sd_sorted_viz['net_democrat_change'], label='Democrat Change', color='blue', alpha=0.7)
        sd_ax1.set_yticks(sd_x_pos)
        sd_ax1.set_yticklabels([f"SD {int(d)}" for d in sd_sorted_viz['district']], fontsize=8)
        sd_ax1.set_xlabel('Net Voter Change', fontsize=12)
        sd_ax1.set_title('State Senate Districts: Party Gains/Losses', fontsize=14, fontweight='bold')
        sd_ax1.legend()
        sd_ax1.grid(axis='x', alpha=0.3)
        
        # Chart 2: Net advantage scatter
        sd_net_rep_adv = sd_results['net_republican_change'] - sd_results['net_democrat_change']
        sd_ax2.scatter(sd_results['net_republican_change'], sd_results['net_democrat_change'], 
                   c=sd_net_rep_adv, cmap='RdYlBu_r', s=100, alpha=0.6)
        sd_ax2.axhline(0, color='black', linestyle='--', alpha=0.3)
        sd_ax2.axvline(0, color='black', linestyle='--', alpha=0.3)
        sd_ax2.set_xlabel('Net Republican Change', fontsize=12)
        sd_ax2.set_ylabel('Net Democrat Change', fontsize=12)
        sd_ax2.set_title('State Senate: Republican vs Democrat Changes', fontsize=14, fontweight='bold')
        sd_ax2.grid(alpha=0.3)

    plt.tight_layout()
        sd_fig
    return


@app.cell
def _():
    mo.md(r"""
    ## Congressional Districts (CD) Analysis
    """)


@app.cell
def _():
    # Load CD results
    cd_file = Path("cd_districts/party_gains_losses_by_district.csv")
    
    if cd_file.exists():
        cd_results = pd.read_csv(cd_file)
        
        print("=" * 80)
        print("CONGRESSIONAL DISTRICTS - SUMMARY")
        print("=" * 80)
        print(f"\nTotal Districts: {len(cd_results)}")
        print(f"Total Republican Change: {cd_results['net_republican_change'].sum():+,.0f} voters")
        print(f"Total Democrat Change: {cd_results['net_democrat_change'].sum():+,.0f} voters")
        print(f"Net Advantage (Rep - Dem): {(cd_results['net_republican_change'].sum() - cd_results['net_democrat_change'].sum()):+,.0f} voters")
        
        cd_rep_favored = len(cd_results[cd_results['net_republican_change'] > cd_results['net_democrat_change']])
        cd_dem_favored = len(cd_results[cd_results['net_democrat_change'] > cd_results['net_republican_change']])
        
        print(f"\nDistricts Favored:")
        print(f"  Republican: {cd_rep_favored} ({cd_rep_favored/len(cd_results)*100:.1f}%)")
        print(f"  Democrat: {cd_dem_favored} ({cd_dem_favored/len(cd_results)*100:.1f}%)")
    else:
        print("⚠️  CD results file not found. Run main.py to generate analysis.")
        cd_results = None
    return cd_file, cd_results


@app.cell
def _():
    # CD Visualization
    if cd_results is not None:
        cd_fig, (cd_ax1, cd_ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # Chart 1: Net changes by district
        cd_sorted_viz = cd_results.sort_values('net_republican_change', ascending=False)
        cd_x_pos = range(len(cd_sorted_viz))
        
        cd_ax1.barh(cd_x_pos, cd_sorted_viz['net_republican_change'], label='Republican Change', color='red', alpha=0.7)
        cd_ax1.barh(cd_x_pos, cd_sorted_viz['net_democrat_change'], label='Democrat Change', color='blue', alpha=0.7)
        cd_ax1.set_yticks(cd_x_pos)
        cd_ax1.set_yticklabels([f"CD {int(d)}" for d in cd_sorted_viz['district']], fontsize=8)
        cd_ax1.set_xlabel('Net Voter Change', fontsize=12)
        cd_ax1.set_title('Congressional Districts: Party Gains/Losses', fontsize=14, fontweight='bold')
        cd_ax1.legend()
        cd_ax1.grid(axis='x', alpha=0.3)
        
        # Chart 2: Net advantage scatter
        cd_net_rep_adv = cd_results['net_republican_change'] - cd_results['net_democrat_change']
        cd_ax2.scatter(cd_results['net_republican_change'], cd_results['net_democrat_change'], 
                   c=cd_net_rep_adv, cmap='RdYlBu_r', s=100, alpha=0.6)
        cd_ax2.axhline(0, color='black', linestyle='--', alpha=0.3)
        cd_ax2.axvline(0, color='black', linestyle='--', alpha=0.3)
        cd_ax2.set_xlabel('Net Republican Change', fontsize=12)
        cd_ax2.set_ylabel('Net Democrat Change', fontsize=12)
        cd_ax2.set_title('Congressional: Republican vs Democrat Changes', fontsize=14, fontweight='bold')
        cd_ax2.grid(alpha=0.3)
        
        plt.tight_layout()
        cd_fig
    return


@app.cell
def _():
    mo.md(r"""
    ## House Districts (HD) Analysis
    """)


@app.cell
def _():
    # Load HD results
    hd_file = Path("hd_districts/party_gains_losses_by_district.csv")
    
    if hd_file.exists():
        hd_results = pd.read_csv(hd_file)
        
        print("=" * 80)
        print("HOUSE DISTRICTS - SUMMARY")
        print("=" * 80)
        print(f"\nTotal Districts: {len(hd_results)}")
        print(f"Total Republican Change: {hd_results['net_republican_change'].sum():+,.0f} voters")
        print(f"Total Democrat Change: {hd_results['net_democrat_change'].sum():+,.0f} voters")
        print(f"Net Advantage (Rep - Dem): {(hd_results['net_republican_change'].sum() - hd_results['net_democrat_change'].sum()):+,.0f} voters")
        
        hd_rep_favored = len(hd_results[hd_results['net_republican_change'] > hd_results['net_democrat_change']])
        hd_dem_favored = len(hd_results[hd_results['net_democrat_change'] > hd_results['net_republican_change']])
        
        print(f"\nDistricts Favored:")
        print(f"  Republican: {hd_rep_favored} ({hd_rep_favored/len(hd_results)*100:.1f}%)")
        print(f"  Democrat: {hd_dem_favored} ({hd_dem_favored/len(hd_results)*100:.1f}%)")
    else:
        print("⚠️  HD results file not found. Run main.py to generate analysis.")
        hd_results = None
    return hd_file, hd_results


@app.cell
def _():
    # HD Visualization
    if hd_results is not None:
        hd_fig, (hd_ax1, hd_ax2) = plt.subplots(1, 2, figsize=(16, 10))
        
        # Chart 1: Net changes by district (top 50)
        hd_sorted_viz = hd_results.sort_values('net_republican_change', ascending=False).head(50)
        hd_x_pos = range(len(hd_sorted_viz))
        
        hd_ax1.barh(hd_x_pos, hd_sorted_viz['net_republican_change'], label='Republican Change', color='red', alpha=0.7)
        hd_ax1.barh(hd_x_pos, hd_sorted_viz['net_democrat_change'], label='Democrat Change', color='blue', alpha=0.7)
        hd_ax1.set_yticks(hd_x_pos)
        hd_ax1.set_yticklabels([f"HD {int(d)}" for d in hd_sorted_viz['district']], fontsize=7)
        hd_ax1.set_xlabel('Net Voter Change', fontsize=12)
        hd_ax1.set_title('House Districts: Top 50 Party Gains/Losses', fontsize=14, fontweight='bold')
        hd_ax1.legend()
        hd_ax1.grid(axis='x', alpha=0.3)
        
        # Chart 2: Net advantage scatter
        hd_net_rep_adv = hd_results['net_republican_change'] - hd_results['net_democrat_change']
        hd_ax2.scatter(hd_results['net_republican_change'], hd_results['net_democrat_change'], 
                   c=hd_net_rep_adv, cmap='RdYlBu_r', s=50, alpha=0.6)
        hd_ax2.axhline(0, color='black', linestyle='--', alpha=0.3)
        hd_ax2.axvline(0, color='black', linestyle='--', alpha=0.3)
        hd_ax2.set_xlabel('Net Republican Change', fontsize=12)
        hd_ax2.set_ylabel('Net Democrat Change', fontsize=12)
        hd_ax2.set_title('House Districts: Republican vs Democrat Changes', fontsize=14, fontweight='bold')
        hd_ax2.grid(alpha=0.3)
        
        plt.tight_layout()
        hd_fig
    return


@app.cell
def _():
    mo.md(r"""
    ## County Analysis
    """)


@app.cell
def _():
    # Load county results
    county_file = Path("party_gains_losses_by_county.csv")
    
    if county_file.exists():
        county_results = pd.read_csv(county_file)
        
        print("=" * 80)
        print("COUNTY ANALYSIS - SUMMARY")
        print("=" * 80)
        print(f"\nTotal Counties: {len(county_results)}")
        print(f"Total Republican Change: {county_results['Net_Republican_Change'].sum():+,.0f} voters")
        print(f"Total Democrat Change: {county_results['Net_Democrat_Change'].sum():+,.0f} voters")
        print(f"Net Advantage (Rep - Dem): {(county_results['Net_Republican_Change'].sum() - county_results['Net_Democrat_Change'].sum()):+,.0f} voters")
        
        rep_gain = len(county_results[county_results['Net_Republican_Change'] > 0])
        dem_gain = len(county_results[county_results['Net_Democrat_Change'] > 0])
        
        print(f"\nCounties with Gains:")
        print(f"  Republican: {rep_gain} ({rep_gain/len(county_results)*100:.1f}%)")
        print(f"  Democrat: {dem_gain} ({dem_gain/len(county_results)*100:.1f}%)")
    else:
        print("⚠️  County results file not found. Run main.py to generate analysis.")
        county_results = None
    return county_file, county_results


@app.cell
def _():
    # County Visualization
    if county_results is not None:
        county_fig, (county_ax1, county_ax2) = plt.subplots(1, 2, figsize=(16, 10))
        
        # Chart 1: Top counties by net change
        county_sorted_viz = county_results.sort_values('Net_Republican_Change', ascending=False).head(30)
        county_x_pos = range(len(county_sorted_viz))
        
        county_ax1.barh(county_x_pos, county_sorted_viz['Net_Republican_Change'], label='Republican Change', color='red', alpha=0.7)
        county_ax1.barh(county_x_pos, county_sorted_viz['Net_Democrat_Change'], label='Democrat Change', color='blue', alpha=0.7)
        county_ax1.set_yticks(county_x_pos)
        county_ax1.set_yticklabels(county_sorted_viz['County'], fontsize=8)
        county_ax1.set_xlabel('Net Voter Change', fontsize=12)
        county_ax1.set_title('Top 30 Counties: Party Gains/Losses', fontsize=14, fontweight='bold')
        county_ax1.legend()
        county_ax1.grid(axis='x', alpha=0.3)
        
        # Chart 2: Scatter plot
        county_net_rep_adv = county_results['Net_Republican_Change'] - county_results['Net_Democrat_Change']
        county_ax2.scatter(county_results['Net_Republican_Change'], county_results['Net_Democrat_Change'], 
                   c=county_net_rep_adv, cmap='RdYlBu_r', s=50, alpha=0.6)
        county_ax2.axhline(0, color='black', linestyle='--', alpha=0.3)
        county_ax2.axvline(0, color='black', linestyle='--', alpha=0.3)
        county_ax2.set_xlabel('Net Republican Change', fontsize=12)
        county_ax2.set_ylabel('Net Democrat Change', fontsize=12)
        county_ax2.set_title('Counties: Republican vs Democrat Changes', fontsize=14, fontweight='bold')
        county_ax2.grid(alpha=0.3)
        
        plt.tight_layout()
        county_fig
    return


@app.cell
def _():
    mo.md(r"""
    ## Comprehensive Summary
    
    ### All District Types Comparison
    
    This section provides an overview of gains/losses across all district types.
    """)


@app.cell
def _(sd_results, cd_results, hd_results):
    # Comprehensive summary table
    summary_data = []
    
    # SD Summary
    try:
        if sd_results is not None:
            summary_data.append({
                "District Type": "State Senate (SD)",
                "Total Districts": len(sd_results),
                "Total Rep Change": f"{sd_results['net_republican_change'].sum():+,.0f}",
                "Total Dem Change": f"{sd_results['net_democrat_change'].sum():+,.0f}",
                "Net Advantage": f"{(sd_results['net_republican_change'].sum() - sd_results['net_democrat_change'].sum()):+,.0f}",
                "Rep Favored": f"{len(sd_results[sd_results['net_republican_change'] > sd_results['net_democrat_change']])} ({len(sd_results[sd_results['net_republican_change'] > sd_results['net_democrat_change']])/len(sd_results)*100:.1f}%)"
            })
    except NameError:
        pass
    
    # CD Summary
    try:
        if cd_results is not None:
            summary_data.append({
                "District Type": "Congressional (CD)",
                "Total Districts": len(cd_results),
                "Total Rep Change": f"{cd_results['net_republican_change'].sum():+,.0f}",
                "Total Dem Change": f"{cd_results['net_democrat_change'].sum():+,.0f}",
                "Net Advantage": f"{(cd_results['net_republican_change'].sum() - cd_results['net_democrat_change'].sum()):+,.0f}",
                "Rep Favored": f"{len(cd_results[cd_results['net_republican_change'] > cd_results['net_democrat_change']])} ({len(cd_results[cd_results['net_republican_change'] > cd_results['net_democrat_change']])/len(cd_results)*100:.1f}%)"
            })
    except NameError:
        pass
    
    # HD Summary
    try:
        if hd_results is not None:
            summary_data.append({
                "District Type": "House (HD)",
                "Total Districts": len(hd_results),
                "Total Rep Change": f"{hd_results['net_republican_change'].sum():+,.0f}",
                "Total Dem Change": f"{hd_results['net_democrat_change'].sum():+,.0f}",
                "Net Advantage": f"{(hd_results['net_republican_change'].sum() - hd_results['net_democrat_change'].sum()):+,.0f}",
                "Rep Favored": f"{len(hd_results[hd_results['net_republican_change'] > hd_results['net_democrat_change']])} ({len(hd_results[hd_results['net_republican_change'] > hd_results['net_democrat_change']])/len(hd_results)*100:.1f}%)"
            })
    except NameError:
        pass
    
    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        print("\n" + "=" * 100)
        print("COMPREHENSIVE SUMMARY - ALL DISTRICT TYPES")
        print("=" * 100)
        print("\n" + summary_df.to_string(index=False))
    else:
        print("⚠️  No district results available. Run main.py to generate analysis.")
    return


if __name__ == "__main__":
    app.run()
