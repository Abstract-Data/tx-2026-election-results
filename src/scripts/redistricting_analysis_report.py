"""
# Texas Redistricting Analysis - 2026 Election Results
# Methodological Report

This marimo notebook documents the complete methodology, data sources, and analysis
for comparing party composition changes due to redistricting in Texas.
"""

# ---
# imports
import polars as pl
import pandas as pd
import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

# ---
# title: Introduction and Overview

"""
# Texas Redistricting Analysis - 2026 Election Results
## Methodological Report

### Purpose
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
"""

# ---
# title: Data Sources

"""
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
  - `DOB`: Date of birth (for age calculation)
  - Address fields: `RHNUM`, `RSTNAME`, `RCITY`, `RZIP`

### 2. Early Voting Data
- **Source**: Early voting records from Texas counties
- **Directory**: `data/early_voting/`
- **Files**: Multiple CSV files with early voting records
- **Records**: ~1.4 million early voters (7.37% of all voters)
- **Key Fields**:
  - `id_voter`: Voter ID (matches VUID)
  - `tx_name`: County name
  - `voting_method`: Method of early voting
  - `precinct`: Precinct code
- **Note**: Early voting data is used to mark who voted early but does NOT determine party affiliation

### 3. Shapefiles - OLD Districts (2022 boundaries)
- **Congressional Districts**: `shapefiles/2022/congressional/tl_2022_48_cd118.shp`
  - Source: U.S. Census Bureau
  - District ID column: `CD118FP`
  
- **State Senate Districts**: `shapefiles/2022/state_senate/tl_2022_48_sldu.shp`
  - Source: U.S. Census Bureau
  - District ID column: `SLDUST`

### 4. Shapefiles - NEW Districts (2026 boundaries, 2023-2026 period)
- **Congressional Districts**: `shapefiles/2024/congressional/PLANC2193.shp`
  - District column: `District`
  
- **State Senate Districts**: `shapefiles/2024/texas_senate/PLANS2168.shp`
  - District column: `District`
  
- **House Districts**: `shapefiles/2024/texas_house/PLANH2316.shp`
  - District column: `District`

### 5. Precinct Shapefiles
- **2024 General Election Precincts**: `shapefiles/2024/general_precincts/Precincts24G.shp`
  - Used for spatial matching to assign 2026 districts to voters
  - Contains `CNTY` (county code) and `PREC` (precinct code)

### Data Quality Notes
- Party affiliation is based on primary registration, not actual voting behavior
- Not all voters have party affiliation (majority are "Unknown" or null)
- Early voting data represents only 7.37% of all registered voters
- Analysis uses ALL registered voters, not just early voters
"""

# ---
# title: Data Processing Pipeline

"""
## Data Processing Pipeline

### Step 1: Process Voter File
1. Read CSV file with schema overrides for text columns (PCT, ZIP codes)
2. Calculate age from date of birth
3. Create age brackets (18-24, 25-34, 35-44, 45-54, 55-64, 65-74, 75+)
4. Select relevant columns including COUNTY, PCT, NEWSD, NEWCD, NEWHD, PRI24, PRI22
5. Save as parquet: `processed_voterfile.parquet`

### Step 2: Process Early Voting Data
1. Read all CSV files from `data/early_voting/` directory
2. Handle schema issues (precinct codes as text)
3. Deduplicate by voter ID (keep most recent record)
4. Extract relevant columns (id_voter, tx_name, voting_method, precinct)
5. Save as parquet: `processed_early_voting.parquet`

### Step 3: Merge Voter File with Early Voting
1. Join on VUID (voter ID)
2. Create `voted_early` boolean flag
3. Map party codes (PRI24/PRI22) to party names:
   - "RE" → "Republican"
   - "DE" → "Democrat"
   - "LI" → "Libertarian"
   - "GR" → "Green"
   - "UN" → "Unaffiliated"
   - Others → "Unknown"
4. Use PRI24 as primary, fallback to PRI22
5. Save as parquet: `early_voting_merged.parquet`

### Step 4: Build Precinct-to-District Lookups
For each district type (SD, CD, HD):
1. Load 2024 precinct shapefile
2. Load 2026 district shapefile
3. Perform spatial intersection (spatial join)
4. For precincts that intersect multiple districts:
   - Calculate overlap area for each intersection
   - Assign precinct to district with largest overlap
5. Create lookup table: CNTY + PREC → 2026_District
6. Cache lookup tables as CSV files

### Step 5: Apply District Assignments
1. Match voterfile COUNTY (name) + PCT (code) to lookup CNTY (code) + PREC (code)
2. Build county name → code mapping by matching precinct codes
3. Join lookup to assign 2026_SD, 2026_CD, 2026_HD to each voter
4. Save updated merged data

### Step 6: Calculate Party Gains/Losses
For each district type:
1. Calculate party composition in OLD districts (group by NEWSD/NEWCD/NEWHD)
2. Calculate party composition in NEW districts (group by 2026_SD/2026_CD/2026_HD)
3. For each NEW district:
   - Identify OLD districts that contributed voters
   - Calculate weighted average party composition from contributing OLD districts
   - Compare actual NEW district composition to expected (weighted average)
   - Calculate net gains/losses for each party
4. Generate reports and visualizations
"""

# ---
# title: Methodology - Spatial Matching

"""
## Methodology: Spatial Matching for District Assignment

### Problem
The voterfile contains OLD district assignments (NEWSD, NEWCD, NEWHD) but not NEW district assignments.
We need to determine which NEW 2026 district each voter belongs to.

### Solution: Precinct-to-District Lookup via Spatial Intersection

#### Step 1: Spatial Intersection
1. Load 2024 precinct shapefile (contains precinct boundaries)
2. Load 2026 district shapefile (contains new district boundaries)
3. Ensure both are in the same coordinate reference system (CRS)
4. Perform spatial join using `geopandas.sjoin()` with `predicate='intersects'`

#### Step 2: Handle Split Precincts
Some precincts may span multiple districts. For these:
1. Calculate intersection area between precinct and each district
2. Assign precinct to the district with the **largest overlap area**
3. This ensures each precinct is assigned to exactly one district

#### Step 3: County Code Matching
The voterfile uses:
- `COUNTY`: County name (e.g., "HARRIS")
- `PCT`: Precinct code (e.g., "001")

The shapefile uses:
- `CNTY`: County code (numeric, e.g., 201)
- `PREC`: Precinct code (e.g., "001")

We build a mapping by:
1. Matching precinct codes (PCT == PREC) across voterfile and shapefile
2. For each COUNTY, find the most common CNTY code
3. Use this mapping to join lookup tables

#### Step 4: Apply Lookup
1. Map COUNTY → CNTY using the mapping
2. Join on CNTY + PCT/PREC to get 2026 district assignment
3. Assign to columns: `2026_SD`, `2026_CD`, `2026_HD`

### Advantages of This Approach
- **Accurate**: Uses actual geographic boundaries
- **Efficient**: Spatial operations are fast with GeoPandas
- **Scalable**: Handles 18+ million voters and 9,000+ precincts
- **No Rate Limits**: Unlike geocoding APIs
- **Precise**: Handles split precincts correctly

### Limitations
- Relies on 2024 precinct boundaries (2026 precincts not yet available)
- Assumes precinct boundaries haven't changed significantly
- County name matching may have edge cases
"""

# ---
# title: Methodology - Party Gains/Losses Calculation

"""
## Methodology: Party Gains/Losses Calculation

### Core Concept
Compare the **actual** party composition of a NEW district to the **expected** party composition
based on the weighted average of the OLD districts that contributed voters to it.

### Detailed Calculation

#### Step 1: Calculate Party Composition in OLD Districts
For each OLD district (e.g., NEWSD):
- Count voters by party (Republican, Democrat, Other)
- Calculate percentages
- Store as baseline

#### Step 2: Calculate Party Composition in NEW Districts
For each NEW district (e.g., 2026_SD):
- Count voters by party
- Calculate percentages
- This is the **actual** composition after redistricting

#### Step 3: Identify Voter Transitions
For each NEW district:
- Find all OLD districts that contributed voters
- Count how many voters moved from each OLD district to this NEW district
- Track party affiliation of transitioning voters

#### Step 4: Calculate Expected Composition (Weighted Average)
For each NEW district:
- Get the party composition of each contributing OLD district
- Weight by the **proportion of voters** from each OLD district
- Calculate weighted average:
  ```
  Expected_Rep_Pct = Σ(Old_Dist_Rep_Pct × Weight)
  Expected_Dem_Pct = Σ(Old_Dist_Dem_Pct × Weight)
  ```
  where Weight = (voters from old_dist / total voters in new_dist)

#### Step 5: Calculate Net Gains/Losses
For each NEW district:
```
Net_Republican_Change = Actual_Rep_Voters - Expected_Rep_Voters
Net_Democrat_Change = Actual_Dem_Voters - Expected_Dem_Voters
```

Where:
- `Actual_Rep_Voters` = actual count in NEW district
- `Expected_Rep_Voters` = (Expected_Rep_Pct / 100) × Total_Voters_in_New_Dist

### Interpretation
- **Positive Net Change**: District gained more voters of this party than expected
- **Negative Net Change**: District lost voters of this party relative to expected
- **Net Advantage**: Rep_Change - Dem_Change shows which party benefited more

### Why Weighted Average?
If a NEW district receives:
- 60% of voters from OLD District A (70% Rep)
- 40% of voters from OLD District B (30% Rep)

The expected composition should be:
- Expected Rep = 0.6 × 70% + 0.4 × 30% = 54%

This is more accurate than a simple average, which would give 50%.

### Data Used
- **Party Affiliation**: From PRI24/PRI22 (primary registration)
- **Voter Count**: ALL registered voters (not just early voters)
- **Total Voters**: 18.6+ million registered voters
"""

# ---
# title: Code Implementation

# Load configuration
data_dir = Path(".")
voterfile_path = data_dir / "processed_voterfile.parquet"
merged_path = data_dir / "early_voting_merged.parquet"

# Load data
print("Loading data...")
merged_df = pl.read_parquet(str(merged_path))
print(f"Loaded {len(merged_df):,} voters")

# Show data structure
print("\nData columns:")
print(merged_df.columns)

# Show party distribution
print("\nParty distribution:")
party_dist = merged_df.group_by("party").agg([
    pl.len().alias("count"),
    (pl.len() / pl.len().sum() * 100).alias("pct")
]).sort("party")
print(party_dist)

# ---
# title: Results Summary

# Load results
sd_results = pd.read_csv("sd_districts/party_gains_losses_by_district.csv")

print("=" * 80)
print("STATE SENATE DISTRICTS - SUMMARY")
print("=" * 80)
print(f"\nTotal Districts: {len(sd_results)}")
print(f"Total Republican Change: {sd_results['net_republican_change'].sum():+,.0f}")
print(f"Total Democrat Change: {sd_results['net_democrat_change'].sum():+,.0f}")
print(f"Net Advantage (Rep - Dem): {(sd_results['net_republican_change'].sum() - sd_results['net_democrat_change'].sum()):+,.0f}")

rep_favored = len(sd_results[sd_results['net_republican_change'] > sd_results['net_democrat_change']])
dem_favored = len(sd_results[sd_results['net_democrat_change'] > sd_results['net_republican_change']])

print(f"\nDistricts Favored:")
print(f"  Republican: {rep_favored} ({rep_favored/len(sd_results)*100:.1f}%)")
print(f"  Democrat: {dem_favored} ({dem_favored/len(sd_results)*100:.1f}%)")

# ---
# title: Visualizations

# Create visualization of party changes
fig, ax = plt.subplots(figsize=(12, 8))

sd_results_sorted = sd_results.sort_values('net_republican_change', ascending=False)

ax.barh(range(len(sd_results_sorted)), 
        sd_results_sorted['net_republican_change'], 
        label='Republican Change', color='red', alpha=0.7)
ax.barh(range(len(sd_results_sorted)), 
        sd_results_sorted['net_democrat_change'], 
        label='Democrat Change', color='blue', alpha=0.7)

ax.set_yticks(range(len(sd_results_sorted)))
ax.set_yticklabels([f"SD {int(d)}" for d in sd_results_sorted['district']])
ax.set_xlabel('Net Voter Change')
ax.set_title('Party Gains/Losses by State Senate District')
ax.legend()
ax.grid(axis='x', alpha=0.3)

plt.tight_layout()
plt.show()

# ---
# title: Key Findings

"""
## Key Findings

### State Senate Districts (SD)
- **38 districts** analyzed
- **Total Republican Change**: +32,130 voters
- **Total Democrat Change**: -30,202 voters
- **Net Republican Advantage**: +62,332 voters

### District Distribution
- **23 districts (60.5%)** favor Republicans
- **15 districts (39.5%)** favor Democrats

### Top Republican Gains
- District 27: +9,151 Rep, -7,547 Dem (net +16,697)
- District 4: +7,900 Rep, -7,464 Dem (net +15,364)
- District 9: +6,765 Rep, -4,815 Dem (net +11,580)

### Top Democrat Gains
- District 25: +7,211 Dem, -7,407 Rep (net +14,618 Dem)
- District 30: +5,572 Dem, -4,390 Rep (net +9,962 Dem)
- District 28: +4,356 Dem, -2,429 Rep (net +6,784 Dem)

### Interpretation
The redistricting shows a net advantage for Republicans in State Senate districts,
with more districts experiencing Republican gains than Democrat gains.
However, some districts show significant Democrat advantages, suggesting
the redistricting process created both competitive and non-competitive districts.
"""

# ---
# title: Limitations and Considerations

"""
## Limitations and Considerations

### Data Limitations
1. **Party Affiliation**: Based on primary registration, not actual voting behavior
   - Many voters (80%+) have no party affiliation
   - Party registration may not reflect general election voting patterns

2. **Precinct Boundaries**: Uses 2024 precincts to assign 2026 districts
   - 2026 precinct boundaries not yet available
   - Assumes precinct boundaries haven't changed significantly

3. **Voter Count**: Uses all registered voters, not just active voters
   - Includes inactive voters who may not vote
   - Does not account for voter turnout

4. **Early Voting**: Early voting data (7.37% of voters) is tracked but not used
   - Could be used for turnout analysis
   - Not used for party composition calculations

### Methodological Considerations
1. **Spatial Matching**: Precinct-to-district assignment may have edge cases
   - County name matching could fail for some counties
   - Split precincts are handled by largest overlap (may not be perfect)

2. **Weighted Average**: Assumes proportional representation
   - If voters from OLD districts are not representative, estimates may be off
   - Does not account for demographic changes between elections

3. **Comparison Method**: Compares to weighted average, not absolute counts
   - Focuses on relative changes, not absolute voter numbers
   - May not capture all redistricting effects

### Future Enhancements
1. Use actual 2026 precinct boundaries when available
2. Incorporate voter turnout data
3. Add demographic analysis (age, race, etc.)
4. Compare to actual election results
5. Analyze competitive districts in more detail
"""

# ---
# title: Conclusion

"""
## Conclusion

This analysis provides a comprehensive methodology for assessing the impact of redistricting
on party composition in Texas. By comparing OLD district boundaries (2022/2024) to NEW
district boundaries (2026), we can quantify how redistricting affects Republican and Democrat
voter representation.

### Key Takeaways
1. **Data-Driven**: Uses all registered voters (18.6+ million) for comprehensive analysis
2. **Spatial Accuracy**: Uses geographic boundaries for precise district assignment
3. **Methodologically Sound**: Weighted average approach accounts for voter transitions
4. **Transparent**: All steps documented and reproducible

### Outputs
- District-by-district party gains/losses
- Summary statistics by district type
- Visualizations and reports
- Detailed transition matrices

### Use Cases
- Redistricting impact assessment
- Electoral competitiveness analysis
- Voter representation analysis
- Policy and research applications
"""

# ---
# title: Appendix - File Structure

"""
## Appendix: File Structure

### Input Files
```
data/
├── voterfiles/
│   └── texasnovember2024.csv
├── early_voting/
│   └── *.csv (multiple files)
└── shapefiles/
    ├── 2022/
    │   ├── congressional/tl_2022_48_cd118.shp
    │   └── state_senate/tl_2022_48_sldu.shp
    ├── 2024/
    │   ├── congressional/PLANC2193.shp
    │   ├── texas_senate/PLANS2168.shp
    │   ├── texas_house/PLANH2316.shp
    │   └── general_precincts/Precincts24G.shp
    └── 2026/
        └── PLANC2333.shp
```

### Output Files
```
.
├── processed_voterfile.parquet
├── processed_early_voting.parquet
├── early_voting_merged.parquet
├── precinct_to_2026_sd_lookup.csv
├── precinct_to_2026_cd_lookup.csv
├── precinct_to_2026_hd_lookup.csv
├── sd_districts/
│   ├── party_gains_losses_by_district.csv
│   ├── party_composition_old_districts.csv
│   └── party_composition_new_districts.csv
├── cd_districts/
│   └── (similar structure)
├── hd_districts/
│   └── (similar structure)
├── comprehensive_gains_losses_breakdown.csv
└── detailed_gains_losses_by_district.csv
```

### Code Files
- `main.py`: Main orchestration script
- `process_voterfile.py`: Voter file processing
- `process_early_voting.py`: Early voting data processing
- `merge_voter_data.py`: Data merging
- `precinct_to_district_lookup.py`: Spatial matching
- `geospatial_match.py`: Turnout calculations
- `district_comparison.py`: Party gains/losses calculation
- `all_districts_gains_losses.py`: Multi-district analysis
- `party_transition_report.py`: Transition reports
- `party_crosstab_report.py`: Crosstab reports
- `create_visualizations.py`: Visualization generation
"""

# ---
# title: References

"""
## References

### Data Sources
- Texas Secretary of State: Voter registration files
- U.S. Census Bureau: 2022 district shapefiles
- Texas Legislative Council: 2026 district shapefiles
- County Election Offices: Early voting records

### Software and Libraries
- **Polars**: Fast DataFrame operations
- **GeoPandas**: Geospatial data processing
- **Pandas**: Data manipulation
- **Matplotlib/Seaborn**: Visualization
- **Shapely**: Spatial geometry operations

### Methodology References
- Redistricting impact analysis best practices
- Spatial data matching techniques
- Voter file analysis methodologies
"""

