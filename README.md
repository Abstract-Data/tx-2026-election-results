<div align="center">
  <img src="assets/abstract-data-logo-4.png" alt="Abstract Data Logo" width="600"/>
</div>

# Texas 2026 Election Results Analysis

[![Python](https://img.shields.io/badge/Python-3.12.8+-blue.svg)](https://www.python.org/)
[![Polars](https://img.shields.io/badge/Polars-1.35.1+-CD792C.svg?logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQiIGhlaWdodD0iMjQiIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTEyIDJMMTMuMDkgOC4yNkwyMCAxMkwxMy4wOSAxNS43NEwxMiAyMkwxMC45MSAxNS43NEw0IDEyTDEwLjkxIDguMjZMMTIgMloiIGZpbGw9IiNDRDc5MkMiLz4KPC9zdmc+)](https://www.pola.rs/)
[![GeoPandas](https://img.shields.io/badge/GeoPandas-0.14.0+-green.svg)](https://geopandas.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0.0+-150458.svg?logo=pandas)](https://pandas.pydata.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104.0+-009688.svg?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Scikit-learn](https://img.shields.io/badge/scikit--learn-1.3.0+-F7931E.svg?logo=scikit-learn)](https://scikit-learn.org/)
[![Matplotlib](https://img.shields.io/badge/Matplotlib-3.8.0+-11557C.svg?logo=python&logoColor=white)](https://matplotlib.org/)
[![Seaborn](https://img.shields.io/badge/Seaborn-0.13.2+-3776AB.svg)](https://seaborn.pydata.org/)
[![Folium](https://img.shields.io/badge/Folium-0.15.0+-77B829.svg)](https://python-visualization.github.io/folium/)
[![Marimo](https://img.shields.io/badge/Marimo-0.17.7+-FF6B6B.svg)](https://marimo.io/)
[![SQLModel](https://img.shields.io/badge/SQLModel-0.0.14+-009688.svg)](https://sqlmodel.tiangolo.com/)
[![UV](https://img.shields.io/badge/UV-latest-FFC131.svg?logo=python&logoColor=white)](https://github.com/astral-sh/uv)
[![Hatchling](https://img.shields.io/badge/Hatchling-latest-FF6B35.svg)](https://hatch.pypa.io/)

A comprehensive analysis pipeline for Texas 2026 election results, examining redistricting impacts, voter turnout patterns, and party composition changes across congressional, state senate, and house districts.

## Table of Contents

- [Texas 2026 Election Results Analysis](#texas-2026-election-results-analysis)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Features](#features)
  - [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Setup](#setup)
  - [Project Structure](#project-structure)
  - [Usage](#usage)
    - [Running the Full Pipeline](#running-the-full-pipeline)
    - [Running Individual Components](#running-individual-components)
  - [Analysis Components](#analysis-components)
    - [Party Affiliation Modeling](#party-affiliation-modeling)
      - [Methodology](#methodology)
      - [Usage](#usage-1)
      - [Performance](#performance)
      - [Output](#output)
    - [Known vs Modeled Voters Comparison](#known-vs-modeled-voters-comparison)
      - [Why This Matters](#why-this-matters)
      - [Usage](#usage-2)
      - [Output](#output-1)
      - [Interpretation](#interpretation)
    - [Marimo Reports](#marimo-reports)
      - [Installation](#installation-1)
      - [Running Reports](#running-reports)
      - [Alternative: Run as Script](#alternative-run-as-script)
      - [Report Contents](#report-contents)
      - [Export Options](#export-options)
  - [Output Structure](#output-structure)
  - [Data Sources](#data-sources)
  - [Methodology](#methodology-1)
    - [Party Gains/Losses Calculation](#party-gainslosses-calculation)
    - [Turnout Metrics](#turnout-metrics)
    - [Spatial Matching](#spatial-matching)
  - [API](#api)
    - [Running the API](#running-the-api)
    - [API Endpoints](#api-endpoints)

## Overview

This project analyzes how redistricting in Texas (from 2022/2024 boundaries to 2026 boundaries) creates opportunities and vulnerabilities for each party across three district types:

- **State Senate Districts (SD)**: 31 districts
- **Congressional Districts (CD)**: 38 districts  
- **House Districts (HD)**: 150 districts

The analysis processes voter registration data, early voting records, and uses XGBoost machine learning to predict party affiliation for general-election-only voters (those with general election history but no primary history - ~95% of voters).

## Features

- **Voter Data Processing**: Processes voterfile and early voting data using Polars for high-performance data manipulation
- **Primary Voter Classification**: Classifies voters based on primary voting history (R/D/Swing/Unknown) from last 4 primaries
- **Geospatial Analysis**: Matches voters to districts using shapefiles and precinct-level lookups
- **Machine Learning Party Prediction**: Uses XGBoost to predict party affiliation for general-election-only voters
- **Feature Engineering**: Creates geographic, demographic, and primary history features for ML model
- **Redistricting Impact Analysis**: Calculates party gains/losses across all district types (CD, SD, HD)
- **Competitiveness Analysis**: Assesses district competitiveness (Solidly R/D vs Competitive) for 2022 and 2026 maps
- **2022 vs 2026 Comparison**: Comprehensive comparison of competitiveness changes between old and new districts
- **Interactive Visualizations**: Creates maps and charts using Matplotlib, Seaborn, and Folium
- **Comprehensive Reporting**: Generates detailed reports with known-only and modeled data comparisons
- **REST API**: FastAPI-based API for accessing analysis results
- **Interactive Reports**: Marimo notebooks for exploratory analysis

## Installation

### Prerequisites

- Python 3.12.8 or higher
- [UV](https://github.com/astral-sh/uv) package manager (recommended)

### Setup

**Note**: For AI agents/assistants working with this codebase, see [.github/AGENTS.md](.github/AGENTS.md) for specific instructions on using UV and comprehensive development guidelines.

```bash
# Clone the repository
git clone <repository-url>
cd tx-2026-election-results

# Install dependencies using UV (recommended)
uv sync

# Or using pip
pip install -e .
```

## Project Structure

```
├── src/
│   ├── tx_election_results/     # Main package
│   │   ├── analysis/            # Analysis scripts
│   │   ├── data/                # Data processing
│   │   ├── geospatial/          # Geospatial operations
│   │   ├── modeling/            # ML models
│   │   ├── precinct/            # Precinct lookups
│   │   ├── visualization/       # Visualization tools
│   │   └── utils/               # Utilities
│   ├── scripts/                 # Utility scripts
│   ├── notebooks/               # Marimo notebooks
│   ├── api/                     # FastAPI application
│   ├── database/                # Database connections
│   └── models/                  # Data models
├── data/
│   └── exports/                 # Analysis outputs
│       ├── csv/                 # CSV reports
│       ├── parquet/             # Processed data files
│       ├── visualizations/      # Charts and maps
│       ├── districts/           # District-specific results
│       └── analysis/            # Detailed analysis outputs
├── main.py                      # Main orchestration script
└── pyproject.toml               # Project configuration
```

## Usage

### Running the Full Pipeline

```bash
uv run python main.py
```

This executes the complete analysis pipeline (15 steps):

**Data Processing (Steps 1-3):**
1. **Process Voterfile**: Loads and processes voter registration data, calculates age brackets
2. **Process Early Voting Data**: Processes early voting records
3. **Merge Data**: Combines voterfile with early voting data

**Geospatial Processing (Steps 4-7):**
4. **Load Shapefiles**: Loads district boundary shapefiles (2022 and 2026)
5. **Calculate Turnout Metrics**: Computes turnout by district
6. **Build Precinct Lookups**: Creates precinct-to-district mappings for CD, SD, HD
7. **Apply Precinct Lookups**: Assigns voters to 2026 districts

**Party Analysis (Steps 8-11):**
8. **Calculate Party Gains/Losses**: Analyzes redistricting impact (State Senate)
9. **Generate All District Gains/Losses**: Analyzes CD, SD, HD redistricting impacts
10. **Generate Party Crosstab Reports**: Creates party composition reports by County/CD/SD/HD
11. **Create Visualizations**: Creates maps and charts

**ML Modeling & Enhanced Analysis (Steps 12-15):**
12. **Classify Primary Voters**: Classifies voters as R/D/Swing/Unknown based on primary history
13. **Train XGBoost Model**: Trains ML model on known primary voters to predict party affiliation
14. **Predict General-Election-Only Voters**: Predicts party for voters with GEN history but no primaries
15. **Redistricting Impact Analysis**: Generates comprehensive analysis with modeled data, competitiveness assessments, and exports

### Running Individual Components

See the [Analysis Components](#analysis-components) section below for details on running specific analyses.

## Analysis Components

### Party Affiliation Modeling

**NEW: Enhanced ML-Based Party Prediction System**

The pipeline now includes a comprehensive machine learning system to predict party affiliation for general-election-only voters (those who vote in general elections but not primaries).

#### Primary Voter Classification (Step 12)

First, voters are classified based on their primary voting history from the last 4 primaries (2024, 2022, 2020, 2018):

- **Republican**: Voters who only voted in Republican primaries
- **Democrat**: Voters who only voted in Democrat primaries  
- **Swing**: Voters who voted in both R and D primaries (mixed history)
- **Unknown**: Voters with no primary voting history

#### ML Model Training (Step 13)

**Model**: XGBoost Classifier

**Training Data**: Only known primary voters (R/D) - typically ~850K voters (4.6% of total)

**Features Used** (districts are NOT used as features):
1. **Geographic Features:**
   - Precinct-level party composition (R/D percentages)
   - County-level party composition
   - ZIP code-level party composition (if available)

2. **Demographic Features:**
   - Age (numeric)
   - Age bracket (categorical, label encoded)
   - Age bracket party composition (R/D percentages)

3. **Primary History Features:**
   - Number of Republican primary votes
   - Number of Democrat primary votes
   - Total primary votes
   - Primary participation rate
   - Primary consistency (1.0 = all same party, 0.0 = mixed)

4. **Categorical Features** (label encoded):
   - County
   - City (RCITY)
   - Age bracket

**Model Configuration:**
- Algorithm: XGBoost (XGBClassifier)
- Objective: Multi-class classification (R/D)
- Evaluation: Cross-validation with stratified splits
- Class weights: Automatically balanced for imbalanced data
- Typical accuracy: 70-85% on test set

#### Prediction for General-Election-Only Voters (Step 14)

**Target Voters**: Only voters who:
- Have general election history (GEN columns with values)
- Have NO primary voting history (total_primary_votes == 0)
- Are classified as "Unknown" (no R/D/Swing classification)

**Prediction Process:**
1. Identifies general-election-only voters (~17.8M voters, 95.4% of total)
2. Applies trained XGBoost model to predict party probabilities
3. Maps probabilities to party scores:
   - **Likely Republican** (≥65% Republican probability)
   - **Lean Republican** (55-65% Republican probability)
   - **Swing** (<55% for either party)
   - **Lean Democrat** (55-65% Democrat probability)
   - **Likely Democrat** (≥65% Democrat probability)
4. Creates `party_final` column combining:
   - Known primary classifications (R/D/Swing) for primary voters
   - Predicted classifications for general-election-only voters

**Note**: Voters with no voting history at all are not modeled and remain "Unknown"

#### Usage

**Running the Full ML Pipeline:**

```bash
# Run steps 12-15 (ML modeling and analysis)
uv run python src/scripts/run_ml_steps.py

# Or run the full pipeline (steps 1-15)
uv run python main.py
```

**Programmatic Usage:**

```python
from tx_election_results.modeling.primary_voter_classifier import classify_primary_voters
from tx_election_results.modeling.feature_engineering import prepare_features_for_ml
from tx_election_results.modeling.party_prediction_model import train_party_prediction_model
from tx_election_results.modeling.predict_general_voters import (
    predict_party_for_general_voters,
    create_final_party_classification
)

# Step 1: Classify primary voters
df = classify_primary_voters(df)

# Step 2: Prepare features
df_features, label_encoders, feature_columns = prepare_features_for_ml(df)

# Step 3: Train model
model, metadata = train_party_prediction_model(
    df_features,
    feature_columns,
    label_encoders,
    output_model_path="models/party_prediction_model.pkl"
)

# Step 4: Predict for general-election-only voters
df_predicted = predict_party_for_general_voters(
    df_features,
    "models/party_prediction_model.pkl",
    feature_columns
)

# Step 5: Create final classification
df_final = create_final_party_classification(df_predicted)
```

#### Performance

- **Feature Engineering**: Processes all 18.6M voters to calculate geographic features (~10-30 minutes)
- **Model Training**: Trains on ~850K known primary voters (~5-15 minutes)
- **Prediction**: Predicts for ~17.8M general-election-only voters in chunks (~10-20 minutes)
- **Total Runtime**: ~30-60 minutes for full ML pipeline
- **Model Accuracy**: Typically 70-85% on test set

#### Output

The output DataFrame includes:
- `primary_classification`: R/D/Swing/Unknown based on primary history
- `predicted_rep_prob`: Probability of being Republican (0-1) for general-election-only voters
- `predicted_dem_prob`: Probability of being Democrat (0-1) for general-election-only voters
- `predicted_party_score`: Categorical score (Likely/Lean R, Swing, Lean/Likely D)
- `party_final`: Final party assignment combining known and predicted classifications

### Competitiveness Analysis

**NEW: Comprehensive Competitiveness Assessment**

The pipeline now includes detailed competitiveness analysis comparing 2022 and 2026 district maps.

#### Methodology

**Competitiveness Classification:**
- **Solidly Republican**: ≥57% of known party voters are Republican
- **Solidly Democrat**: ≥57% of known party voters are Democrat
- **Competitive**: <57% for both parties (neither party has a clear majority)

**Note**: Percentages are calculated based on known party voters (R+D) only, excluding Swing and Unknown voters.

#### 2022 vs 2026 Comparison

The analysis compares:
- Number of competitive districts in 2022 vs 2026
- Number of solidly R/D districts in 2022 vs 2026
- Net changes in competitiveness categories
- District-by-district breakdown of changes

#### Output

Generates competitiveness reports for each district type:
- `data/exports/analysis/competitiveness/{cd/sd/hd}_competitiveness_2022.csv`
- `data/exports/analysis/competitiveness/{cd/sd/hd}_competitiveness_2026.csv`
- `data/exports/analysis/competitiveness/{cd/sd/hd}_competitiveness_comparison.csv`

### Known vs Modeled Voters Comparison

This component provides a direct comparison between:
1. **Analysis based ONLY on known primary voters** (what we know for certain - 4.6% of voters)
2. **Analysis including modeled general-election-only voters** (what we predict - adds ~17.8M voters, 95.4% of total)

#### Why This Matters

- **Only ~4.6% of voters** (853K) have voted in primaries (known party affiliation)
- **~95.4% of voters** (17.8M) have no primary voting history
- **~17.8M voters** have general election history but no primary history (can be modeled)
- This comparison shows how including modeled voters changes competitiveness assessments and party composition

#### Usage

```bash
# Generate modeled data first (if not already done)
uv run python main.py  # Runs through Step 12

# Run the comparison
uv run python src/scripts/compare_known_vs_modeled_voters.py
```

#### Output

The script generates:

1. **Known Voters Only Analysis**
   - Location: `data/exports/analysis/known_vs_modeled_comparison/{hd/cd/sd}_known_only/`
   - Files: `party_gains_losses_by_district.csv`, `party_composition_old_districts.csv`, `party_composition_new_districts.csv`
   - Shows: Analysis based only on voters who actually voted in primaries

2. **With Modeled Voters Analysis**
   - Location: `data/exports/analysis/known_vs_modeled_comparison/{hd/cd/sd}_with_modeled/`
   - Files: Same structure as above, but includes modeled voters
   - Shows: Analysis including predicted party affiliation for non-primary voters

3. **Direct Comparison Files**
   - Location: `data/exports/analysis/known_vs_modeled_comparison/{hd/cd/sd}_known_vs_modeled_comparison.csv`
   - Shows: Side-by-side comparison with difference columns
     - `rep_voters_diff`: Difference in Republican voters
     - `dem_voters_diff`: Difference in Democrat voters
     - `net_advantage_diff`: Net change in party advantage

4. **Summary**
   - Location: `data/exports/analysis/known_vs_modeled_comparison/known_vs_modeled_summary.csv`
   - Shows: Summary table comparing all district types

#### Interpretation

- **Positive Net Advantage Change**: Modeled voters strengthen Republican advantage
- **Negative Net Advantage Change**: Modeled voters strengthen Democrat advantage
- **Large differences**: Districts where modeling has significant impact
- **Small differences**: Districts where known voters are representative

### Marimo Reports

Interactive reports using Marimo notebooks for exploratory analysis and visualization.

#### Installation

```bash
# Install marimo
uv pip install marimo
# or
pip install marimo
```

#### Running Reports

```bash
# Open the marimo editor
marimo edit src/notebooks/district_map_changes_report.py
```

This opens a web interface where you can:
- View and edit the report
- Run cells interactively
- See visualizations
- Export as HTML or PDF

#### Alternative: Run as Script

```bash
python src/notebooks/district_map_changes_report.py
```

#### Report Contents

The report includes:

1. **Introduction**: Overview of the analysis purpose and scope
2. **Data Sources**: Detailed documentation of all input data
3. **Methodology**: Step-by-step explanation of the analysis process
4. **Spatial Matching**: How precinct-to-district assignment works
5. **Party Gains/Losses Calculation**: Detailed methodology
6. **Code Implementation**: Key code snippets
7. **Results Summary**: Key findings
8. **Visualizations**: Charts and graphs
9. **Limitations**: Important caveats and considerations
10. **Conclusion**: Summary and takeaways
11. **Appendix**: File structure and references

#### Export Options

From the marimo interface:
- Export as HTML: Click "Export" → "HTML"
- Export as PDF: Click "Export" → "PDF" (requires additional setup)
- Export as Markdown: The notebook can be converted to markdown

## Output Structure

All analysis outputs are organized under `data/exports/`:

```
data/exports/
├── csv/                        # CSV reports
│   ├── party_gains_losses_by_district.csv
│   ├── sd_gains_losses_summary.csv
│   ├── cd_gains_losses_summary.csv
│   ├── hd_gains_losses_summary.csv
│   ├── voter_classifications.csv
│   └── all_districts_comprehensive_summary.csv
├── parquet/                    # Processed data files
│   ├── processed_voterfile.parquet
│   ├── processed_early_voting.parquet
│   ├── early_voting_merged.parquet
│   └── voters_with_party_modeling.parquet  # With ML predictions
├── models/                     # ML models
│   ├── party_prediction_model.pkl
│   └── party_prediction_model.metadata.joblib
├── visualizations/             # Charts and maps
│   ├── turnout_2022_congressional.png
│   ├── turnout_2022_senate.png
│   ├── turnout_2026.png
│   ├── party_gains_losses_barchart.png
│   ├── party_composition_comparison.png
│   ├── democrat_gains_losses_map.png
│   ├── republican_gains_losses_map.png
│   └── ...
├── districts/                  # District-specific results
│   ├── sd_districts/
│   │   ├── party_composition_old_districts.csv
│   │   ├── party_composition_new_districts.csv
│   │   └── party_gains_losses_by_district.csv
│   ├── cd_districts/
│   │   ├── party_composition_old_districts.csv
│   │   ├── party_composition_new_districts.csv
│   │   └── party_gains_losses_by_district.csv
│   └── hd_districts/
│       ├── party_composition_old_districts.csv
│       ├── party_composition_new_districts.csv
│       └── party_gains_losses_by_district.csv
└── analysis/                   # Detailed analysis outputs
    ├── redistricting_impact/   # Redistricting analysis with modeled data
    │   ├── cd_redistricting_impact.csv
    │   ├── sd_redistricting_impact.csv
    │   └── hd_redistricting_impact.csv
    ├── competitiveness/        # Competitiveness analysis
    │   ├── cd_competitiveness_2022.csv
    │   ├── cd_competitiveness_2026.csv
    │   ├── cd_competitiveness_comparison.csv
    │   └── (similar for sd and hd)
    ├── known_vs_modeled_comparison/
    ├── 2022_vs_2026_comparisons/
    └── ...
```

## Data Sources

The analysis requires:

1. **Voter Registration Data**: Voterfile with registration information
2. **Early Voting Data**: Early voting records by precinct/date
3. **Shapefiles**: District boundary files for 2022 and 2026
4. **Precinct Lookups**: Precinct-to-district mapping files

Configure data paths in `src/tx_election_results/config.py`.

## Methodology

### Primary Voter Classification

Voters are classified based on their primary voting history from the last 4 primaries:

1. Count Republican primary votes (PRI24, PRI22, PRI20, PRI18 where party = "RE")
2. Count Democrat primary votes (PRI24, PRI22, PRI20, PRI18 where party = "DE")
3. Classify:
   - **Republican**: rep_votes > 0 AND dem_votes == 0
   - **Democrat**: dem_votes > 0 AND rep_votes == 0
   - **Swing**: rep_votes > 0 AND dem_votes > 0 (mixed history)
   - **Unknown**: rep_votes == 0 AND dem_votes == 0 (no primary history)

### ML Feature Engineering

Features are calculated for all voters to enable predictions:

1. **Geographic Features**: Party composition at precinct, county, and ZIP levels
2. **Demographic Features**: Age and age bracket party composition
3. **Primary History Features**: Vote counts, participation rates, consistency
4. **Categorical Encoding**: County, City, Age bracket (label encoded)

**Note**: Districts (CD, SD, HD) are explicitly NOT used as features to avoid data leakage.

### Party Prediction Model

1. **Training**: XGBoost trained on known primary voters (R/D only, ~850K voters)
2. **Target**: General-election-only voters (have GEN history, no primary history, ~17.8M voters)
3. **Prediction**: Generates probabilities and categorical scores (Likely/Lean R, Swing, Lean/Likely D)
4. **Final Classification**: Combines known primary classifications with ML predictions

### Party Gains/Losses Calculation

For each new district, the analysis calculates:

1. **Old District Composition**: Party breakdown of voters in old districts (2022 boundaries)
2. **New District Composition**: Party breakdown of voters in new districts (2026 boundaries)
3. **Expected Composition**: Weighted average from contributing old districts
4. **Net Changes**: Difference between actual new composition and expected composition
5. **Known vs Modeled Breakdown**: Separates known primary voters from ML-predicted voters

### Turnout Metrics

Calculates turnout rates by:
- District type (CD, SD, HD)
- Early voting vs. election day
- Party composition
- Geographic region

### Competitiveness Assessment

**Threshold**: 57% of known party voters (R+D only)

**Classification Logic**:
- Calculate Republican percentage: `R / (R + D) * 100`
- Calculate Democrat percentage: `D / (R + D) * 100`
- Classify:
  - **Solidly Republican**: rep_pct ≥ 57%
  - **Solidly Democrat**: dem_pct ≥ 57%
  - **Competitive**: Both < 57%

**Comparison**: Calculates competitiveness for both 2022 and 2026 maps, showing net changes in each category.

### Spatial Matching

Uses a combination of:
- Precinct-level lookups (preferred): Spatial intersection of precinct and district shapefiles
- For split precincts: Assigns to district with largest overlap area
- Geospatial point-in-polygon matching (fallback)
- Address geocoding (when needed)

## Recent Enhancements

### ML-Based Party Prediction (Steps 12-15)

**New Features:**
- Primary voter classification based on last 4 primaries
- XGBoost model training on known primary voters
- Feature engineering with geographic, demographic, and primary history features
- Prediction for general-election-only voters (~17.8M voters)
- Comprehensive competitiveness analysis (2022 vs 2026)
- Enhanced redistricting impact analysis with modeled data

**Key Improvements:**
- More accurate party predictions using ML instead of simple geographic averaging
- Only models voters with general election history (not all unknown voters)
- Separates known vs modeled voters in all analysis outputs
- Competitiveness analysis with 2022 vs 2026 comparisons
- District-by-district breakdown tables in reports

**Output Files:**
- `voters_with_party_modeling.parquet`: Full dataset with ML predictions
- `party_prediction_model.pkl`: Trained XGBoost model
- Competitiveness comparison CSVs for each district type
- Enhanced redistricting impact reports with known/modeled breakdown

### Running Just ML Steps

To run only the ML modeling steps (12-15) after data processing is complete:

```bash
# Run ML pipeline steps
uv run python src/scripts/run_ml_steps.py

# Or use the status checker to monitor progress
bash check_ml_status.sh
```

## API

The project includes a FastAPI-based REST API for accessing analysis results.

### Running the API

```bash
uv run uvicorn src.api.main:app --reload
```

### API Endpoints

- `/api/voters/` - Voter data endpoints
- `/api/districts/` - District information
- `/api/turnout/` - Turnout metrics
- `/api/early-voting/` - Early voting data

See `src/api/` for detailed API documentation.

