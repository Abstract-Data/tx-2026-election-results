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

The analysis processes voter registration data, early voting records, and uses machine learning to model party affiliation for voters without primary voting history (~87% of voters).

## Features

- **Voter Data Processing**: Processes voterfile and early voting data using Polars for high-performance data manipulation
- **Geospatial Analysis**: Matches voters to districts using shapefiles and precinct-level lookups
- **Party Affiliation Modeling**: Uses Random Forest Classifier to predict party affiliation for non-primary voters
- **Redistricting Impact Analysis**: Calculates party gains/losses across all district types
- **Interactive Visualizations**: Creates maps and charts using Matplotlib, Seaborn, and Folium
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

This executes the complete analysis pipeline:

1. **Process Voterfile**: Loads and processes voter registration data
2. **Process Early Voting Data**: Processes early voting records
3. **Merge Data**: Combines voterfile with early voting data
4. **Load Shapefiles**: Loads district boundary shapefiles
5. **Calculate Turnout Metrics**: Computes turnout by district
6. **Build Precinct Lookups**: Creates precinct-to-district mappings
7. **Apply Precinct Lookups**: Assigns voters to 2026 districts
8. **Model Party Affiliation**: Predicts party for non-primary voters
9. **Generate Reports**: Creates party gains/losses and transition reports
10. **Generate Visualizations**: Creates maps and charts

### Running Individual Components

See the [Analysis Components](#analysis-components) section below for details on running specific analyses.

## Analysis Components

### Party Affiliation Modeling

Models party affiliation for voters who haven't voted in primaries by comparing them to known primary voters based on geographic proximity and demographics.

#### Methodology

**Features Used:**
1. **Geographic Features:**
   - Precinct-level party composition (R/D percentages)
   - County-level party composition
   - ZIP code-level party composition (if available)

2. **Demographic Features:**
   - Age
   - Age bracket party composition

**Model Approach:**
- Uses Random Forest Classifier trained on known primary voters
- Compares unknown voters to neighbors in their precinct, county, ZIP code, and age bracket
- Scores voters as:
  - **Likely Republican** (≥65% Republican probability)
  - **Lean Republican** (55-65% Republican probability)
  - **Swing** (<55% for either party)
  - **Lean Democrat** (55-65% Democrat probability)
  - **Likely Democrat** (≥65% Democrat probability)

**Fallback Method:**
If ML model cannot be trained, uses simple geographic averaging:
- Uses precinct → ZIP → county → age bracket (in that order of preference)
- Falls back to neutral (50/50) if no data available

#### Usage

```python
from tx_election_results.modeling.party_affiliation import model_party_affiliation

# Model party affiliation for unknown voters
result_df = model_party_affiliation(
    merged_df_path="data/exports/parquet/early_voting_merged.parquet",
    output_path="data/exports/parquet/voters_with_party_modeling.parquet",
    use_model=True  # Set to False for simple geographic averaging
)
```

#### Performance

- Processes ~16M unknown voters in chunks of 100K
- Uses sampling for training (>500K voters sampled to 500K for speed)
- Model accuracy typically 70-85% on test set

#### Output

The output DataFrame includes:
- `predicted_rep_prob`: Probability of being Republican (0-1)
- `predicted_dem_prob`: Probability of being Democrat (0-1)
- `predicted_party_score`: Categorical score (Likely/Lean R, Swing, Lean/Likely D)
- `party_final`: Final party assignment (uses known party if available, otherwise predicted)

### Known vs Modeled Voters Comparison

This component provides a direct comparison between:
1. **Analysis based ONLY on known primary voters** (what we know for certain)
2. **Analysis including modeled non-primary voters** (what we predict based on demographics and geography)

#### Why This Matters

- **Only ~12.7% of voters** have voted in primaries (known party affiliation)
- **~87.3% of voters** have no primary voting history (unknown party affiliation)
- This comparison shows how including modeled voters changes the party advantage analysis

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
│   └── hd_gains_losses_summary.csv
├── parquet/                    # Processed data files
│   ├── processed_voterfile.parquet
│   ├── processed_early_voting.parquet
│   ├── early_voting_merged.parquet
│   └── voters_with_party_modeling.parquet
├── visualizations/             # Charts and maps
│   ├── turnout_2022_congressional.png
│   ├── turnout_2022_senate.png
│   ├── turnout_2026.png
│   ├── party_gains_losses_barchart.png
│   └── ...
├── districts/                  # District-specific results
│   ├── sd_districts/
│   ├── cd_districts/
│   └── hd_districts/
└── analysis/                   # Detailed analysis outputs
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

### Party Gains/Losses Calculation

For each new district, the analysis calculates:

1. **Old District Composition**: Party breakdown of voters in old districts
2. **New District Composition**: Party breakdown of voters in new districts
3. **Net Changes**: Difference between new and old compositions
4. **Weighted Averages**: Accounts for contributions from multiple old districts

### Turnout Metrics

Calculates turnout rates by:
- District type (CD, SD, HD)
- Early voting vs. election day
- Party composition
- Geographic region

### Spatial Matching

Uses a combination of:
- Precinct-level lookups (preferred)
- Geospatial point-in-polygon matching (fallback)
- Address geocoding (when needed)

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

