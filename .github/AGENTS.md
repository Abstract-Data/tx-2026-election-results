# Texas 2026 Election Results - Agent Instructions

## 🤖 Overview

This document provides comprehensive instructions for all GitHub Copilot Agents, Cursor AI, and other AI assistants working on the Texas 2026 Election Results analysis project. This is the **single source of truth** for all agent operations.

## 📋 Project Summary

**Texas 2026 Election Results** is a data analysis pipeline for processing voter files, early voting data, and calculating turnout metrics across different district boundaries. The project analyzes voter turnout by comparing 2022 and 2026 district boundaries, generating visualizations and insights for electoral analysis.

**Key Features:**
- **Voter File Processing**: Extract and process voter demographics (age, party, district assignments)
- **Early Voting Analysis**: Process daily early voting CSV files and merge with voter records
- **Geospatial Analysis**: Match voters to districts using shapefiles for 2022 and 2026 boundaries
- **Turnout Metrics**: Calculate turnout rates by district for congressional, state senate, and state house districts
- **Visualizations**: Generate choropleth maps and comparative visualizations

## 🏗️ Architecture

### Data Processing Pipeline

The project follows a linear data processing pipeline:

1. **Voter File Processing** (`process_voterfile.py`)
   - Loads voter CSV file
   - Calculates ages from DOB
   - Creates age brackets
   - Extracts district assignments (NEWCD, NEWSD, NEWHD)
   - Extracts party affiliation (PRI24, PRI22)

2. **Early Voting Processing** (`process_early_voting.py`)
   - Loads all daily early voting CSV files
   - Combines and deduplicates records
   - Extracts voter IDs and voting information

3. **Data Merging** (`merge_voter_data.py`)
   - Merges voter file with early voting data
   - Maps party codes to full names
   - Creates `voted_early` boolean flag

4. **Geospatial Matching** (`geospatial_match.py`)
   - Loads shapefiles for 2022 and 2026 districts
   - Matches voters to districts using voterfile district assignments
   - Calculates turnout metrics by district

5. **Visualization** (`create_visualizations.py`)
   - Creates choropleth maps of turnout rates
   - Generates comparative visualizations
   - Creates age bracket analysis charts

6. **Main Orchestration** (`main.py`)
   - Coordinates all processing steps
   - Manages data flow between modules
   - Outputs summary statistics

### Technology Stack

- **Data Processing**: Polars (fast DataFrame operations)
- **Geospatial**: GeoPandas, Shapely (GIS operations)
- **Visualization**: Matplotlib, Folium (maps and charts)
- **Python**: 3.12.8+ (requires modern Python features)

## 🚨 CRITICAL RULES FOR ALL AGENTS

### 0. Dependency Checking (CRITICAL)

**Before starting any task, ALWAYS check for prerequisites:**

- **Step 1 (Voter File Processing)**: No dependencies - can start immediately
- **Step 2 (Early Voting Processing)**: No dependencies - can start immediately
- **Step 3 (Data Merging)**: Requires Steps 1-2 completed - **Build on existing data processing**
- **Step 4 (Geospatial Matching)**: Requires Step 3 completed - **Build on merged data**
- **Step 5 (Visualization)**: Requires Step 4 completed - **Build on turnout metrics**
- **Step 6 (Main Orchestration)**: Requires Steps 1-5 completed - **Build on all modules**

**Dependency Check Process:**

1. **Check for existing code**: Verify if data processing modules exist
2. **Verify data files**: Check if required input files are available
3. **Confirm output formats**: Check if intermediate files match expected schemas
4. **If dependencies not met**: Add comment: "❌ Blocked: Waiting for [prerequisite] to complete"
5. **If dependencies met**: 
   - Checkout the latest branch
   - Create new branch from that point
   - Continue building on existing code
6. **Do not duplicate work**: Use existing functions and data structures

### 0.1. Git Workflow (CRITICAL)

**ALWAYS work on feature branches and create PRs:**

**Branch Naming Convention:**
- `feature/voter-file-processing` (Step 1)
- `feature/early-voting-processing` (Step 2)
- `feature/data-merging` (Step 3) - **Builds on Steps 1-2**
- `feature/geospatial-matching` (Step 4) - **Builds on Step 3**
- `feature/visualizations` (Step 5) - **Builds on Step 4**
- `feature/main-orchestration` (Step 6) - **Builds on Steps 1-5**

**Git Workflow Process:**

1. **Check for existing work**: Look for open PRs from previous steps
2. **Create feature branch**: `git checkout -b feature/[description]`
3. **If building on previous work**: 
   - Checkout the latest branch from previous step
   - Create new branch from that point: `git checkout -b feature/[description]`
   - Continue building on existing code
4. **Work on implementation**: Make all necessary changes
5. **Before committing - ALWAYS run pre-commit hooks** (CRITICAL):
   ```bash
   # Ensure pre-commit hooks are installed (run once per repo)
   uv run pre-commit install
   
   # Pre-commit hooks will automatically run on commit
   # Or manually run to fix issues before committing:
   uv run pre-commit run --all-files
   ```
   **Important**: Pre-commit hooks automatically run `ruff --fix` to fix linting issues before commits. This prevents CI failures from linting errors.
6. **Commit frequently**: `git add . && git commit -m "descriptive message"`
   - Pre-commit hooks will run automatically and fix auto-fixable issues
   - If there are unfixable issues, the commit will be blocked - fix them manually
7. **Push branch**: `git push origin feature/[description]`
8. **Create PR**: Link PR to the issue using "Closes #[issue-number]"
9. **Request review**: Assign appropriate reviewers
10. **Wait for approval**: Do not merge until approved
11. **Merge after approval**: Use "Squash and merge" to keep history clean

**Important**: Each agent builds incrementally on the previous agent's work. Do not duplicate data processing logic or function definitions.

**PR Requirements:**
- Clear, descriptive title
- Link to the issue: "Closes #[issue-number]"
- Detailed description of changes
- List all files created/modified
- Include testing instructions
- Add sample outputs if data processing changes

## 🎯 Agent Specializations

### Data Processing Agent

**Primary File**: `.github/agents/data-processing-instructions.md`

**Expertise**: Polars, data cleaning, ETL pipelines

**Tasks**: Voter file processing, early voting processing, data merging, schema validation

**Dependencies**: None (foundational modules)

**Key Responsibilities:**
- Process large CSV files efficiently using Polars
- Handle data type conversions and validation
- Implement proper error handling for malformed data
- Create reusable data transformation functions

### Geospatial Agent

**Primary File**: `.github/agents/geospatial-instructions.md`

**Expertise**: GeoPandas, Shapely, spatial joins, GIS operations

**Tasks**: Shapefile loading, spatial matching, district assignment, turnout calculations

**Dependencies**: Data processing modules (Step 3)

**Key Responsibilities:**
- Load and validate shapefiles
- Match voters to districts using geospatial operations
- Calculate accurate turnout metrics by district
- Handle coordinate reference system (CRS) conversions

### Visualization Agent

**Primary File**: `.github/agents/visualization-instructions.md`

**Expertise**: Matplotlib, Folium, cartography, data visualization

**Tasks**: Choropleth maps, comparative visualizations, statistical charts

**Dependencies**: Geospatial matching (Step 4)

**Key Responsibilities:**
- Create publication-quality visualizations
- Implement consistent color schemes and legends
- Generate interactive maps when appropriate
- Ensure visualizations are accessible and clear

## 📁 File Structure

### Current Structure

```
tx-2026-election-results/
├── main.py                    # Main orchestration script
├── process_voterfile.py       # Voter file processing
├── process_early_voting.py    # Early voting data processing
├── merge_voter_data.py        # Data merging logic
├── geospatial_match.py        # Geospatial operations
├── create_visualizations.py   # Visualization generation
├── pyproject.toml             # Project dependencies
├── uv.lock                    # Dependency lock file
├── README.md                  # Project documentation
├── .github/
│   └── agents/
│       └── AGENTS.md          # This file
└── visualizations/            # Output directory (generated)
    ├── *.png                  # Visualization outputs
    └── *.html                 # Interactive maps
```

### Recommended Structure (Future)

```
tx-2026-election-results/
├── src/
│   └── tx_election_results/
│       ├── __init__.py
│       ├── data/
│       │   ├── __init__.py
│       │   ├── voterfile.py
│       │   ├── early_voting.py
│       │   └── merge.py
│       ├── geospatial/
│       │   ├── __init__.py
│       │   ├── shapefiles.py
│       │   └── matching.py
│       ├── visualization/
│       │   ├── __init__.py
│       │   ├── maps.py
│       │   └── charts.py
│       └── utils/
│           ├── __init__.py
│           └── helpers.py
├── tests/
│   ├── test_voterfile.py
│   ├── test_early_voting.py
│   ├── test_merge.py
│   ├── test_geospatial.py
│   └── test_visualization.py
├── data/
│   ├── raw/                   # Raw input files
│   ├── processed/             # Processed intermediate files
│   └── output/                # Final outputs
├── main.py                    # Main entry point
├── pyproject.toml
├── README.md
└── .github/
    └── agents/
        └── AGENTS.md
```

## 📚 Dependencies & Libraries

### Core Dependencies

#### Data Processing
- **Polars** (`>=1.35.1`) - Fast DataFrame operations
  - Use for all data loading and transformation
  - Prefer Polars over Pandas for performance
  - Use lazy evaluation for large datasets: `pl.scan_csv()` → `pl.collect()`

- **Pydantic** (`>=2.5.0`) - Data validation and settings management
  - Use for data validation and schema enforcement
  - Use field validators for automatic data cleaning and transformation
  - Use for age calculation and age bracket conversion
  - Configure with `ConfigDict` (not deprecated `class Config`)
  - Integrate with Polars for validated data processing

- **SQLModel** (optional, when needed) - SQL database ORM with Pydantic integration
  - Use when database operations are needed
  - Provides Pydantic validation with database models
  - Use for type-safe database operations

#### Geospatial
- **GeoPandas** (`>=0.14.0`) - Geospatial data operations
  - Use for shapefile loading and spatial operations
  - Ensure CRS consistency: `gdf.to_crs(epsg=4326)`
  - Use spatial joins for matching voters to districts

- **Shapely** (`>=2.0.0`) - Geometric operations
  - Use for point-in-polygon tests
  - Handle geometric validation

#### Visualization
- **Matplotlib** (`>=3.8.0`) - Static visualizations
  - Use for choropleth maps and charts
  - Configure consistent figure sizes and DPI
  - Use appropriate color schemes (e.g., `YlOrRd` for turnout)

- **Folium** (`>=0.15.0`) - Interactive maps
  - Use for interactive web-based maps
  - Embed in HTML outputs

### Development Dependencies

#### Testing
- **Pytest** (`>=8.4.2`) - Testing framework
  - Use for all tests
  - Use fixtures for test data
  - Test with sample data files

#### Code Quality
- **Ruff** (`>=0.1.0`) - Fast linter and formatter
  - Use for linting and formatting
  - Configure with proper rules
  - Pre-commit hooks automatically run `ruff --fix`

- **MyPy** (`>=1.6.0`) - Type checking
  - Use for static type checking
  - Add type hints to all functions

#### Pre-commit
- **Pre-commit** (`>=3.5.0`) - Git hooks
  - Use for pre-commit checks
  - Automatically runs `ruff --fix` before commits

### Key Integration Patterns

#### Polars Data Processing

```python
# Efficient CSV loading
df = pl.read_csv(
    file_path,
    infer_schema_length=10000,
    try_parse_dates=False,
    schema_overrides={"PCT": pl.Utf8}  # Handle alphanumeric columns
)

# Lazy evaluation for large files
df = pl.scan_csv(file_path).filter(pl.col("COUNTY") == "TRAVIS").collect()

# Efficient transformations
df = df.with_columns([
    pl.col("DOB").map_elements(calculate_age, return_dtype=pl.Int64).alias("age")
])
```

#### GeoPandas Spatial Operations

```python
# Load shapefile
gdf = gpd.read_file(shapefile_path)

# Ensure CRS consistency
gdf = gdf.to_crs(epsg=4326)

# Spatial join (if needed)
voters_gdf = gpd.GeoDataFrame(
    voters_df,
    geometry=gpd.points_from_xy(voters_df.lon, voters_df.lat)
)
matched = gpd.sjoin(voters_gdf, districts_gdf, how="left", predicate="within")
```

#### Polars ↔ GeoPandas Conversion

```python
# Polars to Pandas for GeoPandas
voter_pd = voter_pl.to_pandas()

# Pandas/GeoPandas back to Polars
result_pl = pl.from_pandas(result_pd)
```

## 🧪 Testing Requirements

### Data Processing Tests
- **Unit Tests**: Test each processing function with sample data
- **Integration Tests**: Test full pipeline with small datasets
- **Edge Cases**: Test with missing data, malformed files, empty files
- **Performance Tests**: Verify performance on large datasets

### Geospatial Tests
- **Shapefile Loading**: Verify shapefiles load correctly
- **CRS Handling**: Test coordinate reference system conversions
- **Spatial Matching**: Verify voters match to correct districts
- **Edge Cases**: Test with invalid geometries, out-of-bounds points

### Visualization Tests
- **Output Generation**: Verify visualizations are created
- **Format Validation**: Ensure outputs are valid PNG/HTML
- **Visual Consistency**: Check color schemes and legends

### Test Commands

```bash
# Run all tests
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_voterfile.py -v
```

## 🚀 Execution

### Critical: Use UV for Everything

**IMPORTANT**: This project uses [UV](https://github.com/astral-sh/uv) as the package manager and Python environment manager. **ALWAYS use UV commands** for running Python scripts, installing packages, and managing dependencies.

#### Running Python Scripts

**NEVER** run Python scripts directly with `python` or `python3`. **ALWAYS** use:

```bash
uv run python <script.py>
```

If you encounter package build errors, use `--no-project` flag with PYTHONPATH set:

```bash
# Set PYTHONPATH to include src directory, then run with --no-project
PYTHONPATH=/Users/johneakin/PyCharmProjects/tx-2026-election-results/src:$PYTHONPATH uv run --no-project python <script.py>
```

Or use a one-liner:
```bash
uv run --no-project python -c "import sys; sys.path.insert(0, 'src'); exec(open('main.py').read())"
```

**Examples:**
```bash
# Run the main pipeline (preferred)
uv run python main.py

# If build errors occur, use --no-project
uv run --no-project python main.py

# Run a specific script
uv run python src/scripts/analyze_modeling_criteria.py

# Run a module (requires proper PYTHONPATH)
uv run python -c "import sys; sys.path.insert(0, 'src'); from tx_election_results.modeling.party_affiliation import model_party_affiliation; ..."
```

#### Installing Packages

**NEVER** use `pip install`. **ALWAYS** use:

```bash
# Install a new package
uv add <package-name>

# Install development dependencies
uv sync --dev
```

#### Running Commands in General

For any Python-related command, prefix it with `uv run`:

```bash
# Run Python REPL
uv run python

# Run Python with a one-liner
uv run python -c "import polars as pl; print(pl.__version__)"

# Run pytest
uv run pytest

# Run any Python script
uv run python <script>
```

#### Why UV?

UV provides:
- Fast dependency resolution and installation
- Automatic virtual environment management
- Consistent Python version management
- Better handling of package builds and dependencies

### Running the Pipeline

```bash
# Run the complete pipeline
uv run python main.py

# If build errors occur, use --no-project flag
uv run --no-project python main.py

# Run individual modules
uv run python src/tx_election_results/data/voterfile.py
uv run python src/tx_election_results/data/early_voting.py
uv run python src/tx_election_results/data/merge.py
uv run python src/tx_election_results/geospatial/matching.py
uv run python src/tx_election_results/visualization/create_visualizations.py
```

### Environment Setup

1. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Verify installation**:
   ```bash
   uv run python --version  # Should be 3.12.8+
   ```

### Project Structure

This project uses a `src` layout:
- Package code is in `src/tx_election_results/`
- Scripts are in `src/scripts/`
- Main entry point is `main.py` at the root
- Configuration is in `src/tx_election_results/config.py`

### Common Workflows

#### Running the Full Pipeline

```bash
cd /Users/johneakin/PyCharmProjects/tx-2026-election-results

# Try with normal run first
uv run python main.py

# If build errors occur, use --no-project flag
uv run --no-project python main.py
```

#### Running Individual Components

```bash
# Process voterfile
uv run python -c "from tx_election_results.data.voterfile import process_voterfile; from tx_election_results.config import config; process_voterfile(config.VF_2024, str(config.PROCESSED_VOTERFILE))"

# Model party affiliation
uv run python -c "from tx_election_results.modeling.party_affiliation import model_party_affiliation; from tx_election_results.config import config; model_party_affiliation(str(config.MERGED_DATA), str(config.MODELED_DATA))"
```

#### Debugging

```bash
# Run with debug output
uv run python -m pdb main.py

# Check Python version
uv run python --version

# Check installed packages
uv pip list
```

### Troubleshooting

If you encounter package build errors:
1. First try using `uv run --no-project python <script>` to bypass the build
2. Make sure you're using `uv run` before any Python command
3. Try `uv sync` to reinstall dependencies
4. Check that `pyproject.toml` is properly configured
5. The `--no-project` flag tells UV to run Python without building the package, which is often sufficient for running scripts

**Remember:**
- ✅ `uv run python <script>`
- ✅ `uv add <package>`
- ✅ `uv sync`
- ❌ `python <script>` (don't use this)
- ❌ `pip install` (don't use this)
- ❌ `python3 <script>` (don't use this)

Always use UV for everything Python-related in this project.

### Data Requirements

**Input Files:**
- Voter file CSV (Texas November 2024)
- Early voting CSV files (daily files in `data/early_voting/`)
- Shapefiles:
  - 2022 Congressional districts: `tl_2022_48_cd118.shp`
  - 2022 State Senate districts: `tl_2022_48_sldu.shp`
  - 2026 Districts: `PLANC2308.shp`

**Output Files:**
- `processed_voterfile.parquet` - Processed voter data
- `processed_early_voting.parquet` - Processed early voting data
- `early_voting_merged.parquet` - Merged dataset
- `turnout_by_district_*.csv` - Turnout metrics by district
- `visualizations/*` - Generated visualizations

## 📊 Current Project Status

### ✅ Completed Features

- **Voter File Processing**: Age calculation, age brackets, district extraction
- **Early Voting Processing**: Multi-file loading, deduplication
- **Data Merging**: Voter-early voting merge, party mapping
- **Geospatial Matching**: Shapefile loading, district matching, turnout calculations
- **Visualization**: Choropleth maps, comparative visualizations
- **Main Orchestration**: Complete pipeline coordination

### 🚧 In Progress

- **Testing Suite**: Comprehensive test coverage
- **Error Handling**: Robust error handling for edge cases
- **Documentation**: Inline documentation and docstrings

### ⏳ Pending

- **Code Organization**: Refactor into proper package structure
- **Configuration Management**: Move hardcoded paths to config file
- **Performance Optimization**: Optimize for very large datasets
- **Interactive Dashboards**: Create interactive analysis dashboards
- **Automated Reporting**: Generate automated analysis reports

## 🎯 Key Features

### Voter File Processing
- **Age Calculation**: Accurate age calculation from DOB
- **Age Brackets**: Standard demographic age groupings
- **District Extraction**: Congressional, State Senate, State House districts
- **Party Affiliation**: Primary party identification (2022, 2024)

### Early Voting Analysis
- **Multi-file Processing**: Efficiently process multiple daily files
- **Deduplication**: Remove duplicate voter records
- **Data Validation**: Ensure data quality and consistency

### Geospatial Analysis
- **District Matching**: Match voters to districts using voterfile assignments
- **Turnout Metrics**: Calculate turnout rates by district
- **Boundary Comparison**: Compare 2022 vs 2026 district boundaries

### Visualizations
- **Choropleth Maps**: Color-coded district maps showing turnout rates
- **Comparative Analysis**: Side-by-side comparisons of different districts
- **Demographic Breakdowns**: Age bracket and party analysis

## 🚨 Critical Notes

⚠️ **DATA PRIVACY**: Voter data is sensitive. Ensure proper handling and never commit voter files to version control.

⚠️ **DATA ACCURACY**: Verify all calculations and matches. Incorrect turnout metrics can lead to incorrect conclusions.

⚠️ **PERFORMANCE**: Use Polars for data processing. Avoid Pandas for large datasets unless necessary for GeoPandas integration.

⚠️ **NO REDESIGNING**: You must NOT rewrite, redesign, or swap out core libraries (Polars, GeoPandas) without discussing your reasoning and waiting for explicit permission. Please stick to debugging and incremental changes to the current implementation.

## 💻 Coding Standards

### Simplicity First

**CRITICAL**: Write code as simplistic as possible.

- **Prefer simple, readable code over clever optimizations**
- **Use straightforward logic and clear variable names**
- **Avoid unnecessary abstractions or complex patterns**
- **Keep functions small and focused on a single task**
- **Favor explicit over implicit behavior**
- **Document complex logic with comments**

### Data Validation & Transformation

**Use SQLModel/Pydantic for data validation and fixing:**

- **Data Validation**: Use Pydantic models to validate data schemas and types
- **Data Cleaning**: Use Pydantic validators and field validators to automatically fix common data issues
- **Age Conversion**: Use Pydantic validators to convert age calculations and age brackets
- **Type Safety**: Leverage Pydantic's type system for automatic type checking and conversion

**Example Patterns:**

```python
from pydantic import BaseModel, Field, field_validator
from datetime import datetime

class VoterRecord(BaseModel):
    vuid: int
    dob: str = Field(..., description="Date of birth in YYYYMMDD format")
    age: int | None = None
    age_bracket: str | None = None
    
    @field_validator('dob')
    @classmethod
    def validate_dob(cls, v: str) -> str:
        """Validate and fix DOB format."""
        if not v or len(v) != 8:
            raise ValueError(f"Invalid DOB format: {v}")
        return v
    
    @field_validator('age', mode='before')
    @classmethod
    def calculate_age(cls, v, info) -> int | None:
        """Calculate age from DOB if not provided."""
        if v is not None:
            return v
        dob_str = info.data.get('dob')
        if not dob_str:
            return None
        # Age calculation logic here
        return calculate_age_from_dob(dob_str)
    
    @field_validator('age_bracket', mode='before')
    @classmethod
    def convert_age_bracket(cls, v, info) -> str:
        """Convert age to age bracket if not provided."""
        if v:
            return v
        age = info.data.get('age')
        if age is None:
            return "Unknown"
        return create_age_bracket(age)
```

**When to Use SQLModel/Pydantic:**

- **Input Data Validation**: When loading data from CSV files
- **Data Transformation**: When converting between formats (e.g., DOB → age → age bracket)
- **Data Cleaning**: When fixing common data issues automatically
- **Type Safety**: When you need guaranteed type correctness
- **API Responses**: When creating structured output data

**Integration with Polars:**

- Use Pydantic models to validate rows before/after Polars operations
- Convert Polars DataFrames to Pydantic models for validation
- Use Pydantic for data schema definitions, then convert to Polars for processing

```python
# Example: Validate Polars data with Pydantic
def validate_voter_data(df: pl.DataFrame) -> pl.DataFrame:
    """Validate voter data using Pydantic models."""
    validated_rows = []
    for row in df.iter_rows(named=True):
        try:
            validated = VoterRecord(**row)
            validated_rows.append(validated.model_dump())
        except ValidationError as e:
            # Handle validation errors (log, skip, or fix)
            continue
    return pl.DataFrame(validated_rows)
```

### Design Patterns

**Use Factory, Observer, and Strategy patterns for code organization:**

While maintaining simplicity, these patterns help organize code and improve maintainability.

#### Factory Pattern

**Use for creating objects based on type or configuration:**

- **Data Processors**: Create different processors based on file type or data source
- **Visualization Types**: Create different visualization generators based on chart type
- **Geospatial Operations**: Create different spatial operations based on district type

**Example:**

```python
from abc import ABC, abstractmethod
from typing import Protocol

class DataProcessor(Protocol):
    """Protocol for data processors."""
    def process(self, data: pl.DataFrame) -> pl.DataFrame: ...

class VoterFileProcessor:
    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        # Process voter file
        return data

class EarlyVotingProcessor:
    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        # Process early voting data
        return data

class DataProcessorFactory:
    """Factory for creating data processors."""
    
    @staticmethod
    def create(processor_type: str) -> DataProcessor:
        """Create a processor based on type."""
        processors = {
            "voterfile": VoterFileProcessor,
            "early_voting": EarlyVotingProcessor,
        }
        processor_class = processors.get(processor_type)
        if not processor_class:
            raise ValueError(f"Unknown processor type: {processor_type}")
        return processor_class()

# Usage
processor = DataProcessorFactory.create("voterfile")
result = processor.process(data)
```

#### Observer Pattern

**Use for event-driven processing and real-time updates:**

- **Pipeline Progress**: Notify observers of processing stages
- **Data Validation**: Notify observers of validation errors or warnings
- **Visualization Generation**: Notify observers when visualizations are created
- **Progress Tracking**: Track progress through data processing pipeline

**Example:**

```python
from abc import ABC, abstractmethod
from typing import List, Any

class PipelineObserver(ABC):
    """Observer for pipeline events."""
    
    @abstractmethod
    def on_step_start(self, step_name: str) -> None:
        """Called when a pipeline step starts."""
        pass
    
    @abstractmethod
    def on_step_complete(self, step_name: str, result: Any) -> None:
        """Called when a pipeline step completes."""
        pass
    
    @abstractmethod
    def on_error(self, step_name: str, error: Exception) -> None:
        """Called when an error occurs."""
        pass

class ProgressObserver(PipelineObserver):
    """Observer that tracks progress."""
    
    def on_step_start(self, step_name: str) -> None:
        print(f"Starting: {step_name}")
    
    def on_step_complete(self, step_name: str, result: Any) -> None:
        print(f"Completed: {step_name}")
    
    def on_error(self, step_name: str, error: Exception) -> None:
        print(f"Error in {step_name}: {error}")

class PipelineSubject:
    """Subject that notifies observers of pipeline events."""
    
    def __init__(self):
        self._observers: List[PipelineObserver] = []
    
    def attach(self, observer: PipelineObserver) -> None:
        """Attach an observer."""
        self._observers.append(observer)
    
    def notify_step_start(self, step_name: str) -> None:
        """Notify all observers of step start."""
        for observer in self._observers:
            observer.on_step_start(step_name)
    
    def notify_step_complete(self, step_name: str, result: Any) -> None:
        """Notify all observers of step completion."""
        for observer in self._observers:
            observer.on_step_complete(step_name, result)
    
    def notify_error(self, step_name: str, error: Exception) -> None:
        """Notify all observers of errors."""
        for observer in self._observers:
            observer.on_error(step_name, error)

# Usage
pipeline = PipelineSubject()
pipeline.attach(ProgressObserver())
pipeline.notify_step_start("process_voterfile")
```

#### Strategy Pattern

**Use for interchangeable algorithms and behaviors:**

- **Age Calculation**: Different strategies for calculating age from DOB
- **District Matching**: Different strategies for matching voters to districts
- **Visualization Rendering**: Different strategies for rendering charts/maps
- **Data Validation**: Different validation strategies for different data types

**Example:**

```python
from abc import ABC, abstractmethod

class AgeCalculationStrategy(ABC):
    """Strategy for calculating age from DOB."""
    
    @abstractmethod
    def calculate(self, dob_str: str, reference_date: datetime) -> int | None:
        """Calculate age from DOB string."""
        pass

class StandardAgeStrategy(AgeCalculationStrategy):
    """Standard age calculation strategy."""
    
    def calculate(self, dob_str: str, reference_date: datetime) -> int | None:
        if not dob_str or len(dob_str) != 8:
            return None
        try:
            year = int(dob_str[:4])
            month = int(dob_str[4:6])
            day = int(dob_str[6:8])
            dob = datetime(year, month, day)
            age = reference_date.year - dob.year
            if (reference_date.month, reference_date.day) < (dob.month, dob.day):
                age -= 1
            return age
        except (ValueError, TypeError):
            return None

class AgeCalculator:
    """Age calculator using strategy pattern."""
    
    def __init__(self, strategy: AgeCalculationStrategy):
        self._strategy = strategy
    
    def calculate_age(self, dob_str: str, reference_date: datetime = None) -> int | None:
        """Calculate age using the current strategy."""
        if reference_date is None:
            reference_date = datetime.now()
        return self._strategy.calculate(dob_str, reference_date)
    
    def set_strategy(self, strategy: AgeCalculationStrategy) -> None:
        """Change the calculation strategy."""
        self._strategy = strategy

# Usage
calculator = AgeCalculator(StandardAgeStrategy())
age = calculator.calculate_age("19900101", datetime(2024, 11, 1))
```

**When to Use Each Pattern:**

- **Factory Pattern**: Use when you need to create objects based on type/configuration, especially when the creation logic is complex or needs to be centralized
- **Observer Pattern**: Use when you need to notify multiple components of events or state changes, such as pipeline progress or data validation results
- **Strategy Pattern**: Use when you have multiple ways to perform the same operation and want to make them interchangeable, such as different calculation methods or matching algorithms

**Important**: Always prioritize simplicity. Use these patterns when they genuinely improve code organization and maintainability, not for the sake of using patterns.

## 📚 Resources

### Documentation
- [Polars Documentation](https://docs.pola.rs/)
- [GeoPandas Documentation](https://geopandas.org/)
- [Matplotlib Documentation](https://matplotlib.org/)
- [Folium Documentation](https://python-visualization.github.io/folium/)

### Data Sources
- Texas Secretary of State - Voter files
- Early voting data from county election offices
- Census Bureau - TIGER shapefiles

## 🤝 Agent Collaboration

### Communication
- **Use clear, descriptive commit messages**
- **Document all changes in PR descriptions**
- **Ask questions if requirements are unclear**
- **Coordinate with other agents when dependencies exist**

### Code Quality
- **Follow existing code patterns and conventions**
- **Write comprehensive tests for all new code**
- **Ensure all code passes linting and type checking**
- **Maintain backward compatibility when possible**
- **ALWAYS run pre-commit hooks before committing**:
  ```bash
  # Install hooks (run once per repo)
  uv run pre-commit install
  
  # Hooks run automatically on commit, or run manually:
  uv run pre-commit run --all-files
  ```
  Pre-commit hooks automatically run `ruff --fix` to fix linting issues, preventing CI failures.

### Data Handling
- **Never commit raw data files**
- **Use .gitignore for data directories**
- **Validate data before processing**
- **Handle missing data gracefully**

---

**This document is the single source of truth for all agent operations. All agents must follow these instructions exactly.**

