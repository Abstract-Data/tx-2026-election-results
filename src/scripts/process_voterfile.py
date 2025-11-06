"""
Process voterfile to extract counties, calculate ages from DOB, and create age brackets.
"""
from pathlib import Path
from datetime import datetime
import polars as pl


def calculate_age(dob_str: str, reference_date: datetime = None) -> int:
    """
    Calculate age from DOB string in YYYYMMDD format.
    
    Args:
        dob_str: Date of birth as YYYYMMDD string
        reference_date: Reference date for age calculation (defaults to Nov 2024)
    
    Returns:
        Age in years
    """
    if reference_date is None:
        reference_date = datetime(2024, 11, 1)  # November 2024 election
    
    if not dob_str or len(dob_str) != 8:
        return None
    
    try:
        year = int(dob_str[:4])
        month = int(dob_str[4:6])
        day = int(dob_str[6:8])
        
        dob = datetime(year, month, day)
        age = reference_date.year - dob.year
        
        # Adjust if birthday hasn't occurred yet this year
        if (reference_date.month, reference_date.day) < (dob.month, dob.day):
            age -= 1
        
        return age
    except (ValueError, TypeError):
        return None


def create_age_bracket(age: int) -> str:
    """
    Create age bracket from age.
    
    Args:
        age: Age in years
    
    Returns:
        Age bracket string (e.g., "18-24", "25-34", etc.)
    """
    if age is None:
        return "Unknown"
    
    if age < 18:
        return "Under 18"
    elif age <= 24:
        return "18-24"
    elif age <= 34:
        return "25-34"
    elif age <= 44:
        return "35-44"
    elif age <= 54:
        return "45-54"
    elif age <= 64:
        return "55-64"
    elif age <= 74:
        return "65-74"
    else:
        return "75+"


def process_voterfile(voterfile_path: str, output_path: str = None) -> pl.DataFrame:
    """
    Process voterfile to extract counties, calculate ages, and create age brackets.
    
    Args:
        voterfile_path: Path to the voterfile CSV
        output_path: Optional path to save processed data as parquet
    
    Returns:
        Processed DataFrame with VUID, COUNTY, age, age_bracket, and district info
    """
    print(f"Reading voterfile from {voterfile_path}...")
    
    # Read the voterfile
    # Some columns can contain alphanumeric values, so we'll read them as strings
    # Use ignore_errors to handle any parsing issues gracefully
    df = pl.read_csv(
        voterfile_path,
        infer_schema_length=10000,
        try_parse_dates=False,
        ignore_errors=True,  # Convert problematic columns to strings automatically
        schema_overrides={
            "PCT": pl.Utf8,  # Precinct can be alphanumeric
            "MZIP": pl.Utf8,  # ZIP codes can be alphanumeric (e.g., Canadian postal codes)
            "RZIP": pl.Utf8,  # ZIP codes can be alphanumeric
        },
    )
    
    print(f"Loaded {len(df)} voters")
    
    # Calculate age from DOB
    print("Calculating ages from DOB...")
    df = df.with_columns([
        pl.col("DOB").map_elements(
            lambda x: calculate_age(str(x)) if x else None,
            return_dtype=pl.Int64
        ).alias("age")
    ])
    
    # Create age brackets
    print("Creating age brackets...")
    df = df.with_columns([
        pl.col("age").map_elements(
            create_age_bracket,
            return_dtype=pl.Utf8
        ).alias("age_bracket")
    ])
    
    # Select relevant columns (including last 4 primaries)
    columns_to_keep = [
        "VUID",
        "COUNTY",
        "PCT",    # Precinct - needed for 2026 district lookup
        "age",
        "age_bracket",
        "NEWCD",  # OLD Congressional District (from 2022/2024)
        "NEWSD",  # OLD State Senate District (from 2022/2024)
        "NEWHD",  # OLD State House District (from 2022/2024)
        "PRI24",  # 2024 Primary party
        "PRI22",  # 2022 Primary party
        "PRI20",  # 2020 Primary party (if available)
        "PRI18",  # 2018 Primary party (if available)
        "DOB",    # Keep DOB for reference
        # Address fields for spatial matching
        "RHNUM",  # Residential address number
        "RSTNAME", # Residential street name
        "RCITY",   # Residential city
        "RZIP",    # Residential ZIP
    ]
    
    # Add all GEN columns (general election history) - these indicate general election participation
    gen_columns = [col for col in df.columns if col.upper().startswith("GEN")]
    if gen_columns:
        print(f"Found {len(gen_columns)} GEN columns (general election history): {gen_columns[:10]}...")
        columns_to_keep.extend(gen_columns)
    
    # Only keep columns that exist
    available_columns = [col for col in columns_to_keep if col in df.columns]
    df_processed = df.select(available_columns)
    
    print(f"Processed {len(df_processed)} voters")
    print(f"Age bracket distribution:")
    print(df_processed.group_by("age_bracket").agg(pl.count()).sort("age_bracket"))
    
    # Save to parquet if output path provided
    if output_path:
        print(f"Saving processed voterfile to {output_path}...")
        df_processed.write_parquet(output_path)
        print("Saved!")
    
    return df_processed


if __name__ == "__main__":
    voterfile_path = "/Users/johneakin/PyCharmProjects/vep-2024/data/voterfiles/texas/texasnovember2024.csv"
    output_path = "processed_voterfile.parquet"
    
    df = process_voterfile(voterfile_path, output_path)
    print("\nFirst few rows:")
    print(df.head())

