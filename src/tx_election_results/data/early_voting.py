"""
Process early voting data from multiple daily CSV files.
Load, combine, and deduplicate early voting records.
"""
from pathlib import Path
import polars as pl


def process_early_voting(data_dir: str, output_path: str = None) -> pl.DataFrame:
    """
    Load all early voting CSV files, combine them, and deduplicate by voter ID.
    
    Args:
        data_dir: Directory containing early voting CSV files
        output_path: Optional path to save processed data as parquet
    
    Returns:
        Deduplicated DataFrame with early voting records
    """
    data_path = Path(data_dir)
    early_voting_dir = data_path / "early_voting"
    
    if not early_voting_dir.exists():
        raise FileNotFoundError(f"Early voting directory not found: {early_voting_dir}")
    
    # Find all CSV files
    csv_files = sorted(early_voting_dir.glob("*.csv"))
    
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {early_voting_dir}")
    
    print(f"Found {len(csv_files)} early voting CSV files")
    
    # Load all CSV files
    dataframes = []
    for csv_file in csv_files:
        print(f"Loading {csv_file.name}...")
        try:
            df = pl.read_csv(
                csv_file,
                ignore_errors=True,  # Handle alphanumeric precinct values
                schema_overrides={
                    "precinct": pl.Utf8,  # Precinct can be alphanumeric (e.g., "6B", "14A")
                }
            )
            # Add file name for reference (to keep most recent if deduplicating)
            df = df.with_columns([
                pl.lit(csv_file.name).alias("source_file")
            ])
            dataframes.append(df)
        except Exception as e:
            print(f"Warning: Error loading {csv_file.name}: {e}")
            continue
    
    if not dataframes:
        raise ValueError("No valid CSV files could be loaded")
    
    # Combine all dataframes
    print("Combining all early voting files...")
    combined_df = pl.concat(dataframes)
    print(f"Total records before deduplication: {len(combined_df)}")
    
    # Deduplicate by id_voter
    # Keep the first occurrence (or we could keep the most recent by sorting by source_file)
    print("Deduplicating by voter ID...")
    
    # Sort by source_file (alphabetically, which will keep later dates since files are named with dates)
    # Files are named like: STATEWIDE...EarlyVoting.10_31_2025.csv (later dates come later alphabetically)
    combined_df = combined_df.sort("source_file")
    
    # Deduplicate, keeping first (which will be the earliest date due to sort)
    # Actually, let's keep the last (most recent) - so we'll reverse the sort
    combined_df = combined_df.sort("source_file", descending=True)
    deduplicated_df = combined_df.unique(subset=["id_voter"], keep="first")
    
    print(f"Records after deduplication: {len(deduplicated_df)}")
    print(f"Removed {len(combined_df) - len(deduplicated_df)} duplicate records")
    
    # Show summary statistics
    print("\nEarly voting summary:")
    print(f"Total unique voters: {len(deduplicated_df)}")
    print(f"Voting method breakdown:")
    if "voting_method" in deduplicated_df.columns:
        print(deduplicated_df.group_by("voting_method").agg(pl.count()).sort("voting_method"))
    
    # Select relevant columns
    columns_to_keep = [
        "id_voter",
        "tx_name",  # County name
        "voting_method",
        "precinct",
        "source_file",
    ]
    
    available_columns = [col for col in columns_to_keep if col in deduplicated_df.columns]
    final_df = deduplicated_df.select(available_columns)
    
    # Save to parquet if output path provided
    if output_path:
        print(f"\nSaving processed early voting data to {output_path}...")
        final_df.write_parquet(output_path)
        print("Saved!")
    
    return final_df


if __name__ == "__main__":
    data_dir = "/Users/johneakin/Downloads/data"
    output_path = "processed_early_voting.parquet"
    
    df = process_early_voting(data_dir, output_path)
    print("\nFirst few rows:")
    print(df.head())

