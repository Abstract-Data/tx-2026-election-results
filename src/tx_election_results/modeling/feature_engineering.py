"""
Feature engineering for ML model training.
Encodes features for XGBoost: County, Age, City, Primary history, and geographic party composition.
NOTE: Districts are NOT used as features.
"""
import polars as pl
import numpy as np
from typing import Dict, List, Optional
from sklearn.preprocessing import LabelEncoder


def calculate_geographic_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate geographic features based on party composition at different levels.
    
    Creates features:
    - Precinct-level party composition (precinct_rep_pct, precinct_dem_pct)
    - County-level party composition (county_rep_pct, county_dem_pct)
    - ZIP-level party composition (zip_rep_pct, zip_dem_pct)
    
    Args:
        df: DataFrame with party classification and geographic columns
        
    Returns:
        DataFrame with geographic features added
    """
    print("Calculating geographic features...")
    
    # Calculate precinct-level party statistics (include Swing voters)
    precinct_stats = df.filter(
        (pl.col('primary_classification') == 'Republican') | 
        (pl.col('primary_classification') == 'Democrat') |
        (pl.col('primary_classification') == 'Swing')
    ).group_by(['COUNTY', 'PCT']).agg([
        pl.count().alias('total_known_voters'),
        (pl.col('primary_classification') == 'Republican').sum().alias('precinct_republicans'),
        (pl.col('primary_classification') == 'Democrat').sum().alias('precinct_democrats'),
    ]).with_columns([
        (pl.col('precinct_republicans') / pl.col('total_known_voters')).alias('precinct_rep_pct'),
        (pl.col('precinct_democrats') / pl.col('total_known_voters')).alias('precinct_dem_pct'),
    ])
    
    # Calculate county-level party statistics (include Swing voters)
    county_stats = df.filter(
        (pl.col('primary_classification') == 'Republican') | 
        (pl.col('primary_classification') == 'Democrat') |
        (pl.col('primary_classification') == 'Swing')
    ).group_by('COUNTY').agg([
        pl.count().alias('county_total_known'),
        (pl.col('primary_classification') == 'Republican').sum().alias('county_republicans'),
        (pl.col('primary_classification') == 'Democrat').sum().alias('county_democrats'),
    ]).with_columns([
        (pl.col('county_republicans') / pl.col('county_total_known')).alias('county_rep_pct'),
        (pl.col('county_democrats') / pl.col('county_total_known')).alias('county_dem_pct'),
    ])
    
    # Calculate ZIP code-level party statistics (if available, include Swing voters)
    if 'RZIP' in df.columns:
        zip_stats = df.filter(
            (pl.col('primary_classification') == 'Republican') | 
            (pl.col('primary_classification') == 'Democrat') |
            (pl.col('primary_classification') == 'Swing')
        ).group_by('RZIP').agg([
            pl.count().alias('zip_total_known'),
            (pl.col('primary_classification') == 'Republican').sum().alias('zip_republicans'),
            (pl.col('primary_classification') == 'Democrat').sum().alias('zip_democrats'),
        ]).with_columns([
            (pl.col('zip_republicans') / pl.col('zip_total_known')).alias('zip_rep_pct'),
            (pl.col('zip_democrats') / pl.col('zip_total_known')).alias('zip_dem_pct'),
        ])
        
        # Join ZIP stats
        df = df.join(zip_stats, on='RZIP', how='left')
    else:
        df = df.with_columns([
            pl.lit(None).alias('zip_total_known'),
            pl.lit(None).alias('zip_rep_pct'),
            pl.lit(None).alias('zip_dem_pct'),
        ])
    
    # Join precinct and county stats
    df = df.join(precinct_stats, on=['COUNTY', 'PCT'], how='left')
    df = df.join(county_stats, on='COUNTY', how='left')
    
    return df


def calculate_age_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate age-based features.
    
    Creates features based on age bracket party composition.
    
    Args:
        df: DataFrame with age and party classification
        
    Returns:
        DataFrame with age-based features added
    """
    print("Calculating age-based features...")
    
    # Calculate age bracket party statistics (include Swing voters)
    age_stats = df.filter(
        (pl.col('primary_classification') == 'Republican') | 
        (pl.col('primary_classification') == 'Democrat') |
        (pl.col('primary_classification') == 'Swing')
    ).group_by('age_bracket').agg([
        pl.count().alias('age_bracket_total'),
        (pl.col('primary_classification') == 'Republican').sum().alias('age_bracket_republicans'),
        (pl.col('primary_classification') == 'Democrat').sum().alias('age_bracket_democrats'),
    ]).with_columns([
        (pl.col('age_bracket_republicans') / pl.col('age_bracket_total')).alias('age_bracket_rep_pct'),
        (pl.col('age_bracket_democrats') / pl.col('age_bracket_total')).alias('age_bracket_dem_pct'),
    ])
    
    # Join age bracket stats
    df = df.join(age_stats, on='age_bracket', how='left')
    
    return df


def create_primary_history_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Create features from primary voting history.
    
    Creates features:
    - Years since last primary vote
    - Primary vote consistency (all R, all D, mixed)
    - Primary participation rate (votes / available primaries)
    
    Args:
        df: DataFrame with primary voting history
        
    Returns:
        DataFrame with primary history features added
    """
    print("Creating primary history features...")
    
    # Calculate years since last primary (if available)
    # For now, use total_primary_votes as a proxy for recency
    # More recent voters likely have higher counts
    
    # Primary participation rate (votes / available primaries)
    pri_cols = ["PRI24", "PRI22", "PRI20", "PRI18"]
    pri_cols_available = [col for col in pri_cols if col in df.columns]
    num_available_primaries = len(pri_cols_available)
    
    if num_available_primaries > 0:
        df = df.with_columns([
            (pl.col('total_primary_votes') / num_available_primaries).alias('primary_participation_rate')
        ])
    else:
        df = df.with_columns([
            pl.lit(0.0).alias('primary_participation_rate')
        ])
    
    # Primary vote consistency (1.0 = all same party, 0.0 = mixed)
    df = df.with_columns([
        pl.when(
            (pl.col('rep_primary_votes') > 0) & (pl.col('dem_primary_votes') == 0)
        )
        .then(pl.lit(1.0))  # All Republican
        .when(
            (pl.col('dem_primary_votes') > 0) & (pl.col('rep_primary_votes') == 0)
        )
        .then(pl.lit(1.0))  # All Democrat
        .when(
            (pl.col('rep_primary_votes') > 0) & (pl.col('dem_primary_votes') > 0)
        )
        .then(pl.lit(0.0))  # Mixed
        .otherwise(pl.lit(None))  # No votes
        .alias('primary_consistency')
    ])
    
    return df


def encode_categorical_features(
    df: pl.DataFrame,
    label_encoders: Optional[Dict[str, LabelEncoder]] = None
) -> tuple[pl.DataFrame, Dict[str, LabelEncoder]]:
    """
    Encode categorical features for XGBoost using label encoding.
    
    Encodes:
    - County (label encoded)
    - City (RCITY) (label encoded)
    - age_bracket (label encoded)
    
    Args:
        df: DataFrame with categorical columns
        label_encoders: Optional dict of pre-fitted label encoders
        
    Returns:
        Tuple of (encoded DataFrame, label encoders dict)
    """
    print("Encoding categorical features...")
    
    if label_encoders is None:
        label_encoders = {}
    
    # Encode County
    if 'COUNTY' in df.columns:
        if 'COUNTY' not in label_encoders:
            label_encoders['COUNTY'] = LabelEncoder()
            counties = df['COUNTY'].drop_nulls().unique().to_list()
            label_encoders['COUNTY'].fit(counties)
        
        df = df.with_columns([
            pl.col('COUNTY').map_elements(
                lambda x: label_encoders['COUNTY'].transform([x])[0] if x and x in label_encoders['COUNTY'].classes_ else -1,
                return_dtype=pl.Int32
            ).alias('county_encoded')
        ])
    
    # Encode City (RCITY)
    if 'RCITY' in df.columns:
        if 'RCITY' not in label_encoders:
            label_encoders['RCITY'] = LabelEncoder()
            cities = df['RCITY'].drop_nulls().unique().to_list()
            label_encoders['RCITY'].fit(cities)
        
        df = df.with_columns([
            pl.col('RCITY').map_elements(
                lambda x: label_encoders['RCITY'].transform([x])[0] if x and x in label_encoders['RCITY'].classes_ else -1,
                return_dtype=pl.Int32
            ).alias('city_encoded')
        ])
    
    # Encode age_bracket
    if 'age_bracket' in df.columns:
        if 'age_bracket' not in label_encoders:
            label_encoders['age_bracket'] = LabelEncoder()
            age_brackets = df['age_bracket'].drop_nulls().unique().to_list()
            label_encoders['age_bracket'].fit(age_brackets)
        
        df = df.with_columns([
            pl.col('age_bracket').map_elements(
                lambda x: label_encoders['age_bracket'].transform([x])[0] if x and x in label_encoders['age_bracket'].classes_ else -1,
                return_dtype=pl.Int32
            ).alias('age_bracket_encoded')
        ])
    
    return df, label_encoders


def prepare_features_for_ml(
    df: pl.DataFrame,
    label_encoders: Optional[Dict[str, LabelEncoder]] = None
) -> tuple[pl.DataFrame, Dict[str, LabelEncoder], List[str]]:
    """
    Prepare all features for ML model training.
    
    This is the main function that orchestrates all feature engineering steps.
    
    Args:
        df: Input DataFrame with voter data
        label_encoders: Optional pre-fitted label encoders
        
    Returns:
        Tuple of (feature DataFrame, label encoders, feature column names)
    """
    print("=" * 80)
    print("FEATURE ENGINEERING FOR ML MODEL")
    print("=" * 80)
    print()
    
    # Step 1: Calculate geographic features
    df = calculate_geographic_features(df)
    
    # Step 2: Calculate age-based features
    df = calculate_age_features(df)
    
    # Step 3: Create primary history features
    df = create_primary_history_features(df)
    
    # Step 4: Encode categorical features
    df, label_encoders = encode_categorical_features(df, label_encoders)
    
    # Define feature columns for ML model
    # NOTE: Districts are NOT included as features
    feature_columns = [
        'age',  # Numeric
        'county_encoded',  # Categorical (label encoded)
        'city_encoded',  # Categorical (label encoded)
        'age_bracket_encoded',  # Categorical (label encoded)
        'precinct_rep_pct',  # Geographic feature
        'precinct_dem_pct',  # Geographic feature
        'county_rep_pct',  # Geographic feature
        'county_dem_pct',  # Geographic feature
        'zip_rep_pct',  # Geographic feature (may be null)
        'zip_dem_pct',  # Geographic feature (may be null)
        'age_bracket_rep_pct',  # Age-based feature
        'age_bracket_dem_pct',  # Age-based feature
        'rep_primary_votes',  # Primary history
        'dem_primary_votes',  # Primary history
        'total_primary_votes',  # Primary history
        'primary_participation_rate',  # Primary history
        'primary_consistency',  # Primary history
    ]
    
    # Filter to only include columns that exist
    feature_columns = [col for col in feature_columns if col in df.columns]
    
    print(f"\nFeature columns for ML model ({len(feature_columns)}):")
    for col in feature_columns:
        print(f"  - {col}")
    
    print()
    print("=" * 80)
    print("Feature engineering complete!")
    print("=" * 80)
    print()
    
    return df, label_encoders, feature_columns


if __name__ == "__main__":
    # Test with sample data
    import sys
    from pathlib import Path
    
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        from tx_election_results.config import config
        input_path = str(config.MERGED_DATA)
    
    if not Path(input_path).exists():
        print(f"Error: {input_path} not found.")
        print("Please run the merge step first.")
        sys.exit(1)
    
    print(f"Loading data from {input_path}...")
    df = pl.read_parquet(input_path)
    print(f"Loaded {len(df):,} voters")
    
    # Add primary classification if not present
    if 'primary_classification' not in df.columns:
        from tx_election_results.modeling.primary_voter_classifier import classify_primary_voters
        df = classify_primary_voters(df)
    
    df_features, encoders, feature_cols = prepare_features_for_ml(df)
    
    print(f"\nFeature engineering complete!")
    print(f"Feature columns: {feature_cols}")
    print(f"\nFirst few rows of features:")
    print(df_features.select(['VUID'] + feature_cols).head(10))

