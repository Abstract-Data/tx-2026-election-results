"""
Model party affiliation for voters without primary voting history.

Uses geographic proximity and demographic features to predict party affiliation
by comparing to known primary voters in the same area.
"""
import polars as pl
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Literal
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')


def calculate_geographic_features(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate geographic features for modeling.
    
    Creates features based on:
    - Precinct-level party composition
    - County-level party composition
    - ZIP code-level party composition
    """
    print("Calculating geographic features...")
    
        # Calculate precinct-level party statistics (include Swing voters)
    precinct_stats = df.filter(
        (pl.col('party') == 'Republican') | 
        (pl.col('party') == 'Democrat') |
        (pl.col('party') == 'Swing')
    ).group_by(['COUNTY', 'PCT']).agg([
        pl.count().alias('total_known_voters'),
        (pl.col('party') == 'Republican').sum().alias('precinct_republicans'),
        (pl.col('party') == 'Democrat').sum().alias('precinct_democrats'),
    ]).with_columns([
        (pl.col('precinct_republicans') / pl.col('total_known_voters')).alias('precinct_rep_pct'),
        (pl.col('precinct_democrats') / pl.col('total_known_voters')).alias('precinct_dem_pct'),
    ])
    
    # Calculate county-level party statistics (include Swing voters)
    county_stats = df.filter(
        (pl.col('party') == 'Republican') | 
        (pl.col('party') == 'Democrat') |
        (pl.col('party') == 'Swing')
    ).group_by('COUNTY').agg([
        pl.count().alias('county_total_known'),
        (pl.col('party') == 'Republican').sum().alias('county_republicans'),
        (pl.col('party') == 'Democrat').sum().alias('county_democrats'),
    ]).with_columns([
        (pl.col('county_republicans') / pl.col('county_total_known')).alias('county_rep_pct'),
        (pl.col('county_democrats') / pl.col('county_total_known')).alias('county_dem_pct'),
    ])
    
    # Calculate ZIP code-level party statistics (if available, include Swing voters)
    if 'RZIP' in df.columns:
        zip_stats = df.filter(
            (pl.col('party') == 'Republican') | 
            (pl.col('party') == 'Democrat') |
            (pl.col('party') == 'Swing')
        ).group_by('RZIP').agg([
            pl.count().alias('zip_total_known'),
            (pl.col('party') == 'Republican').sum().alias('zip_republicans'),
            (pl.col('party') == 'Democrat').sum().alias('zip_democrats'),
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
    """
    print("Calculating age-based features...")
    
    # Calculate age bracket party statistics (include Swing voters)
    age_stats = df.filter(
        (pl.col('party') == 'Republican') | 
        (pl.col('party') == 'Democrat') |
        (pl.col('party') == 'Swing')
    ).group_by('age_bracket').agg([
        pl.count().alias('age_bracket_total'),
        (pl.col('party') == 'Republican').sum().alias('age_bracket_republicans'),
        (pl.col('party') == 'Democrat').sum().alias('age_bracket_democrats'),
    ]).with_columns([
        (pl.col('age_bracket_republicans') / pl.col('age_bracket_total')).alias('age_bracket_rep_pct'),
        (pl.col('age_bracket_democrats') / pl.col('age_bracket_total')).alias('age_bracket_dem_pct'),
    ])
    
    # Join age bracket stats
    df = df.join(age_stats, on='age_bracket', how='left')
    
    return df


def create_party_score(
    rep_score: float,
    dem_score: float,
    threshold_likely: float = 0.65,
    threshold_lean: float = 0.55
) -> str:
    """
    Create party affiliation score based on Republican and Democrat scores.
    
    Args:
        rep_score: Republican score (0-1)
        dem_score: Democrat score (0-1)
        threshold_likely: Threshold for "Likely" classification
        threshold_lean: Threshold for "Lean" classification
    
    Returns:
        Party affiliation category
    """
    # Normalize scores (they should sum to ~1, but normalize to be safe)
    total = rep_score + dem_score
    if total > 0:
        rep_pct = rep_score / total
        dem_pct = dem_score / total
    else:
        return "Swing"
    
    if rep_pct >= threshold_likely:
        return "Likely Republican"
    elif rep_pct >= threshold_lean:
        return "Lean Republican"
    elif dem_pct >= threshold_likely:
        return "Likely Democrat"
    elif dem_pct >= threshold_lean:
        return "Lean Democrat"
    else:
        return "Swing"


def model_party_affiliation(
    merged_df_path: str,
    output_path: str = None,
    use_model: bool = True,
    fast_mode: bool = False
) -> pl.DataFrame:
    """
    Model party affiliation for voters without primary voting history.
    
    Uses a combination of:
    1. Geographic proximity (precinct, county, ZIP)
    2. Age demographics
    3. Machine learning model (Random Forest) trained on known voters
    
    Args:
        merged_df_path: Path to merged voter data parquet file
        output_path: Optional path to save results
        use_model: Whether to use ML model (True) or simple geographic averaging (False)
        fast_mode: If True, use geographic averaging only (no ML) for speed
    
    Returns:
        DataFrame with predicted party scores and categories
    """
    print("=" * 80)
    print("MODELING PARTY AFFILIATION FOR VOTERS WITHOUT PRIMARY HISTORY")
    print("=" * 80)
    print()
    
    # Load data
    print(f"Loading data from {merged_df_path}...")
    df = pl.read_parquet(merged_df_path)
    print(f"Loaded {len(df):,} voters")
    
    # Normalize party column - handle nulls
    df = df.with_columns([
        pl.when(pl.col('party').is_null())
        .then(pl.lit('Unknown'))
        .otherwise(pl.col('party'))
        .alias('party')
    ])
    
    # Separate known and unknown voters
    # Known voters: those with party affiliation based on last 4 primaries (Republican, Democrat, or Swing)
    known_voters = df.filter(
        (pl.col('party') == 'Republican') | 
        (pl.col('party') == 'Democrat') |
        (pl.col('party') == 'Swing')
    )
    
    # Check for GEN columns (general election history)
    gen_cols = [col for col in df.columns if col.upper().startswith("GEN")]
    
    # Build condition for voters with general election history
    # Only model voters who have voted in general elections but NOT in primaries
    if gen_cols:
        print(f"Found {len(gen_cols)} GEN columns: {gen_cols[:10]}...")
        # Has at least one GEN column with a value (not null/empty)
        has_gen_history = (
            pl.col(gen_cols[0]).is_not_null() &
            (pl.col(gen_cols[0]) != "")
        )
        for gen_col in gen_cols[1:]:
            has_gen_history = has_gen_history | (
                pl.col(gen_col).is_not_null() &
                (pl.col(gen_col) != "")
            )
    else:
        print("⚠️  No GEN columns found! Cannot identify general election history.")
        print("   Will model all unknown voters (not recommended)")
        has_gen_history = pl.lit(True)  # Fallback: include all if no GEN columns
    
    # Only model "secret" voters: those with general election history but no primary history
    # Criteria:
    # 1. party == 'Unknown' (no primary voting history)
    # 2. total_primary_votes == 0 or null (confirmed no primary votes)
    # 3. Has general election history (at least one GEN column with value)
    unknown_voters = df.filter(
        (pl.col('party') == 'Unknown') | 
        (pl.col('party') == 'Other') |
        (pl.col('party').is_null())
    ).filter(
        ((pl.col('total_primary_votes') == 0) | pl.col('total_primary_votes').is_null()) &
        has_gen_history
    )
    
    # Count total unknown voters (for comparison)
    total_unknown = df.filter(
        (pl.col('party') == 'Unknown') | 
        (pl.col('party') == 'Other') |
        (pl.col('party').is_null())
    )
    
    print(f"Known voters (R/D/Swing): {len(known_voters):,} ({len(known_voters)/len(df)*100:.2f}%)")
    print(f"  Breakdown by party:")
    if len(known_voters) > 0:
        party_counts = known_voters['party'].value_counts()
        print(party_counts)
    print(f"Total unknown voters (no primary history): {len(total_unknown):,} ({len(total_unknown)/len(df)*100:.2f}%)")
    print(f"Secret voters to model (GEN history but no primary): {len(unknown_voters):,} ({len(unknown_voters)/len(df)*100:.2f}%)")
    print(f"  Excluded: {len(total_unknown) - len(unknown_voters):,} voters with no voting history at all")
    print()
    
    if len(unknown_voters) == 0:
        print("No secret voters to model!")
        print("(No voters found with general election history but no primary history)")
        return df
    
    # Calculate geographic and demographic features
    print("Step 1: Calculating geographic and demographic features...")
    df = calculate_geographic_features(df)
    df = calculate_age_features(df)
    
    # Split back into known and unknown (using same filter logic for secret voters)
    known_voters = df.filter(
        (pl.col('party') == 'Republican') | (pl.col('party') == 'Democrat')
    )
    
    # Rebuild GEN history condition (same as before)
    gen_cols = [col for col in df.columns if col.upper().startswith("GEN")]
    if gen_cols:
        has_gen_history = (
            pl.col(gen_cols[0]).is_not_null() &
            (pl.col(gen_cols[0]) != "")
        )
        for gen_col in gen_cols[1:]:
            has_gen_history = has_gen_history | (
                pl.col(gen_col).is_not_null() &
                (pl.col(gen_col) != "")
            )
    else:
        has_gen_history = pl.lit(True)  # Fallback if no GEN columns
    
    # Only include secret voters: GEN history but no primary history
    unknown_voters = df.filter(
        (pl.col('party') == 'Unknown') | 
        (pl.col('party') == 'Other') |
        (pl.col('party').is_null())
    ).filter(
        ((pl.col('total_primary_votes') == 0) | pl.col('total_primary_votes').is_null()) &
        has_gen_history
    )
    
    # Force full ML mode (slow but accurate) - disable fast mode
    if fast_mode:
        print()
        print("⚠️  Fast mode requested but using full ML model for accuracy")
        print("   Use --fast flag for geographic averaging only")
        fast_mode = False
    
    if use_model:
        print()
        print("Step 2: Training machine learning model...")
        
        # Prepare training data (use only R/D voters for training, exclude Swing)
        train_df = known_voters.filter(
            (pl.col('party') == 'Republican') | (pl.col('party') == 'Democrat')
        ).select([
            'age',
            'precinct_rep_pct', 'precinct_dem_pct',
            'county_rep_pct', 'county_dem_pct',
            'zip_rep_pct', 'zip_dem_pct',
            'age_bracket_rep_pct', 'age_bracket_dem_pct',
            'party'
        ]).drop_nulls()
        
        if len(train_df) == 0:
            print("Warning: No training data available. Using geographic averaging instead.")
            use_model = False
        else:
            # Convert to pandas for sklearn
            train_pd = train_df.to_pandas()
            
            # Prepare features and target
            feature_cols = [
                'age',
                'precinct_rep_pct', 'precinct_dem_pct',
                'county_rep_pct', 'county_dem_pct',
                'zip_rep_pct', 'zip_dem_pct',
                'age_bracket_rep_pct', 'age_bracket_dem_pct',
            ]
            
            # Fill missing values in feature columns only (exclude party column)
            X = train_pd[feature_cols].copy()
            X = X.fillna(X.select_dtypes(include=[np.number]).median())
            X = X.fillna(0.5)  # Fill any remaining NaNs with neutral
            y = train_pd['party']
            
            # Train model - FULL ML mode for accuracy
            # Use larger sample for better accuracy
            print(f"Preparing training data ({len(X):,} known voters)...")
            if len(X) > 1000000:
                print(f"Large dataset detected ({len(X):,} voters). Sampling 1M for training...")
                X_sample, _, y_sample, _ = train_test_split(
                    X, y, train_size=1000000, random_state=42, stratify=y
                )
            else:
                X_sample, y_sample = X, y
            
            X_train, X_test, y_train, y_test = train_test_split(
                X_sample, y_sample, test_size=0.2, random_state=42, stratify=y_sample
            )
            
            # Use full ML model settings for maximum accuracy
            print(f"Training Random Forest model on {len(X_train):,} voters...")
            print("   This may take 10-20 minutes for maximum accuracy...")
            model = RandomForestClassifier(
                n_estimators=200,  # Increased for accuracy
                max_depth=15,      # Increased for accuracy
                min_samples_split=50,   # More detailed splits
                min_samples_leaf=20,    # More detailed leaves
                random_state=42,
                n_jobs=-1,
                verbose=0
            )
            
            # Train with progress indication
            print("   Training model...")
            model.fit(X_train, y_train)
            print("   ✅ Model training complete!")
            
            # Full evaluation
            print("   Evaluating model accuracy...")
            train_score = model.score(X_train, y_train)
            test_score = model.score(X_test, y_test)
            print(f"   Model accuracy - Train: {train_score:.4f}, Test: {test_score:.4f}")
            
            # Predict probabilities for unknown voters
            print()
            print("Step 3: Predicting party affiliation for unknown voters...")
            print(f"Processing {len(unknown_voters):,} unknown voters...")
            
            # Process in chunks with progress bar
            chunk_size = 200000  # Balanced chunk size
            all_predictions = []
            total_chunks = (len(unknown_voters) - 1) // chunk_size + 1
            
            # Get class order once
            class_order = model.classes_
            if class_order[0] == 'Democrat':
                dem_idx, rep_idx = 0, 1
            else:
                dem_idx, rep_idx = 1, 0
            
            # Process with progress bar
            with tqdm(total=len(unknown_voters), desc="Predicting voters", unit="voters", unit_scale=True) as pbar:
                for i in range(0, len(unknown_voters), chunk_size):
                    chunk = unknown_voters.slice(i, chunk_size)
                    chunk_num = i // chunk_size + 1
                    
                    unknown_pd = chunk.select([
                        'VUID',
                        'age',
                        'precinct_rep_pct', 'precinct_dem_pct',
                        'county_rep_pct', 'county_dem_pct',
                        'zip_rep_pct', 'zip_dem_pct',
                        'age_bracket_rep_pct', 'age_bracket_dem_pct',
                    ]).to_pandas()
                    
                    unknown_X = unknown_pd[feature_cols].fillna(0.5)
                    
                    # Predict with progress indication
                    pbar.set_description(f"Predicting chunk {chunk_num}/{total_chunks}")
                    proba = model.predict_proba(unknown_X)
                    pbar.update(len(chunk))
                    
                    unknown_pd['predicted_dem_prob'] = proba[:, dem_idx]
                    unknown_pd['predicted_rep_prob'] = proba[:, rep_idx]
                    
                    # Normalize probabilities
                    total_prob = unknown_pd['predicted_rep_prob'] + unknown_pd['predicted_dem_prob']
                    unknown_pd['predicted_rep_prob'] = unknown_pd['predicted_rep_prob'] / total_prob
                    unknown_pd['predicted_dem_prob'] = unknown_pd['predicted_dem_prob'] / total_prob
                    
                    # Create party scores (vectorized for speed)
                    rep_pct = unknown_pd['predicted_rep_prob']
                    dem_pct = unknown_pd['predicted_dem_prob']
                    
                    # Create scores using vectorized operations
                    unknown_pd['predicted_party_score'] = pd.Series(index=unknown_pd.index, dtype='object')
                    unknown_pd.loc[rep_pct >= 0.65, 'predicted_party_score'] = 'Likely Republican'
                    unknown_pd.loc[(rep_pct >= 0.55) & (rep_pct < 0.65), 'predicted_party_score'] = 'Lean Republican'
                    unknown_pd.loc[(dem_pct >= 0.65), 'predicted_party_score'] = 'Likely Democrat'
                    unknown_pd.loc[(dem_pct >= 0.55) & (dem_pct < 0.65), 'predicted_party_score'] = 'Lean Democrat'
                    unknown_pd['predicted_party_score'] = unknown_pd['predicted_party_score'].fillna('Swing')
                    
                    all_predictions.append(unknown_pd[['VUID', 'predicted_rep_prob', 'predicted_dem_prob', 'predicted_party_score']])
            
            print("   ✅ Prediction complete!")
            
            # Combine all predictions
            print("   Combining predictions...")
            predictions_pd = pd.concat(all_predictions, ignore_index=True)
            predictions = pl.from_pandas(predictions_pd)
    
    if not use_model:
        print()
        print("Step 2: Using geographic averaging (no ML model)...")
        
        # Simple geographic averaging approach
        unknown_voters = unknown_voters.with_columns([
            # Use precinct if available, otherwise county, otherwise age bracket
            pl.coalesce([
                pl.col('precinct_rep_pct'),
                pl.col('zip_rep_pct'),
                pl.col('county_rep_pct'),
                pl.col('age_bracket_rep_pct'),
                pl.lit(0.5)  # Default to neutral
            ]).alias('predicted_rep_prob'),
            pl.coalesce([
                pl.col('precinct_dem_pct'),
                pl.col('zip_dem_pct'),
                pl.col('county_dem_pct'),
                pl.col('age_bracket_dem_pct'),
                pl.lit(0.5)  # Default to neutral
            ]).alias('predicted_dem_prob'),
        ])
        
        # Normalize probabilities
        total_prob = unknown_voters['predicted_rep_prob'] + unknown_voters['predicted_dem_prob']
        unknown_voters = unknown_voters.with_columns([
            (pl.col('predicted_rep_prob') / total_prob).alias('predicted_rep_prob'),
            (pl.col('predicted_dem_prob') / total_prob).alias('predicted_dem_prob'),
        ])
        
        # Create party scores using vectorized operations (faster)
        predictions = unknown_voters.select(['VUID', 'predicted_rep_prob', 'predicted_dem_prob']).with_columns([
            pl.when(pl.col('predicted_rep_prob') >= 0.65)
            .then(pl.lit('Likely Republican'))
            .when((pl.col('predicted_rep_prob') >= 0.55) & (pl.col('predicted_rep_prob') < 0.65))
            .then(pl.lit('Lean Republican'))
            .when(pl.col('predicted_dem_prob') >= 0.65)
            .then(pl.lit('Likely Democrat'))
            .when((pl.col('predicted_dem_prob') >= 0.55) & (pl.col('predicted_dem_prob') < 0.65))
            .then(pl.lit('Lean Democrat'))
            .otherwise(pl.lit('Swing'))
            .alias('predicted_party_score')
        ])
    
    # Merge predictions back into main dataframe
    print()
    print("Step 4: Merging predictions with main dataset...")
    df = df.join(predictions, on='VUID', how='left')
    
    # Create final party assignment
    # Preserve known party (R/D/Swing) if available, otherwise use predicted score
    df = df.with_columns([
        pl.when(pl.col('party').is_in(['Republican', 'Democrat', 'Swing']))
        .then(pl.col('party'))
        .when(pl.col('predicted_party_score').is_not_null())
        .then(pl.col('predicted_party_score'))
        .otherwise(pl.lit('Unknown'))
        .alias('party_final')
    ])
    
    # Summary statistics
    print()
    print("=" * 80)
    print("MODELING RESULTS")
    print("=" * 80)
    print()
    print("Party Distribution (Final):")
    print(df['party_final'].value_counts().sort('count', descending=True))
    print()
    
    print("Predicted Party Scores (for previously unknown voters):")
    predicted = df.filter(pl.col('predicted_party_score').is_not_null())
    if len(predicted) > 0:
        print(predicted['predicted_party_score'].value_counts().sort('count', descending=True))
    print()
    
    # Save results
    if output_path:
        print(f"Saving results to {output_path}...")
        df.write_parquet(output_path)
        print("Saved!")
    
    return df


if __name__ == "__main__":
    import sys
    
    merged_path = "early_voting_merged.parquet"
    output_path = "voters_with_party_modeling.parquet"
    
    # Check for fast mode flag
    fast_mode = "--fast" in sys.argv or "-f" in sys.argv
    
    if fast_mode:
        print("🚀 FAST MODE ENABLED - Using geographic averaging only (no ML)")
        print("   This will be much faster (5-10 minutes) but less accurate.")
        print("   Remove --fast flag for ML-based predictions (more accurate but slower).")
        print()
    
    result_df = model_party_affiliation(
        merged_df_path=merged_path,
        output_path=output_path,
        use_model=not fast_mode,
        fast_mode=fast_mode
    )
    
    print(f"\n✅ Modeling complete! Results saved to {output_path}")

