"""
Predict party affiliation for voters who vote in general elections but not primaries.
Uses trained ML model to predict how general-election-only voters would vote (R/D/Swing).
"""
import polars as pl
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Tuple
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

from tx_election_results.modeling.party_prediction_model import load_party_prediction_model
from tx_election_results.modeling.feature_engineering import prepare_features_for_ml
from tx_election_results.modeling.primary_voter_classifier import classify_primary_voters


def identify_general_election_voters(df: pl.DataFrame) -> pl.DataFrame:
    """
    Identify voters who have voted in general elections but not in primaries.
    
    Args:
        df: DataFrame with GEN columns and primary voting history
        
    Returns:
        DataFrame with has_gen_history and is_general_only flags
    """
    # Check for GEN columns (general election history)
    gen_cols = [col for col in df.columns if col.upper().startswith("GEN")]
    
    if not gen_cols:
        print("⚠️  No GEN columns found! Cannot identify general election history.")
        return df.with_columns([
            pl.lit(False).alias("has_gen_history"),
            pl.lit(False).alias("is_general_only")
        ])
    
    print(f"Found {len(gen_cols)} GEN columns: {gen_cols[:10]}...")
    
    # Check if voter has general election history
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
    
    # General-election-only voters: have GEN history but no primary history
    df = df.with_columns([
        has_gen_history.alias("has_gen_history"),
        (
            has_gen_history &
            ((pl.col('total_primary_votes') == 0) | pl.col('total_primary_votes').is_null())
        ).alias("is_general_only")
    ])
    
    return df


def predict_party_for_general_voters(
    df: pl.DataFrame,
    model_path: str,
    feature_columns: list[str],
    chunk_size: int = 200000
) -> pl.DataFrame:
    """
    Apply trained ML model to predict party affiliation for general-election-only voters.
    Predicts how these voters would vote (Republican, Democrat, or Swing).
    
    Args:
        df: DataFrame with features and general-election-only voters identified
        model_path: Path to trained XGBoost model
        feature_columns: List of feature column names
        chunk_size: Number of voters to process at a time
        
    Returns:
        DataFrame with predicted_party column added
    """
    print("=" * 80)
    print("PREDICTING PARTY AFFILIATION FOR GENERAL-ELECTION-ONLY VOTERS")
    print("=" * 80)
    print()
    
    # Load model
    model, metadata = load_party_prediction_model(model_path)
    
    # Identify general-election-only voters
    df = identify_general_election_voters(df)
    
    general_only_voters = df.filter(pl.col("is_general_only") == True)
    print(f"General-election-only voters to predict: {len(general_only_voters):,}")
    
    if len(general_only_voters) == 0:
        print("No general-election-only voters found. Nothing to predict.")
        return df.with_columns([
            pl.lit(None).alias("predicted_party"),
            pl.lit(None).alias("predicted_party_prob_rep"),
            pl.lit(None).alias("predicted_party_prob_dem")
        ])
    
    # Prepare features for prediction
    print("\nPreparing features for prediction...")
    
    # Select feature columns
    predict_data = general_only_voters.select(['VUID'] + feature_columns)
    
    # Convert to pandas
    predict_pd = predict_data.to_pandas()
    
    # Prepare feature matrix
    X = predict_pd[feature_columns].copy()
    
    # Fill missing values (same as training)
    for col in feature_columns:
        if X[col].dtype in ['float64', 'float32', 'int64', 'int32']:
            X[col] = X[col].fillna(X[col].median())
        else:
            X[col] = X[col].fillna(-1)
    
    X = X.fillna(0)
    
    # Predict in chunks
    print(f"\nPredicting party affiliation (processing in chunks of {chunk_size})...")
    
    all_predictions = []
    total_chunks = (len(X) - 1) // chunk_size + 1
    
    with tqdm(total=len(X), desc="Predicting voters", unit="voters", unit_scale=True) as pbar:
        for i in range(0, len(X), chunk_size):
            chunk = X.iloc[i:i+chunk_size]
            chunk_num = i // chunk_size + 1
            
            # Predict probabilities
            pbar.set_description(f"Predicting chunk {chunk_num}/{total_chunks}")
            proba = model.predict_proba(chunk)
            
            # Get class order
            class_order = model.classes_
            if len(class_order) == 2:
                # Binary classification: R and D
                if class_order[0] == 'Democrat':
                    dem_idx, rep_idx = 0, 1
                else:
                    dem_idx, rep_idx = 1, 0
            else:
                # Multi-class: find indices
                dem_idx = np.where(class_order == 'Democrat')[0][0] if 'Democrat' in class_order else 0
                rep_idx = np.where(class_order == 'Republican')[0][0] if 'Republican' in class_order else 1
            
            # Store predictions
            chunk_predictions = pd.DataFrame({
                'VUID': predict_pd['VUID'].iloc[i:i+chunk_size].values,
                'predicted_party_prob_rep': proba[:, rep_idx],
                'predicted_party_prob_dem': proba[:, dem_idx],
            })
            
            # Normalize probabilities (they should already sum to 1, but ensure)
            total_prob = chunk_predictions['predicted_party_prob_rep'] + chunk_predictions['predicted_party_prob_dem']
            chunk_predictions['predicted_party_prob_rep'] = chunk_predictions['predicted_party_prob_rep'] / total_prob
            chunk_predictions['predicted_party_prob_dem'] = chunk_predictions['predicted_party_prob_dem'] / total_prob
            
            # Classify as R, D, or Swing based on probabilities
            # If one party has >65% probability, classify as that party
            # Otherwise, classify as Swing
            chunk_predictions['predicted_party'] = chunk_predictions.apply(
                lambda row: 'Republican' if row['predicted_party_prob_rep'] >= 0.65
                else 'Democrat' if row['predicted_party_prob_dem'] >= 0.65
                else 'Swing',
                axis=1
            )
            
            all_predictions.append(chunk_predictions)
            pbar.update(len(chunk))
    
    # Combine all predictions
    print("\nCombining predictions...")
    predictions_df = pd.concat(all_predictions, ignore_index=True)
    predictions = pl.from_pandas(predictions_df)
    
    # Merge predictions back into main dataframe
    print("Merging predictions with main dataset...")
    df = df.join(predictions, on='VUID', how='left')
    
    # Summary statistics
    print("\nPrediction Summary:")
    print("-" * 80)
    predicted = df.filter(pl.col("predicted_party").is_not_null())
    if len(predicted) > 0:
        prediction_counts = predicted.group_by("predicted_party").agg([
            pl.count().alias("count"),
            (pl.count() / pl.len() * 100).alias("percentage")
        ]).sort("predicted_party")
        print(prediction_counts)
        
        print("\nAverage probabilities:")
        print(f"  Republican: {predicted['predicted_party_prob_rep'].mean():.4f}")
        print(f"  Democrat: {predicted['predicted_party_prob_dem'].mean():.4f}")
    
    print()
    print("=" * 80)
    print("Prediction complete!")
    print("=" * 80)
    print()
    
    return df


def create_final_party_classification(df: pl.DataFrame) -> pl.DataFrame:
    """
    Create final party classification combining primary history and predictions.
    
    Strategy:
    - If voter has primary history: use primary_classification (R/D/Swing)
    - If voter is general-election-only: use predicted_party (R/D/Swing)
    - Otherwise: Unknown
    
    Args:
        df: DataFrame with primary_classification and predicted_party
        
    Returns:
        DataFrame with party_final column
    """
    print("Creating final party classification...")
    
    df = df.with_columns([
        pl.when(
            pl.col('primary_classification').is_in(['Republican', 'Democrat', 'Swing'])
        )
        .then(pl.col('primary_classification'))
        .when(
            pl.col('predicted_party').is_in(['Republican', 'Democrat', 'Swing'])
        )
        .then(pl.col('predicted_party'))
        .otherwise(pl.lit('Unknown'))
        .alias('party_final')
    ])
    
    # Summary
    print("\nFinal Party Classification Summary:")
    print("-" * 80)
    final_counts = df.group_by("party_final").agg([
        pl.count().alias("count"),
        (pl.count() / pl.len() * 100).alias("percentage")
    ]).sort("party_final")
    print(final_counts)
    
    return df


if __name__ == "__main__":
    # Test prediction
    import sys
    from pathlib import Path
    
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        model_path = sys.argv[2] if len(sys.argv) > 2 else "data/exports/models/party_prediction_model.pkl"
    else:
        from tx_election_results.config import config
        input_path = str(config.MERGED_DATA)
        model_path = str(config.OUTPUT_DIR / "models" / "party_prediction_model.pkl")
    
    if not Path(input_path).exists():
        print(f"Error: {input_path} not found.")
        print("Please run the merge step first.")
        sys.exit(1)
    
    if not Path(model_path).exists():
        print(f"Error: {model_path} not found.")
        print("Please train the model first.")
        sys.exit(1)
    
    print(f"Loading data from {input_path}...")
    df = pl.read_parquet(input_path)
    print(f"Loaded {len(df):,} voters")
    
    # Classify primary voters
    if 'primary_classification' not in df.columns:
        df = classify_primary_voters(df)
    
    # Prepare features
    df_features, encoders, feature_cols = prepare_features_for_ml(df)
    
    # Load metadata to get feature columns
    metadata_path = Path(model_path).with_suffix('.metadata.joblib')
    if metadata_path.exists():
        import joblib
        metadata = joblib.load(metadata_path)
        feature_cols = metadata.get('feature_columns', feature_cols)
    
    # Predict
    df_predicted = predict_party_for_general_voters(
        df_features,
        model_path,
        feature_cols
    )
    
    # Create final classification
    df_final = create_final_party_classification(df_predicted)
    
    print(f"\n✅ Prediction complete!")
    print(f"Final classification added to {len(df_final):,} voters")
