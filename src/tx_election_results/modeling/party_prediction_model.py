"""
Train XGBoost model to predict party affiliation for voters without primary history.
"""
import polars as pl
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Optional, Dict, Tuple
import joblib
import warnings
warnings.filterwarnings('ignore')

try:
    import xgboost as xgb
    from xgboost import XGBClassifier
except ImportError:
    raise ImportError("xgboost is required. Install with: pip install xgboost")

from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from tqdm import tqdm

from tx_election_results.modeling.feature_engineering import prepare_features_for_ml
from tx_election_results.modeling.primary_voter_classifier import classify_primary_voters


def train_party_prediction_model(
    df: pl.DataFrame,
    feature_columns: List[str],
    label_encoders: Dict,
    output_model_path: Optional[str] = None,
    test_size: float = 0.2,
    random_state: int = 42,
    n_estimators: int = 200,
    max_depth: int = 10,
    learning_rate: float = 0.1,
    subsample: float = 0.8,
    colsample_bytree: float = 0.8,
    use_gpu: bool = False
) -> Tuple[XGBClassifier, Dict]:
    """
    Train XGBoost model to predict party affiliation.
    
    Args:
        df: DataFrame with features and primary_classification
        feature_columns: List of feature column names
        label_encoders: Dict of label encoders for categorical features
        output_model_path: Optional path to save trained model
        test_size: Proportion of data for testing
        random_state: Random seed
        n_estimators: Number of boosting rounds
        max_depth: Maximum tree depth
        learning_rate: Learning rate
        subsample: Subsample ratio of training instances
        colsample_bytree: Subsample ratio of columns when constructing each tree
        use_gpu: Whether to use GPU acceleration
        
    Returns:
        Tuple of (trained model, metadata dict)
    """
    print("=" * 80)
    print("TRAINING XGBOOST PARTY PREDICTION MODEL")
    print("=" * 80)
    print()
    
    # Filter to known primary voters (R and D only, exclude Swing for training)
    # We train on R/D voters, then predict R/D/Swing for general-election-only voters
    known_voters = df.filter(
        (pl.col('primary_classification') == 'Republican') |
        (pl.col('primary_classification') == 'Democrat')
    )
    
    print(f"Training data: {len(known_voters):,} known primary voters (R/D only)")
    
    if len(known_voters) == 0:
        raise ValueError("No known primary voters found for training!")
    
    # Check class distribution
    class_dist = known_voters.group_by('primary_classification').agg(pl.count())
    print("\nClass distribution:")
    print(class_dist)
    
    # Prepare training data
    print("\nPreparing training data...")
    
    # Select features and target
    train_data = known_voters.select(['VUID', 'primary_classification'] + feature_columns)
    
    # Convert to pandas for XGBoost
    train_pd = train_data.to_pandas()
    
    # Separate features and target
    X = train_pd[feature_columns].copy()
    y = train_pd['primary_classification'].copy()
    
    # Fill missing values
    print("Handling missing values...")
    for col in feature_columns:
        if X[col].dtype in ['float64', 'float32', 'int64', 'int32']:
            X[col] = X[col].fillna(X[col].median())
        else:
            X[col] = X[col].fillna(-1)
    
    # Fill any remaining NaNs
    X = X.fillna(0)
    
    # Split into train and test sets
    print(f"\nSplitting data (test_size={test_size})...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )
    
    print(f"Training set: {len(X_train):,} samples")
    print(f"Test set: {len(X_test):,} samples")
    
    # Calculate class weights for imbalanced data
    class_counts = y_train.value_counts()
    total_samples = len(y_train)
    class_weights = {}
    for class_name in class_counts.index:
        class_weights[class_name] = total_samples / (len(class_counts) * class_counts[class_name])
    
    print(f"\nClass weights: {class_weights}")
    
    # Train XGBoost model
    print("\nTraining XGBoost model...")
    print(f"  - n_estimators: {n_estimators}")
    print(f"  - max_depth: {max_depth}")
    print(f"  - learning_rate: {learning_rate}")
    print(f"  - subsample: {subsample}")
    print(f"  - colsample_bytree: {colsample_bytree}")
    
    # XGBoost parameters
    xgb_params = {
        'n_estimators': n_estimators,
        'max_depth': max_depth,
        'learning_rate': learning_rate,
        'subsample': subsample,
        'colsample_bytree': colsample_bytree,
        'random_state': random_state,
        'objective': 'multi:softprob',
        'num_class': 2,  # R and D only
        'eval_metric': 'mlogloss',
        'tree_method': 'hist',
        'verbosity': 0,
    }
    
    if use_gpu:
        xgb_params['tree_method'] = 'gpu_hist'
        xgb_params['gpu_id'] = 0
    
    # Create and train model
    model = XGBClassifier(**xgb_params)
    
    # Fit with progress callback
    print("  Training...")
    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=False
    )
    
    print("  ✅ Model training complete!")
    
    # Evaluate model
    print("\nEvaluating model...")
    
    # Predictions
    y_train_pred = model.predict(X_train)
    y_test_pred = model.predict(X_test)
    
    # Accuracy
    train_accuracy = accuracy_score(y_train, y_train_pred)
    test_accuracy = accuracy_score(y_test, y_test_pred)
    
    print(f"\nModel Accuracy:")
    print(f"  Training: {train_accuracy:.4f}")
    print(f"  Test: {test_accuracy:.4f}")
    
    # Classification report
    print("\nClassification Report (Test Set):")
    print(classification_report(y_test, y_test_pred))
    
    # Confusion matrix
    print("\nConfusion Matrix (Test Set):")
    cm = confusion_matrix(y_test, y_test_pred)
    print(cm)
    
    # Feature importance
    print("\nTop 10 Most Important Features:")
    feature_importance = pd.DataFrame({
        'feature': feature_columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    print(feature_importance.head(10))
    
    # Cross-validation score
    print("\nPerforming cross-validation...")
    cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='accuracy')
    print(f"Cross-validation accuracy: {cv_scores.mean():.4f} (+/- {cv_scores.std() * 2:.4f})")
    
    # Prepare metadata
    metadata = {
        'feature_columns': feature_columns,
        'label_encoders': label_encoders,
        'train_accuracy': float(train_accuracy),
        'test_accuracy': float(test_accuracy),
        'cv_accuracy_mean': float(cv_scores.mean()),
        'cv_accuracy_std': float(cv_scores.std()),
        'class_weights': class_weights,
        'n_estimators': n_estimators,
        'max_depth': max_depth,
        'learning_rate': learning_rate,
        'random_state': random_state,
    }
    
    # Save model and metadata
    if output_model_path:
        model_dir = Path(output_model_path).parent
        model_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\nSaving model to {output_model_path}...")
        joblib.dump(model, output_model_path)
        
        metadata_path = str(Path(output_model_path).with_suffix('.metadata.joblib'))
        print(f"Saving metadata to {metadata_path}...")
        joblib.dump(metadata, metadata_path)
        
        print("✅ Model saved!")
    
    print()
    print("=" * 80)
    print("Model training complete!")
    print("=" * 80)
    print()
    
    return model, metadata


def load_party_prediction_model(model_path: str) -> Tuple[XGBClassifier, Dict]:
    """
    Load trained XGBoost model and metadata.
    
    Args:
        model_path: Path to saved model file
        
    Returns:
        Tuple of (model, metadata)
    """
    print(f"Loading model from {model_path}...")
    model = joblib.load(model_path)
    
    metadata_path = Path(model_path).with_suffix('.metadata.joblib')
    if metadata_path.exists():
        metadata = joblib.load(metadata_path)
    else:
        print("⚠️  Metadata file not found. Using default metadata.")
        metadata = {}
    
    return model, metadata


if __name__ == "__main__":
    # Test training
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
    
    # Classify primary voters
    if 'primary_classification' not in df.columns:
        df = classify_primary_voters(df)
    
    # Prepare features
    df_features, encoders, feature_cols = prepare_features_for_ml(df)
    
    # Train model
    model_path = "data/exports/models/party_prediction_model.pkl"
    model, metadata = train_party_prediction_model(
        df_features,
        feature_cols,
        encoders,
        output_model_path=model_path
    )
    
    print(f"\n✅ Model training complete!")
    print(f"Model saved to: {model_path}")

