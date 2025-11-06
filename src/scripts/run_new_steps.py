#!/usr/bin/env python3
"""
Run only the new steps (12-15) of the pipeline.
Assumes steps 1-11 have already been completed.
"""
import sys
import os
# Add project root and src to path (script is in src/scripts/, run from project root)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(project_root, 'src'))
sys.path.insert(0, project_root)

import polars as pl
from pathlib import Path
from tx_election_results.config import config

print("=" * 80)
print("RUNNING NEW PIPELINE STEPS (12-15)")
print("=" * 80)
print()

# Step 12: Classify primary voters
print("\n[Step 12/15] Classifying primary voters...")
print("-" * 80)
from tx_election_results.modeling.primary_voter_classifier import classify_primary_voters

if config.MERGED_DATA.exists():
    merged_df = pl.read_parquet(str(config.MERGED_DATA))
    print(f"Loaded {len(merged_df):,} voters from {config.MERGED_DATA}")
    merged_df = classify_primary_voters(merged_df)
    merged_df.write_parquet(str(config.MERGED_DATA))
    print(f"Saved updated data with primary classifications")
else:
    print(f"Error: {config.MERGED_DATA} not found. Please run steps 1-11 first.")
    sys.exit(1)

# Step 13: Prepare features and train ML model
print("\n[Step 13/15] Training XGBoost model for party prediction...")
print("-" * 80)
from tx_election_results.modeling.feature_engineering import prepare_features_for_ml
from tx_election_results.modeling.party_prediction_model import train_party_prediction_model

# Prepare features
merged_df_features, label_encoders, feature_columns = prepare_features_for_ml(merged_df)

# Train model
config.MODEL_DIR.mkdir(parents=True, exist_ok=True)
try:
    model, metadata = train_party_prediction_model(
        merged_df_features,
        feature_columns,
        label_encoders,
        output_model_path=str(config.PARTY_PREDICTION_MODEL)
    )
    print(f"\n✅ Model training complete! Model saved to {config.PARTY_PREDICTION_MODEL}")
except Exception as e:
    print(f"\n⚠️  Model training failed: {e}")
    import traceback
    traceback.print_exc()
    print("Continuing without ML predictions...")
    model = None

# Step 14: Predict party for general-election-only voters
print("\n[Step 14/15] Predicting party for general-election-only voters...")
print("-" * 80)
if model is not None:
    from tx_election_results.modeling.predict_general_voters import (
        predict_party_for_general_voters,
        create_final_party_classification
    )
    try:
        merged_df_predicted = predict_party_for_general_voters(
            merged_df_features,
            str(config.PARTY_PREDICTION_MODEL),
            feature_columns
        )
        merged_df_final = create_final_party_classification(merged_df_predicted)
        merged_df_final.write_parquet(str(config.MODELED_DATA))
        print(f"\n✅ Prediction complete! Results saved to {config.MODELED_DATA}")
        analysis_df = merged_df_final
    except Exception as e:
        print(f"\n⚠️  Prediction failed: {e}")
        import traceback
        traceback.print_exc()
        print("Using primary classifications only...")
        from tx_election_results.modeling.predict_general_voters import create_final_party_classification
        merged_df_final = create_final_party_classification(merged_df)
        merged_df_final.write_parquet(str(config.MODELED_DATA))
        analysis_df = merged_df_final
else:
    print("Skipping prediction (no trained model available)")
    from tx_election_results.modeling.predict_general_voters import create_final_party_classification
    merged_df_final = create_final_party_classification(merged_df)
    merged_df_final.write_parquet(str(config.MODELED_DATA))
    analysis_df = merged_df_final

# Step 15: Redistricting impact analysis and exports
print("\n[Step 15/15] Running redistricting impact analysis and exports...")
print("-" * 80)
from tx_election_results.analysis.export_redistricting_data import export_all_redistricting_data
from tx_election_results.analysis.redistricting_impact import analyze_all_district_types
from tx_election_results.analysis.competitiveness import assess_all_district_types
from tx_election_results.visualization.redistricting_visualizations import create_all_redistricting_visualizations

# Export all data
export_results = export_all_redistricting_data(
    analysis_df,
    str(config.OUTPUT_DIR),
    party_col='party_final',
    threshold=config.COMPETITIVENESS_THRESHOLD
)

# Get redistricting and competitiveness results for visualizations
redistricting_results = analyze_all_district_types(
    analysis_df,
    party_col='party_final',
    output_dir=str(config.REDISTRICTING_ANALYSIS_DIR)
)

competitiveness_results = assess_all_district_types(
    analysis_df,
    party_col='party_final',
    threshold=config.COMPETITIVENESS_THRESHOLD,
    output_dir=str(config.COMPETITIVENESS_ANALYSIS_DIR)
)

# Create visualizations
shapefile_paths = {
    '2026_CD': config.SHAPEFILE_2024_CD,
    '2026_SD': config.SHAPEFILE_2024_SD,
    '2026_HD': config.SHAPEFILE_2024_HD,
    '2022_CD': config.SHAPEFILE_2022_CD,
    '2022_SD': config.SHAPEFILE_2022_SD,
}

create_all_redistricting_visualizations(
    redistricting_results,
    competitiveness_results,
    shapefile_paths,
    str(config.VISUALIZATIONS_DIR)
)

print(f"\n✅ Redistricting analysis complete!")

# Summary
print("\n" + "=" * 80)
print("Analysis Complete!")
print("=" * 80)
print(f"\nOutput files:")
print(f"  - Modeled party affiliation: {config.MODELED_DATA}")
print(f"  - ML Model: {config.PARTY_PREDICTION_MODEL}")
print(f"  - Redistricting analysis: {config.REDISTRICTING_ANALYSIS_DIR}")
print(f"  - Competitiveness analysis: {config.COMPETITIVENESS_ANALYSIS_DIR}")
print(f"  - Voter classifications: {config.OUTPUT_DIR / 'csv' / 'voter_classifications.csv'}")
print(f"  - Visualizations: {config.VISUALIZATIONS_DIR} /")

if 'party_final' in analysis_df.columns:
    print(f"\nParty Classification Summary:")
    party_counts = analysis_df.group_by('party_final').agg([
        pl.count().alias("count"),
        (pl.count() / pl.len() * 100).alias("percentage")
    ]).sort('party_final')
    print(party_counts)

print("\n" + "=" * 80)

