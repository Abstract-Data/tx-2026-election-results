#!/usr/bin/env python3
"""
Run ML modeling steps (12-15) to generate party predictions for general-election-only voters.
"""
import polars as pl
from pathlib import Path
from tx_election_results.config import config

def run_ml_pipeline():
    """Run steps 12-15: ML modeling and predictions."""
    print("=" * 80)
    print("RUNNING ML MODELING PIPELINE (Steps 12-15)")
    print("=" * 80)
    
    # Load merged data
    print(f"\nLoading merged data from {config.MERGED_DATA}...")
    if not config.MERGED_DATA.exists():
        print(f"❌ Error: {config.MERGED_DATA} does not exist!")
        print("   Please run steps 1-11 first to generate merged data.")
        return False
    
    merged_df = pl.read_parquet(str(config.MERGED_DATA))
    print(f"✓ Loaded {len(merged_df):,} voters")
    
    # Step 12: Classify primary voters
    print("\n[Step 12/15] Classifying primary voters...")
    print("-" * 80)
    from tx_election_results.modeling.primary_voter_classifier import classify_primary_voters
    merged_df = classify_primary_voters(merged_df)
    merged_df.write_parquet(str(config.MERGED_DATA))
    print("✓ Primary voter classification complete")
    
    # Step 13: Prepare features and train ML model
    print("\n[Step 13/15] Training XGBoost model for party prediction...")
    print("-" * 80)
    from tx_election_results.modeling.feature_engineering import prepare_features_for_ml
    from tx_election_results.modeling.party_prediction_model import train_party_prediction_model
    
    # Prepare features
    print("Preparing features for ML...")
    merged_df_features, label_encoders, feature_columns = prepare_features_for_ml(merged_df)
    print(f"✓ Prepared {len(feature_columns)} features")
    
    # Train model
    config.MODEL_DIR.mkdir(parents=True, exist_ok=True)
    try:
        print("Training XGBoost model...")
        model, metadata = train_party_prediction_model(
            merged_df_features,
            feature_columns,
            label_encoders,
            output_model_path=str(config.PARTY_PREDICTION_MODEL)
        )
        print(f"\n✅ Model training complete! Model saved to {config.PARTY_PREDICTION_MODEL}")
        print(f"   Model accuracy: {metadata.get('accuracy', 'N/A')}")
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
    
    # Step 15: Regenerate redistricting analysis with modeled data
    print("\n[Step 15/15] Regenerating redistricting analysis with modeled data...")
    print("-" * 80)
    from tx_election_results.analysis.export_redistricting_data import export_all_redistricting_data
    from tx_election_results.analysis.redistricting_impact import analyze_all_district_types
    from tx_election_results.analysis.competitiveness import assess_all_district_types
    
    # Export all data with modeled predictions
    export_results = export_all_redistricting_data(
        analysis_df,
        str(config.OUTPUT_DIR),
        party_col='party_final',
        threshold=config.COMPETITIVENESS_THRESHOLD
    )
    
    # Get redistricting and competitiveness results
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
    
    print(f"\n✅ Redistricting analysis with modeled data complete!")
    
    # Summary
    print("\n" + "=" * 80)
    print("ML PIPELINE COMPLETE!")
    print("=" * 80)
    print(f"\nOutput files:")
    print(f"  - Modeled party affiliation: {config.MODELED_DATA}")
    print(f"  - ML Model: {config.PARTY_PREDICTION_MODEL}")
    print(f"  - Redistricting analysis: {config.REDISTRICTING_ANALYSIS_DIR}")
    print(f"  - Competitiveness analysis: {config.COMPETITIVENESS_ANALYSIS_DIR}")
    
    if 'party_final' in analysis_df.columns:
        print(f"\nParty Classification Summary:")
        party_counts = analysis_df.group_by('party_final').agg(pl.count()).sort('party_final')
        print(party_counts)
        
        # Count modeled vs known
        if 'predicted_party_score' in analysis_df.columns:
            modeled_count = analysis_df.filter(pl.col('predicted_party_score').is_not_null()).select(pl.count()).item()
            print(f"\nModeled voters (general-election-only): {modeled_count:,}")
    
    print("\n" + "=" * 80)
    return True


if __name__ == "__main__":
    success = run_ml_pipeline()
    exit(0 if success else 1)

