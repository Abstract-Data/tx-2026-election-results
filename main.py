"""
Main orchestration script for voter turnout analysis.
Processes voterfile, early voting data, calculates turnout metrics,
and generates visualizations comparing 2022 vs 2026 district boundaries.
"""
from pathlib import Path
import polars as pl

from tx_election_results.data.voterfile import process_voterfile
from tx_election_results.data.early_voting import process_early_voting
from tx_election_results.data.merge import merge_voter_data
from tx_election_results.geospatial.shapefiles import load_shapefiles
from tx_election_results.geospatial.matching import (
    calculate_turnout_metrics,
    create_geodataframes_with_turnout
)
from tx_election_results.precinct.lookup import (
    build_precinct_to_district_lookup,
    apply_precinct_lookup
)
from tx_election_results.visualization.create_visualizations import create_all_visualizations
from tx_election_results.analysis.district_comparison import (
    calculate_party_gains_losses,
    create_party_gains_losses_visualizations
)
from tx_election_results.analysis.party_transition_report import generate_party_transition_report
from tx_election_results.analysis.party_crosstab_report import generate_party_crosstab_report
from tx_election_results.analysis.all_districts_gains_losses import generate_all_districts_gains_losses
from tx_election_results.modeling.party_affiliation import model_party_affiliation
from tx_election_results.config import config


def main():
    """Main orchestration function."""
    print("=" * 80)
    print("Voter Turnout Analysis - Texas 2026 Election Results")
    print("=" * 80)
    
    # Step 1: Process voterfile
    print("\n[Step 1/11] Processing voterfile...")
    print("-" * 80)
    voterfile_df = process_voterfile(config.VF_2024, str(config.PROCESSED_VOTERFILE))
    
    # Step 2: Process early voting data
    print("\n[Step 2/11] Processing early voting data...")
    print("-" * 80)
    early_voting_df = process_early_voting(config.EV_DATA_DIR, str(config.PROCESSED_EARLY_VOTING))
    
    # Step 3: Merge voterfile with early voting data
    print("\n[Step 3/11] Merging voterfile with early voting data...")
    print("-" * 80)
    merged_df = merge_voter_data(voterfile_df, early_voting_df, str(config.MERGED_DATA))
    
    # Step 4: Build precinct-to-2026-district lookups for all three district types
    print("\n[Step 4/11] Building precinct-to-2026-district lookups...")
    print("-" * 80)
    print("NOTE: NEWCD/NEWSD/NEWHD in voterfile are OLD districts (2022/2024 boundaries)")
    print("Using 2024 precinct shapefiles to determine which 2026 districts precincts are in")
    print("This allows us to compare OLD districts → NEW districts for each type")
    
    # Load all shapefiles
    gdf_2022_cd, gdf_2022_sd, gdf_2026 = load_shapefiles(
        config.SHAPEFILE_2022_CD,
        config.SHAPEFILE_2022_SD,
        config.SHAPEFILE_2026
    )
    
    # Load 2024 district shapefiles (these are the NEW 2026 districts for 2023-2026 period)
    import geopandas as gpd
    gdf_2026_cd = gpd.read_file(config.SHAPEFILE_2024_CD)
    gdf_2026_sd = gpd.read_file(config.SHAPEFILE_2024_SD)
    gdf_2026_hd = gpd.read_file(config.SHAPEFILE_2024_HD)
    
    print(f"\nLoaded 2026 district shapefiles (2023-2026 boundaries):")
    print(f"  Congressional Districts (CD): {len(gdf_2026_cd)} districts")
    print(f"  State Senate Districts (SD): {len(gdf_2026_sd)} districts")
    print(f"  House Districts (HD): {len(gdf_2026_hd)} districts")
    
    # Build lookups for all three district types
    print("\nBuilding precinct-to-district lookups...")
    
    # 1. State Senate Districts (SD)
    print("\n1. State Senate Districts (SD): OLD NEWSD → NEW 2026_SD")
    lookup_sd = build_precinct_to_district_lookup(
        merged_df,
        gdf_2026_sd,
        district_col_name="District",
        output_col_name="2026_SD",
        precinct_shapefile_path=config.PRECINCT_SHAPEFILE_2024,
        output_path=str(config.PRECINCT_LOOKUP_SD),
        use_cached=True
    )
    merged_df = apply_precinct_lookup(merged_df, lookup_sd, output_col_name="2026_SD")
    
    # 2. Congressional Districts (CD)
    print("\n2. Congressional Districts (CD): OLD NEWCD → NEW 2026_CD")
    lookup_cd = build_precinct_to_district_lookup(
        merged_df,
        gdf_2026_cd,
        district_col_name="District",
        output_col_name="2026_CD",
        precinct_shapefile_path=config.PRECINCT_SHAPEFILE_2024,
        output_path=str(config.PRECINCT_LOOKUP_CD),
        use_cached=True
    )
    merged_df = apply_precinct_lookup(merged_df, lookup_cd, output_col_name="2026_CD")
    
    # 3. House Districts (HD)
    print("\n3. House Districts (HD): OLD NEWHD → NEW 2026_HD")
    lookup_hd = build_precinct_to_district_lookup(
        merged_df,
        gdf_2026_hd,
        district_col_name="District",
        output_col_name="2026_HD",
        precinct_shapefile_path=config.PRECINCT_SHAPEFILE_2024,
        output_path=str(config.PRECINCT_LOOKUP_HD),
        use_cached=True
    )
    merged_df = apply_precinct_lookup(merged_df, lookup_hd, output_col_name="2026_HD")
    
    # Save updated merged data with all 2026 district assignments
    merged_df.write_parquet(str(config.MERGED_DATA))
    print("\nSaved updated merged data with 2026 district assignments (2026_SD, 2026_CD, 2026_HD)")
    
    # Step 5: Calculate turnout metrics
    print("\n[Step 5/11] Calculating turnout metrics by district...")
    print("-" * 80)
    print("Using NEWCD/NEWSD (OLD districts) for 2022 shapefiles")
    print("Using 2026_SD (from precinct lookup) for 2026 shapefile")
    turnout_metrics = calculate_turnout_metrics(
        merged_df,
        gdf_2022_cd,
        gdf_2022_sd,
        gdf_2026,
        output_dir=str(config.OUTPUT_DIR)
    )
    
    # Check if modeled data exists for district analysis
    analysis_df = merged_df
    if config.MODELED_DATA.exists():
        analysis_df = pl.read_parquet(str(config.MODELED_DATA))
        print(f"Using modeled data for party analysis (includes non-primary voters)")
    
    # Step 6: Calculate party gains/losses from redistricting (State Senate only for backward compatibility)
    print("\n[Step 6/12] Calculating party gains/losses from redistricting (State Senate)...")
    print("-" * 80)
    party_results = calculate_party_gains_losses(
        analysis_df,
        old_district_col="NEWSD",
        new_district_col="2026_SD",
        output_dir=str(config.OUTPUT_DIR),
        use_modeled=config.MODELED_DATA.exists()
    )
    
    # Step 7: Create visualizations
    print("\n[Step 7/11] Creating visualizations...")
    print("-" * 80)
    create_all_visualizations(
        shapefile_2022_cd_path=config.SHAPEFILE_2022_CD,
        shapefile_2022_sd_path=config.SHAPEFILE_2022_SD,
        shapefile_2026_path=config.SHAPEFILE_2026,
        turnout_2022_cd_path=str(config.OUTPUT_DIR / "csv" / "turnout_by_district_2022_congressional.csv"),
        turnout_2022_sd_path=str(config.OUTPUT_DIR / "csv" / "turnout_by_district_2022_senate.csv"),
        turnout_2026_path=str(config.OUTPUT_DIR / "csv" / "turnout_by_district_2026.csv"),
        merged_voter_path=str(config.MERGED_DATA),
        output_dir=str(config.VISUALIZATIONS_DIR)
    )
    
    # Check if modeled data exists, if so use it for district analysis
    analysis_df = merged_df
    if config.MODELED_DATA.exists():
        print(f"\n📊 Found modeled party data: {config.MODELED_DATA}")
        print("   Using modeled data for district analysis (includes non-primary voters)")
        analysis_df = pl.read_parquet(str(config.MODELED_DATA))
    else:
        print(f"\n📊 Using merged data (no modeled party predictions yet)")
        print("   Run Step 12 to add modeled party predictions for non-primary voters")
    
    # Step 8: Generate detailed party transition report
    print("\n[Step 8/12] Generating detailed party transition report...")
    print("-" * 80)
    transition_report = generate_party_transition_report(
        analysis_df,
        old_district_col="NEWSD",
        new_district_col="2026_SD",
        output_dir=str(config.OUTPUT_DIR)
    )
    
    # Step 9: Generate party gains/losses for ALL district types (HD, SD, CD)
    print("\n[Step 9/12] Generating party gains/losses for ALL district types...")
    print("-" * 80)
    all_districts_results = generate_all_districts_gains_losses(
        analysis_df,
        output_dir=str(config.OUTPUT_DIR)
    )
    
    # Step 10: Generate party crosstab reports by County/CD/SD/HD
    print("\n[Step 10/12] Generating party crosstab reports by County/CD/SD/HD...")
    print("-" * 80)
    crosstab_reports = generate_party_crosstab_report(
        analysis_df,
        output_dir=str(config.OUTPUT_DIR)
    )
    
    # Step 11: Create party gains/losses visualizations
    print("\n[Step 11/12] Creating party gains/losses visualizations...")
    print("-" * 80)
    create_party_gains_losses_visualizations(
        party_results,
        gdf_2026,
        output_dir=str(config.VISUALIZATIONS_DIR)
    )
    
    # Step 12: Classify primary voters
    print("\n[Step 12/15] Classifying primary voters...")
    print("-" * 80)
    from tx_election_results.modeling.primary_voter_classifier import classify_primary_voters
    merged_df = classify_primary_voters(merged_df)
    merged_df.write_parquet(str(config.MERGED_DATA))
    
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
    print(f"  - Processed voterfile: {config.PROCESSED_VOTERFILE}")
    print(f"  - Processed early voting: {config.PROCESSED_EARLY_VOTING}")
    print(f"  - Merged data: {config.MERGED_DATA}")
    print(f"  - Modeled party affiliation: {config.MODELED_DATA}")
    print(f"  - ML Model: {config.PARTY_PREDICTION_MODEL}")
    print(f"  - Precinct-to-2026-district lookups:")
    print(f"      * SD: {config.PRECINCT_LOOKUP_SD}")
    print(f"      * CD: {config.PRECINCT_LOOKUP_CD}")
    print(f"      * HD: {config.PRECINCT_LOOKUP_HD}")
    print(f"  - Turnout metrics: {config.OUTPUT_DIR / 'csv' / 'turnout_by_district_*.csv'}")
    print(f"  - Party gains/losses: {config.OUTPUT_DIR / 'csv' / 'party_gains_losses_by_district.csv'}")
    print(f"  - Party gains/losses by district type:")
    print(f"      * SD: {config.OUTPUT_DIR / 'districts' / 'sd_districts/'}")
    print(f"      * CD: {config.OUTPUT_DIR / 'districts' / 'cd_districts/'}")
    print(f"      * HD: {config.OUTPUT_DIR / 'districts' / 'hd_districts/'}")
    print(f"  - Party transition report: {config.OUTPUT_DIR / 'csv' / 'party_transition_*.csv'}")
    print(f"  - Party crosstab reports: {config.OUTPUT_DIR / 'csv' / 'party_by_county*.csv'}, {config.OUTPUT_DIR / 'csv' / 'party_gains_losses_by_county*.csv'}")
    print(f"  - Redistricting analysis: {config.REDISTRICTING_ANALYSIS_DIR}")
    print(f"  - Competitiveness analysis: {config.COMPETITIVENESS_ANALYSIS_DIR}")
    print(f"  - Voter classifications: {config.OUTPUT_DIR / 'csv' / 'voter_classifications.csv'}")
    print(f"  - Visualizations: {config.VISUALIZATIONS_DIR} /")
    
    print("\nSummary Statistics:")
    print(f"  - Total voters processed: {len(merged_df):,}")
    print(f"  - Early voters: {merged_df['voted_early'].sum():,}")
    print(f"  - Early voting rate: {merged_df['voted_early'].mean() * 100:.2f}%")
    
    if 'party_final' in analysis_df.columns:
        print(f"\nParty Classification:")
        party_counts = analysis_df.group_by('party_final').agg(pl.count()).sort('party_final')
        print(party_counts)
    
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
