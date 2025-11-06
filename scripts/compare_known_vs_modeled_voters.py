"""
Compare party advantage analysis with and without modeled voters.

This script shows:
1. Analysis based ONLY on known primary voters (what we know)
2. Analysis including modeled non-primary voters (what we predict)
3. Direct comparison showing the impact of including modeled voters
"""
import pandas as pd
import numpy as np
from pathlib import Path
import polars as pl
from tx_election_results.analysis.district_comparison import calculate_party_gains_losses
from tx_election_results.analysis.all_districts_gains_losses import generate_all_districts_gains_losses


def analyze_with_and_without_modeled(
    merged_df_path: str,
    modeled_df_path: str = None,
    output_dir: str = "data/exports"
):
    """
    Analyze party advantage with and without modeled voters.
    
    Args:
        merged_df_path: Path to merged voter data (without modeling)
        modeled_df_path: Path to modeled voter data (with predictions)
        output_dir: Directory to save comparison results
    """
    print("="*100)
    print("COMPARISON: KNOWN PRIMARY VOTERS vs INCLUDING MODELED VOTERS")
    print("="*100)
    print("\nThis analysis compares:")
    print("  1. Analysis based ONLY on known primary voters (what we know)")
    print("  2. Analysis including modeled non-primary voters (what we predict)")
    print("  3. Impact of including modeled voters on party advantage")
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True, parents=True)
    comparison_dir = output_path / "analysis" / "known_vs_modeled_comparison"
    comparison_dir.mkdir(exist_ok=True)
    
    # Load merged data (without modeling)
    print(f"\n{'='*100}")
    print("STEP 1: Loading data...")
    print(f"{'='*100}")
    merged_df = pl.read_parquet(merged_df_path)
    print(f"Loaded merged data: {len(merged_df):,} voters")
    
    # Check if modeled data exists
    has_modeled = False
    if modeled_df_path and Path(modeled_df_path).exists():
        modeled_df = pl.read_parquet(modeled_df_path)
        print(f"Loaded modeled data: {len(modeled_df):,} voters")
        
        # Check if modeled columns exist
        if "predicted_party_score" in modeled_df.columns:
            has_modeled = True
            print("✅ Modeled party predictions found")
        else:
            print("⚠️  Modeled data exists but no predicted_party_score column found")
    else:
        print("⚠️  No modeled data found - will analyze with known voters only")
        print("   To generate modeled data, run: uv run python model_party_affiliation.py")
    
    # Analyze WITHOUT modeled voters (known primary voters only)
    print(f"\n{'='*100}")
    print("STEP 2: Analyzing with KNOWN PRIMARY VOTERS ONLY")
    print(f"{'='*100}")
    print("This shows what we know based on actual primary voting history")
    
    results_known_only = {}
    
    for dist_type, old_col, new_col in [
        ("HD", "NEWHD", "2026_HD"),
        ("CD", "NEWCD", "2026_CD"),
        ("SD", "NEWSD", "2026_SD")
    ]:
        print(f"\n--- {dist_type} Districts (Known Voters Only) ---")
        try:
            result = calculate_party_gains_losses(
                merged_df,
                old_district_col=old_col,
                new_district_col=new_col,
                output_dir=str(comparison_dir / f"{dist_type.lower()}_known_only"),
                use_modeled=False  # Explicitly exclude modeled voters
            )
            results_known_only[dist_type] = result
        except Exception as e:
            print(f"Error analyzing {dist_type}: {e}")
            results_known_only[dist_type] = None
    
    # Analyze WITH modeled voters (if available)
    results_with_modeled = {}
    
    if has_modeled:
        print(f"\n{'='*100}")
        print("STEP 3: Analyzing with MODELED VOTERS INCLUDED")
        print(f"{'='*100}")
        print("This shows predictions including modeled non-primary voters")
        
        for dist_type, old_col, new_col in [
            ("HD", "NEWHD", "2026_HD"),
            ("CD", "NEWCD", "2026_CD"),
            ("SD", "NEWSD", "2026_SD")
        ]:
            print(f"\n--- {dist_type} Districts (With Modeled Voters) ---")
            try:
                result = calculate_party_gains_losses(
                    modeled_df,
                    old_district_col=old_col,
                    new_district_col=new_col,
                    output_dir=str(comparison_dir / f"{dist_type.lower()}_with_modeled"),
                    use_modeled=True  # Include modeled voters
                )
                results_with_modeled[dist_type] = result
            except Exception as e:
                print(f"Error analyzing {dist_type}: {e}")
                results_with_modeled[dist_type] = None
    else:
        print(f"\n{'='*100}")
        print("STEP 3: SKIPPED - No modeled data available")
        print(f"{'='*100}")
        print("To generate modeled data, run the modeling step first.")
    
    # Compare results
    print(f"\n{'='*100}")
    print("STEP 4: COMPARING RESULTS")
    print(f"{'='*100}")
    
    comparison_results = {}
    
    for dist_type in ["HD", "CD", "SD"]:
        if dist_type not in results_known_only or results_known_only[dist_type] is None:
            continue
        
        print(f"\n{'='*100}")
        print(f"{dist_type} DISTRICTS - KNOWN vs MODELED COMPARISON")
        print(f"{'='*100}")
        
        known_df = results_known_only[dist_type]["gains_losses"]
        
        if has_modeled and dist_type in results_with_modeled and results_with_modeled[dist_type] is not None:
            modeled_df_comp = results_with_modeled[dist_type]["gains_losses"]
            
            # Merge for comparison
            comparison = known_df.merge(
                modeled_df_comp,
                on="district",
                suffixes=("_known", "_modeled"),
                how="outer"
            )
            
            # Calculate differences
            comparison["rep_voters_diff"] = (
                comparison["new_republican_voters_modeled"] - 
                comparison["new_republican_voters_known"]
            )
            comparison["dem_voters_diff"] = (
                comparison["new_democrat_voters_modeled"] - 
                comparison["new_democrat_voters_known"]
            )
            comparison["net_advantage_diff"] = (
                comparison["rep_voters_diff"] - comparison["dem_voters_diff"]
            )
            
            # Summary statistics
            print(f"\nKnown Primary Voters Only:")
            known_total_rep = known_df["new_republican_voters"].sum()
            known_total_dem = known_df["new_democrat_voters"].sum()
            known_net = known_total_rep - known_total_dem
            print(f"  Total Republican Voters: {known_total_rep:+,}")
            print(f"  Total Democrat Voters: {known_total_dem:+,}")
            print(f"  Net Advantage: {known_net:+,} (Republican)")
            
            print(f"\nWith Modeled Voters:")
            modeled_total_rep = modeled_df_comp["new_republican_voters"].sum()
            modeled_total_dem = modeled_df_comp["new_democrat_voters"].sum()
            modeled_net = modeled_total_rep - modeled_total_dem
            print(f"  Total Republican Voters: {modeled_total_rep:+,}")
            print(f"  Total Democrat Voters: {modeled_total_dem:+,}")
            print(f"  Net Advantage: {modeled_net:+,} (Republican)")
            
            print(f"\nImpact of Including Modeled Voters:")
            rep_diff = modeled_total_rep - known_total_rep
            dem_diff = modeled_total_dem - known_total_dem
            net_diff = modeled_net - known_net
            print(f"  Additional Republican Voters: {rep_diff:+,}")
            print(f"  Additional Democrat Voters: {dem_diff:+,}")
            print(f"  Net Advantage Change: {net_diff:+,}")
            print(f"    (Positive = shift toward Republicans, Negative = shift toward Democrats)")
            
            # Show breakdown if available
            if "new_rep_known" in modeled_df_comp.columns:
                total_known_rep = modeled_df_comp["new_rep_known"].sum()
                total_modeled_rep = modeled_df_comp["new_rep_modeled"].sum()
                total_known_dem = modeled_df_comp["new_dem_known"].sum()
                total_modeled_dem = modeled_df_comp["new_dem_modeled"].sum()
                
                print(f"\nVoter Breakdown (With Modeled):")
                print(f"  Republican - Known: {total_known_rep:+,} ({total_known_rep/(total_known_rep+total_modeled_rep)*100:.1f}%)")
                print(f"  Republican - Modeled: {total_modeled_rep:+,} ({total_modeled_rep/(total_known_rep+total_modeled_rep)*100:.1f}%)")
                print(f"  Democrat - Known: {total_known_dem:+,} ({total_known_dem/(total_known_dem+total_modeled_dem)*100:.1f}%)")
                print(f"  Democrat - Modeled: {total_modeled_dem:+,} ({total_modeled_dem/(total_known_dem+total_modeled_dem)*100:.1f}%)")
            
            # District-level impact
            print(f"\nTop 10 Districts Where Modeled Voters Increase Republican Advantage:")
            top_rep_impact = comparison.nlargest(10, "net_advantage_diff")[
                ["district", "new_republican_voters_known", "new_republican_voters_modeled",
                 "new_democrat_voters_known", "new_democrat_voters_modeled", "net_advantage_diff"]
            ]
            print(top_rep_impact.to_string(index=False))
            
            print(f"\nTop 10 Districts Where Modeled Voters Increase Democrat Advantage:")
            top_dem_impact = comparison.nsmallest(10, "net_advantage_diff")[
                ["district", "new_republican_voters_known", "new_republican_voters_modeled",
                 "new_democrat_voters_known", "new_democrat_voters_modeled", "net_advantage_diff"]
            ]
            print(top_dem_impact.to_string(index=False))
            
            # Save comparison
            comparison_file = comparison_dir / f"{dist_type.lower()}_known_vs_modeled_comparison.csv"
            comparison.to_csv(comparison_file, index=False)
            print(f"\n✅ Comparison saved to: {comparison_file}")
            
            comparison_results[dist_type] = {
                "known": known_df,
                "modeled": modeled_df_comp,
                "comparison": comparison
            }
        else:
            print(f"\n⚠️  No modeled data available for {dist_type} - showing known voters only")
            comparison_results[dist_type] = {
                "known": known_df,
                "modeled": None,
                "comparison": None
            }
    
    # Create comprehensive summary
    print(f"\n{'='*100}")
    print("COMPREHENSIVE SUMMARY - ALL DISTRICT TYPES")
    print(f"{'='*100}")
    
    summary_data = []
    
    for dist_type in ["HD", "CD", "SD"]:
        if dist_type not in comparison_results:
            continue
        
        result = comparison_results[dist_type]
        
        # Known voters only
        known_df = result["known"]
        known_rep = known_df["new_republican_voters"].sum()
        known_dem = known_df["new_democrat_voters"].sum()
        known_net = known_rep - known_dem
        
        if result["modeled"] is not None:
            modeled_df = result["modeled"]
            modeled_rep = modeled_df["new_republican_voters"].sum()
            modeled_dem = modeled_df["new_democrat_voters"].sum()
            modeled_net = modeled_rep - modeled_dem
            
            net_change = modeled_net - known_net
        else:
            modeled_rep = modeled_dem = modeled_net = net_change = None
        
        summary_data.append({
            "District Type": dist_type,
            "Known Rep Voters": f"{known_rep:+,}",
            "Known Dem Voters": f"{known_dem:+,}",
            "Known Net Advantage": f"{known_net:+,}",
            "Modeled Rep Voters": f"{modeled_rep:+,}" if modeled_rep is not None else "N/A",
            "Modeled Dem Voters": f"{modeled_dem:+,}" if modeled_dem is not None else "N/A",
            "Modeled Net Advantage": f"{modeled_net:+,}" if modeled_net is not None else "N/A",
            "Net Advantage Change": f"{net_change:+,}" if net_change is not None else "N/A",
        })
    
    summary_df = pd.DataFrame(summary_data)
    print("\n" + summary_df.to_string(index=False))
    
    summary_file = comparison_dir / "known_vs_modeled_summary.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"\n✅ Summary saved to: {summary_file}")
    
    print(f"\n{'='*100}")
    print("COMPARISON COMPLETE")
    print(f"{'='*100}")
    print(f"\nAll comparison files saved to: {comparison_dir}/")
    
    return comparison_results


if __name__ == "__main__":
    # Paths
    merged_path = "early_voting_merged.parquet"
    modeled_path = "voters_with_party_modeling.parquet"
    output_dir = "."
    
    results = analyze_with_and_without_modeled(
        merged_df_path=merged_path,
        modeled_df_path=modeled_path,
        output_dir=output_dir
    )

