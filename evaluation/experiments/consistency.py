"""Analyze consistency from pre-generated model run data."""

from pathlib import Path
import pandas as pd
import numpy as np


def analyze_consistency(
    data_dir: Path = Path("evaluation/data/consistency"),
    output_path: Path | None = None
):
    """
    Load saved consistency data and compute variability metrics.
    
    Performs two analyses:
    1. Intra-model consistency: same model, multiple runs
    2. Inter-model consistency: different models on same data
    
    Args:
        data_dir: Directory containing consistency_*.csv files
        output_path: Optional file path to save the report
    """
    if not data_dir.exists():
        msg = f"Error: {data_dir} does not exist.\nRun generate_consistency_data.py first to create the data."
        print(msg)
        return
    
    csv_files = list(data_dir.glob("consistency_*.csv"))
    if not csv_files:
        msg = f"No consistency_*.csv files found in {data_dir}\nRun generate_consistency_data.py first."
        print(msg)
        return
    
    # Collect all output
    report_lines = []
    
    # Store all model data for inter-model comparison
    all_models_data = {}
    
    report_lines.append("\n" + "="*70)
    report_lines.append("INTRA-MODEL CONSISTENCY (same model, multiple runs)")
    report_lines.append("="*70)
    
    for csv_path in sorted(csv_files):
        model_name = csv_path.stem.replace("consistency_", "")
        report_lines.append("\n" + "-"*60)
        report_lines.append(f"Model: {model_name}")
        report_lines.append("-"*60)
        
        df = pd.read_csv(csv_path)
        
        # Store for inter-model comparison
        all_models_data[model_name] = df
        
        # get run columns
        run_cols = [col for col in df.columns if col.startswith("run_")]
        n_runs = len(run_cols)
        n_sentences = len(df)
        
        report_lines.append(f"Runs: {n_runs}, Sentences: {n_sentences}")
        
        # compute per-sentence standard deviation (using sample std)
        scores_array = df[run_cols].values  # shape: (n_sentences, n_runs)
        per_sentence_std = np.std(scores_array, axis=1, ddof=1)  # sample std
        per_sentence_mean = np.mean(scores_array, axis=1)
        
        # Coefficient of variation (normalized variability)
        cv = per_sentence_std / (per_sentence_mean + 1e-9) * 100  # percentage
        
        avg_std = per_sentence_std.mean()
        max_std = per_sentence_std.max()
        median_std = np.median(per_sentence_std)
        avg_cv = cv.mean()
        
        report_lines.append("\nIntra-run variability (across runs for each sentence):")
        report_lines.append(f"  Mean std dev: {avg_std:.4f}")
        report_lines.append(f"    → Average inconsistency per sentence. Lower is better (stable scoring).")
        report_lines.append(f"  Median std dev: {median_std:.4f}")
        report_lines.append(f"    → Middle value, robust to outliers. Compare with mean to detect skew.")
        report_lines.append(f"  Max std dev: {max_std:.4f}")
        report_lines.append(f"    → Worst-case variability. Flags extremely unstable sentences.")
        report_lines.append(f"  Mean coefficient of variation: {avg_cv:.2f}%")
        report_lines.append(f"    → Variability relative to score magnitude. <15% is good, >30% is concerning.")
        
        # identify most inconsistent sentences
        worst_idx = np.argmax(per_sentence_std)
        report_lines.append(f"\nMost variable sentence (std={per_sentence_std[worst_idx]:.3f}, CV={cv[worst_idx]:.1f}%):")
        report_lines.append(f"  '{df.iloc[worst_idx]['sentence']}'")
        report_lines.append(f"  Scores: {[f'{x:.1f}' for x in df.iloc[worst_idx][run_cols].tolist()]}")
        report_lines.append(f"  Range: {df.iloc[worst_idx][run_cols].min():.1f} - {df.iloc[worst_idx][run_cols].max():.1f}")
        
        # Show sentences with high variability (std > 1.0 or CV > 20%)
        high_var_mask = (per_sentence_std > 1.0) | (cv > 20)
        n_high_var = high_var_mask.sum()
        if n_high_var > 0:
            report_lines.append(f"\nSentences with high variability (std>1.0 or CV>20%): {n_high_var}")
            for idx in np.where(high_var_mask)[0][:3]:  # show top 3
                report_lines.append(f"  - std={per_sentence_std[idx]:.3f}, CV={cv[idx]:.1f}%: '{df.iloc[idx]['sentence'][:50]}...'")
    
    # Inter-model consistency analysis
    if len(all_models_data) > 1:
        report_lines.append("\n\n" + "="*70)
        report_lines.append("INTER-MODEL CONSISTENCY (different models on same data)")
        report_lines.append("="*70)
        
        # Build comparison dataframe: take mean across runs for each model
        model_names = sorted(all_models_data.keys())
        
        # Ensure all models have the same sentences in the same order
        base_sentences = all_models_data[model_names[0]]["sentence"].tolist()
        
        comparison_data = {"sentence": base_sentences}
        for model_name in model_names:
            df = all_models_data[model_name]
            run_cols = [col for col in df.columns if col.startswith("run_")]
            # Take mean across runs for each sentence
            comparison_data[model_name] = df[run_cols].mean(axis=1).tolist()
        
        comp_df = pd.DataFrame(comparison_data)
        
        # Pairwise correlations
        report_lines.append("\nPairwise correlations (Pearson):")
        from scipy.stats import pearsonr
        for i, model1 in enumerate(model_names):
            for model2 in model_names[i+1:]:
                corr, pval = pearsonr(comp_df[model1], comp_df[model2])
                report_lines.append(f"  {model1} vs {model2}: r = {corr:.3f} (p={pval:.4f})")
        report_lines.append("  → r measures how similarly models rank sentences. r>0.9 = strong agreement.")
        report_lines.append("  → p-value: if <0.05, correlation is statistically significant.")
        
        # Mean absolute difference between models
        report_lines.append("\nMean absolute difference between models:")
        for i, model1 in enumerate(model_names):
            for model2 in model_names[i+1:]:
                mad = np.abs(comp_df[model1] - comp_df[model2]).mean()
                report_lines.append(f"  {model1} vs {model2}: MAD = {mad:.3f}")
        report_lines.append("  → Average absolute point difference. MAD<0.5 = close agreement, MAD>1.0 = divergent.")
        
        # Sentences with highest disagreement
        if len(model_names) >= 2:
            model_scores = comp_df[model_names].values
            per_sentence_range = model_scores.max(axis=1) - model_scores.min(axis=1)
            per_sentence_std = np.std(model_scores, axis=1, ddof=1)
            
            report_lines.append(f"\nCross-model variability:")
            report_lines.append(f"  Mean std dev across models: {per_sentence_std.mean():.3f}")
            report_lines.append(f"    → Average disagreement per sentence. Lower = models see data similarly.")
            report_lines.append(f"  Mean range across models: {per_sentence_range.mean():.3f}")
            report_lines.append(f"    → Average gap between highest and lowest model scores per sentence.")
            
            worst_idx = np.argmax(per_sentence_std)
            report_lines.append(f"\nMost disagreed sentence (std={per_sentence_std[worst_idx]:.3f}):")
            report_lines.append(f"  '{comp_df.iloc[worst_idx]['sentence']}'")
            for model_name in model_names:
                report_lines.append(f"    {model_name}: {comp_df.iloc[worst_idx][model_name]:.2f}")
            report_lines.append("  → This sentence caused the biggest divergence in model scores.")
    
    # Generate full report text
    report_text = "\n".join(report_lines)
    
    # Print to console
    print(report_text)
    
    # Save to file if requested
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report_text)
        print(f"\n{'='*60}")
        print(f"Consistency report saved to: {output_path}")
        print("="*60)


def main():
    """CLI entrypoint for consistency analysis."""
    import argparse
    parser = argparse.ArgumentParser(description="Analyze consistency from saved data")
    parser.add_argument("--data-dir", default="evaluation/data/consistency", help="Data directory")
    parser.add_argument("--output", help="Output file path for the report (e.g., consistency_report.txt)")
    args = parser.parse_args()
    
    output_path = Path(args.output) if args.output else None
    analyze_consistency(Path(args.data_dir), output_path)


if __name__ == "__main__":
    main()
