"""Unified consistency plotting from existing data files."""

from pathlib import Path
import pandas as pd
import numpy as np


def plot_all_consistency(
    filter_data_dir: Path = Path("evaluation/data/filter_consistency"),
    score_data_dir: Path = Path("evaluation/data/consistency"),
    output_dir: Path = Path("evaluation/results"),
    show: bool = False,
    filter_threshold: float = 4.0,
    bucket_boundaries: list[float] = None
):
    """
    Generate all consistency plots from existing data files.
    
    Args:
        filter_data_dir: Directory with filter_consistency_*.csv files
        score_data_dir: Directory with consistency_*.csv files
        output_dir: Directory to save plots
        show: Whether to display plots interactively
        filter_threshold: Threshold for filter binary decisions
        bucket_boundaries: Bucket boundaries for score consistency
    """
    if bucket_boundaries is None:
        bucket_boundaries = [3, 7]
    
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed, skipping plots")
        return
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Check what data is available
    has_filter = filter_data_dir.exists() and list(filter_data_dir.glob("filter_consistency_*.csv"))
    has_score = score_data_dir.exists() and list(score_data_dir.glob("consistency_*.csv"))
    
    if not has_filter and not has_score:
        print("No consistency data found. Run --generate-filter-consistency or --generate-consistency first.")
        return
    
    # Process filter consistency if available
    if has_filter:
        print("Generating filter consistency plots...")
        filter_metrics = _analyze_filter_consistency(filter_data_dir, filter_threshold)
        _create_consistency_plot(
            metrics=filter_metrics,
            plot_type="filter",
            output_dir=output_dir,
            show=show
        )
        print(f"  → Saved to {output_dir / 'filter_consistency_summary.png'}")
    
    # Process score consistency if available
    if has_score:
        print("Generating score consistency plots...")
        score_metrics = _analyze_score_consistency(score_data_dir, bucket_boundaries)
        _create_consistency_plot(
            metrics=score_metrics,
            plot_type="score",
            output_dir=output_dir,
            show=show
        )
        print(f"  → Saved to {output_dir / 'score_consistency_summary.png'}")
    
    print(f"\nAll consistency plots saved to {output_dir}")


def _analyze_filter_consistency(data_dir: Path, threshold: float):
    """Analyze filter consistency data and extract metrics."""
    csv_files = list(data_dir.glob("filter_consistency_*.csv"))
    
    metrics = {
        'binary_agreement': {},
        'score_variability': {},
        'inter_model_agreement': {}
    }
    
    all_models_data = {}
    
    for csv_path in csv_files:
        model_name = csv_path.stem.replace("filter_consistency_", "")
        df = pd.read_csv(csv_path)
        all_models_data[model_name] = df
        
        run_cols = [col for col in df.columns if col.startswith("run_")]
        scores_array = df[run_cols].values
        
        # Binary agreement
        binary_array = (scores_array >= threshold).astype(int)
        def calc_agreement_rate(row):
            unique, counts = np.unique(row, return_counts=True)
            return counts.max() / len(row)
        
        agreement_rates = np.array([calc_agreement_rate(binary_array[i]) for i in range(len(binary_array))])
        metrics['binary_agreement'][model_name] = {
            'mean_agreement_pct': agreement_rates.mean() * 100
        }
        
        # Score variability
        per_sentence_std = np.std(scores_array, axis=1, ddof=1)
        metrics['score_variability'][model_name] = {
            'mean_std': per_sentence_std.mean()
        }
    
    # Inter-model metrics
    if len(all_models_data) > 1:
        model_names = sorted(all_models_data.keys())
        comparison_data = {}
        
        base_sentences = all_models_data[model_names[0]]["sentence"].tolist()
        comparison_data["sentence"] = base_sentences
        
        for model_name in model_names:
            df = all_models_data[model_name]
            run_cols = [col for col in df.columns if col.startswith("run_")]
            comparison_data[model_name] = df[run_cols].mean(axis=1).tolist()
        
        comp_df = pd.DataFrame(comparison_data)
        
        for i, model1 in enumerate(model_names):
            for model2 in model_names[i+1:]:
                binary1 = comp_df[model1] >= threshold
                binary2 = comp_df[model2] >= threshold
                agreement = (binary1 == binary2).mean() * 100
                pair_key = f"{model1} vs {model2}"
                metrics['inter_model_agreement'][pair_key] = agreement
    
    return metrics


def _analyze_score_consistency(data_dir: Path, bucket_boundaries: list[float]):
    """Analyze score consistency data and extract metrics."""
    csv_files = list(data_dir.glob("consistency_*.csv"))
    
    def assign_bucket(x):
        lower = 1
        for i, b in enumerate(bucket_boundaries):
            if lower <= x <= b:
                return i
            lower = int(b) + 1
        return len(bucket_boundaries)
    
    metrics = {
        'bucket_agreement': {},
        'score_variability': {},
        'inter_model_mad': {}
    }
    
    all_models_data = {}
    
    for csv_path in csv_files:
        model_name = csv_path.stem.replace("consistency_", "")
        df = pd.read_csv(csv_path)
        all_models_data[model_name] = df
        
        run_cols = [col for col in df.columns if col.startswith("run_")]
        scores_array = df[run_cols].values
        
        # Bucket agreement
        bucket_assignments = np.zeros_like(scores_array, dtype=int)
        for i in range(scores_array.shape[0]):
            for j in range(scores_array.shape[1]):
                bucket_assignments[i, j] = assign_bucket(scores_array[i, j])
        
        def calc_bucket_agreement_rate(buckets):
            unique, counts = np.unique(buckets, return_counts=True)
            return counts.max() / len(buckets)
        
        bucket_agreement_rates = np.array([calc_bucket_agreement_rate(bucket_assignments[i]) for i in range(len(bucket_assignments))])
        metrics['bucket_agreement'][model_name] = {
            'mean_agreement_pct': bucket_agreement_rates.mean() * 100
        }
        
        # Score variability
        per_sentence_std = np.std(scores_array, axis=1, ddof=1)
        metrics['score_variability'][model_name] = {
            'mean_std': per_sentence_std.mean()
        }
    
    # Inter-model metrics
    if len(all_models_data) > 1:
        model_names = sorted(all_models_data.keys())
        comparison_data = {}
        
        base_sentences = all_models_data[model_names[0]]["sentence"].tolist()
        comparison_data["sentence"] = base_sentences
        
        for model_name in model_names:
            df = all_models_data[model_name]
            run_cols = [col for col in df.columns if col.startswith("run_")]
            comparison_data[model_name] = df[run_cols].mean(axis=1).tolist()
        
        comp_df = pd.DataFrame(comparison_data)
        
        for i, model1 in enumerate(model_names):
            for model2 in model_names[i+1:]:
                mad = np.abs(comp_df[model1] - comp_df[model2]).mean()
                pair_key = f"{model1} vs {model2}"
                metrics['inter_model_mad'][pair_key] = mad
    
    return metrics


def _create_consistency_plot(metrics, plot_type: str, output_dir: Path, show: bool):
    """Create unified consistency visualization."""
    import matplotlib.pyplot as plt
    
    # Determine which metrics to use based on plot type
    if plot_type == "filter":
        intra_metric1_key = 'binary_agreement'
        intra_metric1_label = 'Mean Agreement Rate'
        intra_metric1_ylabel = 'Agreement Rate (%)'
        intra_metric1_data = [metrics['binary_agreement'][m]['mean_agreement_pct'] for m in metrics['binary_agreement'].keys()]
        
        intra_metric2_key = 'score_variability'
        intra_metric2_label = 'Mean Std Dev'
        intra_metric2_ylabel = 'Standard Deviation (1-5 scale)'
        intra_metric2_data = [metrics['score_variability'][m]['mean_std'] for m in metrics['score_variability'].keys()]
        
        inter_metric_key = 'inter_model_agreement'
        inter_ylabel = 'Binary Agreement Rate (%)'
        inter_title = 'Inter-Model: Binary Decision Agreement'
        inter_ylim = [0, 105]
        
        plot_title = 'Filter Consistency Analysis'
        filename = 'filter_consistency_summary.png'
    else:  # score
        intra_metric1_key = 'bucket_agreement'
        intra_metric1_label = 'Mean Bucket Agreement'
        intra_metric1_ylabel = 'Bucket Agreement Rate (%)'
        intra_metric1_data = [metrics['bucket_agreement'][m]['mean_agreement_pct'] for m in metrics['bucket_agreement'].keys()]
        
        intra_metric2_key = 'score_variability'
        intra_metric2_label = 'Mean Std Dev'
        intra_metric2_ylabel = 'Standard Deviation (1-10 scale)'
        intra_metric2_data = [metrics['score_variability'][m]['mean_std'] for m in metrics['score_variability'].keys()]
        
        inter_metric_key = 'inter_model_mad'
        inter_ylabel = 'Mean Absolute Difference (1-10 scale)'
        inter_title = 'Inter-Model: Mean Absolute Difference'
        inter_ylim = None
        
        plot_title = 'Score Consistency Analysis'
        filename = 'score_consistency_summary.png'
    
    models = list(metrics[intra_metric1_key].keys())
    
    # Create figure with 2 subplots
    fig = plt.figure(figsize=(16, 6))
    
    # Subplot 1: Intra-model consistency
    ax1 = plt.subplot(1, 2, 1)
    x = np.arange(len(models))
    width = 0.35
    
    color1 = 'steelblue'
    bars1 = ax1.bar(x - width/2, intra_metric1_data, width, label=intra_metric1_label, color=color1, alpha=0.8)
    ax1.set_xlabel('Model', fontsize=12, fontweight='bold')
    ax1.set_ylabel(intra_metric1_ylabel, fontsize=12, fontweight='bold', color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.set_ylim([0, 105])
    ax1.set_xticks(x)
    ax1.set_xticklabels(models, rotation=45, ha='right')
    
    # Add value labels
    for bar, val in zip(bars1, intra_metric1_data):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1.5,
                f'{val:.1f}%', ha='center', va='bottom', fontsize=9, color=color1, fontweight='bold')
    
    # Second y-axis
    ax2 = ax1.twinx()
    color2 = 'coral'
    bars2 = ax2.bar(x + width/2, intra_metric2_data, width, label=intra_metric2_label, color=color2, alpha=0.8)
    ax2.set_ylabel(intra_metric2_ylabel, fontsize=12, fontweight='bold', color=color2)
    ax2.tick_params(axis='y', labelcolor=color2)
    
    # Add value labels
    for bar, val in zip(bars2, intra_metric2_data):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, height + ax2.get_ylim()[1]*0.02,
                f'{val:.3f}', ha='center', va='bottom', fontsize=9, color=color2, fontweight='bold')
    
    ax1.set_title('Intra-Model: Agreement & Variability', fontsize=13, fontweight='bold', pad=15)
    
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', framealpha=0.9, fontsize=9)
    ax1.grid(axis='y', alpha=0.3)
    
    # Subplot 2: Inter-model consistency
    ax3 = plt.subplot(1, 2, 2)
    
    if metrics[inter_metric_key]:
        pair_names = list(metrics[inter_metric_key].keys())
        pair_values = [metrics[inter_metric_key][p] for p in pair_names]
        
        # Shorten names
        short_names = []
        for name in pair_names:
            parts = name.split(' vs ')
            if len(parts) == 2:
                short1 = parts[0].replace('gpt-4o-mini-temp1', '4o-mini-t1').replace('gpt-4o-mini', '4o-mini').replace('gpt-4o', '4o')
                short2 = parts[1].replace('gpt-4o-mini-temp1', '4o-mini-t1').replace('gpt-4o-mini', '4o-mini').replace('gpt-4o', '4o')
                short_names.append(f"{short1}\nvs\n{short2}")
            else:
                short_names.append(name)
        
        x_inter = np.arange(len(pair_names))
        bars_inter = ax3.bar(x_inter, pair_values, color='mediumseagreen', alpha=0.8)
        ax3.set_xlabel('Model Pairs', fontsize=12, fontweight='bold')
        ax3.set_ylabel(inter_ylabel, fontsize=12, fontweight='bold')
        if inter_ylim:
            ax3.set_ylim(inter_ylim)
        ax3.set_xticks(x_inter)
        ax3.set_xticklabels(short_names, fontsize=9)
        ax3.set_title(inter_title, fontsize=13, fontweight='bold', pad=15)
        
        # Add value labels
        for bar, val in zip(bars_inter, pair_values):
            y_offset = 1.5 if inter_ylim else ax3.get_ylim()[1]*0.02
            format_str = '{:.1f}%' if plot_type == 'filter' else '{:.3f}'
            ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + y_offset,
                    format_str.format(val), ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        ax3.grid(axis='y', alpha=0.3)
    else:
        ax3.text(0.5, 0.5, 'No inter-model data\n(requires multiple models)', 
                ha='center', va='center', fontsize=12, transform=ax3.transAxes)
        ax3.set_title(inter_title, fontsize=13, fontweight='bold', pad=15)
    
    plt.suptitle(plot_title, fontsize=15, fontweight='bold', y=0.98)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    plt.savefig(output_dir / filename, dpi=150, bbox_inches='tight')
    if show:
        plt.show()
    plt.close()


def main():
    """CLI entrypoint."""
    import argparse
    parser = argparse.ArgumentParser(description="Generate all consistency plots from existing data")
    parser.add_argument("--filter-data-dir", default="evaluation/data/filter_consistency", help="Filter consistency data directory")
    parser.add_argument("--score-data-dir", default="evaluation/data/consistency", help="Score consistency data directory")
    parser.add_argument("--output-dir", default="evaluation/results", help="Output directory for plots")
    parser.add_argument("--show", action="store_true", help="Display plots interactively")
    parser.add_argument("--filter-threshold", type=float, default=4.0, help="Filter threshold")
    parser.add_argument("--bucket", help="Bucket boundaries (comma-separated)")
    args = parser.parse_args()
    
    bucket_boundaries = [float(x) for x in args.bucket.split(',')] if args.bucket else None
    
    plot_all_consistency(
        filter_data_dir=Path(args.filter_data_dir),
        score_data_dir=Path(args.score_data_dir),
        output_dir=Path(args.output_dir),
        show=args.show,
        filter_threshold=args.filter_threshold,
        bucket_boundaries=bucket_boundaries
    )


if __name__ == "__main__":
    main()
