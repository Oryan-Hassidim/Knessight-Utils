"""Command‑line entrypoint for evaluation reports."""

import argparse
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from .methods import metrics

# Load environment variables from .env file (or .env.example as fallback)
if not load_dotenv():
    load_dotenv('.env.example')


def load_excel(path: Path) -> pd.DataFrame:
    return pd.read_excel(path)


def run_filter_eval(path: Path, save_dir: Path | None = None, plot: bool = False, show: bool = False):
    df = load_excel(path)
    manual = df["manual_score"].tolist()
    model = df["model_score"].tolist()

    # compute
    bin_metrics = metrics.binary_metrics(manual, model)
    sweep = metrics.threshold_sweep(manual, model)

    # output to console
    print("Binary metrics (threshold=4):")
    print(bin_metrics)
    print("Threshold sweep:")
    print(sweep)

    # save results if requested
    if save_dir is not None:
        save_dir.mkdir(parents=True, exist_ok=True)
        import json

        with open(save_dir / "filter_binary_metrics.json", "w", encoding="utf-8") as f:
            json.dump(bin_metrics, f, indent=2)
        sweep.to_csv(save_dir / "filter_threshold_sweep.csv", index=False)
        print(f"Saved binary metrics and sweep CSV to {save_dir}")

    # plotting for threshold sweep only
    if plot or show:
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print("matplotlib not installed, skipping plots")
            return
        # threshold sweep plot
        plt.figure()
        ax = sweep.plot(x="threshold", y=["precision", "recall", "f1"], marker="o")
        ax.set_xticks(range(int(sweep["threshold"].min()), int(sweep["threshold"].max()) + 1))
        plt.title("Threshold Sweep")
        outpath = (save_dir / "filter_threshold_sweep.png") if save_dir else Path("filter_threshold_sweep.png")
        if plot:
            plt.savefig(outpath)
        if show:
            plt.show()
        plt.close()
        if plot:
            print(f"Plots written to {save_dir if save_dir else Path.cwd()}")

        


def run_score_eval(
    path: Path,
    save_dir: Path | None = None,
    plot: bool = False,
    show: bool = False,
    bucket_configs: list[list[float]] | None = None,
):
    df = load_excel(path)
    manual = df["manual_score"].tolist()
    model = df["model_score"].tolist()

    if bucket_configs is None:
        bucket_configs = [[3, 7]]

    for idx, boundaries in enumerate(bucket_configs):
        print(f"\nBucket confusion {boundaries}:")
        cm, kappa = metrics.bucket_confusion(manual, model, boundaries)
        
        # Calculate normalized version for display
        cm_array = cm.values
        row_sums = cm_array.sum(axis=1, keepdims=True)
        row_sums_safe = row_sums.copy()
        row_sums_safe[row_sums_safe == 0] = 1  # Avoid division by zero
        cm_normalized = (cm_array / row_sums_safe) * 100
        
        print("\nRaw counts:")
        print(cm)
        print("\nNormalized by row (%):")
        import numpy as np
        cm_norm_df = pd.DataFrame(cm_normalized, 
                                   index=cm.index, 
                                   columns=cm.columns)
        print(cm_norm_df.round(1))
        print(f"\nCohen's Kappa: {kappa:.3f}")

        # save
        if save_dir is not None:
            save_dir.mkdir(parents=True, exist_ok=True)
            suffix = "_" + "_".join(str(int(b)) for b in boundaries)
            cm.to_csv(save_dir / f"score_bucket_confusion{suffix}.csv", index=False)
            with open(save_dir / f"score_kappa{suffix}.txt", "w", encoding="utf-8") as f:
                f.write(str(kappa))
            print(f"Saved score confusion matrix and kappa to {save_dir}")

        # plotting
        if plot or show:
            try:
                import matplotlib.pyplot as plt
                import numpy as np
            except ImportError:
                print("matplotlib not installed, skipping plots")
                return
            plt.figure(figsize=(6, 5))

            # Normalize confusion matrix by row (true label) to get percentages
            cm_array = cm.values
            row_sums = cm_array.sum(axis=1, keepdims=True)
            # Avoid division by zero
            row_sums = np.where(row_sums == 0, 1, row_sums)
            cm_normalized = (cm_array / row_sums) * 100

            plt.imshow(cm_normalized, cmap="Blues", aspect="equal", vmin=0, vmax=100)
            cbar = plt.colorbar()
            cbar.set_label('Percentage (%)', rotation=270, labelpad=20)

            # build bucket labels
            labels = []
            low = 1
            for b in boundaries:
                labels.append(f"{low}-{int(b)}")
                low = int(b) + 1
            labels.append(f"{low}-10")

            # apply labels
            plt.xticks(range(len(labels)), labels)
            plt.yticks(range(len(labels)), labels)

            plt.title(f"Bucket Confusion Matrix (Normalized by Row)\nBoundaries: {boundaries}", pad=15)
            plt.xlabel("Predicted Bucket", fontweight='bold')
            plt.ylabel("True Bucket", fontweight='bold')

            # Add text annotations with percentage and count
            for i in range(cm.shape[0]):
                for j in range(cm.shape[1]):
                    count = int(cm_array[i, j])
                    percentage = cm_normalized[i, j]
                    # Show percentage and count
                    text = f"{percentage:.1f}%\n({count})"
                    # Use white text for dark cells, black for light cells
                    color = "white" if percentage > 50 else "black"
                    plt.text(j, i, text, ha="center", va="center", 
                            fontsize=10, color=color, fontweight='bold')

            suffix = "_" + "_".join(str(int(b)) for b in boundaries)

            outpath = (
                save_dir / f"score_bucket_confusion{suffix}.png"
            ) if save_dir else Path(f"score_bucket_confusion{suffix}.png")

            if plot:
                plt.savefig(outpath, bbox_inches="tight")

            if show:
                plt.show()

            plt.close()
            
            if plot:
                print(f"Plots written to {save_dir if save_dir else Path.cwd()}")

def main():
    parser = argparse.ArgumentParser(description="Run evaluation")
    parser.add_argument("--filter", action="store_true", help="Evaluate filter file")
    parser.add_argument("--score", action="store_true", help="Evaluate score file")
    parser.add_argument("--data-dir", default="evaluation/data")
    parser.add_argument("--output-dir", help="Directory where results (JSON/CSV/plots) will be written")
    parser.add_argument("--plot", action="store_true", help="Also generate plots for the requested evaluations")
    parser.add_argument("--show", action="store_true", help="Display plots interactively (requires GUI backend)")
    parser.add_argument(
        "--bucket",
        action="append",
        help="Bucket boundary set for scoring; comma-separated values (e.g. 3,7). Can be used multiple times.",
    )
    
    # Consistency generation flags
    parser.add_argument(
        "--gen-score",
        action="store_true",
        help="Generate score consistency data (1-10 stance scale)",
    )
    parser.add_argument(
        "--gen-filter",
        action="store_true",
        help="Generate filter consistency data (1-5 relevance scale)",
    )
    parser.add_argument(
        "-n", "--sentences",
        type=int,
        default=100,
        help="Number of sentences to process for consistency (default: 100)",
    )
    parser.add_argument(
        "-r", "--runs",
        type=int,
        default=3,
        help="Number of runs per model (default: 3)",
    )
    parser.add_argument(
        "-t", "--topic",
        default="התיישבות",
        help="Topic name for consistency testing (default: התיישבות)",
    )
    parser.add_argument(
        "--score-dir",
        default="evaluation/data/consistency",
        help="Output directory for score consistency (default: evaluation/data/consistency)",
    )
    parser.add_argument(
        "--filter-dir",
        default="evaluation/data/filter_consistency",
        help="Output directory for filter consistency (default: evaluation/data/filter_consistency)",
    )
    parser.add_argument(
        "--filter-threshold",
        type=float,
        default=4.0,
        help="Relevance threshold for binary filter decisions (default: 4.0)",
    )
    parser.add_argument(
        "--plot-consistency",
        action="store_true",
        help="Plot consistency results from existing data (both filter and score)",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    save_dir = Path(args.output_dir) if args.output_dir else None

    bucket_configs = None
    if args.bucket:
        bucket_configs = [list(map(float, b.split(","))) for b in args.bucket]

    if args.filter:
        path = data_dir / "filter_gold.xlsx"
        run_filter_eval(path, save_dir, args.plot, args.show)
    if args.score:
        path = data_dir / "score_gold.xlsx"
        run_score_eval(path, save_dir, args.plot, args.show, bucket_configs)

    if args.gen_score:
        # generate score consistency data
        from .methods.generate_consistency_data_unified import generate_consistency_data
        generate_consistency_data(
            mode="score",
            output_dir=Path(args.score_dir), 
            n_runs=args.runs,
            n_sentences=args.sentences,
            topic=args.topic
        )

    if args.gen_filter:
        # generate filter consistency data
        from .methods.generate_consistency_data_unified import generate_consistency_data
        generate_consistency_data(
            mode="filter",
            output_dir=Path(args.filter_dir), 
            n_runs=args.runs,
            n_sentences=args.sentences,
            topic=args.topic
        )

    if args.plot_consistency:
        # unified consistency plotting
        from .methods.plot_consistency import plot_all_consistency
        output_dir = save_dir or Path("evaluation/results")
        bucket_boundaries = bucket_configs[0] if bucket_configs else None
        plot_all_consistency(
            filter_data_dir=Path(args.filter_dir),
            score_data_dir=Path(args.score_dir),
            output_dir=output_dir,
            show=args.show,
            filter_threshold=args.filter_threshold,
            bucket_boundaries=bucket_boundaries
        )

if __name__ == "__main__":
    main()
