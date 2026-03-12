"""Command‑line entrypoint for evaluation reports."""

import argparse
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from . import metrics

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
        print(f"Bucket confusion {boundaries}:")
        cm, kappa = metrics.bucket_confusion(manual, model, boundaries)
        print(cm)
        print("Kappa=", kappa)

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
            except ImportError:
                print("matplotlib not installed, skipping plots")
                return
            plt.figure(figsize=(5, 5))

            plt.imshow(cm, cmap="Blues", aspect="equal")
            plt.colorbar()

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

            plt.title(f"Bucket Confusion {boundaries}")
            plt.xlabel("Predicted bucket")
            plt.ylabel("True bucket")

            # numbers inside cells
            for i in range(cm.shape[0]):
                for j in range(cm.shape[1]):
                    plt.text(j, i, int(cm.iat[i, j]), ha="center", va="center")

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
    parser.add_argument(
        "--consistency",
        action="store_true",
        help="Analyze consistency from saved data (run --generate-consistency first)",
    )
    parser.add_argument(
        "--generate-consistency",
        action="store_true",
        help="Generate consistency data by running models multiple times",
    )
    parser.add_argument(
        "--consistency-dir",
        default="evaluation/data/consistency",
        help="Directory for consistency data (default: evaluation/data/consistency)",
    )
    parser.add_argument(
        "--consistency-runs",
        type=int,
        default=3,
        help="Number of runs per model for consistency generation (default: 3)",
    )
    parser.add_argument(
        "--consistency-sentences",
        type=int,
        default=100,
        help="Number of sentences to score for consistency testing (default: 100)",
    )
    parser.add_argument(
        "--consistency-topic",
        default="התיישבות",
        help="Topic name for consistency testing (default: התיישבות)",
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

    if args.generate_consistency:
        # generate consistency data
        from .experiments.generate_consistency_data import generate_consistency_data
        generate_consistency_data(
            Path(args.consistency_dir), 
            n_runs=args.consistency_runs,
            n_sentences=args.consistency_sentences,
            topic=args.consistency_topic
        )

    if args.consistency:
        # analyze existing consistency data
        from .experiments.consistency import analyze_consistency
        output_path = (save_dir / "consistency_report.txt") if save_dir else None
        analyze_consistency(Path(args.consistency_dir), output_path)

if __name__ == "__main__":
    main()
