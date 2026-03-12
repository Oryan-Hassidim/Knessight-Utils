"""Generate filter consistency data by running models multiple times and saving results."""

import pandas as pd
from pathlib import Path
from ..models import FILTER_MODEL_REGISTRY


def load_unfiltered_sentences(n=100, topic="התיישבות"):
    """Load sentences from intermediate filtered CSVs for a specific topic.
    
    Args:
        n: Number of sentences to load
        topic: Topic name to filter files by (e.g., "התיישבות")
    
    Returns:
        List of sentences from intermediate filtered data for this topic
    """
    intermediate_dir = Path("data/intermediate")
    
    # Get filtered CSV files for this specific topic
    csv_files = list(intermediate_dir.glob(f"*_{topic}_filtered.csv"))
    
    if not csv_files:
        raise FileNotFoundError(f"No filtered CSV files found for topic '{topic}' in {intermediate_dir}")
    
    all_sentences = []
    
    # Load sentences from topic-specific CSV files
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            if "Text" in df.columns:
                sentences = df["Text"].dropna().tolist()
                all_sentences.extend(sentences)
        except Exception as e:
            print(f"Warning: Could not load {csv_file}: {e}")
            continue
    
    if not all_sentences:
        raise ValueError(f"No sentences found in filtered CSV files for topic '{topic}'")
    
    print(f"Loaded {len(all_sentences)} total sentences from {len(csv_files)} intermediate files for topic '{topic}'")
    
    # Return first n sentences
    return all_sentences[:n]


def generate_filter_consistency_data(
    output_dir: Path, 
    n_runs: int = 3, 
    n_sentences: int = 100, 
    topic: str = "התיישבות"
):
    """
    Run each filter model multiple times on the same sentences and save results.
    
    Args:
        output_dir: Directory to save CSV files
        n_runs: Number of times to run each model
        n_sentences: Number of sentences to filter
        topic: Topic name to use for loading sentences (default: "התיישבות")
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    sentences = load_unfiltered_sentences(n_sentences, topic)
    
    print(f"Loaded {len(sentences)} sentences for filter consistency testing (topic: '{topic}')")
    
    for model_name, model_fn in FILTER_MODEL_REGISTRY.items():
        print(f"Running filter model '{model_name}' {n_runs} times on {len(sentences)} sentences...")
        
        # collect relevance scores for each run
        runs_data = {"sentence": sentences}
        for run_idx in range(n_runs):
            print(f"  Run {run_idx + 1}/{n_runs}...")
            scores = model_fn(sentences)
            runs_data[f"run_{run_idx}"] = scores
        
        # save to CSV
        df = pd.DataFrame(runs_data)
        output_path = output_dir / f"filter_consistency_{model_name}.csv"
        df.to_csv(output_path, index=False)
        print(f"  Saved to {output_path}")
    
    print(f"\nFilter consistency data generation complete. Files in {output_dir}")


def main():
    """CLI entrypoint for generating filter consistency data."""
    import argparse
    parser = argparse.ArgumentParser(description="Generate filter consistency test data")
    parser.add_argument("--output-dir", default="evaluation/data/filter_consistency", help="Output directory")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per model")
    parser.add_argument("--sentences", type=int, default=100, help="Number of sentences to filter")
    parser.add_argument("--topic", default="התיישבות", help="Topic name for loading sentences")
    args = parser.parse_args()
    
    generate_filter_consistency_data(Path(args.output_dir), args.runs, args.sentences, args.topic)


if __name__ == "__main__":
    main()
