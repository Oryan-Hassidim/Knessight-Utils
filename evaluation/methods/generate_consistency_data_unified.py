"""Generate consistency data for both filter and score models using Batch API."""

import pandas as pd
import json
import yaml
from pathlib import Path
from typing import Dict, List, Literal
import sys

# Add knessight to path for batch_manager import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from knessight.modules.batch_manager import BatchManager
from ..models import MODEL_REGISTRY, FILTER_MODEL_REGISTRY


def load_sentences(n=100, topic="התיישבות"):
    """Load sentences from intermediate filtered CSVs for a specific topic.
    
    Args:
        n: Number of sentences to load
        topic: Topic name to filter files by (e.g., "התיישבות")
    
    Returns:
        List of sentences from filtered data for this topic
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
    
    print(f"Loaded {len(all_sentences)} total sentences from {len(csv_files)} files for topic '{topic}'")
    
    # Return first n sentences
    return all_sentences[:n]


def _load_scoring_prompt(topic: str = "התיישבות") -> str:
    """Load scoring prompt for a specific topic."""
    prompt_path = Path(f"data/config/scoring_prompts/{topic}.txt")
    if not prompt_path.exists():
        raise FileNotFoundError(f"Scoring prompt not found: {prompt_path}")
    return prompt_path.read_text(encoding="utf-8")


def _load_filter_prompt(topics: List[str]) -> str:
    """Load filter prompt for specific topics."""
    # Load topic descriptions
    topics_path = Path("data/config/topic_descriptions.yaml")
    if not topics_path.exists():
        raise FileNotFoundError(f"Topics config not found: {topics_path}")
    
    with open(topics_path, "r", encoding="utf-8") as f:
        topics_config = yaml.safe_load(f)
    
    # Build topic descriptions
    topic_descriptions = []
    for topic in topics:
        description = topics_config.get(topic, topic)
        topic_descriptions.append(f"- {topic}: {description}")
    
    topic_descriptions_text = "\n".join(topic_descriptions)
    
    # Load filter prompt template
    filter_prompt_path = Path("data/config/filter_prompt.txt")
    if not filter_prompt_path.exists():
        raise FileNotFoundError(f"Filter prompt not found: {filter_prompt_path}")
    
    filter_prompt_template = filter_prompt_path.read_text(encoding="utf-8")
    
    # Fill in the template
    return filter_prompt_template.replace("{topic_descriptions}", topic_descriptions_text)


def _build_scoring_request(
    sentence: str, 
    sentence_idx: int, 
    model_name: str, 
    run_idx: int, 
    temperature: float, 
    topic: str
) -> Dict:
    """Build a single scoring request for Batch API."""
    scoring_prompt = _load_scoring_prompt(topic)
    
    system_prompt = (
        "You are a political speech analyst. Score stance on topics from 1 (strongly opposes) "
        "to 10 (strongly supports). Respond with JSON only."
    )
    
    user_message = f"""Speech Text:
{sentence}

Topic: {topic}

{scoring_prompt}

Respond with JSON only, format:
{{
  "stance_score": 1-10
}}"""
    
    # Map model registry names to actual OpenAI model names
    model_mapping = {
        "gpt-4o-mini": "gpt-4o-mini",
        "gpt-4o": "gpt-4o",
        "gpt-4o-mini-temp1": "gpt-4o-mini",
    }
    
    openai_model = model_mapping.get(model_name, "gpt-4o-mini")
    
    return {
        "custom_id": f"{model_name}_run{run_idx}_sent{sentence_idx}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": openai_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "response_format": {"type": "json_object"},
            "temperature": temperature,
        },
    }


def _build_filter_request(
    sentence: str, 
    sentence_idx: int, 
    model_name: str, 
    run_idx: int, 
    temperature: float, 
    topics: List[str]
) -> Dict:
    """Build a single filter request for Batch API."""
    filter_prompt = _load_filter_prompt(topics)
    
    system_prompt = (
        "You are a political speech analyst. Rate speech relevance to topics "
        "on a scale of 1-5. Respond with JSON only."
    )
    
    user_message = f"""Speech Text:
{sentence}

Topics to evaluate:
{', '.join(topics)}

{filter_prompt}

Respond with JSON only, format:
{{
  "topic1": {{"relevance": 1-5}},
  "topic2": {{"relevance": 1-5}},
  ...
}}"""
    
    # Map model registry names to actual OpenAI model names
    model_mapping = {
        "gpt-4o-mini": "gpt-4o-mini",
        "gpt-4o": "gpt-4o",
        "gpt-4o-mini-temp1": "gpt-4o-mini",
    }
    
    openai_model = model_mapping.get(model_name, "gpt-4o-mini")
    
    return {
        "custom_id": f"{model_name}_run{run_idx}_sent{sentence_idx}",
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {
            "model": openai_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "response_format": {"type": "json_object"},
            "temperature": temperature,
        },
    }


def generate_consistency_data(
    mode: Literal["filter", "score"],
    output_dir: Path, 
    n_runs: int = 3, 
    n_sentences: int = 100, 
    topic: str = "התיישבות"
):
    """
    Run models multiple times on the same sentences and save results using Batch API.
    If data already exists, appends new sentences instead of overriding.
    
    Args:
        mode: Either "filter" (1-5 relevance) or "score" (1-10 stance)
        output_dir: Directory to save CSV files
        n_runs: Number of times to run each model
        n_sentences: Number of sentences to process
        topic: Topic name to use for loading sentences and prompts (default: "התיישבות")
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    sentences = load_sentences(n_sentences, topic)
    
    # Select appropriate registry and config based on mode
    if mode == "filter":
        model_registry = FILTER_MODEL_REGISTRY
        file_prefix = "filter_consistency"
        default_score = 3.0
        score_key = "relevance"
        phase_name = "consistency_filtering"
        print(f"Loaded {len(sentences)} sentences for FILTER consistency testing (topic: '{topic}')")
    else:  # score
        model_registry = MODEL_REGISTRY
        file_prefix = "consistency"
        default_score = 5.0
        score_key = "stance_score"
        phase_name = "consistency_scoring"
        print(f"Loaded {len(sentences)} sentences for SCORE consistency testing (topic: '{topic}')")
    
    # Initialize batch manager
    batch_manager = BatchManager()
    
    # Track which sentences are new for each model
    model_new_sentences: Dict[str, List[str]] = {}
    model_existing_data: Dict[str, pd.DataFrame] = {}
    
    # Check existing data for each model
    for model_name in model_registry.keys():
        output_path = output_dir / f"{file_prefix}_{model_name}.csv"
        
        existing_sentences = set()
        existing_df = None
        if output_path.exists():
            existing_df = pd.read_csv(output_path)
            existing_sentences = set(existing_df["sentence"].tolist())
            print(f"Found existing data for '{model_name}' with {len(existing_sentences)} sentences")
        
        # Filter out sentences that already exist
        new_sentences = [s for s in sentences if s not in existing_sentences]
        
        if not new_sentences:
            print(f"All sentences already exist for '{model_name}', skipping...")
            model_new_sentences[model_name] = []
        else:
            print(f"Will process {len(new_sentences)} NEW sentences for '{model_name}'")
            model_new_sentences[model_name] = new_sentences
        
        model_existing_data[model_name] = existing_df
    
    # Build batch requests grouped by model (OpenAI requires same model per batch)
    requests_by_model: Dict[str, List[Dict]] = {
        model_name: [] for model_name in model_registry.keys()
    }
    
    # Temperature settings for each model
    temperature_map = {
        "gpt-4o-mini": 0.3,
        "gpt-4o": 0.3,
        "gpt-4o-mini-temp1": 1.0,
    }
    
    topics = [topic]  # Convert to list for filter mode
    
    for model_name in model_registry.keys():
        new_sentences = model_new_sentences[model_name]
        if not new_sentences:
            continue
        
        temperature = temperature_map.get(model_name, 0.3)
        
        for run_idx in range(n_runs):
            for sent_idx, sentence in enumerate(new_sentences):
                if mode == "filter":
                    request = _build_filter_request(
                        sentence, sent_idx, model_name, run_idx, temperature, topics
                    )
                else:  # score
                    request = _build_scoring_request(
                        sentence, sent_idx, model_name, run_idx, temperature, topic
                    )
                requests_by_model[model_name].append(request)
    
    # Count total requests
    total_requests = sum(len(reqs) for reqs in requests_by_model.values())
    if total_requests == 0:
        print("No new sentences to process!")
        return
    
    print(f"\nBuilt {total_requests} batch requests across {len([r for r in requests_by_model.values() if r])} models ({n_runs} runs × sentences)")
    
    # Submit separate batch for each model (OpenAI constraint)
    batch_ids = []
    batch_id_to_model = {}  # Track which batch belongs to which model
    
    for model_name, requests in requests_by_model.items():
        if not requests:
            continue
        
        print(f"Submitting batch for model '{model_name}' ({len(requests)} requests)...")
        
        batch_id = batch_manager.create_batch(
            requests,
            metadata={
                "phase": phase_name,
                "topic": topic,
                "model": model_name,
                "n_runs": str(n_runs),
            },
        )
        batch_ids.append(batch_id)
        batch_id_to_model[batch_id] = model_name
    
    print(f"\nSubmitted {len(batch_ids)} batch(es) (one per model)")
    
    # Poll batches with 15-second intervals (faster than default 30s)
    print("\nPolling batches... This may take a while for the Batch API to complete.")
    results_map = batch_manager.poll_batches(batch_ids, interval=15)
    
    # Check if all batches completed successfully
    failed = [bid for bid, status in results_map.items() if status != "completed"]
    if failed:
        print(f"\n[ERROR] {len(failed)} batch(es) failed: {failed}")
        print("Please check batch_jobs.json and retry")
        return
    
    print("\nAll batches completed successfully!")
    
    # Retrieve and organize results
    all_results = []
    for batch_id in batch_ids:
        batch_results = batch_manager.retrieve_results(batch_id)
        all_results.extend(batch_results)
    
    print(f"Retrieved {len(all_results)} results")
    
    # Parse results and organize by model/run
    # results_by_model[model_name][run_idx][sent_idx] = score
    results_by_model: Dict[str, Dict[int, Dict[int, float]]] = {
        model_name: {run_idx: {} for run_idx in range(n_runs)}
        for model_name in model_registry.keys()
    }
    
    for result in all_results:
        custom_id = result["custom_id"]
        # Parse custom_id: "model_name_runX_sentY"
        parts = custom_id.split("_")
        model_name = "_".join(parts[:-2])  # Handle model names with underscores
        run_idx = int(parts[-2].replace("run", ""))
        sent_idx = int(parts[-1].replace("sent", ""))
        
        # Extract score from response
        try:
            response_content = result["response"]["body"]["choices"][0]["message"]["content"]
            parsed = json.loads(response_content)
            
            if mode == "filter":
                # Extract relevance score for the first topic
                score = float(parsed.get(topics[0], {}).get(score_key, default_score))
            else:  # score
                score = float(parsed.get(score_key, default_score))
            
            results_by_model[model_name][run_idx][sent_idx] = score
        except Exception as e:
            print(f"Warning: Failed to parse result for {custom_id}: {e}")
            results_by_model[model_name][run_idx][sent_idx] = default_score
    
    # Save results for each model
    for model_name in model_registry.keys():
        new_sentences = model_new_sentences[model_name]
        if not new_sentences:
            continue
        
        output_path = output_dir / f"{file_prefix}_{model_name}.csv"
        
        # Build DataFrame from results
        runs_data = {"sentence": new_sentences}
        for run_idx in range(n_runs):
            scores = [
                results_by_model[model_name][run_idx].get(sent_idx, default_score)
                for sent_idx in range(len(new_sentences))
            ]
            runs_data[f"run_{run_idx}"] = scores
        
        new_df = pd.DataFrame(runs_data)
        
        # Append to existing data if it exists
        existing_df = model_existing_data[model_name]
        if existing_df is not None:
            df = pd.concat([existing_df, new_df], ignore_index=True)
            print(f"Appended {len(new_sentences)} sentences to existing {len(existing_df)} sentences for '{model_name}'")
        else:
            df = new_df
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        print(f"Saved {len(df)} total sentences to {output_path}")
    
    print(f"\n✅ {mode.upper()} consistency data generation complete. Files in {output_dir}")


def main():
    """CLI entrypoint for generating consistency data."""
    import argparse
    parser = argparse.ArgumentParser(description="Generate consistency test data")
    parser.add_argument("--mode", choices=["filter", "score"], required=True, help="Generation mode: filter or score")
    parser.add_argument("--output-dir", help="Output directory (default: auto-selected based on mode)")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per model")
    parser.add_argument("--sentences", type=int, default=100, help="Number of sentences to process")
    parser.add_argument("--topic", default="התיישבות", help="Topic name for loading sentences and prompts")
    args = parser.parse_args()
    
    # Auto-select output directory based on mode if not specified
    if args.output_dir is None:
        if args.mode == "filter":
            args.output_dir = "evaluation/data/filter_consistency"
        else:
            args.output_dir = "evaluation/data/consistency"
    
    generate_consistency_data(
        mode=args.mode,
        output_dir=Path(args.output_dir), 
        n_runs=args.runs, 
        n_sentences=args.sentences, 
        topic=args.topic
    )


if __name__ == "__main__":
    main()
