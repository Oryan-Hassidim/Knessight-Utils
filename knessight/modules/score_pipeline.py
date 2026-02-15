"""Phase 2: Score pipeline - stance scoring with reasoning sampling."""

import csv
import json
import random
from pathlib import Path
from typing import List, Dict, Tuple

from rich.console import Console

from .config import Config
from .database import Database
from .batch_manager import BatchManager
from .job_tracker import JobTracker
from .output import OutputManager


class ScorePipeline:
    """Handles Phase 2: scoring stance on topics with optional reasoning."""

    def __init__(
        self,
        config: Config,
        database: Database,
        batch_manager: BatchManager,
        job_tracker: JobTracker,
        output_manager: OutputManager,
        intermediate_dir: Path = None,
        client_data_dir: Path = None,
    ):
        """Initialize score pipeline.

        Args:
            config: Configuration manager
            database: Database instance
            batch_manager: Batch API manager
            job_tracker: Job tracker
            output_manager: Output manager for aggregation
            intermediate_dir: Directory with filtered speeches
            client_data_dir: Directory for final outputs
        """
        self.config = config
        self.database = database
        self.batch_manager = batch_manager
        self.job_tracker = job_tracker
        self.output_manager = output_manager
        self.console = Console()

        if intermediate_dir is None:
            intermediate_dir = Path.cwd() / "data" / "intermediate"
        if client_data_dir is None:
            client_data_dir = Path.cwd() / "data" / "client_data"

        self.intermediate_dir = Path(intermediate_dir)
        self.client_data_dir = Path(client_data_dir)

    def run(self, pairs: List[Tuple[int, str]], reasoning_rate: float = None):
        """Execute score pipeline for filter-complete pairs.

        Args:
            pairs: List of (person_id, topic) pairs to process
            reasoning_rate: Override config reasoning sample rate
        """
        if not pairs:
            self.console.print("[yellow]No pairs to score[/yellow]")
            return

        if reasoning_rate is None:
            reasoning_rate = self.config.REASONING_SAMPLE_RATE

        self.console.print(
            f"[cyan]Starting score pipeline for {len(pairs)} pairs (reasoning rate: {reasoning_rate:.1%})[/cyan]"
        )

        # Process each pair
        all_batch_ids = []

        for person_id, topic in pairs:
            batch_ids = self._process_pair(person_id, topic, reasoning_rate)
            all_batch_ids.extend(batch_ids)

        # Poll for completion
        if all_batch_ids:
            self.console.print(
                f"\n[cyan]Polling {len(all_batch_ids)} batch jobs...[/cyan]"
            )
            results = self.batch_manager.poll_batches(
                all_batch_ids, self.config.BATCH_POLL_INTERVAL
            )

            # Process results
            for batch_id, status in results.items():
                if status == "completed":
                    self._process_batch_results(batch_id)
                else:
                    self.console.print(
                        f"[red]Batch {batch_id} failed with status: {status}[/red]"
                    )

    def _process_pair(
        self, person_id: int, topic: str, reasoning_rate: float
    ) -> List[str]:
        """Process filtered speeches for scoring.

        Args:
            person_id: MK person_id
            topic: Topic name
            reasoning_rate: Probability of requesting reasoning

        Returns:
            List of batch job IDs created
        """
        # Load filtered speeches
        csv_path = self.intermediate_dir / f"{person_id}_{topic}_filtered.csv"

        if not csv_path.exists():
            self.console.print(
                f"[yellow]No filtered speeches found: {csv_path.name}[/yellow]"
            )
            return []

        filtered_speeches = []
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            filtered_speeches = list(reader)

        if not filtered_speeches:
            self.console.print(
                f"[yellow]Empty filtered speeches: {csv_path.name}[/yellow]"
            )
            return []

        self.console.print(
            f"[cyan]Scoring {len(filtered_speeches)} speeches for person_id {person_id}, topic: {topic}[/cyan]"
        )

        # Load scoring prompt
        scoring_prompt = self.config.load_scoring_prompt(topic)

        # Build scoring requests
        requests = []
        for speech_data in filtered_speeches:
            speech_id = int(speech_data["Id"])
            text = speech_data["Text"]

            # Random sampling for reasoning
            include_reasoning = random.random() < reasoning_rate

            request = self._build_scoring_request(
                speech_id, text, topic, scoring_prompt, include_reasoning
            )
            requests.append(request)

        # Split into batches
        batch_ids = []
        batch_size = self.config.BATCH_SIZE

        for i in range(0, len(requests), batch_size):
            batch_requests = requests[i : i + batch_size]

            batch_id = self.batch_manager.create_batch(
                batch_requests,
                metadata={
                    "phase": "score",
                    "person_id": str(person_id),
                    "topic": topic,
                    "batch_index": str(i // batch_size),
                },
            )
            batch_ids.append(batch_id)

        return batch_ids

    def _build_scoring_request(
        self,
        speech_id: int,
        text: str,
        topic: str,
        scoring_prompt: str,
        include_reasoning: bool,
    ) -> Dict:
        """Build a single scoring request for Batch API.

        Args:
            speech_id: Speech ID
            text: Speech text
            topic: Topic name
            scoring_prompt: Scoring prompt for this topic
            include_reasoning: Whether to request reasoning

        Returns:
            Batch API request object
        """
        reasoning_instruction = ""
        if include_reasoning:
            reasoning_instruction = (
                "\nAlso provide a one-sentence reasoning for your score."
            )

        user_message = f"""Speech Text:
{text}

Topic: {topic}

{scoring_prompt}{reasoning_instruction}

Respond with JSON only, format:
{{
  "stance_score": 1-10{', "reasoning": "one sentence"' if include_reasoning else ''}
}}"""

        return {
            "custom_id": f"score_{speech_id}_{int(include_reasoning)}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o-mini",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a political speech analyst. Score stance on topics from 1 (strongly opposes) to 10 (strongly supports). Respond with JSON only.",
                    },
                    {"role": "user", "content": user_message},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.3,
            },
        }

    def _process_batch_results(self, batch_id: str):
        """Process completed scoring batch and save results.

        Args:
            batch_id: Completed batch job ID
        """
        try:
            results = self.batch_manager.retrieve_results(batch_id)

            # Get metadata
            batch_info = self.batch_manager._batch_jobs.get(batch_id, {})
            person_id = batch_info.get("person_id")
            topic = batch_info.get("topic")

            # Convert person_id from string back to int
            if person_id:
                person_id = int(person_id)

            if not person_id or not topic:
                self.console.print(f"[red]Missing metadata for batch {batch_id}[/red]")
                return

            # Collect scored speeches with reasoning
            scored_speeches = []

            for result in results:
                if result.get("response", {}).get("status_code") != 200:
                    continue

                # Extract speech_id from custom_id
                custom_id = result.get("custom_id", "")
                if not custom_id.startswith("score_"):
                    continue

                parts = custom_id.split("_")
                speech_id = int(parts[1])
                has_reasoning = bool(int(parts[2]))

                # Parse response
                try:
                    response_body = result["response"]["body"]
                    content = response_body["choices"][0]["message"]["content"]
                    score_data = json.loads(content)

                    stance_score = score_data.get("stance_score")
                    reasoning = score_data.get("reasoning")

                    if stance_score is None:
                        continue

                    # Get speech metadata
                    metadata = self.database.get_speech_metadata(speech_id)
                    if not metadata:
                        continue

                    # Load speech text from intermediate file
                    csv_path = (
                        self.intermediate_dir / f"{person_id}_{topic}_filtered.csv"
                    )
                    speech_text = self._get_speech_text_from_csv(csv_path, speech_id)

                    scored_speeches.append(
                        {
                            "Id": speech_id,
                            "Date": metadata["date"],
                            "Topic": topic,
                            "Text": speech_text,
                            "Rank": stance_score,
                            "Reasoning": (
                                reasoning if (has_reasoning and reasoning) else ""
                            ),
                        }
                    )

                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    self.console.print(
                        f"[red]Error parsing result for speech {speech_id}: {e}[/red]"
                    )
                    continue

            # Save scored speeches with reasoning included
            if scored_speeches:
                self._save_scored_speeches(person_id, topic, scored_speeches)

            # Mark as score_complete and trigger aggregation
            self.job_tracker.mark_score_complete(person_id, topic, [batch_id])
            self.output_manager.update_aggregations(person_id, topic, scored_speeches)

        except Exception as e:
            self.console.print(f"[red]Error processing batch {batch_id}: {e}[/red]")

    def _get_speech_text_from_csv(self, csv_path: Path, speech_id: int) -> str:
        """Get speech text from intermediate CSV.

        Args:
            csv_path: Path to filtered CSV
            speech_id: Speech ID

        Returns:
            Speech text
        """
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if int(row["Id"]) == speech_id:
                    return row["Text"]
        return ""

    def _save_scored_speeches(self, person_id: int, topic: str, speeches: List[Dict]):
        """Save scored speeches to final CSV.

        Args:
            person_id: MK person_id
            topic: Topic name
            speeches: List of scored speech dicts
        """
        mk_dir = self.client_data_dir / "mk_data" / str(person_id)
        mk_dir.mkdir(parents=True, exist_ok=True)

        csv_path = mk_dir / f"{topic}.csv"

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["Id", "Date", "Topic", "Text", "Rank", "Reasoning"]
            )
            writer.writeheader()
            writer.writerows(speeches)

        # Count how many have reasoning
        reasoning_count = sum(1 for s in speeches if s.get("Reasoning"))
        self.console.print(
            f"[green]Saved {len(speeches)} scored speeches to {csv_path} ({reasoning_count} with reasoning)[/green]"
        )
