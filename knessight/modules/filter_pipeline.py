"""Phase 1: Filter pipeline - multi-topic relevance scoring."""

import csv
import json
from pathlib import Path
from typing import List, Dict, Tuple
from collections import defaultdict

from rich.console import Console

from .config import Config
from .database import Database, Speech
from .batch_manager import BatchManager
from .job_tracker import JobTracker


class FilterPipeline:
    """Handles Phase 1: filtering speeches for relevance to topics."""

    def __init__(
        self,
        config: Config,
        database: Database,
        batch_manager: BatchManager,
        job_tracker: JobTracker,
        intermediate_dir: Path = None,
    ):
        """Initialize filter pipeline.

        Args:
            config: Configuration manager
            database: Database instance
            batch_manager: Batch API manager
            job_tracker: Job tracker
            intermediate_dir: Directory for intermediate CSV outputs
        """
        self.config = config
        self.database = database
        self.batch_manager = batch_manager
        self.job_tracker = job_tracker
        self.console = Console()

        if intermediate_dir is None:
            intermediate_dir = Path.cwd() / "data" / "intermediate"

        self.intermediate_dir = Path(intermediate_dir)
        self.intermediate_dir.mkdir(parents=True, exist_ok=True)

    def run(self, pairs: List[Tuple[int, str]]):
        """Execute filter pipeline for pending pairs.

        Args:
            pairs: List of (person_id, topic) pairs to process
        """
        if not pairs:
            self.console.print("[yellow]No pairs to filter[/yellow]")
            return

        self.console.print(
            f"[cyan]Starting filter pipeline for {len(pairs)} pairs[/cyan]"
        )

        # Group pairs by person_id to minimize DB queries
        person_topics = defaultdict(set)
        for person_id, topic in pairs:
            person_topics[person_id].add(topic)

        # Process each person
        all_batch_ids = []

        for person_id, topics in person_topics.items():
            batch_ids = self._process_person(person_id, list(topics))
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

    def _process_person(self, person_id: int, topics: List[str]) -> List[str]:
        """Process all speeches for a person against multiple topics.

        Args:
            person_id: The MK's person_id
            topics: List of topics to check

        Returns:
            List of batch job IDs created
        """
        # Load all speeches for this person
        speeches = self.database.get_all_speeches_by_person_id(person_id)

        if not speeches:
            self.console.print(
                f"[yellow]No speeches found for person_id {person_id}[/yellow]"
            )
            return []

        self.console.print(
            f"[cyan]Processing {len(speeches)} speeches for person_id {person_id} against {len(topics)} topics[/cyan]"
        )

        # Build filter prompt with all topics
        filter_prompt = self.config.get_filter_prompt(topics)

        # Create batch requests (one speech checked against all topics)
        requests = []
        for speech in speeches:
            if not speech.text or not speech.text.strip():
                continue

            request = self._build_filter_request(speech, topics, filter_prompt)
            requests.append(request)

        # Split into batches
        batch_ids = []
        batch_size = self.config.BATCH_SIZE

        for i in range(0, len(requests), batch_size):
            batch_requests = requests[i : i + batch_size]

            batch_id = self.batch_manager.create_batch(
                batch_requests,
                metadata={
                    "phase": "filter",
                    "person_id": str(person_id),
                    "topics": ",".join(topics),
                    "batch_index": str(i // batch_size),
                },
            )
            batch_ids.append(batch_id)

        return batch_ids

    def _build_filter_request(
        self, speech: Speech, topics: List[str], filter_prompt: str
    ) -> Dict:
        """Build a single filter request for Batch API.

        Args:
            speech: Speech object
            topics: List of topics to evaluate
            filter_prompt: Filter prompt with topic descriptions

        Returns:
            Batch API request object
        """
        # Format message
        user_message = f"""Speech Text:
{speech.text}

Topics to evaluate:
{', '.join(topics)}

{filter_prompt}

Respond with JSON only, format:
{{
  "topic1": {{"relevance": 1-5}},
  "topic2": {{"relevance": 1-5}},
  ...
}}"""

        return {
            "custom_id": f"speech_{speech.id}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o-mini",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a political speech analyst. Rate speech relevance to topics on a scale of 1-5. Respond with JSON only.",
                    },
                    {"role": "user", "content": user_message},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.3,
            },
        }

    def _process_batch_results(self, batch_id: str):
        """Process completed batch results and save filtered speeches.

        Args:
            batch_id: Completed batch job ID
        """
        self.console.print(f"[cyan]→ Processing results for batch {batch_id}...[/cyan]")

        try:
            results = self.batch_manager.retrieve_results(batch_id)
            self.console.print(f"[cyan]→ Retrieved {len(results)} results[/cyan]")

            # Get metadata
            batch_info = self.batch_manager._batch_jobs.get(batch_id, {})
            person_id = batch_info.get("person_id")
            topics = batch_info.get("topics", "")

            # Convert metadata from strings back to correct types
            if person_id:
                person_id = int(person_id)
            if topics:
                topics = topics.split(",") if isinstance(topics, str) else topics

            if not person_id or not topics:
                self.console.print(f"[red]Missing metadata for batch {batch_id}[/red]")
                return

            # First pass: collect all speech IDs we need
            speech_ids = []
            for result in results:
                if result.get("response", {}).get("status_code") != 200:
                    continue

                custom_id = result.get("custom_id", "")
                if not custom_id.startswith("speech_"):
                    continue

                speech_id = int(custom_id.split("_")[1])
                speech_ids.append(speech_id)

            # Query ONLY the speeches we need by ID
            speech_map = self.database.get_speeches_by_ids(speech_ids)

            # Group all speeches by topic, saving all results regardless of threshold
            all_by_topic = defaultdict(list)

            for result in results:
                if result.get("response", {}).get("status_code") != 200:
                    continue

                # Extract speech_id from custom_id
                custom_id = result.get("custom_id", "")
                if not custom_id.startswith("speech_"):
                    continue

                speech_id = int(custom_id.split("_")[1])

                # Parse response
                try:
                    response_body = result["response"]["body"]
                    content = response_body["choices"][0]["message"]["content"]
                    relevance_scores = json.loads(content)

                    # Get speech text from pre-fetched map
                    speech_text = speech_map.get(speech_id, "")
                    if not speech_text:
                        continue

                    for topic, score_data in relevance_scores.items():
                        relevance = score_data.get("relevance", 0)
                        all_by_topic[topic].append(
                            {
                                "Id": speech_id,
                                "Text": speech_text,
                                "RelevanceScore": relevance,
                            }
                        )

                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    self.console.print(
                        f"[red]Error parsing result for speech {speech_id}: {e}[/red]"
                    )
                    continue

            # Save all speeches to intermediate CSVs (no threshold filtering)
            for topic, speeches in all_by_topic.items():
                self._save_filtered_speeches(person_id, topic, speeches)

            # Mark all topics as filter_complete
            for topic in topics:
                self.job_tracker.mark_filter_complete(person_id, topic, [batch_id])

            self.console.print(
                f"[green]✓ Processed batch {batch_id}: "
                f"{sum(len(s) for s in all_by_topic.values())} speeches saved across {len(all_by_topic)} topics[/green]"
            )

        except Exception as e:
            self.console.print(f"[red]Error processing batch {batch_id}: {e}[/red]")
            import traceback

            traceback.print_exc()
            raise

    def _save_filtered_speeches(self, person_id: int, topic: str, speeches: List[Dict]):
        """Save filtered speeches to intermediate CSV.

        Args:
            person_id: MK person_id
            topic: Topic name
            speeches: List of speech dicts with Id, Text, RelevanceScore
        """
        csv_path = self.intermediate_dir / f"{person_id}_{topic}_filtered.csv"

        # Append mode to support multiple batches
        file_exists = csv_path.exists()

        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["Id", "Text", "RelevanceScore"])

            if not file_exists:
                writer.writeheader()

            writer.writerows(speeches)

        self.console.print(
            f"[green]Saved {len(speeches)} filtered speeches to {csv_path.name}[/green]"
        )
