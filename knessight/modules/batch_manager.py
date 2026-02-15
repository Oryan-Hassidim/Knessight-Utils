"""OpenAI Batch API manager with cost tracking."""

import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import os

from openai import OpenAI
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)


class BatchManager:
    """Manages OpenAI Batch API operations."""

    def __init__(self, cache_dir: Path = None, logs_dir: Path = None):
        """Initialize batch manager.

        Args:
            cache_dir: Directory for batch_jobs.json
            logs_dir: Directory for costs.json and logs
        """
        self.console = Console()
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        if cache_dir is None:
            cache_dir = Path.cwd() / "data" / "cache"
        if logs_dir is None:
            logs_dir = Path.cwd() / "data" / "logs"

        self.cache_dir = Path(cache_dir)
        self.logs_dir = Path(logs_dir)

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        self.batch_jobs_path = self.cache_dir / "batch_jobs.json"
        self.costs_path = self.logs_dir / "costs.json"
        self.failed_speeches_path = self.cache_dir / "failed_speeches.json"

        self._batch_jobs: Dict[str, Dict] = {}
        self._costs: Dict[str, Dict] = {}
        self._failed_speeches: List[Dict] = []

        self._load_data()

    def _load_data(self):
        """Load batch jobs and costs from disk."""
        if self.batch_jobs_path.exists():
            with open(self.batch_jobs_path, "r", encoding="utf-8") as f:
                self._batch_jobs = json.load(f)

        if self.costs_path.exists():
            with open(self.costs_path, "r", encoding="utf-8") as f:
                self._costs = json.load(f)

        if self.failed_speeches_path.exists():
            with open(self.failed_speeches_path, "r", encoding="utf-8") as f:
                self._failed_speeches = json.load(f)

    def _save_batch_jobs(self):
        """Save batch jobs to disk."""
        with open(self.batch_jobs_path, "w", encoding="utf-8") as f:
            json.dump(self._batch_jobs, f, ensure_ascii=False, indent=2)

    def _save_costs(self):
        """Save costs to disk."""
        with open(self.costs_path, "w", encoding="utf-8") as f:
            json.dump(self._costs, f, ensure_ascii=False, indent=2)

    def _save_failed_speeches(self):
        """Save failed speeches to disk."""
        with open(self.failed_speeches_path, "w", encoding="utf-8") as f:
            json.dump(self._failed_speeches, f, ensure_ascii=False, indent=2)

    def create_batch(
        self, requests: List[Dict[str, Any]], metadata: Dict[str, Any]
    ) -> str:
        """Create and submit a batch job.

        Args:
            requests: List of API requests (OpenAI Batch API format)
            metadata: Metadata about the batch (phase, person_id, topic, etc.)

        Returns:
            Batch job ID
        """
        # Create JSONL file for batch
        batch_file_path = (
            self.cache_dir / f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        )
        with open(batch_file_path, "w", encoding="utf-8") as f:
            for req in requests:
                f.write(json.dumps(req, ensure_ascii=False) + "\n")

        # Upload file
        with open(batch_file_path, "rb") as f:
            file_response = self.client.files.create(file=f, purpose="batch")

        # Create batch
        batch = self.client.batches.create(
            input_file_id=file_response.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata=metadata,
        )

        batch_id = batch.id

        # Track batch
        self._batch_jobs[batch_id] = {
            **metadata,
            "submitted_at": datetime.now().isoformat(),
            "status": "submitted",
            "request_count": len(requests),
            "file_id": file_response.id,
        }
        self._save_batch_jobs()

        self.console.print(
            f"[cyan]→[/cyan] Created batch job: {batch_id} ({len(requests)} requests)"
        )

        return batch_id

    def poll_batches(self, batch_ids: List[str], interval: int = 30) -> Dict[str, str]:
        """Poll batch jobs until completion with detailed progress.

        Args:
            batch_ids: List of batch job IDs to poll
            interval: Polling interval in seconds (default: 30)

        Returns:
            Dictionary mapping batch_id to final status
        """
        # Use shorter interval for progress updates
        update_interval = min(5, interval)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            console=self.console,
        ) as progress:

            # Track each batch separately
            batch_tasks = {}
            for batch_id in batch_ids:
                batch_info = self._batch_jobs.get(batch_id, {})
                request_count = batch_info.get("request_count", 0)
                task = progress.add_task(
                    f"[cyan]Batch {batch_id[:8]}...", total=request_count, completed=0
                )
                batch_tasks[batch_id] = task

            completed = {}
            remaining = list(batch_ids)
            last_full_poll = 0

            while remaining:
                current_time = time.time()

                # Do full API poll at specified interval
                should_full_poll = (current_time - last_full_poll) >= interval

                for batch_id in list(remaining):
                    try:
                        batch = self.client.batches.retrieve(batch_id)
                        status = batch.status

                        # Update request counts
                        counts = batch.request_counts
                        task_id = batch_tasks[batch_id]

                        # Update progress bar with actual completion
                        progress.update(
                            task_id,
                            completed=counts.completed,
                            total=counts.total,
                            description=f"[cyan]Batch {batch_id[:8]}... ({counts.completed}/{counts.total} complete, {counts.failed} failed)",
                        )

                        # Update tracking
                        if batch_id in self._batch_jobs:
                            self._batch_jobs[batch_id]["status"] = status

                        if status in ["completed", "failed", "expired", "cancelled"]:
                            completed[batch_id] = status
                            remaining.remove(batch_id)

                            if batch_id in self._batch_jobs:
                                self._batch_jobs[batch_id][
                                    "completed_at"
                                ] = datetime.now().isoformat()

                            # Track costs if completed
                            if status == "completed":
                                self._track_batch_cost(batch_id, batch)

                            color = "green" if status == "completed" else "red"
                            progress.update(
                                task_id,
                                description=f"[{color}]Batch {batch_id[:8]}... {status.upper()}",
                            )

                    except Exception as e:
                        self.console.print(
                            f"[red]Error polling batch {batch_id}: {e}[/red]"
                        )
                        completed[batch_id] = "error"
                        remaining.remove(batch_id)

                if should_full_poll:
                    self._save_batch_jobs()
                    last_full_poll = current_time

                if remaining:
                    time.sleep(update_interval)

        return completed

    def _track_batch_cost(self, batch_id: str, batch):
        """Track token usage and cost for a batch.

        Args:
            batch_id: Batch job ID
            batch: Batch object from API
        """
        usage = batch.request_counts

        self._costs[batch_id] = {
            "timestamp": datetime.now().isoformat(),
            "requests": {
                "total": usage.total,
                "completed": usage.completed,
                "failed": usage.failed,
            },
            "metadata": self._batch_jobs.get(batch_id, {}),
        }

        self._save_costs()

    def retrieve_results(self, batch_id: str) -> List[Dict[str, Any]]:
        """Retrieve results from a completed batch.

        Args:
            batch_id: Batch job ID

        Returns:
            List of response objects
        """
        batch = self.client.batches.retrieve(batch_id)

        if batch.status != "completed":
            raise ValueError(
                f"Batch {batch_id} not completed yet (status: {batch.status})"
            )

        if not batch.output_file_id:
            raise ValueError(f"Batch {batch_id} has no output file")

        # Download results
        file_response = self.client.files.content(batch.output_file_id)
        content = file_response.read().decode("utf-8")

        # Parse JSONL
        results = []
        for line in content.strip().split("\n"):
            if line:
                results.append(json.loads(line))

        return results

    def retry_failed_requests(
        self, failed_requests: List[Dict], metadata: Dict, max_attempts: int = 3
    ) -> Optional[str]:
        """Retry failed requests.

        Args:
            failed_requests: List of failed request objects
            metadata: Metadata for the batch
            max_attempts: Maximum retry attempts

        Returns:
            New batch_id if retried, None if giving up
        """
        attempt = metadata.get("retry_attempt", 0)

        if attempt >= max_attempts:
            self.console.print(
                f"[red]Giving up on {len(failed_requests)} requests after {max_attempts} attempts[/red]"
            )

            # Log to failed speeches
            for req in failed_requests:
                self._failed_speeches.append(
                    {
                        "request": req,
                        "metadata": metadata,
                        "timestamp": datetime.now().isoformat(),
                    }
                )
            self._save_failed_speeches()

            return None

        # Create retry batch
        metadata["retry_attempt"] = attempt + 1
        self.console.print(
            f"[yellow]Retrying {len(failed_requests)} failed requests (attempt {attempt + 1})[/yellow]"
        )

        return self.create_batch(failed_requests, metadata)
