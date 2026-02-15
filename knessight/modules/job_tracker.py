"""Job tracking for filter and score phases per (person_id, topic) pair."""

import json
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from rich.console import Console
from rich.table import Table


class JobTracker:
    """Tracks completion status of filter and score phases."""

    def __init__(self, status_path: Path = None):
        """Initialize job tracker.

        Args:
            status_path: Path to job_status.json file
        """
        self.console = Console()

        if status_path is None:
            status_path = Path.cwd() / "data" / "cache" / "job_status.json"

        self.status_path = Path(status_path)
        self.status_path.parent.mkdir(parents=True, exist_ok=True)

        self._status: Dict[str, Dict] = {}
        self._load_status()

    def _load_status(self):
        """Load job status from disk."""
        if self.status_path.exists():
            with open(self.status_path, "r", encoding="utf-8") as f:
                self._status = json.load(f)

    def _save_status(self):
        """Save job status to disk."""
        with open(self.status_path, "w", encoding="utf-8") as f:
            json.dump(self._status, f, ensure_ascii=False, indent=2)

    def _make_key(self, person_id: int, topic: str) -> str:
        """Create key for (person_id, topic) pair.

        Args:
            person_id: MK person_id
            topic: Topic name

        Returns:
            Key string
        """
        return f"{person_id}_{topic}"

    def get_pending_pairs(
        self, phase: str, all_pairs: List[Tuple[int, str]]
    ) -> List[Tuple[int, str]]:
        """Get pairs that need processing for a phase.

        Args:
            phase: "filter" or "score"
            all_pairs: All (person_id, topic) pairs from input files

        Returns:
            List of pairs needing processing
        """
        pending = []

        for person_id, topic in all_pairs:
            key = self._make_key(person_id, topic)

            if key not in self._status:
                # New pair - needs processing
                if phase == "filter":
                    pending.append((person_id, topic))
                continue

            status = self._status[key]["status"]

            if phase == "filter" and status == "pending":
                pending.append((person_id, topic))
            elif phase == "score" and status == "filter_complete":
                pending.append((person_id, topic))

        return pending

    def mark_filter_complete(
        self, person_id: int, topic: str, batch_job_ids: List[str]
    ):
        """Mark filter phase as complete for a pair.

        Args:
            person_id: MK person_id
            topic: Topic name
            batch_job_ids: List of batch job IDs used
        """
        key = self._make_key(person_id, topic)

        self._status[key] = {
            "status": "filter_complete",
            "filter_batch_job_ids": batch_job_ids,
            "filter_completed_at": datetime.now().isoformat(),
            "score_batch_job_ids": self._status.get(key, {}).get(
                "score_batch_job_ids", []
            ),
        }

        self._save_status()
        self.console.print(
            f"[green]✓[/green] Filter complete: person_id={person_id}, topic={topic}"
        )

    def mark_score_complete(self, person_id: int, topic: str, batch_job_ids: List[str]):
        """Mark score phase as complete for a pair.

        Args:
            person_id: MK person_id
            topic: Topic name
            batch_job_ids: List of batch job IDs used
        """
        key = self._make_key(person_id, topic)

        if key not in self._status:
            self._status[key] = {"status": "pending"}

        self._status[key]["status"] = "score_complete"
        self._status[key]["score_batch_job_ids"] = batch_job_ids
        self._status[key]["score_completed_at"] = datetime.now().isoformat()

        self._save_status()
        self.console.print(
            f"[green]✓[/green] Score complete: person_id={person_id}, topic={topic}"
        )

    def is_pair_complete(self, person_id: int, topic: str, phase: str) -> bool:
        """Check if a pair is complete for a phase.

        Args:
            person_id: MK person_id
            topic: Topic name
            phase: "filter" or "score"

        Returns:
            True if complete
        """
        key = self._make_key(person_id, topic)

        if key not in self._status:
            return False

        status = self._status[key]["status"]

        if phase == "filter":
            return status in ["filter_complete", "score_complete"]
        elif phase == "score":
            return status == "score_complete"

        return False

    def reset_pairs(self, pairs: List[Tuple[int, str]], phase: Optional[str] = None):
        """Reset pairs to pending status (for --force-reprocess).

        Args:
            pairs: List of (person_id, topic) pairs to reset
            phase: If "filter", reset to pending; if "score", reset to filter_complete
        """
        for person_id, topic in pairs:
            key = self._make_key(person_id, topic)

            if phase == "filter" or phase is None:
                self._status[key] = {"status": "pending"}
            elif phase == "score":
                if (
                    key in self._status
                    and self._status[key]["status"] == "score_complete"
                ):
                    self._status[key]["status"] = "filter_complete"

        self._save_status()
        self.console.print(
            f"[yellow]Reset {len(pairs)} pairs for phase: {phase or 'all'}[/yellow]"
        )

    def get_statistics(self) -> Dict[str, int]:
        """Get statistics on job status.

        Returns:
            Dictionary with counts per status
        """
        stats = {
            "pending": 0,
            "filter_complete": 0,
            "score_complete": 0,
            "total": len(self._status),
        }

        for key, data in self._status.items():
            status = data["status"]
            if status in stats:
                stats[status] += 1

        return stats

    def print_status(self):
        """Print current job status as a table."""
        stats = self.get_statistics()

        table = Table(
            title="Job Status Summary", show_header=True, header_style="bold magenta"
        )
        table.add_column("Status", style="cyan")
        table.add_column("Count", justify="right", style="green")
        table.add_column("Percentage", justify="right")

        total = stats["total"]
        if total > 0:
            for status in ["pending", "filter_complete", "score_complete"]:
                count = stats[status]
                percentage = (count / total * 100) if total > 0 else 0
                table.add_row(status, str(count), f"{percentage:.1f}%")

            table.add_row("", "", "", style="dim")
            table.add_row("Total", str(total), "100.0%", style="bold")
        else:
            table.add_row("No jobs tracked yet", "0", "0%")

        self.console.print(table)
