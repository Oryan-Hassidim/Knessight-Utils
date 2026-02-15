"""MK name disambiguation with persistent caching."""

import json
from pathlib import Path
from typing import Dict, List, Optional
from fuzzywuzzy import fuzz
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt

from .database import Database


class Disambiguation:
    """Handles MK name resolution with fuzzy matching and caching."""

    def __init__(self, database: Database, cache_path: Path = None):
        """Initialize disambiguation manager.

        Args:
            database: Database instance for querying people
            cache_path: Path to resolution cache JSON file
        """
        self.database = database
        self.console = Console()

        if cache_path is None:
            cache_path = Path.cwd() / "data" / "cache" / "mk_resolution_cache.json"

        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)

        self._cache: Dict[str, int] = {}
        self._load_cache()

    def _load_cache(self):
        """Load resolution cache from disk."""
        if self.cache_path.exists():
            with open(self.cache_path, "r", encoding="utf-8") as f:
                self._cache = json.load(f)

    def _save_cache(self):
        """Save resolution cache to disk."""
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, ensure_ascii=False, indent=2)

    def resolve_mk_names(self, names: List[str]) -> Dict[str, int]:
        """Resolve list of MK names to person_ids.

        Args:
            names: List of MK names from input file

        Returns:
            Dictionary mapping input name to person_id
        """
        resolved = {}

        for name in names:
            name = name.strip()
            if not name:
                continue

            # Check cache first
            if name in self._cache:
                resolved[name] = self._cache[name]
                self.console.print(
                    f"[green]✓[/green] {name} → person_id {self._cache[name]} (cached)"
                )
                continue

            # Attempt resolution
            person_id = self._resolve_single_name(name)
            if person_id:
                resolved[name] = person_id
                self._cache[name] = person_id
                self._save_cache()

        return resolved

    def _resolve_single_name(self, name: str) -> Optional[int]:
        """Resolve a single MK name.

        Args:
            name: MK name to resolve

        Returns:
            person_id if resolved, None otherwise
        """
        # Search database
        candidates = self.database.search_people_by_name(name)

        if not candidates:
            self.console.print(f"[red]✗[/red] No matches found for '{name}'")
            return None

        # If exact match or single result, use it
        if len(candidates) == 1:
            person_id = candidates[0]["person_id"]
            self.console.print(
                f"[green]✓[/green] {name} → {candidates[0]['name']} (person_id {person_id})"
            )
            return person_id

        # Use fuzzy matching to find best matches
        scored_candidates = []
        for candidate in candidates:
            full_name = candidate["name"]
            # Calculate similarity scores
            ratio = fuzz.ratio(name.lower(), full_name.lower())
            partial = fuzz.partial_ratio(name.lower(), full_name.lower())
            token_sort = fuzz.token_sort_ratio(name.lower(), full_name.lower())

            # Use best score
            score = max(ratio, partial, token_sort)
            scored_candidates.append((score, candidate))

        # Sort by score
        scored_candidates.sort(reverse=True, key=lambda x: x[0])

        # If top match is significantly better (score >= 90), auto-select
        if scored_candidates[0][0] >= 90:
            best_match = scored_candidates[0][1]
            person_id = best_match["person_id"]
            self.console.print(
                f"[green]✓[/green] {name} → {best_match['name']} (person_id {person_id}, score: {scored_candidates[0][0]})"
            )
            return person_id

        # Multiple ambiguous matches - ask user
        return self._interactive_disambiguation(name, scored_candidates)

    def _interactive_disambiguation(
        self, input_name: str, scored_candidates: List[tuple]
    ) -> Optional[int]:
        """Interactively ask user to select correct MK.

        Args:
            input_name: Original input name
            scored_candidates: List of (score, candidate_dict) tuples

        Returns:
            Selected person_id or None if skipped
        """
        self.console.print(
            f"\n[yellow]Multiple matches found for '{input_name}':[/yellow]"
        )

        # Create table
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("#", style="dim", width=4)
        table.add_column("Name", min_width=20)
        table.add_column("Faction/Party", min_width=20)
        table.add_column("person_id", justify="right")
        table.add_column("Score", justify="right")

        for idx, (score, candidate) in enumerate(
            scored_candidates[:10], 1
        ):  # Show top 10
            table.add_row(
                str(idx),
                candidate["name"],
                candidate.get("faction") or candidate.get("party_name") or "—",
                str(candidate["person_id"]),
                f"{score}%",
            )

        self.console.print(table)

        # Prompt for selection
        choice = Prompt.ask(
            "\nSelect number (or 's' to skip)",
            choices=[str(i) for i in range(1, min(11, len(scored_candidates) + 1))]
            + ["s"],
            default="1",
        )

        if choice.lower() == "s":
            self.console.print("[yellow]Skipped[/yellow]")
            return None

        selected = scored_candidates[int(choice) - 1][1]
        person_id = selected["person_id"]
        self.console.print(
            f"[green]✓[/green] Selected: {selected['name']} (person_id {person_id})"
        )

        return person_id

    def load_mk_list_from_file(self, file_path: Path) -> List[str]:
        """Load MK names from input file.

        Args:
            file_path: Path to mks.txt

        Returns:
            List of MK names
        """
        if not file_path.exists():
            raise FileNotFoundError(f"MK list file not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            names = [
                line.strip()
                for line in f
                if line.strip() and not line.strip().startswith("#")
            ]

        return names
