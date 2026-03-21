"""Output aggregation and JSON generation."""

import csv
import json
from pathlib import Path
from typing import List, Dict
from statistics import mean
from datetime import datetime

from rich.console import Console

from .database import Database
from .config import Config


class OutputManager:
    """Manages output aggregation to JSON files."""

    def __init__(self, database: Database, client_data_dir: Path = None, config: Config = None):
        """Initialize output manager.

        Args:
            database: Database instance
            client_data_dir: Directory for client_data outputs
            config: Configuration instance (optional)
        """
        self.database = database
        self.console = Console()
        self.config = config or Config()

        if client_data_dir is None:
            client_data_dir = Path.cwd() / "data" / "client_data"

        self.client_data_dir = Path(client_data_dir)
        self.mk_data_dir = self.client_data_dir / "mk_data"
        self.topics_dir = self.client_data_dir / "topics"
        self.mks_csv_path = self.client_data_dir / "mks.csv"
        self._mks_metadata = self._load_mks_metadata_from_csv()

        self.mk_data_dir.mkdir(parents=True, exist_ok=True)
        self.topics_dir.mkdir(parents=True, exist_ok=True)

    def _load_mks_metadata_from_csv(self) -> Dict[int, Dict[str, str]]:
        """Load MK metadata from client_data/mks.csv.

        Returns:
            Mapping of person_id to metadata fields from CSV.
        """
        metadata_by_id: Dict[int, Dict[str, str]] = {}

        if not self.mks_csv_path.exists():
            self.console.print(
                f"[yellow]mks.csv not found at {self.mks_csv_path}; continuing without CSV metadata[/yellow]"
            )
            return metadata_by_id

        try:
            with open(self.mks_csv_path, "r", newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    id_value = (row.get("id") or row.get("knesset site id") or "").strip()
                    if not id_value:
                        continue

                    try:
                        person_id = int(id_value)
                    except ValueError:
                        continue

                    first_name = (row.get("first name") or "").strip()
                    last_name = (row.get("last name") or "").strip()
                    csv_name = " ".join(part for part in [first_name, last_name] if part)

                    metadata_by_id[person_id] = {
                        "name": csv_name,
                        "image_url": (row.get("image url") or "").strip(),
                    }

        except Exception as e:
            self.console.print(
                f"[yellow]Warning: Could not read mks.csv at {self.mks_csv_path}: {e}[/yellow]"
            )

        return metadata_by_id

    def update_aggregations(
        self, person_id: int, topic: str, scored_speeches: List[Dict]
    ):
        """Update MK and topic aggregation JSONs after scoring.

        Args:
            person_id: MK person_id
            topic: Topic name
            scored_speeches: List of scored speech dicts with Rank and Date
        """
        if not scored_speeches:
            return

        # Calculate statistics
        count = len(scored_speeches)
        ranks = [speech["Rank"] for speech in scored_speeches]
        average = mean(ranks)
        weighted_average = self._calculate_weighted_average(scored_speeches)

        # Update MK main.json
        self._update_mk_json(person_id, topic, count, average, weighted_average)

        # Update topic aggregation JSON
        self._update_topic_json(topic, person_id, count, average, weighted_average)

        self.console.print(
            f"[green]Updated aggregations for person_id={person_id}, topic={topic} "
            f"(count={count}, avg={average:.2f}, weighted_avg={weighted_average:.2f})[/green]"
        )

    def _calculate_weighted_average(self, scored_speeches: List[Dict]) -> float:
        """Calculate time-weighted average using exponential decay.

        Args:
            scored_speeches: List of scored speech dicts with Rank and Date

        Returns:
            Time-weighted average score (newer speeches have more influence)
        """
        if not scored_speeches:
            return 0.0

        # Find most recent speech date for reference
        dates = []
        for speech in scored_speeches:
            try:
                date_str = speech.get("Date", "")
                if date_str:
                    dates.append(datetime.strptime(date_str, "%Y-%m-%d"))
            except (ValueError, TypeError):
                continue

        if not dates:
            # No valid dates, fall back to simple average
            return mean([speech["Rank"] for speech in scored_speeches])

        reference_date = max(dates)  # Most recent speech
        half_life_years = self.config.TIME_WEIGHT_HALF_LIFE_YEARS

        # Calculate weighted average
        weighted_sum = 0.0
        total_weight = 0.0

        for speech in scored_speeches:
            try:
                date_str = speech.get("Date", "")
                if not date_str:
                    continue

                speech_date = datetime.strptime(date_str, "%Y-%m-%d")
                years_ago = (reference_date - speech_date).days / 365.25

                # Exponential decay: weight = 0.5^(years_ago / half_life)
                weight = 0.5 ** (years_ago / half_life_years)

                weighted_sum += speech["Rank"] * weight
                total_weight += weight

            except (ValueError, TypeError, KeyError):
                continue

        if total_weight == 0:
            # Fallback to simple average
            return mean([speech["Rank"] for speech in scored_speeches])

        return weighted_sum / total_weight

    def _update_mk_json(self, person_id: int, topic: str, count: int, average: float, weighted_average: float):
        """Update or create MK's main.json.

        Args:
            person_id: MK person_id
            topic: Topic name
            count: Number of speeches
            average: Average stance score
            weighted_average: Time-weighted average stance score
        """
        mk_dir = self.mk_data_dir / str(person_id)
        mk_dir.mkdir(parents=True, exist_ok=True)

        json_path = mk_dir / "main.json"

        # Load existing or create new
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            # Get MK metadata from database
            metadata = self.database.get_person_metadata(person_id)
            if not metadata:
                self.console.print(
                    f"[red]No metadata found for person_id {person_id}[/red]"
                )
                return

            csv_metadata = self._mks_metadata.get(person_id, {})
            name = csv_metadata.get("name") or metadata["name"]
            image_url = csv_metadata.get("image_url", "")

            data = {
                "id": person_id,
                "knessetSiteId": person_id,
                "name": name,
                "imageUrl": image_url,
                "description": f"Faction: {metadata.get('faction', 'N/A')}, Party: {metadata.get('party_name', 'N/A')}",
                "Topics": [],
            }

        # Update or add topic
        topic_found = False
        for topic_data in data["Topics"]:
            if topic_data["topicName"] == topic:
                topic_data["count"] = count
                topic_data["average"] = round(average, 2)
                topic_data["weighted_average"] = round(weighted_average, 2)
                topic_found = True
                break

        if not topic_found:
            data["Topics"].append(
                {
                    "topicName": topic,
                    "count": count,
                    "average": round(average, 2),
                    "weighted_average": round(weighted_average, 2),
                }
            )

        # Save
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _update_topic_json(
        self, topic: str, person_id: int, count: int, average: float, weighted_average: float
    ):
        """Update or create topic aggregation JSON.

        Args:
            topic: Topic name
            person_id: MK person_id
            count: Number of speeches
            average: Average stance score
            weighted_average: Time-weighted average stance score
        """
        json_path = self.topics_dir / f"{topic}.json"

        # Load existing or create new
        if json_path.exists():
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = {}

        # Update or add MK - Format: [count, average, weighted_average]
        data[str(person_id)] = [count, round(average, 2), round(weighted_average, 2)]

        # Save
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def export_all_scores_csv(self):
        """Export all MK scores to a consolidated CSV.

        Creates CSV with columns: mk_id, topic, average, weighted_average
        by scanning all mk_data/*/main.json files.
        """
        csv_path = self.client_data_dir / "all_mk_scores.csv"
        rows = []

        # Scan all MK directories
        if not self.mk_data_dir.exists():
            self.console.print("[yellow]No mk_data directory found[/yellow]")
            return

        for mk_dir in sorted(self.mk_data_dir.iterdir()):
            if not mk_dir.is_dir():
                continue

            main_json = mk_dir / "main.json"
            if not main_json.exists():
                continue

            try:
                with open(main_json, "r", encoding="utf-8") as f:
                    data = json.load(f)

                mk_id = data.get("id")
                topics = data.get("Topics", [])

                for topic_data in topics:
                    rows.append(
                        {
                            "mk_id": mk_id,
                            "topic": topic_data["topicName"],
                            "average": topic_data.get("average", 0),
                            "weighted_average": topic_data.get("weighted_average", 0),
                        }
                    )

            except (json.JSONDecodeError, KeyError) as e:
                self.console.print(
                    f"[yellow]Warning: Could not parse {main_json}: {e}[/yellow]"
                )
                continue

        # Sort by mk_id, then topic
        rows.sort(key=lambda x: (x["mk_id"], x["topic"]))

        # Write CSV
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            if rows:
                writer = csv.DictWriter(
                    f, fieldnames=["mk_id", "topic", "average", "weighted_average"]
                )
                writer.writeheader()
                writer.writerows(rows)

        self.console.print(
            f"[green]Exported {len(rows)} MK-topic scores to {csv_path}[/green]"
        )
