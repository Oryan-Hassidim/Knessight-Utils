"""Command-line interface for Knessight pipeline."""

from pathlib import Path
from typing import Optional
import typer
from rich.console import Console
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from .modules.config import Config
from .modules.database import Database
from .modules.disambiguation import Disambiguation
from .modules.job_tracker import JobTracker
from .modules.batch_manager import BatchManager
from .modules.filter_pipeline import FilterPipeline
from .modules.score_pipeline import ScorePipeline
from .modules.output import OutputManager


app = typer.Typer(
    name="knessight",
    help="Knessight Backend - Knesset Speech Analysis Pipeline",
    add_completion=False,
)

console = Console()


def load_topics_from_file(file_path: Path) -> list[str]:
    """Load topics from input file.

    Args:
        file_path: Path to topics.txt

    Returns:
        List of topic names
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Topics file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        topics = [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]

    return topics


@app.command()
def filter(
    db_path: str = typer.Option(
        None, help="Path to SQLite database (overrides config)"
    ),
    force_reprocess: bool = typer.Option(False, help="Force reprocess completed pairs"),
):
    """Run Phase 1: Filter speeches for relevance to topics."""
    console.print("[bold cyan]Knessight Filter Pipeline[/bold cyan]\n")

    try:
        # Initialize components
        config = Config()
        # Use CLI arg if provided, otherwise use config
        db_path = db_path or config.DATABASE_PATH
        database = Database(Path(db_path))
        disambiguation = Disambiguation(database)
        job_tracker = JobTracker()
        batch_manager = BatchManager()

        # Set paths from config
        intermediate_dir = Path.cwd() / "data" / "intermediate"

        # Validate config
        errors = config.validate()
        if errors:
            console.print("[red]Configuration errors:[/red]")
            for error in errors:
                console.print(f"  - {error}")
            raise typer.Exit(1)

        # Load MKs and resolve names
        input_dir = Path.cwd() / "data" / "input"
        mks_file = input_dir / "mks.txt"
        topics_file = input_dir / "topics.txt"

        if not mks_file.exists():
            console.print(f"[red]MKs file not found: {mks_file}[/red]")
            raise typer.Exit(1)

        if not topics_file.exists():
            console.print(f"[red]Topics file not found: {topics_file}[/red]")
            raise typer.Exit(1)

        # Load and resolve MKs
        console.print("[cyan]Loading and resolving MK names...[/cyan]")
        mk_names = disambiguation.load_mk_list_from_file(mks_file)
        resolved_mks = disambiguation.resolve_mk_names(mk_names)

        # Load topics
        topics = load_topics_from_file(topics_file)
        console.print(f"\n[cyan]Loaded {len(topics)} topics[/cyan]")

        # Generate all pairs
        all_pairs = [
            (person_id, topic)
            for person_id in resolved_mks.values()
            for topic in topics
        ]

        # Get pending pairs
        if force_reprocess:
            job_tracker.reset_pairs(all_pairs, phase="filter")

        pending_pairs = job_tracker.get_pending_pairs("filter", all_pairs)

        console.print(
            f"\n[cyan]Pending filter pairs: {len(pending_pairs)} / {len(all_pairs)}[/cyan]"
        )

        if not pending_pairs:
            console.print("[green]All pairs already filtered![/green]")
            return

        # Run filter pipeline
        filter_pipeline = FilterPipeline(
            config,
            database,
            batch_manager,
            job_tracker,
            intermediate_dir=intermediate_dir,
        )
        filter_pipeline.run(pending_pairs)

        console.print("\n[bold green]Filter pipeline complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)


@app.command()
def score(
    db_path: str = typer.Option(
        None, help="Path to SQLite database (overrides config)"
    ),
    reasoning_rate: float = typer.Option(
        0.1, help="Probability of requesting reasoning (0.0-1.0)"
    ),
    force_reprocess: bool = typer.Option(False, help="Force reprocess completed pairs"),
):
    """Run Phase 2: Score stance on topics for filtered speeches."""
    console.print("[bold cyan]Knessight Score Pipeline[/bold cyan]\n")

    try:
        # Initialize components
        config = Config()
        # Use CLI arg if provided, otherwise use config
        db_path = db_path or config.DATABASE_PATH
        database = Database(Path(db_path))
        job_tracker = JobTracker()
        batch_manager = BatchManager()
        output_manager = OutputManager(
            database, client_data_dir=Path(config.CLIENT_DATA_PATH)
        )

        # Load topics and MKs
        input_dir = Path.cwd() / "data" / "input"
        mks_file = input_dir / "mks.txt"
        topics_file = input_dir / "topics.txt"

        disambiguation = Disambiguation(database)
        mk_names = disambiguation.load_mk_list_from_file(mks_file)
        resolved_mks = disambiguation.resolve_mk_names(mk_names)
        topics = load_topics_from_file(topics_file)

        # Generate all pairs
        all_pairs = [
            (person_id, topic)
            for person_id in resolved_mks.values()
            for topic in topics
        ]

        # Get pending pairs for scoring (filter_complete)
        if force_reprocess:
            job_tracker.reset_pairs(all_pairs, phase="score")

        pending_pairs = job_tracker.get_pending_pairs("score", all_pairs)

        console.print(
            f"\n[cyan]Pending score pairs: {len(pending_pairs)} / {len(all_pairs)}[/cyan]"
        )

        if not pending_pairs:
            console.print("[green]All pairs already scored![/green]")
            return

        # Run score pipeline
        score_pipeline = ScorePipeline(
            config,
            database,
            batch_manager,
            job_tracker,
            output_manager,
            client_data_dir=Path(config.CLIENT_DATA_PATH),
        )
        score_pipeline.run(pending_pairs, reasoning_rate)

        # Generate mks.csv
        output_manager.generate_mks_csv(list(resolved_mks.values()))

        console.print("\n[bold green]Score pipeline complete![/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)


@app.command()
def both(
    db_path: str = typer.Option(
        None, help="Path to SQLite database (overrides config)"
    ),
    reasoning_rate: float = typer.Option(
        0.1, help="Probability of requesting reasoning (0.0-1.0)"
    ),
    force_reprocess: bool = typer.Option(False, help="Force reprocess all pairs"),
):
    """Run both Filter and Score pipelines sequentially."""
    console.print("[bold cyan]Knessight Full Pipeline[/bold cyan]\n")

    # Run filter
    console.print("[bold]Step 1: Filter Pipeline[/bold]")
    filter(db_path, force_reprocess)

    console.print("\n" + "=" * 60 + "\n")

    # Run score
    console.print("[bold]Step 2: Score Pipeline[/bold]")
    score(reasoning_rate, force_reprocess)

    console.print("\n[bold green]✓ Full pipeline complete![/bold green]")


@app.command()
def status():
    """Display current job status and statistics."""
    console.print("[bold cyan]Knessight Job Status[/bold cyan]\n")

    try:
        job_tracker = JobTracker()
        job_tracker.print_status()

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)


@app.command()
def cleanup_intermediate(
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt")
):
    """Clean up intermediate CSV files for score_complete pairs."""
    console.print("[bold cyan]Cleanup Intermediate Files[/bold cyan]\n")

    try:
        job_tracker = JobTracker()
        intermediate_dir = Path.cwd() / "data" / "intermediate"

        if not intermediate_dir.exists():
            console.print("[yellow]No intermediate directory found[/yellow]")
            return

        # Find score_complete pairs
        completed = []
        for key, data in job_tracker._status.items():
            if data["status"] == "score_complete":
                parts = key.split("_", 1)
                if len(parts) == 2:
                    person_id, topic = parts[0], parts[1]
                    csv_path = intermediate_dir / f"{person_id}_{topic}_filtered.csv"
                    if csv_path.exists():
                        completed.append(csv_path)

        if not completed:
            console.print("[green]No intermediate files to clean up[/green]")
            return

        console.print(
            f"[yellow]Found {len(completed)} intermediate files to remove[/yellow]"
        )

        if not confirm:
            response = typer.confirm("Are you sure you want to delete these files?")
            if not response:
                console.print("[yellow]Cancelled[/yellow]")
                return

        # Delete files
        for csv_path in completed:
            csv_path.unlink()

        console.print(f"[green]✓ Deleted {len(completed)} intermediate files[/green]")

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
