"""Validate configuration before running pipeline."""

from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

from src.modules.config import Config
from src.modules.database import Database
from src.modules.disambiguation import Disambiguation


def validate_configuration():
    """Check all configuration is correct."""
    print("=" * 60)
    print("CONFIGURATION VALIDATION")
    print("=" * 60)

    errors = []
    warnings = []

    # Check API key
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or api_key == "sk-your-api-key-here":
        errors.append("❌ OPENAI_API_KEY not set or using default value")
    else:
        print(f"✓ API Key configured: {api_key[:15]}...")

    # Check config
    config = Config()
    print(f"✓ Filter Model: {config.FILTER_MODEL_NAME}")
    print(f"✓ Score Model: {config.SCORE_MODEL_NAME}")
    print(f"✓ Database: {config.DATABASE_PATH}")

    # Validate config
    config_errors = config.validate()
    if config_errors:
        errors.extend([f"❌ Config: {e}" for e in config_errors])
    else:
        print("✓ Config validation passed")

    # Check database
    try:
        database = Database(Path(config.DATABASE_PATH))
        print(f"✓ Database connected")
    except Exception as e:
        errors.append(f"❌ Database error: {e}")
        return errors, warnings

    # Check MKs
    input_dir = Path.cwd() / "input"
    mks_file = input_dir / "mks.txt"

    if not mks_file.exists():
        errors.append(f"❌ MKs file not found: {mks_file}")
    else:
        disambiguation = Disambiguation(database)
        mk_names = disambiguation.load_mk_list_from_file(mks_file)
        print(f"✓ Loaded {len(mk_names)} MK names")

        # Resolve names
        print("\nResolving MK names:")
        resolved = disambiguation.resolve_mk_names(mk_names)
        print(f"✓ Resolved {len(resolved)}/{len(mk_names)} MKs")

        if len(resolved) < len(mk_names):
            warnings.append(
                f"⚠ {len(mk_names) - len(resolved)} MKs could not be resolved"
            )

        # Check speech counts
        print("\nSpeech counts:")
        for name, person_id in resolved.items():
            speeches = database.get_all_speeches_by_person_id(person_id)
            print(f"  {name} (ID {person_id}): {len(speeches)} speeches")

    # Check topics
    topics_file = input_dir / "topics.txt"
    if not topics_file.exists():
        errors.append(f"❌ Topics file not found: {topics_file}")
    else:
        with open(topics_file, "r", encoding="utf-8") as f:
            topics = [
                line.strip()
                for line in f
                if line.strip() and not line.strip().startswith("#")
            ]
        print(f"\n✓ Loaded {len(topics)} topics: {', '.join(topics)}")

        # Check topic descriptions
        try:
            descriptions = config.load_topic_descriptions()
            for topic in topics:
                if topic not in descriptions:
                    errors.append(
                        f"❌ Topic '{topic}' missing from topic_descriptions.yaml"
                    )
                else:
                    print(f"  ✓ {topic}: {descriptions[topic][:50]}...")
        except Exception as e:
            errors.append(f"❌ Error loading topic descriptions: {e}")

        # Check scoring prompts
        scoring_prompts_dir = config.config_dir / "scoring_prompts"
        for topic in topics:
            prompt_file = scoring_prompts_dir / f"{topic}.txt"
            if not prompt_file.exists():
                errors.append(f"❌ Missing scoring prompt: {prompt_file}")
            else:
                print(f"  ✓ Scoring prompt for {topic} exists")

    print("\n" + "=" * 60)

    if errors:
        print("❌ VALIDATION FAILED")
        for error in errors:
            print(error)
    elif warnings:
        print("⚠ VALIDATION PASSED WITH WARNINGS")
        for warning in warnings:
            print(warning)
    else:
        print("✅ VALIDATION PASSED - READY TO RUN")

    print("=" * 60)

    # Calculate expected pairs
    if not errors:
        print(
            f"\nExpected pairs to process: {len(resolved)} MKs × {len(topics)} topics = {len(resolved) * len(topics)} pairs"
        )

    return errors, warnings


if __name__ == "__main__":
    errors, warnings = validate_configuration()
    if errors:
        exit(1)
