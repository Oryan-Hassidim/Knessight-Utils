"""Configuration management for prompts and settings."""

import os
from pathlib import Path
from typing import Dict, List
import yaml


class Config:
    """Manages loading and access to configuration and prompts."""

    # Default settings
    DATABASE_PATH = "knesset.db"
    CLIENT_DATA_PATH = "data/client_data"
    FILTER_MODEL_NAME = "gpt-4o-mini"
    SCORE_MODEL_NAME = "gpt-4o"
    REASONING_SAMPLE_RATE = 0.1
    BATCH_SIZE = 10000
    BATCH_POLL_INTERVAL = 30
    RELEVANCE_THRESHOLD = 4
    RETRY_ATTEMPTS = 3

    def __init__(self, config_dir: Path = None):
        """Initialize config manager.

        Args:
            config_dir: Path to config directory (defaults to ./config)
        """
        if config_dir is None:
            config_dir = Path.cwd() / "data" / "config"

        self.config_dir = Path(config_dir)
        self._filter_prompt: str = None
        self._topic_descriptions: Dict[str, str] = None
        self._scoring_prompts: Dict[str, str] = {}

        # Override settings from environment variables
        self.DATABASE_PATH = os.getenv("DATABASE_PATH", self.DATABASE_PATH)
        self.CLIENT_DATA_PATH = os.getenv("CLIENT_DATA_PATH", self.CLIENT_DATA_PATH)
        self.FILTER_MODEL_NAME = os.getenv("FILTER_MODEL_NAME", self.FILTER_MODEL_NAME)
        self.SCORE_MODEL_NAME = os.getenv("SCORE_MODEL_NAME", self.SCORE_MODEL_NAME)
        self.REASONING_SAMPLE_RATE = float(
            os.getenv("REASONING_SAMPLE_RATE", self.REASONING_SAMPLE_RATE)
        )
        self.BATCH_SIZE = int(os.getenv("BATCH_SIZE", self.BATCH_SIZE))
        self.BATCH_POLL_INTERVAL = int(
            os.getenv("BATCH_POLL_INTERVAL", self.BATCH_POLL_INTERVAL)
        )
        self.RELEVANCE_THRESHOLD = int(
            os.getenv("RELEVANCE_THRESHOLD", self.RELEVANCE_THRESHOLD)
        )
        self.RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", self.RETRY_ATTEMPTS))

    def load_filter_prompt(self) -> str:
        """Load the general filter prompt template.

        Returns:
            Filter prompt template with {topic_descriptions} placeholder
        """
        if self._filter_prompt is None:
            prompt_path = self.config_dir / "filter_prompt.txt"
            if not prompt_path.exists():
                raise FileNotFoundError(f"Filter prompt not found at {prompt_path}")

            with open(prompt_path, "r", encoding="utf-8") as f:
                self._filter_prompt = f.read()

        return self._filter_prompt

    def load_topic_descriptions(self) -> Dict[str, str]:
        """Load topic descriptions from YAML.

        Returns:
            Dictionary mapping topic name to one-sentence description
        """
        if self._topic_descriptions is None:
            desc_path = self.config_dir / "topic_descriptions.yaml"
            if not desc_path.exists():
                raise FileNotFoundError(f"Topic descriptions not found at {desc_path}")

            with open(desc_path, "r", encoding="utf-8") as f:
                self._topic_descriptions = yaml.safe_load(f)

        return self._topic_descriptions

    def load_scoring_prompt(self, topic: str) -> str:
        """Load scoring prompt for a specific topic.

        Args:
            topic: Topic name

        Returns:
            Scoring prompt for the topic
        """
        if topic not in self._scoring_prompts:
            prompt_path = self.config_dir / "scoring_prompts" / f"{topic}.txt"
            if not prompt_path.exists():
                raise FileNotFoundError(
                    f"Scoring prompt for topic '{topic}' not found at {prompt_path}"
                )

            with open(prompt_path, "r", encoding="utf-8") as f:
                self._scoring_prompts[topic] = f.read()

        return self._scoring_prompts[topic]

    def get_filter_prompt(self, topics: List[str]) -> str:
        """Build filter prompt with specific topic descriptions injected.

        Args:
            topics: List of topic names to include in prompt

        Returns:
            Complete filter prompt with topic descriptions
        """
        template = self.load_filter_prompt()
        descriptions = self.load_topic_descriptions()

        # Build formatted topic descriptions
        topic_desc_text = "\n".join(
            [
                f"- {topic}: {descriptions[topic]}"
                for topic in topics
                if topic in descriptions
            ]
        )

        # Inject into template
        return template.replace("{topic_descriptions}", topic_desc_text)

    def validate(self) -> List[str]:
        """Validate that all required config files exist.

        Returns:
            List of error messages (empty if valid)
        """
        errors = []

        # Check filter prompt
        filter_path = self.config_dir / "filter_prompt.txt"
        if not filter_path.exists():
            errors.append(f"Missing filter prompt: {filter_path}")

        # Check topic descriptions
        desc_path = self.config_dir / "topic_descriptions.yaml"
        if not desc_path.exists():
            errors.append(f"Missing topic descriptions: {desc_path}")

        # Check scoring prompts directory
        scoring_dir = self.config_dir / "scoring_prompts"
        if not scoring_dir.exists():
            errors.append(f"Missing scoring prompts directory: {scoring_dir}")

        return errors
