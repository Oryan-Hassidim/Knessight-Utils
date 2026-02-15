"""Test model name configuration."""

from dotenv import load_dotenv

load_dotenv()

from src.modules.config import Config

config = Config()

print("=" * 60)
print("MODEL CONFIGURATION TEST")
print("=" * 60)
print(f"Filter Model:  {config.FILTER_MODEL_NAME}")
print(f"Score Model:   {config.SCORE_MODEL_NAME}")
print("=" * 60)
print("✓ Model names loaded successfully")
