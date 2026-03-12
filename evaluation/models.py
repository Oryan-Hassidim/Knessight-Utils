"""Model wrappers to score sentences using different LLMs.

To add your own model for consistency testing:

1. Define a function that takes List[str] and returns List[float]:
   def my_model(sentences: List[str]) -> List[float]:
       # Your API calls here
       # Return scores 1-10 for each sentence
       pass

2. Register it in MODEL_REGISTRY:
   MODEL_REGISTRY = {
       "gpt-4o-mini": my_model,
       "gpt-4": another_model,
   }

3. Generate consistency data:
   python -m evaluation.report --generate-consistency

4. Analyze results:
   python -m evaluation.report --consistency
"""

import os
import json
from pathlib import Path
from typing import List, Callable, Dict

import openai


def _load_scoring_prompt(topic: str = "התיישבות") -> str:
    """Load scoring prompt for a specific topic.
    
    Args:
        topic: Topic name (defaults to התיישבות for consistency testing)
    
    Returns:
        Scoring prompt text
    """
    prompt_path = Path(f"data/config/scoring_prompts/{topic}.txt")
    if not prompt_path.exists():
        raise FileNotFoundError(f"Scoring prompt not found: {prompt_path}")
    
    return prompt_path.read_text(encoding="utf-8")


def _call_openai_scoring(sentences: List[str], model_name: str, temperature: float = 0.3, topic: str = "התיישבות") -> List[float]:
    """Call OpenAI API to score sentences on stance (1-10 scale).
    
    Uses the same prompt format as the main program's score_pipeline.
    
    Args:
        sentences: List of text snippets to score
        model_name: OpenAI model name (e.g., "gpt-4o-mini", "gpt-4o")
        temperature: Sampling temperature (default 0.3 for consistency)
        topic: Topic name for loading the appropriate scoring prompt
    
    Returns:
        List of scores (1.0 to 10.0)
    """
    client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    scores = []
    
    # Load the actual scoring prompt used in production
    scoring_prompt = _load_scoring_prompt(topic)
    
    system_prompt = (
        "You are a political speech analyst. Score stance on topics from 1 (strongly opposes) "
        "to 10 (strongly supports). Respond with JSON only."
    )
    
    for sentence in sentences:
        # Match the exact format from score_pipeline._build_scoring_request
        user_message = f"""Speech Text:
{sentence}

Topic: {topic}

{scoring_prompt}

Respond with JSON only, format:
{{
  "stance_score": 1-10
}}"""
        
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            response_format={"type": "json_object"},
            temperature=temperature,
        )
        
        result = json.loads(response.choices[0].message.content)
        score = float(result.get("stance_score", 5.0))
        scores.append(score)
    
    return scores


def gpt_4o_mini_score(sentences: List[str]) -> List[float]:
    """Score using GPT-4o-mini model."""
    return _call_openai_scoring(sentences, "gpt-4o-mini", temperature=0.3)


def gpt_4o_score(sentences: List[str]) -> List[float]:
    """Score using GPT-4o model."""
    return _call_openai_scoring(sentences, "gpt-4o", temperature=0.3)


def gpt_4o_mini_temp_1(sentences: List[str]) -> List[float]:
    """Score using GPT-4o-mini with higher temperature (more variability)."""
    return _call_openai_scoring(sentences, "gpt-4o-mini", temperature=1.0)


def dummy_model_score(sentences: List[str]) -> List[float]:
    """Simple stub that returns 1.0 for all sentences."""
    return [1.0 for _ in sentences]


ModelFunc = Callable[[List[str]], List[float]]

MODEL_REGISTRY: Dict[str, ModelFunc] = {
    "dummy": dummy_model_score,
    "gpt-4o-mini": gpt_4o_mini_score,
    "gpt-4o": gpt_4o_score,
    "gpt-4o-mini-temp1": gpt_4o_mini_temp_1,
}
