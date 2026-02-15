"""Check batch results from the latest filter run."""

from dotenv import load_dotenv

load_dotenv()

import json
from src.modules.batch_manager import BatchManager


def check_batch_results():
    """Check what results we got from the filter batch."""
    bm = BatchManager()

    # Get latest batch
    batch_ids = list(bm._batch_jobs.keys())
    if not batch_ids:
        print("No batches found")
        return

    latest_batch_id = batch_ids[-1]
    print(f"Latest batch: {latest_batch_id}")
    print(f"Batch info: {json.dumps(bm._batch_jobs[latest_batch_id], indent=2)}\n")

    # Get results
    results = bm.retrieve_results(latest_batch_id)
    print(f"Total results: {len(results)}\n")

    if results:
        # Show first result
        print("First result:")
        print(json.dumps(results[0], indent=2)[:2000])

        # Check if any had relevance >= 4
        high_relevance_count = 0
        for result in results:
            try:
                if result.get("response", {}).get("status_code") == 200:
                    content = result["response"]["body"]["choices"][0]["message"][
                        "content"
                    ]
                    scores = json.loads(content)
                    for topic, score_data in scores.items():
                        if score_data.get("relevance", 0) >= 4:
                            high_relevance_count += 1
                            print(
                                f"\nHigh relevance found: {topic} = {score_data.get('relevance')}"
                            )
                            print(f"  Speech ID: {result.get('custom_id')}")
            except Exception as e:
                continue

        print(f"\n\nTotal speeches with relevance >= 4: {high_relevance_count}")


if __name__ == "__main__":
    check_batch_results()
