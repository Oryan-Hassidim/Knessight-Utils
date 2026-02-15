# Knessight Backend

Knessight Backend is a Python CLI system for analyzing Israeli Knesset speeches using AI. It processes speeches in two decoupled phases: **Filter** (relevance scoring) and **Score** (stance analysis), using OpenAI's Batch API for efficient processing at scale.

## Features

- **Two-Phase Pipeline**: Independent Filter and Score phases for flexibility
- **Multi-Topic Filtering**: Evaluates one speech against multiple topics simultaneously
- **Stance Scoring**: Rates speaker stance from 1 (opposes) to 10 (supports)
- **Reasoning Sampling**: Configurable percentage includes AI reasoning for validation
- **Resumable Execution**: Automatic incremental processing of only incomplete pairs
- **Cost Tracking**: Logs token usage and estimated costs
- **MK Name Disambiguation**: Fuzzy matching with interactive resolution and caching

## Installation

Requires Python 3.10+ and [uv](https://github.com/astral-sh/uv) package manager.

```bash
# Install dependencies
uv sync

# Set OpenAI API key
export OPENAI_API_KEY="your-api-key-here"
```

## Project Structure

```
knessight/
├── src/knessight/          # Main package
│   ├── cli.py             # Typer CLI commands
│   └── modules/           # Core modules
├── config/                # Prompts and settings
│   ├── filter_prompt.txt
│   ├── topic_descriptions.yaml
│   └── scoring_prompts/   # Per-topic scoring prompts
├── input/                 # Input files
│   ├── mks.txt           # MK names (one per line)
│   └── topics.txt        # Topics to analyze
├── cache/                 # Job tracking and caches
├── intermediate/          # Phase 1 outputs
├── client_data/          # Final outputs
│   ├── mks.csv
│   ├── mk_data/{id}/
│   └── topics/
└── logs/                 # Execution logs
```

## Configuration

### 1. Database

Place your SQLite database (`knesset.db`) in the project root. It should contain:
- `knesset_speeches_view` (view with id, text, date, person_id, etc.)
- `people` table (person_id, first_name, surname, faction, etc.)
- `names`, `topics`, `topic_extras` tables

### 2. Input Files

**`input/mks.txt`** - MK names to analyze (one per line):
```
יצחק רבין
בנימין נתניהו
שמעון פרס
```

**`input/topics.txt`** - Topics to analyze (one per line):
```
התיישבות
נישואים_אזרחיים
```

### 3. Configuration Files

**`config/topic_descriptions.yaml`** - One-sentence descriptions for each topic:
```yaml
התיישבות: "הקמה והרחבה של יישובים יהודיים בשטחים"
נישואים_אזרחיים: "אפשרות לנישואין אזרחיים מחוץ למסגרת הדתית"
```

**`config/filter_prompt.txt`** - General filter prompt (uses `{topic_descriptions}` placeholder)

**`config/scoring_prompts/{topic}.txt`** - Scoring prompt for each topic (e.g., `התיישבות.txt`)

## Usage

### Run Full Pipeline

Process all pending (MK, topic) pairs through both phases:

```bash
uv run python -m knessight both
```

### Run Individual Phases

**Phase 1: Filter** (identify relevant speeches)
```bash
uv run python -m knessight filter --db-path knesset.db
```

**Phase 2: Score** (rate stance on relevant speeches)
```bash
uv run python -m knessight score --reasoning-rate 0.15
```

### Check Status

View job statistics:
```bash
uv run python -m knessight status
```

### Reprocess

Force reprocess completed pairs:
```bash
uv run python -m knessight filter --force-reprocess
uv run python -m knessight score --force-reprocess
```

### Cleanup

Remove intermediate files for completed pairs:
```bash
uv run python -m knessight cleanup-intermediate --yes
```

## Options

### `filter` Command
- `--db-path`: Path to SQLite database (overrides `DATABASE_PATH` env var)
- `--force-reprocess`: Reset all pairs to pending

### `score` Command
- `--db-path`: Path to SQLite database (overrides `DATABASE_PATH` env var)
- `--reasoning-rate`: Probability of requesting reasoning (default: `0.1` = 10%)
- `--force-reprocess`: Reset completed pairs to filter_complete

### `both` Command
- Combines all options from `filter` and `score`

## Output Structure

### Intermediate Files (Phase 1)
```
intermediate/{person_id}_{topic}_filtered.csv
```
Columns: `Id`, `Text`, `RelevanceScore`

### Final Outputs (Phase 2)

**Per MK:**
```
client_data/mk_data/{person_id}/
├── main.json              # Aggregated data
└── {topic}.csv           # Scored speeches (with optional reasoning)
```

**`main.json`** structure:
```json
{
  "id": 2595,
  "knessetSiteId": 2595,
  "name": "יצחק רבין",
  "imageUrl": "",
  "description": "Faction: מערך, Party: מפלגת העבודה",
  "Topics": [
    {
      "topicName": "התיישבות",
      "count": 150,
      "average": 7.23
    }
  ]
}
```

**`{topic}.csv`** columns:
- `Id`: Speech ID
- `Date`: Speech date
- `Topic`: Topic name
- `Text`: Speech text
- `Rank`: Stance score (1-10)
- `Reasoning`: AI reasoning (empty string if not sampled)

**Topic Aggregations:**
```
client_data/topics/{topic}.json
```
```json
{
  "2595": [150, 7.23],
  "2312": [200, 8.45]
}
```
Format: `person_id: [count, average_stance]`

**MK List:**
```
client_data/mks.csv
```
Columns: `id`, `first name`, `last name`, `knesset site id`, `image url`

### Logs

- `logs/filter.log` - Filter pipeline logs
- `logs/score.log` - Score pipeline logs
- `logs/errors.log` - Error logs
- `logs/costs.json` - Token usage and cost tracking

### Cache Files

- `cache/mk_resolution_cache.json` - Resolved MK names
- `cache/job_status.json` - Phase completion tracking
- `cache/batch_jobs.json` - Batch API job metadata
- `cache/failed_speeches.json` - Failed requests log

## Environment Variables

Configure via environment variables:

```bash
export OPENAI_API_KEY="sk-..."
export DATABASE_PATH="knesset.db"
export CLIENT_DATA_PATH="client_data"
export REASONING_SAMPLE_RATE="0.15"
export BATCH_SIZE="10000"
export BATCH_POLL_INTERVAL="30"
export RELEVANCE_THRESHOLD="4"
export RETRY_ATTEMPTS="3"
```

## How It Works

### Phase 1: Filter
1. Load all speeches for each MK from database
2. For each speech, evaluate relevance against ALL pending topics simultaneously
3. Build filter prompt with topic descriptions
4. Submit to OpenAI Batch API (10K requests per batch)
5. Poll for completion
6. Save speeches with relevance ≥4 to `intermediate/{person_id}_{topic}_filtered.csv`
7. Mark (MK, topic) pairs as "filter_complete"

### Phase 2: Score
1. Load filtered speeches from intermediate CSVs
2. For each speech:
   - Load topic-specific scoring prompt
   - Randomly decide (e.g., 10% chance) if reasoning is requested
   - Submit to Batch API
3. Poll for completion
4. Save results to `client_data/mk_data/{person_id}/{topic}.csv`
5. Update `main.json` and `topics/{topic}.json` with aggregated stats
6. Mark (MK, topic) as "score_complete"

### Incremental Processing
- Each run checks `cache/job_status.json`
- Only processes pairs not yet complete for the current phase
- Filter phase: processes "pending" pairs
- Score phase: processes "filter_complete" pairs
- Use `--force-reprocess` to reset specific phases

## Cost Estimation

Approximate costs (using GPT-4o-mini at $0.150/$0.600 per 1M tokens):

- **Filter**: ~500 tokens per speech × topics
- **Score**: ~400 tokens per speech
- **Example**: 10 MKs × 3 topics × 100 speeches/MK = 3,000 speeches
  - Filter: ~$0.23
  - Score: ~$0.18
  - **Total: ~$0.41**

Monitor actual costs in `logs/costs.json`.

## Troubleshooting

### No speeches found for MK
- Check MK name spelling in `input/mks.txt`
- Verify `person_id` resolution in `cache/mk_resolution_cache.json`
- Query database: `SELECT DISTINCT first_name, surname FROM people`

### Missing topic prompt
- Ensure `config/scoring_prompts/{topic}.txt` exists
- Topic name in file must match `input/topics.txt` exactly

### Batch job stuck
- Check OpenAI Batch API status: https://platform.openai.com/batches
- Batches can take hours; polling interval is 30s by default
- Force completion: manually update `cache/batch_jobs.json` status to "completed"

### Failed speeches
- Review `cache/failed_speeches.json`
- Check `logs/errors.log` for details
- System retries 3 times automatically

## Development

Run tests (if implemented):
```bash
uv run pytest
```

Format code:
```bash
uv run black src/
uv run isort src/
```

## License

MIT License

## Support

For issues or questions, please open a GitHub issue.
