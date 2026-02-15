# Knessight Backend - Quick Start Guide

Get started with Knessight in 5 minutes!

## Prerequisites

- Python 3.10+
- [uv package manager](https://github.com/astral-sh/uv)
- SQLite database with Knesset speeches (`knesset.db`)
- OpenAI API key

## Setup Steps

### 1. Set OpenAI API Key

**Windows (PowerShell):**
```powershell
$env:OPENAI_API_KEY = "sk-your-api-key-here"
```

**Linux/Mac:**
```bash
export OPENAI_API_KEY="sk-your-api-key-here"
```

Or create `.env` file:
```bash
cp .env.example .env
# Edit .env and add your API key, database path, and output path
```

### 2. Place Database

Copy your `knesset.db` SQLite database to the project root (or specify path in `.env`):
```
knessight/
├── knesset.db  ← Place here
├── src/
├── config/
└── ...
```

### 3. Configure Input Files

**Edit `input/mks.txt`** - Add MK names (Hebrew, one per line):
```
יצחק רבין
בנימין נתניהו
שמעון פרס
אריאל שרון
```

**Edit `input/topics.txt`** - Add topics to analyze:
```
התיישבות
נישואים_אזרחיים
```

### 4. Add Topic Descriptions

**Edit `config/topic_descriptions.yaml`** - Match topics from step 3:
```yaml
התיישבות: "הקמה והרחבה של יישובים יהודיים בשטחים"
נישואים_אזרחיים: "אפשרות לנישואין אזרחיים מחוץ למסגרת הדתית"
```

### 5. Create Scoring Prompts

For each topic, create `config/scoring_prompts/{topic}.txt`.

Example - `config/scoring_prompts/התיישבות.txt`:
```
Score the speaker's stance on Israeli settlements from 1 to 10:
- 1-2: Strongly opposes (supports freeze/dismantling)
- 5-6: Neutral or mixed
- 9-10: Strongly supports (advocates expansion)

Provide an integer score from 1 to 10.
```

## Run Pipeline

### Option 1: Full Pipeline (Recommended for First Run)

```bash
cd knessight
uv run python -m knessight both
```

This runs both Filter and Score phases automatically.

### Option 2: Step-by-Step

**Phase 1: Filter relevant speeches**
```bash
uv run python -m knessight filter
```

**Phase 2: Score stance on filtered speeches**
```bash
uv run python -m knessight score
```

## Check Progress

Monitor job status:
```bash
uv run python -m knessight status
```

Output:
```
Job Status Summary
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━┓
┃ Status           ┃ Count ┃ Percentage ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━┩
│ pending          │    12 │       60.0%│
│ filter_complete  │     6 │       30.0%│
│ score_complete   │     2 │       10.0%│
├──────────────────┼───────┼────────────┤
│ Total            │    20 │      100.0%│
└──────────────────┴───────┴────────────┘
```

## View Results

Results are saved to `client_data/`:

**MK Aggregated Data:**
```
client_data/mk_data/2595/
├── main.json              # All topics for this MK
└── התיישבות.csv          # Scored speeches (Reasoning column for sampled speeches)
```

**Topic Aggregations:**
```
client_data/topics/התיישבות.json
```

**MK List:**
```
client_data/mks.csv
```

## Common Issues

### "No speeches found for person_id X"

**Solution:** MK name not resolved correctly. Check:
```bash
cat cache/mk_resolution_cache.json
```

If incorrect, delete the cache and re-run:
```bash
rm cache/mk_resolution_cache.json
uv run python -m knessight filter
```

### "Missing scoring prompt: config/scoring_prompts/topic.txt"

**Solution:** Create the missing prompt file:
```bash
echo "Score stance 1-10..." > "config/scoring_prompts/topic.txt"
```

### Batch job taking a long time

**Expected:** OpenAI Batch API can take several hours. The system polls every 30 seconds automatically.

Check batch status at: https://platform.openai.com/batches

## Next Steps

- **Add more MKs:** Edit `input/mks.txt` and re-run
- **Add more topics:** Update `input/topics.txt`, `config/topic_descriptions.yaml`, and create scoring prompts
- **Adjust reasoning rate:** `uv run python -m knessight score --reasoning-rate 0.2` (20% include reasoning)
- **Reprocess data:** Use `--force-reprocess` flag
- **Monitor costs:** Check `logs/costs.json`

## Getting Help

- Full documentation: See [README.md](README.md)
- Issues: Open a GitHub issue
- Logs: Check `logs/` directory for errors

## Example Workflow

```bash
# 1. Initial setup (one-time)
export OPENAI_API_KEY="sk-..."
cp knesset.db knessight/
nano input/mks.txt  # Add MK names
nano input/topics.txt  # Add topics

# 2. Run full pipeline
uv run python -m knessight both

# 3. Wait for completion (can take hours)
# Check status periodically:
uv run python -m knessight status

# 4. View results
ls client_data/mk_data/
cat client_data/topics/התיישבות.json

# 5. Add more data later
echo "דוד בן גוריון" >> input/mks.txt
uv run python -m knessight both  # Only processes new pairs

# 6. Clean up intermediate files (optional)
uv run python -m knessight cleanup-intermediate --yes
```

You're ready to go! 🚀
