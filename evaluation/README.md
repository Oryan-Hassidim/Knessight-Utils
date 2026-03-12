# Evaluation Suite

This directory contains tools for manually evaluating model quality.

## Setup
1. Place two Excel files under `evaluation/data/`:
   - `filter_gold.xlsx` – columns `sentence_id`, `sentence`, `manual_score`, `model_score`.
   - `score_gold.xlsx` – same format for the scoring phase.

2. Install dependencies:
   ```bash
   pip install pandas scikit-learn openpyxl
   ```

## Usage

### Quick evaluation
Run the report script to print basic metrics:

```bash
python -m evaluation.report --filter --score
```

You can also **save results** and **generate plots** by specifying output and plot flags:

```bash
python -m evaluation.report --filter --score \
    --output-dir evaluation/results --plot
```

This will write JSON/CSV summaries and PNG plots into the given directory.  The command does **not** open windows; to view plots automatically add `--show` or simply open the PNG files yourself.

If you want interactive display in addition to (or instead of) saving, use:

```bash
python -m evaluation.report --filter --score --show
```

### Scoring buckets and consistency

You can evaluate multiple bucket configurations by repeating the `--bucket` option. Each value is a comma‑separated list of boundary points. For example:

```bash
python -m evaluation.report --score \
    --bucket 3,7 --bucket 4,6
```

Results for each configuration are emitted separately (files include the boundaries in their name).

#### Consistency analysis (two-phase workflow)

Consistency testing measures how stable your model is by running it multiple times on the same data. This is split into two phases:

**Phase 1: Generate consistency data**
```bash
# Run your models multiple times and save the results
python -m evaluation.report --generate-consistency
```

This will:
- Load sentences from `evaluation/data/filter_gold.xlsx`
- Run each model in `MODEL_REGISTRY` 3 times (configurable with `--consistency-runs`)
- Save results to `evaluation/data/consistency/consistency_<model_name>.csv`

**Phase 2: Analyze consistency**
```bash
# Analyze the saved data
python -m evaluation.report --consistency
```

This performs two types of analysis:

1. **Intra-model consistency** (same model, multiple runs):
   - Sample standard deviation across runs (Bessel's correction)
   - Coefficient of variation (CV%) to normalize by score magnitude
   - Median and mean variability metrics
   - Flags sentences with high variability (std>1.0 or CV>20%)
   - Shows score ranges for most variable sentences

2. **Inter-model consistency** (different models on same data):
   - Pairwise Pearson correlations between models
   - Mean absolute difference (MAD) between model scores
   - Cross-model variability (std dev and range)
   - Identifies sentences where models disagree most

To save the report to a file, add `--output-dir`:
```bash
python -m evaluation.report --consistency \
    --output-dir evaluation/results
```

This will save the full analysis to `evaluation/results/consistency_report.txt`.

**Benefits of this approach:**
- Generate data once, analyze multiple times
- Avoid expensive API calls during exploratory analysis
- Validate data generation separately from analysis
- Easier debugging and iteration

**Example workflow:**
```bash
# 1. Add your model to evaluation/models.py MODEL_REGISTRY
# 2. Generate data (may take time if calling APIs)
python -m evaluation.report --generate-consistency --consistency-runs 5

# 3. Analyze results (fast, just reads CSVs)
python -m evaluation.report --consistency

# 4. Re-analyze with different code/metrics as needed
python -m evaluation.report --consistency
```

### Experiments
Threshold sweep or consistency checks are in `evaluation/experiments`.

```bash
python -m evaluation.experiments.threshold_sweep
python -m evaluation.experiments.consistency
```

You can extend or write new scripts in `experiments/` for bucket tuning, multi-model comparisons, etc.

## Notes
- This suite is intentionally standalone; invoke manually when needed.
- Metrics functions are in `evaluation/metrics.py` and can be reused in analysis notebooks.
