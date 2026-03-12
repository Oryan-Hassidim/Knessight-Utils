# Evaluation Methods

## Performance Metrics

### Filter Phase

**Threshold Sweep:**
Move threshold from 1 to 5, compute precision/recall/F1 at each level to find optimal cutoff that balances catching relevant speeches vs avoiding false positives.

**Precision-Recall-F1:**
Evaluate binary classification quality using precision (how many predicted relevant are actually relevant), recall (how many actual relevant were found), and F1 (balanced combination of both).

**Confusion Matrix:**
Show counts of true positives, false positives, true negatives, and false negatives to understand exactly where the model succeeds and fails.

### Score Phase

**Bucket Confusion Matrix:**
Group 1-10 scores into buckets (e.g., Low 1-3, Mid 4-7, High 8-10), then show N×N matrix of how often model's predicted bucket matches manual bucket. Reveals systematic scoring biases.

**Cohen's Kappa:**
Measure agreement between model and manual scores while accounting for chance agreement and class imbalance. More robust than simple accuracy.

## Consistency Analysis

### Scoring Consistency

#### Intra-Model Consistency (same model, multiple runs)

**Standard Deviation per Sentence:**
Run the model multiple times on each sentence, calculate how much scores vary. Lower std = more stable/reliable model.

**Coefficient of Variation (CV%):**
Normalize variability by score magnitude (std/mean × 100%). Distinguishes between meaningful variation (high absolute scores with small variation) vs problematic instability (low scores with high variation).

**High Variability Detection:**
Flag sentences where σ>1.0 or CV>20%, indicating the model is unstable on these specific inputs. Helps identify ambiguous cases that need clearer prompts.

#### Inter-Model Consistency (different models, same data)

**Pearson Correlation:**
Measure how similarly two models rank sentences (-1 to +1). High correlation (>0.9) means models agree on which sentences are more/less extreme, even if absolute scores differ.

**Mean Absolute Difference (MAD):**
Calculate average point difference between two models' scores. Shows whether models agree on magnitude (MAD<0.5 = close agreement).

**Cross-Model Variability:**
Find sentences where different models disagree most (high std across models). Identifies inherently ambiguous content that needs better scoring guidelines.

### Filter Consistency

#### Binary Decision Consistency

**Perfect Agreement Rate:**
Percentage of sentences where all runs make the same binary decision (relevant vs. not-relevant). Higher is better for model stability.

**Split Decisions:**
Sentences where the model changes its mind about relevance across runs. Indicates uncertain boundary cases near the threshold.

**Agreement Rate:**
For each sentence, what fraction of runs agree with the majority decision. 100% = all runs identical, <100% = inconsistent.

**Most Unstable Decisions:**
Identify sentences where the model flip-flops most between relevant/not-relevant. These are borderline cases that need clearer filtering criteria.

#### Intra-Model Score Consistency (same filter model, multiple runs)

**Standard Deviation per Sentence:**
Measure how much relevance scores (1-5 scale) vary across runs. Lower std = more stable relevance assessment.

**Coefficient of Variation (CV%):**
Normalize variability by score magnitude. Useful for identifying whether variation is meaningful or problematic.

**High Variability Detection:**
Flag sentences where σ>0.5 or CV>20% on the 1-5 relevance scale. These are sentences where the model struggles to assign consistent relevance.

#### Inter-Model Consistency (different filter models, same data)

**Pearson Correlation:**
Measure how similarly two filter models rank sentence relevance. High correlation means models agree on what's more/less relevant.

**Mean Absolute Difference (MAD):**
Average point difference between filter models on the 1-5 scale. Shows whether models have similar relevance thresholds.

**Binary Decision Agreement:**
Percentage of sentences where two models agree on the binary decision (relevant/not-relevant) given a threshold. High agreement = models use similar criteria.

**Cross-Model Variability:**
Find sentences where different filter models disagree most. Identifies ambiguous content that different models interpret differently.
