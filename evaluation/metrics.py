"""Core evaluation metrics for relevance and scoring."""

from typing import List, Tuple
import pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score, confusion_matrix, cohen_kappa_score, mean_absolute_error, roc_auc_score


def binary_metrics(
    manual: List[float],
    model: List[float],
    manual_thr: float = 4.0,
    model_thr: float = 4.0,
) -> dict:
    """Compute precision/recall/f1 and confusion matrix for binary relevance."""
    y_true = [1 if x >= manual_thr else 0 for x in manual]
    y_pred = [1 if x >= model_thr else 0 for x in model]

    prec = precision_score(y_true, y_pred, zero_division=0)
    rec = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)
    cm = confusion_matrix(y_true, y_pred)

    return {"precision": prec, "recall": rec, "f1": f1, "confusion": cm.tolist()}


import numpy as np

def threshold_sweep(
    manual: List[float],
    model: List[float],
    manual_thr: float = 4.0,
    min_thr: int = 1,
    max_thr: int = 5,
) -> pd.DataFrame:
    """Sweep model thresholds and compute metrics."""
    records = []

    for thr in range(min_thr, max_thr + 1):
        metrics = binary_metrics(manual, model, manual_thr, thr)
        records.append({"threshold": thr, **metrics})

    return pd.DataFrame(records)


def bucket_confusion(
    manual: List[float],
    model: List[float],
    boundaries: List[float],
) -> Tuple[pd.DataFrame, float]:
    """Create confusion matrix for bucketed scores and compute kappa.

    boundaries define integer bucket edges:
    [3,7] -> 1-3, 4-7, 8+
    """

    def bucket(x):
        lower = 1
        for i, b in enumerate(boundaries):
            if lower <= x <= b:
                return i
            lower = b + 1
        return len(boundaries)

    y_true = [bucket(x) for x in manual]
    y_pred = [bucket(x) for x in model]

    labels = list(range(len(boundaries) + 1))
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    kappa = cohen_kappa_score(y_true, y_pred, labels=labels)

    df_cm = pd.DataFrame(cm, index=labels, columns=labels)
    return df_cm, kappa