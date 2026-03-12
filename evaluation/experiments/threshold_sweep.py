"""Standalone threshold sweep on filter gold dataset."""

import pandas as pd
from pathlib import Path
from ..metrics import threshold_sweep


def main():
    path = Path("evaluation/data/filter_gold.xlsx")
    df = pd.read_excel(path)
    manual = df["manual_score"].tolist()
    model = df["model_score"].tolist()
    result = threshold_sweep(manual, model)
    print(result)

if __name__ == "__main__":
    main()
