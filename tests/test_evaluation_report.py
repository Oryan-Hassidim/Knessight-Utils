import pandas as pd
from pathlib import Path

from evaluation import report


def make_excel(tmp_path: Path, name: str, manual, model) -> Path:
    df = pd.DataFrame({"manual_score": manual, "model_score": model})
    path = tmp_path / name
    df.to_excel(path, index=False)
    return path


def test_filter_save_and_plot(tmp_path, capsys):
    path = make_excel(tmp_path, "filter.xlsx", [1, 5, 4, 7], [2, 3, 5, 8])
    outdir = tmp_path / "out"
    report.run_filter_eval(path, outdir, plot=True, show=False)
    captured = capsys.readouterr()
    assert "Saved binary metrics" in captured.out
    assert "Plots written" in captured.out
    assert (outdir / "filter_binary_metrics.json").exists()
    assert (outdir / "filter_threshold_sweep.csv").exists()
    assert (outdir / "filter_threshold_sweep.png").exists()
    # filter confusion no longer produced
    assert not (outdir / "filter_confusion.png").exists()


def test_score_multiple_buckets(tmp_path, capsys):
    # ensure multiple bucket configurations produce separate files
    path = make_excel(tmp_path, "score2.xlsx", [1, 5, 4, 7], [2, 3, 5, 8])
    outdir = tmp_path / "out3"
    report.run_score_eval(path, outdir, plot=False, show=False, bucket_configs=[[3,7],[4,6]])
    assert (outdir / "score_bucket_confusion_3_7.csv").exists()
    assert (outdir / "score_bucket_confusion_4_6.csv").exists()
    # kappa files too
    assert (outdir / "score_kappa_3_7.txt").exists()
    assert (outdir / "score_kappa_4_6.txt").exists()


def test_bucket_confusion_full_matrix():
    # even if data touches only two buckets, the output should be 3x3
    from evaluation.methods import metrics
    manual = [1, 2, 5]  # buckets 0,0,1 with boundaries [3,7]
    model = [2, 2, 6]
    cm, _ = metrics.bucket_confusion(manual, model, [3, 7])
    assert cm.shape == (3, 3)


def test_consistency_workflow(monkeypatch, tmp_path, capsys):
    """Test consistency plotting workflow with existing consistency CSV data."""
    score_dir = tmp_path / "consistency"
    filter_dir = tmp_path / "filter_consistency"
    output_dir = tmp_path / "output"
    score_dir.mkdir()
    filter_dir.mkdir()

    # Minimal mock consistency data (2 runs, 3 sentences)
    mock_df = pd.DataFrame(
        {
            "sentence": ["sentence A", "sentence B", "sentence C"],
            "run_0": [1.0, 2.0, 3.0],
            "run_1": [1.5, 2.0, 3.5],
        }
    )
    mock_df.to_csv(score_dir / "consistency_gpt-4o-mini.csv", index=False)
    mock_df.to_csv(filter_dir / "filter_consistency_gpt-4o-mini.csv", index=False)

    monkeypatch.setattr(
        "sys.argv",
        [
            "report.py",
            "--plot-consistency",
            "--score-dir",
            str(score_dir),
            "--filter-dir",
            str(filter_dir),
            "--output-dir",
            str(output_dir),
        ],
    )
    report.main()

    out = capsys.readouterr().out
    assert "Generating filter consistency plots" in out
    assert "Generating score consistency plots" in out
    assert (output_dir / "filter_consistency_summary.png").exists()
    assert (output_dir / "score_consistency_summary.png").exists()



def test_score_save_and_plot(tmp_path, capsys):
    path = make_excel(tmp_path, "score.xlsx", [1, 5, 4, 7], [2, 3, 5, 8])
    outdir = tmp_path / "out2"
    report.run_score_eval(path, outdir, plot=True)
    captured = capsys.readouterr()
    assert "Saved score confusion" in captured.out
    assert "Plots written" in captured.out
    assert (outdir / "score_bucket_confusion_3_7.csv").exists()
    assert (outdir / "score_kappa_3_7.txt").exists()
    assert (outdir / "score_bucket_confusion_3_7.png").exists()
