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
    """Test the two-phase consistency workflow: generate then analyze."""
    import pandas as pd
    
    # Setup: create filter_gold.xlsx with sample sentences
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    filter_gold = data_dir / "filter_gold.xlsx"
    df = pd.DataFrame({
        "sentence": ["sentence A", "sentence B", "sentence C"],
        "manual_score": [1, 2, 3],
        "model_score": [1, 2, 3]
    })
    df.to_excel(filter_gold, index=False)
    
    consistency_dir = tmp_path / "consistency"
    
    # Patch load_sentences to use our temp file
    import evaluation.experiments.generate_consistency_data as gen_mod
    
    def fake_load(n=100):
        df2 = pd.read_excel(filter_gold)
        return df2["sentence"].tolist()
    
    monkeypatch.setattr(gen_mod, "load_sentences", fake_load)
    
    # Phase 1: Generate data
    monkeypatch.setattr("sys.argv", [
        "report.py",
        "--generate-consistency",
        "--consistency-dir", str(consistency_dir),
        "--consistency-runs", "2"
    ])
    report.main()
    
    # Verify CSV files were created
    csv_files = list(consistency_dir.glob("consistency_*.csv"))
    assert len(csv_files) > 0, "No consistency CSV files generated"
    
    # Phase 2: Analyze
    capsys.readouterr()  # clear previous output
    output_dir = tmp_path / "output"
    monkeypatch.setattr("sys.argv", [
        "report.py",
        "--consistency",
        "--consistency-dir", str(consistency_dir),
        "--output-dir", str(output_dir)
    ])
    report.main()
    
    out = capsys.readouterr().out
    assert "Model:" in out or "Model" in out
    assert "variability" in out.lower()
    
    # Verify report was saved
    report_file = output_dir / "consistency_report.txt"
    assert report_file.exists(), "Consistency report file was not created"
    report_text = report_file.read_text(encoding="utf-8")
    assert "Model:" in report_text
    assert "variability" in report_text.lower()



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
