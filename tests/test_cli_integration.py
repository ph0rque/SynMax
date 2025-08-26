import os, subprocess, sys

PYTHON = sys.executable


def run_query(q: str):
    env = os.environ.copy()
    # point to fixture dir so default discovery finds it? We'll pass --path
    cmd = [PYTHON, '-m', 'agent.cli.main', '--path', 'tests/fixtures/sample.parquet', '--query', q]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout + proc.stderr


def test_cli_top_pipelines():
    code, out = run_query('top 3 pipeline_name by scheduled_quantity')
    assert code == 0
    assert 'Executed SQL' in out or 'Artifacts saved to' in out


def test_cli_anomalies_vs_category():
    code, out = run_query('identify anomalous points that behave outside of their point categories in 2024 z=2.5 min_days=2')
    assert code == 0
    assert 'anomalous' in out.lower() or 'Artifacts saved to' in out
