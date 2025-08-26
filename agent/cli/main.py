import os
import sys
from typing import Optional

import duckdb

try:
    from rich.console import Console
    from rich.prompt import Prompt
    from rich.panel import Panel
    from rich.table import Table
except Exception:  # pragma: no cover
    Console = None
    Prompt = None
    Panel = None
    Table = None

DEFAULT_DATA_DIR = os.path.abspath(os.path.join(os.getcwd(), "data"))


def find_parquet_path(user_path: Optional[str]) -> str:
    if user_path and os.path.exists(user_path):
        return os.path.abspath(user_path)
    if os.path.isdir(DEFAULT_DATA_DIR):
        # pick first .parquet file
        for name in os.listdir(DEFAULT_DATA_DIR):
            if name.endswith(".parquet"):
                return os.path.join(DEFAULT_DATA_DIR, name)
    raise FileNotFoundError("No parquet file found. Pass --path or place a .parquet in ./data/")


def open_duckdb():
    return duckdb.connect(database=':memory:')


def preview_rows(con, parquet_path: str, limit: int = 10):
    return con.execute(
        "SELECT * FROM read_parquet(?) LIMIT ?", [parquet_path, limit]
    ).fetch_df()


def _render_result(console, title: str, result):
    if console and Table:
        table = Table(title=title)
        try:
            import pyarrow as pa  # type: ignore
            if isinstance(result, pa.Table):
                for name in result.column_names:
                    table.add_column(name)
                for i in range(min(20, result.num_rows)):
                    row = [str(result.column(j)[i].as_py()) for j in range(result.num_columns)]
                    table.add_row(*row)
                console.print(table)
                return
        except Exception:
            pass
        console.print(str(result))
    else:
        print(result)


def main(argv=None):
    argv = argv or sys.argv[1:]
    import argparse
    import re
    import time as _time
    parser = argparse.ArgumentParser(prog="synmax-agent", description="SynMax Data Agent")
    parser.add_argument("--path", dest="path", default=None, help="Path to parquet (defaults to ./data)")
    parser.add_argument("--model", dest="model", default=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
    parser.add_argument("--max-preview-rows", dest="max_preview_rows", type=int, default=1000)
    parser.add_argument("--timeout-sec", dest="timeout_sec", type=int, default=30)
    parser.add_argument("--save-run", dest="save_run", action="store_true", default=True)
    parser.add_argument("--no-save-run", dest="save_run", action="store_false")
    parser.add_argument("--query", dest="query", default=None, help="Run a single question non-interactively and exit")
    args = parser.parse_args(argv)

    console = Console() if Console else None

    parquet_path = None
    try:
        parquet_path = find_parquet_path(args.path)
    except Exception as e:
        if console:
            console.print(Panel.fit(str(e)))
        else:
            print(str(e))
        sys.exit(2)

    con = open_duckdb()
    df = preview_rows(con, parquet_path, min(10, args.max_preview_rows))

    if console:
        console.print(Panel.fit(f"Loaded dataset: {parquet_path}"))
        console.print(df.head())
    else:
        print(f"Loaded dataset: {parquet_path}")
        print(df.head())

    from agent.exec.duck import DuckDBExecutor
    from agent.utils.schema_cache import SchemaCache
    from agent.planner.rule_planner import parse_simple
    from agent.exec.sql_builder import build_sql
    from agent.report.reporter import Reporter
    from agent.tools.analytics import correlation_pipelines, cluster_pipelines_monthly, anomalies_vs_category
    from agent.planner.llm_explain import summarize_answer

    def parse_int(s: str, default: int) -> int:
        try:
            return int(s)
        except Exception:
            return default

    def run_once(question: str):
        executor = DuckDBExecutor()
        schema = SchemaCache().get_or_load(executor, parquet_path)
        ql = question.lower()

        # Analytics triggers
        if "correlation" in ql or "correlat" in ql:
            t0 = _time.time()
            result = correlation_pipelines(executor, parquet_path)
            latency = _time.time() - t0
            _render_result(console, "pipeline correlation (top pairs)", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, "--analytics: correlation_pipelines", result, args.model) or ""
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + "Notes: correlation of daily totals across top pipelines")
                run_dir = reporter.save_artifacts({"intent": "analytic", "notes": "correlation"}, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0
        if "cluster" in ql or "clustering" in ql:
            # parse k and scaling from text (e.g., "k=6", "scale=minmax")
            k = 5
            scaling = "standard"
            m = re.search(r"k\s*=\s*(\d+)", ql)
            if m:
                k = parse_int(m.group(1), 5)
            m = re.search(r"scale\s*=\s*(standard|minmax|none)", ql)
            if m:
                scaling = m.group(1)
            t0 = _time.time()
            result = cluster_pipelines_monthly(executor, parquet_path, k=k, scaling=scaling)
            latency = _time.time() - t0
            _render_result(console, f"pipeline clusters (k={k}, scaling={scaling})", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, f"--analytics: cluster_pipelines_monthly (k={k}, scaling={scaling})", result, args.model) or ""
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: k-means clustering (k={k}, scaling={scaling})")
                run_dir = reporter.save_artifacts({"intent": "analytic", "notes": "clustering"}, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0

        parsed = parse_simple(question, schema)
        # Anomalies vs category directive
        if parsed.special and parsed.special.get("type") == "anomalies_vs_category":
            z = float(parsed.special.get("z", 3.0))
            min_days = int(parsed.special.get("min_days", 3))
            # parse optional state/year/receipts/deliveries
            year = None
            m = re.search(r"\b(20\d{2})\b", ql)
            if m:
                year = parse_int(m.group(1), None)  # type: ignore[arg-type]
            state = None
            m = re.search(r"\bstate\s+([A-Z]{2})\b|\bin\s+([A-Z]{2})\b", ql)
            state = m.group(1) if m and m.group(1) else (m.group(2) if m else None)
            rds = None
            if "receipts" in ql:
                rds = -1
            if "deliveries" in ql:
                rds = 1
            t0 = _time.time()
            result = anomalies_vs_category(executor, parquet_path, z_threshold=z, min_anomaly_days=min_days, year=year, state=state, rec_del_sign=rds)
            latency = _time.time() - t0
            _render_result(console, "anomalous locations vs category baseline", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, f"--analytics: anomalies_vs_category (z>={z}, min_days={min_days})", result, args.model) or ""
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: category baseline z-threshold {z}, min days {min_days}")
                run_dir = reporter.save_artifacts({"intent": "analytic", "notes": "anomalies_vs_category"}, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0

        # Deterministic rule-based
        if parsed.plan is None:
            msg = "I can: anomalies vs category, correlations, clustering, preview, sums, distincts, group-bys, and top-N by a column."
            (console.print(Panel.fit(msg)) if console else print(msg))
            return 1
        sql, params = build_sql(parquet_path, parsed.plan, schema)
        if console:
            console.print(Panel.fit("Executed SQL:"))
            console.print(sql)
        else:
            print("Executed SQL:\n" + sql)
        t0 = _time.time()
        result = executor.query(sql, params)
        latency = _time.time() - t0
        _render_result(console, parsed.notes, result)
        if args.save_run:
            reporter = Reporter()
            plan_dict = {
                "intent": parsed.intent,
                "notes": parsed.notes,
                "plan": {
                    "columns": parsed.plan.columns,
                    "group_by": parsed.plan.group_by,
                    "aggregations": parsed.plan.aggregations,
                    "limit": parsed.plan.limit,
                },
            }
            expl = summarize_answer(question, sql, result, args.model) or ""
            summary = f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: {parsed.notes}"
            run_dir = reporter.save_artifacts(plan_dict, sql, result, markdown_summary=summary, latency_sec=latency)
            if console:
                console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            else:
                print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")
        return 0

    # Non-interactive
    if args.query:
        code = run_once(args.query)
        sys.exit(code)

    # Interactive loop
    while True:
        q = Prompt.ask("Ask a question (:exit to quit)") if Prompt else input("Q (:exit to quit): ")
        if q.strip().lower() in {":exit", ":quit", "exit", "quit"}:
            break
        run_once(q)


if __name__ == "__main__":
    main()
