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
    from agent.tools.analytics import correlation_pipelines, cluster_pipelines_monthly, anomalies_vs_category, anomalies_iqr, sudden_shifts, trends_summary
    from agent.planner.llm_explain import summarize_answer, generate_hypotheses
    from agent.utils.profile_cache import ProfileCache
    from agent.utils.caveats import build_caveats
    from agent.utils.answers import make_concise_answer
    from agent.planner.openai_planner import choose_analytic_tool

    profile_cache = ProfileCache()

    def parse_int(s: str, default: int) -> int:
        try:
            return int(s)
        except Exception:
            return default

    def run_once(question: str):
        executor = DuckDBExecutor()
        schema = SchemaCache().get_or_load(executor, parquet_path)
        ql = question.lower()
        prof = profile_cache.get_or_profile(executor, parquet_path, sample_rows=10000)

        # Analytics triggers
        if "correlation" in ql or "correlat" in ql:
            m = re.search(r"method\s*=\s*(pearson|spearman)", ql)
            method = m.group(1) if m else "pearson"
            include_p = bool(re.search(r"p[-_ ]?value\s*=\s*(1|true|yes)", ql))
            t0 = _time.time()
            result = correlation_pipelines(executor, parquet_path, method=method, include_pvalue=include_p)
            latency = _time.time() - t0
            concise = make_concise_answer(result, {"analytics": "correlation"})
            (console.print(concise) if console else print(concise))
            _render_result(console, "pipeline correlation (top pairs)", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, f"--analytics: correlation_pipelines (method={method}, include_pvalue={include_p})", result, args.model) or ""
                caveats = build_caveats(result, {"analytics": "correlation", "profile": prof, "method": method, "include_pvalue": include_p})
                missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: correlation (method={method}, include_pvalue={include_p})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                plan = {"intent": "analytic", "notes": "correlation", "params": {"method": method, "include_pvalue": include_p}, "pseudo": "pivot daily totals by pipeline, compute pairwise correlations"}
                run_dir = reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0
        if "cluster" in ql or "clustering" in ql:
            k = 5
            scaling = "standard"
            algorithm = "kmeans"
            seed = 42
            m = re.search(r"k\s*=\s*(\d+)", ql)
            if m:
                k = parse_int(m.group(1), 5)
            m = re.search(r"scale\s*=\s*(standard|minmax|none)", ql)
            if m:
                scaling = m.group(1)
            m = re.search(r"algo(rithm)?\s*=\s*(kmeans|minibatch)", ql)
            if m:
                algorithm = m.group(2)
            m = re.search(r"seed\s*=\s*(\d+)", ql)
            if m:
                seed = parse_int(m.group(1), 42)
            t0 = _time.time()
            result = cluster_pipelines_monthly(executor, parquet_path, k=k, scaling=scaling, algorithm=algorithm, seed=seed)
            latency = _time.time() - t0
            concise = make_concise_answer(result, {"analytics": "clustering"})
            (console.print(concise) if console else print(concise))
            _render_result(console, f"pipeline clusters (k={k}, scaling={scaling}, algorithm={algorithm}, seed={seed})", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, f"--analytics: cluster_pipelines_monthly (k={k}, scaling={scaling}, algorithm={algorithm}, seed={seed})", result, args.model) or ""
                caveats = build_caveats(result, {"analytics": "clustering", "profile": prof, "algorithm": algorithm})
                missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: clustering (k={k}, scaling={scaling}, algorithm={algorithm}, seed={seed})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                plan = {"intent": "analytic", "notes": "clustering", "params": {"k": k, "scaling": scaling, "algorithm": algorithm, "seed": seed}, "pseudo": "monthly totals by pipeline -> scale -> (MiniBatch)KMeans -> labels & silhouette"}
                run_dir = reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0

        # Additional analytics
        if "iqr" in ql and ("anomal" in ql or "outlier" in ql):
            k = 1.5
            m = re.search(r"k\s*=\s*([0-9]+(?:\.[0-9]+)?)", ql)
            if m:
                try:
                    k = float(m.group(1))
                except Exception:
                    k = 1.5
            t0 = _time.time()
            result = anomalies_iqr(executor, parquet_path, k=k)
            latency = _time.time() - t0
            concise = make_concise_answer(result, {"analytics": "anomalies_iqr"})
            (console.print(concise) if console else print(concise))
            _render_result(console, f"daily outliers by IQR (k={k})", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, f"--analytics: anomalies_iqr (k={k})", result, args.model) or ""
                caveats = build_caveats(result, {"analytics": "anomalies_iqr", "profile": prof})
                missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: IQR outliers (k={k})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                plan = {"intent": "analytic", "notes": "anomalies_iqr", "params": {"k": k}, "pseudo": "daily totals -> IQR fences -> flag days outside [Q1-k*IQR, Q3+k*IQR]"}
                run_dir = reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0

        if "sudden" in ql or "shift" in ql:
            window = 7
            sigma = 3.0
            m = re.search(r"window\s*=\s*(\d+)", ql)
            if m:
                window = parse_int(m.group(1), 7)
            m = re.search(r"sigma\s*=\s*([0-9]+(?:\.[0-9]+)?)", ql)
            if m:
                try:
                    sigma = float(m.group(1))
                except Exception:
                    sigma = 3.0
            t0 = _time.time()
            result = sudden_shifts(executor, parquet_path, window=window, sigma=sigma)
            latency = _time.time() - t0
            concise = make_concise_answer(result, {"analytics": "sudden_shifts"})
            (console.print(concise) if console else print(concise))
            _render_result(console, f"sudden shifts (window={window}, sigma={sigma})", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, f"--analytics: sudden_shifts (window={window}, sigma={sigma})", result, args.model) or ""
                caveats = build_caveats(result, {"analytics": "sudden_shifts", "profile": prof})
                missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: sudden shifts (window={window}, sigma={sigma})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                plan = {"intent": "analytic", "notes": "sudden_shifts", "params": {"window": window, "sigma": sigma}, "pseudo": "daily totals -> rolling mean/std -> |x-mean|/std >= sigma"}
                run_dir = reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0

        if "trend" in ql or "trends" in ql:
            by = "month" if "month" in ql else ("day" if "day" in ql else "month")
            t0 = _time.time()
            result = trends_summary(executor, parquet_path, by=by)
            latency = _time.time() - t0
            concise = make_concise_answer(result, {"analytics": "trends"})
            (console.print(concise) if console else print(concise))
            _render_result(console, f"trends summary by {by}", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, f"--analytics: trends_summary (by={by})", result, args.model) or ""
                caveats = build_caveats(result, {"analytics": "trends", "profile": prof})
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: trends summary (by={by})\n" + ("\n".join(f"- {c}" for c in caveats)))
                run_dir = reporter.save_artifacts({"intent": "analytic", "notes": "trends"}, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0

        parsed = parse_simple(question, schema)
        if parsed.special and parsed.special.get("type") == "anomalies_vs_category":
            z = float(parsed.special.get("z", 3.0))
            min_days = int(parsed.special.get("min_days", 3))
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
            concise = make_concise_answer(result, {"analytics": "anomalies_vs_category"})
            (console.print(concise) if console else print(concise))
            _render_result(console, "anomalous locations vs category baseline", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, f"--analytics: anomalies_vs_category (z>={z}, min_days={min_days})", result, args.model) or ""
                caveats = build_caveats(result, {"analytics": "anomalies_vs_category", "profile": prof})
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: category baseline z-threshold {z}, min days {min_days}\n" + ("\n".join(f"- {c}" for c in caveats)))
                run_dir = reporter.save_artifacts({"intent": "analytic", "notes": "anomalies_vs_category"}, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0
        if parsed.special and parsed.special.get("type") == "trends":
            by = parsed.special.get("by", "month")
            t0 = _time.time()
            result = trends_summary(executor, parquet_path, by=by)
            latency = _time.time() - t0
            concise = make_concise_answer(result, {"analytics": "trends"})
            (console.print(concise) if console else print(concise))
            _render_result(console, f"trends summary by {by}", result)
            if args.save_run:
                reporter = Reporter()
                expl = summarize_answer(question, f"--analytics: trends_summary (by={by})", result, args.model) or ""
                caveats = build_caveats(result, {"analytics": "trends", "profile": prof})
                hypo = generate_hypotheses(question, expl or f"trends {by}", args.model) or ""
                missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + ("Hypotheses:\n" + hypo + "\n\n" if hypo else "") + f"Notes: trends (by={by})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                plan = {"intent": "analytic", "notes": "trends", "params": {"by": by}, "pseudo": "aggregate totals by period (month/day) -> compute growth and MAs"}
                run_dir = reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                (console.print(Panel.fit(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)")) if console else print(f"Artifacts saved to {run_dir} (Latency: {latency:.2f}s)"))
            return 0

        # If rule parser failed, try OpenAI planner for complex analytic mapping
        if parsed.plan is None:
            cols = [c.name for c in schema.columns]
            directive = choose_analytic_tool(question, cols, args.model)
            if directive and directive.get('tool'):
                tool = directive['tool']
                params = directive.get('params', {})
                # Dispatch to tool
                if tool == 'correlation':
                    t0 = _time.time()
                    method = params.get('method', 'pearson')
                    include_pvalue = bool(params.get('include_pvalue', False))
                    result = correlation_pipelines(executor, parquet_path, method=method, include_pvalue=include_pvalue)
                    latency = _time.time() - t0
                    concise = make_concise_answer(result, {"analytics": "correlation"})
                    (console.print(concise) if console else print(concise))
                    _render_result(console, "pipeline correlation (top pairs)", result)
                    if args.save_run:
                        reporter = Reporter()
                        expl = summarize_answer(question, f"--analytics: correlation_pipelines (method={method}, include_pvalue={include_pvalue})", result, args.model) or ""
                        caveats = build_caveats(result, {"analytics": "correlation", "profile": prof, "method": method, "include_pvalue": include_pvalue})
                        missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                        summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: correlation (method={method}, include_pvalue={include_pvalue})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                        plan = {"intent": "analytic", "notes": "correlation", "params": {"method": method, "include_pvalue": include_pvalue}, "pseudo": "pivot daily totals by pipeline, compute pairwise correlations"}
                        reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                    return 0
                if tool == 'clustering':
                    k = int(params.get('k', 5))
                    scaling = params.get('scaling', 'standard')
                    algorithm = params.get('algorithm', 'kmeans')
                    seed = int(params.get('seed', 42))
                    t0 = _time.time()
                    result = cluster_pipelines_monthly(executor, parquet_path, k=k, scaling=scaling, algorithm=algorithm, seed=seed)
                    latency = _time.time() - t0
                    concise = make_concise_answer(result, {"analytics": "clustering"})
                    (console.print(concise) if console else print(concise))
                    _render_result(console, f"pipeline clusters (k={k}, scaling={scaling}, algorithm={algorithm}, seed={seed})", result)
                    if args.save_run:
                        reporter = Reporter()
                        expl = summarize_answer(question, f"--analytics: cluster_pipelines_monthly (k={k}, scaling={scaling}, algorithm={algorithm}, seed={seed})", result, args.model) or ""
                        caveats = build_caveats(result, {"analytics": "clustering", "profile": prof, "algorithm": algorithm})
                        missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                        summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: clustering (k={k}, scaling={scaling}, algorithm={algorithm}, seed={seed})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                        plan = {"intent": "analytic", "notes": "clustering", "params": {"k": k, "scaling": scaling, "algorithm": algorithm, "seed": seed}, "pseudo": "monthly totals by pipeline -> scale -> (MiniBatch)KMeans -> labels & silhouette"}
                        reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                    return 0
                if tool == 'anomalies_vs_category':
                    t0 = _time.time()
                    result = anomalies_vs_category(executor, parquet_path, **{k: v for k, v in params.items() if k in {'z_threshold','min_anomaly_days','year','state','rec_del_sign'}})
                    latency = _time.time() - t0
                    concise = make_concise_answer(result, {"analytics": "anomalies_vs_category"})
                    (console.print(concise) if console else print(concise))
                    _render_result(console, "anomalous locations vs category baseline", result)
                    if args.save_run:
                        reporter = Reporter()
                        z = params.get('z_threshold'); mnd = params.get('min_anomaly_days'); yr = params.get('year'); st = params.get('state'); rds = params.get('rec_del_sign')
                        expl = summarize_answer(question, f"--analytics: anomalies_vs_category (z>={z}, min_days={mnd}, year={yr}, state={st}, rec_del_sign={rds})", result, args.model) or ""
                        caveats = build_caveats(result, {"analytics": "anomalies_vs_category", "profile": prof})
                        missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                        summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: category baseline anomalies (z>={z}, min_days={mnd})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                        plan = {"intent": "analytic", "notes": "anomalies_vs_category", "params": {"z_threshold": z, "min_anomaly_days": mnd, "year": yr, "state": st, "rec_del_sign": rds}, "pseudo": "per-day per-category baselines -> z-scores per location -> group & rank"}
                        reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                    return 0
                if tool == 'anomalies_iqr':
                    k = float(params.get('k', 1.5))
                    t0 = _time.time()
                    result = anomalies_iqr(executor, parquet_path, k=k)
                    latency = _time.time() - t0
                    concise = make_concise_answer(result, {"analytics": "anomalies_iqr"})
                    (console.print(concise) if console else print(concise))
                    _render_result(console, "daily outliers by IQR", result)
                    if args.save_run:
                        reporter = Reporter()
                        expl = summarize_answer(question, f"--analytics: anomalies_iqr (k={k})", result, args.model) or ""
                        caveats = build_caveats(result, {"analytics": "anomalies_iqr", "profile": prof})
                        missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                        summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: IQR outliers (k={k})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                        plan = {"intent": "analytic", "notes": "anomalies_iqr", "params": {"k": k}, "pseudo": "daily totals -> IQR fences -> flag outliers"}
                        reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                    return 0
                if tool == 'sudden_shifts':
                    window = int(params.get('window', 7))
                    sigma = float(params.get('sigma', 3.0))
                    t0 = _time.time()
                    result = sudden_shifts(executor, parquet_path, window=window, sigma=sigma)
                    latency = _time.time() - t0
                    concise = make_concise_answer(result, {"analytics": "sudden_shifts"})
                    (console.print(concise) if console else print(concise))
                    _render_result(console, f"sudden shifts (window={window}, sigma={sigma})", result)
                    if args.save_run:
                        reporter = Reporter()
                        expl = summarize_answer(question, f"--analytics: sudden_shifts (window={window}, sigma={sigma})", result, args.model) or ""
                        caveats = build_caveats(result, {"analytics": "sudden_shifts", "profile": prof})
                        missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                        summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: sudden shifts (window={window}, sigma={sigma})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                        plan = {"intent": "analytic", "notes": "sudden_shifts", "params": {"window": window, "sigma": sigma}, "pseudo": "daily totals -> rolling mean/std -> |x-mean|/std >= sigma"}
                        reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                    return 0
                if tool == 'trends':
                    by = params.get('by', 'month')
                    t0 = _time.time()
                    result = trends_summary(executor, parquet_path, by=by)
                    latency = _time.time() - t0
                    concise = make_concise_answer(result, {"analytics": "trends"})
                    (console.print(concise) if console else print(concise))
                    _render_result(console, f"trends summary by {by}", result)
                    if args.save_run:
                        reporter = Reporter()
                        expl = summarize_answer(question, f"--analytics: trends_summary (by={by})", result, args.model) or ""
                        caveats = build_caveats(result, {"analytics": "trends", "profile": prof})
                        missing_note = "Missing-value handling: COALESCE(scheduled_quantity,0) for totals."
                        summary = (f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + f"Notes: trends (by={by})\n- {missing_note}\n" + ("\n".join(f"- {c}" for c in caveats)))
                        plan = {"intent": "analytic", "notes": "trends", "params": {"by": by}, "pseudo": "aggregate totals by period -> growth & MAs"}
                        reporter.save_artifacts(plan, None, result, summary, latency_sec=latency)
                    return 0
            # If still unknown, inform user
            msg = "I can: anomalies vs category, correlations, clustering, preview, sums, distincts, group-bys, and top-N by a column."
            (console.print(Panel.fit(msg)) if console else print(msg))
            return 1

        # Deterministic rule-based
        sql, params = build_sql(parquet_path, parsed.plan, schema)
        if console:
            console.print(Panel.fit("Executed SQL:"))
            console.print(sql)
        else:
            print("Executed SQL:\n" + sql)
        t0 = _time.time()
        result = executor.query(sql, params)
        latency = _time.time() - t0
        concise = make_concise_answer(result, {"intent": parsed.intent})
        (console.print(concise) if console else print(concise))
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
            caveats = build_caveats(result, {"profile": prof})
            # Optional hypothesis generation
            hypo = generate_hypotheses(question, expl or parsed.notes, args.model) or ""
            summary = f"Question: {question}\n\n" + (expl + "\n\n" if expl else "") + ("Hypotheses:\n" + hypo + "\n\n" if hypo else "") + f"Notes: {parsed.notes}\n" + ("\n".join(f"- {c}" for c in caveats))
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
