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


def main(argv=None):
    argv = argv or sys.argv[1:]
    import argparse
    parser = argparse.ArgumentParser(prog="synmax-agent", description="SynMax Data Agent")
    parser.add_argument("--path", dest="path", default=None, help="Path to parquet (defaults to ./data)")
    parser.add_argument("--model", dest="model", default=os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
    parser.add_argument("--max-preview-rows", dest="max_preview_rows", type=int, default=1000)
    parser.add_argument("--timeout-sec", dest="timeout_sec", type=int, default=30)
    parser.add_argument("--save-run", dest="save_run", action="store_true", default=True)
    parser.add_argument("--no-save-run", dest="save_run", action="store_false")
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

    # Simple interactive loop
    while True:
        q = Prompt.ask("Ask a question (:exit to quit)") if Prompt else input("Q (:exit to quit): ")
        if q.strip().lower() in {":exit", ":quit", "exit", "quit"}:
            break

        # Rule-based deterministic planner path
        from agent.exec.duck import DuckDBExecutor
        from agent.utils.schema_cache import SchemaCache
        from agent.planner.rule_planner import parse_simple
        from agent.exec.sql_builder import build_sql

        executor = DuckDBExecutor()
        schema = SchemaCache().get_or_load(executor, parquet_path)
        parsed = parse_simple(q, schema)
        if parsed.plan is None:
            msg = "I can preview, sum scheduled_quantity, distinct counts, group-bys, and top-N by a column."
            (console.print(Panel.fit(msg)) if console else print(msg))
            continue

        sql, params = build_sql(parquet_path, parsed.plan, schema)
        result = executor.query(sql, params)

        if console and Table:
            table = Table(title=f"{parsed.notes}")
            try:
                import pyarrow as pa  # type: ignore
                if isinstance(result, pa.Table):
                    for name in result.column_names:
                        table.add_column(name)
                    for i in range(min(20, result.num_rows)):
                        row = [str(result.column(j)[i].as_py()) for j in range(result.num_columns)]
                        table.add_row(*row)
                    console.print(table)
                else:
                    console.print(str(result))
            except Exception:
                console.print(str(result))
        else:
            print(result)


if __name__ == "__main__":
    main()
