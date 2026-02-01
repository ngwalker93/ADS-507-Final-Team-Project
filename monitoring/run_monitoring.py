#Enables pipeline monitoring through CI status check
#This file executes the monitoring process for FDA drug shortages automatically.
#Purpose is to automate pipeline healthchecks and quality after each run and fails the 
#pipeline if any issues are detected.



from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text


SQL_FILES = [
    "monitoring/pipeline_health.sql",
    "monitoring/data_quality_checks.sql",
]

REPORT_DIR = Path("monitoring/reports")
REPORT_PATH = REPORT_DIR / "monitoring_report.txt"


def get_db_engine():
    """Create a SQLAlchemy engine from environment variables (with safe defaults)."""
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db = os.getenv("DB_NAME", "fda_shortage_db")

    conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str, pool_pre_ping=True)


def split_sql_into_statements(sql_text: str) -> list[str]:
    """
    Split a SQL file into executable statements.

    Rules:
    - Ignore blank lines and comment-only lines (starting with --)
    - Treat ';' as the end of a statement
    """
    statements: list[str] = []
    buffer: list[str] = []

    for line in sql_text.splitlines():
        stripped = line.strip()

        # Skip empty lines and full-line comments
        if not stripped or stripped.startswith("--"):
            continue

        buffer.append(line)

        # End statement when line ends with ';'
        if stripped.endswith(";"):
            stmt = "\n".join(buffer).strip()
            if stmt:
                statements.append(stmt)
            buffer = []

    # Handle any leftover statement without a trailing ';'
    if buffer:
        stmt = "\n".join(buffer).strip()
        if stmt:
            statements.append(stmt)

    return statements


def format_df(df: pd.DataFrame) -> str:
    """Format a dataframe as readable text for the report."""
    if df.empty:
        return "(no rows returned)"
    return df.to_string(index=False)


def run_sql_file(conn, file_path: str) -> tuple[list[str], bool]:
    """
    Execute statements in one SQL file and return (report_lines, had_failure).
    """
    p = Path(file_path)
    lines: list[str] = []
    had_failure = False

    lines.append(f"\n## {file_path}")

    if not p.exists():
        lines.append(f"[FAIL] Missing SQL file: {file_path}")
        return lines, True

    sql_text = p.read_text(encoding="utf-8")
    statements = split_sql_into_statements(sql_text)

    if not statements:
        lines.append("[WARN] No executable SQL statements found.")
        return lines, False

    for i, stmt in enumerate(statements, start=1):
        lines.append(f"\n--- Statement {i} ---")
        try:
            result = conn.execute(text(stmt))
            if result.returns_rows:
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                lines.append(format_df(df))
            else:
                lines.append("[OK] Executed (no rows returned)")
        except Exception as e:
            lines.append(f"[FAIL] {type(e).__name__}: {e}")
            had_failure = True

    return lines, had_failure


def main() -> None:
    # Ensure reports folder exists (GitHub may not keep empty folders without .gitkeep)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    header = [
        "FDA PIPELINE MONITORING REPORT",
        "=" * 72,
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
        "",
        "Checks included:",
        "- Pipeline health (row counts)",
        "- Data quality (join match %, missing keys)",
        "",
    ]

    engine = get_db_engine()
    report_lines = header
    had_failure = False

    try:
        with engine.connect() as conn:
            for sql_file in SQL_FILES:
                lines, failed = run_sql_file(conn, sql_file)
                report_lines.extend(lines)
                had_failure = had_failure or failed
    finally:
        engine.dispose()

    REPORT_PATH.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(f"Monitoring report saved to: {REPORT_PATH}")

    # Optional: fail the workflow/run if any monitoring step failed
    if had_failure:
        raise SystemExit("Monitoring detected failures. See monitoring_report.txt for details.")


if __name__ == "__main__":
    main()


