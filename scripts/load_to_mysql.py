"""
Load Processed Data to MySQL
Loads cleaned CSV files into MySQL database tables
"""

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


print("Starting data load to MySQL...")

def get_engine():
    """Create a SQLAlchemy engine using environment variables."""
    user = os.getenv("DB_USER", "pipeline_user")
    password = os.getenv("DB_PASSWORD", "pipeline_password")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db = os.getenv("DB_NAME", "fda_shortage_db")

    conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str, pool_pre_ping=True)


def require_file(path):
    """Stop immediately if a required file is missing."""
    if not Path(path).exists():
        raise FileNotFoundError(f"Required file not found: {path}")


def clear_tables(conn):
    """
    Remove existing rows without dropping tables.
    This function disables foreign key checks to allow truncation
    of child tables before parent tables.
    """
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

    # child tables first
    conn.execute(text("DELETE FROM raw_ndc_packaging;"))
    conn.execute(text("DELETE FROM shortage_contacts;"))

    # parent tables next
    conn.execute(text("DELETE FROM raw_ndc;"))
    conn.execute(text("DELETE FROM raw_drug_shortages;"))

    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))


def load_csv(conn, csv_path: str, table_name: str) -> int:
    """Read a CSV and append rows into an existing MySQL table."""
    df = pd.read_csv(csv_path)
    df.to_sql(
        name=table_name,
        con=conn,
        if_exists="append",   # important: do not drop tables
        index=False,
        chunksize=2000,
        method="multi",
    )
    return len(df)


def main() -> None:
    print("Starting data load to MySQL (pipeline-safe)...")

    # These files must exist by the time this script runs
    csv_plan = [
        ("data/ndc_core.csv", "raw_ndc"),
        ("data/ndc_packaging.csv", "raw_ndc_packaging"),
        ("data/drug_shortages_core.csv", "raw_drug_shortages"),
        ("data/shortage_contacts.csv", "shortage_contacts"),
    ]

    for csv_path, _ in csv_plan:
        require_file(csv_path)

    engine = get_engine()

    try:
        with engine.begin() as conn:
            print(" Connected. Clearing existing rows...")
            clear_tables(conn)
            print(" Tables cleared.")

            print("\nLoading CSV files into MySQL tables...")
            for csv_path, table_name in csv_plan:
                print(f" Loading {csv_path} into {table_name}...")
                rows = load_csv(conn, csv_path, table_name)
                print(f" Inserted {rows:,} rows into {table_name}")

        # verification
        with engine.connect() as conn:
            print("\nRow count verification:")
            for _, table_name in csv_plan:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};")).scalar()
                print(f"  {table_name}: {int(cnt):,} rows")

        print("\n Data load completed successfully.")

    finally:
        engine.dispose()
        print("Database connection closed.")


if __name__ == "__main__":
    main()