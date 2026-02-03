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

    # Fail fast if required vars are missing (recommended)
    required = ["DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    user = os.getenv("DB_USER")                 
    password = os.getenv("DB_PASSWORD")         
    host = os.getenv("DB_HOST", "127.0.0.1")    
    port = os.getenv("DB_PORT", "3306")         
    db = os.getenv("DB_NAME")                   

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

def load_shortage_contacts(conn, csv_path: str) -> int:
    """
    Convert package_ndc in CSV -> shortage_id in DB, then insert.
    Assumes shortage_contacts table has columns: shortage_id, contact_info
    """
    df = pd.read_csv(csv_path)

    # If the file is empty, just do nothing safely
    if df.empty:
        return 0

    # Pull mapping from DB: package_ndc -> one shortage_id
    # (We take MIN(shortage_id) for that package_ndc to keep it deterministic)
    mapping_df = pd.read_sql(
        """
        SELECT package_ndc, MIN(shortage_id) AS shortage_id
        FROM raw_drug_shortages
        WHERE package_ndc IS NOT NULL AND package_ndc <> ''
        GROUP BY package_ndc
        """,
        conn,
    )
    # Merge to get shortage_id
    df["package_ndc"] = df["package_ndc"].astype(str).str.strip()
    merged = df.merge(mapping_df, on="package_ndc", how="left")

    # Keep only rows that successfully mapped to a shortage_id (required FK)
    merged = merged.dropna(subset=["shortage_id"])

     # Build final dataframe matching the DB table
    out = merged[["shortage_id", "contact_info"]].copy()
    out["shortage_id"] = out["shortage_id"].astype(int)

    out.to_sql(
        name="shortage_contacts",
        con=conn,
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi",
    )
    return len(out)


def main() -> None:
    print("Starting data load to MySQL (pipeline-safe)...")

    # These files must exist by the time this script runs
    csv_plan = [
        ("data/ndc_core.csv", "raw_ndc"),
        ("data/ndc_packaging.csv", "raw_ndc_packaging"),
        ("data/drug_shortages_core.csv", "raw_drug_shortages"),
        
    ]
    # Contacts require mapping (package_ndc -> shortage_id)
    contacts_csv = "data/shortage_contacts.csv"

    for csv_path, _ in csv_plan:
        require_file(csv_path)
    require_file(contacts_csv)    

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
            print(f" Loading {contacts_csv} into shortage_contacts (mapping shortage_id)...")
            rows = load_shortage_contacts(conn, contacts_csv)
            print(f" Inserted {rows:,} rows into shortage_contacts")    

        # verification
        with engine.connect() as conn:
            print("\nRow count verification:")
            for _, table_name in csv_plan:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};")).scalar()
                print(f"  {table_name}: {int(cnt):,} rows")
            cnt = conn.execute(text("SELECT COUNT(*) FROM shortage_contacts;")).scalar()
            print(f"  shortage_contacts: {int(cnt):,} rows")
        print("\n Data load completed successfully.")

    finally:
        engine.dispose()
        print("Database connection closed.")


if __name__ == "__main__":
    main()