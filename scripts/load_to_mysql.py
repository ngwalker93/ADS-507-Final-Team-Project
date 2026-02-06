"""
Load Processed Data to MySQL
Loads cleaned CSV files into MySQL database tables
"""

import pandas as pd
from sqlalchemy import create_engine
import os

print("Starting data load to MySQL...")

# ============================================
# Database Connection Configuration
# ============================================

# MySQL connection parameters
# Update these with your actual MySQL credentials
DB_USER = 'root'  # Change if different
DB_PASSWORD = '5791216sS$'  # CHANGE THIS to your MySQL password
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'fda_shortage_db'

# Create connection string
connection_string = f'mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

try:
    # Create database engine
    engine = create_engine(connection_string)
    print(f"✓ Connected to MySQL database: {DB_NAME}")
    
except Exception as e:
    print(f"✗ Error connecting to MySQL: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure MySQL is running")
    print("2. Update DB_PASSWORD in this script")
    print("3. Ensure database 'fda_shortage_db' exists (run 01_create_tables.sql first)")
    exit(1)

# ============================================
# Load CSV Files into MySQL Tables
# ============================================

# Define CSV files and their corresponding MySQL tables
csv_table_mapping = {
    'data/ndc_core.csv': 'raw_ndc',
    'data/ndc_packaging.csv': 'raw_ndc_packaging',
    'data/drug_shortages_core.csv': 'raw_drug_shortages',
    'data/shortage_contacts.csv': 'shortage_contacts'
}

# Load each CSV into its table
for csv_file, table_name in csv_table_mapping.items():
    
    print(f"\nLoading {csv_file} into {table_name}...")
    
    try:
        # Check if CSV file exists
        if not os.path.exists(csv_file):
            print(f"  ⚠ Warning: {csv_file} not found. Skipping.")
            print(f"     Run process_data.py first to create this file.")
            continue
        
        # Read CSV file
        df = pd.read_csv(csv_file)
        print(f"  - Read {len(df)} rows from CSV")
        
        # Load into MySQL
        # if_exists='append' will add to existing data
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False
        )
        
        print(f"  ✓ Loaded {len(df)} rows into {table_name}")
        
    except Exception as e:
        print(f"  ✗ Error loading {csv_file}: {e}")
        print(f"  Full error details:")
        import traceback
        traceback.print_exc()
        continue

# ============================================
# Verify Data Load
# ============================================

print("\n" + "="*50)
print("Data Load Verification")
print("="*50)

try:
    # Check row counts in each table
    for table_name in csv_table_mapping.values():
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = pd.read_sql(query, engine)
        count = result['count'][0]
        print(f"{table_name}: {count} rows")
    
    print("\n✓ All data loaded successfully!")
    print("\nNext steps:")
    print("1. Run sql/02_transformations.sql to create enriched tables")
    print("2. Run sql/03_analysis_queries.sql to analyze the data")
    
except Exception as e:
    print(f"\n✗ Error during verification: {e}")

finally:
    # Close the database connection
    engine.dispose()
    print("\nDatabase connection closed.")
