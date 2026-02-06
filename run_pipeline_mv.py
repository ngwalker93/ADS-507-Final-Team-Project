"""
FDA Data Pipeline Runner
========================
Convenience script that executes the complete ETL pipeline in sequence.
This is optional - you can also run scripts individually following the README.

PREREQUISITE: You must create database tables first!
Run sql/01_create_tables.sql in MySQL Workbench before running this script.

Usage:
    python run_pipeline.py

Requirements:
    - MySQL server must be running
    - Database tables must be created (01_create_tables.sql)
    - Database credentials must be configured in scripts/load_to_mysql.py
"""

import subprocess
import sys
import os
from datetime import datetime


def print_header(message):
    """Print a formatted header"""
    print(f"\n{'='*70}")
    print(f"  {message}")
    print(f"{'='*70}\n")


def print_step(step_num, total_steps, description):
    """Print step information"""
    print(f"\n[{step_num}/{total_steps}] {description}")
    print("-" * 70)


def run_command(command, description):
    """
    Execute a command and handle errors
    
    Args:
        command: Shell command to execute
        description: Human-readable description of the step
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=False,
            text=True
        )
        print(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ ERROR: {description} failed!")
        print(f"Command: {command}")
        print(f"Exit code: {e.returncode}")
        return False


def check_prerequisites():
    """Check if user has created database tables"""
    print("\n" + "="*70)
    print("PREREQUISITE CHECK")
    print("="*70)
    print("\nBefore running this script, you must create the database tables.")
    print("\nSteps if you haven't done this yet:")
    print("  1. Open MySQL Workbench")
    print("  2. File → Open SQL Script → sql/01_create_tables.sql")
    print("  3. Click the lightning bolt ⚡ to execute")
    print("  4. Then come back and run this script")
    print("\n" + "="*70)
    print("\nHave you already run sql/01_create_tables.sql?")
    print("  [YES] Press Enter to continue")
    print("  [NO]  Press Ctrl+C to exit and create tables first")
    print("="*70 + "\n")
    
    try:
        input()  # Wait for user confirmation
    except KeyboardInterrupt:
        print("\n\n✗ Exiting. Please create database tables first.")
        print("   Run sql/01_create_tables.sql in MySQL Workbench")
        sys.exit(0)


def main():
    """Execute the complete pipeline"""
    
    start_time = datetime.now()
    
    print_header("FDA Drug Shortage Analysis Pipeline")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check if we're in the right directory
    if not os.path.exists("scripts/download_data.py"):
        print("✗ ERROR: Must run from project root directory")
        print("Current directory:", os.getcwd())
        sys.exit(1)
    
    # Check prerequisites
    check_prerequisites()
    
    total_steps = 3
    
    # Step 1: Download data
    print_step(1, total_steps, "Downloading FDA datasets")
    if not run_command("python scripts/download_data.py", "Data download"):
        sys.exit(1)
    
    # Step 2: Process data
    print_step(2, total_steps, "Processing and cleaning data")
    if not run_command("python scripts/process_data.py", "Data processing"):
        sys.exit(1)
    
    # Step 3: Load to MySQL
    print_step(3, total_steps, "Loading data to MySQL")
    print("Note: Ensure MySQL is running and credentials are configured")
    if not run_command("python scripts/load_to_mysql.py", "Data loading"):
        sys.exit(1)
    
    # Calculate duration
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Success message
    print_header("Pipeline Completed Successfully!")
    print(f"Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration: {duration.seconds // 60} minutes {duration.seconds % 60} seconds")
    
    # Next steps
    print("\n" + "="*70)
    print("NEXT STEPS - Complete these manually:")
    print("="*70)
    print("\n1. Open MySQL Workbench and run SQL transformations:")
    print("   File → Open SQL Script → sql/02_transformations.sql")
    print("   Click the lightning bolt ⚡ to execute")
    print("\n2. Run analysis queries:")
    print("   File → Open SQL Script → sql/03_analysis_queries.sql")
    print("   Click the lightning bolt ⚡ to execute")
    print("   Click through Result tabs to view query outputs")
    print("\n3. Launch the interactive dashboard:")
    print("   python -m streamlit run scripts/dashboard.py")
    print("   Dashboard opens at http://localhost:8501")
    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    main()
