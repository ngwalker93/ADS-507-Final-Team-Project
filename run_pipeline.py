#this script will run full FDA ETL pipeline using Github Actions
#The order follwed for ETL steps is
#dowload data
#process data
#load to MYSQL

import os
import subprocess
import sys
from dotenv import load_dotenv

load_dotenv()

def run(cmd, use_shell=False):
    print(f"Running: {cmd}")
    subprocess.run(cmd, shell=use_shell, check=True)

def main():
    try:
        # 1. Download data
        run("python scripts/download_data.py", use_shell=True)

        # 2. Process data
        run("python scripts/process_data.py", use_shell=True)

        # 3. Load data to MySQL
        run("python scripts/load_to_mysql.py", use_shell=True)

        # 4. Run SQL transformations (IMPORTANT FIX)
        run(
            f"""
            mysql -h {os.environ['DB_HOST']} \
                  -P {os.environ['DB_PORT']} \
                  -u {os.environ['DB_USER']} \
                  -p{os.environ['DB_PASSWORD']} \
                  {os.environ['DB_NAME']} \
                  < sql/02_transformations.sql
            """,
            use_shell=True
        )

        print("ETL pipeline completed successfully.")

    except subprocess.CalledProcessError:
        print("ETL pipeline failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()