# ADS-507 Final Team Project
## FDA Drug Shortage Analysis Pipeline

### Team Members
- Mark Villanueva
- Nancy Walker
- Sheshma Jaganathan

---

## Project Overview

This project builds a MySQL-based data pipeline that combines two FDA datasets (National Drug Code database and Drug Shortages) to enable enriched analysis. By joining these datasets, we can answer questions that aren't possible with either dataset alone, such as:
- Which manufacturers have the highest shortage risk?
- Do branded drugs have longer shortage durations than generics?
- Which package types are most vulnerable to shortages?

---

## Repository Structure
```
ADS-507-Final-Team-Project/
├── data/                      # Local data storage (not committed to GitHub)
│   └── DATA_SOURCE.md         # Data source documentation
├── docs/                      # Documentation and diagrams
├── scripts/                   # Python automation scripts
│   ├── download_data.py       # Downloads FDA datasets
│   ├── process_data.py        # Cleans and processes data
│   └── load_to_mysql.py       # Loads data into MySQL
├── sql/                       # SQL scripts
│   ├── 01_create_tables.sql   # Creates database structure
│   ├── 02_transformations.sql # Joins datasets (required SQL transformation)
│   └── 03_analysis_queries.sql# Analytical queries
├── .gitignore                 # Prevents committing large files
└── requirements.txt           # Python dependencies
```

---

## Prerequisites

Before running the pipeline, ensure you have:

1. **Python 3.8+** installed
   - Check: `python --version`
   - Download: https://www.python.org/downloads/

2. **MySQL Server** installed and running
   - MySQL Workbench (recommended for running SQL scripts)
   - Download: https://dev.mysql.com/downloads/

3. **Git** (for cloning repository)
   - Download: https://git-scm.com/downloads

---

## Setup Instructions

### Step 1: Clone the Repository
```bash
git clone https://github.com/ngwalker93/ADS-507-Final-Team-Project.git
cd ADS-507-Final-Team-Project
```

### Step 2: Install Python Dependencies
```bash
pip install -r requirements.txt
```

This installs: pandas, requests, mysql-connector-python, sqlalchemy, streamlit

---

## Pipeline Execution (Sequential Order)

### **Phase 1: Download Raw Data**
```bash
python scripts/download_data.py
```

**What it does:**
- Downloads FDA NDC database (~119MB)
- Downloads FDA Drug Shortages dataset (~2MB)
- Saves raw JSON files to `data/` folder

**Expected output:**
- `data/drug-ndc-0001-of-0001.json`
- `data/drug_shortages_raw.json`

---

### **Phase 2: Process and Clean Data**
```bash
python scripts/process_data.py
```

**What it does:**
- Reads raw JSON files
- Normalizes nested structures
- Creates clean CSV tables

**Expected output:**
- `data/ndc_core.csv` (core drug products)
- `data/ndc_packaging.csv` (packaging information)
- `data/drug_shortages_core.csv` (shortage data)
- `data/shortage_contacts.csv` (contact information)

---

### **Phase 3: Create MySQL Database**

Open **MySQL Workbench** and run:
```bash
sql/01_create_tables.sql
```

**What it does:**
- Creates database: `fda_shortage_db`
- Creates 4 tables: raw_ndc, raw_ndc_packaging, raw_drug_shortages, shortage_contacts

**Expected output:**
- Database and empty tables created in MySQL

---

### **Phase 4: Load Data into MySQL**

**IMPORTANT:** Before running, update database credentials in `scripts/load_to_mysql.py`:
```python
DB_USER = 'root'              # Your MySQL username
DB_PASSWORD = 'your_password' # Your MySQL password
```

Then run:
```bash
python scripts/load_to_mysql.py
```

**What it does:**
- Connects to MySQL database
- Loads CSV files into corresponding tables
- Verifies row counts

**Expected output:**
- All 4 tables populated with data
- Verification report showing row counts

---

### **Phase 5: Run SQL Transformations**

Open **MySQL Workbench** and run:
```bash
sql/02_transformations.sql
```

**What it does:**
- Joins shortage data with NDC data (required SQL transformation)
- Creates enriched table: `shortages_with_ndc`
- Creates 4 analysis views for dashboard queries

**Expected output:**
- New table: `shortages_with_ndc`
- 4 views: current_package_shortages, multi_package_shortages, manufacturer_risk_analysis, current_manufacturer_risk

---

### **Phase 6: Run Analysis Queries**

Open **MySQL Workbench** and run:
```bash
sql/03_analysis_queries.sql
```

**What it does:**
- Executes 10 analytical queries
- Demonstrates insights only possible through joined data

**Expected output:**
- Query results showing manufacturer risk, shortage trends, etc.

---

## Verification Checklist

After completing all phases, verify:

- [ ] `data/` folder contains 4 CSV files
- [ ] MySQL database `fda_shortage_db` exists
- [ ] 4 raw tables contain data (check row counts)
- [ ] `shortages_with_ndc` table exists and has data
- [ ] 4 analysis views exist
- [ ] Analysis queries return results

---

## Data Sources

- **FDA NDC Database:** https://open.fda.gov/apis/drug/ndc/
- **FDA Drug Shortages:** https://open.fda.gov/apis/drug/drugshortages/

See `data/DATA_SOURCE.md` for detailed documentation.

---

## License

Final project for ADS-507 at University of San Diego
