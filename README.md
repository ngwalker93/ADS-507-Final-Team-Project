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
- How do prescription vs OTC drugs differ in shortage patterns?
- Which routes of administration (oral, IV, injection) are most affected?

**Key Achievement:** ~90% of drug shortage records successfully matched with NDC product data, enabling comprehensive enriched analysis.

---

## Repository Structure
```
ADS-507-Final-Team-Project/
├── .github/workflows/         # CI/CD automation (optional)
│   └── ci.yml                 # Automated dependency validation
├── data/                      # Local data storage (not committed to GitHub)
│   └── DATA_SOURCE.md         # Data source documentation
├── docs/                      # Documentation and diagrams
├── scripts/                   # Python automation scripts
│   ├── download_data.py       # Downloads FDA datasets
│   ├── process_data.py        # Cleans and processes data
│   ├── load_to_mysql.py       # Loads data into MySQL
│   └── dashboard.py           # Streamlit interactive dashboard
├── sql/                       # SQL scripts
│   ├── 01_create_tables.sql   # Creates database structure
│   ├── 02_transformations.sql # Joins datasets (required SQL transformation)
│   └── 03_analysis_queries.sql# Analytical queries
├── .gitignore                 # Prevents committing large files
├── requirements.txt           # Python dependencies
├── run_pipeline.py            # Convenience script (optional)
└── README.md                  # This file
```

---

## Prerequisites

Before running the pipeline, ensure you have:

1. **Python 3.8+** installed
   - Check: `python --version`
   - Download: https://www.python.org/downloads/
   - **IMPORTANT:** During installation, check "Add Python to PATH"

2. **MySQL Server** installed and running
   - MySQL Workbench (recommended for running SQL scripts)
   - Download: https://dev.mysql.com/downloads/
   - Remember your root password!

3. **Git** (for cloning repository)
   - Download: https://git-scm.com/downloads
   - Or simply download as ZIP from GitHub

---

## Setup Instructions

### Step 1: Download the Repository

**Option A: Download ZIP**
1. Click the green "Code" button on GitHub
2. Click "Download ZIP"
3. Extract to a location like `C:\Users\YourName\Desktop\ADS-507`

**Option B: Clone with Git**
```bash
git clone https://github.com/YOUR-USERNAME/ADS-507-Final-Team-Project.git
cd ADS-507-Final-Team-Project
```

### Step 2: Install Python Dependencies

Open Command Prompt or PowerShell and navigate to the project folder:
```bash
cd path\to\ADS-507-Final-Team-Project
```

Install required packages:
```bash
python -m pip install -r requirements.txt
```

This installs: pandas, requests, mysql-connector-python, sqlalchemy, streamlit, plotly

**Time:** 2-3 minutes

---

## Pipeline Execution Options

**You have TWO ways to run the pipeline:**

### **Option A: Automated (Convenience Script)** ⚡

**Step 1: Create database tables FIRST** (one-time setup)
1. Open MySQL Workbench
2. Run `sql/01_create_tables.sql` (Phase 3 from manual instructions below)

**Step 2: Run automated Python pipeline:**
```bash
python run_pipeline.py
```
This automates Phases 1, 2, and 4 (download → process → load)

**Step 3: Complete SQL analysis manually:**
- Run `sql/02_transformations.sql` in MySQL Workbench (Phase 5)
- Run `sql/03_analysis_queries.sql` in MySQL Workbench (Phase 6)

**Step 4: Launch dashboard:**
```bash
python -m streamlit run scripts/dashboard.py
```
**Note:** If dashboard fails with "No module named 'plotly'", install it:
```bash
python -m pip install plotly
```
**Then launch the dashboard again.**

---

### **Option B: Manual (Step-by-Step)** 📖
Follow all phases 1-7 below for full control and inspection at each step.

---

**Recommendation:** Use Option B (manual) for first-time setup to understand each phase. Use Option A for subsequent runs or re-execution.

**Note:** The automated script will check that you've created tables and prompt you if you haven't.

---

## Pipeline Execution (Sequential Order)

### **Phase 1: Download Raw Data**
```bash
python scripts/download_data.py
```

**What it does:**
- Downloads FDA NDC database (~119MB compressed, expands to ~500MB)
- Downloads FDA Drug Shortages dataset (~2MB)
- Saves raw JSON files to `data/` folder

**Expected output:**
- `data/drug-ndc-0001-of-0001.json` (131,118 products)
- `data/drug-shortages-0001-of-0001.json` (1,838 shortage records)

**Time:** 2-5 minutes (depending on internet speed)

---

### **Phase 2: Process and Clean Data**
```bash
python scripts/process_data.py
```

**What it does:**
- Reads raw JSON files
- Normalizes nested structures
- Extracts package_ndc for joining
- **Removes duplicates** (FDA data contains duplicate records)
- Creates clean CSV tables

**Expected output:**
- `data/ndc_core.csv` (~128,805 products after deduplication)
- `data/ndc_packaging.csv` (~244,786 packaging records after deduplication)
- `data/drug_shortages_core.csv` (~1,810 shortages after deduplication)
- `data/shortage_contacts.csv` (~1,810 contact records)

**Time:** 1-2 minutes

**Technical Note:** The script removes duplicate product_ndc and package_ndc values that exist in the raw FDA data.

---

### **Phase 3: Create MySQL Database**

Open **MySQL Workbench** and connect to your local MySQL server.

Then:
1. Click **File** → **Open SQL Script**
2. Navigate to `sql/01_create_tables.sql`
3. Click **Open**
4. Click the **lightning bolt icon** ⚡ (or press Ctrl+Shift+Enter) to execute

**What it does:**
- Creates database: `fda_shortage_db`
- Creates 4 tables with proper structure and indexes:
  + `raw_ndc` (product information)
  + `raw_ndc_packaging` (packaging details)
  + `raw_drug_shortages` (shortage data)
  + `shortage_contacts` (contact information)

**Expected output:**
- Database and empty tables created
- Green checkmarks in Action Output
- Success message displayed

**Time:** < 1 minute

---

### **Phase 4: Load Data into MySQL**

**IMPORTANT:** Before running, update database credentials in `scripts/load_to_mysql.py`:

Open the file and change line ~18:
```python
DB_USER = 'root'              # Your MySQL username
DB_PASSWORD = 'your_password' # CHANGE THIS to your MySQL password!
```

Then run:
```bash
python scripts/load_to_mysql.py
```

**What it does:**
- Connects to MySQL database
- Loads CSV files into corresponding tables
- Uses `if_exists='append'` to avoid foreign key conflicts
- Verifies row counts

**Expected output:**
```
raw_ndc: 128805 rows
raw_ndc_packaging: 244786 rows
raw_drug_shortages: 1810 rows
shortage_contacts: 1810 rows
```

**Time:** 1-3 minutes

---

### **Phase 5: Run SQL Transformations**

Open **MySQL Workbench** and run:
1. **File** → **Open SQL Script**
2. Select `sql/02_transformations.sql`
3. Click the **lightning bolt** ⚡

**What it does:**
- **CRITICAL:** Joins shortage data with NDC data (required SQL transformation!)
- Creates enriched table: `shortages_with_ndc` (~1,810 records)
- Uses `ROW_NUMBER()` to generate shortage_id values
- Creates 4 analysis views for dashboard queries
- Adds indexes for performance

**Expected output:**
- Table `shortages_with_ndc` created with ~1,810 rows
- Match rate: ~90% (shortages successfully matched with NDC data)
- 4 views created:
  + `current_package_shortages`
  + `multi_package_shortages`
  + `manufacturer_risk_analysis`
  + `current_manufacturer_risk`

**Time:** < 1 minute

**Technical Notes:**
- Uses `ROW_NUMBER() OVER (ORDER BY s.package_ndc)` to generate shortage_id
- Indexes on TEXT columns include prefix lengths
- LEFT JOIN preserves all shortage records, even those without NDC matches

---

### **Phase 6: Run Analysis Queries**

Open **MySQL Workbench** and run:
1. **File** → **Open SQL Script**
2. Select `sql/03_analysis_queries.sql`
3. Click the **lightning bolt** ⚡

**What it does:**
- Executes 10 analytical queries
- Demonstrates insights only possible through joined data

**Expected output:**
- 10+ Result tabs with query outputs
- Click through Result 1, Result 2, etc. to see analysis results
- Examples: top manufacturers, brand vs generic, route analysis, etc.

**Time:** < 1 minute

---

### **Phase 7: Run Interactive Dashboard**

**IMPORTANT:** Update database password in `scripts/dashboard.py`

Then run:
```bash
python -m streamlit run scripts/dashboard.py
```

**What it does:**
- Launches interactive web dashboard
- Opens automatically in your browser at `http://localhost:8501`
- Displays real-time visualizations and metrics

**Features:**
- Key metrics overview (total/current shortages, affected manufacturers)
- Top manufacturers by risk (bar chart)
- Brand vs generic analysis (pie chart)
- Route of administration analysis (bar chart)
- Prescription vs OTC comparison
- Detailed shortage table (searchable, filterable, downloadable)

**To stop the dashboard:** Press `Ctrl+C` in the terminal

**Time:** Dashboard loads in 5-10 seconds

**Troubleshooting:**
- If you get "streamlit not recognized", use `python -m streamlit run scripts/dashboard.py` instead
- If you get "No module named 'plotly'" error, first run: `python -m pip install plotly`, then launch the dashboard again.
  
---

## Verification Checklist

After completing all phases, verify:

- [ ] `data/` folder contains 6 files (2 JSON, 4 CSV)
- [ ] MySQL database `fda_shortage_db` exists
- [ ] 4 raw tables contain data (check row counts in Workbench)
- [ ] `shortages_with_ndc` table exists and has ~1,810 rows
- [ ] Match percentage is approximately 90%
- [ ] 4 analysis views exist
- [ ] Analysis queries return results (10+ Result tabs)
- [ ] Dashboard launches and displays data

---

## Continuous Integration (Optional)

This repository includes automated testing via GitHub Actions:

**What it does:**
- Automatically tests dependency installation on every push
- Validates that `requirements.txt` is working correctly
- Runs basic import checks for key packages

**View test results:**
1. Go to the repository on GitHub
2. Click the **"Actions"** tab
3. See green checkmark ✅ (passing) or red X ❌ (failing)

**Note:** This is supplemental validation. The primary workflow is the manual README-driven process described above, as it allows inspection of data at each step and demonstrates understanding of the pipeline.

---

## Data Sources

- **FDA NDC Database:** https://open.fda.gov/apis/drug/ndc/
- **FDA Drug Shortages:** https://open.fda.gov/apis/drug/drugshortages/

See `data/DATA_SOURCE.md` for detailed documentation.

---

## Project Results

### Data Pipeline Statistics
- **128,805** drug products in NDC database (after deduplication)
- **244,786** packaging records (after deduplication)
- **1,810** drug shortage records (after deduplication)
- **~90%** successful match rate (shortages matched with NDC data)
- **1,810** enriched shortage records with full product details

### Key Insights Enabled by Data Join
- Brand name vs generic drug shortage patterns
- Product type analysis (prescription vs OTC vs bulk ingredients)
- Route of administration vulnerability assessment
- Package type distribution in shortages
- Manufacturer portfolio risk analysis

These insights are **only possible** because we joined shortage data with the NDC product database.

---

## License

Educational project for ADS-507 at University of San Diego

---

**Last Updated:** February 1, 2026  
**Pipeline Status:** Fully functional and tested on Windows 11  
**Tested On:** Python 3.14.2, MySQL 8.0, Windows PowerShell
