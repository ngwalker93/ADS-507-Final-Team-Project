# FDA Data Sources

## Why This Folder is Empty

The FDA datasets are too large to store directly in GitHub (119MB NDC database exceeds GitHub's file size limits). Instead, the data is downloaded dynamically at runtime using the `download_data.py` script.

## Primary Data Sources

### 1. FDA National Drug Code (NDC) Database
- **Overview:** https://open.fda.gov/apis/drug/ndc/
- **Download Instructions:** https://open.fda.gov/apis/drug/ndc/download/
- **Direct Download URL:** https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip
- **Size:** ~119 MB (compressed)
- **Format:** JSON (zipped)
- **Description:** Comprehensive database of drug products with NDC codes, manufacturer information, packaging details, and product characteristics

### 2. FDA Drug Shortages
- **Overview:** https://open.fda.gov/apis/drug/drugshortages/
- **Download Instructions:** https://open.fda.gov/apis/drug/drugshortages/download/
- **Direct Download URL:** https://download.open.fda.gov/drug/shortage/drug-shortage-0001-of-0001.json.zip
- **Size:** ~2 MB
- **Format:** JSON (zipped)
- **Description:** Current and historical drug shortage information including status, reasons, and affected manufacturers

## How to Download

Run the automated download script:
```bash
python scripts/download_data.py
```

This will create the following files in this directory:
- `drug-ndc-0001-of-0001.json` - Raw NDC data
- `drug-shortage-0001-of-0001.json` - Raw shortage data
- CSV files (created by process_data.py)

## Data Pipeline Flow

1. Download raw data → `download_data.py`
2. Process and clean data → `process_data.py`
3. Load into MySQL → `load_to_mysql.py`
