"""
FDA Data Download Script
Downloads NDC and Drug Shortage datasets from FDA's official sources
"""

import pandas as pd
import requests
import zipfile
import io
import json
import os

print("Starting FDA data download...")

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# ============================================
# Download NDC Dataset
# ============================================
print("\n1. Downloading NDC dataset (this may take a few minutes, ~119MB)...")

ndc_url = "https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip"

try:
    response = requests.get(ndc_url)
    response.raise_for_status()
    
    print("   Download complete. Extracting...")
    
    # Extract zip file
    zip_bytes = io.BytesIO(response.content)
    with zipfile.ZipFile(zip_bytes) as z:
        z.extractall("data")
    
    print("   ✓ NDC dataset downloaded and extracted to data/")
    
except Exception as e:
    print(f"   ✗ Error downloading NDC dataset: {e}")

# ============================================
# Download Drug Shortages Dataset
# ============================================
print("\n2. Downloading Drug Shortages dataset...")

shortages_url = "https://download.open.fda.gov/drug/shortages/drug-shortages-0001-of-0001.json.zip"

try:
    response = requests.get(shortages_url)
    response.raise_for_status()
    
    print("   Download complete. Extracting...")
    
    # Extract zip file
    zip_bytes = io.BytesIO(response.content)
    with zipfile.ZipFile(zip_bytes) as z:
        z.extractall("data")
    
    print("   ✓ Drug Shortages dataset downloaded and extracted to data/")
    
except Exception as e:
    print(f"   ✗ Error downloading Drug Shortages dataset: {e}")

print("\n✓ All downloads complete!")
print("\nNext step: Run process_data.py to clean and prepare the data for MySQL")
